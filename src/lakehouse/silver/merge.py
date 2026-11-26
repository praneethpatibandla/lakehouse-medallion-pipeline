"""Reusable building blocks for incremental silver loads.

Silver tables are not rebuilt from scratch every day. Each run takes one
bronze batch, keeps the newest copy of every record, and MERGEs it into the
existing Delta table: new keys are inserted, known keys are updated. The
helpers here are shared by the silver jobs (orders in step 09, customers in
step 10, products and order items in step 11).

The SQL condition builders and the metrics parser are plain Python, so they
are unit tested without Spark.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Sequence

from lakehouse.utils.tables import create_table_if_not_exists

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

TARGET_ALIAS = "t"
SOURCE_ALIAS = "s"


# --------------------------------------------------------------------------
# Pure Python: SQL conditions and MERGE metrics
# --------------------------------------------------------------------------

def merge_condition(keys: Sequence[str], target: str = TARGET_ALIAS,
                    source: str = SOURCE_ALIAS) -> str:
    """Return the MERGE join condition, e.g. "t.`order_id` = s.`order_id`".

    Backticks keep the condition valid even for column names with odd
    characters. Several keys are combined with AND (a composite key).
    """
    if not keys:
        raise ValueError("At least one key column is required for a MERGE")
    return " AND ".join(f"{target}.`{key}` = {source}.`{key}`" for key in keys)


def newer_condition(sequence_column: str, target: str = TARGET_ALIAS,
                    source: str = SOURCE_ALIAS) -> str:
    """Return the "only update if the incoming row is not older" condition.

    Protects against out-of-order data: if Monday's file is reprocessed after
    Tuesday's, Monday's older status must not overwrite Tuesday's newer one.
    `>=` (not `>`) lets a rerun of the same batch refresh the row, which
    keeps reruns harmless. A target row with no sequence value can always be
    replaced, because there is nothing to compare against.
    """
    col = f"`{sequence_column}`"
    return f"{target}.{col} IS NULL OR {source}.{col} >= {target}.{col}"


@dataclass(frozen=True)
class MergeStats:
    """Row counts reported by Delta for one MERGE."""

    source_rows: int
    inserted: int
    updated: int
    deleted: int = 0


def parse_merge_metrics(metrics: dict[str, str] | None) -> MergeStats:
    """Turn Delta's `operationMetrics` (all values are strings) into numbers."""
    metrics = metrics or {}

    def number(name: str) -> int:
        return int(metrics.get(name) or 0)

    return MergeStats(
        source_rows=number("numSourceRows"),
        inserted=number("numTargetRowsInserted"),
        updated=number("numTargetRowsUpdated"),
        deleted=number("numTargetRowsDeleted"),
    )


# --------------------------------------------------------------------------
# Spark: deduplication and the MERGE itself
# --------------------------------------------------------------------------

def split_null_keys(df: "DataFrame", keys: Sequence[str]) -> tuple["DataFrame", "DataFrame"]:
    """Split rows into (rows with every key present, rows missing a key).

    A row without its business key cannot be matched to anything, so it can
    never be merged. The job counts and logs these instead of loading them.
    """
    from functools import reduce

    from pyspark.sql import functions as F

    has_keys = reduce(lambda a, b: a & b, [F.col(k).isNotNull() for k in keys])
    return df.where(has_keys), df.where(~has_keys)


def deduplicate_latest(df: "DataFrame", keys: Sequence[str],
                       order_columns: Sequence[str]) -> "DataFrame":
    """Keep one row per key: the one with the highest `order_columns` values.

    Columns are compared in order (the second breaks ties in the first) and
    NULLs sort last, so a row with a real timestamp beats one without. This
    must run on *typed* columns: as strings, "18/11/2026" would sort after
    "2026-11-19" and the wrong copy would win.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    window = Window.partitionBy(*keys).orderBy(*[F.col(c).desc_nulls_last() for c in order_columns])
    return (df.withColumn("_row_rank", F.row_number().over(window))
              .where(F.col("_row_rank") == 1)
              .drop("_row_rank"))


def upsert(spark: "SparkSession", df: "DataFrame", path: str, keys: Sequence[str],
           sequence_column: str | None = None) -> MergeStats:
    """MERGE `df` into the Delta table at `path` (insert new keys, update known ones).

    `df` must already have one row per key: Delta fails a MERGE when two
    source rows match the same target row, because it cannot know which one
    should win. That is why the jobs call `deduplicate_latest` first.

    The table is created (empty, with `df`'s schema) if it does not exist,
    so the first run and every later run use the same MERGE code path.
    """
    table = create_table_if_not_exists(spark, path, df.schema)
    version_before = current_version(table)
    update_when = newer_condition(sequence_column) if sequence_column else None
    (table.alias(TARGET_ALIAS)
          .merge(df.alias(SOURCE_ALIAS), merge_condition(keys))
          .whenMatchedUpdateAll(condition=update_when)
          .whenNotMatchedInsertAll()
          .execute())
    return last_operation_stats(table, version_before)


def current_version(table) -> int | None:
    """Return the table's latest commit version (None for a table with no commits)."""
    latest = table.history(1).select("version").first()
    return latest["version"] if latest else None


def last_operation_stats(table, version_before: int | None = None) -> MergeStats:
    """Read the row counts of the latest commit from the Delta transaction log.

    If no new commit was made since `version_before` (a MERGE with nothing to
    change may not commit), zero counts are returned instead of the metrics
    of some older operation.
    """
    latest = table.history(1).select("version", "operationMetrics").first()
    if latest is None or (version_before is not None and latest["version"] == version_before):
        return MergeStats(source_rows=0, inserted=0, updated=0)
    return parse_merge_metrics(latest["operationMetrics"])
