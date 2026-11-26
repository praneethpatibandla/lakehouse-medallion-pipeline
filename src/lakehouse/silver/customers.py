"""Silver customers as a Type 2 slowly changing dimension (SCD2).

Customers arrive as a full daily snapshot. When a customer moves city or
changes segment we do not overwrite the old row: we close it and add a new
version, so history is kept and old orders can still be reported with the
attributes that were true when they were placed.

Every row has:

    valid_from   first batch date this version was seen
    valid_to     batch date it was replaced (exclusive), NULL while current
    is_current   true for the one live version of each customer
    _row_hash    SHA-256 of the tracked attributes, for cheap change detection

The MERGE uses the "staged union" pattern from the Delta Lake docs. Each
incoming customer is staged once with `_merge_key = customer_id`; a changed
customer is staged a second time with `_merge_key = NULL`:

    staged row                    matches current row?   action
    new customer (key)            no                     insert version 1
    unchanged customer (key)      yes, same hash         nothing
    changed customer (key)        yes, different hash    close old version
    changed customer (NULL key)   never                  insert new version

Closing and inserting happen in one MERGE, so the table is never left with
a customer that has zero or two current rows.

Usage (after loading bronze customers for the same day):
    python -m lakehouse.silver.customers --batch-date 2026-11-01
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import TYPE_CHECKING

from lakehouse.bronze.schemas import CUSTOMERS
from lakehouse.silver.merge import (
    MergeStats,
    deduplicate_latest,
    current_version,
    last_operation_stats,
    split_null_keys,
)
from lakehouse.silver.transforms import clean_customers
from lakehouse.utils.config import PipelineConfig, load_config
from lakehouse.utils.logger import get_logger
from lakehouse.utils.tables import create_table_if_not_exists, read_table

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = get_logger(__name__)

TABLE = "customers"
KEY = "customer_id"
# Every descriptive attribute is tracked: a change to any of them creates a
# new version. Pipeline columns (_run_id, _ingested_at, ...) change on every
# load and are deliberately left out, or every run would create a version.
TRACKED_COLUMNS = tuple(c for c in CUSTOMERS if c != KEY)
SCD_COLUMNS = ("valid_from", "valid_to", "is_current", "_row_hash")
MERGE_KEY = "_merge_key"
# Placeholder for NULL inside the hash input (see `with_row_hash`).
_NULL_TOKEN = "<null>"


class OutOfOrderBatchError(RuntimeError):
    """Raised when a snapshot older than the table's history is applied."""


def check_batch_order(batch_date: date, latest_valid_from: date | None) -> None:
    """Refuse to apply a snapshot older than the newest version in the table.

    SCD2 history is built forward in time. Applying 1 Nov after 3 Nov would
    close the 3 Nov versions and make 1 Nov data look current. Re-running the
    newest date is allowed: unchanged rows produce no change at all.
    """
    if latest_valid_from is not None and batch_date < latest_valid_from:
        raise OutOfOrderBatchError(
            f"Cannot apply customers snapshot {batch_date}: the table already has "
            f"versions from {latest_valid_from}. Snapshots must be applied in date "
            "order; rebuild the table to backfill older dates."
        )


def with_row_hash(df: "DataFrame", columns: tuple[str, ...] = TRACKED_COLUMNS) -> "DataFrame":
    """Add `_row_hash`, a fingerprint of the tracked attributes.

    Comparing one hash is simpler than comparing every column with
    NULL-safe equality. NULLs are replaced by a token first because
    `concat_ws` skips NULLs: ("a", NULL, "b") and ("a", "b", NULL) would
    otherwise hash the same.
    """
    from pyspark.sql import functions as F

    parts = [F.coalesce(F.col(c).cast("string"), F.lit(_NULL_TOKEN)) for c in columns]
    return df.withColumn("_row_hash", F.sha2(F.concat_ws("||", *parts), 256))


def prepare_snapshot(bronze: "DataFrame", batch_date: date) -> tuple["DataFrame", "DataFrame"]:
    """Clean one snapshot and shape it as new current versions.

    Returns (one row per customer with SCD columns, rows rejected for a
    missing customer_id).
    """
    from pyspark.sql import functions as F

    keyed, rejected = split_null_keys(bronze.transform(clean_customers), (KEY,))
    latest = deduplicate_latest(keyed, (KEY,), ("_ingested_at",))
    staged = (with_row_hash(latest)
              .withColumn("valid_from", F.lit(batch_date))
              .withColumn("valid_to", F.lit(None).cast("date"))
              .withColumn("is_current", F.lit(True)))
    return staged, rejected


def apply_scd2(spark: "SparkSession", snapshot: "DataFrame", path: str,
               batch_date: date) -> MergeStats:
    """MERGE a prepared snapshot into the SCD2 table at `path`.

    In the returned stats, `updated` is the number of versions closed and
    `inserted` the number of new versions (new customers + changed ones).
    """
    from pyspark.sql import functions as F

    table = create_table_if_not_exists(spark, path, snapshot.schema)
    current = table.toDF().where(F.col("is_current"))
    check_batch_order(batch_date, current.agg(F.max("valid_from")).first()[0])

    # Customers whose attributes differ from their current version.
    changed = (snapshot.alias("s")
               .join(current.alias("t"), F.col(f"s.{KEY}") == F.col(f"t.{KEY}"))
               .where(F.col("s._row_hash") != F.col("t._row_hash"))
               .select("s.*"))
    version_before = current_version(table)
    staged = (snapshot.withColumn(MERGE_KEY, F.col(KEY))
              .unionByName(changed.withColumn(MERGE_KEY, F.lit(None).cast("string"))))

    (table.alias("t")
          .merge(staged.alias("s"), f"t.`{KEY}` = s.`{MERGE_KEY}` AND t.is_current = true")
          # Close the old version; valid_to is exclusive (= new valid_from).
          .whenMatchedUpdate(condition="t._row_hash <> s._row_hash",
                             set={"is_current": "false", "valid_to": "s.valid_from"})
          .whenNotMatchedInsert(values={c: f"s.`{c}`" for c in snapshot.columns})
          .execute())
    return last_operation_stats(table, version_before)


def load_silver_customers(spark: "SparkSession", config: PipelineConfig,
                          batch_date: date) -> MergeStats:
    """Apply one day's customer snapshot from bronze to silver."""
    from pyspark.sql import functions as F

    bronze = (read_table(spark, config.table_path("bronze", TABLE))
              .where(F.col("_batch_date") == F.lit(batch_date)))
    snapshot, rejected = prepare_snapshot(bronze, batch_date)

    rejected_count = rejected.count()
    if rejected_count:
        logger.warning("silver.customers: %d rows for %s have no customer_id and were skipped",
                       rejected_count, batch_date)

    stats = apply_scd2(spark, snapshot, config.table_path("silver", TABLE), batch_date)
    logger.info("silver.customers %s: %d new versions (%d of them replace a closed version)",
                batch_date, stats.inserted, stats.updated)
    return stats


def main(argv: list[str] | None = None) -> None:
    from lakehouse.utils.spark import get_spark

    parser = argparse.ArgumentParser(description="Apply a customers snapshot as SCD Type 2.")
    parser.add_argument("--batch-date", type=date.fromisoformat, required=True)
    args = parser.parse_args(argv)

    config = load_config()
    spark = get_spark("silver-customers", config)
    load_silver_customers(spark, config, args.batch_date)
    spark.stop()


if __name__ == "__main__":
    main()
