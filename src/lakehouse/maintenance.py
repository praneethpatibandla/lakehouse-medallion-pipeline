"""Table maintenance: OPTIMIZE (with Z-ORDER) and VACUUM for every Delta table.

Why a Delta table needs maintenance:

* Small files. Every daily batch, MERGE and audit append writes new files,
  often only a few KB each. Reading a table means opening every file, so
  thousands of tiny files make queries slow even when the data is small.
  OPTIMIZE rewrites them into a few large files (bin-packing).
* Data skipping. Delta keeps min/max statistics per file. Z-ORDER sorts the
  rows by the given columns while compacting, so each file covers a narrow
  range of those columns and a filter such as `order_id = 'O-123'` can skip
  most files without opening them. It is most useful on the columns used in
  filters and MERGE joins.
* Old files. Overwrites, MERGEs and OPTIMIZE never delete data files; they
  only mark them as removed in the transaction log, which is what makes time
  travel possible. VACUUM physically deletes removed files older than the
  retention period, so storage does not grow forever.

The job is meant to run after the daily pipeline (for example once a day, or
once a week for VACUUM). Tables that do not exist yet are skipped.

Usage:
    python -m lakehouse.maintenance                      # every table
    python -m lakehouse.maintenance --tables silver.orders gold.fact_sales
    python -m lakehouse.maintenance --vacuum-dry-run     # list files VACUUM would delete
    python -m lakehouse.maintenance --skip-vacuum

The SQL builders and validation rules are plain Python and unit tested
without Spark.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable, Sequence

from lakehouse.utils.config import PipelineConfig, load_config
from lakehouse.utils.logger import get_logger
from lakehouse.utils.tables import delta_table, sql_ref, table_exists

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = get_logger(__name__)

# Delta's own default (7 days). Removed files younger than this may still be
# read by a running query or needed for time travel, so VACUUM refuses a
# shorter retention unless a safety check is turned off. We do not allow it.
MIN_RETAIN_HOURS = 168


@dataclass(frozen=True)
class TableMaintenance:
    """How one table is maintained."""

    layer: str
    table: str
    # Columns to cluster on while compacting. Empty = compaction only, which
    # is right for small tables (dimensions, aggregates) that are always read
    # in full anyway.
    zorder_by: tuple[str, ...] = ()

    @property
    def name(self) -> str:
        return f"{self.layer}.{self.table}"


# Z-ORDER choices follow how each table is queried:
#   silver tables are MERGE targets, joined on their business key every run;
#   fact_sales is filtered by date and sliced by product in most reports.
# Partition columns are never Z-ORDER columns: within one partition they
# have a single value, so there is nothing to sort.
TABLES: tuple[TableMaintenance, ...] = (
    TableMaintenance("bronze", "customers"),
    TableMaintenance("bronze", "products"),
    TableMaintenance("bronze", "orders"),
    TableMaintenance("bronze", "order_items"),
    TableMaintenance("audit", "ingestion_log"),
    TableMaintenance("silver", "customers", ("customer_id",)),
    TableMaintenance("silver", "products"),
    TableMaintenance("silver", "orders", ("order_id",)),
    TableMaintenance("silver", "order_items", ("order_id",)),
    TableMaintenance("gold", "dim_date"),
    TableMaintenance("gold", "dim_customer"),
    TableMaintenance("gold", "dim_product"),
    TableMaintenance("gold", "fact_sales", ("date_key", "product_key")),
    TableMaintenance("gold", "agg_daily_sales"),
    TableMaintenance("gold", "agg_customer_lifetime_value"),
)


# --------------------------------------------------------------------------
# Pure Python: validation, SQL and metrics
# --------------------------------------------------------------------------

def select_tables(names: Iterable[str] | None,
                  tables: Sequence[TableMaintenance] = TABLES) -> list[TableMaintenance]:
    """Return the specs for `names` ("layer.table"), or all of them for None."""
    if not names:
        return list(tables)
    by_name = {t.name: t for t in tables}
    unknown = sorted(set(names) - set(by_name))
    if unknown:
        raise ValueError(f"Unknown tables {unknown}. Expected some of: {sorted(by_name)}")
    return [by_name[n] for n in names]


def validate_retention(retain_hours: int) -> int:
    """Refuse a VACUUM retention shorter than Delta's 7-day safety margin."""
    if retain_hours < MIN_RETAIN_HOURS:
        raise ValueError(
            f"VACUUM retention must be at least {MIN_RETAIN_HOURS} hours, got {retain_hours}. "
            "A shorter retention can delete files that running queries or time travel "
            "still need.")
    return retain_hours


def check_zorder_columns(zorder_by: Sequence[str], columns: Iterable[str],
                         partition_columns: Iterable[str]) -> None:
    """Fail early with a clear message instead of a long Spark stack trace."""
    columns, partition_columns = set(columns), set(partition_columns)
    missing = [c for c in zorder_by if c not in columns]
    if missing:
        raise ValueError(f"Z-ORDER columns {missing} are not in the table")
    partitioned = [c for c in zorder_by if c in partition_columns]
    if partitioned:
        raise ValueError(f"Z-ORDER columns {partitioned} are partition columns")


def optimize_sql(path: str, zorder_by: Sequence[str] = ()) -> str:
    """Return the OPTIMIZE statement, with ZORDER BY when columns are given."""
    sql = f"OPTIMIZE {sql_ref(path)}"
    if zorder_by:
        sql += " ZORDER BY (" + ", ".join(f"`{c}`" for c in zorder_by) + ")"
    return sql


def vacuum_sql(path: str, retain_hours: int, dry_run: bool = False) -> str:
    """Return the VACUUM statement. DRY RUN lists files without deleting them."""
    sql = f"VACUUM {sql_ref(path)} RETAIN {validate_retention(retain_hours)} HOURS"
    return sql + " DRY RUN" if dry_run else sql


@dataclass(frozen=True)
class OptimizeStats:
    """What one OPTIMIZE did, from the Delta transaction log."""

    files_removed: int
    files_added: int
    bytes_removed: int = 0
    bytes_added: int = 0


def parse_optimize_metrics(metrics: dict[str, str] | None) -> OptimizeStats:
    """Turn Delta's OPTIMIZE `operationMetrics` (string values) into numbers."""
    metrics = metrics or {}

    def number(name: str) -> int:
        return int(metrics.get(name) or 0)

    return OptimizeStats(
        files_removed=number("numRemovedFiles"),
        files_added=number("numAddedFiles"),
        bytes_removed=number("numRemovedBytes"),
        bytes_added=number("numAddedBytes"),
    )


# --------------------------------------------------------------------------
# Spark
# --------------------------------------------------------------------------

def optimize_table(spark: "SparkSession", path: str,
                   zorder_by: Sequence[str] = ()) -> OptimizeStats:
    """Compact (and optionally Z-ORDER) one Delta table."""
    table = delta_table(spark, path)
    detail = table.detail().select("partitionColumns").first()
    check_zorder_columns(zorder_by, table.toDF().columns, detail["partitionColumns"])

    version_before = table.history(1).select("version").first()["version"]
    spark.sql(optimize_sql(path, zorder_by))
    # A table that is already compact gets no new commit; report zeros then,
    # not the metrics of whatever operation happened to be last.
    latest = table.history(1).select("version", "operationMetrics").first()
    if latest["version"] == version_before:
        return OptimizeStats(files_removed=0, files_added=0)
    return parse_optimize_metrics(latest["operationMetrics"])


def vacuum_table(spark: "SparkSession", path: str, retain_hours: int,
                 dry_run: bool = False) -> int | None:
    """VACUUM one table. Returns the number of files listed in a dry run."""
    result = spark.sql(vacuum_sql(path, retain_hours, dry_run))
    return result.count() if dry_run else None


def run_maintenance(spark: "SparkSession", config: PipelineConfig,
                    tables: Sequence[TableMaintenance] = TABLES,
                    retain_hours: int = MIN_RETAIN_HOURS, vacuum: bool = True,
                    vacuum_dry_run: bool = False) -> dict[str, OptimizeStats]:
    """Maintain each table in turn and return the OPTIMIZE stats per table.

    One table failing does not stop the others: the error is logged and the
    job fails at the end, so every table still gets its maintenance.
    """
    validate_retention(retain_hours)
    results: dict[str, OptimizeStats] = {}
    failed: list[str] = []
    for spec in tables:
        path = config.table_path(spec.layer, spec.table)
        if not table_exists(spark, path):
            logger.info("%s: no Delta table at %s, skipped", spec.name, path)
            continue
        try:
            stats = optimize_table(spark, path, spec.zorder_by)
            results[spec.name] = stats
            logger.info("%s: OPTIMIZE%s compacted %d files into %d",
                        spec.name, f" ZORDER BY {list(spec.zorder_by)}" if spec.zorder_by else "",
                        stats.files_removed, stats.files_added)
            if vacuum:
                listed = vacuum_table(spark, path, retain_hours, vacuum_dry_run)
                if vacuum_dry_run:
                    logger.info("%s: VACUUM would delete %d files", spec.name, listed)
                else:
                    logger.info("%s: VACUUM done (retain %d hours)", spec.name, retain_hours)
        except Exception:  # keep going; report every failure at the end
            logger.exception("%s: maintenance failed", spec.name)
            failed.append(spec.name)
    if failed:
        raise RuntimeError(f"Maintenance failed for: {failed}")
    return results


def main(argv: list[str] | None = None) -> None:
    from lakehouse.utils.spark import get_spark

    parser = argparse.ArgumentParser(description="OPTIMIZE and VACUUM the Delta tables.")
    parser.add_argument("--tables", nargs="+", metavar="LAYER.TABLE",
                        help="Tables to maintain (default: all)")
    parser.add_argument("--retain-hours", type=int, default=MIN_RETAIN_HOURS,
                        help=f"VACUUM retention in hours (minimum {MIN_RETAIN_HOURS})")
    parser.add_argument("--skip-vacuum", action="store_true", help="Only run OPTIMIZE")
    parser.add_argument("--vacuum-dry-run", action="store_true",
                        help="List the files VACUUM would delete, delete nothing")
    args = parser.parse_args(argv)

    # Validate before starting Spark, so a typo fails in a second, not a minute.
    tables = select_tables(args.tables)
    validate_retention(args.retain_hours)

    config = load_config()
    spark = get_spark("table-maintenance", config)
    try:
        run_maintenance(spark, config, tables, args.retain_hours,
                        vacuum=not args.skip_vacuum, vacuum_dry_run=args.vacuum_dry_run)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
