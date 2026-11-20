"""Bronze ingestion: landing-zone files -> Delta tables, plus load metadata.

Each run loads one `batch_date`. Four metadata columns are added to every row
so any bronze record can be traced back to where and when it came from:

    _source_file   full path of the file the row was read from
    _ingested_at   when this run loaded it (UTC)
    _batch_date    the business date of the batch (also the partition column)
    _run_id        id shared by every table loaded in the same run

Lines Spark cannot parse are not loaded into bronze. They go, with their raw
text, to a quarantine table (`<data_root>/quarantine/<source>`). If too large
a share of a batch is corrupt the load stops before bronze is touched. Every
load, good or failed, appends a row to the ingestion audit log (see audit.py).

Writes replace only the partition for that batch date (Delta `replaceWhere`),
so re-running a day gives the same result instead of duplicate rows. This is
true for the quarantine table as well.

Usage (generate data first, see README):
    python -m lakehouse.bronze.ingest --batch-date 2026-11-01
    python -m lakehouse.bronze.ingest --batch-date 2026-11-01 --tables orders order_items
    python -m lakehouse.bronze.ingest --batch-date 2026-11-01 --max-quarantine-ratio 0.1
"""

from __future__ import annotations

import argparse
import uuid
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

from lakehouse.bronze.audit import (
    DEFAULT_MAX_QUARANTINE_RATIO,
    AuditRecord,
    check_quarantine_threshold,
    write_audit_record,
)
from lakehouse.bronze.schemas import CORRUPT_RECORD_COLUMN, SOURCE_COLUMNS, spark_schema
from lakehouse.utils.config import PipelineConfig, load_config
from lakehouse.utils.logger import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = get_logger(__name__)

METADATA_COLUMNS = ("_source_file", "_ingested_at", "_batch_date", "_run_id")
AUDIT_TABLE = "ingestion_log"


@dataclass(frozen=True)
class Source:
    """How one source arrives in the landing zone."""

    name: str
    file_format: str  # "csv" or "json"
    # True: a new folder per day (landing/<name>/batch_date=YYYY-MM-DD/).
    # False: one full snapshot file that the source overwrites (reference data).
    daily: bool


SOURCES: dict[str, Source] = {
    "customers": Source("customers", "csv", daily=False),
    "products": Source("products", "csv", daily=False),
    "orders": Source("orders", "json", daily=True),
    "order_items": Source("order_items", "csv", daily=True),
}


def source_path(config: PipelineConfig, name: str, batch_date: date) -> str:
    """Return the landing folder to read for a source and batch date."""
    if name not in SOURCES:
        raise ValueError(f"Unknown source '{name}'. Expected one of: {sorted(SOURCES)}")
    folder = config.landing_path / name
    if SOURCES[name].daily:
        folder = folder / f"batch_date={batch_date.isoformat()}"
    return str(folder)


def read_source(spark: "SparkSession", source: Source, path: str) -> "DataFrame":
    """Read landing files with the explicit all-string schema.

    An explicit schema is faster than inference (no extra pass over the data)
    and stops a single odd file from changing a column's type. In PERMISSIVE
    mode a line that cannot be parsed (truncated JSON, a CSV row with the
    wrong number of fields) is kept: its raw text goes into `_corrupt_record`
    and the source columns are NULL. `split_corrupt` then separates them.
    """
    reader = (
        spark.read.schema(spark_schema(source.name, with_corrupt_column=True))
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COLUMN)
    )
    if source.file_format == "csv":
        # enforceSchema=false makes Spark compare the CSV header with the
        # schema and fail loudly if the source renamed or reordered columns,
        # instead of silently loading values into the wrong columns. The
        # corrupt-record column is left out of that header check by Spark.
        return reader.option("header", "true").option("enforceSchema", "false").csv(path)
    if source.file_format == "json":
        return reader.json(path)
    raise ValueError(f"Unsupported format '{source.file_format}' for {source.name}")


def split_corrupt(df: "DataFrame") -> tuple["DataFrame", "DataFrame"]:
    """Split a parsed batch into (clean rows, corrupt rows).

    Clean rows lose the `_corrupt_record` column, so the bronze schema is the
    same as before quarantine existed. Corrupt rows keep every column: Spark
    refuses queries on raw files that select *only* the corrupt-record column,
    and the NULL source columns cost nothing to store.
    """
    from pyspark.sql import functions as F

    is_corrupt = F.col(CORRUPT_RECORD_COLUMN).isNotNull()
    clean = df.where(~is_corrupt).drop(CORRUPT_RECORD_COLUMN)
    corrupt = df.where(is_corrupt).withColumn("_quarantine_reason", F.lit("unparseable"))
    return clean, corrupt


def add_load_metadata(df: "DataFrame", batch_date: date, run_id: str) -> "DataFrame":
    """Append the bronze metadata columns."""
    from pyspark.sql import functions as F

    return (
        df
        # `_metadata` is the hidden file-metadata column of file sources. It
        # replaces input_file_name(), which Unity Catalog does not support.
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_batch_date", F.lit(batch_date))
        .withColumn("_run_id", F.lit(run_id))
    )


def write_partition(df: "DataFrame", table_path: str, batch_date: date) -> None:
    """Overwrite only this batch date's partition of a Delta table.

    Used for bronze and quarantine tables. An empty DataFrame still runs the
    replace, which clears rows left by an earlier run of the same day.
    """
    (
        df.write.format("delta")
        .mode("overwrite")
        # Without replaceWhere, mode("overwrite") would wipe every other day.
        .option("replaceWhere", f"_batch_date = '{batch_date.isoformat()}'")
        .partitionBy("_batch_date")
        .save(table_path)
    )


def _partition_count(spark: "SparkSession", table_path: str, batch_date: date) -> int:
    """Count one batch date's rows in a Delta table."""
    return (spark.read.format("delta").load(table_path)
            .where(f"_batch_date = '{batch_date.isoformat()}'").count())


def ingest(spark: "SparkSession", config: PipelineConfig, name: str,
           batch_date: date, run_id: str,
           max_quarantine_ratio: float = DEFAULT_MAX_QUARANTINE_RATIO) -> int:
    """Load one source for one batch date into bronze. Returns the rows loaded.

    Order of operations: quarantine first, check the threshold, then publish
    to bronze. A batch that fails the check never reaches bronze, and the
    audit log records why.
    """
    path = source_path(config, name, batch_date)
    audit = AuditRecord(run_id=run_id, source=name, batch_date=batch_date, source_path=path)
    audit_table = config.table_path("audit", AUDIT_TABLE)
    try:
        if not Path(path).exists():
            raise FileNotFoundError(f"No landing data for {name} at {path}. Run the data generator first.")

        df = add_load_metadata(read_source(spark, SOURCES[name], path), batch_date, run_id)
        clean, corrupt = split_corrupt(df)
        rows_read = df.count()

        quarantine_target = config.table_path("quarantine", name)
        write_partition(corrupt, quarantine_target, batch_date)
        rows_quarantined = _partition_count(spark, quarantine_target, batch_date)
        check_quarantine_threshold(name, rows_read, rows_quarantined, max_quarantine_ratio)

        target = config.table_path("bronze", name)
        write_partition(clean, target, batch_date)
        # Count from the written partition, not the source DataFrame, so the
        # log reports what actually landed in the table.
        rows_loaded = _partition_count(spark, target, batch_date)
        if rows_loaded + rows_quarantined != rows_read:
            logger.warning("bronze.%s: %d read but %d loaded + %d quarantined",
                           name, rows_read, rows_loaded, rows_quarantined)
    except Exception as exc:
        audit.fail(exc)
        write_audit_record(spark, audit, audit_table)
        logger.error("bronze.%s failed for %s (run %s): %s", name, batch_date, run_id, exc)
        raise

    audit.succeed(rows_read, rows_loaded, rows_quarantined)
    write_audit_record(spark, audit, audit_table)
    logger.info("bronze.%s: %d rows loaded, %d quarantined for %s (run %s)",
                name, rows_loaded, rows_quarantined, batch_date, run_id)
    return rows_loaded


def main(argv: list[str] | None = None) -> None:
    from lakehouse.utils.spark import get_spark

    parser = argparse.ArgumentParser(description="Load landing files into bronze Delta tables.")
    parser.add_argument("--batch-date", type=date.fromisoformat, required=True)
    parser.add_argument("--tables", nargs="+", choices=sorted(SOURCES), default=list(SOURCE_COLUMNS))
    parser.add_argument("--max-quarantine-ratio", type=float, default=DEFAULT_MAX_QUARANTINE_RATIO,
                        help="stop a load if more than this share of rows is corrupt (default: 0.05)")
    args = parser.parse_args(argv)

    config = load_config()
    spark = get_spark("bronze-ingest", config)
    run_id = uuid.uuid4().hex
    for name in args.tables:
        ingest(spark, config, name, args.batch_date, run_id, args.max_quarantine_ratio)
    spark.stop()


if __name__ == "__main__":
    main()
