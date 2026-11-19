"""Bronze ingestion: landing-zone files -> Delta tables, plus load metadata.

Each run loads one `batch_date`. Four metadata columns are added to every row
so any bronze record can be traced back to where and when it came from:

    _source_file   full path of the file the row was read from
    _ingested_at   when this run loaded it (UTC)
    _batch_date    the business date of the batch (also the partition column)
    _run_id        id shared by every table loaded in the same run

Writes replace only the partition for that batch date (Delta `replaceWhere`),
so re-running a day gives the same result instead of duplicate rows.

Usage (generate data first, see README):
    python -m lakehouse.bronze.ingest --batch-date 2026-11-01
    python -m lakehouse.bronze.ingest --batch-date 2026-11-01 --tables orders order_items
"""

from __future__ import annotations

import argparse
import uuid
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

from lakehouse.bronze.schemas import SOURCE_COLUMNS, spark_schema
from lakehouse.utils.config import PipelineConfig, load_config
from lakehouse.utils.logger import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = get_logger(__name__)

METADATA_COLUMNS = ("_source_file", "_ingested_at", "_batch_date", "_run_id")


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
    and stops a single odd file from changing a column's type.
    """
    reader = spark.read.schema(spark_schema(source.name)).option("mode", "PERMISSIVE")
    if source.file_format == "csv":
        # enforceSchema=false makes Spark compare the CSV header with the
        # schema and fail loudly if the source renamed or reordered columns,
        # instead of silently loading values into the wrong columns.
        return reader.option("header", "true").option("enforceSchema", "false").csv(path)
    if source.file_format == "json":
        # Corrupt lines become all-NULL rows for now; step 07 captures the raw
        # text and moves them to quarantine.
        return reader.json(path)
    raise ValueError(f"Unsupported format '{source.file_format}' for {source.name}")


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


def write_bronze(df: "DataFrame", table_path: str, batch_date: date) -> None:
    """Overwrite only this batch date's partition of a bronze Delta table."""
    (
        df.write.format("delta")
        .mode("overwrite")
        # Without replaceWhere, mode("overwrite") would wipe every other day.
        .option("replaceWhere", f"_batch_date = '{batch_date.isoformat()}'")
        .partitionBy("_batch_date")
        .save(table_path)
    )


def ingest(spark: "SparkSession", config: PipelineConfig, name: str,
           batch_date: date, run_id: str) -> int:
    """Load one source for one batch date into bronze. Returns the row count."""
    path = source_path(config, name, batch_date)
    if not Path(path).exists():
        raise FileNotFoundError(f"No landing data for {name} at {path}. Run the data generator first.")

    df = add_load_metadata(read_source(spark, SOURCES[name], path), batch_date, run_id)
    target = config.table_path("bronze", name)
    write_bronze(df, target, batch_date)

    # Count from the written partition, not the source DataFrame, so the log
    # reports what actually landed in the table.
    rows = (spark.read.format("delta").load(target)
            .where(f"_batch_date = '{batch_date.isoformat()}'").count())
    logger.info("bronze.%s: %d rows for %s (run %s)", name, rows, batch_date, run_id)
    return rows


def main(argv: list[str] | None = None) -> None:
    from lakehouse.utils.spark import get_spark

    parser = argparse.ArgumentParser(description="Load landing files into bronze Delta tables.")
    parser.add_argument("--batch-date", type=date.fromisoformat, required=True)
    parser.add_argument("--tables", nargs="+", choices=sorted(SOURCES), default=list(SOURCE_COLUMNS))
    args = parser.parse_args(argv)

    config = load_config()
    spark = get_spark("bronze-ingest", config)
    run_id = uuid.uuid4().hex
    for name in args.tables:
        ingest(spark, config, name, args.batch_date, run_id)
    spark.stop()


if __name__ == "__main__":
    main()
