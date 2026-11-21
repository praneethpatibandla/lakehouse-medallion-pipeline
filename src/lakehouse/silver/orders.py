"""Silver orders: clean, deduplicate and MERGE one bronze batch.

Flow for one batch date:

    bronze.orders (that day's partition)
        -> clean_orders           typed columns + _dq_issues (step 08)
        -> drop rows with no order_id (logged; they cannot be merged)
        -> deduplicate_latest     one row per order_id, newest updated_at wins
        -> MERGE into silver.orders
               new order_id            -> insert
               known order_id, newer   -> update
               known order_id, older   -> ignored (out-of-order data)

The source re-sends an order when its status changes, so the same order_id
can appear several times in one day and again on later days. Silver keeps
exactly one current row per order. Rows with data quality issues are kept and
flagged in `_dq_issues`; gold decides what to exclude.

Usage (after loading bronze for the same day):
    python -m lakehouse.silver.orders --batch-date 2026-11-01
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import TYPE_CHECKING

from lakehouse.silver.merge import MergeStats, deduplicate_latest, split_null_keys, upsert
from lakehouse.silver.transforms import clean_orders
from lakehouse.utils.config import PipelineConfig, load_config
from lakehouse.utils.logger import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = get_logger(__name__)

TABLE = "orders"
KEYS = ("order_id",)
# The source stamps every change in updated_at. `_ingested_at` breaks ties
# between copies with the same updated_at (the later load wins).
SEQUENCE_COLUMN = "updated_at"
ORDER_COLUMNS = (SEQUENCE_COLUMN, "_ingested_at")


def prepare_orders(bronze: "DataFrame") -> tuple["DataFrame", "DataFrame"]:
    """Return (rows ready to merge, rows rejected for a missing key)."""
    from pyspark.sql import functions as F

    cleaned = bronze.transform(clean_orders)
    keyed, rejected = split_null_keys(cleaned, KEYS)
    latest = deduplicate_latest(keyed, KEYS, ORDER_COLUMNS)
    # When silver last wrote the row: handy when debugging "why did this change?"
    return latest.withColumn("_silver_updated_at", F.current_timestamp()), rejected


def read_bronze_batch(spark: "SparkSession", config: PipelineConfig, batch_date: date) -> "DataFrame":
    """Read only one day's partition, so each run processes just the new data."""
    from pyspark.sql import functions as F

    return (spark.read.format("delta").load(config.table_path("bronze", TABLE))
            .where(F.col("_batch_date") == F.lit(batch_date)))


def load_silver_orders(spark: "SparkSession", config: PipelineConfig, batch_date: date) -> MergeStats:
    """Merge one bronze batch of orders into silver. Returns the MERGE row counts."""
    bronze = read_bronze_batch(spark, config, batch_date)
    staged, rejected = prepare_orders(bronze)

    rejected_count = rejected.count()
    if rejected_count:
        logger.warning("silver.orders: %d rows for %s have no order_id and were skipped",
                       rejected_count, batch_date)

    stats = upsert(spark, staged, config.table_path("silver", TABLE), KEYS, SEQUENCE_COLUMN)
    logger.info("silver.orders %s: %d unique orders in batch, %d inserted, %d updated",
                batch_date, stats.source_rows, stats.inserted, stats.updated)
    return stats


def main(argv: list[str] | None = None) -> None:
    from lakehouse.utils.spark import get_spark

    parser = argparse.ArgumentParser(description="Merge one bronze batch of orders into silver.")
    parser.add_argument("--batch-date", type=date.fromisoformat, required=True)
    args = parser.parse_args(argv)

    config = load_config()
    spark = get_spark("silver-orders", config)
    load_silver_orders(spark, config, args.batch_date)
    spark.stop()


if __name__ == "__main__":
    main()
