"""Silver products: the current product catalogue (Type 1).

Products arrive as a full daily snapshot, like customers. Unlike customers,
products are kept as Type 1: a changed product simply overwrites its row.

Why no history here? The attribute that matters most for reporting, the
price, is already copied onto every order line at order time (see
`data_generator/orders.py`). Sales are always valued at the price the
customer paid, so a later catalogue price change cannot rewrite old revenue.
The trade-off: if a product moves to another category, past sales move with
it. If the business needed "sales by category as it was then", this table
would become SCD2 like customers.

Flow for one batch date:

    bronze.products (that day's snapshot)
        -> clean_products         typed columns + _dq_issues (step 08)
        -> drop rows with no product_id (logged)
        -> deduplicate_latest     one row per product_id
        -> MERGE into silver.products (insert new, overwrite known)

The snapshot has no "updated_at" column, so the batch date is the sequence
column: replaying an older snapshot late cannot overwrite a newer one.

Usage (after loading bronze products for the same day):
    python -m lakehouse.silver.products --batch-date 2026-11-01
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import TYPE_CHECKING

from lakehouse.silver.merge import MergeStats, deduplicate_latest, split_null_keys, upsert
from lakehouse.silver.transforms import clean_products
from lakehouse.utils.config import PipelineConfig, load_config
from lakehouse.utils.logger import get_logger
from lakehouse.utils.tables import read_table

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = get_logger(__name__)

TABLE = "products"
KEYS = ("product_id",)
# No change timestamp in the source: the newer snapshot wins.
SEQUENCE_COLUMN = "_batch_date"
ORDER_COLUMNS = (SEQUENCE_COLUMN, "_ingested_at")


def prepare_products(bronze: "DataFrame") -> tuple["DataFrame", "DataFrame"]:
    """Return (one cleaned row per product, rows rejected for a missing key)."""
    from pyspark.sql import functions as F

    keyed, rejected = split_null_keys(bronze.transform(clean_products), KEYS)
    latest = deduplicate_latest(keyed, KEYS, ORDER_COLUMNS)
    return latest.withColumn("_silver_updated_at", F.current_timestamp()), rejected


def load_silver_products(spark: "SparkSession", config: PipelineConfig,
                         batch_date: date) -> MergeStats:
    """Merge one day's product snapshot from bronze into silver."""
    from pyspark.sql import functions as F

    bronze = (read_table(spark, config.table_path("bronze", TABLE))
              .where(F.col("_batch_date") == F.lit(batch_date)))
    staged, rejected = prepare_products(bronze)

    rejected_count = rejected.count()
    if rejected_count:
        logger.warning("silver.products: %d rows for %s have no product_id and were skipped",
                       rejected_count, batch_date)

    stats = upsert(spark, staged, config.table_path("silver", TABLE), KEYS, SEQUENCE_COLUMN)
    logger.info("silver.products %s: %d inserted, %d updated",
                batch_date, stats.inserted, stats.updated)
    return stats


def main(argv: list[str] | None = None) -> None:
    from lakehouse.utils.spark import get_spark

    parser = argparse.ArgumentParser(description="Merge one products snapshot into silver.")
    parser.add_argument("--batch-date", type=date.fromisoformat, required=True)
    args = parser.parse_args(argv)

    config = load_config()
    spark = get_spark("silver-products", config)
    load_silver_products(spark, config, args.batch_date)
    spark.stop()


if __name__ == "__main__":
    main()
