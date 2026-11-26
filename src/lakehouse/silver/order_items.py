"""Silver order items: cleaned, priced and conformed to orders and products.

"Conformed" means the table speaks the same language as the rest of silver:
the same key formats and types as silver.orders and silver.products, money as
DECIMAL, and every line checked against the tables it refers to. Gold can
then join items to orders and products without re-cleaning anything.

Flow for one batch date:

    bronze.order_items (that day's partition)
        -> clean_order_items         typed columns + _dq_issues (step 08)
        -> drop rows missing order_id or line_number (logged)
        -> deduplicate_latest        one row per (order_id, line_number)
        -> add_line_amounts          gross, discount and net amount per line
        -> conform                   flag unknown_product / orphan_order
        -> MERGE into silver.order_items on (order_id, line_number)

Run silver.orders and silver.products for the day first: the conformance
checks read them.

Usage:
    python -m lakehouse.silver.order_items --batch-date 2026-11-01
"""

from __future__ import annotations

import argparse
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from typing import TYPE_CHECKING

from lakehouse.silver.merge import MergeStats, deduplicate_latest, split_null_keys, upsert
from lakehouse.silver.transforms import append_dq_issues, clean_order_items
from lakehouse.utils.config import PipelineConfig, load_config
from lakehouse.utils.logger import get_logger
from lakehouse.utils.tables import read_table, table_exists

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = get_logger(__name__)

TABLE = "order_items"
# A line is identified by its order and its position in the order.
KEYS = ("order_id", "line_number")
# Items have no change timestamp; a later batch wins over an earlier one.
SEQUENCE_COLUMN = "_batch_date"
ORDER_COLUMNS = (SEQUENCE_COLUMN, "_ingested_at")

AMOUNT_TYPE = "decimal(12,2)"
AMOUNT_COLUMNS = ("line_gross_amount", "line_discount_amount", "line_net_amount")
CENT = Decimal("0.01")


class MissingUpstreamTableError(RuntimeError):
    """Raised when a table the conformance checks need has not been built yet."""


# --------------------------------------------------------------------------
# Pure Python: the pricing rule, written once so it can be tested and
# compared against the Spark version.
# --------------------------------------------------------------------------

def line_amounts(quantity: int | None, unit_price: Decimal | None,
                 discount_pct: Decimal | None) -> tuple[Decimal | None, Decimal | None,
                                                         Decimal | None]:
    """Return (gross, discount, net) for one order line.

        gross    = quantity * unit_price                 (exact, already in cents)
        discount = gross * discount_pct / 100, rounded half-up to the cent
        net      = gross - discount

    The discount is rounded once per line, and net is gross minus the rounded
    discount, so gross = discount + net always holds to the cent. Any missing
    input gives NULL for the amounts that depend on it, the same way Spark
    propagates NULL.
    """
    gross = None
    if quantity is not None and unit_price is not None:
        gross = Decimal(quantity) * unit_price
    discount = None
    if gross is not None and discount_pct is not None:
        discount = (gross * discount_pct / Decimal(100)).quantize(CENT, rounding=ROUND_HALF_UP)
    net = gross - discount if gross is not None and discount is not None else None
    return gross, discount, net


# --------------------------------------------------------------------------
# Spark
# --------------------------------------------------------------------------

def add_line_amounts(df: "DataFrame") -> "DataFrame":
    """Add the three amount columns, following `line_amounts` exactly.

    Everything stays DECIMAL: Spark's `round` on a decimal rounds half-up,
    like `ROUND_HALF_UP` above. Lines with a bad quantity still get amounts
    (possibly negative); they are flagged in `_dq_issues` and gold leaves
    them out, so nothing is hidden here.
    """
    from pyspark.sql import functions as F

    return (df
            .withColumn("line_gross_amount",
                        (F.col("quantity").cast("decimal(10,0)") * F.col("unit_price"))
                        .cast(AMOUNT_TYPE))
            .withColumn("line_discount_amount",
                        F.expr("round(line_gross_amount * discount_pct / 100, 2)")
                        .cast(AMOUNT_TYPE))
            .withColumn("line_net_amount",
                        (F.col("line_gross_amount") - F.col("line_discount_amount"))
                        .cast(AMOUNT_TYPE)))


def flag_missing_reference(df: "DataFrame", reference: "DataFrame", key: str,
                           rule: str) -> "DataFrame":
    """Append `rule` to `_dq_issues` when `df[key]` has no match in `reference`.

    This is a referential integrity check, like a foreign key in a database
    (Delta does not enforce foreign keys). The reference side is only the
    distinct keys, broadcast to every executor: products and orders are much
    smaller than order lines, so no shuffle of the big side is needed. A NULL
    key never matches, so it is flagged too.
    """
    from pyspark.sql import functions as F

    ref_keys = reference.select(F.col(key).alias("_ref_key")).distinct()
    joined = df.join(F.broadcast(ref_keys), F.col(key) == F.col("_ref_key"), "left")
    return append_dq_issues(joined, {rule: F.col("_ref_key").isNull()}).drop("_ref_key")


def conform(items: "DataFrame", products: "DataFrame", orders: "DataFrame") -> "DataFrame":
    """Flag lines whose product or order is not in silver."""
    out = flag_missing_reference(items, products, "product_id", "unknown_product")
    return flag_missing_reference(out, orders, "order_id", "orphan_order")


def prepare_order_items(bronze: "DataFrame", products: "DataFrame",
                        orders: "DataFrame") -> tuple["DataFrame", "DataFrame"]:
    """Return (rows ready to merge, rows rejected for a missing key)."""
    from pyspark.sql import functions as F

    keyed, rejected = split_null_keys(bronze.transform(clean_order_items), KEYS)
    latest = deduplicate_latest(keyed, KEYS, ORDER_COLUMNS)
    staged = conform(add_line_amounts(latest), products, orders)
    return staged.withColumn("_silver_updated_at", F.current_timestamp()), rejected


def read_silver_table(spark: "SparkSession", config: PipelineConfig, table: str) -> "DataFrame":
    """Read an upstream silver table, with a clear error if it is missing."""
    path = config.table_path("silver", table)
    if not table_exists(spark, path):
        raise MissingUpstreamTableError(
            f"silver.{table} not found at {path}. Run `python -m lakehouse.silver.{table}` "
            "for this batch date before silver.order_items.")
    return read_table(spark, path)


def load_silver_order_items(spark: "SparkSession", config: PipelineConfig,
                            batch_date: date) -> MergeStats:
    """Merge one bronze batch of order items into silver."""
    from pyspark.sql import functions as F

    products = read_silver_table(spark, config, "products")
    orders = read_silver_table(spark, config, "orders")
    bronze = (read_table(spark, config.table_path("bronze", TABLE))
              .where(F.col("_batch_date") == F.lit(batch_date)))
    staged, rejected = prepare_order_items(bronze, products, orders)

    rejected_count = rejected.count()
    if rejected_count:
        logger.warning("silver.order_items: %d rows for %s have no order_id or line_number "
                       "and were skipped", rejected_count, batch_date)

    stats = upsert(spark, staged, config.table_path("silver", TABLE), KEYS, SEQUENCE_COLUMN)
    logger.info("silver.order_items %s: %d inserted, %d updated",
                batch_date, stats.inserted, stats.updated)
    return stats


def main(argv: list[str] | None = None) -> None:
    from lakehouse.utils.spark import get_spark

    parser = argparse.ArgumentParser(description="Merge one bronze batch of order items into silver.")
    parser.add_argument("--batch-date", type=date.fromisoformat, required=True)
    args = parser.parse_args(argv)

    config = load_config()
    spark = get_spark("silver-order-items", config)
    load_silver_order_items(spark, config, args.batch_date)
    spark.stop()


if __name__ == "__main__":
    main()
