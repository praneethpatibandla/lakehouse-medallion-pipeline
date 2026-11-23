"""Gold fact table: fact_sales.

Grain: one row per order line (order_id, line_number). The grain is the
first decision for any fact table: it says exactly what one row means, so
every measure on the row (quantity, net amount, ...) can be summed safely.

    silver.order_items  --+
    silver.orders       --+--> keep reportable lines --> look up surrogate keys --> fact_sales
    gold.dim_customer   --+        (exclusion rules)     date / customer / product
    gold.dim_product    --+

Which lines are left out, and why:
    Silver keeps every row and labels problems in `_dq_issues`. Gold is where
    the "is this a sale we can report?" decision is made. A line is excluded
    when its numbers or its date cannot be trusted (bad quantity, unparseable
    price, an order that does not exist, no order timestamp). A line whose
    *descriptive* data is missing (unknown product, missing or unknown
    customer) is kept and pointed at the unknown member (-1): the money is
    real, only the label is missing.

Customer versions:
    dim_customer has one row per customer version (SCD2). Each sale gets the
    version that was valid on the order date, so "sales by state" shows the
    state the customer lived in when they bought, not where they live today.

The table is rebuilt in full with one atomic Delta overwrite, like the
dimensions. Step 15 partitions it and step 17 makes the run incremental.

Usage (after silver and the gold dimensions are built):
    python -m lakehouse.gold.facts
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import TYPE_CHECKING, Iterable, Sequence

from lakehouse.gold.dimensions import UNKNOWN_KEY
from lakehouse.utils.config import PipelineConfig, load_config
from lakehouse.utils.logger import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = get_logger(__name__)

# Data quality labels (from silver) that make a line unusable as a sale.
# Labels not listed here, such as unknown_product, missing_customer_id or
# unknown_status, do not exclude the line.
EXCLUDING_ITEM_ISSUES = (
    "invalid_line_number",
    "invalid_quantity",
    "invalid_unit_price",
    "invalid_discount_pct",
    "non_positive_quantity",
    "discount_out_of_range",
    "orphan_order",
)
EXCLUDING_ORDER_ISSUES = ("invalid_order_ts",)
# Extra reasons gold adds itself, for values that are simply missing.
MISSING_ORDER_DATE = "missing_order_date"
MISSING_AMOUNT = "missing_amount"

FACT_COLUMNS = (
    "order_id", "line_number", "date_key", "customer_key", "product_key",
    "order_ts", "order_status", "payment_method", "currency",
    "quantity", "unit_price", "discount_pct",
    "gross_amount", "discount_amount", "net_amount",
)

# A date before any real order: the first version of each customer is
# stretched back to it (see `pick_customer_version`).
BEGINNING_OF_TIME = date(1900, 1, 1)


# --------------------------------------------------------------------------
# Pure Python: the rules, written once so they can be unit tested and
# compared against the Spark version.
# --------------------------------------------------------------------------

def exclusion_reasons(item_issues: Iterable[str], order_issues: Iterable[str],
                      has_order_date: bool, has_amount: bool) -> list[str]:
    """Return why a line cannot be a sale; an empty list means keep it."""
    reasons = [i for i in item_issues if i in EXCLUDING_ITEM_ISSUES]
    reasons += [i for i in order_issues if i in EXCLUDING_ORDER_ISSUES]
    if not has_order_date:
        reasons.append(MISSING_ORDER_DATE)
    if not has_amount:
        reasons.append(MISSING_AMOUNT)
    return reasons


def pick_customer_version(versions: Sequence[tuple[int, date, date | None]],
                          order_date: date) -> int:
    """Return the customer_key of the version valid on `order_date`.

    `versions` are (customer_key, valid_from, valid_to) for one customer,
    with valid_to exclusive and NULL for the current version. A version
    matches when valid_from <= order_date < valid_to.

    The first version's valid_from is the day the pipeline first saw the
    customer, which can be later than their oldest orders (e.g. a backfill of
    older orders). Those orders get the first version: it is the best
    description we have. With no versions at all the result is the unknown
    member.
    """
    if not versions:
        return UNKNOWN_KEY
    first_from = min(v[1] for v in versions)
    for key, valid_from, valid_to in versions:
        start = BEGINNING_OF_TIME if valid_from == first_from else valid_from
        if start <= order_date and (valid_to is None or order_date < valid_to):
            return key
    return UNKNOWN_KEY


# --------------------------------------------------------------------------
# Spark
# --------------------------------------------------------------------------

def _string_array(values: Sequence[str]):
    from pyspark.sql import functions as F

    return F.array(*[F.lit(v) for v in values])


def add_exclusion_reasons(lines: "DataFrame") -> "DataFrame":
    """Add `_exclusion_reasons`, following `exclusion_reasons` exactly.

    Expects the joined line + order columns `_item_issues`, `_order_issues`,
    `order_ts` and `line_net_amount`.
    """
    from pyspark.sql import functions as F

    def keep_listed(column: str, allowed: Sequence[str]):
        return F.filter(F.coalesce(F.col(column), F.array().cast("array<string>")),
                        lambda x: F.array_contains(_string_array(allowed), x))

    missing = F.array(
        F.when(F.col("order_ts").isNull(), F.lit(MISSING_ORDER_DATE)),
        F.when(F.col("line_net_amount").isNull(), F.lit(MISSING_AMOUNT)),
    )
    return lines.withColumn("_exclusion_reasons", F.concat(
        keep_listed("_item_issues", EXCLUDING_ITEM_ISSUES),
        keep_listed("_order_issues", EXCLUDING_ORDER_ISSUES),
        F.filter(missing, lambda x: x.isNotNull()),
    ))


def join_lines_to_orders(items: "DataFrame", orders: "DataFrame") -> "DataFrame":
    """Attach order-level columns to each line.

    A left join: a line whose order is missing (orphan_order) keeps its row
    here and is excluded by the rules, so it can still be counted.
    """
    from pyspark.sql import functions as F

    order_cols = orders.select(
        "order_id", "customer_id", "order_ts",
        F.col("status").alias("order_status"), "payment_method", "currency",
        F.col("_dq_issues").alias("_order_issues"))
    return (items.withColumnRenamed("_dq_issues", "_item_issues")
                 .join(order_cols, "order_id", "left"))


def customer_version_lookup(dim_customer: "DataFrame") -> "DataFrame":
    """Return (customer_id, customer_key, _from, _to) ranges for the SCD2 join.

    Same rule as `pick_customer_version`: the first version of each customer
    starts at BEGINNING_OF_TIME. The unknown member is not part of the lookup.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    first = F.min("valid_from").over(Window.partitionBy("customer_id"))
    return (dim_customer.where(F.col("customer_key") != UNKNOWN_KEY)
            .select("customer_id", "customer_key", "valid_to",
                    F.when(F.col("valid_from") == first, F.lit(BEGINNING_OF_TIME))
                     .otherwise(F.col("valid_from")).alias("_from"))
            .withColumnRenamed("valid_to", "_to"))


def assign_keys(lines: "DataFrame", dim_customer: "DataFrame",
                dim_product: "DataFrame") -> "DataFrame":
    """Add date_key, customer_key and product_key; no match gives -1.

    The date key is computed, not looked up: it is a pure function of the
    date (yyyymmdd), so a join with dim_date would add cost and nothing else.
    Both dimension joins are left joins, so a line is never dropped because a
    description is missing.
    """
    from pyspark.sql import functions as F

    with_date = (lines.withColumn("_order_date", F.to_date("order_ts"))
                      .withColumn("date_key",
                                  F.date_format("_order_date", "yyyyMMdd").cast("int")))

    customers = customer_version_lookup(dim_customer).alias("c")
    in_range = ((F.col("l.customer_id") == F.col("c.customer_id"))
                & (F.col("l._order_date") >= F.col("c._from"))
                & (F.col("c._to").isNull() | (F.col("l._order_date") < F.col("c._to"))))
    with_customer = (with_date.alias("l").join(customers, in_range, "left")
                     .select("l.*", F.coalesce("c.customer_key", F.lit(UNKNOWN_KEY))
                                     .cast("bigint").alias("customer_key")))

    # dim_product is small: broadcast it so the large line table is not shuffled.
    products = dim_product.where(F.col("product_key") != UNKNOWN_KEY).select(
        "product_id", "product_key")
    return (with_customer.join(F.broadcast(products), "product_id", "left")
            .withColumn("product_key",
                        F.coalesce("product_key", F.lit(UNKNOWN_KEY)).cast("bigint")))


def build_fact_sales(items: "DataFrame", orders: "DataFrame", dim_customer: "DataFrame",
                     dim_product: "DataFrame") -> tuple["DataFrame", "DataFrame"]:
    """Return (fact rows, excluded lines with `_exclusion_reasons`)."""
    from pyspark.sql import functions as F

    lines = add_exclusion_reasons(join_lines_to_orders(items, orders))
    is_excluded = F.size("_exclusion_reasons") > 0
    excluded = lines.where(is_excluded)
    fact = (assign_keys(lines.where(~is_excluded), dim_customer, dim_product)
            .select(
                "order_id", "line_number", "date_key", "customer_key", "product_key",
                "order_ts", "order_status", "payment_method", "currency",
                "quantity", "unit_price", "discount_pct",
                F.col("line_gross_amount").alias("gross_amount"),
                F.col("line_discount_amount").alias("discount_amount"),
                F.col("line_net_amount").alias("net_amount"),
            ))
    return fact, excluded


def write_fact(df: "DataFrame", path: str) -> None:
    """Replace the whole table in one atomic Delta commit."""
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(path))


def log_exclusions(excluded: "DataFrame") -> dict[str, int]:
    """Count excluded lines per reason, so a jump in bad data is visible."""
    from pyspark.sql import functions as F

    rows = (excluded.select(F.explode("_exclusion_reasons").alias("reason"))
            .groupBy("reason").count().collect())
    counts = {r["reason"]: r["count"] for r in rows}
    for reason, n in sorted(counts.items()):
        logger.warning("gold.fact_sales: %d lines excluded for %s", n, reason)
    return counts


def load_fact_sales(spark: "SparkSession", config: PipelineConfig) -> dict[str, int]:
    """Rebuild gold.fact_sales and return row counts."""
    def read(layer: str, table: str) -> "DataFrame":
        return spark.read.format("delta").load(config.table_path(layer, table))

    fact, excluded = build_fact_sales(
        read("silver", "order_items"), read("silver", "orders"),
        read("gold", "dim_customer"), read("gold", "dim_product"))

    path = config.table_path("gold", "fact_sales")
    write_fact(fact, path)
    written = spark.read.format("delta").load(path)
    stats = {
        "rows": written.count(),
        "unknown_customer": written.where(f"customer_key = {UNKNOWN_KEY}").count(),
        "unknown_product": written.where(f"product_key = {UNKNOWN_KEY}").count(),
        "excluded": excluded.count(),
    }
    log_exclusions(excluded)
    logger.info("gold.fact_sales: %(rows)d rows (%(unknown_customer)d unknown customer, "
                "%(unknown_product)d unknown product), %(excluded)d lines excluded", stats)
    return stats


def main(argv: list[str] | None = None) -> None:
    from lakehouse.utils.spark import get_spark

    parser = argparse.ArgumentParser(description="Rebuild the gold fact_sales table.")
    parser.parse_args(argv)

    config = load_config()
    spark = get_spark("gold-fact-sales", config)
    load_fact_sales(spark, config)
    spark.stop()


if __name__ == "__main__":
    main()
