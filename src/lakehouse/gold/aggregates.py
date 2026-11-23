"""Gold aggregates: daily sales and customer lifetime value.

The star schema (steps 12-13) can answer any question, but dashboards ask the
same few questions all day. Aggregate tables answer them once per run, so a
BI tool reads a few hundred pre-computed rows instead of scanning every order
line on each refresh.

    agg_daily_sales                one row per calendar day, including days
                                   with no sales (zeros, not missing rows)
    agg_customer_lifetime_value    one row per customer (business key, not
                                   per SCD2 version)

Revenue rule, used by both tables: an order counts towards revenue unless it
was cancelled or returned. Cancelled and returned orders are still counted in
their own columns, so nothing disappears from the report.

Both tables are rebuilt in full from gold.fact_sales with an atomic overwrite.

Usage (after gold.fact_sales is built):
    python -m lakehouse.gold.aggregates
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from typing import TYPE_CHECKING, Iterable

from lakehouse.gold.dimensions import UNKNOWN_KEY
from lakehouse.utils.config import PipelineConfig, load_config
from lakehouse.utils.logger import get_logger

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession

logger = get_logger(__name__)

REVENUE_STATUSES = ("placed", "shipped", "delivered")
CENT = Decimal("0.01")

DAILY_SALES_COLUMNS = (
    "date_key", "date", "order_count", "revenue_order_count", "cancelled_order_count",
    "returned_order_count", "customer_count", "units_sold", "gross_amount",
    "discount_amount", "net_revenue", "avg_order_value",
)
CLV_COLUMNS = (
    "customer_id", "segment", "state", "first_order_date", "last_order_date",
    "order_count", "lifetime_net_revenue", "avg_order_value",
    "customer_tenure_days", "days_since_last_order",
)


# --------------------------------------------------------------------------
# Pure Python: the business rules
# --------------------------------------------------------------------------

def is_revenue_status(status: str | None) -> bool:
    """True when an order with this status counts towards revenue."""
    return status in REVENUE_STATUSES


def average_order_value(revenue: Decimal, orders: int) -> Decimal | None:
    """Revenue per order, rounded half-up to the cent; None with no orders.

    None rather than 0: a day with no orders has no average, and a 0 would
    drag down any average of averages computed on top of this column.
    """
    if orders == 0:
        return None
    return (revenue / Decimal(orders)).quantize(CENT, rounding=ROUND_HALF_UP)


@dataclass(frozen=True)
class OrderSummary:
    """One order, already summed over its lines: the input to CLV."""

    order_date: date
    status: str
    net_amount: Decimal


def customer_lifetime_value(orders: Iterable[OrderSummary], as_of: date) -> dict:
    """Return the CLV metrics for one customer, following the Spark version.

    Only revenue orders count. A customer with none (say their only order was
    cancelled) gets order_count 0 and None dates here; the Spark table leaves
    such customers out, as they have no lifetime value yet. `as_of` is the last order
    date in the whole fact table, not today, so a rebuild of old data gives
    the same numbers whenever it runs.
    """
    kept = [o for o in orders if is_revenue_status(o.status)]
    revenue = sum((o.net_amount for o in kept), Decimal("0.00"))
    first = min((o.order_date for o in kept), default=None)
    last = max((o.order_date for o in kept), default=None)
    return {
        "first_order_date": first,
        "last_order_date": last,
        "order_count": len(kept),
        "lifetime_net_revenue": revenue,
        "avg_order_value": average_order_value(revenue, len(kept)),
        "customer_tenure_days": (last - first).days if kept else None,
        "days_since_last_order": (as_of - last).days if kept else None,
    }


# --------------------------------------------------------------------------
# Spark
# --------------------------------------------------------------------------

def _is_revenue() -> "Column":
    from pyspark.sql import functions as F

    return F.coalesce(F.col("order_status").isin(*REVENUE_STATUSES), F.lit(False))


def _avg_order_value(revenue: str, orders: str) -> "Column":
    """Spark version of `average_order_value` (decimal `round` is half-up)."""
    from pyspark.sql import functions as F

    per_order = F.col(revenue) / F.when(F.col(orders) > 0, F.col(orders))
    return F.round(per_order, 2).cast("decimal(12,2)")


def build_daily_sales(fact: "DataFrame", dim_date: "DataFrame") -> "DataFrame":
    """One row per day between the first and last sale, zeros on quiet days."""
    from pyspark.sql import functions as F

    rev = _is_revenue()
    zero = F.lit(0).cast("decimal(12,2)")

    def revenue_sum(column: str) -> "Column":
        return F.coalesce(F.sum(F.when(rev, F.col(column))), zero).cast("decimal(18,2)")

    def distinct_orders(condition: "Column") -> "Column":
        # count(DISTINCT x) ignores NULLs, so non-matching rows drop out.
        return F.countDistinct(F.when(condition, F.col("order_id")))

    daily = fact.groupBy("date_key").agg(
        F.countDistinct("order_id").alias("order_count"),
        distinct_orders(rev).alias("revenue_order_count"),
        distinct_orders(F.col("order_status") == "cancelled").alias("cancelled_order_count"),
        distinct_orders(F.col("order_status") == "returned").alias("returned_order_count"),
        # The unknown customer (-1) is many people, so it is not counted as one.
        F.countDistinct(F.when(rev & (F.col("customer_key") != UNKNOWN_KEY),
                               F.col("customer_key"))).alias("customer_count"),
        F.coalesce(F.sum(F.when(rev, F.col("quantity"))), F.lit(0)).cast("bigint")
         .alias("units_sold"),
        revenue_sum("gross_amount").alias("gross_amount"),
        revenue_sum("discount_amount").alias("discount_amount"),
        revenue_sum("net_amount").alias("net_revenue"),
    )

    # Restrict the calendar to the range that has data, then left join, so a
    # day with no orders appears with zeros instead of being missing.
    bounds = fact.agg(F.min("date_key").alias("lo"), F.max("date_key").alias("hi"))
    calendar = (dim_date.crossJoin(bounds)
                .where(F.col("date_key").between(F.col("lo"), F.col("hi")))
                .select("date_key", "date"))
    counts = ("order_count", "revenue_order_count", "cancelled_order_count",
              "returned_order_count", "customer_count", "units_sold")
    amounts = ("gross_amount", "discount_amount", "net_revenue")
    return (calendar.join(daily, "date_key", "left")
            .fillna(0, subset=list(counts))
            .select("date_key", "date",
                    *[F.col(c).cast("bigint").alias(c) for c in counts],
                    *[F.coalesce(F.col(c), zero).cast("decimal(18,2)").alias(c)
                      for c in amounts])
            .withColumn("avg_order_value",
                        _avg_order_value("net_revenue", "revenue_order_count"))
            .select(*DAILY_SALES_COLUMNS))


def build_customer_lifetime_value(fact: "DataFrame",
                                  dim_customer: "DataFrame") -> "DataFrame":
    """One row per customer_id, following `customer_lifetime_value`.

    Grouping is on customer_id, not customer_key: a customer who moved has
    two keys (two SCD2 versions) but is still one customer. Descriptive
    columns come from the *current* version.
    """
    from pyspark.sql import functions as F

    versions = dim_customer.where(F.col("customer_key") != UNKNOWN_KEY)
    # Step 1: lines -> orders. An order's lines share date and status.
    orders = (fact.where(_is_revenue())
              .join(versions.select("customer_key", "customer_id"), "customer_key")
              .groupBy("customer_id", "order_id")
              .agg(F.to_date(F.min("order_ts")).alias("order_date"),
                   F.sum("net_amount").alias("order_net")))
    # Step 2: orders -> customers.
    as_of = fact.agg(F.to_date(F.max("order_ts")).alias("as_of"))
    per_customer = (orders.groupBy("customer_id").agg(
                        F.min("order_date").alias("first_order_date"),
                        F.max("order_date").alias("last_order_date"),
                        F.count("order_id").alias("order_count"),
                        F.sum("order_net").cast("decimal(18,2)")
                         .alias("lifetime_net_revenue"))
                    .crossJoin(as_of))

    current = versions.where("is_current").select("customer_id", "segment", "state")
    return (per_customer.join(current, "customer_id", "left")
            .withColumn("avg_order_value",
                        _avg_order_value("lifetime_net_revenue", "order_count"))
            .withColumn("customer_tenure_days",
                        F.datediff("last_order_date", "first_order_date"))
            .withColumn("days_since_last_order", F.datediff("as_of", "last_order_date"))
            .select(*CLV_COLUMNS))


def write_aggregate(df: "DataFrame", path: str) -> None:
    """Replace the whole table in one atomic Delta commit."""
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(path))


def build_aggregates(spark: "SparkSession", config: PipelineConfig) -> dict[str, int]:
    """Rebuild both aggregate tables and return their row counts."""
    def read(table: str) -> "DataFrame":
        return spark.read.format("delta").load(config.table_path("gold", table))

    fact = read("fact_sales")
    tables = {
        "agg_daily_sales": build_daily_sales(fact, read("dim_date")),
        "agg_customer_lifetime_value": build_customer_lifetime_value(
            fact, read("dim_customer")),
    }
    counts = {}
    for name, df in tables.items():
        path = config.table_path("gold", name)
        write_aggregate(df, path)
        counts[name] = spark.read.format("delta").load(path).count()
        logger.info("gold.%s: %d rows", name, counts[name])
    return counts


def main(argv: list[str] | None = None) -> None:
    from lakehouse.utils.spark import get_spark

    parser = argparse.ArgumentParser(description="Rebuild the gold aggregate tables.")
    parser.parse_args(argv)

    config = load_config()
    spark = get_spark("gold-aggregates", config)
    build_aggregates(spark, config)
    spark.stop()


if __name__ == "__main__":
    main()
