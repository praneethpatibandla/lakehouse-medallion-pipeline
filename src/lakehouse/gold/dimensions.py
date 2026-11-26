"""Gold dimensions: dim_date, dim_customer and dim_product.

Gold is a star schema: one fact table of sales (step 13) surrounded by
dimensions that describe the who, what and when of each sale. Analysts join
facts to dimensions on *surrogate keys*, small integer keys owned by the
warehouse, rather than on source IDs.

    dim_date       one row per calendar day, key 20261101 (yyyymmdd)
    dim_customer   one row per customer *version* (from the SCD2 silver table)
    dim_product    one row per product (Type 1)

Each dimension is rebuilt in full on every run and written with a Delta
overwrite. They are small, a full rebuild is always correct, and the overwrite
is a single atomic commit: readers see either the old table or the new one.

Both entity dimensions get an "unknown member" row with key -1. A sale whose
customer or product is missing still joins to something, so totals in a
report never silently lose rows to an inner join.

Usage (after silver customers and products are built):
    python -m lakehouse.gold.dimensions
    python -m lakehouse.gold.dimensions --start-date 2024-01-01 --end-date 2027-12-31
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta
from typing import TYPE_CHECKING, Any

from lakehouse.utils.config import PipelineConfig, load_config
from lakehouse.utils.logger import get_logger
from lakehouse.utils.tables import read_table, save_table

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession

logger = get_logger(__name__)

UNKNOWN_KEY = -1
# Customers sign up from 2023 (see the data generator); orders are recent.
DEFAULT_START_DATE = date(2023, 1, 1)
DEFAULT_END_DATE = date(2027, 12, 31)

DIM_DATE_COLUMNS = ("date_key", "date", "year", "quarter", "month", "month_name",
                    "day_of_month", "day_of_week", "day_name", "iso_week",
                    "is_weekend")


# --------------------------------------------------------------------------
# Pure Python: the date dimension
# --------------------------------------------------------------------------

def date_key(day: date) -> int:
    """Return the yyyymmdd integer key for a date, e.g. 2026-11-01 -> 20261101.

    A readable integer is the usual date key: it sorts in date order, and
    anyone can tell which day a fact row belongs to without a join.
    """
    return day.year * 10000 + day.month * 100 + day.day


def date_rows(start: date, end: date) -> list[dict[str, Any]]:
    """Return one dim_date row per day from `start` to `end`, both included.

    Built in Python rather than with Spark date functions because the rules
    (ISO weeks, Monday = 1) are then explicit and unit tested, and a few
    thousand rows are trivial to create on the driver.
    """
    if start > end:
        raise ValueError(f"start date {start} is after end date {end}")
    rows = []
    day = start
    while day <= end:
        rows.append({
            "date_key": date_key(day),
            "date": day,
            "year": day.year,
            "quarter": (day.month - 1) // 3 + 1,
            "month": day.month,
            "month_name": day.strftime("%B"),
            "day_of_month": day.day,
            "day_of_week": day.isoweekday(),        # Monday = 1 ... Sunday = 7
            "day_name": day.strftime("%A"),
            "iso_week": day.isocalendar()[1],
            "is_weekend": day.isoweekday() >= 6,
        })
        day += timedelta(days=1)
    return rows


# --------------------------------------------------------------------------
# Spark
# --------------------------------------------------------------------------

def build_dim_date(spark: "SparkSession", start: date, end: date) -> "DataFrame":
    schema = ("date_key int, date date, year int, quarter int, month int, "
              "month_name string, day_of_month int, day_of_week int, day_name string, "
              "iso_week int, is_weekend boolean")
    rows = [tuple(r[c] for c in DIM_DATE_COLUMNS) for r in date_rows(start, end)]
    return spark.createDataFrame(rows, schema=schema)


def surrogate_key(*columns: str) -> "Column":
    """Deterministic BIGINT key from the given columns.

    A hash of the business key gives the same surrogate key on every rebuild,
    so a fact table built yesterday still joins to today's dimension.
    `monotonically_increasing_id()` or `row_number()` would renumber rows
    whenever the input changes. (A 64-bit hash collision is possible in
    theory; at millions of rows the chance is negligible. Warehouses that
    must rule it out keep a key-mapping table instead.)
    """
    from pyspark.sql import functions as F

    return F.xxhash64(*[F.col(c) for c in columns])


def with_unknown_member(df: "DataFrame", key_column: str,
                        values: dict[str, Any]) -> "DataFrame":
    """Append the "unknown" row (key -1) to a dimension.

    Columns not given in `values` are NULL. Every value is cast to the
    column's type, so the extra row always matches the dimension's schema.
    """
    from pyspark.sql import functions as F

    values = {key_column: UNKNOWN_KEY, **values}
    unknown = df.sparkSession.range(1).select(
        *[F.lit(values.get(f.name)).cast(f.dataType).alias(f.name) for f in df.schema.fields])
    return df.unionByName(unknown)


def build_dim_customer(silver_customers: "DataFrame") -> "DataFrame":
    """One row per customer version, keyed on (customer_id, valid_from).

    The SCD2 columns are kept: the fact build (step 13) picks the version
    whose [valid_from, valid_to) range contains the order date.
    """
    from pyspark.sql import functions as F

    dim = silver_customers.select(
        surrogate_key("customer_id", "valid_from").alias("customer_key"),
        "customer_id",
        "first_name",
        "last_name",
        F.concat_ws(" ", "first_name", "last_name").alias("full_name"),
        "city", "state", "country", "segment", "signup_date",
        "valid_from", "valid_to", "is_current",
    )
    return with_unknown_member(dim, "customer_key", {
        "customer_id": "UNKNOWN", "full_name": "Unknown customer", "is_current": True})


def build_dim_product(silver_products: "DataFrame") -> "DataFrame":
    """One row per product. `unit_price` becomes `list_price` here, to keep it
    apart from the price actually paid, which lives on the fact table."""
    dim = silver_products.select(
        surrogate_key("product_id").alias("product_key"),
        "product_id", "product_name", "category", "brand",
        silver_products["unit_price"].alias("list_price"),
        "is_active",
    )
    return with_unknown_member(dim, "product_key", {
        "product_id": "UNKNOWN", "product_name": "Unknown product", "category": "unknown"})


def write_dimension(df: "DataFrame", path: str) -> None:
    """Replace the whole table in one atomic Delta commit.

    `overwriteSchema` lets a rebuild add or rename columns; without it Delta
    rejects a schema change on overwrite.
    """
    save_table(df.write.format("delta")
               .mode("overwrite")
               .option("overwriteSchema", "true"), path)


def build_dimensions(spark: "SparkSession", config: PipelineConfig,
                     start: date = DEFAULT_START_DATE,
                     end: date = DEFAULT_END_DATE) -> dict[str, int]:
    """Build all three dimensions and return their row counts."""
    silver = {t: read_table(spark, config.table_path("silver", t))
              for t in ("customers", "products")}
    dims = {
        "dim_date": build_dim_date(spark, start, end),
        "dim_customer": build_dim_customer(silver["customers"]),
        "dim_product": build_dim_product(silver["products"]),
    }
    counts = {}
    for name, df in dims.items():
        path = config.table_path("gold", name)
        write_dimension(df, path)
        counts[name] = read_table(spark, path).count()
        logger.info("gold.%s: %d rows", name, counts[name])
    return counts


def main(argv: list[str] | None = None) -> None:
    from lakehouse.utils.spark import get_spark

    parser = argparse.ArgumentParser(description="Rebuild the gold dimension tables.")
    parser.add_argument("--start-date", type=date.fromisoformat, default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", type=date.fromisoformat, default=DEFAULT_END_DATE)
    args = parser.parse_args(argv)

    config = load_config()
    spark = get_spark("gold-dimensions", config)
    build_dimensions(spark, config, args.start_date, args.end_date)
    spark.stop()


if __name__ == "__main__":
    main()
