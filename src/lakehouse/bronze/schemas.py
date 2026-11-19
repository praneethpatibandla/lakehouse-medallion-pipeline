"""Explicit schemas for the landing-zone sources.

Bronze keeps data as received, so every source column is read as a string.
Casting to dates, decimals and integers happens in silver (step 08), where a
value that fails to cast can be handled on purpose. If bronze cast instead,
a value like "18/11/2026 14:05" would silently turn into NULL and the original
text would be lost before anyone could look at it.

Column names are plain Python tuples so they can be checked against the data
generator in tests without pyspark. `spark_schema()` turns them into a Spark
StructType when a job actually needs one.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

CUSTOMERS = ("customer_id", "first_name", "last_name", "city", "state",
             "country", "segment", "signup_date")
PRODUCTS = ("product_id", "product_name", "category", "brand", "unit_price", "is_active")
ORDERS = ("order_id", "customer_id", "order_ts", "status", "payment_method",
          "currency", "updated_at")
ORDER_ITEMS = ("order_id", "line_number", "product_id", "quantity",
               "unit_price", "discount_pct")

SOURCE_COLUMNS: dict[str, tuple[str, ...]] = {
    "customers": CUSTOMERS,
    "products": PRODUCTS,
    "orders": ORDERS,
    "order_items": ORDER_ITEMS,
}


def spark_schema(source: str) -> "StructType":
    """Return the all-string Spark schema for a source, e.g. spark_schema("orders")."""
    from pyspark.sql.types import StringType, StructField, StructType

    if source not in SOURCE_COLUMNS:
        raise ValueError(f"Unknown source '{source}'. Expected one of: {sorted(SOURCE_COLUMNS)}")
    # nullable=True: bronze must accept whatever arrives, including missing values.
    return StructType([StructField(name, StringType(), True) for name in SOURCE_COLUMNS[source]])
