from datetime import date
from decimal import Decimal

from data_generator.orders import STATUSES
from lakehouse.bronze.schemas import SOURCE_COLUMNS
from lakehouse.silver.transforms import CLEANERS, TARGET_TYPES, VALID_ORDER_STATUSES


def test_target_types_only_name_real_columns():
    for source, types in TARGET_TYPES.items():
        assert set(types) <= set(SOURCE_COLUMNS[source]), source


def test_every_source_has_a_cleaner():
    assert set(CLEANERS) == set(SOURCE_COLUMNS)


def test_valid_statuses_match_the_source_system():
    assert set(VALID_ORDER_STATUSES) == set(STATUSES)


# ---------------------------------------------------------------------------
# Spark tests: use the shared `spark` fixture (tests/conftest.py) and are
# skipped when pyspark / delta-spark / Java are missing.
# ---------------------------------------------------------------------------

def _orders(spark, rows):
    from lakehouse.bronze.schemas import spark_schema
    return spark.createDataFrame(rows, schema=spark_schema("orders"))


def test_clean_orders_parses_both_timestamp_formats(spark):
    from pyspark.sql import functions as F

    from lakehouse.silver.transforms import clean_orders

    df = _orders(spark, [
        ("O1", "c00001", "2026-11-01T14:05:00", "placed", "Card", "usd", "2026-11-01T14:05:00"),
        ("O2", "C00002", "18/11/2026 14:05", "placed", "card", "USD", "2026-11-18T14:05:00"),
    ]).transform(clean_orders)
    # Format inside Spark: collect() converts timestamps to the laptop's local
    # time zone, which would make the assertion depend on where tests run.
    df = df.withColumn("ts_text", F.date_format("order_ts", "yyyy-MM-dd HH:mm"))
    rows = {r["order_id"]: r for r in df.collect()}
    assert rows["O1"]["ts_text"] == "2026-11-01 14:05"
    assert rows["O2"]["ts_text"] == "2026-11-18 14:05"
    assert rows["O1"]["customer_id"] == "C00001"
    assert (rows["O1"]["payment_method"], rows["O1"]["currency"]) == ("card", "USD")
    assert rows["O1"]["_dq_issues"] == [] and rows["O2"]["_dq_issues"] == []


def test_clean_orders_flags_problems_instead_of_failing(spark):
    from lakehouse.silver.transforms import clean_orders

    df = _orders(spark, [
        ("O1", "  ", "not a date", "  Shipped ", "card", "USD", "2026-11-01T14:05:00"),
        ("O2", "C00002", "2026-11-01T10:00:00", "lost", "card", "USD", "2026-11-01T10:00:00"),
    ]).transform(clean_orders)
    rows = {r["order_id"]: r for r in df.collect()}
    assert rows["O1"]["status"] == "shipped"
    assert rows["O1"]["customer_id"] is None
    assert rows["O1"]["order_ts"] is None
    assert set(rows["O1"]["_dq_issues"]) == {"invalid_order_ts", "missing_customer_id"}
    assert rows["O2"]["_dq_issues"] == ["unknown_status"]


def test_clean_order_items_casts_and_flags(spark):
    from lakehouse.bronze.schemas import spark_schema
    from lakehouse.silver.transforms import clean_order_items

    df = spark.createDataFrame([
        ("O1", "1", "p001", " 2 ", "19.99", "10"),
        ("O1", "2", "P002", "-1", "abc", "0"),
    ], schema=spark_schema("order_items")).transform(clean_order_items)
    rows = {r["line_number"]: r for r in df.collect()}
    assert rows[1]["quantity"] == 2
    assert rows[1]["unit_price"] == Decimal("19.99")
    assert rows[1]["product_id"] == "P001"
    assert rows[1]["_dq_issues"] == []
    assert rows[2]["unit_price"] is None
    assert set(rows[2]["_dq_issues"]) == {"invalid_unit_price", "non_positive_quantity"}


def test_clean_customers_and_products(spark):
    from lakehouse.bronze.schemas import spark_schema
    from lakehouse.silver.transforms import clean_customers, clean_products

    customers = spark.createDataFrame(
        [("C00001", "ada", "LOVELACE", "Portland", "or", "us", "Retail", "2024-03-05")],
        schema=spark_schema("customers")).transform(clean_customers)
    c = customers.first()
    assert (c["first_name"], c["last_name"], c["state"]) == ("Ada", "Lovelace", "OR")
    assert c["signup_date"] == date(2024, 3, 5)

    products = spark.createDataFrame(
        [("P001", "Desk Lamp", "Home", "Lumo", "24.50", "false")],
        schema=spark_schema("products")).transform(clean_products)
    p = products.first()
    assert p["unit_price"] == Decimal("24.50") and p["is_active"] is False
    assert p["category"] == "home"
