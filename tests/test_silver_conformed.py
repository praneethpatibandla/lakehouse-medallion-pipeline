"""Tests for silver products and order items (step 11)."""

from datetime import date, datetime
from decimal import Decimal

import pytest

from lakehouse.bronze.schemas import ORDER_ITEMS, PRODUCTS
from lakehouse.silver import order_items, products
from lakehouse.silver.order_items import AMOUNT_COLUMNS, line_amounts


def test_keys_are_source_columns():
    assert set(products.KEYS) <= set(PRODUCTS)
    assert set(order_items.KEYS) <= set(ORDER_ITEMS)
    # Derived columns must not collide with anything the source sends.
    assert not set(AMOUNT_COLUMNS) & set(ORDER_ITEMS)


def test_line_amounts_simple_line():
    assert line_amounts(2, Decimal("19.99"), Decimal("0")) == (
        Decimal("39.98"), Decimal("0.00"), Decimal("39.98"))


def test_line_amounts_rounds_discount_half_up_and_keeps_totals_consistent():
    # 3 * 4.75 = 14.25; 10% = 1.425 -> 1.43 (half-up), net 12.82
    gross, discount, net = line_amounts(3, Decimal("4.75"), Decimal("10"))
    assert (gross, discount, net) == (Decimal("14.25"), Decimal("1.43"), Decimal("12.82"))
    assert discount + net == gross


def test_line_amounts_propagates_missing_inputs_like_sql_null():
    assert line_amounts(None, Decimal("5.00"), Decimal("0")) == (None, None, None)
    assert line_amounts(2, Decimal("5.00"), None) == (Decimal("10.00"), None, None)


# ---------------------------------------------------------------------------
# Spark + Delta tests: skipped when pyspark / delta-spark / Java are missing.
# ---------------------------------------------------------------------------

DAY1 = date(2026, 11, 1)


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    from lakehouse.utils.config import PipelineConfig
    from lakehouse.utils.spark import get_spark

    config = PipelineConfig(env="test", data_root=tmp_path_factory.mktemp("lake"),
                            log_level="WARNING", shuffle_partitions=1)
    session = get_spark("test-silver-conformed", config)
    yield session
    session.stop()


def _bronze(spark, source, rows, batch_date=DAY1):
    from pyspark.sql import functions as F

    from lakehouse.bronze.schemas import spark_schema

    return (spark.createDataFrame(rows, schema=spark_schema(source))
            .withColumn("_source_file", F.lit(f"{source}.csv"))
            .withColumn("_ingested_at", F.lit(datetime(2026, 11, 2)))
            .withColumn("_batch_date", F.lit(batch_date))
            .withColumn("_run_id", F.lit("run-1")))


def test_prepare_products_cleans_and_rejects_null_keys(spark):
    staged, rejected = products.prepare_products(_bronze(spark, "products", [
        ("p0001", "Acme Mug", "Kitchen", "Acme", "12.50", "true"),
        (None, "Ghost", "kitchen", "Acme", "1.00", "true"),
    ]))
    row = staged.collect()[0]
    assert (row["product_id"], row["category"], row["unit_price"]) == (
        "P0001", "kitchen", Decimal("12.50"))
    assert rejected.count() == 1


def test_spark_amounts_match_the_python_rule(spark):
    from lakehouse.silver.transforms import clean_order_items

    bronze = _bronze(spark, "order_items", [
        ("O1", "1", "P0001", "3", "4.75", "10"),
        ("O1", "2", "P0002", "2", "19.99", "0"),
    ])
    rows = order_items.add_line_amounts(bronze.transform(clean_order_items)).collect()
    for r in rows:
        expected = line_amounts(r["quantity"], r["unit_price"], r["discount_pct"])
        assert (r["line_gross_amount"], r["line_discount_amount"], r["line_net_amount"]) == expected


def test_prepare_order_items_flags_unknown_products_and_orphan_orders(spark):
    silver_products = spark.createDataFrame([("P0001",)], "product_id string")
    silver_orders = spark.createDataFrame([("O1",)], "order_id string")
    staged, rejected = order_items.prepare_order_items(_bronze(spark, "order_items", [
        ("O1", "1", "P0001", "1", "10.00", "0"),   # clean
        ("O1", "2", "P9999", "1", "10.00", "0"),   # product not in catalogue
        ("O2", "1", "P0001", "1", "10.00", "0"),   # order never arrived
        ("O1", "x", "P0001", "1", "10.00", "0"),   # line_number does not cast -> no key
    ]), silver_products, silver_orders)

    issues = {(r["order_id"], r["line_number"]): r["_dq_issues"] for r in staged.collect()}
    assert issues == {("O1", 1): [], ("O1", 2): ["unknown_product"], ("O2", 1): ["orphan_order"]}
    assert rejected.count() == 1
