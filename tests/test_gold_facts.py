"""Tests for the gold fact table (step 13)."""

from datetime import date, datetime
from decimal import Decimal

import pytest

from lakehouse.gold.dimensions import UNKNOWN_KEY
from lakehouse.gold.facts import (
    FACT_COLUMNS,
    MISSING_AMOUNT,
    MISSING_ORDER_DATE,
    exclusion_reasons,
    pick_customer_version,
)

# ---------------------------------------------------------------------------
# Pure Python rules
# ---------------------------------------------------------------------------


def test_clean_line_is_kept():
    assert exclusion_reasons([], [], has_order_date=True, has_amount=True) == []


def test_descriptive_issues_do_not_exclude_a_line():
    # The money is real; only the label is missing, so the line maps to -1.
    reasons = exclusion_reasons(["unknown_product"], ["missing_customer_id", "unknown_status"],
                                has_order_date=True, has_amount=True)
    assert reasons == []


def test_untrustworthy_numbers_or_dates_exclude_a_line():
    assert exclusion_reasons(["non_positive_quantity"], [], True, True) == ["non_positive_quantity"]
    assert exclusion_reasons(["orphan_order"], [], True, True) == ["orphan_order"]
    assert exclusion_reasons([], ["invalid_order_ts"], False, True) == [
        "invalid_order_ts", MISSING_ORDER_DATE]
    assert exclusion_reasons([], [], True, False) == [MISSING_AMOUNT]


# Two versions of one customer: moved on 2026-11-05.
VERSIONS = [(11, date(2026, 11, 1), date(2026, 11, 5)), (12, date(2026, 11, 5), None)]


def test_order_gets_the_version_valid_on_its_date():
    assert pick_customer_version(VERSIONS, date(2026, 11, 3)) == 11
    # valid_to is exclusive: on the day of the change the new version applies.
    assert pick_customer_version(VERSIONS, date(2026, 11, 5)) == 12
    assert pick_customer_version(VERSIONS, date(2027, 3, 1)) == 12


def test_order_before_first_snapshot_gets_the_first_version():
    assert pick_customer_version(VERSIONS, date(2026, 6, 30)) == 11


def test_customer_without_versions_is_unknown():
    assert pick_customer_version([], date(2026, 11, 3)) == UNKNOWN_KEY


# ---------------------------------------------------------------------------
# Spark tests: skipped when pyspark / delta-spark / Java are missing.
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    from lakehouse.utils.config import PipelineConfig
    from lakehouse.utils.spark import get_spark

    config = PipelineConfig(env="test", data_root=tmp_path_factory.mktemp("lake"),
                            log_level="WARNING", shuffle_partitions=1)
    session = get_spark("test-gold-facts", config)
    yield session
    session.stop()


def _inputs(spark):
    orders = spark.createDataFrame([
        ("O1", "C1", datetime(2026, 11, 3, 10), "placed", "card", "USD", []),
        ("O2", "C1", datetime(2026, 11, 6, 9), "shipped", "card", "USD", []),
        ("O3", None, datetime(2026, 11, 6, 12), "placed", "paypal", "USD",
         ["missing_customer_id"]),
        ("O4", "C1", None, "placed", "card", "USD", ["invalid_order_ts"]),
    ], "order_id string, customer_id string, order_ts timestamp, status string, "
       "payment_method string, currency string, _dq_issues array<string>")

    d = Decimal
    items = spark.createDataFrame([
        ("O1", 1, "P1", 2, d("10.00"), d("0"), d("20.00"), d("0.00"), d("20.00"), []),
        ("O2", 1, "P9", 1, d("5.00"), d("0"), d("5.00"), d("0.00"), d("5.00"),
         ["unknown_product"]),
        ("O3", 1, "P1", 1, d("10.00"), d("0"), d("10.00"), d("0.00"), d("10.00"), []),
        ("O4", 1, "P1", 1, d("10.00"), d("0"), d("10.00"), d("0.00"), d("10.00"), []),
        ("O1", 2, "P1", -1, d("10.00"), d("0"), d("-10.00"), d("0.00"), d("-10.00"),
         ["non_positive_quantity"]),
        ("O9", 1, "P1", 1, d("10.00"), d("0"), d("10.00"), d("0.00"), d("10.00"),
         ["orphan_order"]),
    ], "order_id string, line_number int, product_id string, quantity int, "
       "unit_price decimal(10,2), discount_pct decimal(5,2), line_gross_amount decimal(12,2), "
       "line_discount_amount decimal(12,2), line_net_amount decimal(12,2), "
       "_dq_issues array<string>")

    dim_customer = spark.createDataFrame([
        (11, "C1", date(2026, 11, 1), date(2026, 11, 5)),
        (12, "C1", date(2026, 11, 5), None),
        (UNKNOWN_KEY, "UNKNOWN", None, None),
    ], "customer_key bigint, customer_id string, valid_from date, valid_to date")
    dim_product = spark.createDataFrame(
        [(101, "P1"), (UNKNOWN_KEY, "UNKNOWN")], "product_key bigint, product_id string")
    return items, orders, dim_customer, dim_product


def test_fact_sales_keys_and_exclusions(spark):
    from lakehouse.gold.facts import build_fact_sales

    fact, excluded = build_fact_sales(*_inputs(spark))
    assert tuple(fact.columns) == FACT_COLUMNS

    rows = {r["order_id"]: r for r in fact.collect()}
    assert set(rows) == {"O1", "O2", "O3"}
    # SCD2: the order on 11-03 gets version 11, the one on 11-06 version 12.
    assert (rows["O1"]["customer_key"], rows["O2"]["customer_key"]) == (11, 12)
    assert rows["O1"]["date_key"] == 20261103
    # Missing descriptions point at the unknown member instead of being dropped.
    assert rows["O2"]["product_key"] == UNKNOWN_KEY
    assert rows["O3"]["customer_key"] == UNKNOWN_KEY

    reasons = {(r["order_id"], r["line_number"]): r["_exclusion_reasons"]
               for r in excluded.collect()}
    assert reasons == {
        ("O1", 2): ["non_positive_quantity"],
        ("O4", 1): ["invalid_order_ts", MISSING_ORDER_DATE],
        ("O9", 1): ["orphan_order", MISSING_ORDER_DATE],
    }
