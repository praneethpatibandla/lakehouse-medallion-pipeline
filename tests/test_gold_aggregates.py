"""Tests for the gold aggregates (step 14)."""

from datetime import date, datetime
from decimal import Decimal

from lakehouse.gold.aggregates import (
    CLV_COLUMNS,
    DAILY_SALES_COLUMNS,
    OrderSummary,
    average_order_value,
    customer_lifetime_value,
    is_revenue_status,
)
from lakehouse.gold.dimensions import UNKNOWN_KEY

# ---------------------------------------------------------------------------
# Pure Python rules
# ---------------------------------------------------------------------------


def test_revenue_statuses():
    assert all(is_revenue_status(s) for s in ("placed", "shipped", "delivered"))
    assert not any(is_revenue_status(s) for s in ("cancelled", "returned", None))


def test_average_order_value_rounds_half_up_and_handles_no_orders():
    assert average_order_value(Decimal("10.00"), 3) == Decimal("3.33")
    assert average_order_value(Decimal("0.05"), 2) == Decimal("0.03")   # 0.025 -> 0.03
    assert average_order_value(Decimal("0.00"), 0) is None


def test_customer_lifetime_value_ignores_cancelled_and_returned_orders():
    orders = [
        OrderSummary(date(2026, 11, 1), "delivered", Decimal("40.00")),
        OrderSummary(date(2026, 11, 4), "shipped", Decimal("20.00")),
        OrderSummary(date(2026, 11, 6), "cancelled", Decimal("99.00")),
        OrderSummary(date(2026, 11, 7), "returned", Decimal("15.00")),
    ]
    clv = customer_lifetime_value(orders, as_of=date(2026, 11, 10))
    assert clv == {
        "first_order_date": date(2026, 11, 1),
        "last_order_date": date(2026, 11, 4),
        "order_count": 2,
        "lifetime_net_revenue": Decimal("60.00"),
        "avg_order_value": Decimal("30.00"),
        "customer_tenure_days": 3,
        "days_since_last_order": 6,
    }


def test_customer_lifetime_value_with_no_revenue_orders():
    clv = customer_lifetime_value(
        [OrderSummary(date(2026, 11, 1), "cancelled", Decimal("5.00"))], date(2026, 11, 2))
    assert clv["order_count"] == 0
    assert clv["lifetime_net_revenue"] == Decimal("0.00")
    assert clv["avg_order_value"] is None and clv["first_order_date"] is None


# ---------------------------------------------------------------------------
# Spark tests: skipped when pyspark / delta-spark / Java are missing.
# ---------------------------------------------------------------------------

def _fact(spark):
    d = Decimal
    rows = [
        # order, line, date_key, customer_key, ts, status, qty, gross, discount, net
        ("O1", 1, 20261101, 11, datetime(2026, 11, 1, 9), "delivered", 2, d("20.00"), d("2.00"), d("18.00")),
        ("O1", 2, 20261101, 11, datetime(2026, 11, 1, 9), "delivered", 1, d("5.00"), d("0.00"), d("5.00")),
        ("O2", 1, 20261101, UNKNOWN_KEY, datetime(2026, 11, 1, 15), "placed", 1, d("7.00"), d("0.00"), d("7.00")),
        ("O3", 1, 20261101, 11, datetime(2026, 11, 1, 18), "cancelled", 3, d("30.00"), d("0.00"), d("30.00")),
        # 2026-11-02 has no orders at all.
        ("O4", 1, 20261103, 12, datetime(2026, 11, 3, 10), "shipped", 1, d("10.00"), d("0.00"), d("10.00")),
    ]
    return spark.createDataFrame(
        rows, "order_id string, line_number int, date_key int, customer_key bigint, "
              "order_ts timestamp, order_status string, quantity int, "
              "gross_amount decimal(12,2), discount_amount decimal(12,2), "
              "net_amount decimal(12,2)")


def test_daily_sales_fills_quiet_days_and_excludes_cancelled_revenue(spark):
    from lakehouse.gold.aggregates import build_daily_sales
    from lakehouse.gold.dimensions import build_dim_date

    dim_date = build_dim_date(spark, date(2026, 10, 1), date(2026, 12, 31))
    daily = build_daily_sales(_fact(spark), dim_date)
    assert tuple(daily.columns) == DAILY_SALES_COLUMNS

    rows = {r["date_key"]: r for r in daily.collect()}
    assert sorted(rows) == [20261101, 20261102, 20261103]
    nov1 = rows[20261101]
    assert (nov1["order_count"], nov1["revenue_order_count"], nov1["cancelled_order_count"]) == (3, 2, 1)
    assert nov1["customer_count"] == 1          # the unknown customer is not counted
    assert nov1["units_sold"] == 4
    assert nov1["net_revenue"] == Decimal("30.00")
    assert nov1["avg_order_value"] == Decimal("15.00")
    quiet = rows[20261102]
    assert (quiet["order_count"], quiet["net_revenue"], quiet["avg_order_value"]) == (
        0, Decimal("0.00"), None)


def test_customer_lifetime_value_groups_versions_by_customer_id(spark):
    from lakehouse.gold.aggregates import build_customer_lifetime_value

    dim_customer = spark.createDataFrame([
        (11, "C1", "consumer", "OR", False),
        (12, "C1", "consumer", "CO", True),     # same customer after a move
        (UNKNOWN_KEY, "UNKNOWN", None, None, True),
    ], "customer_key bigint, customer_id string, segment string, state string, "
       "is_current boolean")
    clv = build_customer_lifetime_value(_fact(spark), dim_customer)
    assert tuple(clv.columns) == CLV_COLUMNS

    rows = clv.collect()
    assert [r["customer_id"] for r in rows] == ["C1"]
    c1 = rows[0]
    assert c1["state"] == "CO"                  # from the current version
    assert c1["order_count"] == 2               # O1 and O4; O3 was cancelled
    assert c1["lifetime_net_revenue"] == Decimal("33.00")
    assert c1["avg_order_value"] == Decimal("16.50")
    assert (c1["customer_tenure_days"], c1["days_since_last_order"]) == (2, 0)
