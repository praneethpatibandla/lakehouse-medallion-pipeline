"""Tests for the gold dimensions (step 12)."""

from datetime import date, datetime
from decimal import Decimal

import pytest

from lakehouse.gold.dimensions import (
    DIM_DATE_COLUMNS,
    UNKNOWN_KEY,
    date_key,
    date_rows,
)


def test_date_key_is_sortable_yyyymmdd():
    assert date_key(date(2026, 11, 1)) == 20261101
    assert date_key(date(2026, 1, 9)) < date_key(date(2026, 10, 1))


def test_date_rows_cover_the_range_inclusively_without_gaps():
    rows = date_rows(date(2024, 2, 27), date(2024, 3, 2))   # leap year
    assert [r["date"] for r in rows] == [date(2024, 2, 27), date(2024, 2, 28), date(2024, 2, 29),
                                         date(2024, 3, 1), date(2024, 3, 2)]
    assert all(tuple(r) == DIM_DATE_COLUMNS for r in rows)


def test_date_rows_calendar_attributes():
    sunday, = date_rows(date(2026, 11, 1), date(2026, 11, 1))
    assert sunday["date_key"] == 20261101
    assert (sunday["year"], sunday["quarter"], sunday["month"]) == (2026, 4, 11)
    assert (sunday["month_name"], sunday["day_name"]) == ("November", "Sunday")
    assert (sunday["day_of_week"], sunday["is_weekend"]) == (7, True)
    monday, = date_rows(date(2026, 11, 2), date(2026, 11, 2))
    assert (monday["day_of_week"], monday["is_weekend"]) == (1, False)


def test_iso_week_at_year_boundary():
    # 1 Jan 2027 is a Friday: it still belongs to ISO week 53 of 2026.
    jan1, = date_rows(date(2027, 1, 1), date(2027, 1, 1))
    assert jan1["iso_week"] == 53


def test_date_rows_rejects_reversed_range():
    with pytest.raises(ValueError):
        date_rows(date(2026, 12, 1), date(2026, 11, 1))


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
    session = get_spark("test-gold-dimensions", config)
    yield session
    session.stop()


def _silver_customers(spark):
    schema = ("customer_id string, first_name string, last_name string, city string, "
              "state string, country string, segment string, signup_date date, "
              "valid_from date, valid_to date, is_current boolean, _row_hash string")
    return spark.createDataFrame([
        ("C00001", "Ada", "Lovelace", "Portland", "OR", "US", "consumer", date(2024, 3, 5),
         date(2026, 11, 1), date(2026, 11, 2), False, "h1"),
        ("C00001", "Ada", "Lovelace", "Denver", "CO", "US", "consumer", date(2024, 3, 5),
         date(2026, 11, 2), None, True, "h2"),
    ], schema=schema)


def test_dim_customer_has_one_key_per_version_plus_unknown(spark):
    from lakehouse.gold.dimensions import build_dim_customer

    rows = build_dim_customer(_silver_customers(spark)).collect()
    keys = [r["customer_key"] for r in rows]
    assert len(keys) == len(set(keys)) == 3
    unknown = [r for r in rows if r["customer_key"] == UNKNOWN_KEY]
    assert unknown[0]["customer_id"] == "UNKNOWN"
    assert {r["full_name"] for r in rows} == {"Ada Lovelace", "Unknown customer"}
    # Rebuilding gives the same keys: facts built earlier still join.
    again = [r["customer_key"] for r in build_dim_customer(_silver_customers(spark)).collect()]
    assert sorted(again) == sorted(keys)


def test_dim_product_renames_price_and_adds_unknown(spark):
    from lakehouse.gold.dimensions import build_dim_product

    silver = spark.createDataFrame(
        [("P0001", "Acme Mug", "kitchen", "Acme", Decimal("12.50"), True, datetime(2026, 11, 1))],
        "product_id string, product_name string, category string, brand string, "
        "unit_price decimal(10,2), is_active boolean, _ingested_at timestamp")
    dim = build_dim_product(silver)
    assert "list_price" in dim.columns and "unit_price" not in dim.columns
    assert not any(c.startswith("_") for c in dim.columns)
    assert {r["product_id"] for r in dim.collect()} == {"P0001", "UNKNOWN"}


def test_dim_date_schema_and_row_count(spark):
    from lakehouse.gold.dimensions import build_dim_date

    dim = build_dim_date(spark, date(2026, 1, 1), date(2026, 12, 31))
    assert tuple(dim.columns) == DIM_DATE_COLUMNS
    assert dim.count() == 365
