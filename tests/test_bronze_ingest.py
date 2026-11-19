from datetime import date
from pathlib import Path

import pytest

from data_generator.orders import ORDER_ITEM_COLUMNS, generate_daily_batch, write_batch
from data_generator.reference_data import (
    CUSTOMER_COLUMNS,
    PRODUCT_COLUMNS,
    generate_customers,
    generate_products,
    write_csv,
)
from lakehouse.bronze.ingest import METADATA_COLUMNS, SOURCES, source_path
from lakehouse.bronze.schemas import SOURCE_COLUMNS
from lakehouse.utils.config import load_config

DAY = date(2026, 11, 1)


def test_schemas_match_generator_output():
    """Contract test: the bronze schema must match what the source writes."""
    assert list(SOURCE_COLUMNS["customers"]) == CUSTOMER_COLUMNS
    assert list(SOURCE_COLUMNS["products"]) == PRODUCT_COLUMNS
    assert list(SOURCE_COLUMNS["order_items"]) == ORDER_ITEM_COLUMNS
    batch = generate_daily_batch(DAY, ["C00001"], generate_products(3), orders_per_day=1, dirty_rate=0)
    assert set(SOURCE_COLUMNS["orders"]) == set(batch.orders[0])


def test_every_source_has_a_schema():
    assert set(SOURCES) == set(SOURCE_COLUMNS)


def test_source_path(monkeypatch, tmp_path):
    monkeypatch.setenv("LAKEHOUSE_DATA_ROOT", str(tmp_path))
    config = load_config()
    daily = Path(source_path(config, "orders", DAY))
    assert daily == tmp_path / "landing" / "orders" / "batch_date=2026-11-01"
    snapshot = Path(source_path(config, "customers", DAY))
    assert snapshot == tmp_path / "landing" / "customers"
    with pytest.raises(ValueError):
        source_path(config, "invoices", DAY)


def test_metadata_columns_do_not_clash_with_source_columns():
    source_columns = {c for columns in SOURCE_COLUMNS.values() for c in columns}
    assert not source_columns & set(METADATA_COLUMNS)


def test_ingest_is_idempotent(monkeypatch, tmp_path):
    """Needs pyspark, delta-spark and Java; skipped when they are missing."""
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    from lakehouse.bronze.ingest import ingest
    from lakehouse.utils.spark import get_spark

    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    monkeypatch.setenv("LAKEHOUSE_DATA_ROOT", str(tmp_path))
    config = load_config()
    customers = generate_customers(5)
    write_csv(customers, CUSTOMER_COLUMNS, config.landing_path / "customers" / "customers.csv")
    batch = generate_daily_batch(DAY, [c["customer_id"] for c in customers],
                                 generate_products(3), orders_per_day=10, dirty_rate=0)
    write_batch(batch, config.landing_path)

    spark = get_spark("test-bronze", config)
    assert ingest(spark, config, "orders", DAY, "run-1") == 10
    # Re-running the same day replaces the partition instead of appending.
    assert ingest(spark, config, "orders", DAY, "run-2") == 10
    assert ingest(spark, config, "customers", DAY, "run-2") == 5

    table = spark.read.format("delta").load(config.table_path("bronze", "orders"))
    assert set(METADATA_COLUMNS) <= set(table.columns)
    assert {row["_run_id"] for row in table.select("_run_id").collect()} == {"run-2"}
