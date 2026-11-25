from datetime import date, datetime

import pytest

from lakehouse.bronze.schemas import CUSTOMERS
from lakehouse.silver.customers import (
    KEY,
    SCD_COLUMNS,
    TRACKED_COLUMNS,
    OutOfOrderBatchError,
    check_batch_order,
)

DAY1, DAY2, DAY3 = date(2026, 11, 1), date(2026, 11, 2), date(2026, 11, 3)


def test_tracked_columns_are_every_attribute_except_the_key():
    assert KEY not in TRACKED_COLUMNS
    assert set(TRACKED_COLUMNS) | {KEY} == set(CUSTOMERS)
    # Pipeline metadata must never be tracked, or every load makes a new version.
    assert not any(c.startswith("_") for c in TRACKED_COLUMNS)


def test_scd_columns_do_not_clash_with_source_columns():
    assert not set(SCD_COLUMNS) & set(CUSTOMERS)


def test_check_batch_order_allows_first_load_same_day_and_later_days():
    check_batch_order(DAY1, None)
    check_batch_order(DAY2, DAY2)
    check_batch_order(DAY3, DAY2)


def test_check_batch_order_rejects_older_snapshots():
    with pytest.raises(OutOfOrderBatchError):
        check_batch_order(DAY1, DAY2)


# ---------------------------------------------------------------------------
# Spark + Delta tests: skipped when pyspark / delta-spark / Java are missing.
# ---------------------------------------------------------------------------

def _snapshot(spark, rows, batch_date):
    """Bronze-shaped customers snapshot, prepared for the SCD2 merge."""
    from pyspark.sql import functions as F

    from lakehouse.bronze.schemas import spark_schema
    from lakehouse.silver.customers import prepare_snapshot

    bronze = (spark.createDataFrame(rows, schema=spark_schema("customers"))
              .withColumn("_source_file", F.lit("customers.csv"))
              .withColumn("_ingested_at", F.lit(datetime(2026, 11, 3)))
              .withColumn("_batch_date", F.lit(batch_date))
              .withColumn("_run_id", F.lit(f"run-{batch_date}")))
    snapshot, _ = prepare_snapshot(bronze, batch_date)
    return snapshot


ADA = ("C00001", "Ada", "Lovelace", "Portland", "OR", "US", "consumer", "2024-03-05")
ALAN = ("C00002", "Alan", "Turing", "Seattle", "WA", "US", "corporate", "2024-04-01")


def test_scd2_keeps_history_and_reruns_are_no_ops(spark, tmp_path):
    from lakehouse.silver.customers import apply_scd2

    path = str(tmp_path / "silver_customers")
    first = apply_scd2(spark, _snapshot(spark, [ADA, ALAN], DAY1), path, DAY1)
    assert (first.inserted, first.updated) == (2, 0)

    # Day 2: Ada moved to Denver, Alan unchanged (messy case must not count
    # as a change), Grace is new.
    ada_moved = ADA[:3] + ("Denver", "CO") + ADA[5:]
    alan_messy = ("c00002", " alan ", "TURING") + ALAN[3:]
    grace = ("C00003", "Grace", "Hopper", "Boston", "MA", "US", "consumer", "2024-05-01")
    day2 = _snapshot(spark, [ada_moved, alan_messy, grace], DAY2)
    second = apply_scd2(spark, day2, path, DAY2)
    assert (second.inserted, second.updated) == (2, 1)

    # Same snapshot again: nothing changes.
    rerun = apply_scd2(spark, day2, path, DAY2)
    assert (rerun.inserted, rerun.updated) == (0, 0)

    rows = spark.read.format("delta").load(path).collect()
    assert len(rows) == 4
    ada = sorted((r for r in rows if r["customer_id"] == "C00001"), key=lambda r: r["valid_from"])
    assert [(r["city"], r["is_current"], r["valid_to"]) for r in ada] == [
        ("Portland", False, DAY2), ("Denver", True, None)]
    assert sum(r["is_current"] for r in rows) == 3

    with pytest.raises(OutOfOrderBatchError):
        apply_scd2(spark, _snapshot(spark, [ADA], DAY1), path, DAY1)
