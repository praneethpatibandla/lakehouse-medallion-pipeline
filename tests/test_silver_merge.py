from datetime import date, datetime

import pytest

from lakehouse.silver.merge import (
    MergeStats,
    merge_condition,
    newer_condition,
    parse_merge_metrics,
)


def test_merge_condition_single_and_composite_keys():
    assert merge_condition(["order_id"]) == "t.`order_id` = s.`order_id`"
    assert merge_condition(("order_id", "line_number")) == (
        "t.`order_id` = s.`order_id` AND t.`line_number` = s.`line_number`")


def test_merge_condition_needs_a_key():
    with pytest.raises(ValueError):
        merge_condition([])


def test_newer_condition_ignores_older_rows_but_allows_reruns():
    cond = newer_condition("updated_at")
    assert cond == "t.`updated_at` IS NULL OR s.`updated_at` >= t.`updated_at`"


def test_parse_merge_metrics_converts_strings_and_defaults_to_zero():
    stats = parse_merge_metrics({"numSourceRows": "12", "numTargetRowsInserted": "10",
                                 "numTargetRowsUpdated": "2"})
    assert stats == MergeStats(source_rows=12, inserted=10, updated=2, deleted=0)
    assert parse_merge_metrics(None) == MergeStats(0, 0, 0, 0)


# ---------------------------------------------------------------------------
# Spark + Delta tests: skipped when pyspark / delta-spark / Java are missing.
# ---------------------------------------------------------------------------

DAY1, DAY2 = date(2026, 11, 1), date(2026, 11, 2)


def _bronze_orders(spark, rows, batch_date):
    """Build a bronze-shaped DataFrame: all-string source columns + metadata."""
    from pyspark.sql import functions as F

    from lakehouse.bronze.schemas import spark_schema

    return (spark.createDataFrame(rows, schema=spark_schema("orders"))
            .withColumn("_source_file", F.lit("test.json"))
            .withColumn("_ingested_at", F.lit(datetime(2026, 11, 3)))
            .withColumn("_batch_date", F.lit(batch_date))
            .withColumn("_run_id", F.lit(f"run-{batch_date}")))


def test_prepare_orders_keeps_newest_copy_and_rejects_null_keys(spark):
    from lakehouse.silver.orders import prepare_orders

    bronze = _bronze_orders(spark, [
        ("O1", "C1", "2026-11-01T10:00:00", "placed", "card", "USD", "2026-11-01T10:00:00"),
        ("O1", "C1", "2026-11-01T10:00:00", "shipped", "card", "USD", "2026-11-01T18:00:00"),
        (None, "C2", "2026-11-01T11:00:00", "placed", "card", "USD", "2026-11-01T11:00:00"),
    ], DAY1)
    staged, rejected = prepare_orders(bronze)
    rows = staged.collect()
    assert [(r["order_id"], r["status"]) for r in rows] == [("O1", "shipped")]
    assert rejected.count() == 1


def test_upsert_inserts_updates_and_ignores_older_rows(spark, tmp_path):
    from lakehouse.silver.merge import upsert
    from lakehouse.silver.orders import KEYS, SEQUENCE_COLUMN, prepare_orders

    path = str(tmp_path / "silver_orders")

    def load(rows, day):
        staged, _ = prepare_orders(_bronze_orders(spark, rows, day))
        return upsert(spark, staged, path, KEYS, SEQUENCE_COLUMN)

    first = load([
        ("O1", "C1", "2026-11-01T10:00:00", "placed", "card", "USD", "2026-11-01T10:00:00"),
        ("O2", "C2", "2026-11-01T11:00:00", "placed", "card", "USD", "2026-11-01T11:00:00"),
    ], DAY1)
    assert (first.inserted, first.updated) == (2, 0)

    # Day 2: O1 shipped (newer), O3 is new.
    second = load([
        ("O1", "C1", "2026-11-01T10:00:00", "shipped", "card", "USD", "2026-11-02T09:00:00"),
        ("O3", "C3", "2026-11-02T08:00:00", "placed", "card", "USD", "2026-11-02T08:00:00"),
    ], DAY2)
    assert (second.inserted, second.updated) == (1, 1)

    # Day 1 replayed late: its older O1 copy must not undo the "shipped" status.
    load([("O1", "C1", "2026-11-01T10:00:00", "placed", "card", "USD", "2026-11-01T10:00:00")], DAY1)

    table = spark.read.format("delta").load(path)
    statuses = {r["order_id"]: r["status"] for r in table.collect()}
    assert statuses == {"O1": "shipped", "O2": "placed", "O3": "placed"}
