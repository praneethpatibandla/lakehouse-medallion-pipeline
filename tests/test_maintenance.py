"""Tests for the table maintenance job (step 15)."""

import pytest

from lakehouse.maintenance import (
    MIN_RETAIN_HOURS,
    TABLES,
    OptimizeStats,
    TableMaintenance,
    check_zorder_columns,
    optimize_sql,
    parse_optimize_metrics,
    select_tables,
    vacuum_sql,
    validate_retention,
)

# ---------------------------------------------------------------------------
# Pure Python
# ---------------------------------------------------------------------------


def test_optimize_sql_with_and_without_zorder():
    assert optimize_sql("/lake/gold/dim_date") == "OPTIMIZE delta.`/lake/gold/dim_date`"
    assert (optimize_sql("/lake/gold/fact_sales", ("date_key", "product_key"))
            == "OPTIMIZE delta.`/lake/gold/fact_sales` ZORDER BY (`date_key`, `product_key`)")


def test_vacuum_sql_and_dry_run():
    assert vacuum_sql("/t", 168) == "VACUUM delta.`/t` RETAIN 168 HOURS"
    assert vacuum_sql("/t", 240, dry_run=True) == "VACUUM delta.`/t` RETAIN 240 HOURS DRY RUN"


def test_retention_below_seven_days_is_refused():
    assert validate_retention(MIN_RETAIN_HOURS) == 168
    with pytest.raises(ValueError, match="at least 168"):
        validate_retention(0)
    with pytest.raises(ValueError):
        vacuum_sql("/t", 24)


def test_zorder_columns_must_exist_and_not_be_partition_columns():
    cols = ["order_id", "date_key", "month_key"]
    check_zorder_columns(["order_id", "date_key"], cols, ["month_key"])  # fine
    with pytest.raises(ValueError, match="not in the table"):
        check_zorder_columns(["nope"], cols, [])
    with pytest.raises(ValueError, match="partition columns"):
        check_zorder_columns(["month_key"], cols, ["month_key"])


def test_parse_optimize_metrics_handles_strings_and_missing_values():
    metrics = {"numRemovedFiles": "12", "numAddedFiles": "1", "numRemovedBytes": "4096"}
    assert parse_optimize_metrics(metrics) == OptimizeStats(12, 1, 4096, 0)
    assert parse_optimize_metrics(None) == OptimizeStats(0, 0, 0, 0)


def test_select_tables():
    assert select_tables(None) == list(TABLES)
    picked = select_tables(["gold.fact_sales", "silver.orders"])
    assert [t.name for t in picked] == ["gold.fact_sales", "silver.orders"]
    with pytest.raises(ValueError, match="Unknown tables"):
        select_tables(["gold.nope"])


def test_table_specs_are_unique_and_fact_is_not_zordered_on_its_partition():
    from lakehouse.gold.facts import FACT_COLUMNS, FACT_PARTITION_COLUMNS

    assert len({t.name for t in TABLES}) == len(TABLES)
    fact = select_tables(["gold.fact_sales"])[0]
    check_zorder_columns(fact.zorder_by, FACT_COLUMNS, FACT_PARTITION_COLUMNS)


# ---------------------------------------------------------------------------
# Spark tests: skipped when pyspark / delta-spark / Java are missing.
# ---------------------------------------------------------------------------

@pytest.fixture
def lake(spark, lake_config):
    """The shared session plus an empty lake folder for each test."""
    return spark, lake_config


def _write_small_files(spark, path, batches=5):
    """Append several tiny batches: the small-file problem in miniature."""
    for i in range(batches):
        (spark.createDataFrame([(f"O{i}", 202611, i)], "order_id string, month_key int, n int")
         .write.format("delta").mode("append").partitionBy("month_key").save(path))


def test_optimize_compacts_small_files_and_vacuum_dry_run_lists_nothing_new(lake):
    from lakehouse.maintenance import optimize_table, vacuum_table

    spark, config = lake
    path = config.table_path("silver", "orders")
    _write_small_files(spark, path)

    stats = optimize_table(spark, path, ("order_id",))
    assert stats.files_removed == 5 and stats.files_added == 1
    assert spark.read.format("delta").load(path).count() == 5

    # Running it again finds nothing to do and reports zeros.
    assert optimize_table(spark, path) == OptimizeStats(0, 0)
    # The compacted-away files are younger than 7 days, so nothing is deletable yet.
    assert vacuum_table(spark, path, MIN_RETAIN_HOURS, dry_run=True) == 0


def test_zorder_on_partition_column_is_rejected(lake):
    from lakehouse.maintenance import optimize_table

    spark, config = lake
    path = config.table_path("gold", "partitioned")
    _write_small_files(spark, path, batches=1)
    with pytest.raises(ValueError, match="partition columns"):
        optimize_table(spark, path, ("month_key",))


def test_run_maintenance_skips_missing_tables(lake):
    from lakehouse.maintenance import run_maintenance

    spark, config = lake
    _write_small_files(spark, config.table_path("silver", "orders"), batches=2)
    specs = [TableMaintenance("silver", "orders", ("order_id",)),
             TableMaintenance("gold", "does_not_exist")]
    results = run_maintenance(spark, config, specs, vacuum_dry_run=True)
    assert set(results) == {"silver.orders"}
