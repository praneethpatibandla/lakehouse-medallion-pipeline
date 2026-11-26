from datetime import date, datetime

import pytest

from lakehouse.pipeline import (
    BRONZE_ORDER,
    DAILY_SOURCES,
    FAILED,
    RUN_COLUMNS,
    STAGE_GOLD,
    STAGE_SILVER,
    SUCCESS,
    RunPlan,
    RunRecord,
    complete_batch_dates,
    incomplete_batch_dates,
    landed_dates,
    late_batch_dates,
    missing_bronze_sources,
    parse_batch_folder,
    plan_run,
    processed_dates,
    watermark,
)

D1, D2, D3, D4 = (date(2026, 11, d) for d in (1, 2, 3, 4))


def test_parse_batch_folder():
    assert parse_batch_folder("batch_date=2026-11-01") == D1
    assert parse_batch_folder("batch_date=2026-13-01") is None
    assert parse_batch_folder("2026-11-01") is None
    assert parse_batch_folder("_temporary") is None


def test_landed_dates_ignores_empty_and_unrelated_folders(tmp_path):
    root = tmp_path / "orders"
    (root / "batch_date=2026-11-01").mkdir(parents=True)
    (root / "batch_date=2026-11-01" / "orders.json").write_text("{}\n")
    (root / "batch_date=2026-11-02").mkdir()                      # empty: still uploading
    (root / "batch_date=2026-11-03").mkdir()
    (root / "batch_date=2026-11-03" / "_COPYING_").write_text("")  # marker file only
    (root / "notes").mkdir()
    assert landed_dates(tmp_path, "orders") == {D1}
    assert landed_dates(tmp_path, "order_items") == set()


def test_only_dates_with_every_daily_source_are_complete():
    by_source = {"orders": {D1, D2, D3}, "order_items": {D1, D3}}
    assert complete_batch_dates(by_source) == [D1, D3]
    assert incomplete_batch_dates(by_source) == [D2]
    assert complete_batch_dates({}) == []


def test_watermark_uses_only_successful_rows_of_the_stage():
    rows = [(STAGE_SILVER, D1, SUCCESS), (STAGE_SILVER, D2, SUCCESS),
            (STAGE_SILVER, D3, FAILED), (STAGE_GOLD, D1, SUCCESS)]
    assert watermark(rows, STAGE_SILVER) == D2
    assert watermark(rows, STAGE_GOLD) == D1
    assert watermark([], STAGE_SILVER) is None
    assert processed_dates(rows) == {D1, D2}


def test_late_dates_are_reported():
    # D2 arrived after D3 had already been processed.
    assert late_batch_dates([D1, D2, D3, D4], {D1, D3}, D3) == [D2]
    assert late_batch_dates([D1], set(), None) == []


def test_first_run_processes_everything_and_builds_gold():
    assert plan_run([D2, D1, D3], None, None) == RunPlan((D1, D2, D3), True, D3)


def test_only_dates_after_the_watermark_are_processed():
    plan = plan_run([D1, D2, D3], silver_mark=D1, gold_mark=D1)
    assert plan.batch_dates == (D2, D3)
    assert plan.build_gold and plan.gold_through == D3


def test_until_and_max_batches_limit_the_run():
    assert plan_run([D1, D2, D3, D4], None, None, until=D3).batch_dates == (D1, D2, D3)
    plan = plan_run([D1, D2, D3, D4], None, None, max_batches=2)
    assert plan.batch_dates == (D1, D2) and plan.gold_through == D2
    with pytest.raises(ValueError):
        plan_run([D1], None, None, max_batches=0)


def test_nothing_new_means_nothing_to_do():
    assert plan_run([D1, D2], D2, D2) == RunPlan((), False, D2)
    assert plan_run([], None, None) == RunPlan((), False, None)


def test_failed_gold_is_retried_without_new_batches():
    # Silver reached D2 but the last gold build (or its checks) failed.
    plan = plan_run([D1, D2], silver_mark=D2, gold_mark=D1)
    assert plan.batch_dates == ()
    assert plan.build_gold and plan.gold_through == D2


def test_run_record_rows():
    ok = RunRecord("run-1", STAGE_SILVER, D1).succeed().to_row()
    assert len(ok) == len(RUN_COLUMNS)
    assert dict(zip(RUN_COLUMNS, ok, strict=True))["status"] == SUCCESS

    failed = dict(zip(RUN_COLUMNS, RunRecord("run-1", STAGE_GOLD, D1)
                      .fail(RuntimeError("x" * 5000)).to_row(), strict=True))
    assert failed["status"] == FAILED
    assert failed["error_message"].startswith("RuntimeError: ")
    assert len(failed["error_message"]) == 1000

    with pytest.raises(ValueError):
        RunRecord("run-1", STAGE_SILVER, D1).to_row()


def test_missing_bronze_sources_uses_newest_attempt():
    t1, t2 = datetime(2026, 11, 2, 6), datetime(2026, 11, 2, 7)
    rows = [(name, D1, SUCCESS, t1) for name in BRONZE_ORDER]
    assert missing_bronze_sources(rows, D1) == []
    # Nothing loaded for D2 yet: every source is missing, in load order.
    assert missing_bronze_sources(rows, D2) == list(BRONZE_ORDER)
    # orders loaded, then a re-run failed: orders is no longer ready.
    rows.append(("orders", D1, FAILED, t2))
    assert missing_bronze_sources(rows, D1) == ["orders"]
    # Row order does not matter, only started_at does.
    assert missing_bronze_sources(list(reversed(rows)), D1) == ["orders"]


def test_reference_data_loads_before_daily_sources():
    assert set(BRONZE_ORDER) == set(DAILY_SOURCES) | {"customers", "products"}
    assert BRONZE_ORDER.index("products") < BRONZE_ORDER.index("order_items")


# ---------------------------------------------------------------------------
# Spark tests: the whole pipeline on a tiny lake. They use the shared `spark`
# fixture (tests/conftest.py) and are skipped without pyspark / delta / Java.
# ---------------------------------------------------------------------------

def _land_reference_data(config):
    from data_generator.reference_data import (CUSTOMER_COLUMNS, PRODUCT_COLUMNS,
                                               generate_customers, generate_products, write_csv)
    customers, products = generate_customers(20), generate_products(10)
    write_csv(customers, CUSTOMER_COLUMNS, config.landing_path / "customers" / "customers.csv")
    write_csv(products, PRODUCT_COLUMNS, config.landing_path / "products" / "products.csv")
    return [c["customer_id"] for c in customers], products


def _land_day(config, day, customer_ids, products, corrupt_lines=0):
    from data_generator.orders import generate_daily_batch, write_batch
    # dirty_rate=0 keeps every data quality error check green, so the test
    # is about the runner, not about the generator's random defects.
    batch = generate_daily_batch(day, customer_ids, products, orders_per_day=15, dirty_rate=0)
    batch.corrupt_lines.extend(['{"order_id": "broken'] * corrupt_lines)
    write_batch(batch, config.landing_path)


def _run(spark, config):
    """What `python -m lakehouse.pipeline` does, minus the CLI."""
    from lakehouse.pipeline import read_run_state, run_pipeline

    by_source = {name: landed_dates(config.landing_path, name) for name in DAILY_SOURCES}
    state = read_run_state(spark, config)
    plan = plan_run(complete_batch_dates(by_source),
                    watermark(state, STAGE_SILVER), watermark(state, STAGE_GOLD))
    run_pipeline(spark, config, plan, run_id=f"run-{len(state)}")
    return plan


def _count(spark, config, layer, table):
    return spark.read.format("delta").load(config.table_path(layer, table)).count()


def test_pipeline_processes_only_new_batches(spark, lake_config):
    from lakehouse.pipeline import read_run_state

    config = lake_config
    customer_ids, products = _land_reference_data(config)
    for day in (D1, D2):
        _land_day(config, day, customer_ids, products)

    first = _run(spark, config)
    assert first.batch_dates == (D1, D2) and first.build_gold
    assert _count(spark, config, "silver", "orders") == 30
    fact_rows = _count(spark, config, "gold", "fact_sales")
    assert fact_rows > 0

    # Nothing new landed: the second run does nothing at all.
    assert _run(spark, config) == RunPlan((), False, D2)
    assert _count(spark, config, "gold", "fact_sales") == fact_rows

    # A new day arrives: only that day is loaded, and gold is rebuilt.
    _land_day(config, D3, customer_ids, products)
    third = _run(spark, config)
    assert third.batch_dates == (D3,) and third.gold_through == D3
    assert _count(spark, config, "silver", "orders") == 45
    assert _count(spark, config, "gold", "fact_sales") > fact_rows

    state = read_run_state(spark, config)
    assert watermark(state, STAGE_SILVER) == D3 and watermark(state, STAGE_GOLD) == D3
    assert all(status == SUCCESS for _, _, status in state)


def test_failed_batch_stops_the_run_and_is_retried(spark, lake_config):
    from lakehouse.bronze.audit import QuarantineThresholdExceeded
    from lakehouse.pipeline import read_run_state

    config = lake_config
    customer_ids, products = _land_reference_data(config)
    _land_day(config, D1, customer_ids, products)
    _land_day(config, D2, customer_ids, products, corrupt_lines=5)  # 25% corrupt
    _land_day(config, D3, customer_ids, products)

    with pytest.raises(QuarantineThresholdExceeded):
        _run(spark, config)
    state = read_run_state(spark, config)
    assert watermark(state, STAGE_SILVER) == D1        # D1 kept, D3 never attempted
    assert watermark(state, STAGE_GOLD) is None        # gold not built from a partial run
    assert (STAGE_SILVER, D2, FAILED) in state
    assert D3 not in processed_dates(state)

    # The source re-sends a clean file; the next run continues from D2.
    _land_day(config, D2, customer_ids, products)
    retry = _run(spark, config)
    assert retry.batch_dates == (D2, D3) and retry.build_gold
    assert _count(spark, config, "silver", "orders") == 45
    assert watermark(read_run_state(spark, config), STAGE_GOLD) == D3
