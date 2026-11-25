from datetime import date

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
    assert dict(zip(RUN_COLUMNS, ok))["status"] == SUCCESS

    failed = dict(zip(RUN_COLUMNS, RunRecord("run-1", STAGE_GOLD, D1)
                      .fail(RuntimeError("x" * 5000)).to_row()))
    assert failed["status"] == FAILED
    assert failed["error_message"].startswith("RuntimeError: ")
    assert len(failed["error_message"]) == 1000

    with pytest.raises(ValueError):
        RunRecord("run-1", STAGE_SILVER, D1).to_row()


def test_reference_data_loads_before_daily_sources():
    assert set(BRONZE_ORDER) == set(DAILY_SOURCES) | {"customers", "products"}
    assert BRONZE_ORDER.index("products") < BRONZE_ORDER.index("order_items")
