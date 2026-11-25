from datetime import date

import pytest

from data_generator.orders import generate_daily_batch, write_batch
from data_generator.reference_data import generate_products
from lakehouse.bronze.audit import (
    AUDIT_COLUMNS,
    FAILED,
    SUCCESS,
    AuditRecord,
    QuarantineThresholdExceeded,
    check_quarantine_threshold,
)

DAY = date(2026, 11, 1)


def _record() -> AuditRecord:
    return AuditRecord(run_id="run-1", source="orders", batch_date=DAY, source_path="/landing/orders")


def test_successful_record_row():
    row = _record().succeed(rows_read=101, rows_loaded=100, rows_quarantined=1).to_row()
    assert tuple(row) == AUDIT_COLUMNS
    assert row["status"] == SUCCESS
    assert row["rows_loaded"] == 100
    assert row["error_message"] is None
    assert row["duration_seconds"] >= 0


def test_failed_record_keeps_error_type_and_message():
    row = _record().fail(FileNotFoundError("no landing data")).to_row()
    assert row["status"] == FAILED
    assert row["error_message"] == "FileNotFoundError: no landing data"
    assert row["rows_loaded"] is None


def test_unfinished_record_cannot_be_written():
    with pytest.raises(ValueError):
        _record().to_row()


def test_error_message_is_truncated():
    row = _record().fail(RuntimeError("x" * 5000)).to_row()
    assert len(row["error_message"]) == 1000


@pytest.mark.parametrize("read,quarantined,expected", [(100, 0, 0.0), (100, 5, 0.05), (0, 0, 0.0)])
def test_threshold_allows_small_ratios(read, quarantined, expected):
    assert check_quarantine_threshold("orders", read, quarantined, max_ratio=0.05) == expected


def test_threshold_stops_mostly_corrupt_batches():
    with pytest.raises(QuarantineThresholdExceeded, match="orders"):
        check_quarantine_threshold("orders", rows_read=100, rows_quarantined=6, max_ratio=0.05)


@pytest.mark.parametrize("read,quarantined,ratio", [(10, 11, 0.05), (10, 1, 1.5), (10, 1, -0.1)])
def test_threshold_rejects_invalid_input(read, quarantined, ratio):
    with pytest.raises(ValueError):
        check_quarantine_threshold("orders", read, quarantined, max_ratio=ratio)


def test_corrupt_lines_are_quarantined_and_audited(spark, lake_config):
    from lakehouse.bronze.ingest import AUDIT_TABLE, ingest

    config = lake_config
    batch = generate_daily_batch(DAY, ["C00001"], generate_products(3), orders_per_day=10, dirty_rate=0)
    batch.corrupt_lines.append('{"order_id": "O20261101-000')  # truncated JSON
    write_batch(batch, config.landing_path)

    # 1 of 11 lines is corrupt (9%): over the default limit, so bronze is not written.
    with pytest.raises(QuarantineThresholdExceeded):
        ingest(spark, config, "orders", DAY, "run-1")
    assert ingest(spark, config, "orders", DAY, "run-2", max_quarantine_ratio=0.2) == 10

    quarantine = spark.read.format("delta").load(config.table_path("quarantine", "orders"))
    rows = quarantine.collect()
    assert len(rows) == 1  # rerun replaced run-1's quarantine row
    assert rows[0]["_corrupt_record"].startswith('{"order_id"')

    audit = spark.read.format("delta").load(config.table_path("audit", AUDIT_TABLE))
    by_run = {r["run_id"]: r for r in audit.collect()}
    assert by_run["run-1"]["status"] == FAILED
    assert by_run["run-2"]["status"] == SUCCESS
    assert (by_run["run-2"]["rows_read"], by_run["run-2"]["rows_quarantined"]) == (11, 1)
