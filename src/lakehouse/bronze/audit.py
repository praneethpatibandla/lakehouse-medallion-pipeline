"""Ingestion audit log and the quarantine threshold check.

Every bronze load, successful or not, appends one row to an audit Delta table
(`<data_root>/audit/ingestion_log`). The table answers operational questions
without re-reading any data:

    - Did yesterday's orders load, and how many rows?
    - How many records were quarantined, and is that number growing?
    - Which run failed, when, and with what error?

The log is append-only: a rerun adds a new row with a new run id instead of
changing history. Records are built in plain Python so they can be tested
without Spark; only `write_audit_record` needs a SparkSession.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType

SUCCESS = "success"
FAILED = "failed"

# Default share of a batch that may be quarantined before the load is stopped.
# The generator corrupts well under 1% of lines; 5% means "something is
# really wrong with this file", not "a few bad lines".
DEFAULT_MAX_QUARANTINE_RATIO = 0.05


class QuarantineThresholdExceeded(RuntimeError):
    """Raised when too much of a batch is unparseable to trust the rest."""


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class AuditRecord:
    """One row of the ingestion audit log (one source, one batch, one run)."""

    run_id: str
    source: str
    batch_date: date
    source_path: str
    started_at: datetime = field(default_factory=_utc_now)
    finished_at: datetime | None = None
    status: str | None = None
    rows_read: int | None = None
    rows_loaded: int | None = None
    rows_quarantined: int | None = None
    error_message: str | None = None

    def succeed(self, rows_read: int, rows_loaded: int, rows_quarantined: int) -> "AuditRecord":
        self.rows_read = rows_read
        self.rows_loaded = rows_loaded
        self.rows_quarantined = rows_quarantined
        self.status = SUCCESS
        self.finished_at = _utc_now()
        return self

    def fail(self, error: BaseException) -> "AuditRecord":
        self.status = FAILED
        # Keep the message short enough to read in a table; the full
        # traceback is in the job log.
        self.error_message = f"{type(error).__name__}: {error}"[:1000]
        self.finished_at = _utc_now()
        return self

    @property
    def duration_seconds(self) -> float | None:
        if self.finished_at is None:
            return None
        return round((self.finished_at - self.started_at).total_seconds(), 3)

    def to_row(self) -> dict:
        """Return the record as a flat dict in audit-table column order."""
        if self.status is None:
            raise ValueError("Call succeed() or fail() before writing an audit record")
        row = asdict(self)
        row["duration_seconds"] = self.duration_seconds
        return {name: row[name] for name in AUDIT_COLUMNS}


AUDIT_COLUMNS = ("run_id", "source", "batch_date", "source_path", "status",
                 "rows_read", "rows_loaded", "rows_quarantined", "error_message",
                 "started_at", "finished_at", "duration_seconds")


def check_quarantine_threshold(source: str, rows_read: int, rows_quarantined: int,
                               max_ratio: float = DEFAULT_MAX_QUARANTINE_RATIO) -> float:
    """Return the quarantined share of a batch, or raise if it is too high.

    A handful of bad lines is normal and should not stop the pipeline. A file
    that is mostly garbage (wrong format, truncated upload) should: loading
    the few rows that happened to parse would publish a badly incomplete day.
    """
    if not 0 <= max_ratio <= 1:
        raise ValueError("max_ratio must be between 0 and 1")
    if rows_quarantined > rows_read:
        raise ValueError("rows_quarantined cannot be larger than rows_read")
    ratio = rows_quarantined / rows_read if rows_read else 0.0
    if ratio > max_ratio:
        raise QuarantineThresholdExceeded(
            f"{source}: {rows_quarantined} of {rows_read} rows ({ratio:.1%}) are corrupt, "
            f"above the limit of {max_ratio:.1%}. Bronze was not updated; see the quarantine table."
        )
    return ratio


def audit_schema() -> "StructType":
    """Spark schema of the audit table. Explicit, so an all-NULL column
    (e.g. error_message on a good day) still gets the right type."""
    from pyspark.sql.types import (DateType, DoubleType, LongType, StringType,
                                   StructField, StructType, TimestampType)

    types = {
        "run_id": StringType(), "source": StringType(), "batch_date": DateType(),
        "source_path": StringType(), "status": StringType(), "rows_read": LongType(),
        "rows_loaded": LongType(), "rows_quarantined": LongType(),
        "error_message": StringType(), "started_at": TimestampType(),
        "finished_at": TimestampType(), "duration_seconds": DoubleType(),
    }
    return StructType([StructField(name, types[name], True) for name in AUDIT_COLUMNS])


def write_audit_record(spark: "SparkSession", record: AuditRecord, table_path: str) -> None:
    """Append one record to the audit Delta table (created on first write)."""
    # A tuple in schema order avoids relying on dict-to-column name matching.
    row = tuple(record.to_row().values())
    df = spark.createDataFrame([row], schema=audit_schema())
    df.write.format("delta").mode("append").save(table_path)
