"""End-to-end pipeline runner with incremental watermarks.

Until now each layer was run by hand, one module and one date at a time.
This runner does a whole run in one command:

    1. Find the batch dates waiting in the landing zone.
    2. Compare them with the watermark (the newest date already processed)
       and keep only the new ones.
    3. For each new date, oldest first: bronze ingestion of every source,
       then the four silver loads. The watermark moves forward after each
       date, so a failure on day 3 keeps days 1 and 2.
    4. Once all dates are in silver: rebuild gold (dimensions, fact,
       aggregates) and run the data quality suites.
    5. Optionally, OPTIMIZE / VACUUM the tables.

State is kept in an append-only Delta table, `<data_root>/audit/pipeline_runs`,
with one row per stage attempt:

    stage = "silver"   bronze + silver for one batch date
    stage = "gold"     gold rebuild + data quality, up to a batch date

The silver watermark is the newest batch date with a successful "silver"
row; the gold watermark is the newest date gold was successfully built up
to. Keeping them separate means a gold or quality failure is retried by the
next run even when there are no new landing files.

Usage (generate data first, see README):
    python -m lakehouse.pipeline                    # process everything new
    python -m lakehouse.pipeline --dry-run          # only show the plan
    python -m lakehouse.pipeline --until 2026-11-02 --max-batches 1
    python -m lakehouse.pipeline --maintenance      # also OPTIMIZE / VACUUM
"""

from __future__ import annotations

import argparse
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Sequence

from lakehouse.bronze.audit import DEFAULT_MAX_QUARANTINE_RATIO
from lakehouse.bronze.audit import SUCCESS as INGEST_SUCCESS
from lakehouse.bronze.ingest import AUDIT_TABLE, SOURCES, ingest
from lakehouse.gold.aggregates import build_aggregates
from lakehouse.gold.dimensions import build_dimensions
from lakehouse.gold.facts import load_fact_sales
from lakehouse.maintenance import run_maintenance
from lakehouse.quality.suites import run_suites
from lakehouse.silver.customers import load_silver_customers
from lakehouse.silver.order_items import load_silver_order_items
from lakehouse.silver.orders import load_silver_orders
from lakehouse.silver.products import load_silver_products
from lakehouse.utils.config import PipelineConfig, load_config
from lakehouse.utils.logger import get_logger
from lakehouse.utils.tables import read_table, save_table, table_exists

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType

logger = get_logger(__name__)

RUNS_TABLE = "pipeline_runs"
STAGE_SILVER = "silver"
STAGE_GOLD = "gold"
SUCCESS = "success"
FAILED = "failed"

# Reference data first: silver order items are checked against silver
# products and orders, so those must be loaded before them.
BRONZE_ORDER = ("customers", "products", "orders", "order_items")
# Only sources that arrive as one folder per day decide which dates exist.
DAILY_SOURCES = tuple(name for name, source in SOURCES.items() if source.daily)
BATCH_FOLDER_PREFIX = "batch_date="

RUN_COLUMNS = ("run_id", "stage", "batch_date", "status", "error_message",
               "started_at", "finished_at")


# --------------------------------------------------------------------------
# Pure Python: discovering batches and planning a run
# --------------------------------------------------------------------------

def parse_batch_folder(name: str) -> date | None:
    """Return the date of a `batch_date=YYYY-MM-DD` folder name, else None."""
    if not name.startswith(BATCH_FOLDER_PREFIX):
        return None
    try:
        return date.fromisoformat(name[len(BATCH_FOLDER_PREFIX):])
    except ValueError:
        return None


def _has_data_files(folder: Path) -> bool:
    # Files starting with "." or "_" are markers or partial uploads, not data.
    return any(p.is_file() and not p.name.startswith((".", "_")) for p in folder.iterdir())


def landed_dates(landing_path: Path, source: str) -> set[date]:
    """Return the batch dates that have data files for one daily source."""
    root = landing_path / source
    if not root.is_dir():
        return set()
    dates = set()
    for folder in root.iterdir():
        day = parse_batch_folder(folder.name)
        if folder.is_dir() and day is not None and _has_data_files(folder):
            dates.add(day)
    return dates


def complete_batch_dates(dates_by_source: dict[str, set[date]]) -> list[date]:
    """Return, oldest first, the dates that every daily source has delivered.

    A date where orders arrived but order items did not is incomplete:
    loading it would publish orders without lines. It is picked up by a
    later run once the missing files are there.
    """
    if not dates_by_source:
        return []
    return sorted(set.intersection(*dates_by_source.values()))


def incomplete_batch_dates(dates_by_source: dict[str, set[date]]) -> list[date]:
    """Return the dates that only some daily sources have delivered."""
    if not dates_by_source:
        return []
    every = set.union(*dates_by_source.values())
    return sorted(every - set(complete_batch_dates(dates_by_source)))


def watermark(rows: Iterable[tuple[str, date, str]], stage: str) -> date | None:
    """Return the newest batch date with a successful row for `stage`.

    `rows` are (stage, batch_date, status) tuples from the runs table. Failed
    attempts never move the watermark, so the next run retries them.
    """
    dates = [day for row_stage, day, status in rows
             if row_stage == stage and status == SUCCESS]
    return max(dates, default=None)


def processed_dates(rows: Iterable[tuple[str, date, str]], stage: str = STAGE_SILVER) -> set[date]:
    """Return every batch date with a successful row for `stage`."""
    return {day for row_stage, day, status in rows if row_stage == stage and status == SUCCESS}


def late_batch_dates(available: Iterable[date], processed: set[date],
                     mark: date | None) -> list[date]:
    """Return dates that landed after the watermark had already passed them.

    A watermark only looks forward, so a day that arrives late (2 Nov landing
    after 3 Nov was processed) is never picked up automatically. That is the
    price of a simple watermark; the runner reports such dates so they can be
    backfilled on purpose instead of being lost silently.
    """
    if mark is None:
        return []
    return sorted(d for d in available if d <= mark and d not in processed)


def missing_bronze_sources(audit_rows: Iterable[tuple[str, date, str, datetime]],
                           batch_date: date) -> list[str]:
    """Return the sources whose bronze load for `batch_date` is not done.

    `audit_rows` are (source, batch_date, status, started_at) rows from the
    ingestion log. Only the newest attempt per source counts: a source that
    loaded once and then failed on a re-run is not ready. Used by the
    Databricks silver notebook, which runs separately from bronze and must
    not merge (and mark as done) a day that bronze never loaded.
    """
    latest: dict[str, tuple[datetime, str]] = {}
    for source, day, status, started_at in audit_rows:
        if day == batch_date and (source not in latest or started_at > latest[source][0]):
            latest[source] = (started_at, status)
    return [name for name in BRONZE_ORDER
            if name not in latest or latest[name][1] != INGEST_SUCCESS]


@dataclass(frozen=True)
class RunPlan:
    """What one run will do."""

    batch_dates: tuple[date, ...]
    build_gold: bool
    # The batch date gold will be built up to (the silver watermark after
    # this run's batches), or None when there is nothing in silver yet.
    gold_through: date | None


def plan_run(available: Sequence[date], silver_mark: date | None, gold_mark: date | None,
             until: date | None = None, max_batches: int | None = None) -> RunPlan:
    """Decide which batch dates to process and whether to rebuild gold.

    New dates are the ones after the silver watermark (and not after `until`),
    oldest first. `max_batches` caps how many one run takes, which keeps a
    long backlog from turning into one huge, all-or-nothing run.
    Gold is rebuilt when silver will be ahead of it after this run.
    """
    if max_batches is not None and max_batches < 1:
        raise ValueError("max_batches must be at least 1")
    pending = [d for d in sorted(set(available))
               if (silver_mark is None or d > silver_mark) and (until is None or d <= until)]
    if max_batches is not None:
        pending = pending[:max_batches]

    gold_through = pending[-1] if pending else silver_mark
    build_gold = gold_through is not None and (gold_mark is None or gold_mark < gold_through)
    return RunPlan(tuple(pending), build_gold, gold_through)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class RunRecord:
    """One row of the runs table: one stage attempt in one run."""

    run_id: str
    stage: str
    batch_date: date
    started_at: datetime = field(default_factory=_utc_now)
    finished_at: datetime | None = None
    status: str | None = None
    error_message: str | None = None

    def succeed(self) -> "RunRecord":
        self.status = SUCCESS
        self.finished_at = _utc_now()
        return self

    def fail(self, error: BaseException) -> "RunRecord":
        self.status = FAILED
        self.error_message = f"{type(error).__name__}: {error}"[:1000]
        self.finished_at = _utc_now()
        return self

    def to_row(self) -> tuple:
        """Return the record as a tuple in runs-table column order."""
        if self.status is None:
            raise ValueError("Call succeed() or fail() before writing a run record")
        return tuple(getattr(self, name) for name in RUN_COLUMNS)


# --------------------------------------------------------------------------
# Spark: reading and writing run state, running the stages
# --------------------------------------------------------------------------

def runs_schema() -> "StructType":
    from pyspark.sql.types import DateType, StringType, StructField, StructType, TimestampType

    types = {"run_id": StringType(), "stage": StringType(), "batch_date": DateType(),
             "status": StringType(), "error_message": StringType(),
             "started_at": TimestampType(), "finished_at": TimestampType()}
    return StructType([StructField(name, types[name], True) for name in RUN_COLUMNS])


def read_run_state(spark: "SparkSession", config: PipelineConfig) -> list[tuple[str, date, str]]:
    """Return (stage, batch_date, status) for every recorded attempt.

    The table holds a few rows per day, so collecting it to the driver is
    fine and keeps the watermark logic in plain, tested Python.
    """
    path = config.table_path("audit", RUNS_TABLE)
    if not table_exists(spark, path):
        return []  # first run ever: no watermark yet
    rows = read_table(spark, path).select("stage", "batch_date", "status").collect()
    return [(r["stage"], r["batch_date"], r["status"]) for r in rows]


def read_ingestion_log(spark: "SparkSession", config: PipelineConfig
                       ) -> list[tuple[str, date, str, datetime]]:
    """Return (source, batch_date, status, started_at) for every bronze load."""
    path = config.table_path("audit", AUDIT_TABLE)
    if not table_exists(spark, path):
        return []
    rows = read_table(spark, path).select("source", "batch_date", "status", "started_at").collect()
    return [(r["source"], r["batch_date"], r["status"], r["started_at"]) for r in rows]


def write_run_record(spark: "SparkSession", config: PipelineConfig, record: RunRecord) -> None:
    """Append one record to the runs table (created on first write)."""
    df = spark.createDataFrame([record.to_row()], schema=runs_schema())
    save_table(df.write.format("delta").mode("append"), config.table_path("audit", RUNS_TABLE))


def process_batch(spark: "SparkSession", config: PipelineConfig, batch_date: date,
                  run_id: str, max_quarantine_ratio: float = DEFAULT_MAX_QUARANTINE_RATIO) -> None:
    """Load one batch date into bronze and then silver.

    Every step is idempotent on its own (partition replace in bronze, MERGE
    in silver), so retrying a date after a failure halfway is safe.
    """
    load_bronze_batch(spark, config, batch_date, run_id, max_quarantine_ratio)
    load_silver_batch(spark, config, batch_date)


def load_bronze_batch(spark: "SparkSession", config: PipelineConfig, batch_date: date,
                      run_id: str, max_quarantine_ratio: float = DEFAULT_MAX_QUARANTINE_RATIO) -> None:
    """Ingest every source of one batch date into bronze."""
    for name in BRONZE_ORDER:
        ingest(spark, config, name, batch_date, run_id, max_quarantine_ratio)


def load_silver_batch(spark: "SparkSession", config: PipelineConfig, batch_date: date) -> None:
    """Load one batch date from bronze into the four silver tables."""
    load_silver_customers(spark, config, batch_date)
    load_silver_products(spark, config, batch_date)
    load_silver_orders(spark, config, batch_date)
    load_silver_order_items(spark, config, batch_date)


def build_gold_layer(spark: "SparkSession", config: PipelineConfig) -> None:
    """Rebuild gold from the whole of silver, then run the quality checks.

    Gold is small enough to rebuild in full, which is simpler and always
    consistent with silver; it is done once per run, not once per date.
    The checks run before the gold stage is marked successful, so a
    failed check keeps the gold watermark where it was.
    """
    build_dimensions(spark, config)
    load_fact_sales(spark, config)
    build_aggregates(spark, config)
    run_suites(spark, config)


def run_stage(spark: "SparkSession", config: PipelineConfig, record: RunRecord, action) -> None:
    """Run one stage and record its outcome, re-raising any failure."""
    try:
        action()
    except Exception as exc:
        write_run_record(spark, config, record.fail(exc))
        logger.error("Run %s: %s stage for %s failed: %s",
                     record.run_id, record.stage, record.batch_date, exc)
        raise
    write_run_record(spark, config, record.succeed())


def run_pipeline(spark: "SparkSession", config: PipelineConfig, plan: RunPlan, run_id: str,
                 max_quarantine_ratio: float = DEFAULT_MAX_QUARANTINE_RATIO,
                 maintenance: bool = False) -> None:
    """Execute a plan. Stops at the first failed date.

    Later dates are not attempted after a failure: the customers SCD2 table
    must be built in date order, and the watermark must never skip a gap.
    """
    for batch_date in plan.batch_dates:
        logger.info("Run %s: loading batch %s into bronze and silver", run_id, batch_date)
        # `day=batch_date` binds the current date now. The lambda is called
        # straight away, but a plain closure over a loop variable is a classic
        # bug if it ever gets called later (it would see the last date).
        run_stage(spark, config, RunRecord(run_id, STAGE_SILVER, batch_date),
                   lambda day=batch_date: process_batch(spark, config, day, run_id,
                                                        max_quarantine_ratio))

    if plan.build_gold:
        logger.info("Run %s: rebuilding gold through %s", run_id, plan.gold_through)
        run_stage(spark, config, RunRecord(run_id, STAGE_GOLD, plan.gold_through),
                   lambda: build_gold_layer(spark, config))

    if maintenance:
        # Not part of the watermark: skipping or failing it never makes data wrong.
        run_maintenance(spark, config)


def plan_pending_run(spark: "SparkSession", config: PipelineConfig, until: date | None = None,
                     max_batches: int | None = None) -> RunPlan:
    """Look at the landing zone and the watermarks, and plan the next run.

    Used by the command line below and by the Databricks notebooks, so both
    pick the same dates.
    """
    by_source = {name: landed_dates(config.landing_path, name) for name in DAILY_SOURCES}
    available = complete_batch_dates(by_source)
    for day in incomplete_batch_dates(by_source):
        logger.warning("Batch %s is incomplete (not every source has landed); skipped", day)

    state = read_run_state(spark, config)
    silver_mark = watermark(state, STAGE_SILVER)
    gold_mark = watermark(state, STAGE_GOLD)
    for day in late_batch_dates(available, processed_dates(state), silver_mark):
        logger.warning("Batch %s landed after the watermark (%s) passed it and will not be "
                       "processed automatically; backfill it on purpose", day, silver_mark)

    plan = plan_run(available, silver_mark, gold_mark, until, max_batches)
    logger.info("Watermarks: silver=%s gold=%s. Plan: %s",
                silver_mark, gold_mark, describe_plan(plan))
    return plan


def describe_plan(plan: RunPlan) -> str:
    dates = ", ".join(d.isoformat() for d in plan.batch_dates) or "none"
    gold = f"rebuild through {plan.gold_through}" if plan.build_gold else "up to date"
    return f"new batches: {dates}; gold: {gold}"


def main(argv: list[str] | None = None) -> None:
    from lakehouse.utils.spark import get_spark

    parser = argparse.ArgumentParser(description="Run the pipeline for every new batch date.")
    parser.add_argument("--until", type=date.fromisoformat,
                        help="do not process batch dates after this one")
    parser.add_argument("--max-batches", type=int, help="process at most this many dates")
    parser.add_argument("--max-quarantine-ratio", type=float, default=DEFAULT_MAX_QUARANTINE_RATIO)
    parser.add_argument("--maintenance", action="store_true",
                        help="run OPTIMIZE / VACUUM at the end")
    parser.add_argument("--dry-run", action="store_true",
                        help="print the plan and change nothing")
    args = parser.parse_args(argv)

    config = load_config()
    spark = get_spark("pipeline", config)
    try:
        plan = plan_pending_run(spark, config, args.until, args.max_batches)
        if args.dry_run:
            return
        if not plan.batch_dates and not plan.build_gold and not args.maintenance:
            logger.info("Nothing to do")
            return
        run_id = uuid.uuid4().hex
        run_pipeline(spark, config, plan, run_id, args.max_quarantine_ratio, args.maintenance)
        logger.info("Run %s finished", run_id)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
