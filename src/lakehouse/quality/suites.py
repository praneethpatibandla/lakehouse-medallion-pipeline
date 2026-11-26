"""The data quality rules for each table, and the job that runs them.

Silver keeps imperfect rows on purpose (they are labelled in `_dq_issues`),
so most silver rules are warnings with a tolerance: a few flagged rows are
normal, a sudden jump is worth a look. Gold is what people report from, so
its structural rules (keys, joins, amounts) are errors with no tolerance.

Every result is appended to the audit table `audit/dq_results`, so the
history of each check can be queried ("since when has this warning been
failing?"). After all checks have run, the job fails if any error-severity
check failed.

Usage (after the tables are built):
    python -m lakehouse.quality.suites                  # every table
    python -m lakehouse.quality.suites --layer gold
    python -m lakehouse.quality.suites --tables gold.fact_sales
"""

from __future__ import annotations

import argparse
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Iterable, Sequence

from lakehouse.gold.dimensions import UNKNOWN_KEY
from lakehouse.quality.checks import (
    WARN,
    Check,
    CheckResult,
    accepted_values,
    between,
    expression,
    foreign_key,
    format_result,
    min_row_count,
    not_null,
    raise_on_failures,
    run_check,
    unique,
)
from lakehouse.silver.transforms import VALID_ORDER_STATUSES
from lakehouse.utils.config import PipelineConfig, load_config
from lakehouse.utils.logger import get_logger
from lakehouse.utils.tables import read_table, save_table

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = get_logger(__name__)

RESULTS_TABLE = "dq_results"
RESULT_COLUMNS = ("run_id", "checked_at", "table_name", "check_name", "severity",
                  "passed", "failed_rows", "total_rows", "failure_ratio",
                  "max_failure_ratio")

# Share of silver rows that may carry a data quality label before we warn.
# The generator makes about 5% of records dirty; 10% means "more than usual".
SILVER_FLAGGED_TOLERANCE = 0.10
NO_DQ_ISSUES = "size(_dq_issues) = 0"

SUITES: dict[str, tuple[Check, ...]] = {
    "silver.orders": (
        min_row_count(1),
        not_null("order_id"),
        unique("order_id"),
        expression("rows_without_dq_issues", NO_DQ_ISSUES,
                   severity=WARN, max_failure_ratio=SILVER_FLAGGED_TOLERANCE),
    ),
    "silver.order_items": (
        min_row_count(1),
        not_null("order_id", "line_number"),
        unique("order_id", "line_number"),
        expression("rows_without_dq_issues", NO_DQ_ISSUES,
                   severity=WARN, max_failure_ratio=SILVER_FLAGGED_TOLERANCE),
    ),
    "silver.customers": (
        min_row_count(1),
        not_null("customer_id", "valid_from"),
        # SCD2 invariant: exactly one live version per customer...
        unique("customer_id", where="is_current"),
        # ...and only the live version is open-ended.
        expression("current_iff_open", "is_current = (valid_to IS NULL)"),
    ),
    "silver.products": (
        min_row_count(1),
        unique("product_id"),
    ),
    "gold.dim_date": (
        unique("date_key"),
        not_null("date_key", "date"),
    ),
    "gold.dim_customer": (
        unique("customer_key"),
        # The unknown member must exist, or facts pointing at -1 would orphan.
        min_row_count(1, where=f"customer_key = {UNKNOWN_KEY}"),
    ),
    "gold.dim_product": (
        unique("product_key"),
        min_row_count(1, where=f"product_key = {UNKNOWN_KEY}"),
    ),
    "gold.fact_sales": (
        min_row_count(1),
        unique("order_id", "line_number"),
        not_null("date_key", "customer_key", "product_key", "month_key", "net_amount"),
        foreign_key("date_key", "gold.dim_date"),
        foreign_key("customer_key", "gold.dim_customer"),
        foreign_key("product_key", "gold.dim_product"),
        between("quantity", low=1),
        between("discount_pct", low=0, high=100),
        expression("net_is_gross_minus_discount",
                   "net_amount = gross_amount - discount_amount"),
        # Unknown statuses are kept in gold (step 13), so this is a warning.
        accepted_values("order_status", VALID_ORDER_STATUSES,
                        severity=WARN, max_failure_ratio=0.01),
        # Sales we cannot attribute to a customer: some are expected.
        expression("known_customer", f"customer_key <> {UNKNOWN_KEY}",
                   severity=WARN, max_failure_ratio=0.05),
    ),
    "gold.agg_daily_sales": (
        unique("date_key"),
        between("net_revenue", low=0),
    ),
}


def select_suites(tables: Iterable[str] | None = None, layer: str | None = None,
                  suites: dict[str, tuple[Check, ...]] = SUITES) -> dict[str, tuple[Check, ...]]:
    """Pick suites by table name ("layer.table") or by layer."""
    if tables:
        unknown = sorted(set(tables) - set(suites))
        if unknown:
            raise ValueError(f"No checks defined for {unknown}. Known: {sorted(suites)}")
        return {t: suites[t] for t in tables}
    if layer:
        picked = {t: c for t, c in suites.items() if t.split(".")[0] == layer}
        if not picked:
            raise ValueError(f"No checks defined for layer '{layer}'")
        return picked
    return dict(suites)


def result_rows(results: Sequence[CheckResult], run_id: str,
                checked_at: datetime) -> list[tuple]:
    """Results as tuples in RESULT_COLUMNS order, ready for the audit table."""
    return [(run_id, checked_at, r.table, r.check, r.severity, r.passed,
             r.failed_rows, r.total_rows, round(r.failure_ratio, 6), r.max_failure_ratio)
            for r in results]


# --------------------------------------------------------------------------
# Spark
# --------------------------------------------------------------------------

def write_results(spark: "SparkSession", rows: list[tuple], path: str) -> None:
    """Append the results to the dq_results audit table."""
    schema = ("run_id string, checked_at timestamp, table_name string, check_name string, "
              "severity string, passed boolean, failed_rows bigint, total_rows bigint, "
              "failure_ratio double, max_failure_ratio double")
    save_table(spark.createDataFrame(rows, schema).write.format("delta").mode("append"), path)


def run_suites(spark: "SparkSession", config: PipelineConfig,
               suites: dict[str, tuple[Check, ...]] = SUITES,
               fail_on_error: bool = True) -> list[CheckResult]:
    """Run every check, record the results, then fail on blocking failures.

    All checks run before anything is raised, so one run reports every
    problem at once instead of one per rerun.
    """
    cache: dict[str, "DataFrame"] = {}

    def load(name: str) -> "DataFrame":
        if name not in cache:
            layer, table = name.split(".", 1)
            cache[name] = read_table(spark, config.table_path(layer, table))
        return cache[name]

    results: list[CheckResult] = []
    for table, checks in suites.items():
        df = load(table)
        for check in checks:
            result = run_check(table, check, df, load)
            results.append(result)
            log = logger.info if result.passed else (
                logger.warning if result.severity == WARN else logger.error)
            log(format_result(result))

    run_id = uuid.uuid4().hex
    write_results(spark, result_rows(results, run_id, datetime.now(timezone.utc)),
                  config.table_path("audit", RESULTS_TABLE))
    passed = sum(r.passed for r in results)
    logger.info("Data quality run %s: %d of %d checks passed", run_id, passed, len(results))
    if fail_on_error:
        raise_on_failures(results)
    return results


def main(argv: list[str] | None = None) -> None:
    from lakehouse.utils.spark import get_spark

    parser = argparse.ArgumentParser(description="Run the data quality checks.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--tables", nargs="+", metavar="LAYER.TABLE")
    group.add_argument("--layer", choices=["silver", "gold"])
    args = parser.parse_args(argv)
    suites = select_suites(args.tables, args.layer)

    config = load_config()
    spark = get_spark("data-quality", config)
    try:
        run_suites(spark, config, suites)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
