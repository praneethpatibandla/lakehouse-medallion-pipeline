"""A small, declarative data quality check framework.

A check is a rule about a table ("order_id is unique", "every product_key
exists in dim_product"). Running it counts the rows that break the rule and
compares that count with an allowed failure ratio:

    failed_rows / total_rows <= max_failure_ratio   -> pass
    otherwise                                      -> fail

Every check has a severity:

    error   a failure stops the pipeline: the data is wrong, and publishing
            it would mislead the people who read it
    warn    a failure is logged and recorded, but the run continues: the
            data is suspicious and someone should look at it

Four kinds of check cover most needs:

    RowCheck         each row is valid on its own (not null, accepted values,
                     a range, any SQL expression)
    UniqueCheck      no two rows share the same key
    ForeignKeyCheck  every value exists in another table
    MinRowCount      the table is not (nearly) empty

The check definitions, the SQL they generate and the pass/fail arithmetic
are plain Python and unit tested without Spark. Only `run_check` needs one.
This is the same idea as Great Expectations or dbt tests, kept small enough
to read in one sitting.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Sequence, Union

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

ERROR = "error"
WARN = "warn"
SEVERITIES = (ERROR, WARN)


class DataQualityError(RuntimeError):
    """Raised when at least one error-severity check fails."""


@dataclass(frozen=True)
class _CheckBase:
    name: str
    severity: str = ERROR
    # Share of rows allowed to fail. 0.0 = no exceptions at all.
    max_failure_ratio: float = 0.0
    # Optional SQL filter: the check only looks at these rows,
    # e.g. "is_current" to check only current SCD2 versions.
    where: str | None = None

    def __post_init__(self) -> None:
        if self.severity not in SEVERITIES:
            raise ValueError(f"{self.name}: severity must be one of {SEVERITIES}")
        if not 0 <= self.max_failure_ratio <= 1:
            raise ValueError(f"{self.name}: max_failure_ratio must be between 0 and 1")


@dataclass(frozen=True)
class RowCheck(_CheckBase):
    """Each row must satisfy `valid_sql` (a SQL boolean expression).

    A row where the expression is NULL counts as failed: "we cannot tell"
    is not a pass.
    """

    valid_sql: str = "true"


@dataclass(frozen=True)
class UniqueCheck(_CheckBase):
    """No two rows may share the same values in `columns`.

    Failed rows = every row whose key occurs more than once, so two copies
    of one order count as 2 failed rows.
    """

    columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class ForeignKeyCheck(_CheckBase):
    """Every non-NULL `column` value must exist in `ref_table`.`ref_column`.

    `ref_table` is "layer.table". NULLs are left to a not_null check, so a
    rule never reports the same problem twice.
    """

    column: str = ""
    ref_table: str = ""
    ref_column: str = ""


@dataclass(frozen=True)
class MinRowCount(_CheckBase):
    """The table must have at least `min_rows` rows.

    Catches the quiet failure where every other check passes because there
    is nothing to check.
    """

    min_rows: int = 1


Check = Union[RowCheck, UniqueCheck, ForeignKeyCheck, MinRowCount]


# --------------------------------------------------------------------------
# Pure Python: building checks
# --------------------------------------------------------------------------

def _q(column: str) -> str:
    return f"`{column}`"


def _sql_literal(value) -> str:
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return str(value)


def not_null(*columns: str, **options) -> RowCheck:
    """All of `columns` must be present."""
    if not columns:
        raise ValueError("not_null needs at least one column")
    valid = " AND ".join(f"{_q(c)} IS NOT NULL" for c in columns)
    return RowCheck(name=f"not_null({', '.join(columns)})", valid_sql=valid, **options)


def accepted_values(column: str, values: Sequence, **options) -> RowCheck:
    """`column` must be one of `values` (a NULL fails)."""
    if not values:
        raise ValueError("accepted_values needs at least one value")
    listed = ", ".join(_sql_literal(v) for v in values)
    return RowCheck(name=f"accepted_values({column})",
                    valid_sql=f"{_q(column)} IN ({listed})", **options)


def between(column: str, low=None, high=None, **options) -> RowCheck:
    """`column` must be within [low, high]; leave one side None for no bound."""
    if low is None and high is None:
        raise ValueError("between needs a low or a high bound")
    parts = []
    if low is not None:
        parts.append(f"{_q(column)} >= {_sql_literal(low)}")
    if high is not None:
        parts.append(f"{_q(column)} <= {_sql_literal(high)}")
    return RowCheck(name=f"between({column}, {low}, {high})",
                    valid_sql=" AND ".join(parts), **options)


def expression(name: str, valid_sql: str, **options) -> RowCheck:
    """Any rule that fits in one SQL boolean expression."""
    return RowCheck(name=name, valid_sql=valid_sql, **options)


def unique(*columns: str, **options) -> UniqueCheck:
    if not columns:
        raise ValueError("unique needs at least one column")
    return UniqueCheck(name=f"unique({', '.join(columns)})", columns=tuple(columns), **options)


def foreign_key(column: str, ref_table: str, ref_column: str | None = None,
                **options) -> ForeignKeyCheck:
    ref_column = ref_column or column
    return ForeignKeyCheck(name=f"foreign_key({column} -> {ref_table}.{ref_column})",
                           column=column, ref_table=ref_table, ref_column=ref_column,
                           **options)


def min_row_count(min_rows: int = 1, **options) -> MinRowCount:
    if min_rows < 0:
        raise ValueError("min_rows cannot be negative")
    return MinRowCount(name=f"min_row_count({min_rows})", min_rows=min_rows, **options)


def failing_rows_sql(check: RowCheck) -> str:
    """The filter that selects the rows breaking a RowCheck.

    `NOT coalesce(valid, false)`: a NULL result becomes a failure instead of
    slipping through, which a plain `NOT valid` would allow.
    """
    return f"NOT coalesce(({check.valid_sql}), false)"


# --------------------------------------------------------------------------
# Pure Python: results
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class CheckResult:
    """The outcome of one check on one table."""

    table: str
    check: str
    severity: str
    failed_rows: int
    total_rows: int
    max_failure_ratio: float
    passed: bool

    @property
    def failure_ratio(self) -> float:
        return self.failed_rows / self.total_rows if self.total_rows else 0.0


def evaluate(table: str, check: Check, failed_rows: int, total_rows: int) -> CheckResult:
    """Turn raw counts into a pass/fail result.

    For MinRowCount the "failed rows" are the rows missing below the minimum
    and the check passes only when there are none, whatever the ratio.
    """
    if failed_rows < 0 or total_rows < 0:
        raise ValueError("row counts cannot be negative")
    if isinstance(check, MinRowCount):
        passed = failed_rows == 0
    else:
        ratio = failed_rows / total_rows if total_rows else 0.0
        passed = ratio <= check.max_failure_ratio
    return CheckResult(table=table, check=check.name, severity=check.severity,
                       failed_rows=failed_rows, total_rows=total_rows,
                       max_failure_ratio=check.max_failure_ratio, passed=passed)


def blocking_failures(results: Sequence[CheckResult]) -> list[CheckResult]:
    """The failed error-severity checks: the ones that must stop the run."""
    return [r for r in results if not r.passed and r.severity == ERROR]


def format_result(result: CheckResult) -> str:
    status = "PASS" if result.passed else ("FAIL" if result.severity == ERROR else "WARN")
    return (f"{status} {result.table}: {result.check} - {result.failed_rows}/"
            f"{result.total_rows} rows failed ({result.failure_ratio:.2%}, "
            f"allowed {result.max_failure_ratio:.2%})")


def raise_on_failures(results: Sequence[CheckResult]) -> None:
    """Raise DataQualityError listing every blocking failure, if any."""
    blocking = blocking_failures(results)
    if blocking:
        lines = "\n  ".join(format_result(r) for r in blocking)
        raise DataQualityError(f"{len(blocking)} data quality check(s) failed:\n  {lines}")


# --------------------------------------------------------------------------
# Spark: counting failed rows
# --------------------------------------------------------------------------

def count_failures(check: Check, df: "DataFrame",
                   load_table: Callable[[str], "DataFrame"] | None = None) -> int:
    """Return how many rows of `df` break `check` (`df` already filtered)."""
    from pyspark.sql import functions as F

    if isinstance(check, RowCheck):
        return df.where(failing_rows_sql(check)).count()

    if isinstance(check, UniqueCheck):
        # A NULL key is a different problem (not_null), so it is not counted
        # here; Spark would otherwise group all NULLs together as duplicates.
        keyed = df.dropna(subset=list(check.columns))
        duplicated = keyed.groupBy(*check.columns).count().where(F.col("count") > 1)
        total = duplicated.agg(F.sum("count")).first()[0]
        return int(total or 0)

    if isinstance(check, ForeignKeyCheck):
        if load_table is None:
            raise ValueError(f"{check.name}: a table loader is needed for foreign keys")
        ref = load_table(check.ref_table).select(
            F.col(check.ref_column).alias("_ref_key")).distinct()
        values = df.where(F.col(check.column).isNotNull())
        # left_anti keeps the rows with no match: the orphans.
        return values.join(ref, values[check.column] == ref["_ref_key"], "left_anti").count()

    if isinstance(check, MinRowCount):
        return max(check.min_rows - df.count(), 0)

    raise TypeError(f"Unknown check type: {type(check).__name__}")


def run_check(table: str, check: Check, df: "DataFrame",
              load_table: Callable[[str], "DataFrame"] | None = None) -> CheckResult:
    """Run one check against a table's DataFrame and evaluate the result."""
    if check.where:
        df = df.where(check.where)
    total = df.count()
    return evaluate(table, check, count_failures(check, df, load_table), total)
