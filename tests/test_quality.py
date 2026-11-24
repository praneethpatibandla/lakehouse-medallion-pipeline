"""Tests for the data quality framework and suites (step 16)."""

from datetime import datetime

import pytest

from lakehouse.quality.checks import (
    ERROR,
    WARN,
    DataQualityError,
    RowCheck,
    accepted_values,
    between,
    blocking_failures,
    evaluate,
    expression,
    failing_rows_sql,
    foreign_key,
    min_row_count,
    not_null,
    raise_on_failures,
    unique,
)
from lakehouse.quality.suites import RESULT_COLUMNS, SUITES, result_rows, select_suites

# ---------------------------------------------------------------------------
# Pure Python: building checks
# ---------------------------------------------------------------------------


def test_row_check_builders_generate_sql():
    assert not_null("a", "b").valid_sql == "`a` IS NOT NULL AND `b` IS NOT NULL"
    assert accepted_values("status", ["placed", "it's"]).valid_sql == \
        "`status` IN ('placed', 'it''s')"
    assert between("qty", low=1).valid_sql == "`qty` >= 1"
    assert between("pct", 0, 100).valid_sql == "`pct` >= 0 AND `pct` <= 100"


def test_null_results_count_as_failures():
    check = expression("positive", "x > 0")
    assert failing_rows_sql(check) == "NOT coalesce((x > 0), false)"


def test_builders_reject_meaningless_rules():
    with pytest.raises(ValueError):
        not_null()
    with pytest.raises(ValueError):
        unique()
    with pytest.raises(ValueError):
        between("x")
    with pytest.raises(ValueError):
        accepted_values("x", [])
    with pytest.raises(ValueError, match="severity"):
        RowCheck(name="x", severity="fatal")
    with pytest.raises(ValueError, match="max_failure_ratio"):
        RowCheck(name="x", max_failure_ratio=1.5)


def test_foreign_key_defaults_ref_column_to_same_name():
    fk = foreign_key("product_key", "gold.dim_product")
    assert (fk.ref_table, fk.ref_column) == ("gold.dim_product", "product_key")


# ---------------------------------------------------------------------------
# Pure Python: evaluating results
# ---------------------------------------------------------------------------


def test_evaluate_uses_the_failure_ratio():
    strict = unique("id")
    assert evaluate("t", strict, 0, 100).passed
    assert not evaluate("t", strict, 1, 100).passed

    tolerant = expression("clean", "true", severity=WARN, max_failure_ratio=0.05)
    assert evaluate("t", tolerant, 5, 100).passed        # exactly at the limit
    assert not evaluate("t", tolerant, 6, 100).passed
    assert evaluate("t", tolerant, 0, 0).failure_ratio == 0.0   # empty table


def test_min_row_count_ignores_the_ratio():
    check = min_row_count(10)
    assert evaluate("t", check, 0, 12).passed
    assert not evaluate("t", check, 3, 7).passed


def test_only_error_failures_block_the_run():
    results = [
        evaluate("t", unique("id"), 0, 10),                          # pass
        evaluate("t", expression("w", "true", severity=WARN), 2, 10),  # warn
    ]
    assert blocking_failures(results) == []
    raise_on_failures(results)  # does not raise

    results.append(evaluate("t", not_null("id"), 1, 10))
    assert [r.check for r in blocking_failures(results)] == ["not_null(id)"]
    with pytest.raises(DataQualityError, match="1 data quality check"):
        raise_on_failures(results)


# ---------------------------------------------------------------------------
# Suites
# ---------------------------------------------------------------------------


def test_select_suites():
    assert select_suites() == SUITES
    assert set(select_suites(layer="silver")) == {t for t in SUITES if t.startswith("silver.")}
    assert list(select_suites(["gold.fact_sales"])) == ["gold.fact_sales"]
    with pytest.raises(ValueError):
        select_suites(["gold.nope"])


def test_suite_check_names_are_unique_per_table():
    for table, checks in SUITES.items():
        names = [c.name for c in checks]
        assert len(names) == len(set(names)), table


def test_fact_checks_reference_tables_with_suites():
    for check in SUITES["gold.fact_sales"]:
        if hasattr(check, "ref_table"):
            assert check.ref_table in SUITES


def test_result_rows_follow_column_order():
    result = evaluate("gold.fact_sales", unique("order_id"), 2, 8)
    (row,) = result_rows([result], "run1", datetime(2026, 11, 23))
    assert len(row) == len(RESULT_COLUMNS)
    assert dict(zip(RESULT_COLUMNS, row))["failure_ratio"] == 0.25
    assert dict(zip(RESULT_COLUMNS, row))["severity"] == ERROR


# ---------------------------------------------------------------------------
# Spark tests: skipped when pyspark / delta-spark / Java are missing.
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    from lakehouse.utils.config import PipelineConfig
    from lakehouse.utils.spark import get_spark

    config = PipelineConfig(env="test", data_root=tmp_path_factory.mktemp("lake"),
                            log_level="WARNING", shuffle_partitions=1)
    session = get_spark("test-quality", config)
    yield session
    session.stop()


def test_spark_checks_count_failed_rows(spark):
    from lakehouse.quality.checks import run_check

    df = spark.createDataFrame(
        [("A", 1, "placed"), ("A", 2, "placed"), ("B", None, "lost"), (None, 4, None)],
        "id string, qty int, status string")
    dims = {"ref": spark.createDataFrame([("A",)], "id string")}

    def count(check):
        return run_check("t", check, df, dims.__getitem__).failed_rows

    assert count(not_null("id")) == 1
    assert count(unique("id")) == 2                 # both copies of A; NULL not counted
    assert count(between("qty", low=2)) == 2        # qty 1 fails, NULL qty fails too
    assert count(accepted_values("status", ["placed"])) == 2
    assert count(foreign_key("id", "ref")) == 1     # B is an orphan; NULL skipped
    assert count(min_row_count(10)) == 6
    assert count(unique("id", where="qty > 1")) == 0  # filter applied first


def test_run_suites_records_results_and_fails_on_errors(spark, tmp_path):
    from lakehouse.quality.suites import RESULTS_TABLE, run_suites
    from lakehouse.utils.config import PipelineConfig

    config = PipelineConfig(env="test", data_root=tmp_path, log_level="WARNING",
                            shuffle_partitions=1)
    (spark.createDataFrame([("P1",), ("P1",)], "product_id string")
     .write.format("delta").save(config.table_path("silver", "products")))
    suites = {"silver.products": SUITES["silver.products"]}

    with pytest.raises(DataQualityError, match="unique"):
        run_suites(spark, config, suites)
    recorded = spark.read.format("delta").load(config.table_path("audit", RESULTS_TABLE))
    assert recorded.count() == len(suites["silver.products"])
    assert recorded.where("NOT passed").count() == 1
