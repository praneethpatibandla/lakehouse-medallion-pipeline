"""Shared pytest fixtures.

Spark tests use ONE local SparkSession for the whole test run. Starting a
JVM with the Delta jars takes several seconds, so a session per test module
(as the tests had before) made the suite slower with every new module.

Tests that use the `spark` fixture are marked `spark` automatically, so the
fast, pure-Python tests can be run on their own:

    pytest -m "not spark"     # seconds, no Java needed
    pytest -m spark           # only the Spark / Delta tests
    pytest                    # everything

When pyspark, delta-spark or Java is missing, Spark tests are skipped with
the reason shown in `pytest -rs`, instead of failing.
"""

from __future__ import annotations

import os
import shutil

import pytest

from lakehouse.utils.config import PipelineConfig


def _java_available() -> bool:
    return bool(shutil.which("java") or os.environ.get("JAVA_HOME"))


def make_config(data_root) -> PipelineConfig:
    """A test config built directly, not from environment variables, so no
    setting leaks between tests. One shuffle partition keeps tiny test
    DataFrames from being split into many tasks and files."""
    return PipelineConfig(env="test", data_root=data_root, log_level="WARNING",
                          shuffle_partitions=1)


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """One Delta-enabled local SparkSession shared by every test."""
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    if not _java_available():
        pytest.skip("Java is not installed (Spark needs Java 11 or 17)")
    # A Databricks variable left in the environment would make get_spark
    # return a plain session without the local Delta settings.
    os.environ.pop("DATABRICKS_RUNTIME_VERSION", None)

    from lakehouse.utils.spark import get_spark

    session = get_spark("lakehouse-tests", make_config(tmp_path_factory.mktemp("spark")))
    yield session
    session.stop()


@pytest.fixture
def lake_config(tmp_path) -> PipelineConfig:
    """A config whose data root is a fresh, empty folder for each test.

    Tables are plain folders of files, so a new folder is a new, empty lake:
    tests cannot see each other's tables even though they share a session.
    """
    return make_config(tmp_path / "lake")


def pytest_collection_modifyitems(items):
    for item in items:
        if "spark" in item.fixturenames:
            item.add_marker(pytest.mark.spark)
