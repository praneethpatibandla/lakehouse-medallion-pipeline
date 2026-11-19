"""Spark session factory configured for Delta Lake.

Every job gets its SparkSession from `get_spark()` instead of building its own,
so all jobs share the same settings (Delta extensions, time zone, shuffle
partitions). Two environments are supported:

* Locally, a `local[*]` session is built and the Delta Lake jars are added with
  `configure_spark_with_delta_pip`, which matches the jar version to the
  installed `delta-spark` Python package.
* On Databricks a session already exists and Delta is built in, so the
  existing session is returned unchanged (step 20 relies on this).

Usage (smoke test: writes and reads a tiny Delta table):
    python -m lakehouse.utils.spark
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from lakehouse.utils.config import PipelineConfig, load_config
from lakehouse.utils.logger import get_logger

if TYPE_CHECKING:  # only for type hints; pyspark is imported lazily below
    from pyspark.sql import SparkSession

logger = get_logger(__name__)

# The two settings that turn a plain Spark session into a Delta-aware one:
# the SQL extension adds commands such as MERGE, OPTIMIZE and VACUUM, and the
# catalog lets `CREATE TABLE ... USING DELTA` and DESCRIBE HISTORY work.
DELTA_SPARK_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}


def is_databricks() -> bool:
    """True when running on a Databricks cluster or serverless compute."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def build_spark_conf(config: PipelineConfig) -> dict[str, str]:
    """Return the Spark settings for a local run as plain strings.

    Kept separate from `get_spark()` so the settings can be unit tested
    without starting a JVM.
    """
    conf = dict(DELTA_SPARK_CONF)
    conf.update({
        # The default of 200 shuffle partitions is sized for clusters. On a
        # laptop with small data it creates hundreds of tiny tasks and files.
        "spark.sql.shuffle.partitions": str(config.shuffle_partitions),
        # Interpret and display timestamps in UTC whatever the laptop's time
        # zone is, so results do not change between machines.
        "spark.sql.session.timeZone": "UTC",
        # Keep Spark's default table folder inside the (git-ignored) data root
        # instead of the current working directory.
        "spark.sql.warehouse.dir": str(config.data_root / "spark-warehouse"),
        # The progress bar interleaves with our log lines; turn it off.
        "spark.ui.showConsoleProgress": "false",
    })
    return conf


def get_spark(app_name: str = "lakehouse", config: PipelineConfig | None = None) -> "SparkSession":
    """Return a SparkSession that can read and write Delta tables.

    Calling it twice returns the same session (`getOrCreate`).
    """
    # Imported here, not at module level, so modules and tests that only need
    # pure Python do not require pyspark to be installed.
    from pyspark.sql import SparkSession

    if is_databricks():
        # Serverless compute does not allow most session settings to be
        # changed, and Delta is already configured, so use the session as is.
        return SparkSession.builder.getOrCreate()

    from delta import configure_spark_with_delta_pip

    config = config or load_config()
    builder = SparkSession.builder.appName(app_name).master("local[*]")
    for key, value in build_spark_conf(config).items():
        builder = builder.config(key, value)

    # Adds the delta-spark jars (downloaded from Maven on the first run).
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark %s session '%s' started (shuffle partitions=%s)",
                spark.version, app_name, config.shuffle_partitions)
    return spark


def main() -> None:
    """Smoke test: write a three-row Delta table, read it back, show its history."""
    config = load_config()
    spark = get_spark("spark-smoke-test", config)
    path = str(config.data_root / "_smoke_test" / "delta_check")

    spark.range(3).write.format("delta").mode("overwrite").save(path)
    rows = spark.read.format("delta").load(path).count()
    version = spark.sql(f"DESCRIBE HISTORY delta.`{path}`").agg({"version": "max"}).first()[0]
    logger.info("Delta OK: %d rows at %s (table version %s)", rows, path, version)
    spark.stop()


if __name__ == "__main__":
    main()
