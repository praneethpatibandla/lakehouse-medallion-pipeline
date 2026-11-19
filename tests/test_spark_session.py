import pytest

from lakehouse.utils.config import load_config
from lakehouse.utils.spark import DELTA_SPARK_CONF, build_spark_conf, is_databricks


def test_conf_enables_delta(monkeypatch, tmp_path):
    monkeypatch.setenv("LAKEHOUSE_DATA_ROOT", str(tmp_path))
    conf = build_spark_conf(load_config())
    for key, value in DELTA_SPARK_CONF.items():
        assert conf[key] == value


def test_conf_uses_config_values(monkeypatch, tmp_path):
    monkeypatch.setenv("LAKEHOUSE_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("LAKEHOUSE_SHUFFLE_PARTITIONS", "3")
    conf = build_spark_conf(load_config())
    assert conf["spark.sql.shuffle.partitions"] == "3"
    assert conf["spark.sql.session.timeZone"] == "UTC"
    assert conf["spark.sql.warehouse.dir"].startswith(str(tmp_path))
    # Spark expects every config value as a string.
    assert all(isinstance(value, str) for value in conf.values())


def test_is_databricks(monkeypatch):
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    assert not is_databricks()
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "client.1.0")
    assert is_databricks()


def test_delta_round_trip(monkeypatch, tmp_path):
    """Needs pyspark, delta-spark and Java; skipped when they are missing."""
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    from lakehouse.utils.spark import get_spark

    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    monkeypatch.setenv("LAKEHOUSE_DATA_ROOT", str(tmp_path))
    spark = get_spark("test-delta-round-trip", load_config())
    path = str(tmp_path / "numbers")
    spark.range(5).write.format("delta").save(path)
    assert spark.read.format("delta").load(path).count() == 5
