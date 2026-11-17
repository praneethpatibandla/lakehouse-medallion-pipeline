from pathlib import Path

import pytest

from lakehouse.utils.config import load_config
from lakehouse.utils.logger import get_logger


def test_defaults(monkeypatch):
    for var in ("LAKEHOUSE_ENV", "LAKEHOUSE_DATA_ROOT", "LAKEHOUSE_LOG_LEVEL",
                "LAKEHOUSE_SHUFFLE_PARTITIONS"):
        monkeypatch.delenv(var, raising=False)
    config = load_config()
    assert config.env == "dev"
    assert config.log_level == "INFO"
    assert config.shuffle_partitions == 4
    assert config.data_root.name == "data"


def test_environment_overrides(monkeypatch, tmp_path):
    monkeypatch.setenv("LAKEHOUSE_ENV", "test")
    monkeypatch.setenv("LAKEHOUSE_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("LAKEHOUSE_SHUFFLE_PARTITIONS", "2")
    config = load_config()
    assert config.env == "test"
    assert config.data_root == tmp_path
    assert config.shuffle_partitions == 2
    assert config.bronze_path == tmp_path / "bronze"


def test_table_path(monkeypatch, tmp_path):
    monkeypatch.setenv("LAKEHOUSE_DATA_ROOT", str(tmp_path))
    config = load_config()
    assert Path(config.table_path("silver", "orders")) == tmp_path / "silver" / "orders"
    with pytest.raises(ValueError):
        config.table_path("platinum", "orders")


@pytest.mark.parametrize("name,value", [
    ("LAKEHOUSE_ENV", "staging"),
    ("LAKEHOUSE_SHUFFLE_PARTITIONS", "many"),
    ("LAKEHOUSE_SHUFFLE_PARTITIONS", "0"),
])
def test_invalid_values_are_rejected(monkeypatch, name, value):
    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError):
        load_config()


def test_logger_does_not_duplicate_handlers():
    first = get_logger("lakehouse.test")
    second = get_logger("lakehouse.test")
    assert first is second
    assert len(first.handlers) == 1
