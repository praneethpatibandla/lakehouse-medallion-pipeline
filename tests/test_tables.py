"""Pure-Python tests for table references and the Unity Catalog layout.

The Spark side of `lakehouse.utils.tables` (path-based tables) is covered by
every Spark test, since all jobs now read and write through it. The
table-name side only runs on Databricks.
"""

from pathlib import Path

import pytest

from lakehouse.utils.config import PipelineConfig
from lakehouse.utils.tables import is_table_name, sql_ref
from lakehouse.utils.unity_catalog import (
    DEFAULT_CATALOG,
    setup_statements,
    unity_catalog_config,
    volume_path,
)


def _config(catalog=None) -> PipelineConfig:
    return PipelineConfig(env="test", data_root=Path("/lake"), log_level="INFO",
                          shuffle_partitions=1, catalog=catalog)


def test_table_name_or_path():
    assert is_table_name("workspace.silver.orders")
    assert not is_table_name("/lake/silver/orders")
    assert not is_table_name("data/silver/orders")
    assert not is_table_name(r"C:\lake\silver\orders")
    assert not is_table_name("silver.orders")            # two parts is not a UC name
    assert not is_table_name("/Volumes/workspace/raw/files")


def test_sql_ref():
    assert sql_ref("workspace.gold.fact_sales") == "workspace.gold.fact_sales"
    assert sql_ref("/lake/gold/fact_sales") == "delta.`/lake/gold/fact_sales`"


def test_local_config_uses_paths():
    config = _config()
    assert config.table_path("silver", "orders") == str(Path("/lake/silver/orders"))


def test_catalog_config_uses_table_names_but_landing_stays_a_folder():
    config = _config("workspace")
    assert config.table_path("bronze", "orders") == "workspace.bronze.orders"
    assert config.table_path("audit", "pipeline_runs") == "workspace.audit.pipeline_runs"
    assert config.table_path("landing", "orders") == str(Path("/lake/landing/orders"))
    with pytest.raises(ValueError):
        config.table_path("platinum", "orders")


@pytest.mark.parametrize("bad", ["my-catalog", "x; DROP TABLE y", "1abc", ""])
def test_catalog_name_is_validated(bad):
    with pytest.raises(ValueError):
        _config(bad)


def test_unity_catalog_config():
    config = unity_catalog_config()
    assert config.catalog == DEFAULT_CATALOG
    assert str(config.data_root) == volume_path(DEFAULT_CATALOG) == "/Volumes/workspace/raw/files"
    assert str(config.landing_path) == "/Volumes/workspace/raw/files/landing"
    assert config.table_path("gold", "dim_date") == "workspace.gold.dim_date"


def test_setup_statements():
    sql = setup_statements("workspace")
    assert not any(s.startswith("CREATE CATALOG") for s in sql)
    for schema in ("raw", "bronze", "silver", "gold", "quarantine", "audit"):
        assert any(s.startswith(f"CREATE SCHEMA IF NOT EXISTS `workspace`.`{schema}`") for s in sql)
    assert sql[-1].startswith("CREATE VOLUME IF NOT EXISTS `workspace`.`raw`.`files`")

    assert setup_statements("lakehouse", create_catalog=True)[0] == \
        "CREATE CATALOG IF NOT EXISTS `lakehouse`"
    with pytest.raises(ValueError):
        setup_statements("bad name")
