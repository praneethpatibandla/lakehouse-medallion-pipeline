"""Unity Catalog layout used when the pipeline runs on Databricks.

    <catalog>                      default: "workspace" (exists on Free Edition)
      raw.files       (volume)     landing files: /Volumes/<catalog>/raw/files/landing/...
      bronze.*        (tables)     raw data plus load metadata
      silver.*        (tables)     cleaned, merged
      gold.*          (tables)     star schema and aggregates
      quarantine.*    (tables)     rows that could not be parsed
      audit.*         (tables)     ingestion log, pipeline runs, data quality results

Only the landing files live in a volume. Volumes are for files; tables are
managed tables, so Unity Catalog owns their storage, permissions and lineage.
Everything here is plain Python (SQL strings and a config), so it is unit
tested without Databricks.
"""

from __future__ import annotations

from pathlib import Path

from lakehouse.utils.config import TABLE_LAYERS, PipelineConfig

DEFAULT_CATALOG = "workspace"
LANDING_SCHEMA = "raw"
LANDING_VOLUME = "files"

SCHEMA_COMMENTS = {
    LANDING_SCHEMA: "Landing zone: source files in a volume",
    "bronze": "Raw data as received, plus load metadata",
    "silver": "Cleaned, deduplicated, incrementally merged data",
    "gold": "Star schema and aggregates for analytics",
    "quarantine": "Rows that could not be parsed, kept for inspection",
    "audit": "Ingestion log, pipeline runs and data quality results",
}


def volume_path(catalog: str) -> str:
    """Path of the landing volume as seen by Python and Spark on Databricks."""
    return f"/Volumes/{catalog}/{LANDING_SCHEMA}/{LANDING_VOLUME}"


def unity_catalog_config(catalog: str = DEFAULT_CATALOG, log_level: str = "INFO") -> PipelineConfig:
    """Config for a Databricks run: tables in `catalog`, files in its volume.

    shuffle_partitions is not used there: get_spark() leaves the Databricks
    session as it is, and serverless compute tunes partitions itself.
    """
    return PipelineConfig(env="prod", data_root=Path(volume_path(catalog)),
                          log_level=log_level, shuffle_partitions=4, catalog=catalog)


def setup_statements(catalog: str, create_catalog: bool = False) -> list[str]:
    """SQL that creates the schemas and the landing volume. Safe to re-run.

    `create_catalog` is off by default: Free Edition already has the
    `workspace` catalog, and creating catalogs needs a privilege a normal
    user may not have.
    """
    # Validates the name (identifiers only) before it is put into SQL.
    PipelineConfig(env="prod", data_root=Path("."), log_level="INFO",
                   shuffle_partitions=1, catalog=catalog)
    statements = []
    if create_catalog:
        statements.append(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
    for schema in (LANDING_SCHEMA, *TABLE_LAYERS):
        statements.append(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}` "
                          f"COMMENT '{SCHEMA_COMMENTS[schema]}'")
    statements.append(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{LANDING_SCHEMA}`.`{LANDING_VOLUME}` "
                      "COMMENT 'Landing files for the lakehouse pipeline'")
    return statements
