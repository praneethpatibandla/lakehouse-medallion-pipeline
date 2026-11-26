"""Central configuration for the pipeline.

Every path and tunable setting lives here, so no other module hard-codes a
location. Values can be overridden with environment variables, which is how
the same code can run on a laptop and on a cluster without edits.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

# Repository root: .../src/lakehouse/utils/config.py -> three levels up from src
PROJECT_ROOT = Path(__file__).resolve().parents[3]

# Layers stored as tables. On Unity Catalog each one is a schema of the
# catalog (e.g. workspace.bronze); "landing" is always a folder of files.
TABLE_LAYERS = ("bronze", "silver", "gold", "quarantine", "audit")
_IDENTIFIER = re.compile(r"[A-Za-z_]\w*")


@dataclass(frozen=True)
class PipelineConfig:
    """Immutable settings for one pipeline run."""

    env: str
    data_root: Path
    log_level: str
    shuffle_partitions: int
    # Unity Catalog catalog name. None (the default, used locally) stores
    # every table as a Delta folder under data_root. When set, tables are
    # managed tables named <catalog>.<layer>.<table>, and only the landing
    # files are read from data_root (a Unity Catalog volume).
    catalog: str | None = None

    def __post_init__(self) -> None:
        # The name is put into SQL and table names, so allow identifiers only.
        if self.catalog is not None and not _IDENTIFIER.fullmatch(self.catalog):
            raise ValueError(f"Invalid catalog name '{self.catalog}'")

    # --- landing zone: files exactly as the source system delivered them ---
    @property
    def landing_path(self) -> Path:
        return self.data_root / "landing"

    # --- medallion layers ---
    @property
    def bronze_path(self) -> Path:
        return self.data_root / "bronze"

    @property
    def silver_path(self) -> Path:
        return self.data_root / "silver"

    @property
    def gold_path(self) -> Path:
        return self.data_root / "gold"

    # --- records that failed ingestion, kept for inspection ---
    @property
    def quarantine_path(self) -> Path:
        return self.data_root / "quarantine"

    # --- pipeline bookkeeping: ingestion audit log, later run state ---
    @property
    def audit_path(self) -> Path:
        return self.data_root / "audit"

    def table_path(self, layer: str, table: str) -> str:
        """Return where a table lives, e.g. table_path("bronze", "orders").

        Locally this is a folder path (`<data_root>/bronze/orders`). With a
        catalog it is a table name (`<catalog>.bronze.orders`). Read and write
        it with the helpers in `lakehouse.utils.tables`, which handle both.
        """
        if self.catalog and layer in TABLE_LAYERS:
            return f"{self.catalog}.{layer}.{table}"
        layers = {
            "landing": self.landing_path,
            "bronze": self.bronze_path,
            "silver": self.silver_path,
            "gold": self.gold_path,
            "quarantine": self.quarantine_path,
            "audit": self.audit_path,
        }
        if layer not in layers:
            raise ValueError(f"Unknown layer '{layer}'. Expected one of: {sorted(layers)}")
        return str(layers[layer] / table)


def load_config() -> PipelineConfig:
    """Build the config from environment variables, falling back to local defaults.

    Environment variables:
        LAKEHOUSE_ENV                 dev | test | prod   (default: dev)
        LAKEHOUSE_DATA_ROOT           base folder for all data (default: <repo>/data)
        LAKEHOUSE_LOG_LEVEL           DEBUG | INFO | WARNING | ERROR (default: INFO)
        LAKEHOUSE_SHUFFLE_PARTITIONS  Spark shuffle partitions (default: 4)
        LAKEHOUSE_CATALOG             Unity Catalog catalog for the tables
                                      (default: unset = Delta folders under data root)
    """
    env = os.getenv("LAKEHOUSE_ENV", "dev").lower()
    if env not in {"dev", "test", "prod"}:
        raise ValueError(f"LAKEHOUSE_ENV must be dev, test or prod, got '{env}'")

    raw_partitions = os.getenv("LAKEHOUSE_SHUFFLE_PARTITIONS", "4")
    try:
        shuffle_partitions = int(raw_partitions)
    except ValueError as exc:
        raise ValueError(
            f"LAKEHOUSE_SHUFFLE_PARTITIONS must be an integer, got '{raw_partitions}'"
        ) from exc
    if shuffle_partitions < 1:
        raise ValueError("LAKEHOUSE_SHUFFLE_PARTITIONS must be at least 1")

    return PipelineConfig(
        env=env,
        data_root=Path(os.getenv("LAKEHOUSE_DATA_ROOT", PROJECT_ROOT / "data")),
        log_level=os.getenv("LAKEHOUSE_LOG_LEVEL", "INFO").upper(),
        shuffle_partitions=shuffle_partitions,
        catalog=os.getenv("LAKEHOUSE_CATALOG") or None,
    )
