"""Read and write Delta tables by path (local) or by name (Unity Catalog).

Locally every table is a folder, e.g. `data/silver/orders`, and Spark reads it
with `.load(path)`. On Databricks with Unity Catalog the same table is a
managed table with a three-level name, e.g. `workspace.silver.orders`, read
with `spark.read.table(name)`; Databricks decides where its files live.

`PipelineConfig.table_path()` returns one or the other (see config.py), and
the jobs pass that reference to the helpers below instead of calling
`.load()` / `.save()` / `DeltaTable.forPath()` themselves. That keeps one
copy of the pipeline logic for both environments.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, DataFrameWriter, SparkSession

# catalog.schema.table, each part a plain identifier. A local path always
# contains a "/" (or "\" on Windows), so the two can never be confused.
_TABLE_NAME = re.compile(r"[A-Za-z_]\w*\.[A-Za-z_]\w*\.[A-Za-z_]\w*")


def is_table_name(ref: str) -> bool:
    """True for a Unity Catalog name like `main.silver.orders`, False for a path."""
    return bool(_TABLE_NAME.fullmatch(ref))


def sql_ref(ref: str) -> str:
    """How to name the table in SQL: the name itself, or delta.`<path>`."""
    return ref if is_table_name(ref) else f"delta.`{ref}`"


def read_table(spark: "SparkSession", ref: str) -> "DataFrame":
    """Read a whole Delta table."""
    if is_table_name(ref):
        return spark.read.table(ref)
    return spark.read.format("delta").load(ref)


def save_table(writer: "DataFrameWriter", ref: str) -> None:
    """Finish a `df.write.format("delta")...` chain into a path or a table.

    Options set on the writer (mode, replaceWhere, overwriteSchema,
    partitionBy) work the same way for both. `saveAsTable` creates a managed
    table on the first write.
    """
    if is_table_name(ref):
        writer.saveAsTable(ref)
    else:
        writer.save(ref)


def table_exists(spark: "SparkSession", ref: str) -> bool:
    """True if the Delta table exists."""
    if is_table_name(ref):
        return spark.catalog.tableExists(ref)
    from delta.tables import DeltaTable

    return DeltaTable.isDeltaTable(spark, ref)


def delta_table(spark: "SparkSession", ref: str):
    """Return the `DeltaTable` handle used for MERGE, history and detail."""
    from delta.tables import DeltaTable

    if is_table_name(ref):
        return DeltaTable.forName(spark, ref)
    return DeltaTable.forPath(spark, ref)


def create_table_if_not_exists(spark: "SparkSession", ref: str, schema):
    """Create an empty Delta table with `schema` unless it exists; return its handle."""
    from delta.tables import DeltaTable

    builder = DeltaTable.createIfNotExists(spark)
    builder = builder.tableName(ref) if is_table_name(ref) else builder.location(ref)
    return builder.addColumns(schema).execute()
