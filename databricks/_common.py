# Databricks notebook source
# MAGIC %md
# MAGIC # Shared setup for the lakehouse notebooks
# MAGIC
# MAGIC Not run on its own. The other notebooks start with `%run ./_common`, which runs this
# MAGIC notebook in their context. It:
# MAGIC
# MAGIC 1. makes the repository's Python code importable (`src/lakehouse`, `data_generator`), so the
# MAGIC    notebooks reuse the tested pipeline code instead of copying it;
# MAGIC 2. defines the widgets every notebook shares;
# MAGIC 3. builds `config`, which stores tables as Unity Catalog managed tables
# MAGIC    (`<catalog>.<layer>.<table>`) and reads landing files from the volume
# MAGIC    `/Volumes/<catalog>/raw/files`.

# COMMAND ----------

import sys
from pathlib import Path

# In a Git folder the working directory of a notebook is the folder it is in
# (databricks/), so the repository root is one level up.
REPO_ROOT = Path.cwd().parent
if not (REPO_ROOT / "src" / "lakehouse").is_dir():
    raise RuntimeError(
        f"Cannot find src/lakehouse under {REPO_ROOT}. Open these notebooks from a Git folder "
        "that contains the whole repository (Workspace > Create > Git folder).")
for folder in (REPO_ROOT / "src", REPO_ROOT):
    if str(folder) not in sys.path:
        sys.path.insert(0, str(folder))

# COMMAND ----------

# Widgets are the notebook's parameters. When a job runs the notebook, job
# parameters with the same names replace these defaults.
dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("max_batches", "", "Max batch dates (empty = all)")

from lakehouse.utils.unity_catalog import unity_catalog_config

catalog = dbutils.widgets.get("catalog").strip()
max_batches = int(dbutils.widgets.get("max_batches")) if dbutils.widgets.get("max_batches").strip() else None
config = unity_catalog_config(catalog)
print(f"catalog={catalog}  landing={config.landing_path}  max_batches={max_batches}")
