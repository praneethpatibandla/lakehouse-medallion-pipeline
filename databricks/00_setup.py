# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup: schemas, landing volume and sample data
# MAGIC
# MAGIC Run once (it is safe to re-run). It creates, in the chosen catalog:
# MAGIC
# MAGIC | Object | Purpose |
# MAGIC |---|---|
# MAGIC | schema `raw` + volume `raw.files` | landing zone for the source files |
# MAGIC | schemas `bronze`, `silver`, `gold` | the medallion layers (managed Delta tables) |
# MAGIC | schemas `quarantine`, `audit` | unparseable rows, load and run logs |
# MAGIC
# MAGIC Then it writes synthetic source files into the volume with the same generator used
# MAGIC locally. Generate more days later by changing `start_date` and running the last cell again.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

dbutils.widgets.text("start_date", "2026-11-01", "First order date")
dbutils.widgets.text("days", "3", "Days of orders to generate")
dbutils.widgets.dropdown("create_catalog", "false", ["false", "true"], "Create the catalog")

# COMMAND ----------

from lakehouse.utils.unity_catalog import setup_statements

# Free Edition already has the `workspace` catalog; only set create_catalog
# to true when using a new catalog you have permission to create.
for statement in setup_statements(catalog, dbutils.widgets.get("create_catalog") == "true"):
    print(statement)
    spark.sql(statement)

display(spark.sql(f"SHOW SCHEMAS IN `{catalog}`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample data
# MAGIC Volumes can be read and written with normal Python file functions under `/Volumes/...`,
# MAGIC so the generator writes there exactly as it writes to `data/landing/` on a laptop.

# COMMAND ----------

from data_generator import orders, reference_data

landing = str(config.landing_path)
reference_data.main(["--customers", "500", "--products", "100", "--output", landing])
orders.main(["--start-date", dbutils.widgets.get("start_date"),
             "--days", dbutils.widgets.get("days"),
             "--orders-per-day", "200", "--landing", landing])

display(dbutils.fs.ls(f"{landing}/orders"))
