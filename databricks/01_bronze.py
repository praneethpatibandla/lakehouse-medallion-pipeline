# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze: load new landing files
# MAGIC
# MAGIC Finds the batch dates that are complete in the landing volume and newer than the
# MAGIC silver watermark (the same plan the local runner makes, `lakehouse.pipeline`), and
# MAGIC loads each one into the bronze tables with `lakehouse.bronze.ingest`.
# MAGIC Unparseable rows go to `quarantine.*`; every load is logged in `audit.ingestion_log`.
# MAGIC
# MAGIC Re-running is safe: a batch date replaces its own partition instead of adding rows.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

import uuid

from lakehouse.pipeline import load_bronze_batch, plan_pending_run

plan = plan_pending_run(spark, config, max_batches=max_batches)
run_id = uuid.uuid4().hex
print(f"run {run_id}: loading {[d.isoformat() for d in plan.batch_dates] or 'nothing new'}")

for batch_date in plan.batch_dates:
    load_bronze_batch(spark, config, batch_date, run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What was loaded

# COMMAND ----------

if plan.batch_dates:
    display(spark.table(config.table_path("audit", "ingestion_log"))
            .where(f"run_id = '{run_id}'")
            .select("batch_date", "source", "status", "rows_read", "rows_loaded",
                    "rows_quarantined", "duration_seconds")
            .orderBy("batch_date", "source"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Rows per batch date in bronze orders, with the load metadata columns.
# MAGIC SELECT _batch_date, count(*) AS rows, min(_ingested_at) AS first_loaded
# MAGIC FROM IDENTIFIER(:catalog || '.bronze.orders')
# MAGIC GROUP BY _batch_date
# MAGIC ORDER BY _batch_date
