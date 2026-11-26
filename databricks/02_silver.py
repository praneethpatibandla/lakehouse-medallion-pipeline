# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver: clean, deduplicate and merge
# MAGIC
# MAGIC Processes the same batch dates as `01_bronze` (the plan only moves when this notebook
# MAGIC succeeds), oldest first:
# MAGIC
# MAGIC * `silver.customers` - SCD Type 2 history
# MAGIC * `silver.products` - current state
# MAGIC * `silver.orders` - deduplicated, MERGE upsert
# MAGIC * `silver.order_items` - line amounts, checked against orders and products
# MAGIC
# MAGIC After each date it appends a `success` row to `audit.pipeline_runs`, which moves the
# MAGIC silver watermark. A date whose bronze load is missing or failed is refused, so it is
# MAGIC never marked as done by mistake.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

import uuid

from lakehouse.pipeline import (
    STAGE_SILVER,
    RunRecord,
    load_silver_batch,
    missing_bronze_sources,
    plan_pending_run,
    read_ingestion_log,
    run_stage,
)

plan = plan_pending_run(spark, config, max_batches=max_batches)
run_id = uuid.uuid4().hex
ingestion_log = read_ingestion_log(spark, config)
print(f"run {run_id}: merging {[d.isoformat() for d in plan.batch_dates] or 'nothing new'}")

for batch_date in plan.batch_dates:
    missing = missing_bronze_sources(ingestion_log, batch_date)
    if missing:
        # Stop here: later dates must wait, because customers are built in date order.
        raise RuntimeError(f"Bronze is not loaded for {batch_date} ({', '.join(missing)}). "
                           "Run 01_bronze first.")
    # run_stage records success or failure in audit.pipeline_runs.
    run_stage(spark, config, RunRecord(run_id, STAGE_SILVER, batch_date),
              lambda day=batch_date: load_silver_batch(spark, config, day))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check the results

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Row counts per silver table.
# MAGIC SELECT 'customers' AS table_name, count(*) AS rows FROM IDENTIFIER(:catalog || '.silver.customers')
# MAGIC UNION ALL SELECT 'products', count(*) FROM IDENTIFIER(:catalog || '.silver.products')
# MAGIC UNION ALL SELECT 'orders', count(*) FROM IDENTIFIER(:catalog || '.silver.orders')
# MAGIC UNION ALL SELECT 'order_items', count(*) FROM IDENTIFIER(:catalog || '.silver.order_items')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Customers with more than one version: the SCD2 history at work.
# MAGIC SELECT customer_id, valid_from, valid_to, is_current
# MAGIC FROM IDENTIFIER(:catalog || '.silver.customers')
# MAGIC WHERE customer_id IN (
# MAGIC   SELECT customer_id FROM IDENTIFIER(:catalog || '.silver.customers')
# MAGIC   GROUP BY customer_id HAVING count(*) > 1)
# MAGIC ORDER BY customer_id, valid_from
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The watermark history written by this notebook.
# MAGIC SELECT run_id, stage, batch_date, status, error_message, started_at
# MAGIC FROM IDENTIFIER(:catalog || '.audit.pipeline_runs')
# MAGIC ORDER BY started_at DESC
# MAGIC LIMIT 20
