# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold: star schema, aggregates and data quality
# MAGIC
# MAGIC Rebuilds the gold layer from the whole of silver with `lakehouse.pipeline.build_gold_layer`,
# MAGIC the same function the local runner calls:
# MAGIC
# MAGIC * `gold.dim_date`, `gold.dim_customer`, `gold.dim_product` - dimensions
# MAGIC * `gold.fact_sales` - one row per order line, partitioned by month
# MAGIC * `gold.agg_daily_sales`, `gold.agg_customer_lifetime_value` - pre-computed answers
# MAGIC * then every data quality suite, with results appended to `audit.dq_results`
# MAGIC
# MAGIC It only rebuilds when silver is ahead of gold (compare the `silver` and `gold` rows in
# MAGIC `audit.pipeline_runs`). If a blocking quality check fails, the gold stage is recorded as
# MAGIC `failed` and the next run tries again. Set `force_rebuild` to `true` to rebuild anyway,
# MAGIC for example after changing code under `src/lakehouse/gold/`.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

dbutils.widgets.dropdown("force_rebuild", "false", ["false", "true"], "Rebuild even if up to date")

# COMMAND ----------

import uuid

from lakehouse.pipeline import (
    STAGE_GOLD,
    RunRecord,
    build_gold_layer,
    gold_rebuild_target,
    read_run_state,
    run_stage,
)

force = dbutils.widgets.get("force_rebuild") == "true"
target = gold_rebuild_target(read_run_state(spark, config), force=force)

if target is None:
    print("Gold is up to date with silver (or silver is empty): nothing to do.")
else:
    run_id = uuid.uuid4().hex
    print(f"run {run_id}: rebuilding gold through {target}")
    # Records success or failure in audit.pipeline_runs and re-raises a failure,
    # so a job run that fails here shows as failed.
    run_stage(spark, config, RunRecord(run_id, STAGE_GOLD, target),
              lambda: build_gold_layer(spark, config))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check the results

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Row counts per gold table. The two entity dimensions include the unknown member (key -1).
# MAGIC SELECT 'dim_date' AS table_name, count(*) AS rows FROM IDENTIFIER(:catalog || '.gold.dim_date')
# MAGIC UNION ALL SELECT 'dim_customer', count(*) FROM IDENTIFIER(:catalog || '.gold.dim_customer')
# MAGIC UNION ALL SELECT 'dim_product', count(*) FROM IDENTIFIER(:catalog || '.gold.dim_product')
# MAGIC UNION ALL SELECT 'fact_sales', count(*) FROM IDENTIFIER(:catalog || '.gold.fact_sales')
# MAGIC UNION ALL SELECT 'agg_daily_sales', count(*) FROM IDENTIFIER(:catalog || '.gold.agg_daily_sales')
# MAGIC UNION ALL SELECT 'agg_customer_lifetime_value', count(*)
# MAGIC   FROM IDENTIFIER(:catalog || '.gold.agg_customer_lifetime_value')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Results of the most recent data quality run (failed checks first).
# MAGIC SELECT table_name, check_name, severity, passed, failed_rows, total_rows,
# MAGIC        round(failure_ratio, 4) AS failure_ratio, max_failure_ratio
# MAGIC FROM IDENTIFIER(:catalog || '.audit.dq_results')
# MAGIC WHERE run_id = (
# MAGIC   SELECT max_by(run_id, checked_at) FROM IDENTIFIER(:catalog || '.audit.dq_results'))
# MAGIC ORDER BY passed, table_name, check_name

# COMMAND ----------

# Delta keeps every version of a table. Each gold rebuild is one atomic
# overwrite, so it shows up here as a single operation (for example CREATE OR REPLACE
# TABLE AS SELECT on Unity Catalog), with the rows it wrote in operationMetrics.
from lakehouse.utils.tables import sql_ref

display(spark.sql(f"DESCRIBE HISTORY {sql_ref(config.table_path('gold', 'fact_sales'))}")
        .select("version", "timestamp", "operation", "operationMetrics")
        .limit(5))
