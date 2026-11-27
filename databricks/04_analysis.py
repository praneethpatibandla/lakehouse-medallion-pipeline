# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Analysis: SQL on the gold tables
# MAGIC
# MAGIC Business questions answered with SQL on the star schema built by `03_gold`. Each query
# MAGIC shows one technique worth knowing: window functions, a star join, a point-in-time join on
# MAGIC an SCD Type 2 dimension, conditional aggregation.
# MAGIC
# MAGIC Revenue rule (the same as in `lakehouse.gold.aggregates`): an order counts towards revenue
# MAGIC when its status is `placed`, `shipped` or `delivered`; cancelled and returned orders do not.
# MAGIC
# MAGIC Every table name is built from the `catalog` widget with `IDENTIFIER(...)`, so the
# MAGIC queries work in any catalog. To reuse a query in the SQL editor, keep `:catalog` (the
# MAGIC editor asks for its value) or replace the whole `IDENTIFIER(...)` with the table name,
# MAGIC e.g. `workspace.gold.fact_sales`.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Daily revenue with a 7-day moving average
# MAGIC Reads the small pre-computed `agg_daily_sales` table instead of the fact table. Quiet days
# MAGIC are rows with zeros (not missing rows), so the moving average really covers 7 days.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date,
# MAGIC        revenue_order_count,
# MAGIC        net_revenue,
# MAGIC        avg_order_value,
# MAGIC        round(avg(net_revenue) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2)
# MAGIC          AS net_revenue_7d_avg
# MAGIC FROM IDENTIFIER(:catalog || '.gold.agg_daily_sales')
# MAGIC ORDER BY date

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Revenue by category per month, with share and rank
# MAGIC A classic star join: the fact table joined to two dimensions on surrogate keys. The window
# MAGIC functions work on the grouped result, so each category is compared with its own month.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH monthly AS (
# MAGIC   SELECT d.year, d.month, d.month_name, p.category,
# MAGIC          sum(f.net_amount) AS net_revenue,
# MAGIC          sum(f.quantity) AS units_sold
# MAGIC   FROM IDENTIFIER(:catalog || '.gold.fact_sales') AS f
# MAGIC   JOIN IDENTIFIER(:catalog || '.gold.dim_date') AS d ON f.date_key = d.date_key
# MAGIC   JOIN IDENTIFIER(:catalog || '.gold.dim_product') AS p ON f.product_key = p.product_key
# MAGIC   WHERE f.order_status IN ('placed', 'shipped', 'delivered')
# MAGIC   GROUP BY d.year, d.month, d.month_name, p.category
# MAGIC )
# MAGIC SELECT year, month_name, category, units_sold, net_revenue,
# MAGIC        round(100 * net_revenue / sum(net_revenue) OVER (PARTITION BY year, month), 1)
# MAGIC          AS pct_of_month,
# MAGIC        rank() OVER (PARTITION BY year, month ORDER BY net_revenue DESC) AS rank_in_month
# MAGIC FROM monthly
# MAGIC ORDER BY year, month, rank_in_month

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Top 10 customers by lifetime value
# MAGIC From `agg_customer_lifetime_value`: one row per customer (not per SCD2 version), with
# MAGIC the segment and state of the customer's current version.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, segment, state, order_count, lifetime_net_revenue, avg_order_value,
# MAGIC        first_order_date, last_order_date, days_since_last_order
# MAGIC FROM IDENTIFIER(:catalog || '.gold.agg_customer_lifetime_value')
# MAGIC ORDER BY lifetime_net_revenue DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Why SCD Type 2 matters: state at purchase vs current state
# MAGIC Each sale points at the customer *version* that was valid on the order date. This query
# MAGIC finds revenue that a report would put in the wrong state if it joined on the customer's
# MAGIC current address instead. With untouched sample data the result is empty, because the
# MAGIC generator never moves a customer; the build log (step 21) shows how to move one.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH sales AS (
# MAGIC   SELECT c.customer_id, c.state AS state_at_purchase, f.order_id, f.net_amount
# MAGIC   FROM IDENTIFIER(:catalog || '.gold.fact_sales') AS f
# MAGIC   JOIN IDENTIFIER(:catalog || '.gold.dim_customer') AS c ON f.customer_key = c.customer_key
# MAGIC   WHERE f.order_status IN ('placed', 'shipped', 'delivered')
# MAGIC     AND f.customer_key <> -1                -- skip the unknown member
# MAGIC ),
# MAGIC current_version AS (
# MAGIC   SELECT customer_id, state AS current_state
# MAGIC   FROM IDENTIFIER(:catalog || '.gold.dim_customer')
# MAGIC   WHERE is_current AND customer_key <> -1
# MAGIC )
# MAGIC SELECT s.state_at_purchase, cv.current_state,
# MAGIC        count(DISTINCT s.customer_id) AS customers,
# MAGIC        count(DISTINCT s.order_id) AS orders,
# MAGIC        sum(s.net_amount) AS net_revenue
# MAGIC FROM sales AS s
# MAGIC JOIN current_version AS cv ON s.customer_id = cv.customer_id
# MAGIC WHERE s.state_at_purchase <> cv.current_state
# MAGIC GROUP BY s.state_at_purchase, cv.current_state
# MAGIC ORDER BY net_revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Cancellation and return rates by payment method
# MAGIC The fact table's grain is the order *line*, so orders are counted with
# MAGIC `count(DISTINCT order_id)`; a plain `count(*)` would count an order once per line.
# MAGIC `count(DISTINCT CASE WHEN ... THEN order_id END)` is conditional aggregation: the CASE
# MAGIC returns NULL for other rows, and NULLs are not counted.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT payment_method,
# MAGIC        count(DISTINCT order_id) AS orders,
# MAGIC        round(100 * count(DISTINCT CASE WHEN order_status = 'cancelled' THEN order_id END)
# MAGIC              / count(DISTINCT order_id), 1) AS pct_cancelled,
# MAGIC        round(100 * count(DISTINCT CASE WHEN order_status = 'returned' THEN order_id END)
# MAGIC              / count(DISTINCT order_id), 1) AS pct_returned
# MAGIC FROM IDENTIFIER(:catalog || '.gold.fact_sales')
# MAGIC GROUP BY payment_method
# MAGIC ORDER BY orders DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Average revenue by day of week
# MAGIC Joins the daily aggregate to `dim_date` for calendar attributes. Calendar logic lives in
# MAGIC the date dimension, so every report agrees on what a "weekend" is.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT d.day_of_week, d.day_name, d.is_weekend,
# MAGIC        count(*) AS days,
# MAGIC        round(avg(a.net_revenue), 2) AS avg_net_revenue,
# MAGIC        round(avg(a.revenue_order_count), 1) AS avg_orders
# MAGIC FROM IDENTIFIER(:catalog || '.gold.agg_daily_sales') AS a
# MAGIC JOIN IDENTIFIER(:catalog || '.gold.dim_date') AS d ON a.date_key = d.date_key
# MAGIC GROUP BY d.day_of_week, d.day_name, d.is_weekend
# MAGIC ORDER BY d.day_of_week

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Repeat customers by segment
# MAGIC Share of customers who ordered more than once, and how much more they spend.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT segment,
# MAGIC        count(*) AS customers,
# MAGIC        round(100 * avg(CASE WHEN order_count > 1 THEN 1 ELSE 0 END), 1) AS pct_repeat,
# MAGIC        round(avg(CASE WHEN order_count > 1 THEN lifetime_net_revenue END), 2)
# MAGIC          AS avg_value_repeat,
# MAGIC        round(avg(CASE WHEN order_count = 1 THEN lifetime_net_revenue END), 2)
# MAGIC          AS avg_value_one_time
# MAGIC FROM IDENTIFIER(:catalog || '.gold.agg_customer_lifetime_value')
# MAGIC GROUP BY segment
# MAGIC ORDER BY customers DESC
