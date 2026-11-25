# lakehouse-medallion-pipeline

An end-to-end batch data pipeline for e-commerce orders, built with **PySpark**
and **Delta Lake** using the medallion (bronze / silver / gold) architecture.
It runs entirely on a laptop, and a later phase runs the same pipeline on
Databricks (Free Edition) with Unity Catalog.

> Work in progress. See [ROADMAP.md](ROADMAP.md) for what is done and what is next.

## What it does

```
raw CSV / JSON files
        |
        v
  BRONZE  raw data as received, plus load metadata
        |
        v
  SILVER  cleaned, deduplicated, incrementally merged
        |
        v
  GOLD    star schema and aggregates ready for analytics
```

## Project layout

```
src/lakehouse/
  bronze/    raw ingestion
  silver/    cleaning, deduplication, merges
  gold/      dimensions, facts, aggregates
  quality/   data quality checks
  utils/     config, logging, Spark session
  pipeline.py      end-to-end runner with incremental watermarks
  maintenance.py   OPTIMIZE / Z-ORDER / VACUUM job
data_generator/   creates synthetic source data
tests/            unit tests
docs/             build log and design notes
data/             local data, generated at runtime (not committed)
```

## Setup

Requires Python 3.10+ and Java 11 or 17 (needed by Spark).

```bash
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Check that Spark and Delta Lake work (the first run downloads the Delta jars):

```bash
PYTHONPATH=src python -m lakehouse.utils.spark
```

## Generate sample data

The pipeline reads synthetic source files from `data/landing/`. Create them
from the repository root:

```bash
# customers and products (reference data)
PYTHONPATH=src python -m data_generator.reference_data --customers 500 --products 100

# orders and order items, one folder per day, ~5% deliberately dirty records
PYTHONPATH=src python -m data_generator.orders --start-date 2026-11-01 --days 3 --orders-per-day 200
```

## Run the whole pipeline

One command loads every new batch date through bronze and silver, then
rebuilds gold and runs the data quality checks:

```bash
PYTHONPATH=src python -m lakehouse.pipeline --dry-run   # show what would run
PYTHONPATH=src python -m lakehouse.pipeline
```

The runner keeps a watermark (the newest batch date already processed) in
`data/audit/pipeline_runs/`, so running it again only processes dates that
have landed since. Generate another day and run it again to see this. Useful
options: `--until DATE`, `--max-batches N` and `--maintenance` (also run
OPTIMIZE / VACUUM). If a run fails it stops at that date; fix the cause and
run it again to continue from there. To start from scratch, delete every
folder in `data/` except `landing/`.

The sections below run each step on its own, which is useful for learning
and debugging.

## Load bronze

Load one day of landing files into bronze Delta tables under `data/bronze/`.
Re-running a day replaces that day instead of duplicating it.

```bash
PYTHONPATH=src python -m lakehouse.bronze.ingest --batch-date 2026-11-01
```

Lines that cannot be parsed go to `data/quarantine/<source>/` with their raw
text, and every load is recorded in the audit table `data/audit/ingestion_log/`.
If more than 5% of a batch is corrupt the load stops before bronze is updated
(change the limit with `--max-quarantine-ratio`).

## Build silver

Merge one day of bronze orders into the silver table `data/silver/orders/`.
Duplicate copies of an order are collapsed to the newest one, new orders are
inserted and changed orders are updated (Delta `MERGE`). Re-running a day, or
running an older day late, never makes the data older or duplicated.

```bash
PYTHONPATH=src python -m lakehouse.silver.orders --batch-date 2026-11-01
```

Customers are kept as a Type 2 slowly changing dimension in
`data/silver/customers/`: a changed customer gets a new version, and the old
one is closed with `valid_to` and `is_current = false`. Snapshots must be
applied in date order.

```bash
PYTHONPATH=src python -m lakehouse.silver.customers --batch-date 2026-11-01
```

Products are kept as a current-state table in `data/silver/products/`, and
order items in `data/silver/order_items/` with line amounts (gross, discount,
net) and flags for lines whose product or order is unknown. Order items are
checked against silver orders and products, so build those first:

```bash
PYTHONPATH=src python -m lakehouse.silver.products --batch-date 2026-11-01
PYTHONPATH=src python -m lakehouse.silver.order_items --batch-date 2026-11-01
```

## Build gold

Rebuild the dimension tables of the star schema in `data/gold/`: `dim_date`
(calendar), `dim_customer` (one row per customer version) and `dim_product`.
Each has a surrogate key, plus an "unknown" row with key `-1` for sales whose
customer or product is missing.

```bash
PYTHONPATH=src python -m lakehouse.gold.dimensions
```

Then rebuild the fact table `fact_sales`: one row per order line, with keys
to the three dimensions and the line amounts. Each sale points at the
customer version that was valid on the order date. Lines with numbers or
dates that cannot be trusted (bad quantity, unknown order, no timestamp) are
left out and counted in the log; lines with an unknown product or customer
are kept under key `-1`.

```bash
PYTHONPATH=src python -m lakehouse.gold.facts
```

Finally, rebuild the aggregate tables that dashboards read: `agg_daily_sales`
(one row per day, with zeros on days without orders) and
`agg_customer_lifetime_value` (one row per customer). Cancelled and returned
orders are counted but do not add to revenue.

```bash
PYTHONPATH=src python -m lakehouse.gold.aggregates
```

`fact_sales` is partitioned by month (`month_key`, e.g. `202611`). Filter on
`month_key` in queries to read only that month's files.

## Check data quality

Run the data quality rules for silver and gold (unique keys, no orphan
foreign keys, valid amounts, SCD2 consistency, share of flagged rows):

```bash
PYTHONPATH=src python -m lakehouse.quality.suites            # every table
PYTHONPATH=src python -m lakehouse.quality.suites --layer gold
```

Failed `error` checks stop the job with a non-zero exit code; `warn` checks
are only logged. Every result is appended to `data/audit/dq_results/`.

## Maintain the tables

Daily loads and merges leave many small files behind, and old file versions
are kept for time travel. Compact every table (and Z-ORDER the large ones on
their join and filter columns), then delete files that have been unused for
more than 7 days:

```bash
PYTHONPATH=src python -m lakehouse.maintenance
# only some tables, or only list what VACUUM would delete
PYTHONPATH=src python -m lakehouse.maintenance --tables silver.orders gold.fact_sales
PYTHONPATH=src python -m lakehouse.maintenance --vacuum-dry-run
```

## Run the tests

```bash
pytest -q                 # everything
pytest -q -m "not spark"  # only the fast pure-Python tests (no Java needed)
pytest -q -m spark -rs    # only the Spark / Delta tests, showing skip reasons
```

Spark tests share one local SparkSession (see `tests/conftest.py`). They are
skipped, not failed, when pyspark, delta-spark or Java is missing; `-rs`
prints why.
