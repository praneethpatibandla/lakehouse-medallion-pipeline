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

## Run the tests

```bash
pytest -q
```
