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

## Run the tests

```bash
pytest -q
```
