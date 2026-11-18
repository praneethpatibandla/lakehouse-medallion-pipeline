# Build log

One entry per roadmap step: what was added, and why it matters.

## 01. Project structure, dependencies and roadmap

**What:** Created the folder layout, pinned dependencies in `requirements.txt`,
added a `.gitignore`, and wrote the roadmap.

**Why:**
- The `bronze / silver / gold` folders mirror the medallion architecture, so
  anyone opening the repo can see the data flow from the layout alone.
- Versions are pinned because PySpark and Delta Lake must be compatible with
  each other (`delta-spark` 3.2.x works with Spark 3.5.x). Unpinned installs are
  a common cause of "works on my machine" failures.
- `data/` is ignored by git. Data is generated at runtime; committing data
  files bloats a repository and is rarely acceptable in a real team.

## 02. Central configuration and logging

**What:** Added `src/lakehouse/utils/config.py` (all paths and settings in one
frozen dataclass, overridable with environment variables),
`src/lakehouse/utils/logger.py` (one shared log format), and tests for both.

**Why:**
- No module hard-codes a path. To move the pipeline from a laptop to cloud
  storage you change `LAKEHOUSE_DATA_ROOT`, not the code. This is the
  "config separate from code" principle.
- The dataclass is `frozen`, so settings cannot be changed halfway through a
  run by accident.
- Invalid settings fail immediately with a clear message, instead of failing
  twenty minutes into a Spark job.
- A consistent log format (`time | level | module | message`) makes failed runs
  quick to diagnose.

**Try it:**
```bash
pytest tests/test_config.py -v
```

## 03. Synthetic data generator: customers and products

**What:** Added `data_generator/reference_data.py`, which creates customers and
products and writes them as CSV files into the landing zone
(`data/landing/customers/customers.csv` and `data/landing/products/products.csv`),
plus tests in `tests/test_reference_data.py`.

**Why:**
- Customers and products are *reference* (master) data: small tables that
  orders point at. They are generated first so every order created in the next
  step can reference IDs that really exist.
- Everything is driven by a seed. Running the generator twice gives identical
  files, so a bug found in the pipeline can be reproduced with the same input.
- Prices are written as strings with exactly two decimals. Money should never
  be stored as a float, because floats cannot represent most cents exactly;
  later the silver layer will cast these to `DECIMAL(10,2)`.
- City and state are picked as a pair, so the data is internally consistent.
  Unrealistic random data hides bugs that real data would expose.
- Some products are inactive (discontinued). A real catalogue keeps old
  products because historical orders still reference them, and the gold layer
  needs to handle that.
- The generator uses only the standard library, so it is fast to test and does
  not need Spark. Writing to a separate `landing` folder mimics a source system
  dropping files that the pipeline then picks up.

**Try it:**
```bash
PYTHONPATH=src python -m data_generator.reference_data --customers 500 --products 100
head -3 data/landing/customers/customers.csv
pytest tests/test_reference_data.py -v
```

## 04. Synthetic data generator: orders and order items in daily batches

**What:** Added `data_generator/orders.py`, which writes one folder per day:
orders as JSON lines (`data/landing/orders/batch_date=YYYY-MM-DD/orders.json`)
and order items as CSV (`data/landing/order_items/batch_date=YYYY-MM-DD/order_items.csv`).
About 5% of orders get one deliberate defect. Tests are in
`tests/test_orders_generator.py`.

**Why:**
- **Daily batches** are how most batch pipelines receive data: a source system
  exports one drop per day. One folder per `batch_date` lets the pipeline load
  only new days (incremental loads, step 17) and lets you re-run a single day
  without touching the others. The `key=value` folder name is the Hive
  partition convention that Spark understands automatically.
- **Two formats** (JSON and CSV) on purpose: ingestion code in a real job has
  to handle more than one, and each format fails in different ways.
- **Header and line tables** (orders vs order items) is the normal shape of
  transactional data. Joining them correctly is what the gold fact table does.
- **The unit price is copied onto each order item.** If the catalogue price
  changes later, past orders must still show the price the customer paid.
- **Dirty records** are what the later layers exist to handle. Each defect maps
  to a technique you will see in the pipeline:
  - duplicate (a later copy with a newer `updated_at`) -> deduplicate and keep
    the latest version, then Delta MERGE (step 09)
  - corrupt JSON line -> quarantine instead of failing the whole load (step 07)
  - null or unknown `customer_id`, zero or negative quantity -> data quality
    rules (steps 08 and 16)
  - `18/11/2026 14:05` style timestamps and `"  Shipped "` statuses ->
    cleaning and standardisation (step 08)
- The random generator is seeded with the seed **and** the date, so every day
  is different but re-running a day produces exactly the same file. Re-running
  is safe because writing a date replaces that date's folder content.

**Try it:**
```bash
PYTHONPATH=src python -m data_generator.reference_data
PYTHONPATH=src python -m data_generator.orders --start-date 2026-11-01 --days 3 --orders-per-day 200
head -3 data/landing/orders/batch_date=2026-11-01/orders.json
pytest tests/test_orders_generator.py -v
```
The log line for each day shows how many of each defect were injected.
