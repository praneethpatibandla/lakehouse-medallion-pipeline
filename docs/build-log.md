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
