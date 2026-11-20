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

## 05. Spark session factory configured for Delta Lake

**What:** Added `src/lakehouse/utils/spark.py` with `get_spark()`, the one place
every job gets its SparkSession from, plus `build_spark_conf()` (the settings
as a plain dict) and a small smoke test you can run from the command line.
Tests are in `tests/test_spark_session.py`.

**Why:**
- **Delta needs two settings** on the session: the SQL extension (adds `MERGE`,
  `OPTIMIZE`, `VACUUM`, `DESCRIBE HISTORY`) and the Delta catalog. Without them
  `format("delta")` writes still partly work, but SQL commands fail with
  confusing errors. Putting them in one factory means no job can forget them.
- **`configure_spark_with_delta_pip`** adds the Delta jars that match the
  installed `delta-spark` package. This is why step 01 pinned
  `pyspark==3.5.3` with `delta-spark==3.2.1`: a mismatched pair fails at
  start-up with class-not-found errors.
- **Shuffle partitions come from config** (default 4). Spark's default of 200
  is sized for clusters; on a laptop with a few MB of data it creates hundreds
  of tiny tasks and tiny files, which is slow and is the "small files problem"
  you will see again in step 15.
- **UTC session time zone** so timestamps mean the same thing on every machine.
  A pipeline whose output depends on the laptop's time zone is a bug waiting
  to happen.
- **Databricks detection.** On Databricks a session already exists, Delta is
  built in, and serverless compute rejects most session settings. When the
  `DATABRICKS_RUNTIME_VERSION` environment variable is present, `get_spark()`
  simply returns the existing session, so the same job code runs in both
  places (used in Phase 6).
- **pyspark is imported inside the function.** Pure-Python modules and tests
  can import this file without pyspark installed; only code that really needs
  Spark pays the start-up cost. `build_spark_conf()` is separate for the same
  reason: the settings can be unit tested without a JVM.

**Try it:**
```bash
PYTHONPATH=src python -m lakehouse.utils.spark   # needs Java 11 or 17
pytest tests/test_spark_session.py -v
```
The smoke test writes a three-row Delta table under `data/_smoke_test/` and
logs its row count and table version. The last test in the file starts a real
Spark session and is skipped automatically if pyspark is not installed.

## 06. Explicit schemas and raw ingestion into Delta with load metadata

**What:** Added `src/lakehouse/bronze/schemas.py` (the column list of every
source and an all-string Spark schema) and `src/lakehouse/bronze/ingest.py`,
which reads one batch date from the landing zone, adds load metadata columns
and writes it into a bronze Delta table partitioned by `_batch_date`. Tests
are in `tests/test_bronze_ingest.py`.

**Why:**
- **Explicit schema instead of `inferSchema`.** Inference reads the data an
  extra time, and its answer can change from one file to the next (a column
  that was an integer yesterday becomes a string today because of one bad
  value). A fixed schema makes bronze predictable.
- **Every column is a string in bronze.** Bronze is "data as received". If
  bronze cast `order_ts` to a timestamp, the value `18/11/2026 14:05` would
  silently become NULL and the original text would be gone. Casting is done in
  silver (step 08), where failures can be counted and handled.
- **`enforceSchema=false` for CSV** makes Spark check the header against the
  schema. If the source system renames or reorders a column, the load fails
  loudly instead of putting values in the wrong columns.
- **Load metadata columns** (`_source_file`, `_ingested_at`, `_batch_date`,
  `_run_id`) answer "where did this row come from and when?" — the first
  question in any data incident. The underscore prefix marks them as pipeline
  columns, and a test checks they never clash with source columns.
- **`_metadata.file_path` instead of `input_file_name()`.** Both give the
  source file, but Unity Catalog on Databricks does not support
  `input_file_name()`, so this keeps the code portable to Phase 6.
- **Idempotent reruns with `replaceWhere`.** A plain append would double the
  rows if a day is loaded twice; a plain overwrite would delete every other
  day. `mode("overwrite")` plus `replaceWhere _batch_date = '<day>'` replaces
  just that day's partition in one atomic Delta transaction, so re-running a
  failed or corrected day is always safe.
- **Reference data is stored as daily snapshots.** Customers and products
  arrive as one full file. Each load keeps that day's snapshot as its own
  partition, so silver can compare snapshots to detect changes for the SCD
  Type 2 customer dimension (step 10).
- **Contract test.** `test_schemas_match_generator_output` fails if the data
  generator and the bronze schema drift apart, the local version of a data
  contract between a producer and a consumer.
- **Known gap:** corrupt JSON lines currently load as all-NULL rows. Step 07
  keeps their raw text and moves them to a quarantine table.

**Try it:**
```bash
PYTHONPATH=src python -m data_generator.reference_data
PYTHONPATH=src python -m data_generator.orders --start-date 2026-11-01 --days 2
PYTHONPATH=src python -m lakehouse.bronze.ingest --batch-date 2026-11-01
PYTHONPATH=src python -m lakehouse.bronze.ingest --batch-date 2026-11-01   # same counts: idempotent
PYTHONPATH=src python -m lakehouse.bronze.ingest --batch-date 2026-11-02
pytest tests/test_bronze_ingest.py -v
```
To look at the result in a PySpark shell:
```python
from lakehouse.utils.spark import get_spark
spark = get_spark()
spark.read.format("delta").load("data/bronze/orders").groupBy("_batch_date").count().show()
spark.sql("DESCRIBE HISTORY delta.`data/bronze/orders`").select("version", "operation").show()
```

## 07. Corrupt-record quarantine and ingestion audit log

**What:** Bronze ingestion now separates unparseable records from good ones.
Bad lines are written with their raw text to a quarantine Delta table
(`data/quarantine/<source>/`), and every load appends a row to an audit Delta
table (`data/audit/ingestion_log/`). New code is in
`src/lakehouse/bronze/audit.py`; `ingest.py` gained `split_corrupt()` and a
`--max-quarantine-ratio` option. Tests are in `tests/test_bronze_audit.py`.

**Why:**
- **Don't silently drop, don't silently load.** Before this step a truncated
  JSON line became a row of NULLs in bronze. Reading with
  `columnNameOfCorruptRecord` keeps the original text in `_corrupt_record`, so
  the bad line can be inspected, reported to the source team, fixed and
  replayed. This is the "dead-letter queue" idea from streaming, applied to
  batch files.
- **Quarantine, check, then publish.** Corrupt rows are written first and
  counted, then `check_quarantine_threshold()` decides whether the batch is
  trustworthy. A few bad lines are normal; a file that is 30% garbage usually
  means a broken export, and loading the 70% that parsed would publish an
  incomplete day that looks complete. If the limit is exceeded the run fails
  *before* bronze is touched, so downstream layers never see a half-good day.
- **Audit log as a Delta table, append-only.** Each load records run id,
  source, batch date, rows read / loaded / quarantined, status, error and
  duration. Operational questions ("did yesterday load? are bad rows
  increasing?") become a SQL query instead of digging through logs. Failed
  runs are logged too, which is exactly when you need the record most.
  Appending (never overwriting) keeps the full history of reruns.
- **Reconciliation.** `rows_read = rows_loaded + rows_quarantined` is checked
  and a warning is logged if it does not hold. Row-count reconciliation
  between stages is one of the cheapest and most useful data quality checks.
- **Quarantine is idempotent too.** It uses the same `replaceWhere` partition
  overwrite as bronze (the function was renamed to `write_partition`), so a
  rerun clears the previous run's quarantined rows for that day, even when the
  new run has none.
- **A Spark gotcha handled on purpose.** Spark refuses queries on raw JSON/CSV
  that select *only* the corrupt-record column. The quarantine table keeps all
  columns (the source columns are just NULL), which avoids the error without
  `.cache()` — caching is not available on Databricks serverless compute,
  which Phase 6 uses.
- **Pure-Python audit record.** `AuditRecord` and the threshold check need no
  Spark, so their rules are unit tested in milliseconds; only the final write
  uses Spark.

**Try it:**
```bash
PYTHONPATH=src python -m data_generator.orders --start-date 2026-11-01 --days 1
PYTHONPATH=src python -m lakehouse.bronze.ingest --batch-date 2026-11-01
pytest tests/test_bronze_audit.py -v
```
Inspect the results in a PySpark shell:
```python
from lakehouse.utils.spark import get_spark
spark = get_spark()
spark.read.format("delta").load("data/quarantine/orders").select("_batch_date", "_corrupt_record").show(truncate=False)
spark.read.format("delta").load("data/audit/ingestion_log").orderBy("started_at") \
    .select("source", "batch_date", "status", "rows_read", "rows_loaded", "rows_quarantined").show()
```
To see the threshold stop a load, run with `--max-quarantine-ratio 0` on a day
that has at least one corrupt line: the command fails, bronze is unchanged,
and a `failed` row appears in the audit log.

## 08. Cleaning and standardisation transforms as pure functions

**What:** Added `src/lakehouse/silver/transforms.py`: small, side-effect-free
functions that turn an all-string bronze DataFrame into a typed, tidy one —
`clean_string`, `parse_timestamp`, `safe_cast`, `cast_columns`,
`standardize_case`, `add_dq_issues` — and one composed cleaner per source
(`clean_orders`, `clean_order_items`, `clean_customers`, `clean_products`).
Tests are in `tests/test_silver_transforms.py`. Nothing writes to silver yet;
the silver jobs in steps 09-11 apply these with `df.transform(clean_orders)`.

**Why:**
- **Pure functions (DataFrame in, DataFrame out).** No reads, writes or
  global state, so each rule can be tested with a two-row DataFrame built in
  the test, and the same code runs unchanged locally and on Databricks.
  `DataFrame.transform` lets jobs chain them in a readable pipeline.
- **Casting happens here, not in bronze.** Bronze kept everything as text
  (step 06). Silver converts to real types: timestamps, `DATE`, `INT`,
  `BOOLEAN`, and `DECIMAL(10,2)` for money (never `DOUBLE`, which cannot store
  most cents exactly).
- **`try_cast` / `try_to_timestamp` instead of `cast` / `to_timestamp`.** With
  ANSI SQL mode on — the default on Databricks serverless — a plain cast
  raises an error on the first bad value and the whole job fails. The `try_`
  versions return NULL, so one bad row does not stop the batch.
- **Multiple timestamp formats.** The source sometimes sends `18/11/2026
  14:05` instead of ISO-8601. `parse_timestamp` tries each known layout in
  order with `coalesce`, so known variants are rescued and only truly
  unparseable values become NULL.
- **Flag, don't drop: the `_dq_issues` column.** Each row gets an array of the
  rules it broke (`invalid_order_ts`, `missing_customer_id`, `unknown_status`,
  `non_positive_quantity`, ...). An empty array means clean. A NULL created by
  a failed cast is flagged as `invalid_<column>`, so it can be told apart from
  a value that was missing in the source. Whether to drop, fix or keep a
  flagged row is decided by the job that uses it, and the issues are easy to
  count: `explode(_dq_issues)` then `groupBy`.
- **Standardisation.** Strings are trimmed and empty strings become NULL, so
  one `isNull()` check catches every kind of "missing". Codes are given one
  case (`"  Shipped "` becomes `shipped`, `usd` becomes `USD`) so joins and
  group-bys don't split the same value into several.
- **Declarative types.** `TARGET_TYPES` lists the type of each column in one
  dictionary. A test checks it only names real source columns, and another
  checks the allowed order statuses match the source system's list, so the
  cleaning rules can't drift away from the data they clean.
- **Pipeline metadata is untouched.** Underscore columns (`_batch_date`,
  `_run_id`, ...) are passed through as is, keeping lineage back to bronze.

**Try it:**
```bash
pytest tests/test_silver_transforms.py -v
```
Clean a day of bronze orders in a PySpark shell (after running steps 06-07):
```python
from pyspark.sql import functions as F
from lakehouse.utils.spark import get_spark
from lakehouse.silver.transforms import clean_orders
spark = get_spark()
orders = spark.read.format("delta").load("data/bronze/orders").transform(clean_orders)
orders.printSchema()
orders.select(F.explode("_dq_issues").alias("issue")).groupBy("issue").count().show()
```
