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

## 09. Deduplication and incremental upserts for orders with Delta MERGE

**What:** Added the first silver job, `src/lakehouse/silver/orders.py`, and the
shared helpers it uses in `src/lakehouse/silver/merge.py`. For one batch date
the job reads that day's bronze partition, cleans it with `clean_orders`
(step 08), drops rows without an `order_id`, keeps the newest copy of each
order and MERGEs the result into the Delta table `data/silver/orders/`. Tests
are in `tests/test_silver_merge.py`.

**Why:**
- **Incremental, not full reload.** Each run processes only one new bronze
  partition and merges it into silver. Rebuilding silver from all of bronze
  every day gets slower as history grows; an upsert costs roughly the size of
  the new batch.
- **MERGE = insert new, update existing, in one atomic commit.** A new
  `order_id` is inserted; a known one is updated. Delta writes the whole
  MERGE as one transaction, so a failed run leaves the table as it was.
- **Deduplicate before merging.** The source re-sends an order when its status
  changes, so one batch can hold several copies of the same order. Delta
  refuses a MERGE where two source rows match one target row ("multiple
  source rows matched"), because it cannot know which should win.
  `deduplicate_latest` uses `row_number()` over a window partitioned by the
  key and ordered by `updated_at` (newest first, NULLs last), with
  `_ingested_at` as the tie-breaker. It runs *after* casting: compared as
  strings, `18/11/2026 14:05` would sort after `2026-11-19T...` and the wrong
  copy would win.
- **Out-of-order protection.** The update only happens when
  `s.updated_at >= t.updated_at`. If Monday's batch is replayed after
  Tuesday's, Monday's older "placed" status cannot overwrite Tuesday's
  "shipped". `>=` (not `>`) still lets a rerun of the same day go through, so
  reruns stay idempotent: same input, same table.
- **Rows without a key are skipped and logged.** A row with no `order_id`
  can never be matched or updated later, so it is counted and reported
  instead of being inserted as an orphan. Other data quality problems are
  kept and flagged in `_dq_issues` (step 08); gold decides what to exclude.
- **One code path for the first and later runs.**
  `DeltaTable.createIfNotExists` creates an empty table with the right schema,
  then the same MERGE runs every time. No special "first load" branch to
  test separately.
- **Metrics from the transaction log.** After the MERGE, Delta's
  `operationMetrics` (rows inserted / updated) are read from the table
  history and logged, so each run reports what it changed without an extra
  count query.
- **Reusable helpers.** `merge_condition`, `newer_condition` and
  `parse_merge_metrics` are plain Python and unit tested without Spark;
  customers, products and order items (steps 10-11) reuse the same pieces.

**Try it:**
```bash
PYTHONPATH=src python -m data_generator.reference_data
PYTHONPATH=src python -m data_generator.orders --start-date 2026-11-01 --days 2
PYTHONPATH=src python -m lakehouse.bronze.ingest --batch-date 2026-11-01
PYTHONPATH=src python -m lakehouse.bronze.ingest --batch-date 2026-11-02
PYTHONPATH=src python -m lakehouse.silver.orders --batch-date 2026-11-01
PYTHONPATH=src python -m lakehouse.silver.orders --batch-date 2026-11-02
# run day 1 again: 0 inserted, the log shows the rows were only refreshed
PYTHONPATH=src python -m lakehouse.silver.orders --batch-date 2026-11-01
pytest tests/test_silver_merge.py -v
```
Check that every order appears once, and look at the MERGE history:
```python
from lakehouse.utils.spark import get_spark
spark = get_spark()
orders = spark.read.format("delta").load("data/silver/orders")
orders.groupBy("order_id").count().where("count > 1").show()   # expect no rows
spark.sql("DESCRIBE HISTORY delta.`data/silver/orders`") \
    .select("version", "operation", "operationMetrics").show(truncate=False)
```

## 10. Slowly changing dimension (Type 2) for customers

**What:** Added `src/lakehouse/silver/customers.py`. Each daily customers
snapshot from bronze is cleaned, deduplicated and applied to
`data/silver/customers/` as a Type 2 slowly changing dimension: when a
customer's attributes change, the old row is closed and a new version is
added. Rows carry `valid_from`, `valid_to`, `is_current` and `_row_hash`.
Tests are in `tests/test_silver_customers.py`. `merge.py` also gained
`current_version()`, so a MERGE that changes nothing reports zero rows instead
of the counts of an older commit.

**Why:**
- **Type 1 vs Type 2.** Type 1 overwrites the old value: simple, but history
  is lost, so "sales by customer city" would move a customer's past orders to
  their new city. Type 2 keeps every version with a validity range, so gold
  can join each order to the version that was valid on the order date
  (`order_date >= valid_from AND (valid_to IS NULL OR order_date < valid_to)`).
- **Change detection with a hash.** `_row_hash` is a SHA-256 of the tracked
  attributes. Comparing one column is simpler and cheaper than comparing
  every attribute with NULL-safe equality. NULLs are replaced by a token
  before hashing, because `concat_ws` skips NULLs and `("a", NULL, "b")`
  would otherwise look the same as `("a", "b", NULL)`.
- **Hash cleaned values, track only business attributes.** The hash is built
  after `clean_customers`, so `" alan "` vs `Alan` is not a "change". Pipeline
  columns like `_run_id` are left out of the hash; if they were in it, every
  load would create a new version of every customer.
- **One atomic MERGE with the "staged union" trick.** A MERGE can do one
  action per matched row, but a changed customer needs two: close the old
  row *and* insert a new one. So a changed customer is staged twice: once
  with `_merge_key = customer_id` (matches the current row, which is closed)
  and once with `_merge_key = NULL` (matches nothing, so it is inserted).
  Both happen in one Delta transaction, so a customer never has zero or two
  current rows, even if the job crashes.
- **Idempotent reruns.** Re-applying the same snapshot finds identical hashes
  and changes nothing.
- **Forward-only history.** `check_batch_order` refuses a snapshot older than
  the newest `valid_from` in the table. Applying 1 Nov after 3 Nov would
  close the 3 Nov versions and make old data look current. Backfilling
  history means rebuilding the table in date order.
- **`valid_to` is exclusive.** The closed row's `valid_to` equals the new
  row's `valid_from`, so ranges never overlap or leave gaps, and a date falls
  in exactly one version.
- **Known limitation.** A customer who disappears from the snapshot stays
  current. Handling deletes (closing the row, or a "deleted" flag) needs a
  rule agreed with the source team, because a missing row can also mean a
  broken export.

**Try it:** the generator always writes the same customers, so change one by
hand to see a new version appear:
```bash
PYTHONPATH=src python -m data_generator.reference_data
PYTHONPATH=src python -m lakehouse.bronze.ingest --batch-date 2026-11-01 --tables customers
PYTHONPATH=src python -m lakehouse.silver.customers --batch-date 2026-11-01

# move customer C00001 to Denver, CO, then load the next day's snapshot
python - <<'PY'
import csv
path = "data/landing/customers/customers.csv"
rows = list(csv.DictReader(open(path)))
rows[0].update(city="Denver", state="CO")
with open(path, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
PY
PYTHONPATH=src python -m lakehouse.bronze.ingest --batch-date 2026-11-02 --tables customers
PYTHONPATH=src python -m lakehouse.silver.customers --batch-date 2026-11-02
pytest tests/test_silver_customers.py -v
```
Look at the two versions of the customer:
```python
from lakehouse.utils.spark import get_spark
spark = get_spark()
spark.read.format("delta").load("data/silver/customers").where("customer_id = 'C00001'") \
    .select("customer_id", "city", "state", "valid_from", "valid_to", "is_current").show()
```
Running `--batch-date 2026-11-01` again now fails on purpose with
`OutOfOrderBatchError`.

## 11. Conformed products and order items

**What:** Added `src/lakehouse/silver/products.py` and
`src/lakehouse/silver/order_items.py`. Products are merged into
`data/silver/products/` as a Type 1 table (one current row per product).
Order items are merged into `data/silver/order_items/` on the composite key
`(order_id, line_number)`, get three money columns (`line_gross_amount`,
`line_discount_amount`, `line_net_amount`), and are checked against silver
orders and products: a line whose product is not in the catalogue gets
`unknown_product` in `_dq_issues`, a line whose order is not in silver gets
`orphan_order`. In `transforms.py`, `_append_issues` became the public
`append_dq_issues`, since other modules now use it. Tests are in
`tests/test_silver_conformed.py`.

**Why:**
- **What "conformed" means.** Every silver table uses the same key formats,
  types and rules, so gold can join them without cleaning again. Product IDs
  are upper-cased the same way in products and order items, money is DECIMAL
  everywhere, and references between tables are checked once, here.
- **Products are Type 1, customers are Type 2.** The price is copied onto
  every order line when the order is placed, so revenue never depends on
  today's catalogue price. That removes the main reason to keep product
  history. The cost: if a product changes category, its past sales move with
  it. Choosing SCD type per table, based on what the business reports on, is
  a common interview question.
- **Composite key.** One order has several lines, so `order_id` alone is not
  unique. `merge_condition` from step 09 already supports several keys, so
  the same `upsert` helper works unchanged.
- **No change timestamp? Use the batch date.** Products and items have no
  `updated_at`, so `_batch_date` is the sequence column: a late replay of an
  older day cannot overwrite data from a newer one, and reruns of the same
  day are still allowed (`>=`).
- **Money rules written once, in Python.** `line_amounts` is the reference
  rule (round the discount half-up to the cent, then net = gross - discount,
  so the parts always add up). The Spark version must give the same answer,
  and a Spark test compares them row by row. Amounts stay DECIMAL; a DOUBLE
  would turn 0.1 + 0.2 into 0.30000000000000004.
- **Referential integrity without foreign keys.** Delta does not enforce
  foreign keys, so the job checks them: a left join to the distinct keys of
  the other table, with `broadcast` because products and orders are small
  compared to order lines (no shuffle of the big side). Bad lines are
  flagged, not dropped; gold decides what to exclude, and the flags make the
  problems countable.
- **Known limitation.** The flags reflect the state when the line was
  loaded. If an order arrives a day after its items, the items keep
  `orphan_order` until their own day is rerun. The data quality framework
  (step 16) will report these counts.

**Try it:** run silver for orders and products before order items:
```bash
PYTHONPATH=src python -m data_generator.reference_data
PYTHONPATH=src python -m data_generator.orders --start-date 2026-11-01 --days 1
PYTHONPATH=src python -m lakehouse.bronze.ingest --batch-date 2026-11-01
PYTHONPATH=src python -m lakehouse.silver.orders --batch-date 2026-11-01
PYTHONPATH=src python -m lakehouse.silver.products --batch-date 2026-11-01
PYTHONPATH=src python -m lakehouse.silver.order_items --batch-date 2026-11-01
pytest tests/test_silver_conformed.py -v
```
Count the flagged lines and check that the amounts add up:
```python
from pyspark.sql import functions as F
from lakehouse.utils.spark import get_spark
spark = get_spark()
items = spark.read.format("delta").load("data/silver/order_items")
items.select(F.explode("_dq_issues").alias("issue")).groupBy("issue").count().show()
items.where("line_discount_amount + line_net_amount <> line_gross_amount").count()  # expect 0
```

## 12. Dimensions: dim_date, dim_customer, dim_product

**What:** Added `src/lakehouse/gold/dimensions.py`, which rebuilds three
gold tables under `data/gold/`: `dim_date` (one row per day, 2023-2027 by
default), `dim_customer` (one row per customer version from the SCD2 silver
table) and `dim_product` (one row per product). Each has a surrogate key
(`date_key`, `customer_key`, `product_key`), and the customer and product
dimensions get an "unknown" row with key `-1`. Tests are in
`tests/test_gold_dimensions.py`.

**Why:**
- **Star schema.** Gold is shaped for analysts, not for pipelines: one fact
  table of sales (step 13) joined to small descriptive dimensions. Queries
  like "net sales by month and customer segment" become one join per
  dimension and a GROUP BY, and BI tools understand this shape natively.
- **Surrogate keys instead of source IDs.** `customer_id` is not unique in
  the dimension: a Type 2 customer has one row per version. The surrogate
  key identifies one version, so the fact row points at the version that was
  valid when the order was placed. Keys also shield gold from source ID
  changes.
- **Deterministic keys.** Keys are a hash (`xxhash64`) of the business key
  (`customer_id + valid_from`, or `product_id`), so a rebuild gives every row
  the same key again. `monotonically_increasing_id()` or `row_number()`
  would renumber rows whenever the input changes and break facts that were
  built earlier.
- **`yyyymmdd` date key.** `20261101` sorts like a date, is readable without
  a join, and is the usual partition-friendly key for facts.
- **dim_date is generated, not derived from orders.** A calendar table has
  every day, including days with no sales, so a daily report shows a zero
  instead of a missing day. Attributes like ISO week and weekend are computed
  once, in plain Python, and unit tested (1 Jan 2027 is in ISO week 53 of
  2026, which a naive `year + weekofyear` grouping gets wrong).
- **Unknown member (-1).** Some order lines have a missing or orphan
  customer or an unknown product (steps 04 and 11). Pointing them at `-1`
  instead of dropping them in an inner join keeps report totals equal to
  the real totals and makes the problem visible as "Unknown customer".
- **Full rebuild with an atomic overwrite.** Dimensions are small, so
  rebuilding them each run is simpler than merging and always correct. A
  Delta overwrite is one commit, so readers never see a half-written table,
  and the old version is still there for time travel.
- **`unit_price` becomes `list_price`.** The catalogue price and the price
  actually paid are different facts; naming them differently avoids someone
  summing the wrong one.

**Try it:** build silver customers and products first (steps 10-11), then:
```bash
PYTHONPATH=src python -m lakehouse.gold.dimensions
pytest tests/test_gold_dimensions.py -v
```
```python
from lakehouse.utils.spark import get_spark
spark = get_spark()
for name in ("dim_date", "dim_customer", "dim_product"):
    spark.read.format("delta").load(f"data/gold/{name}").createOrReplaceTempView(name)
spark.sql("SELECT * FROM dim_date WHERE date_key BETWEEN 20261228 AND 20270103").show()
spark.sql("SELECT * FROM dim_customer WHERE customer_key = -1").show()
spark.sql("SELECT category, count(*) FROM dim_product GROUP BY category").show()
```

## 13. Fact table: fact_sales

**What:** Added `src/lakehouse/gold/facts.py`, which rebuilds
`data/gold/fact_sales`: one row per order line with `date_key`,
`customer_key` and `product_key`, the order's status, payment method and
currency, and the measures `quantity`, `gross_amount`, `discount_amount` and
`net_amount`. Lines that cannot be reported are left out, and the run logs
how many were dropped for each reason. Tests are in `tests/test_gold_facts.py`.

**Why:**
- **Grain first.** The grain is "one order line". Stating it up front tells
  everyone what a row means and which columns can be summed: summing
  `net_amount` over any filter gives revenue, while summing an order-level
  number (like a shipping fee) on a line-level table would double count. That
  is why only line-level measures live here.
- **Exclude vs. keep as unknown.** Silver labels problems; gold decides.
  The rule is: if the *numbers or the date* cannot be trusted (zero or
  negative quantity, unparseable price, an order that does not exist, no
  order timestamp), the line is not a sale we can report, so it is left out.
  If only the *description* is missing (unknown product, missing or unknown
  customer), the money is real, so the line is kept and points at the
  unknown member (`-1`). Totals then match the real business totals.
- **Excluded rows are counted, not silently dropped.** `build_fact_sales`
  returns the excluded lines with `_exclusion_reasons`, and the job logs one
  count per reason. A sudden jump in `orphan_order` points at a broken
  upstream load. The data quality framework (step 16) will turn this into a
  check that can fail the run.
- **Point-in-time join for SCD2.** A customer who moved from Portland to
  Denver has two versions. The join condition
  `valid_from <= order_date < valid_to` picks the version that was true when
  they bought, so "sales by state" is historically correct. This is the main
  reason for keeping Type 2 history in the first place. Orders older than the
  first snapshot of a customer (a backfill, say) get the first version,
  because a join that finds nothing would wrongly label a known customer
  "unknown".
- **Left joins, never inner joins, to dimensions.** An inner join would
  silently drop a sale whenever a dimension row is missing. A left join plus
  `coalesce(key, -1)` keeps the row count of the fact equal to the number of
  reportable lines.
- **Date key computed, not joined.** `date_key` is `yyyymmdd` of the order
  date, a pure function of the date, so a join to `dim_date` would cost a
  shuffle and add nothing.
- **Broadcast the product dimension.** dim_product has a few hundred rows;
  broadcasting it sends a copy to every executor so the large line table is
  never shuffled for that join.
- **Rules in plain Python first.** `exclusion_reasons` and
  `pick_customer_version` state the rules in a few lines that are unit tested
  without Spark, and the Spark code follows them exactly (the Spark test
  checks the same cases).
- **Full rebuild for now.** Like the dimensions, the fact is replaced in one
  atomic Delta overwrite. That is simple and always correct at this size.
  Step 15 adds partitioning and step 17 makes the run incremental.

**Try it:** build silver for a few days and the dimensions (steps 09-12), then:
```bash
PYTHONPATH=src python -m lakehouse.gold.facts
pytest tests/test_gold_facts.py -v
```
```python
from lakehouse.utils.spark import get_spark
spark = get_spark()
for name in ("fact_sales", "dim_customer", "dim_product", "dim_date"):
    spark.read.format("delta").load(f"data/gold/{name}").createOrReplaceTempView(name)
spark.sql("""
    SELECT d.month_name, c.segment, p.category, round(sum(f.net_amount), 2) AS net_sales
    FROM fact_sales f
    JOIN dim_date d     ON f.date_key = d.date_key
    JOIN dim_customer c ON f.customer_key = c.customer_key
    JOIN dim_product p  ON f.product_key = p.product_key
    GROUP BY 1, 2, 3 ORDER BY net_sales DESC
""").show()
# Sales that could not be tied to a known customer or product:
spark.sql("SELECT customer_key, product_key, count(*) FROM fact_sales "
          "WHERE customer_key = -1 OR product_key = -1 GROUP BY 1, 2").show()
```
Inner joins are safe in that query because every key, including `-1`, has a
dimension row.

## 14. Aggregates: daily sales and customer lifetime value

**What:** Added `src/lakehouse/gold/aggregates.py`, which rebuilds two
tables from `fact_sales`: `agg_daily_sales` (orders, cancelled and returned
orders, customers, units, gross, discount, net revenue and average order
value per day) and `agg_customer_lifetime_value` (first and last order date,
order count, lifetime net revenue, average order value, tenure and days since
the last order per customer). Tests are in `tests/test_gold_aggregates.py`.

**Why:**
- **Why aggregate tables at all.** A dashboard refreshing "revenue per day"
  would otherwise scan every order line on every refresh. Computing it once
  per pipeline run turns that into a read of a few hundred rows. The fact
  table stays the source of truth; aggregates are a cache with a fixed shape.
- **One revenue rule, defined once.** "Revenue" excludes cancelled and
  returned orders. The rule lives in `REVENUE_STATUSES` and is used by both
  tables, so the daily total and the sum of customer lifetime values can
  never disagree on what counts. Cancelled and returned orders still get
  their own counts, so they are visible instead of silently missing.
- **Days with no sales are rows of zeros.** The daily table starts from
  `dim_date` (limited to the range that has data) and left joins the sales.
  A chart then shows a dip to zero instead of quietly connecting the days on
  either side, and "average daily revenue" divides by the right number of
  days.
- **Counting orders on a line-level table.** The fact grain is one order
  line, so `count(*)` would count lines. Orders are counted with
  `count(DISTINCT order_id)`, and conditional counts use
  `count(DISTINCT CASE WHEN ... THEN order_id END)`, which works because
  `count` ignores NULLs.
- **Average order value is NULL, not 0, with no orders.** There is no
  average of nothing; a 0 would also pull down any average taken over the
  column later.
- **CLV is grouped by `customer_id`, not `customer_key`.** A customer who
  moved has two SCD2 versions and therefore two surrogate keys, but they are
  still one customer. Grouping by key would split their history in two. The
  descriptive columns (segment, state) come from the current version. The
  unknown customer (-1) is left out because it is many people at once.
- **Two-step aggregation for CLV.** Lines are first summed into orders,
  then orders into customers. Averaging per order needs order totals; taking
  the average straight over lines would give "average line value".
- **Reproducible "days since last order".** It is measured against the last
  order date in the data, not today's date, so rebuilding last month's data
  gives the same numbers whenever it runs, and tests do not depend on the
  clock.
- **Same rules in plain Python.** `customer_lifetime_value` and
  `average_order_value` are unit tested without Spark, and the Spark tests
  check the same cases on a small fact table.

**Try it:** build gold dimensions and facts first (steps 12-13), then:
```bash
PYTHONPATH=src python -m lakehouse.gold.aggregates
pytest tests/test_gold_aggregates.py -v
```
```python
from lakehouse.utils.spark import get_spark
spark = get_spark()
daily = spark.read.format("delta").load("data/gold/agg_daily_sales")
daily.orderBy("date_key").show()
clv = spark.read.format("delta").load("data/gold/agg_customer_lifetime_value")
clv.orderBy(clv.lifetime_net_revenue.desc()).show(10)
# The two tables agree on total revenue:
daily.agg({"net_revenue": "sum"}).show()
clv.agg({"lifetime_net_revenue": "sum"}).show()
```
The two totals differ only by revenue from the unknown customer (-1), which
is in the daily table but not in the customer table.

## 15. Partitioning, OPTIMIZE / Z-ORDER and VACUUM maintenance job

**What:** `fact_sales` is now written partitioned by a new `month_key`
column (yyyymm). Added `src/lakehouse/maintenance.py`, a job that runs
`OPTIMIZE` on every Delta table (with `ZORDER BY` on the large ones) and then
`VACUUM` with a 7-day retention. Tests are in `tests/test_maintenance.py`,
and the fact test now checks `month_key`.

**Why:**
- **Partition by month, not by day.** Partitioning splits a table into one
  folder per value, so a query filtered on that value reads only its folder
  (partition pruning). But each partition holds at least one file, and a day
  here is a few hundred rows. Daily partitions would create thousands of
  tiny files, which is slower than no partitioning at all. A common rule of
  thumb is to partition only when each partition is around 1 GB or more;
  at this size monthly partitions are already generous, and they show the
  pattern. Bronze stays partitioned by `_batch_date` because there the
  partition is the unit of reloading (`replaceWhere`), not a query filter.
- **`month_key` is a real column.** Spark can only prune partitions when
  the filter is on the partition column itself. A filter on `date_key`
  alone does not prune folders, so reports should add `month_key`.
  (Delta generated columns can do this automatically; a plain column is
  simpler to explain and works the same on Databricks.)
- **OPTIMIZE fixes the small-file problem.** Every append, MERGE and
  overwrite writes new files. OPTIMIZE bin-packs them into large files in a
  new commit; readers see the same rows before and after.
- **Z-ORDER helps data skipping.** Delta stores min/max values per file.
  Z-ORDER sorts rows by the chosen columns while compacting, so each file
  covers a narrow range and a filter or MERGE join can skip most files.
  The columns are the ones each table is filtered or joined on: business
  keys for the silver MERGE targets (`order_id`, `customer_id`) and
  `date_key, product_key` for the fact table. Small tables get compaction
  only, because they are always read in full. A partition column is never
  Z-ORDERed (it has one value per partition); the job checks this before
  calling Spark so the error is readable.
- **VACUUM deletes old files, carefully.** OPTIMIZE, MERGE and overwrite
  only mark old files as removed in the transaction log; they stay on disk
  for time travel. VACUUM deletes removed files older than the retention.
  The job refuses less than 168 hours (Delta's default), because a shorter
  retention can delete files that a running query, or a time travel query
  within that window, still needs. `--vacuum-dry-run` lists what would be
  deleted first.
- **One failure does not stop the rest.** Each table is maintained in its
  own try block; failures are logged and the job raises at the end, so one
  broken table does not leave all the others unmaintained.
- **On Databricks.** Unity Catalog managed tables can use predictive
  optimization, which runs OPTIMIZE and VACUUM automatically. Knowing what
  those commands do is still what lets you choose partition and clustering
  columns. Newer Delta versions also offer liquid clustering
  (`CLUSTER BY`) as a replacement for partitioning plus Z-ORDER.

**Try it:** after building gold (steps 12-14):
```bash
PYTHONPATH=src python -m lakehouse.gold.facts        # rewrite fact_sales partitioned
ls data/gold/fact_sales/                              # month_key=202611/ ...
PYTHONPATH=src python -m lakehouse.maintenance --vacuum-dry-run
PYTHONPATH=src python -m lakehouse.maintenance
pytest tests/test_maintenance.py -v
```
```python
from lakehouse.utils.spark import get_spark
spark = get_spark()
spark.sql("DESCRIBE HISTORY delta.`data/silver/orders`") \
     .select("version", "operation", "operationMetrics").show(truncate=False)
# Partition pruning: look for PartitionFilters in the plan
spark.read.format("delta").load("data/gold/fact_sales") \
     .where("month_key = 202611").explain()
```
