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

## 16. Data quality check framework

**What:** Added `src/lakehouse/quality/checks.py`, a small framework of
declarative checks (`not_null`, `unique`, `accepted_values`, `between`,
`expression`, `foreign_key`, `min_row_count`), and
`src/lakehouse/quality/suites.py`, which defines the rules for each silver
and gold table, runs them, appends every result to `audit/dq_results`, and
fails the job if an error-level check fails. Tests are in
`tests/test_quality.py`.

**Why:**
- **Tests check code; data quality checks check data.** Unit tests prove the
  transformations are right for the inputs we thought of. Real data keeps
  inventing new inputs, so the output itself must be checked on every run.
  This is the same idea as dbt tests or Great Expectations, written out by
  hand so every part is visible.
- **Rules are data, not code.** Each check is a small frozen dataclass, and
  a table's suite is a tuple of them. Adding a rule is one line, the full
  list of rules for a table can be read at a glance, and the rules can be
  unit tested without Spark.
- **Count failing rows, then compare with a tolerance.** Every check
  reduces to "how many rows break the rule?". `max_failure_ratio` says how
  many are acceptable: 0 for keys and joins, a few percent for things the
  generator makes dirty on purpose. A "5% of rows have issues" rule is much
  more useful than "any issue fails", which would fail every day and get
  ignored.
- **Error vs warn.** `error` means the data is wrong and publishing it would
  mislead (duplicate fact rows double revenue; an orphan key drops sales
  from every inner join). `warn` means "suspicious, look at it" (more
  flagged rows than usual, unknown order statuses). Silver keeps dirty rows
  on purpose, so its content rules are warnings; gold is what people report
  from, so its structural rules are errors.
- **NULL is a failure, not a pass.** In SQL, `NULL > 0` is NULL, and a
  `WHERE NOT (x > 0)` filter would quietly skip it. Failing rows are found
  with `NOT coalesce(rule, false)`, so "cannot tell" counts as failed.
  `unique` and `foreign_key` ignore NULLs on purpose: that is what
  `not_null` is for, and one problem should be reported once.
- **Rules that guard earlier steps.** The suites encode the promises made
  by previous steps: one current SCD2 version per customer and only that one
  open-ended (step 10), every fact key present in its dimension and the
  unknown member present (steps 12-13), `net = gross - discount` (step 11),
  one row per day in the daily aggregate (step 14). If a later change breaks
  one of them, the pipeline says so instead of publishing wrong numbers.
- **`min_row_count` catches the silent failure.** An empty table passes
  every other rule, because there is nothing to break them.
- **Run everything, then fail.** All checks run before the job raises, so
  one run reports every problem instead of one per rerun.
- **Results are kept.** Each run appends to `audit/dq_results` with a run
  id, so trends can be queried, for example how the share of flagged orders
  changes over time.

**Try it:** after building silver and gold:
```bash
PYTHONPATH=src python -m lakehouse.quality.suites
PYTHONPATH=src python -m lakehouse.quality.suites --tables gold.fact_sales
pytest tests/test_quality.py -v
```
```python
from lakehouse.utils.spark import get_spark
spark = get_spark()
spark.read.format("delta").load("data/audit/dq_results") \
     .orderBy("checked_at", "table_name").show(50, truncate=False)
```
To see a check fail, append a duplicate row to a gold table (for example
re-append one `dim_product` row) and run the suite again: the job exits with
an error listing `unique(product_key)`. Rebuild the table afterwards with
`python -m lakehouse.gold.dimensions`.

## 17. End-to-end pipeline runner with incremental watermarks

**What:** Added `src/lakehouse/pipeline.py`, one command that runs the whole
pipeline: it finds the batch dates in the landing zone, keeps the ones after
the watermark, loads each of them through bronze and silver (oldest first),
then rebuilds gold and runs the data quality suites once. Every stage attempt
is appended to a new Delta table, `audit/pipeline_runs`, which is where the
watermarks come from. Planning logic is pure Python and tested in
`tests/test_pipeline.py`.

**Why:**
- **A watermark makes the pipeline incremental.** The watermark is the
  newest batch date that was processed successfully. Each run only loads
  dates after it, so a daily schedule processes one new day instead of
  reprocessing everything, and a run after a week offline catches up on all
  seven days on its own.
- **State lives in a table, not in the scheduler.** The watermark is
  computed from `audit/pipeline_runs` (the newest `success` row). Failed
  attempts are recorded too but never move it, so the next run retries them
  automatically. Because the table is append-only, it is also a history of
  every run: what ran, when, and what failed.
- **Two watermarks: silver and gold.** Bronze + silver are processed per
  date; gold is rebuilt once per run from all of silver. If gold or a data
  quality check fails, the silver dates are already done, so a single
  watermark would say "nothing new" and gold would stay stale forever. A
  separate gold watermark ("gold built through date X") makes the next run
  retry gold even when no new files have arrived.
- **Commit progress per date, stop at the first failure.** The silver
  watermark moves after each date, so a failure on day 3 keeps days 1-2.
  Later dates are not attempted: the customers SCD2 table must be built in
  date order (step 10), and a watermark with a gap in it would skip the
  failed day forever.
- **Retries are safe because each step is idempotent.** Bronze replaces the
  batch's partition and silver uses MERGE, so re-running a date that failed
  halfway gives the same result as running it once. Idempotency is what
  makes "just run it again" a valid recovery plan.
- **Only complete batches are loaded.** A date counts only when every daily
  source (orders and order items) has data files in its folder. Loading
  orders without their lines would publish a half day. Files starting with
  `.` or `_` (upload markers) are ignored for the same reason.
- **The trade-off: late data.** A watermark only looks forward. If 2 Nov
  lands after 3 Nov was processed, it is never picked up automatically. The
  runner logs a warning for such dates instead of losing them silently.
  Handling late data properly means a backfill, which is a deliberate action
  (and for SCD2 customers, a rebuild).
- **`--max-batches` and `--until` bound a run.** A big backlog in one run is
  all-or-nothing work that takes long and is painful to retry; capping it
  keeps runs small. `--dry-run` shows the plan without changing anything,
  which is the first thing to run when something looks wrong.
- **Maintenance stays outside the watermark.** OPTIMIZE / VACUUM never change
  what the data says, so `--maintenance` runs them at the end but their
  result does not affect the run state.

**Try it:**
```bash
PYTHONPATH=src python -m data_generator.reference_data --customers 500 --products 100
PYTHONPATH=src python -m data_generator.orders --start-date 2026-11-01 --days 3
PYTHONPATH=src python -m lakehouse.pipeline --dry-run      # plan: 3 new batches, build gold
PYTHONPATH=src python -m lakehouse.pipeline --max-batches 2
PYTHONPATH=src python -m lakehouse.pipeline                # only 2026-11-03 is new
PYTHONPATH=src python -m lakehouse.pipeline                # "Nothing to do"
PYTHONPATH=src python -m data_generator.orders --start-date 2026-11-04 --days 1
PYTHONPATH=src python -m lakehouse.pipeline --maintenance  # picks up 2026-11-04
pytest tests/test_pipeline.py -v
```
```python
from lakehouse.utils.spark import get_spark
spark = get_spark()
spark.read.format("delta").load("data/audit/pipeline_runs") \
     .orderBy("started_at").show(truncate=False)
```

## 18. Unit tests with a local Spark session

**What:** Added `tests/conftest.py` with one shared, Delta-enabled local
`spark` fixture and a `lake_config` fixture that gives each test an empty lake
folder. The per-module Spark fixtures that every test file had copied were
removed, and the bronze and session tests now use the shared fixture too.
Tests that need Spark are marked `spark` automatically, so
`pytest -m "not spark"` runs only the fast tests. Two end-to-end tests were
added to `tests/test_pipeline.py`: they generate a tiny lake and run the
pipeline runner from step 17 through bronze, silver, gold and the quality
checks.

**Why:**
- **Two kinds of tests.** Pure-Python tests (planning, keys, check rules)
  run in milliseconds and need nothing installed. Spark tests prove that the
  DataFrame code really does what the pure logic says, but need a JVM. Both
  are needed; the `spark` marker lets you pick the fast set while editing and
  run everything before a commit.
- **One session for the whole run.** Starting Spark with the Delta jars
  takes several seconds. With one session per test module, every new module
  made the suite slower. A `scope="session"` fixture starts it once. This is
  the standard pattern for PySpark test suites.
- **Isolation comes from folders, not sessions.** Path-based Delta tables
  are just folders, so giving each test a fresh `tmp_path` lake is enough to
  keep tests from seeing each other's tables, even though they share one
  session.
- **Configs are built, not read from the environment.** `make_config` creates
  a `PipelineConfig` directly with one shuffle partition. Setting environment
  variables in one test can leak into another; an explicit object cannot.
  One shuffle partition stops tiny DataFrames being split into many tasks.
- **Skip, don't fail, when Spark is missing.** The fixture skips with a clear
  reason when pyspark, delta-spark or Java is not installed (`pytest -rs`
  shows it). A machine without Java can still run the pure tests, and CI
  (next step) installs everything so nothing is skipped there.
- **End-to-end tests catch what unit tests cannot.** Each step was tested on
  its own, but the runner depends on the steps fitting together: load order
  (products before order items), the watermark moving per date, gold only
  being built after every date succeeded. The first test runs two days, then
  checks that a rerun does nothing and that a third day is picked up alone.
  The second makes day 2 mostly corrupt and checks that day 1 is kept, day 3
  is not attempted, gold is not built, and the next run continues from day 2
  once a clean file arrives.
- **Generated data with `dirty_rate=0`.** The end-to-end tests are about the
  runner, so the data is clean and every quality error check passes; the
  dirty-data paths already have their own focused tests.

**Try it:** needs Java 11 or 17 and `pip install -r requirements.txt`:
```bash
pytest -q -m "not spark"                    # fast tests only
pytest -q -m spark -rs                      # Spark tests, show skip reasons
pytest tests/test_pipeline.py -v            # runner: planning + end-to-end
pytest --durations=5                        # the slowest tests
```

## 19. GitHub Actions CI: lint and tests

**What:** Added a GitHub Actions workflow (`.github/workflows/ci.yml`) with
two jobs: `lint` runs `ruff check`, and `test` installs Java 17 and the
pinned requirements and runs the whole pytest suite, Spark tests included,
on Python 3.10 and 3.11. It runs on every push to `main`, on pull requests
and on demand. Lint settings are in `ruff.toml`, and the lint tool is pinned
in a new `requirements-dev.txt`. The shared Spark fixture now fails instead
of skipping when `LAKEHOUSE_REQUIRE_SPARK=1`, which CI sets. Fixing the lint
findings also fixed a closure over a loop variable in the pipeline runner.

**Why:**
- **CI is the team's shared definition of "works".** Tests that only run on
  one laptop prove little: they may depend on something installed there, or
  simply not be run before a push. CI runs them on a clean machine for every
  change, and the badge in the README shows the current state.
- **Lint catches bugs, not just style.** Rules are picked for value: pyflakes
  (`F`) finds unused imports and undefined names, bugbear (`B`) finds likely
  bugs. It found one here: a `lambda` in a `for` loop that captured
  `batch_date`. Python closures look up the variable when called, not when
  created, so if the lambda were ever called after the loop moved on, it
  would process the wrong day. It is called immediately today, so it was
  not a live bug, but `lambda day=batch_date:` binds the value now and makes
  the code safe to change. Explaining this is a common interview question.
- **Two jobs, fast feedback first.** The lint job installs only ruff and
  finishes in seconds; the test job has to install Spark and download the
  Delta jars. Splitting them means a typo is reported in seconds, and both
  results are shown separately on the pull request.
- **A skipped test is a silent pass.** Locally, Spark tests skip when Java
  is missing so the fast tests are still usable. In CI that same behaviour
  would be dangerous: a broken install would skip 26 tests and the build
  would still be green. `LAKEHOUSE_REQUIRE_SPARK=1` turns those skips into
  errors, so green really means "every test ran".
- **Reproducible environment.** The runner installs the exact pinned
  versions (`pyspark`, `delta-spark`, `ruff`), the same Java major version
  Spark supports, and tests the oldest supported Python (3.10) as well as
  the current one. `fail-fast: false` lets both versions finish so you see
  whether a failure is version-specific.
- **Caching keeps it quick and less flaky.** pip downloads and the Delta
  jars (stored by Spark in `~/.ivy2`) are cached, keyed on the requirement
  files, so a dependency change automatically refreshes the cache.
- **Small safety settings.** `permissions: contents: read` gives the
  workflow token the least access it needs, `timeout-minutes` stops a hung
  Spark job from burning minutes, and `concurrency` cancels an outdated run
  when a newer commit is pushed.

**Try it:**
```bash
pip install -r requirements-dev.txt
ruff check .                                   # same lint as CI
LAKEHOUSE_REQUIRE_SPARK=1 pytest -q -rs        # same tests as CI: errors if Spark cannot start
```
Then open the **Actions** tab on GitHub to see the run for this commit, or
click **Run workflow** on the CI workflow to start one by hand.

## 20. Databricks setup notebook (catalog, schemas, volume) and bronze + silver notebooks

**What:** The pipeline can now store its tables either as Delta folders
(local) or as Unity Catalog managed tables (Databricks), with one setting.
`PipelineConfig` has a new optional `catalog`; when it is set,
`table_path("silver", "orders")` returns `workspace.silver.orders` instead
of a folder. A new module, `lakehouse/utils/tables.py`, has the only code
that cares about the difference (`read_table`, `save_table`,
`table_exists`, `delta_table`, `create_table_if_not_exists`, `sql_ref`), and
every job now uses it instead of `.load()` / `.save()` / `DeltaTable.forPath()`.
`lakehouse/utils/unity_catalog.py` describes the Databricks layout (schemas,
landing volume) as SQL strings and a config. Three notebooks were added in
`databricks/` (plus `_common`, shared by all of them):

| Notebook | Does |
|---|---|
| `00_setup` | creates schemas `raw`, `bronze`, `silver`, `gold`, `quarantine`, `audit` and the volume `raw.files`; writes sample data into the volume |
| `01_bronze` | plans the new batch dates and loads each into bronze |
| `02_silver` | loads the same dates into the four silver tables and moves the watermark |

The runner was split slightly so the notebooks can reuse it:
`plan_pending_run` (landing + watermarks -> plan), `load_bronze_batch`,
`load_silver_batch`, and `run_stage` (was `_run_stage`). New pure function
`missing_bronze_sources` is tested in `tests/test_pipeline.py`; the table
references and the Unity Catalog layout are tested in `tests/test_tables.py`.

**Why:**
- **One codebase, two platforms.** Copying the transforms into notebooks
  would give two versions that slowly drift apart, and the notebook copy
  would have no tests. Instead the notebooks are thin: they set parameters
  and call the same tested functions the local runner calls. This is how
  teams keep notebooks maintainable: logic in Python modules, notebooks as
  the entry points.
- **Managed tables, not paths, on Unity Catalog.** Locally a table is a
  folder. On Unity Catalog a table is an object with a three-level name
  (`catalog.schema.table`); Databricks stores the files and enforces
  permissions, lineage and auditing on the name. Writing Delta files to a
  path would bypass all of that. `saveAsTable(name)` creates a managed table on
  the first write, and every writer option already used (`replaceWhere`,
  `overwriteSchema`, `partitionBy`) works the same way with it.
- **Volumes are for files, tables are for tables.** Source files are not
  tables, so they go in a Unity Catalog volume (`/Volumes/<catalog>/raw/files`).
  Volumes look like a normal folder to Python and Spark, which is why the
  data generator and the landing-zone scan (`landed_dates`) work unchanged.
- **Schemas map to layers.** `bronze`, `silver` and `gold` become schemas,
  so the medallion architecture is visible in Catalog Explorer, and access
  can be granted per layer (e.g. analysts get `SELECT` on `gold` only).
- **The catalog is a parameter.** Free Edition comes with a `workspace`
  catalog, which is the default. Pointing the same notebooks at another
  catalog (say `dev` vs `prod`) is the standard way to separate
  environments on Unity Catalog. The name is validated because it ends up
  inside SQL statements.
- **Setup is idempotent.** Every statement is `CREATE ... IF NOT EXISTS`, so
  running the setup twice is harmless, like everything else in this
  pipeline.
- **Two notebooks, one plan, one watermark.** Bronze and silver are separate
  notebooks (so a job can show them as separate tasks), but both ask
  `plan_pending_run` which dates to process. Bronze does not move the
  watermark; silver moves it after each date. So both see the same pending
  dates, and a failed silver run is retried next time.
- **Guarding the hand-off.** If silver ran for a date bronze never loaded,
  it would merge nothing, mark the date as done, and the watermark would
  skip that day forever. `missing_bronze_sources` checks the ingestion log
  (newest attempt per source) and refuses such a date. Checking upstream
  completion before marking work done is a general orchestration rule.

**Try it on Databricks Free Edition** (I could not run this; please report
anything that fails):
1. Sign in at the Databricks Free Edition workspace. In the left sidebar
   click **Workspace**, open your user folder, then **Create > Git folder**.
   Paste `https://github.com/praneethpatibandla/lakehouse-medallion-pipeline`
   and click **Create Git folder**. (Later, use the branch button and
   **Pull** to get new commits.)
2. Open `databricks/00_setup`. In the compute selector at the top right
   choose **Serverless**. Click **Run all**. The widgets at the top default
   to catalog `workspace`, start date `2026-11-01` and 3 days.
3. Click **Catalog** in the sidebar: under `workspace` you should see the
   schemas `raw`, `bronze`, `silver`, `gold`, `quarantine`, `audit`, and the
   volume `raw > files` with a `landing/` folder inside.
4. Open `databricks/01_bronze`, **Run all**. The table at the end lists one
   row per source and date. Then open `databricks/02_silver`, **Run all**.
5. Run both again: they print "nothing new". To see incremental loading,
   set `start_date` to `2026-11-04` and `days` to `1` in `00_setup`, run its
   last cell only, then run `01_bronze` and `02_silver` again.
6. If you edit code under `src/` in the Git folder, click **Detach and
   re-attach** (or restart Python with `dbutils.library.restartPython()`) so
   the notebooks import the new version.

Locally nothing changes; check that everything still works with:
```bash
pytest -q
PYTHONPATH=src python -m lakehouse.pipeline --dry-run
```

## 21. Gold notebook and SQL analysis queries on the gold tables

**What:** Two more notebooks finish the medallion flow on Databricks.

| Notebook | Does |
|---|---|
| `03_gold` | rebuilds the gold star schema and aggregates with `build_gold_layer` (the same function the local runner uses), runs every data quality suite, records the `gold` stage in `audit.pipeline_runs`, then shows row counts, the latest quality results and the Delta history of `fact_sales` |
| `04_analysis` | seven SQL queries on the gold tables, each showing one technique |

A new pure function, `gold_rebuild_target` in `lakehouse/pipeline.py`,
decides whether gold needs rebuilding from the runs table alone: yes when
the silver watermark is newer than the gold watermark (or when
`force_rebuild` is set), and then up to which date. It is tested in
`tests/test_pipeline.py`.

The analysis queries:

| # | Question | Technique |
|---|---|---|
| 1 | Daily revenue and its 7-day moving average | window frame `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW` |
| 2 | Revenue by category per month, share and rank | star join + `sum() OVER (PARTITION BY ...)`, `rank()` |
| 3 | Top 10 customers by lifetime value | reading a pre-computed aggregate |
| 4 | Revenue that would land in the wrong state without SCD2 | point-in-time join vs current version |
| 5 | Cancellation and return rates by payment method | `count(DISTINCT CASE WHEN ...)` at the right grain |
| 6 | Average revenue by day of week | calendar attributes from `dim_date` |
| 7 | Repeat customers by segment | conditional averages |

**Why:**
- **Gold is its own step.** On Databricks each notebook becomes a task in
  a job (step 22). Gold runs once after all new dates are in silver, not
  once per date, because it is rebuilt in full from silver. That is cheap
  at this size and always consistent: no incremental gold logic to get
  wrong.
- **Decide from recorded state, not from "what just ran".** The gold
  notebook does not know what silver did a minute ago in another task. It
  reads the watermarks from `audit.pipeline_runs`, so it gives the right
  answer whether it runs inside a job, by hand, or after a failed attempt.
  A failed gold run (for example a blocking quality check) is stored as
  `failed`, does not move the gold watermark, and is retried next time even
  if no new data arrives.
- **Quality checks gate the "done" flag.** The checks run inside the gold
  stage, so gold is only marked successful when its data passed. A table
  that is wrong but marked done is worse than a run that is visibly failed.
- **Aggregates for dashboards, the star schema for questions.** Queries 1, 3,
  6 and 7 read small pre-computed tables; queries 2, 4 and 5 need the detail
  of the fact table. Knowing which to use is a common interview topic.
- **Grain discipline.** `fact_sales` has one row per order *line*. Counting
  orders with `count(*)` would count an order once per line, so query 5
  uses `count(DISTINCT order_id)`. Always ask "what does one row mean?"
  before aggregating.
- **SCD Type 2 pays off in query 4.** Each sale points at the customer
  version valid on the order date. Joining on the current version instead
  would move past revenue to the customer's new state. Query 4 shows
  exactly how much revenue would move.
- **Parameterised SQL.** Table names are built with
  `IDENTIFIER(:catalog || '.gold.fact_sales')`, so the same queries run
  against any catalog (dev / prod) and the catalog name is passed as a
  parameter, not pasted into the SQL text.
- **Delta history.** `DESCRIBE HISTORY` lists every version of a table. Each
  gold rebuild is one atomic overwrite, so it appears as one version, and
  an older version can be read with time travel (`VERSION AS OF`) if a
  rebuild ever goes wrong.

**Try it on Databricks Free Edition** (I could not run Databricks or Spark
here; please report anything that fails):
1. In your Git folder, click the branch name at the top and **Pull** to get
   this commit.
2. Run `databricks/00_setup`, `01_bronze` and `02_silver` if you have not
   yet (step 20). Then open `databricks/03_gold`, choose **Serverless**,
   **Run all**. It prints `rebuilding gold through 2026-11-03` and shows
   six gold tables and the quality results (every check should pass).
3. Run `03_gold` again: it prints "Gold is up to date". Set the
   `force_rebuild` widget to `true` and run it again to rebuild anyway; a new
   version appears in the `DESCRIBE HISTORY` output.
4. Open `databricks/04_analysis`, **Run all**. Under each table result
   you can click **+ > Visualization** to turn it into a chart (a line chart
   of query 1 works well).
5. To see query 4 return rows, move one customer to another state and load
   one more day. In `00_setup`, set `start_date` to the day after your last
   one and `days` to `1`, and run the sample data cell. That cell rewrites
   `customers.csv`, so only now add a new cell below it and run:
   ```python
   import csv
   path = f"{config.landing_path}/customers/customers.csv"
   rows = list(csv.DictReader(open(path)))
   rows[0].update(city="Denver", state="CO")
   with open(path, "w", newline="") as f:
       writer = csv.DictWriter(f, fieldnames=rows[0].keys())
       writer.writeheader()
       writer.writerows(rows)
   ```
   Then run `01_bronze`, `02_silver`, `03_gold` and query 4 again. Customer
   `C00001` gets a second version, and the orders placed before the move
   keep the old state. (Query 4 only shows the move if that customer had
   orders before it.)

Locally nothing changes:
```bash
pytest -q tests/test_pipeline.py
PYTHONPATH=src python -m lakehouse.pipeline   # still builds gold the same way
```

## 22. Databricks job definition that runs the notebooks in order on a schedule

**What:** `databricks.yml` at the repository root is a Databricks Asset
Bundle. It defines one job, `lakehouse-medallion-daily`:

```
bronze (01_bronze) --> silver (02_silver) --> gold (03_gold)
every day 06:00 UTC, serverless, one run at a time
```

- Job **parameters** `catalog` and `max_batches` are passed to every task
  and fill the notebook widgets with the same names.
- **Retries:** bronze and silver retry once after 2 minutes; gold does not.
- **Timeouts** of one hour per task, so a stuck task cannot run forever.
- The `dev` target uses development mode: the job name gets a
  `[dev <your name>]` prefix, files go to your own user folder, and the
  schedule is paused until you turn it on.

`.databricks/` (the CLI's local state) is ignored by git.
`tests/test_databricks_job.py` reads the YAML and checks that the tasks
run the notebooks in medallion order, that every notebook path exists, that
no task asks for a cluster (Free Edition is serverless only), and that every
job parameter matches a widget in `_common.py`. PyYAML is added to
`requirements-dev.txt` for that test.

**Why:**
- **Orchestration as code.** A job clicked together in the UI exists only
  in that workspace; nobody can review a change to it, and it is lost if the
  workspace is. In the bundle, the job lives in git next to the notebooks
  it runs, so a change to a task and a change to the code go through the
  same commit. Asset Bundles are Databricks' standard way to do CI/CD.
- **Dependencies, not timing.** Silver `depends_on` bronze, and gold on
  silver. Silver starts only if bronze succeeded, and if bronze fails the
  later tasks are skipped. Scheduling three separate jobs at 06:00, 06:20
  and 06:40 would break the day bronze is slow.
- **Retry only what is idempotent.** Bronze replaces its own partition and
  silver uses MERGE, so running them twice gives the same result: a retry
  can fix a temporary error (say, a serverless start-up hiccup). Gold
  usually fails on a blocking data quality check, and retrying the same
  check on the same data is pointless, so it fails fast and alerts instead.
- **One run at a time.** Two overlapping runs would read the same
  watermark and process the same dates. The result would still be correct
  (everything is idempotent), but `max_concurrent_runs: 1` makes the second
  run wait or be skipped instead of doing the work twice.
- **The watermarks make the schedule safe.** The job does not say "process
  yesterday". Each notebook asks the runs table what is new, so a run that
  finds nothing new does nothing, and a run after a two-day outage catches
  up on both days, oldest first. That is the difference between a schedule
  and an incremental pipeline.
- **Parameters, not copies.** Pointing the same job at another catalog is a
  change of one variable (`--var catalog=dev_lakehouse`), not a second job.
- **Development mode is a safety net.** Deploying from a laptop should
  never start a production schedule by accident, so development mode pauses
  it and namespaces the job under your name.
- **Testing a config file.** The job itself can only run on Databricks,
  but a wrong path or a misspelled parameter is a plain text mistake that
  a unit test catches in milliseconds. A parameter with no matching widget
  is especially sneaky: the notebook silently uses its default.

**Try it on Databricks Free Edition** (I could not run the Databricks CLI or
a workspace here; please report anything that fails). The notebooks from
steps 20 and 21 must have run once (`00_setup` at least), so the schemas
and the landing volume exist.

*Option A: Databricks CLI on your laptop (recommended).*
1. Install the CLI: on macOS `brew tap databricks/tap && brew install databricks`;
   on Windows `winget install Databricks.DatabricksCLI`. Check with
   `databricks -v` (use a recent version, 0.230 or newer).
2. Copy your workspace URL from the browser (the part up to
   `.databricks.com`) and run `databricks auth login --host <that URL>`. A
   browser tab opens; approve, and the CLI saves a profile.
3. In the repository folder:
   ```bash
   databricks bundle validate
   databricks bundle deploy
   databricks bundle run lakehouse_daily
   ```
   `run` prints a link to the run. In the workspace, **Jobs & Pipelines**
   now lists `[dev <your name>] lakehouse-medallion-daily`; its graph shows
   the three tasks in a row.
4. To let the schedule run daily, uncomment the two `presets` lines at the
   bottom of `databricks.yml` and deploy again. Free Edition has a daily
   compute quota, so pause it again when you are not using it.

*Option B: by hand in the UI (same result, but not versioned).*
1. **Jobs & Pipelines > Create > Job**. Name it `lakehouse-medallion-daily`.
2. First task: key `bronze`, type **Notebook**, source **Workspace**, path
   `databricks/01_bronze` in your Git folder, compute **Serverless**.
3. **+ Add task** twice for `silver` (`02_silver`, depends on `bronze`)
   and `gold` (`03_gold`, depends on `silver`).
4. In the right panel: **Job parameters** add `catalog` = `workspace` and
   `max_batches` = empty; **Schedules & Triggers > Add trigger > Scheduled**,
   daily; under **Advanced**, maximum concurrent runs 1.
5. Optional: under **Job notifications** add your address for failures.
   (Kept out of the bundle on purpose, so no address is in the repository.)

*See it work incrementally:* run the job once; every task finishes quickly
and the bronze output says "nothing new". Then generate one more day in
`00_setup` (`start_date` = the day after your last one, `days` = `1`, run
the sample data cell) and run the job again: bronze and silver load that
one day and gold rebuilds through it. `audit.pipeline_runs` shows a new
`silver` and `gold` row.

Locally, check the job definition with:
```bash
pip install -r requirements-dev.txt
pytest -q tests/test_databricks_job.py
```

## 23. Architecture diagram and final README covering both local and Databricks runs

**What:** The project is complete, so the documentation now describes a
finished system instead of a work in progress.

- `docs/architecture.md` (new) has three Mermaid diagrams, which GitHub
  draws directly on the page:
  1. **Data flow:** every source file, every table in each layer, and the
     transforms between them, including the quarantine path.
  2. **Control flow:** what one run of the pipeline does with the
     watermarks, and where it writes to `audit.pipeline_runs`.
  3. **Local vs Databricks:** one code base, two ways to run it, and the
     one setting (`PipelineConfig.catalog`) that switches between them.

  It also has a table reference: grain and load pattern of every table.
- `README.md` now opens with highlights, a text architecture diagram, a
  local-vs-Databricks comparison, the tech stack and a four-command quick
  start. The detailed sections for each step are unchanged below that, and
  a short "possible next steps" list closes it.
- `tests/test_docs.py` (new) checks that every relative link in the
  Markdown files points at a file that exists, that `architecture.md`
  mentions every table the pipeline writes (taken from the maintenance
  table list, so a new table without docs fails the test), and that every
  ticked roadmap step has a build log entry.

**Why:**
- **A reviewer reads the README for two minutes.** A recruiter or
  interviewer will not run the code. The top of the README has to say what
  the project shows (incremental loads, MERGE, SCD2, data quality, CI,
  Databricks) before the reader scrolls. The how-to stays below for anyone
  who does run it.
- **Grain is the first question about any table.** "One row per what?" is
  what an analyst, a reviewer and an interviewer all ask first. Writing the
  grain and the load pattern (append, partition replace, MERGE, full
  rebuild) next to every table is the shortest description of a data
  model, and a good thing to be able to recite in an interview.
- **Diagrams as code.** A Mermaid diagram is text in git: it is reviewed
  in the same commit as the change it describes, and nobody needs the
  original drawing tool to update it. An exported PNG goes stale the first
  time a table is added. The README keeps a plain-text version too, which
  reads fine in a terminal or an editor.
- **Separate what is shared from what differs.** The local-vs-Databricks
  table makes the design point of steps 20 to 22 visible: the transforms
  are identical, and only storage addresses, compute and orchestration
  change. That is how real teams keep one code base across dev laptops and
  production clusters.
- **Tests for documentation.** Broken links and undocumented tables are
  the usual way docs rot. Checking them in CI costs nothing and turns
  "please remember to update the docs" into a failing build.

**Try it:**
```bash
pytest -q tests/test_docs.py
```
Then open the repository on GitHub and view `docs/architecture.md`: the
diagrams are rendered on the page. (I could check the Mermaid syntax only
by reading it here; if a diagram shows an error box on GitHub, the message
names the line.) In VS Code the "Markdown Preview Mermaid Support"
extension shows them locally.
