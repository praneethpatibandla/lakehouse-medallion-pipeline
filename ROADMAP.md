# Roadmap

The project is built in small steps. Each step is one commit, and each commit
has a matching entry in [`docs/build-log.md`](docs/build-log.md) explaining what
was added and why.

## Phase 1 - Foundations
- [x] 01. Project structure, dependencies and roadmap
- [x] 02. Central configuration and logging
- [x] 03. Synthetic data generator: customers and products
- [x] 04. Synthetic data generator: orders and order items in daily batches, with deliberately dirty records
- [ ] 05. Spark session factory configured for Delta Lake

## Phase 2 - Bronze (raw)
- [ ] 06. Explicit schemas and raw ingestion into Delta with load metadata columns
- [ ] 07. Corrupt-record quarantine and ingestion audit log

## Phase 3 - Silver (clean)
- [ ] 08. Cleaning and standardisation transforms as pure functions
- [ ] 09. Deduplication and incremental upserts for orders with Delta MERGE
- [ ] 10. Slowly changing dimension (Type 2) for customers
- [ ] 11. Conformed products and order items

## Phase 4 - Gold (analytics)
- [ ] 12. Dimensions: dim_date, dim_customer, dim_product
- [ ] 13. Fact table: fact_sales
- [ ] 14. Aggregates: daily sales and customer lifetime value
- [ ] 15. Partitioning, OPTIMIZE / Z-ORDER and VACUUM maintenance job

## Phase 5 - Production readiness
- [ ] 16. Data quality check framework
- [ ] 17. End-to-end pipeline runner with incremental watermarks
- [ ] 18. Unit tests with a local Spark session
- [ ] 19. GitHub Actions CI: lint and tests
## Phase 6 - Databricks
Runs the same pipeline on Databricks Free Edition (serverless compute, Unity
Catalog). Notebooks live in `databricks/` as `.py` source files so they can be
pulled into a workspace with a Git folder. They reuse the logic in
`src/lakehouse/` instead of copying it. Source files are uploaded to a Unity
Catalog volume, and tables are created as managed tables using the
`catalog.schema.table` three-level namespace.
- [ ] 20. Databricks setup notebook (catalog, schemas, volume) and bronze + silver notebooks
- [ ] 21. Gold notebook and SQL analysis queries on the gold tables
- [ ] 22. Databricks job definition that runs the notebooks in order on a schedule

## Phase 7 - Wrap-up
- [ ] 23. Architecture diagram and final README covering both local and Databricks runs
