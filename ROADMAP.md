# Roadmap

The project is built in small steps. Each step is one commit, and each commit
has a matching entry in [`docs/build-log.md`](docs/build-log.md) explaining what
was added and why.

## Phase 1 - Foundations
- [x] 01. Project structure, dependencies and roadmap
- [ ] 02. Central configuration and logging
- [ ] 03. Synthetic data generator: customers and products
- [ ] 04. Synthetic data generator: orders and order items in daily batches, with deliberately dirty records
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
- [ ] 20. Architecture diagram, final README and notes on running it on Azure Databricks
