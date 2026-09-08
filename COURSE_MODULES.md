# Course Modules

Canonical owner of the full course roadmap: module numbers and titles, purpose,
major topics, prerequisites, production relevance, contribution to the final
project, and planning status.

This file does **not** contain notebook sequences, exercises, or code. Detailed
design lives in a module's `README.md` when present; see the status legend.

## Status legend


| Status      | Meaning                                                                                 |
| ----------- | --------------------------------------------------------------------------------------- |
| Not Started | Roadmap/design stage — a module `README.md` may exist, but no learner notebooks         |
| Started     | The module `README.md` design is complete and learner-notebook authoring is active      |
| Complete    | Notebooks written, authoring-quality checked, and runtime-validated in Azure Databricks |


Prerequisites list direct dependencies. The course learning path is
cumulative unless stated otherwise.

## Phases

- [Phase I — Language and Engine Foundations](#phase-i--language-and-engine-foundations-modules-14)
- [Phase II — Core Data Engineering Skills](#phase-ii--core-data-engineering-skills-modules-59)
- [Phase III — Lakehouse Design and Implementation](#phase-iii--lakehouse-design-and-implementation-modules-1014)
- [Phase IV — Reliable Batch Pipelines](#phase-iv--reliable-batch-pipelines-modules-1516)
- [Phase V — Quality, Delivery, and Operations](#phase-v--quality-delivery-and-operations-modules-1721)



## The running use case

Every module threads through the same small rideshare dataset, so each topic
builds on the same data instead of switching examples. The tables, their
schema, join keys, and physical layout:
`[docs/data/dataset-overview.md](docs/data/dataset-overview.md)`.

---



## Phase I — Language and Engine Foundations (Modules 1–4)

Learn the Databricks environment, build DataFrame fluency, handle imperfect
data, and understand Spark execution.


| #   | Module                                           | Purpose                                                                                                                                              | Major Topics                                                                                                                                                                                                                                                                            | Prerequisites                                                                                 | Production Relevance                                                   | Final-Project Contribution                                                           | Status   |
| --- | ------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- | ------------------------------------------------------------------------------------ | -------- |
| 1   | Azure Databricks and Spark Foundations           | Orient in the Azure Databricks workspace and build a mental model of how Spark executes code — before real data-engineering logic                    | Spark architecture (driver/executor, jobs/stages/tasks); compute types and access modes; notebooks, magics, `dbutils`; `SparkSession` and first DataFrame                                                                                                                               | None — assumes root [README.md](README.md#who-this-is-for) baseline (basic Python, basic SQL) | Compute and platform literacy every later module depends on            | Establishes the environment the final project runs in                                | Complete |
| 2   | DataFrame Fundamentals                           | Build core DataFrame fluency: create, inspect, reshape, express, filter, and query through temp views and Spark SQL                                  | Creating/inspecting DataFrames; `select`/`withColumn`/`withColumns`/rename/`drop`; `F.col`/`F.when`/`F.lit`; `F.expr`/`selectExpr`; `filter`/`where`; intro NULL/blank traps; session and global temp views; `%sql`/`spark.sql`                                                         | Module 1                                                                                      | Core API fluency used in every notebook                                | Forms the DataFrame layer reused throughout                                          | Complete |
| 3   | Data Cleaning, NULL Semantics, and Type Handling | Fix imperfect values and write NULL-aware predicates on hand-built rideshare DataFrames — before file-based ingestion                                | Three-valued logic and NULL-safe predicates (`isin` trap, `eqNullSafe`); missing/blank/sentinel/`NaN` and normalize-first; `na.drop`/`fill`/`replace` and `F.coalesce`; `cast`/`try_cast` and rejected-row detection; numeric overflow; date/timestamp parsing (Spark 4 / ANSI `try_*`) | Module 2                                                                                      | Correctness in filters, comparisons, and in-memory cleaning transforms | Reusable NULL-safe predicates and cleaning patterns for later joins and aggregations | Complete |
| 4   | Transformations, Actions, and Lazy Evaluation    | Understand Spark's lazy execution model on chains learners already write — plans run on actions; the optimizer can rewrite; narrow vs wide (shuffle) | Transformations vs actions; lazy evaluation and the query plan; narrow vs wide / `Exchange`; common actions and driver-memory risk                                                                                                                                                      | Modules 2–3                                                                                   | Builds performance-aware coding habits early                           | Shapes how pipeline logic is written efficiently                                     | Complete |




## Phase II — Core Data Engineering Skills (Modules 5–9)

Land files, transform data, build analytical tables and KPIs, and re-express
the pipeline in Spark SQL.


| #   | Module                                                  | Purpose                                                                                                                     | Major Topics                                                                                                                                                                                                                                                 | Prerequisites                                                                                                                                | Production Relevance                                       | Final-Project Contribution                                                                                    | Status   |
| --- | ------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- | -------- |
| 5   | Reading, Writing, and Schemas                           | Land the shared rideshare dataset on UC Volumes and read/write production formats with explicit schemas                     | UC Volumes and data landing (including controlled-bad sources); CSV/JSON/Parquet/XML/Avro reads; explicit schemas vs inference; write modes; Delta file write and managed `saveAsTable` preview                                                              | Module 4; [additional environment and privilege requirements](05%20-%20Reading%2C%20Writing%2C%20and%20Schemas/README.md#before-notebook-01) | Volume-based file I/O patterns used in real ingestion jobs | Lands raw files for the Phase II learning pipeline. The production medallion lands its own copy in Module 14. | Complete |
| 6   | Built-in Functions, Complex Types, and UDF Alternatives | Transform landing data with Spark built-ins, work with nested types, and write curated outputs — prefer built-ins over UDFs | Built-in `F.*` transforms; Volume path vs managed table; structs/arrays/`explode`; cleaned `curated/` outputs; built-in vs Python UDF (Pandas/Arrow note only)                                                                                               | Module 5                                                                                                                                     | Performant, idiomatic transformation logic                 | Teaches cleaning and enrichment patterns later reused in Silver                                               | Complete |
| 7   | Joins and Set Operations                                | Join and combine rideshare tables with predictable row counts and clear keys — no silent cardinality or key traps           | Grain and cardinality; join types and silent failures; lookup joins, column cleanup, broadcast; semi/anti; set operations; read landing and Module 6 `curated/` / write managed tables (`trip_enriched`, `trip_driver_assignment`); high-level AQE awareness | Module 6                                                                                                                                     | Multi-table integration — a core production pattern        | Builds teaching managed tables. Production tables are rebuilt in Gold.                                        | Complete |
| 8   | Aggregations and Window Functions                       | Produce analytics-ready summaries and KPI tables                                                                            | `groupBy` and aggregates (collections, percentiles, distinct counts); pivot; window functions (ranking, running totals, lag/lead); Top-N per group; sampling; managed Delta `kpi_*` tables (`saveAsTable`)                                                   | Module 7                                                                                                                                     | Analytics and reporting layers                             | Produces teaching KPI tables later rebuilt as Gold                                                            | Complete |
| 9   | Spark SQL and DataFrame Interoperability                | Re-express DataFrame-based rideshare analytics in Spark SQL and choose deliberate SQL–DataFrame interoperability patterns   | Dual-API entry points; SQL joins and aggregations; `PIVOT`, `UNPIVOT`, and `TABLESAMPLE`; windows and `QUALIFY`; CTEs and named parameters; rebuilds Module 8 KPI outputs in Spark SQL from Module 7 managed tables                                          | Module 8                                                                                                                                     | Supports SQL-first collaboration and dual-API validation   | Enables SQL-based transforms and cross-API validation                                                         | Complete |




## Phase III — Lakehouse Design and Implementation (Modules 10–14)

Build Delta Lake foundations, then table maintenance, schema, and
introductory `MERGE`; govern existing assets; design the medallion
architecture; and build its full-refresh implementation.


| #   | Module                                           | Purpose                                                                                                                                                 | Major Topics                                                                                                                                                                                                                                                                                                                                                                               | Prerequisites | Production Relevance                                                                              | Final-Project Contribution                                                                  | Status      |
| --- | ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------- | ------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- | ----------- |
| 10  | Delta Lake Foundations                           | Understand what a Delta table is, how it tracks versions, how managed and external tables differ, and how historical states can be queried and restored | Parquet vs Delta update behavior; `_delta_log`, versions, `add`/`remove`, current snapshot, `DESCRIBE HISTORY`; managed vs external Delta tables (storage location, file lifecycle on `DROP`/`UNDROP`, external re-registration, managed default vs path control); time travel (`VERSION AS OF`, `TIMESTAMP AS OF`, PySpark historical reads, `RESTORE`) plus a retention/`VACUUM` warning | Module 9      | Lakehouse storage foundation for all later modules                                                | Delta mental model on a handmade lab extract; does not mutate `trip_enriched` or KPI tables | Complete    |
| 11  | Delta Lake Transactions, Schema, and Maintenance | Apply deletion-vector maintenance, schema change, introductory `MERGE`, and ACID / optimistic concurrency after the foundations module                  | Deletion vectors; `REORG TABLE ... APPLY (PURGE)`; `VACUUM`; schema enforcement and evolution; introductory `MERGE`; ACID and optimistic concurrency                                                                                                                                                                                                                                       | Module 10     | Transactional correctness, table maintenance, and upsert syntax used before incremental pipelines | Delta operations later reused on medallion tables; production `MERGE` is Module 15          | Started     |
| 12  | Unity Catalog and Data Governance                | Govern existing Module 5–9 `landing` / `processed` assets with least privilege                                                                          | Managed vs external Unity Catalog objects; grants and privileges; ownership; storage credentials; minimum-privilege design; future medallion schemas will need their own roles                                                                                                                                                                                                             | Module 11     | Governance compliance — required in any real Databricks environment                               | Applies least-privilege governance to existing Unity Catalog assets                         | Not Started |
| 13  | Medallion Architecture and Layer Design          | Paper-design the medallion architecture without creating lakehouse objects                                                                              | Layer rules and quality boundaries; schema names; new medallion landing location; source-to-target mapping; no `CREATE SCHEMA` and no tables; Module 5 `landing` / `processed` objects are not medallion layers                                                                                                                                                                            | Module 12     | The standard lakehouse architecture pattern                                                       | Documents the project's medallion design                                                    | Not Started |
| 14  | Build the Full-Refresh Medallion Pipeline        | Create `rideshare_dev.bronze`, `.silver`, and `.gold` and land a fresh copy of the raw files                                                            | New medallion landing volume; copy repo `data/raw` there; first grants on the new schemas; full-refresh managed Delta tables; introduce `src/`; do not use Module 5 `landing` / `processed` objects, curated folders, or teaching tables                                                                                                                                                   | Module 13     | First production-shaped lakehouse implementation                                                  | Creates the production medallion and reusable package                                       | Not Started |




## Phase IV — Reliable Batch Pipelines (Modules 15–16)

Make the medallion pipeline incremental and reliable, then express it as a
required batch Lakeflow Pipeline.


| #   | Module                                              | Purpose                                                                      | Major Topics                                                                                                                                                              | Prerequisites | Production Relevance                              | Final-Project Contribution                           | Status      |
| --- | --------------------------------------------------- | ---------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------- | ------------------------------------------------- | ---------------------------------------------------- | ----------- |
| 15  | Reliable Batch Ingestion and Incremental Processing | Make the Module 14 pipeline incremental and resilient                        | Production `MERGE`; `COPY INTO`; `REPLACE WHERE` / `INSERT REPLACE`; Change Data Feed; idempotency; deduplication; late-arriving data; backfills; batch state; quarantine | Module 14     | Ingestion reliability — a core production concern | Implements the project's incremental load logic      | Not Started |
| 16  | Lakeflow Declarative Pipelines for Batch            | Re-express the required Bronze→Silver→Gold flow as a batch Lakeflow Pipeline | Lakeflow Pipelines on pipeline-managed compute; materialized views (batch only); no streaming or Auto Loader                                                              | Module 15     | A managed orchestration path for batch workloads  | Required declarative pipeline variant of the project | Not Started |




## Phase V — Quality, Delivery, and Operations (Modules 17–21)

Test, tune, deploy, operate, and integrate the complete production pipeline.


| #   | Module                                   | Purpose                                                                      | Major Topics                                                                                                                                                                                                                                                | Prerequisites | Production Relevance                     | Final-Project Contribution                       | Status      |
| --- | ---------------------------------------- | ---------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------- | ---------------------------------------- | ------------------------------------------------ | ----------- |
| 17  | Testing, Data Quality, and Code Quality  | Build confidence in pipeline correctness                                     | `pytest` for pure Python helpers; Databricks-based data-quality checks; linting/typing gates                                                                                                                                                                | Module 16     | Reliability and maintainability          | Adds the project's test suite and quality gates  | Not Started |
| 18  | Performance and Spark Internals          | Tune the tested pipeline through plan analysis and Spark internals           | `partitionBy` pruning (beyond Module 5); data skipping; `OPTIMIZE`, auto-compaction, and Predictive Optimization; liquid clustering (Z-ORDER mention only); SortMerge vs broadcast with AQE off (beyond Module 7); shuffles; AQE; caching; Photon awareness | Module 17     | Cost and performance tuning              | Applies an optimization pass to the project      | Not Started |
| 19  | Lakeflow Jobs, Packaging, and Deployment | Package `src/` and deploy the required pipeline plus quality tasks           | Lakeflow Jobs; job tasks; create `databricks.yml` from scratch; package `src/`                                                                                                                                                                              | Module 18     | Deployment and CI/CD foundation          | Produces the project's deployable job definition | Not Started |
| 20  | Observability and Production Operations  | Operate batch pipelines in production                                        | Structured logging; monitoring; alerts; run-history triage; operational runbooks                                                                                                                                                                            | Module 19     | Operational readiness                    | Adds monitoring/alerting to the project          | Not Started |
| 21  | End-to-End Deployable Batch Project      | Integrate Modules 1–20 into one deployable batch project — no new major APIs | Capstone integration of all prior modules                                                                                                                                                                                                                   | Module 20     | This module *is* the production capstone | The final project itself                         | Not Started |


