# Module 1 — Azure Databricks and Spark Foundations

## Purpose

Build a mental model of how Spark executes code, then work in the Databricks
notebook — before real data-engineering logic.

## Learning objectives

By the end of this module, you'll be able to:

- Confirm the live `SparkSession` (`spark`), Spark version, and application
  id on classic all-purpose compute
- Work in a notebook: shared Python state, magics (`%sql`, `%fs`, `%sh`),
  and `dbutils.fs`
- Build a first DataFrame from Python rows and inspect it with `show` /
  `display` / `printSchema`

## Prerequisites

None — first module. Assumes the audience baseline in
[`README.md`](../README.md#who-this-is-for): basic Python, basic SQL.

## Dataset

Small **ad-hoc** rideshare-flavored DataFrames built in code (a few rows), not
`data/raw/`. Volume file reading starts in Module 5. Full course dataset:
[`docs/data/dataset-overview.md`](../docs/data/dataset-overview.md).

## Notebook 01 — Apache Spark Architecture

### Context

Proof lab after Notion 10. Confirm the live `spark` session on classic
all-purpose compute.

### Learning objectives

- Print that `spark` is a SparkSession
- Print the Spark engine version
- Print this session's Spark application id

### Lesson flow

Read Notion 10; attach classic all-purpose; print `type(spark).__name__`,
`spark.version`, and `spark.app.id`.

### Expected state

Not applicable — no persistent data state.

### Boundaries

No DataFrame examples, Spark UI, or jobs/stages lab. Architecture roles stay
on Notion 10; jobs, stages, and tasks stay on Notion 11.

### Next

`02 - Databricks Workspace and Notebook`

## Notebook 02 — Databricks Workspace and Notebook

### Context

Lab after Notion 12. Shared Python state, magics, and `dbutils.fs` on the
attached compute.

### Learning objectives

- Print a Python value defined in an earlier cell
- Run a `%sql` cell and confirm SQL cannot see a Python local
- Run `%sh` on the driver for a quick check
- List files with `%fs` and `dbutils.fs`

### Lesson flow

Shared Python state (`base_fare`); `%sql` hello; expected failing
`%sql SELECT base_fare`; `%sh`; `%fs ls /databricks-datasets`;
`dbutils.fs.ls`.

### Expected state

Not applicable — no persistent data state.

Expected failure: `%sql SELECT base_fare` (`AnalysisException`).

### Next

`03 - Your First DataFrame`

## Notebook 03 — Your First DataFrame

### Context

First DataFrame from in-notebook Python rows — not file reads from
`data/raw/`.

### Learning objectives

- Build a small rideshare DataFrame from Python rows (no explicit schema)
- Inspect with `show` / `display` / `printSchema`
- Explain why an inferred schema is fine for demos, not for production

### Lesson flow

Small rideshare DataFrame from Python rows (no explicit schema); `show` /
`display` / `printSchema`; inferred schema fine for demos, not for
production.

### Expected state

Not applicable — no persistent data state. Ad-hoc in-notebook rows only; see
Dataset.

### Next

Module 2 — DataFrame Fundamentals (`01` in that module).

## Minimum privileges required

- Unity Catalog: none — this module does not read or write governed data
- Workspace: **`CAN ATTACH TO`** (or **`CAN RESTART`**) on the compute used here
