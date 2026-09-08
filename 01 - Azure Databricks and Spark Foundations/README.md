# Module 1 — Azure Databricks and Spark Foundations

## Purpose

Orient in the Azure Databricks workspace and build a mental model of how Spark
executes code — before real data-engineering logic.

## Learning objectives

By the end of this module, you'll be able to:

- Explain what Spark is and why it exists (unified engine, distributed
  processing) at a data-engineer level — not a distributed-systems deep dive
- Describe the driver/executor model and how a job breaks into stages and tasks
- Navigate the workspace: compute types and access modes, attach a notebook,
  Databricks Runtime / LTS versioning
- Work in a notebook: cells, magic commands, `dbutils`
- Use the provided `SparkSession` (`spark`) and build a first DataFrame

## Prerequisites

None — first module. Assumes the audience baseline in
[`README.md`](../README.md#who-this-is-for): basic Python, basic SQL.

## Dataset

Small **ad-hoc** rideshare-flavored DataFrames built in code (a few rows), not
`data/raw/`. Volume file reading starts in Module 5. Full course dataset:
[`docs/data/dataset-overview.md`](../docs/data/dataset-overview.md).

## Notebook 01 — Introduction to Azure Databricks and the Workspace

### Context

First orientation in the Azure Databricks workspace, before Spark architecture
or DataFrames.

### Learning objectives

- Navigate the workspace browser, notebook editor, and compute attach
- Explain Databricks Runtime / LTS, including the classic vs serverless
  gotcha

### Lesson flow

Workspace browser, notebook editor, compute attach; DBR / LTS (classic vs
serverless gotcha).

### Expected state

Not applicable — no persistent data state.

### Exercise

Hands-on on the compute just attached: confirm attach, read the Spark version
from the session, and record the Runtime version from the compute UI.

### Next

`02 - Apache Spark Architecture and PySpark`

## Notebook 02 — Apache Spark Architecture and PySpark

### Context

Build the mental model of how Spark executes a request once compute is
attached.

### Learning objectives

- Explain why Spark distributes work and how PySpark relates to Spark
- Use the provided `SparkSession` (`spark`)
- Describe driver/executors and jobs → stages → tasks
- Observe one live request in the Spark UI on classic compute

### Lesson flow

Why Spark distributes work; PySpark ↔ Spark; `SparkSession`; driver/executors
(Diagram A); jobs → stages → tasks (Diagram B); `spark.range(...).count()`
stand-in; Spark UI on classic all-purpose Standard.

### Expected state

Not applicable — no persistent data state.

### Exercise

Change the stand-in count request and find the new job in the Spark UI
(job/stage/task).

### Next

`03 - Working with Notebooks`

## Notebook 03 — Working with Notebooks

### Context

How notebook cells, languages, magics, and `dbutils` share — and do not share
— a live session.

### Learning objectives

- Explain cell run order and shared Python state
- Explain that languages keep separate state
- Use magics (`%md`, `%sql`, `%fs`, `%sh`) and `%fs` vs `dbutils.fs`

### Lesson flow

Cell run order and shared Python state; languages keep separate state; magics
(`%md`, `%sql`, `%fs`, `%sh`); `%fs` vs `dbutils.fs`.

### Expected state

Not applicable — no persistent data state.

### Exercise

List filesystem entries with `dbutils.fs`, filter names, and print the
matches — a short hands-on on the attached compute.

### Next

`04 - Your First DataFrame`

## Notebook 04 — Your First DataFrame

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

### Exercise

Add rideshare-style rows and inspect with `show`, `display`, and
`printSchema`.

### Next

Module 2 — DataFrame Fundamentals (`01` in that module).

## Minimum privileges required

- Unity Catalog: none — this module does not read or write governed data
- Workspace: **`CAN ATTACH TO`** (or **`CAN RESTART`**) on the compute used here
