# Module 4 — Transformations, Actions, and Lazy Evaluation

## Purpose

Understand Spark's lazy execution model on chains learners already write:
DataFrames build logical plans that run only when an action is called; the
optimizer can rewrite that plan (for example, applying a filter earlier); and
some transformations shuffle data between worker nodes while others do not.

## Learning objectives

By the end of this module, you'll be able to:

- Distinguish **transformations** (new DataFrame, build logical plan) from
  **actions** (execute the plan; return a result or trigger terminal writes
  such as **`.save()`** / **`.saveAsTable()`** via `DataFrameWriter`)
- Explain **lazy evaluation**: why Spark waits for an action
- Inspect logical and physical plans with **`.explain()`** and spot optimizer
  changes
- Differentiate **narrow** (no cross-partition move) from **wide**
  (requires a **shuffle**) transformations
- Identify **`Exchange`** in the physical plan as a shuffle / stage boundary
- Read **jobs**, **stages**, **tasks**, and the stage **DAG**; a failed task
  retries from the lineage
- Recognize common **shuffle triggers** such as `groupBy` and `orderBy`
- Choose common actions (`first`, `head`, `take`, `tail`, `isEmpty`,
  `toPandas`) and know their driver-side memory risks

## Prerequisites

Module 2 — DataFrame Fundamentals and Module 3 — Data Cleaning, NULL Semantics,
and Type Handling. Comfortable creating, inspecting, reshaping, expressing,
and filtering DataFrames (`select`, `withColumn`, `filter` / `where`, `F.col`,
`F.when`, `F.expr`, etc.).

## Dataset

Small **ad-hoc** rideshare-flavored DataFrames built in code, aligned with
[`docs/data/dataset-overview.md`](../docs/data/dataset-overview.md). Volume
file reading starts in Module 5.

## Notebook 01 — Transformations vs Actions

### Context

Write a zone fare list. Transformations build a plan. Actions run it. Classic
all-purpose compute.

### Learning objectives

- See that transformations build a plan and return new DataFrames
- DataFrames are immutable
- Use the `show` action to run that plan

### Lesson flow

Hand-built `trip_id`, `pickup_zone`, `base_fare_amount` (one `NULL` fare,
mixed-case zones); `filter` missing fare; `upper(pickup_zone)`; `groupBy` +
`sum` as `trip_summary`; one `show()`. Immutability of `trips` is shown live
in class.

### Expected state

Not applicable — no persistent data state.

### Next

`02 - Lazy Evaluation and the Query Plan`

## Notebook 02 — Lazy Evaluation and the Query Plan

### Context

Spark records transformations first and executes them only when an action
requests a result. Same six trips as notebook 01. Classic all-purpose
compute. Spark UI is on this compute.

### Learning objectives

- Understand lazy evaluation
- Inspect a query plan with `.explain()`
- See how Spark optimizes a logical plan before execution

### Lesson flow

Rebuild notebook 01's `trips`; same report as 01 in separate cells, with
`upper` first and the missing-fare `filter` last, then `groupBy` + `sum`;
logical plan built but not executed; `.explain(mode="extended")` — parsed
order vs `Project`/`Filter` folded into `LocalRelation`; `show()` requests
the result; Spark UI physical plan plus jobs and stages.

### Expected state

Not applicable — no persistent data state.

### Next

`03 - Narrow vs Wide Transformations`

## Notebook 03 — Narrow vs Wide Transformations

### Context

Filter and uppercase stay local. `groupBy` shuffles. Same six trips as
notebook 01. Classic all-purpose compute. Spark UI is on this compute.

### Learning objectives

- Differentiate narrow from wide transformations
- Identify `Exchange` in the physical plan
- Read jobs, stages, tasks, and the stage DAG
- Explain that a failed task retries from the lineage

### Lesson flow

Rebuild notebook 01's `trips`; AQE off and `shuffle.partitions = 2`; do not
`repartition`; print `partition_id` (often one pile); narrow `filter` +
`upper` — no `Exchange`, one stage; wide `groupBy` + `sum` — `Exchange`, two
stages; one `show()` each (do not also `collect()`); Spark UI DAG; failed
task retries from lineage; shuffle triggers (`groupBy` here; `orderBy` /
`sort`). Deep tuning → Module 18.

### Expected state

Not applicable — no persistent data state.

### Next

`04 - Common DataFrame Actions`

### Boundaries

Do not call `repartition` to fake input partitions. Do not crash an executor
to demo fault tolerance.

## Notebook 04 — Common DataFrame Actions

### Context

Pull a few rows to the driver. Same six trips as notebook 01. Classic
all-purpose compute.

### Learning objectives

- Choose `first`, `head`, `take`, `tail`, `isEmpty`, and `toPandas`
- Know which of those pull a large result onto the driver

### Lesson flow

Rebuild notebook 01's `trips`; drop missing fare and `orderBy` fare;
`first` / `head` (trip `1003`); `head(3)` / `take(3)`; `tail(3)`; `isEmpty`
vs an empty filter; `toPandas` same driver risk as `collect`; writes →
Module 5.

### Expected state

Not applicable — no persistent data state.

### Next

Module 5 — Reading, Writing, and Schemas.

## Minimum privileges required

- Unity Catalog: none — hand-built DataFrames only
- Workspace: **`CAN ATTACH TO`** (or **`CAN RESTART`**) on the compute used here
