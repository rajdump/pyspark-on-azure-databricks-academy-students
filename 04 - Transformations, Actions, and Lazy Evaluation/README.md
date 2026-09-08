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

Distinguish transformations from actions on chains learners already write.

### Learning objectives

- Distinguish transformations (new DataFrame, build logical plan) from
  actions (execute the plan)

### Lesson flow

Transformations vs actions; chain before one action — example 1 (`filter`,
`withColumn`, `select`); example 2 (`filter`, `orderBy`, `limit`, `select`).

### Expected state

Not applicable — no persistent data state.

### Exercise

Explain a chain on a slightly different DataFrame.

### Next

`02 - Lazy Evaluation and the Query Plan`

## Notebook 02 — Lazy Evaluation and the Query Plan

### Context

Why Spark waits for an action, and how the optimizer can rewrite a plan.

### Learning objectives

- Explain lazy evaluation
- Inspect logical and physical plans with `.explain()` and spot optimizer
  changes

### Lesson flow

Why Spark waits for an action; `.explain(mode="extended")`; optimizer can
push a late filter earlier on one narrow chain.

### Expected state

Not applicable — no persistent data state.

### Exercise

Inspect an optimized plan.

### Next

`03 - Narrow vs Wide Transformations`

## Notebook 03 — Narrow vs Wide Transformations

### Context

Local work versus shuffles — `Exchange` as a stage boundary.

### Learning objectives

- Differentiate narrow from wide transformations
- Identify `Exchange` in the physical plan
- Recognize common shuffle triggers such as `groupBy` and `orderBy`

### Lesson flow

Prefer classic all-purpose (**Dedicated**) for partition/shuffle teaching —
Standard/serverless may collapse this sample to one partition; inspect
partition distribution; narrow `filter` (no `Exchange`, one stage); wide
`groupBy` + `Exchange` + Spark UI; common shuffle triggers (deep tuning →
Module 17).

### Expected state

Not applicable — no persistent data state.

### Exercise

Predict shuffle on a slightly different chain.

### Next

`04 - Common DataFrame Actions`

## Notebook 04 — Common DataFrame Actions

### Context

Return types and driver-side memory risk for common pull/check actions.

### Learning objectives

- Choose common actions (`first`, `head`, `take`, `tail`, `isEmpty`,
  `toPandas`) and know their driver-side memory risks

### Lesson flow

Return types and driver size risk (`show` / `count` / `collect` already
known); sort then compare `first()` / `head()` / `head(n)` / `take(n)`;
`tail(n)` (order not guaranteed unless sorted); `isEmpty()` vs `count() == 0`;
`toPandas()` same driver risk as `collect()`; `DataFrame.write` → Module 5.

### Expected state

Not applicable — no persistent data state.

### Exercise

Practice pull/check actions.

### Next

Module 5 — Reading, Writing, and Schemas.

## Minimum privileges required

- Unity Catalog: none — hand-built DataFrames only
- Workspace: **`CAN ATTACH TO`** (or **`CAN RESTART`**) on the compute used here
