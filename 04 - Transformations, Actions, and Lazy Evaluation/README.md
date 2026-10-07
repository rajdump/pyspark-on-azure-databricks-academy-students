# Module 4 — Transformations, Actions, and Lazy Evaluation

## Purpose

Understand Spark's lazy execution model on chains learners already write:
DataFrames build logical plans that run only when an action is called; the
optimizer can rewrite that plan; and some transformations shuffle data
between worker nodes while others do not.

## Learning objectives

By the end of this module, you'll be able to:

- Distinguish transformations from actions
- Explain lazy evaluation and inspect plans with `.explain()`
- Differentiate narrow from wide transformations and identify `Exchange`
- Read how an action starts a job, how a shuffle splits stages, and how
  tasks process partitions
- Practice common DataFrame actions and keep Driver-returning results small

## Prerequisites

Module 2 — DataFrame Fundamentals and Module 3 — Data Cleaning, NULL Semantics,
and Type Handling.

## Dataset

Small **ad-hoc** rideshare-flavored DataFrames built in code, aligned with
[`docs/data/dataset-overview.md`](../docs/data/dataset-overview.md). Volume
file reading starts in Module 5.

Notebooks **01**, **02**, and **05** use a six-row `trips` DataFrame
(`trip_id`, `pickup_zone`, `base_fare_amount`, one `NULL` fare). Notebooks
**03** and **04** use a separate eight-row `trips` DataFrame with mixed-case
zones and one `NULL` fare.

## Notebook 01 — Transformations vs Actions

### Context

Write a zone fare list. Transformations build a plan. Actions run it. Classic
all-purpose compute.

### Learning objectives

- See that transformations define work and return new DataFrames
- DataFrames are immutable
- See that actions trigger Spark to execute the required work
- Use the `show` action to request and display a result

### Lesson flow

Six-row `trips`; `filter` missing fare; `upper(pickup_zone)`; `groupBy` +
`sum` as `trip_summary`; `show()`; second `show()` on source `trips`.

### Expected state

Not applicable — no persistent data state.

### Next

`02 - Lazy Evaluation and the Query Plan`

## Notebook 02 — Lazy Evaluation and the Query Plan

### Context

Transformations record a plan; an action requests the result. Six-row
`trips`. Classic all-purpose compute, access mode **Dedicated**. Spark UI is
on this compute.

### Learning objectives

- Understand lazy evaluation
- Inspect a query plan with `.explain()`
- See how Spark optimizes a logical plan before execution

### Lesson flow

`upper` first, missing-fare `filter` last, then `groupBy` + `sum`;
`.explain(mode="extended")` before any result; `show()`; Spark UI SQL /
DataFrame query details.

### Expected state

Not applicable — no persistent data state.

### Next

`03 - Narrow and Wide Transformations — Shuffle`

## Notebook 03 — Narrow and Wide Transformations — Shuffle

### Context

`filter` and `upper` stay in existing partitions. The grouped aggregation
shuffles. Eight-row `trips`. Classic all-purpose compute, access mode
**Dedicated**.

### Learning objectives

- Differentiate narrow from wide transformations

### Lesson flow

AQE off and `shuffle.partitions = 2`; `partition_id` on the source, after
`filter`, and after `upper`; grouped `sum` then `partition_id`; `.explain()`
and read `HashAggregate` / `Exchange` / `HashAggregate`.

### Expected state

Not applicable — no persistent data state.

### Next

`04 - Jobs, Stages, and Tasks`

### Boundaries

Do not call `repartition` to fake input partitions. Job and stage structure
wait for notebook **04**.

## Notebook 04 — Jobs, Stages, and Tasks

### Context

The same eight-row chain as notebook **03**, now to show how Spark organizes
execution. Classic all-purpose compute, access mode **Dedicated**.

### Learning objectives

- Understand how an action starts Spark execution
- Understand how a shuffle separates execution into stages
- Understand how tasks process partitions within each stage

### Lesson flow

AQE off and `shuffle.partitions = 2`; `filter` then `upper`; grouped `sum`;
`show()` starts the job; stages split at the shuffle; tasks process
partitions.

### Expected state

Not applicable — no persistent data state.

### Next

`05 - Common DataFrame Actions`

### Boundaries

Do not crash an executor to demo fault tolerance.

## Notebook 05 — Common DataFrame Actions

### Context

Hands-on practice for common DataFrame actions. Six-row `trips`. Classic
all-purpose compute.

### Learning objectives

- Run `show`, `count`, `first`, `head`, `take`, `tail`, and `isEmpty`
- Run `collect` and `toPandas` on a small result
- Reduce with `filter`, `select`, and `limit` before `collect`

### Lesson flow

`show` / `count`; `first` / `head`; `head(3)` / `take(3)`; `tail(3)`;
`isEmpty` vs an empty filter; `collect`; `toPandas`; `filter` + `select` +
`limit` then `collect`. Driver-memory warning for `collect`, `toPandas`, and
large `n`.

### Expected state

Not applicable — no persistent data state.

### Next

Module 5 — Reading, Writing, and Schemas.

### Boundaries

Do not teach writes. Do not add DAG, cache, checkpoint, or RDD content.

## Minimum privileges required

- Unity Catalog: none — hand-built DataFrames only
- Workspace: **`CAN ATTACH TO`** (or **`CAN RESTART`**) on the compute used here
