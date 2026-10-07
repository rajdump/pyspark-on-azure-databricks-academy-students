# Module 2 — DataFrame Fundamentals

## Purpose

Build core DataFrame fluency: create, inspect, reshape, express, filter, and
query through temp views and Spark SQL.

## Learning objectives

By the end of this module, you'll be able to:

- Create DataFrames from Python rows four ways: unnamed/inferred,
  named/inferred, named with DDL, named with `StructType`
- Inspect contents, structure, size, and summary stats
- Select, add, rename, recalculate, and drop columns
- Build Column expressions with `F.col`, `F.lit`, `F.when` / `otherwise`,
  `F.expr`, and `selectExpr`
- Filter with `filter` / `where`, including intro NULL and blank traps
- Query through session temp views (`%sql`, `spark.sql`) and a classic-only
  global temporary view

## Prerequisites

Module 1 — Azure Databricks and Spark Foundations.

Read the matching page in the course Notion hub before notebooks **01–05**.

## Dataset

Small **ad-hoc** rideshare-flavored DataFrames built in code, aligned with
[`docs/data/dataset-overview.md`](../docs/data/dataset-overview.md). Volume
file reading starts in Module 5.

## Notebook 01 — Creating DataFrames

### Context

Lab after the **Creating DataFrames** Notion page.

### Learning objectives

- Create DataFrames unnamed/inferred, named/inferred, named with DDL, and
  named with `StructType`
- Inspect each path and explain inferred vs production risk

### Lesson flow

Rows only (`_1`, `_2`, …); named + inferred (compare types with the RideEase
model on Notion); DDL (`int` / `decimal(8,2)` vs inferred `long` / `double`);
`StructType` (same contract as DDL).

### Expected state

Not applicable — no persistent data state.

### Next

`02 - Inspecting DataFrames`

## Notebook 02 — Inspecting DataFrames

### Context

Lab after the **Inspecting DataFrames** Notion page.

### Learning objectives

- Inspect contents with `show` options and `display`
- Inspect structure with `printSchema`, `schema`, `columns`, `dtypes`
- Check size with `count` and `isEmpty`; use `describe` / `summary`
- Distinguish metadata checks from methods that run Spark work

### Lesson flow

One intentionally bad trip (negative `trip_distance_miles`, huge
`ride_duration_mins`); `show` options (`n`, `truncate`, `vertical`) /
`display`; `printSchema`, `schema`, `columns`, `dtypes`; `count`, `isEmpty`;
a filter can make a DataFrame empty; `describe` / `summary`.

### Expected state

Not applicable — no persistent data state.

### Next

`03 - Selecting and Transforming Columns`

## Notebook 03 — Selecting and Transforming Columns

### Context

Lab after the **Selecting and Transforming Columns** Notion page.

### Learning objectives

- Select, add, rename, recalculate, and drop columns
- Transforms return a new DataFrame
- Build Column expressions with `F.col`, `alias`, light `cast`, `F.lit`, and
  `F.when` / `otherwise`
- Choose `select` vs `withColumn` and chain into a small ops-style output

### Lesson flow

`select` / reorder; name strings vs `F.col`; `alias`, arithmetic, light
`cast`, `F.lit`; `F.when` / `otherwise`; add with `withColumn` vs `select`;
recalculate without duplicate names; `withColumns`; `withColumnRenamed` /
`withColumnsRenamed` / `drop`; chain into one output; source `df` unchanged.

### Expected state

Not applicable — no persistent data state.

### Next

`04 - SQL Expressions in DataFrame Code`

## Notebook 04 — SQL Expressions in DataFrame Code

### Context

Lab after the **SQL Expressions in DataFrame Code** Notion page.

### Learning objectives

- Use `F.expr` and `selectExpr`, including SQL `CASE WHEN`
- Reuse named SQL strings

### Lesson flow

`F.expr` (reuse `mph_sql`); `selectExpr` (pass `mph_sql` with no `F.expr`);
SQL `CASE WHEN`; `selectExpr` ops-style output; source `df` unchanged.

### Expected state

Not applicable — no persistent data state.

### Boundaries

`%sql` / `spark.sql` wait for notebook **06**.

### Next

`05 - Filtering Rows`

## Notebook 05 — Filtering Rows

### Context

Lab after the **Filtering Rows** Notion page.

### Learning objectives

- Filter with Column ops and SQL strings; combine with `AND` vs `&`
- Use `|`, `~`, `isin`, `between`, `like`
- Apply intro NULL checks (`isNull` / `isNotNull`); empty string ≠ NULL

### Lesson flow

Sample includes NULL, empty-string, and negative-distance rows; `filter` /
`where`; SQL `AND` vs Column `&`; Python `and` / `or` / `not` fail on
Columns; `|`, `~`, `isin`, `between`, `like`; `== None` vs `isNull`; empty
string ≠ NULL; chain a usable-trip filter; source `df` unchanged.

### Expected state

Not applicable — no persistent data state.

### Next

`06 - Querying DataFrames with SQL`

## Notebook 06 — Querying DataFrames with SQL

### Context

Session temporary views with `%sql` and `spark.sql`, then a classic-only
global temporary view.

### Learning objectives

- Explain why `%sql` and `spark.sql` cannot see a Python DataFrame variable
- Register a session temporary view with `createOrReplaceTempView`
- Query that view with `%sql` and with `spark.sql`
- Register a global temporary view with `createOrReplaceGlobalTempView` and
  query `global_temp` on classic compute

### Lesson flow

`SELECT … FROM df` fails; `createOrReplaceTempView("trips")`; `%sql`;
`spark.sql` returns a DataFrame; `createOrReplaceGlobalTempView("trips_global")`;
query `global_temp.trips_global`; prefer session views.

### Expected state

Not applicable — no persistent data state.

Global temporary views require classic all-purpose compute. They are not
supported on serverless.

### Boundaries

`F.when` / `F.expr` / `selectExpr` (notebooks 03–04). Side-by-side DataFrame
remakes of the same SQL (Module 9). Persisted tables.

### Next

Module 3 — Data Cleaning, NULL Semantics, and Type Handling.

## Minimum privileges required

- Unity Catalog: none — this module does not read or write governed data
- Workspace: **`CAN ATTACH TO`** (or **`CAN RESTART`**) on the compute used here
