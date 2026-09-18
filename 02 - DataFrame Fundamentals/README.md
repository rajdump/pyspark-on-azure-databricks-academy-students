# Module 2 — DataFrame Fundamentals

## Purpose

Build core DataFrame fluency: create, inspect, reshape, express, filter, and
query through temp views and Spark SQL. Reshape uses the DataFrame API and SQL
expression strings; filtering includes intro NULL and blank traps. This is the
API layer every later notebook reuses.

## Learning objectives

By the end of this module, you'll be able to:

- Explain what a Spark DataFrame is: distributed rows plus named, typed
  columns and schema metadata
- Create DataFrames from Python rows four ways: unnamed/inferred,
  named/inferred, named with DDL, named with `StructType`
- Explain why inferred schemas are convenient for demos but risky in production
- Inspect beyond a first look (`show`, `display`, `printSchema`, `columns`,
  `dtypes`, `count`, summary stats)
- Select, add, rename, recalculate, and drop columns (`select`, `withColumn` /
  `withColumns`, `withColumnRenamed` / `withColumnsRenamed`, `drop`)
- Build Column expressions with `F.col`, `alias`, light `cast`, `F.lit`, and
  `F.when` / `otherwise`
- Express the same logic as SQL strings with `F.expr` and `selectExpr`
  (including `CASE WHEN`) and reuse named SQL strings
- Filter with `filter` / `where` (Column ops and SQL strings); intro NULL
  checks (`isNull` / `isNotNull`); empty string ≠ NULL
- Register a session temporary view with `createOrReplaceTempView`; query with
  `%sql` and `spark.sql`
- Register a global temporary view with `createOrReplaceGlobalTempView` and
  query `global_temp` on classic compute
- Prefer clear chained transforms that leave the original frame unchanged
  until you assign a new one

## Prerequisites

Module 1 — Azure Databricks and Spark Foundations. You should already attach
compute, use notebook cells, and create a small DataFrame with
`spark.createDataFrame(rows, columns)` plus basic `show` / `display` /
`printSchema`.

Read the matching page in the course Notion hub before notebooks **01–05**.
Notebook **06** global temporary views require classic all-purpose compute —
not serverless.

## Dataset

Small **ad-hoc** rideshare-flavored DataFrames built in code, aligned with
[`docs/data/dataset-overview.md`](../docs/data/dataset-overview.md). Volume
file reading starts in Module 5.

## Notebook 01 — Creating DataFrames

### Context

Lab after the **Creating DataFrames** Notion page. Four ways to create a
DataFrame from Python rows.

### Learning objectives

- Create DataFrames unnamed/inferred, named/inferred, named with DDL, and
  named with `StructType`
- Inspect each path and explain inferred vs production risk

### Lesson flow

Create without columns/schema (`_1`, `_2`, …); named + inferred (compare
types with the RideEase model on Notion); DDL (`int` / `decimal(8,2)` vs
inferred `long` / `double`); `StructType` (same contract as DDL); inspect
each path.

### Expected state

Not applicable — no persistent data state.

### Next

`02 - Inspecting DataFrames`

## Notebook 02 — Inspecting DataFrames

### Context

Lab after the **Inspecting DataFrames** Notion page. Inspect beyond a first
look: contents, structure, size, and summary stats.

### Learning objectives

- Inspect contents with `show` options and `display`
- Inspect structure with `printSchema`, `schema`, `columns`, `dtypes`
- Check size with `count` and `isEmpty`; use `describe` / `summary`
- Distinguish metadata checks from methods that run Spark work

### Lesson flow

One intentionally bad trip (negative `trip_distance_miles`, huge
`ride_duration_mins`); contents: `show` options (`n`, `truncate`,
`vertical`) / `display`; structure: `printSchema`, `schema`, `columns`,
`dtypes` (metadata — no Spark job); size: `count`, `isEmpty`; a filter can
make a DataFrame empty; `describe` / `summary`.

### Expected state

Not applicable — no persistent data state.

### Next

`03 - Selecting and Transforming Columns`

## Notebook 03 — Selecting and Transforming Columns

### Context

Lab after the **Selecting and Transforming Columns** Notion page. Reshape
columns with the DataFrame API — the transforms later notebooks reuse.

### Learning objectives

- Select, add, rename, recalculate, and drop columns
- Transforms return a new DataFrame
- Build Column expressions with `F.col`, `alias`, light `cast`, `F.lit`, and
  `F.when` / `otherwise`
- Choose `select` vs `withColumn` and chain into a small ops-style output

### Lesson flow

`select` / reorder; `select` returns a new DataFrame (`df` unchanged); name
strings vs `F.col`; `alias`, arithmetic, light `cast`, `F.lit`; `F.when` /
`otherwise`; add with `withColumn` vs `select`; recalculate with
`withColumn` vs `select("*", expr.alias(existing_name))` (duplicate column
names); `withColumns`; `withColumnRenamed` / `withColumnsRenamed` / `drop`
(missing names do not error); chain into a small ops-style output; source
`df` unchanged.

### Expected state

Not applicable — no persistent data state.

### Next

`04 - SQL Expressions in DataFrame Code`

## Notebook 04 — SQL Expressions in DataFrame Code

### Context

Lab after the **SQL Expressions in DataFrame Code** Notion page. Express the
same column logic as SQL strings inside DataFrame code.

### Learning objectives

- Use `F.expr` and `selectExpr`, including SQL `CASE WHEN`
- Reuse named SQL strings

### Lesson flow

`F.expr` (SQL string → Column; reuse `mph_sql`); `selectExpr` (SQL strings
→ new DataFrame; pass `mph_sql` with no `F.expr`); SQL `CASE WHEN` via
`F.expr` and `selectExpr`; `selectExpr` ops-style output; source `df`
unchanged.

### Expected state

Not applicable — no persistent data state.

### Boundaries

`%sql` / `spark.sql` (notebook 06).

### Next

`05 - Filtering Rows`

## Notebook 05 — Filtering Rows

### Context

Lab after the **Filtering Rows** Notion page. Keep rows with `filter` /
`where`, including intro NULL and blank traps.

### Learning objectives

- Filter with Column ops and SQL strings; combine with `AND` vs `&`
- Use `|`, `~`, `isin`, `between`, `like`
- Apply intro NULL checks (`isNull` / `isNotNull`); empty string ≠ NULL

### Lesson flow

Sample includes NULL, empty-string, and negative-distance rows; `filter` /
`where`; combine with SQL `AND` vs Column `&` (parens); Python `and` /
`or` / `not` fail on Columns; `|`, `~`, `isin`, `between`, `like`; intro
NULL (`== None` and `!= None` vs `isNull` / `isNotNull`); empty string ≠
NULL; chain a usable-trip filter into one output; source `df` unchanged.

### Expected state

Not applicable — no persistent data state.

### Next

`06 - Querying DataFrames with SQL`

## Notebook 06 — Querying DataFrames with SQL

### Context

Query a DataFrame through a session temporary view and Spark SQL, then a
classic-only global temporary view.

### Learning objectives

- Explain why `%sql` and `spark.sql` cannot see a Python DataFrame variable
- Register a session temporary view with `createOrReplaceTempView`
- Query that view with `%sql` and with `spark.sql`
- Register a global temporary view with `createOrReplaceGlobalTempView` and
  query `global_temp` on classic compute

### Lesson flow

Why `%sql` / `spark.sql` cannot see a Python variable (`SELECT … FROM df`
fails); session temp views (`createOrReplaceTempView("trips")`); `%sql`;
`spark.sql` returns a DataFrame you can keep transforming in Python;
`createOrReplaceGlobalTempView("trips_global")`; query
`global_temp.trips_global`; prefer session views.

### Expected state

Not applicable — no persistent data state.

Global temporary views require classic all-purpose compute. They are not
supported on serverless.

### Boundaries

`F.when` / `F.expr` / `selectExpr` (notebooks 03–04). Side-by-side DataFrame
remakes of the same SQL query (Module 9). Persisted tables.

### Next

Module 3 — Data Cleaning, NULL Semantics, and Type Handling.

## Minimum privileges required

- Unity Catalog: none — this module does not read or write governed data
- Workspace: **`CAN ATTACH TO`** (or **`CAN RESTART`**) on the compute used here
