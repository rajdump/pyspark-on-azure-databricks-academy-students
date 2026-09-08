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
  (including `CASE WHEN`) and choose the clearer form
- Filter with `filter` / `where` (Column ops and SQL strings); intro NULL
  checks (`isNull` / `isNotNull`); empty string ≠ NULL
- Register a session temporary view with `createOrReplaceTempView`; query with
  `%sql` and `spark.sql`; recognize global temporary views on classic compute
- Prefer clear chained transforms that leave the original frame unchanged
  until you assign a new one

## Prerequisites

Module 1 — Azure Databricks and Spark Foundations. You should already attach
compute, use notebook cells, and create a small DataFrame with
`spark.createDataFrame(rows, columns)` plus basic `show` / `display` /
`printSchema`.

## Dataset

Small **ad-hoc** rideshare-flavored DataFrames built in code, aligned with
[`docs/data/dataset-overview.md`](../docs/data/dataset-overview.md). Volume
file reading starts in Module 5.

## Notebook 01 — Creating DataFrames

### Context

What a DataFrame is, and four ways to create one from Python rows.

### Learning objectives

- Explain what a Spark DataFrame is
- Create DataFrames unnamed/inferred, named/inferred, named with DDL, and
  named with `StructType`
- Inspect each path and explain inferred vs production risk

### Lesson flow

What a DataFrame is; create without columns/schema (`_1`, `_2`, …); named +
inferred; DDL; `StructType`; inspect each path; inferred vs production risk;
keep examples tiny (2–3 rows).

### Expected state

Not applicable — no persistent data state.

### Exercise

Medium exercise on inferred vs explicit schema inspection, on a slightly
different rideshare DataFrame.

### Next

`02 - Inspecting DataFrames`

## Notebook 02 — Inspecting DataFrames

### Context

Inspect beyond a first look: contents, structure, size, and summary stats.

### Learning objectives

- Inspect contents with `show` options and `display`
- Inspect structure with `printSchema`, `schema`, `columns`, `dtypes`
- Check size with `count` and `isEmpty`; use `describe` / `summary`
- Distinguish metadata checks from methods that run Spark work

### Lesson flow

Contents: `show` options (`n`, `truncate`, `vertical`) / `display`;
structure: `printSchema`, `schema`, `columns`, `dtypes`; size: `count`,
`isEmpty`; `describe` / `summary`; metadata checks vs methods that run Spark
work.

### Expected state

Not applicable — no persistent data state.

### Exercise

Short hands-on on a slightly different rideshare DataFrame.

### Next

`03 - Selecting and Transforming Columns`

## Notebook 03 — Selecting and Transforming Columns

### Context

Reshape columns with the DataFrame API — the transforms later notebooks
reuse.

### Learning objectives

- Select, add, rename, recalculate, and drop columns
- Build Column expressions with `F.col`, `alias`, light `cast`, `F.lit`, and
  `F.when` / `otherwise`
- Choose `select` vs `withColumn` and chain into a small ops-style output

### Lesson flow

`select` / immutability; name strings vs `F.col`; `alias`, arithmetic, light
`cast`, `F.lit`; `F.when` / `otherwise`; `withColumn` / `withColumns`;
`withColumnRenamed` / `withColumnsRenamed` / `drop`; when to use `select` vs
`withColumn`; chain into a small ops-style output.

### Expected state

Not applicable — no persistent data state.

### Exercise

Short hands-on on a slightly different rideshare DataFrame.

### Next

`04 - SQL Expressions in DataFrame Code`

## Notebook 04 — SQL Expressions in DataFrame Code

### Context

Express the same column logic as SQL strings inside DataFrame code.

### Learning objectives

- Use `F.expr` and `selectExpr`, including SQL `CASE WHEN`
- Recognize misspelled-column `AnalysisException` across styles
- Distinguish Python `SyntaxError` from Spark SQL parse errors and choose
  a consistent style

### Lesson flow

`F.expr`; `selectExpr`; SQL `CASE WHEN`; misspelled columns
(`AnalysisException`) across styles; Python `SyntaxError` vs Spark SQL parse
errors; choose and reuse related rules consistently.

### Expected state

Not applicable — no persistent data state.

### Exercise

Short hands-on on a slightly different rideshare DataFrame.

### Next

`05 - Filtering Rows`

## Notebook 05 — Filtering Rows

### Context

Keep rows with `filter` / `where`, including intro NULL and blank traps.

### Learning objectives

- Filter with Column ops and SQL strings; combine with `AND` vs `&`
- Use `|`, `~`, `isin`, `between`, `like`
- Apply intro NULL checks (`isNull` / `isNotNull`); empty string ≠ NULL

### Lesson flow

`filter` / `where`; combine with SQL `AND` vs Column `&` (parens); why Python
`and` fails; `|`, `~`, `isin`, `between`, `like`; intro NULL (`isNull` /
`isNotNull`; `== None` fails); empty string ≠ NULL; deeper NULL → Module 3.

### Expected state

Not applicable — no persistent data state.

### Exercise

Short hands-on on a slightly different rideshare DataFrame.

### Next

`06 - Querying DataFrames with SQL`

## Notebook 06 — Querying DataFrames with SQL

### Context

Query a DataFrame through temp views and Spark SQL — including when
side-by-side APIs are the learning objective.

### Learning objectives

- Express the same calculated column via `F.when`, `F.expr`, and `selectExpr`
- Register a session temporary view; query with `%sql` and `spark.sql`
- Recognize global temporary views on classic compute (not serverless)

### Lesson flow

Same calculated column via `F.when`, `F.expr`, `selectExpr`; why `%sql`
cannot see a Python variable; session temp views (`createOrReplaceTempView`);
`%sql` and `spark.sql`; global temp views (`global_temp`) — classic only /
not serverless; session vs global vs persisted table.

### Expected state

Not applicable — no persistent data state.

### Exercise

Short hands-on on a slightly different rideshare DataFrame.

### Next

Module 3 — Data Cleaning, NULL Semantics, and Type Handling.

## Minimum privileges required

- Unity Catalog: none — this module does not read or write governed data
- Workspace: **`CAN ATTACH TO`** (or **`CAN RESTART`**) on the compute used here
- Global temporary view demo (**`06 - Querying DataFrames with SQL`**): classic
  all-purpose compute; not available on serverless
