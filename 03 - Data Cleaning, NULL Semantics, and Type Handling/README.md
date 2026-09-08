# Module 3 — Data Cleaning, NULL Semantics, and Type Handling

## Purpose

Fix imperfect values and write NULL-aware predicates on hand-built rideshare
DataFrames — before file-based ingestion. Module 2 introduced the DataFrame
API and filter traps; this module goes deeper on three-valued logic, messy
values, safe casting, and parsing under Spark 4 / ANSI mode (prefer `try_*`
helpers over disabling ANSI globally).

## Learning objectives

By the end of this module, you'll be able to:

- Explain three-valued logic (`TRUE` / `FALSE` / `NULL`) and why filters keep
  only `TRUE` rows
- Build NULL-safe predicates with `isNull` / `isNotNull`, the `isin` + NULL
  trap, and `eqNullSafe` / `<=>`
- Identify missing data as `NULL`, blanks, sentinels, and `NaN`; normalize to
  real `NULL` before drop/fill
- Use `na.drop` (`how="any"` / `"all"`, `subset`), `na.fill`, and `na.replace`;
  use `F.coalesce` for column fallbacks (not partition `DataFrame.coalesce(n)`)
- Cast with `cast` and `try_cast`; detect rows rejected by a cast
- Handle numeric overflow and unparseable dates/timestamps with Spark 4 /
  ANSI `try_*` helpers
- Chain cleaning and predicate logic on small hand-built DataFrames

## Prerequisites

Module 2 — DataFrame Fundamentals. You should already know `select`,
`withColumn`, `filter` / `where`, `F.col`, `F.when`, intro NULL checks, and
empty string vs `NULL`.

## Dataset

Small **ad-hoc** rideshare-flavored DataFrames built in code, aligned with
[`docs/data/dataset-overview.md`](../docs/data/dataset-overview.md). Volume
file reading starts in Module 5.

## Notebook 01 — NULL Semantics and Predicate Correctness

### Context

Three-valued logic and NULL-safe predicates — before messy-value cleanup.

### Learning objectives

- Explain three-valued logic and why filters keep only `TRUE` rows
- Build NULL-safe predicates with `isNull` / `isNotNull`, the `isin` + NULL
  trap, and `eqNullSafe` / `<=>`

### Lesson flow

Three-valued logic as columns (`TRUE` / `FALSE` / `NULL`); filters keep only
`TRUE`; `isNull` / `isNotNull`; `isin` + `None` trap; `eqNullSafe` / `<=>`;
reusable eligibility / quality predicate chain.

### Expected state

Not applicable — no persistent data state.

### Exercise

NULL-safe filter on a slightly different messy DataFrame.

### Next

`02 - Missing, Blank, and Sentinel Values`

## Notebook 02 — Missing, Blank, and Sentinel Values

### Context

Normalize missing shapes to real `NULL` before drop/fill.

### Learning objectives

- Identify `NULL`, blanks, sentinels, and `NaN`
- Use `na.drop` / `na.fill` / `na.replace` and `F.coalesce` (not partition
  coalesce)

### Lesson flow

`NULL`, blanks, sentinels (`"N/A"`, `-1`), `NaN`; normalize before `na.drop`
/ `na.fill`; `na.drop` (`how="any"` / `"all"`, `subset`) / `na.fill` /
`na.replace`; `F.coalesce` (not partition coalesce).

### Expected state

Not applicable — no persistent data state.

### Exercise

Sentinel normalize on a slightly different messy DataFrame.

### Next

`03 - Safe Type Casting`

## Notebook 03 — Safe Type Casting

### Context

`cast` vs `try_cast` under Spark 4 / ANSI, and rejected-row detection.

### Learning objectives

- Cast with `cast` and `try_cast`
- Detect rows rejected by a cast

### Lesson flow

`cast` vs `try_cast` under Spark 4 / ANSI; rejected-row pattern
(`source.isNotNull() & casted.isNull()`); unsupported type pairs.

### Expected state

Not applicable — no persistent data state.

### Exercise

Rejected-cast detection on a slightly different messy DataFrame.

### Next

`04 - Numeric Overflow and Date-Timestamp Parsing`

## Notebook 04 — Numeric Overflow and Date-Timestamp Parsing

### Context

Overflow and unparseable dates/timestamps with Spark 4 / ANSI `try_*`
helpers.

### Learning objectives

- Handle numeric overflow (`try_sum` / `try_avg`)
- Parse dates/timestamps with formats and session timezone; use
  `try_to_date` / `try_to_timestamp`

### Lesson flow

Cast / arithmetic overflow; `try_sum` / `try_avg`; `to_date` /
`to_timestamp` with formats; session timezone (`spark.sql.session.timeZone`);
`try_to_date` / `try_to_timestamp`; invalid source vs invalid format.

### Expected state

Not applicable — no persistent data state.

### Exercise

Safe date parse on a slightly different messy DataFrame.

### Next

Module 4 — Transformations, Actions, and Lazy Evaluation.

## Minimum privileges required

- Unity Catalog: none — hand-built DataFrames only
- Workspace: **`CAN ATTACH TO`** (or **`CAN RESTART`**) on the compute used here
