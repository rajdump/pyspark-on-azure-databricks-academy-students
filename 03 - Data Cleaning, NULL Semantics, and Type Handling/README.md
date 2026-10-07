# Module 3 — Data Cleaning, NULL Semantics, and Type Handling

## Purpose

Fix imperfect values and write NULL-aware predicates on hand-built rideshare
DataFrames — before file-based ingestion.

## Learning objectives

By the end of this module, you'll be able to:

- Explain three-valued logic and why filters keep only `TRUE` rows
- Build NULL-safe predicates with `isNull` / `isNotNull`, the `isin` + NULL
  trap, and `eqNullSafe` / `<=>`
- Identify `NULL`, blanks, sentinels, and `NaN`; normalize before drop/fill
- Use `na.drop`, `na.fill`, `na.replace`, and `F.coalesce`
- Cast with `cast` and `try_cast`; detect rejected rows
- Handle numeric overflow and unparseable dates with Spark 4 / ANSI `try_*`
  helpers

## Prerequisites

Module 2 — DataFrame Fundamentals. Classic all-purpose compute or serverless.

## Dataset

Small **ad-hoc** rideshare-flavored DataFrames built in code, aligned with
[`docs/data/dataset-overview.md`](../docs/data/dataset-overview.md). Volume
file reading starts in Module 5.

## Notebook 01 — NULL Semantics and Predicate Correctness

### Context

Three-valued logic and NULL-safe predicates — before messy-value cleanup.

### Learning objectives

- Explain three-valued logic and why filters keep only `TRUE` rows
- Build NULL-safe predicates with `isNull` / `isNotNull`, and `eqNullSafe` /
  `<=>`

### Lesson flow

Card-tip reward columns (`TRUE` / `FALSE` / `NULL`); filter keeps only
`TRUE`; `isNull` / `isNotNull` on `payment_method`; `~isin` drops `NULL`
pickup (zones 74 and 231); `None` in the `isin` list empties the filter;
`isNull() | ~isin(74, 231)` keeps missing pickup; `eqNullSafe` / `<=>`;
filter `location_allowed & qualifies_for_reward`.

### Expected state

Not applicable — no persistent data state.

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

`NULL` vs blanks / `"N/A"` / `-1` / `NaN`; trim payment; store sentinels,
`NaN`, and empty payment as `NULL`; count missing payments; `na.fill`;
`na.drop` (`how="any"` / `"all"`, `subset`); `F.coalesce` recorded / backup /
`"unknown"`; chain normalize, decide, validate.

### Expected state

Not applicable — no persistent data state.

### Next

`03 - Safe Type Casting`

## Notebook 03 — Safe Type Casting

### Context

Invalid text under ANSI: `cast` fails the job; `try_cast` returns `NULL`.

### Learning objectives

- Use `cast` and see invalid text fail the job under ANSI
- Use `try_cast` so invalid text becomes `NULL` and the job continues
- Find rejected rows with `source.isNotNull() & casted.isNull()`

### Lesson flow

String `base_fare_amount` including `"N/A"` and a true `NULL`; `cast` to
`decimal(10,2)` fails (`CAST_INVALID_INPUT`); `try_cast` writes `NULL` for
invalid text; original `NULL` is not a rejected row.

### Expected state

Not applicable — no persistent data state.

### Next

`04 - Numeric Overflow and Date-Timestamp Parsing`

### Boundaries

Overflow and date/timestamp parsing wait for notebook **04**. Do not disable
ANSI.

## Notebook 04 — Numeric Overflow and Date-Timestamp Parsing

### Context

Overflow and unparseable dates under ANSI: `+` / `to_date` fail the job;
`try_add` / `try_to_date` / `try_to_timestamp` return `NULL`.

### Learning objectives

- Use `+` and see overflow fail the job under ANSI
- Use `try_add` so overflow becomes `NULL` and the job continues
- Parse dates with `try_to_date` / `try_to_timestamp` and find failed
  conversions

### Lesson flow

`trip_id`, `ride_duration_mins`, `trip_date` text including `"not-a-date"`
and a true `NULL`; `+` fails (`ARITHMETIC_OVERFLOW`) on max `int`; `try_add`
writes `NULL`; `to_date` with `yyyy-MM-dd` fails (`CAST_INVALID_INPUT`);
print session timezone; `try_to_date` / `try_to_timestamp` write `NULL` for
invalid text; original `NULL` is not a failed conversion.

### Expected state

Not applicable — no persistent data state.

### Next

Module 4 — Transformations, Actions, and Lazy Evaluation.

### Boundaries

Integer `cast` overflow, `try_sum` / `try_avg` / `try_divide`, and invalid
format patterns are out of scope. Do not disable ANSI.

## Minimum privileges required

- Unity Catalog: none — hand-built DataFrames only
- Workspace: **`CAN ATTACH TO`** (or **`CAN RESTART`**) on the compute used here
