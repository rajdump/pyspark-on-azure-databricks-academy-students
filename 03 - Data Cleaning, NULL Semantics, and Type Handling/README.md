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
Classic all-purpose or serverless.

### Learning objectives

- Explain three-valued logic and why filters keep only `TRUE` rows
- Build NULL-safe predicates with `isNull` / `isNotNull`, and `eqNullSafe` /
  `<=>`

### Lesson flow

Card-tip reward columns (`TRUE` / `FALSE` / `NULL`); filter keeps only
`TRUE`; `isNull` / `isNotNull` on `payment_method`; `~isin` drops `NULL`
pickup (zones other than 74 and 231); `None` in the `isin` list empties the
filter; `isNull() | ~isin(74, 231)` keeps missing pickup — do not put
`None` in the list; `eqNullSafe` / `<=>`; filter `location_allowed &
qualifies_for_reward`.

### Expected state

Not applicable — no persistent data state.

### Next

`02 - Missing, Blank, and Sentinel Values`

## Notebook 02 — Missing, Blank, and Sentinel Values

### Context

Normalize missing shapes to real `NULL` before drop/fill. Classic
all-purpose or serverless.

### Learning objectives

- Identify `NULL`, blanks, sentinels, and `NaN`
- Use `na.drop` / `na.fill` / `na.replace` and `F.coalesce` (not partition
  coalesce)

### Lesson flow

`NULL` vs blanks / `"N/A"` / `-1` / `NaN`; trim payment; `when` then
`na.replace` to store sentinels, `NaN`, and empty payment as `NULL`; count
missing payments (keep rows); `na.fill` subset then dict then by type;
`na.drop` (`how="any"` / `"all"`, `subset`); `F.coalesce` recorded /
backup / `"unknown"` (not partition coalesce); chain normalize, decide
(drop wait, fill tip and payment), validate.

### Expected state

Not applicable — no persistent data state.

### Next

`03 - Safe Type Casting`

## Notebook 03 — Safe Type Casting

### Context

`cast` fails the job on invalid text under ANSI; `try_cast` returns `NULL`
and the job continues. Then find rejected rows. Classic all-purpose or
serverless.

### Learning objectives

- Use `cast` and see invalid text fail the job under ANSI
- Use `try_cast` so invalid text becomes `NULL` and the job continues
- Find rejected rows with `source.isNotNull() & casted.isNull()`

### Lesson flow

String `base_fare_amount` including `"N/A"` and a true `NULL`; `cast` to
`decimal(10,2)` fails the job (`CAST_INVALID_INPUT`); `try_cast` writes
`NULL` for invalid text; rejected rows are
`source.isNotNull() & casted.isNull()` — an original `NULL` is not
rejected. Do not disable ANSI.

### Expected state

Not applicable — no persistent data state.

### Next

`04 - Numeric Overflow and Date-Timestamp Parsing`

### Boundaries

Overflow, `try_add`, and date/timestamp parsing belong in notebook 04. Do
not teach unsupported type pairs or disable ANSI.

## Notebook 04 — Numeric Overflow and Date-Timestamp Parsing

### Context

`+` fails the job on overflow under ANSI; `try_add` returns `NULL` and the
job continues. `to_date` fails on invalid text; `try_to_date` /
`try_to_timestamp` return `NULL`. Then find failed conversions. Classic
all-purpose or serverless.

### Learning objectives

- Use `+` and see overflow fail the job under ANSI
- Use `try_add` so overflow becomes `NULL` and the job continues
- Parse dates with `try_to_date` / `try_to_timestamp` and find failed
  conversions

### Lesson flow

One DataFrame: `trip_id`, `ride_duration_mins`, `trip_date` text including
`"not-a-date"` and a true `NULL`; `ride_duration_mins + ride_duration_mins`
fails the job (`ARITHMETIC_OVERFLOW`) on the max `int`; `try_add` writes
`NULL`; `to_date` with `yyyy-MM-dd` fails the job (`CAST_INVALID_INPUT`);
print session timezone; `try_to_date` / `try_to_timestamp` write `NULL`
for invalid text; failed conversions are
`source.isNotNull() & parsed.isNull()` — an original `NULL` is not a
failed conversion. Do not disable ANSI.

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
