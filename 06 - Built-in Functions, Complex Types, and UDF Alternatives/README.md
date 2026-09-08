# Module 6 — Built-in Functions, Complex Types, and UDF Alternatives

## Purpose

Transform landing data with Spark built-ins, work with nested types, and write
curated outputs — prefer built-ins over UDFs.

Python UDFs appear only in the final notebook as a contrast for logic that
cannot be expressed with built-ins. Pandas/Arrow UDFs are mentioned briefly as
an advanced fallback; this course does not teach them further.

## Learning objectives

By the end of this module, you'll be able to:

- Apply string, numeric, date/time, and conditional transforms with
  **`pyspark.sql.functions`** (`F.*`) on DataFrames loaded from Volume paths
- Load the same logical dataset from a **Volume path** and a **managed Unity
  Catalog table**, then apply identical transform chains after load
- Access **struct** fields, work with **array** columns, and flatten nested
  data with **`explode`** / **`explode_outer`**
- Review Module 3 cleaning patterns (NULL-safe predicates, normalization,
  safe casts) on full-size controlled-bad CSV variants and carry those same
  cleaned DataFrames into **curated** outputs
- Explain when to prefer built-ins over **Python UDFs** (and when Pandas/Arrow
  UDFs exist as an advanced fallback outside this course)

## Prerequisites

Complete Module 5 content notebooks **`01`–`07`**. You need:

| Asset | Notes |
|---|---|
| Landing volume | `/Volumes/rideshare_dev/landing/source_files/{dataset}/` populated |
| Controlled-bad CSVs | `/Volumes/rideshare_dev/landing/source_files/trip/bad_trip_data.csv` and `/Volumes/rideshare_dev/landing/source_files/payment/bad_payment_data.csv` from Module 5 `01 - Unity Catalog Volumes and Data Landing.py` |
| Managed table | **`rideshare_dev.processed.trip_time_preview`** from Module 5 `07 - Write Patterns and Table Preview.py` |
| Write model | Comfort with transformations vs actions; **`DataFrame.write`** executes on **`.save()`** / **`.parquet()`** / **`.saveAsTable()`** (Module 4) |

Recall Module 3 (`01 - NULL Semantics and Predicate Correctness.py`–
`03 - Safe Type Casting.py`): NULL-aware filters, normalize-before-drop,
`F.coalesce`, `try_cast`.

This module reads the **landing volume** and (`01 - Column Transforms with
Built-in Functions.py` only) one **managed table**. It does **not** read
`/Volumes/rideshare_dev/processed/output_files/practice/`.

Optional hygiene: Module 5 `99 - Rideshare Project Cleanup and Reset.py`
Level 1 clears `/Volumes/rideshare_dev/processed/output_files/practice/`
before start. Use Level 2 to wipe
`/Volumes/rideshare_dev/processed/output_files/curated/` only when rerunning
from a clean curated state.

## Dataset

Schemas, column names, join keys, and Volume path rules:
[`docs/data/dataset-overview.md`](../docs/data/dataset-overview.md).

| Role | Path |
|---|---|
| Reads | `/Volumes/rideshare_dev/landing/source_files/{dataset}/` |
| Module 6 writes | `/Volumes/rideshare_dev/processed/output_files/curated/{output_name}/` |

| Output | Path | Grain / contract |
|---|---|---|
| Flattened drivers | `/Volumes/rideshare_dev/processed/output_files/curated/drivers_flat/` | One row per **`driver_id`** + **`trip_id`** after **`explode`** on **`trips_assigned`**; trips **1–100** |
| Cleaned trip | `/Volumes/rideshare_dev/processed/output_files/curated/trip/` | One row per **`trip_id`** from **`bad_trip_data.csv`** after missing-key rejection and **`dropDuplicates`**; 106 rows; preserve location join keys |
| Cleaned payment | `/Volumes/rideshare_dev/processed/output_files/curated/payment/` | One row per **`trip_id`** from **`bad_payment_data.csv`** after missing-key rejection; 105 rows |

Write curated outputs as **Parquet** under each folder with
**`.mode("overwrite")`** unless a notebook states otherwise. Module 7 reads
these curated folders plus **landing** datasets such as **`trip_time`** and
**`zone_lookup`** where joins require them.

The `curated/` tier is created on first write. Schema names `landing` /
`processed` are not medallion Bronze/Silver/Gold (Modules 12–13).

**Cleanup:** reuse Module 5 `99 - Rideshare Project Cleanup and Reset.py`
(Level 2 clears Module 6 curated outputs). This module has no dedicated
cleanup notebook.

## Notebook 01 — Column Transforms with Built-in Functions

### Context

Same transforms after load from a Volume path vs a managed table — no curated
write.

### Learning objectives

- Apply string, numeric, date/time, and conditional `F.*` transforms
- Load the same logical dataset from a Volume path and a managed table, then
  apply identical chains after load

### Lesson flow

Volume vs managed table on **`trip_time`**: landing
**`/Volumes/rideshare_dev/landing/source_files/trip_time/trip_time.parquet`**
vs **`rideshare_dev.processed.trip_time_preview`** — same transforms after
load; built-ins by dataset (**`trip`**: string, numeric/decimal, conditional
— no date cols; **`trip_time`**: **`trip_date`**, **`hour_of_day`**; optional
light **`payment`** decimals); no curated write — **`03`** re-reads landing.

### Expected state

- Input: landing **`trip_time`** Parquet and
  **`rideshare_dev.processed.trip_time_preview`**; also landing **`trip`**
  (and optional light **`payment`**)
- Output: none (no curated write)

### Exercise

Short hands-on that repeats the demonstrated pattern on slightly different
columns or values.

### Next

`02 - Complex Types, Structs, Arrays, and explode`

## Notebook 02 — Complex Types, Structs, Arrays, and explode

### Context

Flatten nested **`drivers`** XML and write curated `drivers_flat`.

### Learning objectives

- Access struct fields, work with array columns, and flatten with
  **`explode`** / **`explode_outer`**

### Lesson flow

Landing **`drivers`** XML with **`rowTag`** (same as Module 5
`05 - Reading XML.py` — do not read
`/Volumes/rideshare_dev/processed/output_files/practice/`); struct fields
(**`vehicle.*`**); arrays; **`explode`** / **`explode_outer`** on
**`trips_assigned`**; write
**`/Volumes/rideshare_dev/processed/output_files/curated/drivers_flat/`**.

### Expected state

- Input: landing **`drivers`**
- Output: `/Volumes/rideshare_dev/processed/output_files/curated/drivers_flat/`
  — one row per **`driver_id`** + **`trip_id`**; trips **1–100**. See Dataset.

### Exercise

Short hands-on that repeats the demonstrated pattern on slightly different
columns or values.

### Next

`03 - Cleaning and Curated Outputs`

## Notebook 03 — Cleaning and Curated Outputs

### Context

Full-size controlled-bad CSVs → curated **`trip`** and **`payment`**.

### Learning objectives

- Review Module 3 cleaning patterns on full-size controlled-bad CSV variants
  and persist cleaned DataFrames to curated outputs

### Lesson flow

Full-size **`bad_trip_data.csv`** (108 source rows) and
**`bad_payment_data.csv`** (106 source rows); one forward chain per file
(missing-key rejection, **`dropDuplicates`** on trip **`trip_id`**,
normalization, failed-conversion handling, invalid-value rules); persist
enrichment/cleaning columns only here; write
**`/Volumes/rideshare_dev/processed/output_files/curated/trip/`** and
**`/Volumes/rideshare_dev/processed/output_files/curated/payment/`**.

### Expected state

- Input: landing **`bad_trip_data.csv`** (108 source rows) and
  **`bad_payment_data.csv`** (106 source rows)
- Output: curated **`trip/`** (106 rows) and **`payment/`** (105 rows). See
  Dataset.

### Exercise

Short hands-on that repeats the demonstrated pattern on slightly different
columns or values.

### Next

`04 - Built-ins First, When (Not) to Use UDFs`

## Notebook 04 — Built-ins First, When (Not) to Use UDFs

### Context

Built-ins as default; Python UDF contrast only — do not overwrite curated
outputs.

### Learning objectives

- Explain when to prefer built-ins over Python UDFs (and when Pandas/Arrow
  UDFs exist as an advanced fallback outside this course)

### Lesson flow

Built-ins as default; Python UDF contrast; short Pandas/Arrow note (not
taught further); demo on a small column rule — **do not overwrite** curated
outputs.

### Expected state

- Input: a small column-rule demo (not a curated overwrite)
- Output: none — do not overwrite curated outputs

### Exercise

Short hands-on that repeats the demonstrated pattern on slightly different
columns or values.

### Boundaries

Do not overwrite curated **`trip/`**, **`payment/`**, or **`drivers_flat/`**.

### Next

Module 7 — Joins and Set Operations.

## Minimum privileges required

- Unity Catalog (objects from Module 5 — no **`CREATE CATALOG`**, external
  location, or volume DDL here):
  - **`USE CATALOG`** on **`rideshare_dev`**
  - **`USE SCHEMA`** on **`rideshare_dev.landing`** and
    **`rideshare_dev.processed`**
  - **`READ VOLUME`** on **`rideshare_dev.landing.source_files`**
  - **`WRITE VOLUME`** on **`rideshare_dev.processed.output_files`**
  - **`SELECT`** on **`rideshare_dev.processed.trip_time_preview`**
    (`01 - Column Transforms with Built-in Functions.py` only)
- Workspace: **`CAN ATTACH TO`** (or **`CAN RESTART`**) on the compute used here
