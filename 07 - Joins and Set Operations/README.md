# Module 7 — Joins and Set Operations

## Purpose

Join and combine rideshare tables with predictable row counts and clear keys —
no silent cardinality or key traps (M:M fanout, wrong outer join, ambiguous
columns after a dimension lookup).

## Learning objectives

By the end of this module, you'll be able to:

- Define **grain** and **cardinality** (1:1, 1:M, M:1, M:M) and predict join
  row counts before running
- Write equi-joins in **string**, **list**, and **Boolean** form
- Choose **inner / left / right / full** and explain NULL-key behavior
  (`NULL = NULL` is not true; use **`eqNullSafe`** when NULLs must match)
- Profile keys (`rows`, `countDistinct`, nulls) and resolve duplicates with
  **`Window` + `row_number`** (not `dropDuplicates`) when payloads differ
- Run a **repeated lookup join** (`zone_lookup` for pickup and dropoff),
  clean columns with **`select` / rename**, and **`F.broadcast`** a small
  dimension (confirm in **`.explain()`**)
- Use **`left_semi` / `left_anti`**, and contrast anti-join with **`subtract()`**
- Combine frames with **`union` / `unionByName` / `intersect` /
  `intersectAll` / `subtract` / `exceptAll`**
- Apply those patterns in `07 - Build Unified Curated Tables.py` to write
  **`trip_enriched`** and **`trip_driver_assignment`**

## Prerequisites

Complete Module 6 notebooks **`01`–`04`**. You need:

| Asset | Rows / notes | Source |
|---|---|---|
| `/Volumes/rideshare_dev/processed/output_files/curated/trip/` | 106 — one per `trip_id`; has pickup/dropoff location IDs | Module 6 `03 - Cleaning and Curated Outputs.py` |
| `/Volumes/rideshare_dev/processed/output_files/curated/payment/` | 105 — one per `trip_id` (no row for trip 106) | Module 6 `03 - Cleaning and Curated Outputs.py` |
| `/Volumes/rideshare_dev/processed/output_files/curated/drivers_flat/` | one row per (`driver_id`, `trip_id`); trips 1–100 covered | Module 6 `02 - Complex Types, Structs, Arrays, and explode.py` |
| Landing `trip`, `trip_time`, `payment` | 100 each; **1:1** on `trip_id`; `/Volumes/rideshare_dev/landing/source_files/{dataset}/` | Module 5 `01 - Unity Catalog Volumes and Data Landing.py` |
| Landing `zone_lookup` | 22; location_id **21–22** never used by any trip; `/Volumes/rideshare_dev/landing/source_files/zone_lookup/` | Module 5 `01 - Unity Catalog Volumes and Data Landing.py` |

Also recall Module 3 **`eqNullSafe`** for NULL-aware join keys.

Does **not** read
`/Volumes/rideshare_dev/processed/output_files/practice/`. See **Cleanup**
below: Level 1 for `practice/` hygiene; Level 4 to drop managed tables. Do
**not** run Level 2 before this module — that deletes the curated inputs.

## Dataset

Schemas, join keys, and the `zone_lookup` 21–22 design:
[`docs/data/dataset-overview.md`](../docs/data/dataset-overview.md).

Business requirements, target-column scope, and source-to-target mappings:
[`requirements/BRD.md`](requirements/BRD.md) |
[`requirements/trip_enriched_mapping.md`](requirements/trip_enriched_mapping.md) |
[`requirements/trip_driver_assignment_mapping.md`](requirements/trip_driver_assignment_mapping.md).

| Role | Location |
|---|---|
| Landing reads | `/Volumes/rideshare_dev/landing/source_files/{dataset}/` |
| Curated reads (Module 6) | `/Volumes/rideshare_dev/processed/output_files/curated/{name}/` |
| Module writes | Unity Catalog managed tables (below) |

| Output table | Contract |
|---|---|
| `rideshare_dev.processed.trip_enriched` | One row per curated `trip` `trip_id` (106), left-joined to landing `trip_time`, curated `payment`, pickup/dropoff zones. 16 columns: selected trip attributes (excludes operational timing columns per BRD §8) + time + core payment facts (payment method, base fare, tip, driver payout) + borough and zone name for pickup and drop-off. Full payment breakdown stays in `/Volumes/rideshare_dev/processed/output_files/curated/payment/`. |
| `rideshare_dev.processed.trip_driver_assignment` | One row per (`driver_id`, `trip_id`) from `/Volumes/rideshare_dev/processed/output_files/curated/drivers_flat/` plus agreed trip descriptors (service type, distance, duration, and pickup/drop-off location IDs) — 13 columns total. Time, payment, and zone-name attributes remain available through `trip_enriched`. |

**Expected NULLs after left joins (intentional)** — teaching material, not a
defect. Full contract:
[`docs/data/dataset-overview.md`](../docs/data/dataset-overview.md)
(Module 7 — `trip_enriched`).

Driver assignment: no unexpected key gaps on trips 1–100 (every column fully
populated).

Writes use `saveAsTable` overwrite (Delta by default). Delta internals → Module 10.

**Cleanup:** Module 5 `99 - Rideshare Project Cleanup and Reset.py` Level 2
clears Module 6 curated Parquet only — by design, Levels 1–2 never touch
managed tables. To reset Module 7 managed tables (**`trip_enriched`**,
**`trip_driver_assignment`**) and Module 8 KPI tables (**`kpi_*`**), use
**99** Level 4 (full project teardown): its `DROP CATALOG rideshare_dev
CASCADE` step drops every managed table in the catalog, current and future,
with no per-table statement required.

## Notebook 01 — Grain, Join Syntax, and Unmatched Keys

### Context

Grain, cardinality, and join syntax — skill-building only (no write).

### Learning objectives

- Define grain and cardinality and predict join row counts before running
- Write equi-joins in string, list, and Boolean form

### Lesson flow

Grain; 1:1 / 1:M / M:M; string join `trip`↔`trip_time` → 100; list join
`trip_charges`↔`rate_card` (`trip_id` alone → 12 wrong; `["trip_id",
"charge_type"]` → 4); Boolean rename + duplicate-column trap; unmatched-keys
exercise (expect 3 / 5 / 5 / 7).

### Expected state

- Input: landing `trip`, `trip_time` (+ constructed frames). No `payment`.
- Output: none (no write)

### Exercise

Unmatched-keys exercise (expect 3 / 5 / 5 / 7).

### Next

`02 - Silent Join Failures and Validation`

## Notebook 02 — Silent Join Failures and Validation

### Context

M:M fanout, key profiling, and NULL-key traps — still no write.

### Learning objectives

- Profile keys and resolve duplicates with `Window` + `row_number` (not
  `dropDuplicates`) when payloads differ
- Explain NULL-key behavior and use `eqNullSafe` when NULLs must match

### Lesson flow

M:M fanout; key profiling; `dropDuplicates` vs window dedup; NULL keys +
`eqNullSafe`; accidental Cartesian; profile → predict → run → verify.

### Expected state

- Input: landing `trip`, `trip_time`, `payment` (+ frames)
- Output: none (no write)

### Exercise

Short hands-on on the demonstrated join-validation pattern.

### Next

`03 - Lookup Joins, Columns, and Broadcast`

## Notebook 03 — Lookup Joins, Columns, and Broadcast

### Context

Repeated zone lookup, column cleanup, and broadcast — reused in notebook
**07**.

### Learning objectives

- Run a repeated lookup join (`zone_lookup` for pickup and dropoff)
- Clean columns with `select` / rename and `F.broadcast` a small dimension
  (confirm in `.explain()`)

### Lesson flow

Apply **01–02** join/alias/profile patterns (no re-teach); fact vs dim;
repeated pickup/dropoff lookup; `select`/rename; unmatched 21–22 **practice**;
`-1` threshold then `F.broadcast` + `.explain()` (reused in **07**).

### Expected state

- Input: `zone_lookup` (22);
  `/Volumes/rideshare_dev/processed/output_files/curated/trip/` (106)
- Output: none (no write)

### Exercise

Unmatched 21–22 practice.

### Next

`04 - Semi Joins and Anti Joins`

## Notebook 04 — Semi Joins and Anti Joins

### Context

`left_semi` / `left_anti` on curated trip vs payment.

### Learning objectives

- Use `left_semi` / `left_anti`, and contrast anti-join with `subtract()`
  (bridge to **06**)

### Lesson flow

`left_semi` / `left_anti` (trip 106 on anti); reverse anti; bridge to
**`subtract()`** in **06**.

### Expected state

- Input: curated `trip/` (106), curated `payment/` (105)
- Output: none (no write)

### Exercise

Short hands-on on semi/anti.

### Next

`05 - Union and unionByName`

## Notebook 05 — Union and unionByName

### Context

Stack frames with `union` / `unionByName` — constructed frames only.

### Learning objectives

- Combine frames with `union` / `unionByName` (column-order trap;
  `allowMissingColumns`)

### Lesson flow

`union` vs `unionByName`; column-order trap; `allowMissingColumns`; when
`distinct()` after union.

### Expected state

- Input: constructed frames (no landing read)
- Output: none (no write)

### Exercise

Short hands-on on union alignment.

### Next

`06 - Intersect, subtract, and exceptAll`

## Notebook 06 — Intersect, subtract, and exceptAll

### Context

Whole-row set ops on constructed frames.

### Learning objectives

- Use `intersect` / `intersectAll` / `subtract` / `exceptAll` and SQL
  `EXCEPT` naming

### Lesson flow

Whole-row set ops; `intersect` vs `intersectAll`; `subtract` vs `exceptAll`;
SQL `EXCEPT` naming.

### Expected state

- Input: constructed frames (no landing read)
- Output: none (no write)

### Exercise

Short hands-on on set ops.

### Next

`07 - Build Unified Curated Tables`

## Notebook 07 — Build Unified Curated Tables

### Context

Write-only business flow: load, build both tables per mapping docs,
`saveAsTable` overwrite.

### Learning objectives

- Apply the module patterns to write **`trip_enriched`** and
  **`trip_driver_assignment`**

### Lesson flow

Write-only business flow: load → stepwise left joins + zone broadcast →
select 16/13 mapping columns → `saveAsTable` overwrite; short AQE note — no
profiling, validation, or practice.

### Expected state

- Input: curated trip/payment/drivers_flat; landing `trip_time`,
  `zone_lookup`
- Output: `rideshare_dev.processed.trip_enriched` (106 rows, 16 columns) and
  `rideshare_dev.processed.trip_driver_assignment` (13 columns). See Dataset.

### Exercise

An exercise does not apply — this notebook is write-only (no practice).

### Next

Module 8 — Aggregations and Window Functions.

## Minimum privileges required

- Unity Catalog (objects from Module 5 — no catalog/external-location DDL here):
  - **`USE CATALOG`** on **`rideshare_dev`**
  - **`USE SCHEMA`** on **`rideshare_dev.landing`** and **`rideshare_dev.processed`**
  - **`READ VOLUME`** on **`rideshare_dev.landing.source_files`**
  - **`READ VOLUME`** on **`rideshare_dev.processed.output_files`**
    (curated Parquet — `03 - Lookup Joins, Columns, and Broadcast.py`,
    `04 - Semi Joins and Anti Joins.py`,
    `07 - Build Unified Curated Tables.py`)
  - **`CREATE TABLE`** on **`rideshare_dev.processed`**
    (`07 - Build Unified Curated Tables.py` writes)
- Workspace: **`CAN ATTACH TO`** (or **`CAN RESTART`**) on the compute used here
