# Module 7 — Joins and Set Operations

## Purpose

Join and combine rideshare tables with predictable row counts and clear keys —
no silent cardinality or key traps (M:M fanout, wrong outer join, ambiguous
columns after a dimension lookup).

## Learning objectives

By the end of this module, you'll be able to:

- Write equi-joins in **string**, **list**, and **Boolean** form
- Choose **inner / left / right / full** joins for matched and unmatched keys
- Define **grain** and **cardinality** (1:1, 1:M, M:1, M:M) and explain join
  row counts, including row multiplication from repeated keys
- Profile keys (`rows`, `countDistinct`, nulls) before joining, explain
  why `NULL` keys do not match (`NULL = NULL` is not true), and use
  **`eqNullSafe`** when `NULL` keys must match
- Explain **`crossJoin`** and an always-true join condition
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

## Notebook 01 — Introduction to PySpark Joins

### Context

Join syntax and join types on two small constructed DataFrames —
skill-building only (no write).

### Learning objectives

- Write a join with `df1.join(df2, on, how)`
- Write the join condition as a string, a list, or a Boolean expression
- Compare inner, left, right, and full joins when keys are unmatched

### Lesson flow

Two constructed frames with `drivers_flat` columns: `driver_trips`
(D001/8, D002/4) and `driver_trip_details` (D001/8, D002/5).
Single-column join on `trip_id` in string and Boolean form → 1 row each;
multi-column join on `driver_id` + `trip_id` in list and Boolean form →
1 row each; inner / left / right / full on the list form → 1, 2, 2, 3 rows.

### Expected state

- Input: constructed frames only (no file reads)
- Output: none (no write)

### Next

`02 - Join Cardinality, NULL Keys, and Cross Joins`

## Notebook 02 — Join Cardinality, NULL Keys, and Cross Joins

### Context

How many rows a join returns, and why — skill-building only (no write).

### Learning objectives

- Identify 1:1, 1:M, M:1, and M:M joins
- Explain why a join can return more rows than either input DataFrame
- Check join keys for repeated and `NULL` values before joining
- Explain why `NULL` join keys do not match, and match them with `eqNullSafe`
- Explain how a cross join pairs every row with every row

### Lesson flow

Three constructed frames used throughout: `drivers` (D001, D002),
`driver_trips` (D001: 8, 25, 43; D002: 4, 5; `NULL`: 101), and `trips`
(`trip_id`, `service_type`, `pickup_location_id` for the same six trips).
Grain and cardinality: 1:1 `driver_trips` ↔ `trips` on `trip_id` → 6 rows;
1:M `drivers` ↔ `driver_trips` → 5 rows; M:1 reversed → 5 rows; M:M
relationship between drivers and pickup zones, shown by joining `drivers`
→ `driver_trips` → `trips` and selecting `driver_name`,
`pickup_location_id` → 5 rows (Ravi Kumar: zones 1, 8, 9; zone 8: both
drivers). Key profiling with `count("*")`, `countDistinct`,
`NULL` count (2/2/0 vs 6/2/1). `NULL` keys: ad hoc
`drivers_with_unknown` (D001, D002, `NULL` "Unknown") joined to
`driver_trips`: `==` → 5 rows, `eqNullSafe` → 6 rows (trip 101 matches
"Unknown"). `crossJoin` of drivers and distinct service types →
2 × 3 = 6-row grid.

### Expected state

- Input: constructed frames only (no file reads)
- Output: none (no write)

### Next

`03 - Sort-Merge Join vs Broadcast Hash Join`

## Notebook 03 — Sort-Merge Join vs Broadcast Hash Join

### Context

Repeated zone lookup, column cleanup, and broadcast — reused in notebook
**07**.

### Learning objectives

- Run a repeated lookup join (`zone_lookup` for pickup and dropoff)
- Clean columns with `select` / rename and `F.broadcast` a small dimension
  (confirm in `.explain()`)

### Lesson flow

Apply **01–02** join and key-profiling patterns (no re-teach); fact vs dim;
repeated pickup/dropoff lookup; `select`/rename; unmatched 21–22 **practice**;
`-1` threshold then `F.broadcast` + `.explain()` (reused in **07**).

### Expected state

- Input: `zone_lookup` (22);
  `/Volumes/rideshare_dev/processed/output_files/curated/trip/` (106)
- Output: none (no write)

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

### Next

Module 8 — Aggregations and Window Functions.

## Minimum privileges required

- Unity Catalog (objects from Module 5 — no catalog/external-location DDL here):
  - **`USE CATALOG`** on **`rideshare_dev`**
  - **`USE SCHEMA`** on **`rideshare_dev.landing`** and **`rideshare_dev.processed`**
  - **`READ VOLUME`** on **`rideshare_dev.landing.source_files`**
  - **`READ VOLUME`** on **`rideshare_dev.processed.output_files`**
    (curated Parquet — `03 - Sort-Merge Join vs Broadcast Hash Join.py`,
    `04 - Semi Joins and Anti Joins.py`,
    `07 - Build Unified Curated Tables.py`)
  - **`CREATE TABLE`** on **`rideshare_dev.processed`**
    (`07 - Build Unified Curated Tables.py` writes)
- Workspace: **`CAN ATTACH TO`** (or **`CAN RESTART`**) on the compute used here
