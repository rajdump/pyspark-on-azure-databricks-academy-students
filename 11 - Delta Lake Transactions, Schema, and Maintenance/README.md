# Module 11 — Delta Lake Transactions, Schema, and Maintenance

## Purpose

Apply deletion-vector maintenance, schema change, introductory `MERGE`, and
ACID / optimistic concurrency after the foundations module.

## Learning objectives

By the end of this module, you'll be able to:

- Compare `UPDATE` with and without **deletion vectors**, then physically
  remove old row bytes with `REORG TABLE ... APPLY (PURGE)` and `VACUUM`
- Enforce a table schema, add a column, and apply `NOT NULL` / `CHECK`
- Apply an introductory `MERGE` (matched update and not-matched insert)
- Explain ACID and optimistic concurrency, including a write conflict and
  retry

This module does not teach `OPTIMIZE` or file layout (Module 18), production
incremental `MERGE` (Module 15), or grants (Module 12).

## Prerequisites

Complete Module 10 notebooks **`01`–`04`**. You need the Module 5 platform
(not the teaching pipeline tables):

| Asset | Notes | Source |
|---|---|---|
| Catalog `rideshare_dev`; schema `processed` | Course catalog | Module 5 `01 - Unity Catalog Volumes and Data Landing.py` |
| External location `el_rideshare_dev` | Table `LOCATION` | Module 5 `01 - Unity Catalog Volumes and Data Landing.py` |

Module 10 `04 - Delta Time Travel and Restore` warned that `VACUUM` removes
files time travel needs. Notebook **01** is the first cleanup on an
external table so `LIST` works.

Does **not** read or mutate `trip_enriched`, `trip_driver_assignment`, KPI
tables, or `curated/`.

## Dataset

Notebooks **00–01** use the lab Parquet in
[`docs/data/dataset-overview.md`](../docs/data/dataset-overview.md)
(Module 11) so a one-row rewrite is visible on `LIST` / `DESCRIBE HISTORY`
— not the handmade extract and not the 100-row source files.

Notebook **01** DML uses `row_id` and `passenger_fare` from that file.
Do not invent extra columns. Row picks and new values live in the
notebook, not here.

Notebooks **02–04** use the same handmade extract as Module 10
(`trip_id` **1001–1004**). Columns from `trip.service_type` and `payment`
(no `driver_payout_amount` on first write). `service_type` uppercase;
`payment_method` lowercase.

```text
trip_id bigint
service_type string
payment_method string
base_fare_amount decimal(10,2)
tip_amount decimal(10,2)
```

Notebook **02** adds `driver_payout_amount decimal(10,2)` from
[`payment`](../docs/data/dataset-overview.md#payment). Leave that column
**NULL** on the four lab rows — do not invent payout amounts.

| trip_id | service_type | payment_method | base_fare_amount | tip_amount | Lab use |
|---|---|---|---:|---:|---|
| 1001 | STANDARD | card | 20.00 | 3.00 | 03 exercise: `MERGE` tip → **4.00**; 04 OCC |
| 1002 | SHARED | cash | 15.00 | 0.00 | 02–04: present |
| 1003 | PREMIUM | card | 40.00 | 6.00 | 03 `MERGE` → **10.00**; 04 OCC |
| 1004 | STANDARD | wallet | 25.00 | 2.50 | 03 `MERGE` insert; 04 OCC |

First write in each of **02–04**: deletion vectors **off**. Notebook **01**
sets deletion vectors and auto-compaction **off** after registering
notebook **00**'s table (lab control, not taught). Ignore `.crc` files
in listings.

Object locations:
[`docs/data/dataset-overview.md`](../docs/data/dataset-overview.md)
(Module 11 — Delta Lake Transactions, Schema, and Maintenance). `{url}` is
the `url` column from `DESCRIBE EXTERNAL LOCATION el_rideshare_dev` (strip
a trailing slash).

| Notebook | Object |
|---|---|
| 00 | `data/lab/fare_dv_lab.parquet` → `{url}/external-tables/fare_dv_lab`, then `CONVERT TO DELTA` |
| 01 | `rideshare_dev.processed.fare_dv_lab` at `{url}/external-tables/fare_dv_lab` |
| 02–04 | `rideshare_dev.processed.fare_maint_lab` at `{url}/external-tables/fare_maint_lab` |

External table (not a Volume path). Bound `LOCATION` uses `spark.sql`;
`%sql` only for fixed names. Do not `CREATE TABLE` at a Volume path.

**Cleanup:** **00** `DROP`s leftover `fare_dv_lab` and `rm`s its folder
before copy. **01** must not `rm` that folder. **02–04** setup `DROP`s
`fare_maint_lab` and `rm`s its folder. `DROP TABLE` does not delete those
files. Module 5 `99` Level 1 does not clear `external-tables/` (same as
Module 10 notebook 03).

## Notebook 00 — Copy Fare DV Lab File

### Context

Setup for notebook **01**. Open from the course Git folder.

### Learning objectives

- Copy the lab Parquet, `LIST`, and `CONVERT TO DELTA` so notebook **01**
  can register an external table

### Lesson flow

Setup for notebook **01**. Open from the course Git folder.
`DROP TABLE IF EXISTS fare_dv_lab`; `rm` the destination folder; copy
`fare_dv_lab.parquet`; `LIST`; `CONVERT TO DELTA`; `LIST`. Do not
`CREATE TABLE`.

### Expected state

- Input: `data/lab/fare_dv_lab.parquet`
- Output: `{url}/external-tables/fare_dv_lab` converted to Delta. See
  Dataset.

### Exercise

An exercise does not apply — this is a setup notebook.

### Boundaries

No DV teaching, `UPDATE`, `VACUUM`, `OPTIMIZE`.

### Next

`01 - Deletion Vectors, REORG TABLE, and VACUUM`

## Notebook 01 — Deletion Vectors, REORG TABLE, and VACUUM

### Context

First cleanup of obsolete physical data on **00**'s large external table,
after Module 10's retention/`VACUUM` warning.

### Learning objectives

- Compare `UPDATE` with and without deletion vectors
- Physically remove old row bytes with `REORG TABLE ... APPLY (PURGE)` and
  `VACUUM`

### Lesson flow

After Module 10 `04` retention/`VACUUM` warning: cleanup of obsolete
physical data on **00**'s large external `fare_dv_lab` (not `fare_maint_lab`
/ `fare_timetravel_lab`; do not `rm` **00**'s folder). First cell: title, DV
context, learning objectives only — no Reads / Writes / Prerequisites.
Register **00**'s folder; DV and auto-compact **off** (control, not taught).
Compare one `UPDATE` with DV off vs on (`LIST`, `DESCRIBE HISTORY`).
`DELETE` a row (hidden until purge). `VACUUM RETAIN 0 HOURS`, then
`REORG TABLE ... APPLY (PURGE)`, then a second `VACUUM`.

### Expected state

- Input: **00**'s folder `{url}/external-tables/fare_dv_lab`
- Output: `rideshare_dev.processed.fare_dv_lab` at that location. DML uses
  `row_id` and `passenger_fare` from the lab file only.

### Exercise

An exercise does not apply.

### Boundaries

No `OPTIMIZE`, auto-compact teaching, `VERSION AS OF` / `RESTORE`, `MERGE`,
other `REORG` `APPLY` clauses, or partition `WHERE`. Do not `rm` **00**'s
folder.

### Next

`02 - Schema Enforcement and Evolution`

## Notebook 02 — Schema Enforcement and Evolution

### Context

Enforce schema, add a column, and apply `NOT NULL` / `CHECK` on
`fare_maint_lab`.

### Learning objectives

- Enforce a table schema, add a column, and apply `NOT NULL` / `CHECK`

### Lesson flow

**0** `CREATE` extract columns only (no `driver_payout_amount`), `INSERT`
**1001–1004**, **4** rows. **1** enforcement: write/append a DataFrame that
includes `driver_payout_amount` → expected fail. **2** `ALTER TABLE ADD
COLUMN driver_payout_amount DECIMAL(10,2)`; `mergeSchema` write succeeds;
`SELECT` still **4** rows; payout is **NULL**. **3** `NOT NULL` on
`trip_id`; `CHECK (tip_amount >= 0)`; one insert that violates `CHECK` →
expected fail.

### Expected state

- Output: `rideshare_dev.processed.fare_maint_lab` at
  `{url}/external-tables/fare_maint_lab`
- Expected rows: **4**; `driver_payout_amount` **NULL** on lab rows
- Expected failure: append with extra column before `ALTER`; insert that
  violates `CHECK`

### Exercise

An exercise does not apply.

### Boundaries

No column mapping, `DROP COLUMN`, identity/generated columns, `MERGE`, DV,
or `OPTIMIZE`.

### Next

`03 - Introductory MERGE`

## Notebook 03 — Introductory MERGE

### Context

Matched update and not-matched insert — not production incremental `MERGE`.

### Learning objectives

- Apply an introductory `MERGE` (matched update and not-matched insert)

### Lesson flow

DV off. **0** `CREATE` + `INSERT` **1001–1003** only (**3** rows; 1003 tip
**6.00**; **1004** absent). **1** `MERGE` from a source with 1003 tip
**10.00** and extract row **1004**: `WHEN MATCHED` update tip; `WHEN NOT
MATCHED` insert. **2** `SELECT` **4** rows; 1003 is **10.00**; 1004 present.

### Expected state

- Output: `rideshare_dev.processed.fare_maint_lab`
- Expected rows: after setup **3**; after `MERGE` **4** (1003 tip **10.00**;
  1004 present)

### Exercise

`MERGE` **1001** **3.00 → 4.00**; still **4** rows; 1003 stays **10.00**.

### Boundaries

No production incremental `MERGE` (Module 15), CDF, or `REPLACE WHERE`.

### Next

`04 - ACID and Optimistic Concurrency`

## Notebook 04 — ACID and Optimistic Concurrency

### Context

ACID and optimistic concurrency, including a write conflict and retry.

### Learning objectives

- Explain ACID and optimistic concurrency, including a write conflict and
  retry

### Lesson flow

DV off. **0** `CREATE` + `INSERT` **1001–1004**. **1** `UPDATE` 1003 tip
**6.00 → 10.00**; `DESCRIBE HISTORY`. **2** Explain OCC: readers see a
snapshot; a writer validates against that version; overlapping writers on
the same files conflict. **3** One overlapping-write demo (second writer
loses with a concurrent-modification error, then retries); **4** rows
remain. **4** `SHOW TBLPROPERTIES` glance (`delta.enableDeletionVectors`).
Mention: deletion vectors can allow row-level concurrency for
non-overlapping rows — no lab.

### Expected state

- Output: `rideshare_dev.processed.fare_maint_lab`
- Expected rows: **4** remain after the OCC demo
- Expected failure: concurrent-modification error on the overlapping write,
  then retry

### Exercise

An exercise does not apply.

### Boundaries

No isolation-level tour, checkpoints, protocol versions, or `OPTIMIZE`.

### Next

Module 12 — Unity Catalog and Data Governance.

## Minimum privileges required

- Unity Catalog (no catalog / external-location / volume **CREATE** /
  **DROP** here — Module 5 already created those objects):
  - **`USE CATALOG`** on **`rideshare_dev`**
  - **`USE SCHEMA`** on **`rideshare_dev.processed`**
  - **`CREATE TABLE`** on **`rideshare_dev.processed`**
  - **`CREATE EXTERNAL TABLE`**, **`READ FILES`**, and **`WRITE FILES`**
    on **`el_rideshare_dev`**
  - Table owner **`SELECT`** / **`MODIFY`** on
    **`rideshare_dev.processed.fare_dv_lab`** and
    **`rideshare_dev.processed.fare_maint_lab`**
- Workspace: **`CAN ATTACH TO`** (or **`CAN RESTART`**) on the compute used here
