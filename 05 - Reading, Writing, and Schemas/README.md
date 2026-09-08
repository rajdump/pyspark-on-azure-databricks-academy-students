# Module 5 — Reading, Writing, and Schemas

## Purpose

Land the shared rideshare dataset on UC Volumes and read/write production
formats with explicit schemas.

## Learning objectives

By the end of this module, you'll be able to:

- Set Tier 1 lab config (storage account, container, storage credential,
  ADLS folder) and create `rideshare_dev` landing/processed volumes
- Copy repo source files into
  `/Volumes/rideshare_dev/landing/source_files/{dataset}/` and verify
- Land full-size controlled-bad `bad_trip_data.csv` and `bad_payment_data.csv`
  variants for the Module 6 cleaning walkthrough
- Read one production format per dataset — CSV, JSON Lines, Parquet, XML,
  Avro — with explicit schemas and informed use of **`inferSchema`**
- Apply light reshape after read; compare format trade-offs
- Write practice outputs under
  `/Volumes/rideshare_dev/processed/output_files/practice/{output_name}/`
- Use save modes and a brief partitioned write; preview Delta as a **file**
  format under
  `/Volumes/rideshare_dev/processed/output_files/practice/` and create
  managed table **`rideshare_dev.processed.trip_time_preview`** with
  **`saveAsTable`** (files vs managed tables)

## Prerequisites

Module 4 — Transformations, Actions, and Lazy Evaluation. Understand
transformations vs actions, lazy evaluation, and that **`DataFrame.write`**
returns a writer; execution happens on terminal methods such as **`.save()`**,
**`.parquet()`**, or **`.saveAsTable()`**.

Each student uses **their own** Azure storage account and Databricks
workspace. `01 - Unity Catalog Volumes and Data Landing.py` creates the
course catalog, external location, schemas, and volumes in that account.

### Before Notebook 01

Complete these before running **`01 - Unity Catalog Volumes and Data Landing`**:

1. Own Azure Databricks workspace with Unity Catalog (Premium-capable)
2. Ability to **`CREATE CATALOG`** and **`CREATE EXTERNAL LOCATION`** on the
   metastore, plus **`CREATE EXTERNAL LOCATION`** on the storage credential
   named in the config cell
3. Azure Data Lake Storage Gen2 account + container, and a Unity Catalog
   **storage credential** that already exists and can access that storage
   (Access Connector / credential setup is in the course PDF — not this repo)
4. This course repo as a Databricks **Git folder** (open Notebook **01** from
   that folder so the copy cell can find `data/raw`)
5. Notebook attached to compute
6. In **`01 - Unity Catalog Volumes and Data Landing`** and
   **`99 - Rideshare Project Cleanup and Reset`**, overwrite the config cell
   with **your** storage account, container, storage credential, and ADLS
   folder

## Dataset

Schemas, column names, Volume path rules, and the repo → Volume upload map:
[`docs/data/dataset-overview.md`](../docs/data/dataset-overview.md).

| Role | Path |
|---|---|
| Reads | `/Volumes/rideshare_dev/landing/source_files/{dataset}/` |
| Module 5 writes | `/Volumes/rideshare_dev/processed/output_files/practice/{output_name}/` |

Do **not** use shorthand `processed/` alone. The `practice/` and `curated/`
tiers under `/Volumes/rideshare_dev/processed/output_files/` are created on
first write — `01 - Unity Catalog Volumes and Data Landing.py` does not
pre-create them. Schema names `landing` / `processed` are not medallion
layers (Modules 12–13).

`01 - Unity Catalog Volumes and Data Landing.py` creates platform objects;
**Module 12** explains governance (grants, ownership, credentials, least
privilege) on those existing objects.

## Notebook 01 — Unity Catalog Volumes and Data Landing

### Context

Create the course catalog, volumes, and land repo source files — including
controlled-bad CSVs for Module 6.

### Learning objectives

- Set Tier 1 lab config and create `rideshare_dev` landing/processed volumes
- Copy canonical + controlled-bad sources into landing and verify

### Lesson flow

Config cell (your Azure values); create ADLS project folder in Azure Portal;
external location `el_rideshare_dev`, catalog `rideshare_dev`, schemas,
volumes; `mkdirs`; copy canonical + controlled-bad sources into landing;
verify.

### Expected state

- Input: repo `data/raw` (open from the Git folder) plus config-cell Azure
  values
- Output: landing files under
  `/Volumes/rideshare_dev/landing/source_files/{dataset}/`; catalog
  `rideshare_dev`; external location `el_rideshare_dev`; schemas and volumes

### Exercise

Short hands-on on the landing you just created.

### Next

`02 - Reading CSV`

## Notebook 02 — Reading CSV

### Context

Read **`trip`** from landing with an explicit schema.

### Learning objectives

- Read CSV with an explicit schema vs **`inferSchema`**
- Apply light reshape; write a practice output

### Lesson flow

Read **`trip`** from landing; explicit schema vs **`inferSchema`**; light
reshape; practice write.

### Expected state

- Input: `/Volumes/rideshare_dev/landing/source_files/trip/`
- Output: practice write under
  `/Volumes/rideshare_dev/processed/output_files/practice/{output_name}/`

### Exercise

Short hands-on on the CSV read/write pattern.

### Next

`03 - Reading JSON`

## Notebook 03 — Reading JSON

### Context

Read **`zone_lookup`** (JSON Lines) from landing.

### Learning objectives

- Read JSON Lines with an explicit schema

### Lesson flow

Read **`zone_lookup`** (JSON Lines) from landing.

### Expected state

- Input: `/Volumes/rideshare_dev/landing/source_files/zone_lookup/`

### Exercise

Short hands-on on the JSON read.

### Next

`04 - Reading Parquet`

## Notebook 04 — Reading Parquet

### Context

Read **`trip_time`** from landing.

### Learning objectives

- Read Parquet with an explicit schema

### Lesson flow

Read **`trip_time`** from landing.

### Expected state

- Input: `/Volumes/rideshare_dev/landing/source_files/trip_time/`

### Exercise

Short hands-on on the Parquet read.

### Next

`05 - Reading XML`

## Notebook 05 — Reading XML

### Context

Read **`drivers`** with **`rowTag`** only — nested flatten is Module 6.

### Learning objectives

- Read XML with **`rowTag`** only — no **`explode`**

### Lesson flow

Read **`drivers`** with **`rowTag`** only — no **`explode`** (Module 6).

### Expected state

- Input: `/Volumes/rideshare_dev/landing/source_files/drivers/`

### Exercise

Short hands-on on the XML read.

### Next

`06 - Reading Avro`

## Notebook 06 — Reading Avro

### Context

Read **`payment`** from landing (Avro copied in notebook **01**).

### Learning objectives

- Read Avro with an explicit schema

### Lesson flow

Read **`payment`** from landing (Avro copied in
`01 - Unity Catalog Volumes and Data Landing.py`).

### Expected state

- Input: `/Volumes/rideshare_dev/landing/source_files/payment/`

### Exercise

Short hands-on on the Avro read.

### Next

`07 - Write Patterns and Table Preview`

## Notebook 07 — Write Patterns and Table Preview

### Context

Save modes, a brief partitioned write, Delta as a **file** format, and a
managed **`saveAsTable`** preview.

### Learning objectives

- Use save modes and a brief partitioned write
- Preview Delta as a file format under `practice/` and create managed table
  **`rideshare_dev.processed.trip_time_preview`**
- Distinguish files vs managed tables

### Lesson flow

Save modes; brief partitioned write; Delta **file** under
`/Volumes/rideshare_dev/processed/output_files/practice/` + managed
**`saveAsTable`** to **`rideshare_dev.processed.trip_time_preview`**
(managed location ≠ external volume); files vs tables; Module 6
`01 - Column Transforms with Built-in Functions.py` reads this table
alongside landing **`trip_time`** Parquet; deep Delta → Module 10.

### Expected state

- Input: landing **`trip_time`** (and prior practice patterns)
- Output: Delta files under
  `/Volumes/rideshare_dev/processed/output_files/practice/`; managed table
  **`rideshare_dev.processed.trip_time_preview`**

### Exercise

Short hands-on on save modes / table preview.

### Next

Module 6 `01 - Column Transforms with Built-in Functions`.
`99 - Rideshare Project Cleanup and Reset` is recovery only (clear
`practice/` or tear down) — not the successor.

## Notebook 99 — Rideshare Project Cleanup and Reset

### Context

Utility reset if something goes wrong. All cleanup actions are off by
default.

### Learning objectives

- Clear `practice/` without touching `curated/`
- Clear `curated/` (wide blast radius)
- Clear landing and recopy from notebook **01**
- Fully tear down catalog, external location, and ADLS folder while leaving
  the storage credential in place

### Lesson flow

Level 1 clear `/Volumes/rideshare_dev/processed/output_files/practice/`;
Level 2 clear `/Volumes/rideshare_dev/processed/output_files/curated/`
(Module 6 Parquet); Level 3 clear landing; Level 4 full teardown (drops
managed tables including Module 7/8 `saveAsTable` outputs).

### Expected state

Not applicable — no persistent data state this notebook is required to leave
behind. It removes objects created by this module and later writes.

### Exercise

An exercise does not apply — this is a cleanup/reset utility.

### Boundaries

Level 2 deletes Module 6 curated Parquet. Level 4 drops the catalog and
managed tables (including Module 7/8 `saveAsTable` outputs). Flags stay off
until the learner intends that blast radius.

### Next

`01 - Unity Catalog Volumes and Data Landing` (recovery returns to the
workflow that invokes this notebook).

## Minimum privileges required

- Unity Catalog: **`CREATE CATALOG`** and **`CREATE EXTERNAL LOCATION`** on the
  metastore; **`CREATE EXTERNAL LOCATION`** on the storage credential in the
  config cell; **`CREATE SCHEMA`**, **`CREATE VOLUME`**, and read/write course
  volumes under `rideshare_dev` after creation
- Workspace: **`CAN ATTACH TO`** (or **`CAN RESTART`**) on the compute used here
- Azure RBAC: roles on **your** storage account for the access connector behind
  your storage credential (including File Events–related roles when testing
  the external location — see `01 - Unity Catalog Volumes and Data Landing.py`
  troubleshooting)
- Storage credential: must already exist; this module does not create it
