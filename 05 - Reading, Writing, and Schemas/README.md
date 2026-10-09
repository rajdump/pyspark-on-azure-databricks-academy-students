# Module 5 — Reading, Writing, and Schemas

## Purpose

Land the shared rideshare dataset on UC Volumes and read/write production
formats, comparing schema inference with explicit schemas.

## Learning objectives

By the end of this module, you'll be able to:

- Set Tier 1 lab config (storage account, container, storage credential,
  ADLS folder) and create `rideshare_dev` landing/processed volumes
- Copy repo source files into
  `/Volumes/rideshare_dev/landing/source_files/{dataset}/` and verify
- Land full-size controlled-bad `bad_trip_data.csv` and `bad_payment_data.csv`
  variants for the Module 6 cleaning walkthrough
- Read one production format per dataset — CSV, JSON Lines, Parquet, XML,
  Avro — and compare inferred, embedded, and explicit schemas
- Apply light reshape after read; write each format back and compare what
  types survive the round trip
- Write practice outputs under
  `/Volumes/rideshare_dev/processed/output_files/practice/{output_name}/`
- Use save modes and a brief partitioned write; preview Delta as a **file**
  format under
  `/Volumes/rideshare_dev/processed/output_files/practice/` and create
  managed table **`rideshare_dev.processed.trip_time_preview`** with
  **`saveAsTable`** (files vs managed tables)

## Prerequisites

Module 4 — Transformations, Actions, and Lazy Evaluation. This module
introduces **`DataFrame.write`**: it returns a writer; execution happens on
terminal methods such as **`.save()`**, **`.parquet()`**, or
**`.saveAsTable()`**.

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

### Next

`02 - Reading CSV`

## Notebook 02 — Reading CSV

### Context

Read **`trip`** from landing with an explicit schema.

### Learning objectives

- Read CSV with an explicit schema vs **`inferSchema`**
- Apply light reshape; write a practice output

### Lesson flow

Read **`trip`** from landing; without vs with **`header=True`**;
**`.csv(...)`** shorthand; **`inferSchema`**; explicit schema (DDL string and
**`StructType`**); malformed records on a demo file (**`FAILFAST`**,
**`PERMISSIVE`**, **`DROPMALFORMED`**); light reshape; CSV round trip.

### Expected state

- Input: `/Volumes/rideshare_dev/landing/source_files/trip/`
- Output: `practice/trip_csv_roundtrip/` and `practice/malformed_csv_demo/`
  under `/Volumes/rideshare_dev/processed/output_files/`

### Next

`03 - Reading JSON`

## Notebook 03 — Reading JSON

### Context

Read **`zone_lookup`** (JSON Lines) from landing.

### Learning objectives

- Explain what JSON Lines means
- Compare an inferred JSON schema with an explicit schema
- Handle missing and extra fields with an explicit schema
- Read multiline JSON with **`multiLine=True`**
- Write JSON and read it back with the schema

### Lesson flow

Read **`zone_lookup`** (JSON Lines) from landing; **`.json(...)`** shorthand;
schema inference; explicit schema (DDL string and **`StructType`**); missing
and extra fields; multiline JSON without and with **`multiLine=True`**; light
reshape; JSON round trip.

### Expected state

- Input: `/Volumes/rideshare_dev/landing/source_files/zone_lookup/`
- Expected rows: 22
- Output: `practice/zone_lookup_json_roundtrip/`,
  `practice/zone_lookup_schema_demo/`, and
  `practice/zone_lookup_multiline_demo/` under
  `/Volumes/rideshare_dev/processed/output_files/`

### Next

`04 - Reading Parquet`

## Notebook 04 — Reading Parquet

### Context

Read **`trip_time`** from landing.

### Learning objectives

- Explain why Parquet does not need **`inferSchema`**
- Read Parquet without a schema and with an explicit schema
- Explain how Parquet matches schema columns to file columns
- Write Parquet and read it back with its types

### Lesson flow

Read **`trip_time`** from landing; Parquet vs CSV schema handling;
**`.parquet(...)`** shorthand; read without a schema (types from the footer);
explicit schema (DDL string and **`StructType`**); light reshape; Parquet
round trip.

### Expected state

- Input: `/Volumes/rideshare_dev/landing/source_files/trip_time/`
- Expected rows: 100
- Output: `practice/trip_time_parquet_roundtrip/` under
  `/Volumes/rideshare_dev/processed/output_files/`

### Next

`05 - Reading XML`

## Notebook 05 — Reading XML

### Context

Read **`drivers`** with **`rowTag`** only — nested flatten is Module 6.

### Learning objectives

- Explain why the XML reader needs **`rowTag`**
- Read XML and inspect the structure Spark infers
- Explain how nested XML elements become nested columns
- Select a field inside a nested column

### Lesson flow

XML layout; read fails without **`rowTag`**, succeeds with
**`rowTag="driver"`**; **`.xml(...)`** shorthand; inferred schema; nested
**`vehicle`** and **`trips_assigned`** columns; dot-notation field selection;
write the flat subset as JSON and read it back. No **`explode`** (Module 6).

### Expected state

- Input: `/Volumes/rideshare_dev/landing/source_files/drivers/`
- Expected rows: 12
- Output: `practice/drivers_json_roundtrip/` under
  `/Volumes/rideshare_dev/processed/output_files/`

### Next

`06 - Reading Avro`

## Notebook 06 — Reading Avro

### Context

Read **`payment`** from landing (Avro copied in notebook **01**).

### Learning objectives

- Read Avro and inspect the schema stored in the file
- Explain why Avro does not need **`inferSchema`**
- Read Avro with an explicit schema
- Explain how Avro matches schema fields to file fields
- Write Avro and read the output back

### Lesson flow

Read **`payment`** from landing (Avro copied in
`01 - Unity Catalog Volumes and Data Landing.py`); schema from the file
header, no **`inferSchema`** and no **`.avro(...)`** shorthand; explicit
schema (DDL string and **`StructType`**), matched by name; light reshape;
Avro round trip.

### Expected state

- Input: `/Volumes/rideshare_dev/landing/source_files/payment/`
- Expected rows: 100
- Output: `practice/payment_avro_roundtrip/` under
  `/Volumes/rideshare_dev/processed/output_files/`

### Next

`07 - Write Patterns and Table Preview`

## Notebook 07 — Write Patterns and Table Preview

### Context

Save modes, a brief partitioned write, Delta as a **file** format, and a
managed **`saveAsTable`** preview.

### Learning objectives

- Use the four save modes
- Write a partitioned output
- Write Delta files to a Volume path
- Create a managed table with **`saveAsTable`**
- Read files by path and a table by name

### Lesson flow

Save modes (**`overwrite`**, **`append`**, **`ignore`**, **`errorifexists`**);
partitioned write by **`hour_of_day`**; Delta **file** write and read by path;
managed **`saveAsTable`** to **`rideshare_dev.processed.trip_time_preview`**
(files go to the catalog managed location, not the external volume); files
vs tables; Module 6 `01 - Column Transforms with Built-in Functions.py` reads
this table alongside landing **`trip_time`** Parquet; deep Delta → Module 10.

### Expected state

- Input: `/Volumes/rideshare_dev/landing/source_files/trip_time/`
- Expected rows: 100
- Output: `practice/write_modes_demo/`, `practice/trip_time_partitioned/`, and
  `practice/trip_time_delta_file/` under
  `/Volumes/rideshare_dev/processed/output_files/`; managed table
  **`rideshare_dev.processed.trip_time_preview`**

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
managed tables including Module 7/8 `saveAsTable` outputs). In Level 4 the
ADLS folder delete can fail with `LOCATION_OVERLAP` because the catalog
managed storage sits inside it; the notebook then prints a manual Azure
Portal delete step.

### Expected state

Not applicable — no persistent data state this notebook is required to leave
behind. It removes objects created by this module and later writes.

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
