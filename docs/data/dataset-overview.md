# Dataset Overview — Rideshare

Canonical contracts for the shared rideshare dataset: schemas, join keys, row
counts, Volume paths, and Unity Catalog object names.

Explainer: [`dataset-guide.md`](dataset-guide.md). Lesson design and
privileges: each module `README.md`.
  
## Core data model

Four core tables. Modules 1–4 build small in-memory DataFrames on these
schemas and do not read files. File I/O starts in Module 5. Curated and
managed tables later add columns and rows; they do not change the schemas
below.

| Table | Rows | Role |
|---|---:|---|
| `trip` | 100 | Central fact table |
| `trip_time` | 100 | 1:1 extension of `trip` — date and time |
| `payment` | 100 | 1:1 extension of `trip` — fare breakdown |
| `zone_lookup` | 22 | Dimension — pickup/dropoff locations |

### `trip`

```text
trip_id bigint
service_type string
pickup_location_id int
dropoff_location_id int
trip_distance_miles decimal(8,2)
request_to_pickup_mins int
ride_duration_mins int
driver_arrival_to_pickup_mins int
```

### `trip_time`

```text
trip_id bigint
trip_date date
hour_of_day int
```

Date range: **2026-03-01 – 2026-03-14** (14 distinct dates).

### `payment`

```text
trip_id bigint
payment_method string
base_fare_amount decimal(10,2)
surge_amount decimal(10,2)
tax_amount decimal(10,2)
tip_amount decimal(10,2)
discount_amount decimal(10,2)
driver_payout_amount decimal(10,2)
```

### `zone_lookup`

```text
location_id int
borough_name string
zone_name string
service_zone string
```

### Join keys

- `trip.trip_id = trip_time.trip_id`
- `trip.trip_id = payment.trip_id`
- `trip.pickup_location_id = zone_lookup.location_id`
- `trip.dropoff_location_id = zone_lookup.location_id`

Every `trip` pickup/dropoff uses `location_id` **1–20** only.
`zone_lookup` rows **21** (`Newark Airport`) and **22** (`Hoboken Terminal`)
are unmatched on purpose. Both have `borough_name` = `New Jersey`.

Driver assignments are 1:N after explode — see
[Supplementary: `drivers`](#supplementary-drivers-nested-xml).

## Supplementary: `drivers` (nested XML)

12 `<driver>` records. Not a fifth core table. Each `trips_assigned` element
equals a `trip.trip_id`.

```text
driver_id       string, e.g. D001
name            string
license_number  string
vehicle         struct — make, model, year, body_type
trips_assigned  repeated trip_id list
```

## Module pipeline

**5** lands files → **6** curated Parquet → **7** managed Delta → **8** KPI
tables → **9** Spark SQL reread (no durable writes) → **10** isolated Delta
labs → **11** isolated Delta labs (does not mutate pipeline tables).

`…/` in the tables below expands from these Volume roots (also
[Path patterns](#path-patterns)):

```text
/Volumes/rideshare_dev/landing/source_files/{dataset}/
/Volumes/rideshare_dev/processed/output_files/practice/{output_name}/
/Volumes/rideshare_dev/processed/output_files/curated/{output_name}/
```

Notebooks use `/Volumes/...` paths, not hardcoded `abfss://` URLs. Module 5
notebooks **01** / **99** may build an `abfss://` root from config for
external-location / managed-location DDL and ADLS teardown only.

### Module 5 — Reading, Writing, and Schemas

**Reads:** repo `data/raw`. **Writes:** landing Volume; managed
`trip_time_preview`; practice files under `…/practice/` (teaching only —
Modules 6–9 do not read them).
[Module 5 README](../../05%20-%20Reading%2C%20Writing%2C%20and%20Schemas/README.md).

Notebook **01** copies one folder per dataset and creates the catalog,
schemas, and volumes in [UC objects](#uc-objects).

#### Source files

| Dataset | Format | Repo source (Git) | Volume destination |
|---|---|---|---|
| `trip` | CSV | `data/raw/csv/trip.csv` | `…/landing/source_files/trip/` |
| `trip_time` | Parquet | `data/raw/parquet/trip_time.parquet` | `…/landing/source_files/trip_time/` |
| `zone_lookup` | JSON Lines | `data/raw/json/zone_lookup.json` | `…/landing/source_files/zone_lookup/` |
| `drivers` | XML | `data/raw/xml/drivers.xml` | `…/landing/source_files/drivers/` |
| `payment` | Avro | `data/raw/avro/payment.avro` | `…/landing/source_files/payment/` |

Canonical `zone_lookup` JSON is newline-delimited. Extra CSV, JSON, and
Parquet copies of the four core tables exist under `data/raw/` for
authoring; Module 5 lands **one** format per dataset (table above).
`drivers` is XML only.

`rideshare_dev.processed.trip_time_preview` — same schema and grain as
`trip_time` (**100**). Notebook **07** writes it. Module 6 notebook **01**
reads it next to landing `trip_time`.

#### Controlled-bad variants

Notebook **01** also lands two full-size bad CSVs. Module 6 notebook **03**
uses them as its only trip and payment inputs. They do **not** replace the
100-row core contracts used by other source-reading notebooks.

| Purpose | Repo source (Git) | Volume destination | Source rows |
|---|---|---|---:|
| Trip cleaning | `data/raw/csv/bad_trip_data.csv` | `…/landing/source_files/trip/bad_trip_data.csv` | 108 |
| Payment cleaning | `data/raw/csv/bad_payment_data.csv` | `…/landing/source_files/payment/bad_payment_data.csv` | 106 |

Both files keep the CSV header and all 100 original records.

- **`bad_trip_data`:** appends trips 101–106, one duplicate of trip 101, and
  one missing-key row.
- **`bad_payment_data`:** appends five uniquely keyed rows (101–105) plus one
  missing-key row (no payment for trip 106).

### Module 6 — Built-in Functions, Complex Types, and UDF Alternatives

**Reads:** landing (including the controlled-bad CSVs in notebook **03**);
`trip_time_preview` in notebook **01** only. **Writes:** curated Parquet
under `…/processed/output_files/curated/{name}/`.
[Module 6 README](../../06%20-%20Built-in%20Functions%2C%20Complex%20Types%2C%20and%20UDF%20Alternatives/README.md).

There is **no** curated `trip_time` or `zone_lookup`; Module 7 still reads
those from landing.

| Output | Grain / rows |
|---|---|
| `curated/drivers_flat/` | One row per (`driver_id`, `trip_id`); trips **1–100** |
| `curated/trip/` | One row per `trip_id` — **106** (from `bad_trip_data.csv`) |
| `curated/payment/` | One row per `trip_id` — **105** (from `bad_payment_data.csv`; no row for trip 106) |

#### `curated/trip` schema

```text
trip_id bigint
service_type string
service_label string
pickup_location_id int
dropoff_location_id int
trip_distance_miles decimal(8,2)
trip_distance_km double
request_to_pickup_mins int
driver_arrival_to_pickup_mins int
request_to_driver_arrival_mins int
ride_duration_mins int
diff_ride_duration_wait_mins int
ride_duration_band string
```

#### `curated/payment` schema

Parent: **`payment`**. All 8 inherited columns are unchanged and come first,
in `payment` order, followed by:

```text
charge_before_tip decimal(16,2)
tip_percent_of_base decimal(16,1)
```

#### `drivers_flat` schema

```text
driver_id string
driver_name string
license_number string
vehicle_make string
vehicle_model string
vehicle_year long
vehicle_body_type string
trip_id bigint
```

### Module 7 — Joins and Set Operations

**Reads:** curated `trip`, `payment`, `drivers_flat`; landing `trip_time`,
`zone_lookup`. **Writes:** managed Delta in `rideshare_dev.processed`
(`saveAsTable`).
[Module 7 README](../../07%20-%20Joins%20and%20Set%20Operations/README.md).

| Table | Grain / rows | Columns |
|---|---|---:|
| `rideshare_dev.processed.trip_enriched` | One row per curated `trip_id` — **106** | 16 |
| `rideshare_dev.processed.trip_driver_assignment` | One row per (`driver_id`, `trip_id`) — **100** (trips 101–106 have no assignment) | 13 |

#### `trip_enriched`

```text
trip_id bigint
service_type string
pickup_location_id int
dropoff_location_id int
trip_distance_miles decimal(8,2)
ride_duration_mins int
trip_date date
hour_of_day int
payment_method string
base_fare_amount decimal(10,2)
tip_amount decimal(10,2)
driver_payout_amount decimal(10,2)
pickup_borough string
pickup_zone string
dropoff_borough string
dropoff_zone string
```

**Group-key casing** (from Module 6; inherited here): `service_type` is
**uppercase** (`STANDARD`, `SHARED`, `PREMIUM`, `XL`, `UNKNOWN`).
`payment_method` is **lowercase** (`card`, `wallet`, `cash`, `corporate`,
`unknown`, plus **1 NULL** for trip 106). `UNKNOWN` / `unknown` are string
sentinels, **not** NULL.

**Inherited NULLs.**

| Column(s) | NULL on `trip_id` | Rows | Cause |
|---|---|---:|---|
| `trip_date`, `hour_of_day` | 101–106 | 6 | `trip_time` has only 100 rows — left join |
| `payment_method`, `driver_payout_amount` | 106 | 1 | `curated/payment` has 105 rows — left join |
| `base_fare_amount` | 104, 106 | 2 | Left join **plus** trip 104 negative fare rejected in Module 6 |
| `tip_amount` | 103, 106 | 2 | Left join **plus** trip 103 `not_a_number` tip rejected in Module 6 |
| `trip_distance_miles` | 103, 105, 106 | 3 | Module 6 positive-value rule |

`ride_duration_mins`, `service_type`, and the four zone columns have **no**
NULLs.

#### `trip_driver_assignment`

Parent: **`drivers_flat`**. All 8 inherited columns are unchanged and come
first, in `drivers_flat` order, followed by:

```text
service_type string
trip_distance_miles decimal(8,2)
ride_duration_mins int
pickup_location_id int
dropoff_location_id int
```

**NULLs:** None.

### Module 8 — Aggregations and Window Functions

**Reads:** `trip_enriched`, `trip_driver_assignment`. **Writes:** three
managed `kpi_*` tables (`saveAsTable`).
[Module 8 README](../../08%20-%20Aggregations%20and%20Window%20Functions/README.md)
(column formulas: [Shared paths and assets](../../08%20-%20Aggregations%20and%20Window%20Functions/README.md#shared-paths-and-assets)).

| Table | Grain / rows | Source table |
|---|---|---|
| `rideshare_dev.processed.kpi_daily_trip_summary` | One row per **`trip_date`** — **14** (drops NULL-`trip_date` trips 101–106) | `trip_enriched` |
| `rideshare_dev.processed.kpi_zone_performance` | One row per (**`pickup_borough`**, **`pickup_zone`**) — **20** | `trip_enriched` |
| `rideshare_dev.processed.kpi_driver_productivity` | One row per **`driver_id`** — **12** | `trip_driver_assignment` |

### Module 9 — Spark SQL and DataFrame Interoperability

**Reads:** `trip_enriched`, `trip_driver_assignment`,
`kpi_daily_trip_summary`, `kpi_zone_performance`. **Writes:** none
(session temp views only).
[Module 9 README](../../09%20-%20Spark%20SQL%20and%20DataFrame%20Interoperability/README.md).

Notebook **06** rebuilds the three Module 8 KPI contracts in Spark SQL from
the Module 7 tables — read-only.

### Module 10 — Delta Lake Foundations

**Reads / mutates:** none of `trip_enriched`, the KPI tables, or `curated/`.
**Writes:** isolated lab folders and tables below.
[Module 10 README](../../10%20-%20Delta%20Lake%20Foundations/README.md)
(extract, DDL, and cleanup).

`{url}` is defined in [UC objects](#uc-objects).

| Object | Location |
|---|---|
| `fare_correction_parquet/` | `/Volumes/rideshare_dev/processed/output_files/practice/fare_correction_parquet/` |
| `fare_correction_delta/` | `/Volumes/rideshare_dev/processed/output_files/practice/fare_correction_delta/` |
| `fare_log_delta/` | `/Volumes/rideshare_dev/processed/output_files/practice/fare_log_delta/` |
| `rideshare_dev.processed.fare_managed_lab` | Managed (no `LOCATION`) |
| `rideshare_dev.processed.fare_external_lab` | `{url}/external-tables/fare_external_lab` — **not** a Volume path |
| `rideshare_dev.processed.fare_timetravel_lab` | Managed (no `LOCATION`) |

Notebooks **01–02** use Volume paths under `practice/` (`ls` / `.save` /
path DML). Notebook **03** `CREATE` uses that `external-tables` folder only
— never the external-location root. `DROP TABLE` on the external name
leaves those files. `DROP CATALOG CASCADE` drops the Unity Catalog names; it
does not by itself delete that ADLS folder.

### Module 11 — Delta Lake Transactions, Schema, and Maintenance

**Reads / mutates:** none of `trip_enriched`, the KPI tables, or `curated/`.
**Writes:** isolated lab tables and the lab Parquet copy below.
[Module 11 README](../../11%20-%20Delta%20Lake%20Transactions%2C%20Schema%2C%20and%20Maintenance/README.md)
(lab rows, DDL, mutations, copy, and cleanup).

`{url}` is defined in [UC objects](#uc-objects).

| Object | Location |
|---|---|
| `data/lab/fare_dv_lab.parquet` | Author workspace file (~300 MB). Gitignored — not on GitHub. Not in Module 5 landing. |
| `rideshare_dev.processed.fare_dv_lab` | `{url}/external-tables/fare_dv_lab` — notebook **00** copies then `CONVERT TO DELTA`; notebook **01** registers the UC name — **not** a Volume path |
| `rideshare_dev.processed.fare_maint_lab` | `{url}/external-tables/fare_maint_lab` — notebooks **02–04** — **not** a Volume path |

`DROP TABLE` leaves `external-tables/` files. Module 5 `99` Level 1 does not
clear `external-tables/` or `data/lab/` (same as Module 10 notebook 03).

## Unity Catalog platform reference

### UC objects

`{url}` is the `url` column from
`DESCRIBE EXTERNAL LOCATION el_rideshare_dev` (strip a trailing slash).

| Platform piece | Value |
|---|---|
| Catalog | `rideshare_dev` (`MANAGED LOCATION` `{url}/uc-managed`) |
| Schemas | `landing`, `processed` |
| Volumes (both external) | `landing.source_files` at `{url}/landing`; `processed.output_files` at `{url}/processed` |
| External location | `el_rideshare_dev` |
| Storage credential | Student-provided name in the config cell |

`landing` and `processed` are Unity Catalog schemas — **not** medallion
Bronze/Silver/Gold.

A Unity Catalog table `LOCATION` must be a cloud URL **outside** Volume
storage. `/Volumes/...` and `{url}/processed/...` are the `output_files`
Volume (file `ls` and DataFrameWriter `.save` only). Do not `CREATE TABLE`
at a Volume path.

### Managed tables

Pipeline teaching tables — Unity Catalog managed Delta in
`rideshare_dev.processed` (`landing` has none). Grain and schemas are in the
pipeline sections above. Module 10 and 11 lab tables are listed under
those module headings, not here.

| Table | Module |
|---|---|
| `rideshare_dev.processed.trip_time_preview` | 5 |
| `rideshare_dev.processed.trip_enriched` | 7 |
| `rideshare_dev.processed.trip_driver_assignment` | 7 |
| `rideshare_dev.processed.kpi_daily_trip_summary` | 8 |
| `rideshare_dev.processed.kpi_zone_performance` | 8 |
| `rideshare_dev.processed.kpi_driver_productivity` | 8 |

### Path patterns

`practice/` and `curated/` are directories inside `output_files`, created on
first write.

| Kind | Destination |
|---|---|
| Landing files | `…/landing/source_files/{dataset}/` |
| Practice files (Module 5 teaching writes; Module 10 notebooks 01–02) | `…/practice/{output_name}/` |
| Curated Parquet (Module 6 writes; Module 7 reads) | `…/curated/{output_name}/` |
| Pipeline managed tables | `rideshare_dev.processed` — [Managed tables](#managed-tables) |
| External table `LOCATION` (Module 10 notebook 03; Module 11 notebooks 01–04) | `{url}/external-tables/…` |
| Lab Parquet (Module 11 notebook 00) | workspace `data/lab/` (gitignored) → `{url}/external-tables/…` — [Module 11](#module-11--delta-lake-transactions-schema-and-maintenance) |

## Does not cover

- Why the model looks this way, and why row counts / NULLs change —
  [`dataset-guide.md`](dataset-guide.md)
- KPI column formulas — Module 8 README (Shared paths and assets)
- Module 10 extract rows and lab DDL — Module 10 README
- Module 11 extract and **02–04** mutations — Module 11 README; **00–01**
  DML is in those notebooks
- Privileges — each module README
- Medallion `bronze` / `silver` / `gold` — Modules 13–14
