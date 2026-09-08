# Module 8 — Aggregations and Window Functions

## Purpose

Produce analytics-ready summaries and KPI tables from the Module 7 managed
tables.

## Learning objectives

By the end of this module, you'll be able to:

- Name and verify the **output grain** of grouped and windowed calculations
- Build aliased aggregates with single or composite keys, and reason about
  NULL keys, NULL values, and count semantics
- Choose whether to filter input rows (`WHERE`) or aggregated groups (`HAVING`)
- Use advanced aggregates and pivoting to summarize and reshape data
- Build windows for ranking, running calculations, and row-to-row comparisons
- Control NULL sort placement with **`nullsFirst` / `nullsLast`**
- Select **Top-N per group** (including tie-selection policy) and draw
  reproducible samples with **`sample`** / **`sampleBy`** / **`randomSplit`**
- Apply the module patterns in `08 - Build KPI Tables.py` to write three
  managed `kpi_*` tables for Module 9

## Prerequisites

Complete Module 7 notebooks **`01`–`07`**. You need:

| Asset | Rows / notes | Source |
|---|---|---|
| `rideshare_dev.processed.trip_enriched` | 106 — one per `trip_id`; 16 columns | Module 7 `07 - Build Unified Curated Tables.py` |
| `rideshare_dev.processed.trip_driver_assignment` | 100 — one per (`driver_id`, `trip_id`); 12 drivers, trips 1–100; 13 columns | Module 7 `07 - Build Unified Curated Tables.py` |

`trip_enriched` is the primary source for Notebooks **01–07**.
`trip_driver_assignment` appears where a **1:M** grain (many trips per driver)
makes a point that trip grain cannot. `01 - GroupBy and Basic Aggregations.py`
owns the shared setup description; later notebooks load without re-describing
it.

Also recall: Module 3 NULL / `F.coalesce`; Module 4 wide/`Exchange` stages;
Module 7 `02 - Silent Join Failures and Validation.py` (`Window` +
`row_number` dedup — revisited in **05**; Top-N reuses the pattern in **07**).

Does **not** read `/Volumes/rideshare_dev/processed/output_files/practice/`
or Module 6 `/Volumes/rideshare_dev/processed/output_files/curated/` — the
managed tables already carry what this module needs.

## Dataset

Schemas, inherited NULLs, and group-key values:
[`docs/data/dataset-overview.md`](../docs/data/dataset-overview.md).
KPI column formulas: see Shared paths and assets below.

`08 - Build KPI Tables.py` writes Unity Catalog managed Delta tables with
`.mode("overwrite").saveAsTable(...)`. Module 9
`04 - SQL Windows and QUALIFY.py` reads the daily and zone tables. Module 9
`06 - End-to-End SQL Pipeline.py` rebuilds all three KPI contracts in Spark
SQL from the source tables (read-only).

| Table | Grain / rows | Source |
|---|---|---|
| `rideshare_dev.processed.kpi_daily_trip_summary` | One row per **`trip_date`** — **14**. Explicitly drops NULL-`trip_date` trips **101–106** (that filter also removes all measure-NULL rows; remaining trips 1–100 are fully populated) | `trip_enriched` |
| `rideshare_dev.processed.kpi_zone_performance` | One row per (**`pickup_borough`**, **`pickup_zone`**) — **20**. All 106 rows; primary NULL-aggregate surface | `trip_enriched` |
| `rideshare_dev.processed.kpi_driver_productivity` | One row per **`driver_id`** — **12**. Includes fleet-wide `distance_dense_rank` after aggregate | `trip_driver_assignment` |

**Cleanup:** Module 5 `99 - Rideshare Project Cleanup and Reset.py` Level 4
(catalog teardown) drops these managed tables with the rest of
`rideshare_dev` — same as Module 7. Level 2 clears Module 6
`/Volumes/rideshare_dev/processed/output_files/curated/` Parquet only and
does **not** remove KPI tables.

## Shared paths and assets

Module-wide reused KPI formulas (consumed by notebook **08** and by Module 9).

### `kpi_daily_trip_summary` columns

| Column | Formula |
|---|---|
| `trip_date` | key |
| `trip_count` | `count("*")` |
| `total_base_fare` | `sum(base_fare_amount)` |
| `total_tip` | `sum(tip_amount)` |
| `total_driver_payout` | `sum(driver_payout_amount)` |
| `total_distance_miles` | `sum(trip_distance_miles)` — Module 9 running-total candidate |
| `avg_distance_miles` | `round(avg(trip_distance_miles), 2)` |
| `avg_ride_duration_mins` | `round(avg(ride_duration_mins), 2)` |

### `kpi_zone_performance` columns

| Column | Formula |
|---|---|
| `pickup_borough`, `pickup_zone` | composite key |
| `pickup_location_id` | `max(pickup_location_id)` — deterministic per zone |
| `trip_count` | `count("*")` |
| `total_base_fare`, `total_tip` | sums (NULL-skipping) |
| `tip_percent_of_base` | `when(sum(base_fare_amount) > 0, round(100 * sum(tip) / sum(base), 1)).otherwise(NULL)` — not avg of row percents |
| `avg_distance_miles`, `avg_ride_duration_mins` | rounded avgs |

NULL-affected pickup zones (contract): Financial District (104 base), Harlem
(106 base/tip/distance — densest), Astoria (103 tip/distance), Williamsburg
(105 distance only).

### `kpi_driver_productivity` columns

| Column | Formula |
|---|---|
| `driver_id` | key |
| `driver_name` | `max(driver_name)` |
| `trip_count` | `count("*")` |
| `total_distance_miles` | `sum(trip_distance_miles)` |
| `avg_ride_duration_mins` | `round(avg(ride_duration_mins), 2)` |
| `unique_service_types` | `sort_array(collect_set(service_type))` |
| `distance_dense_rank` | after agg: `dense_rank` over fleet by `total_distance_miles` desc |

## Notebook 01 — GroupBy and Basic Aggregations

### Context

Output grain and basic `groupBy().agg()` — no write.

### Learning objectives

- Name and verify the output grain of grouped calculations
- Build aliased aggregates and reason about NULL values and count semantics

### Lesson flow

Output grain; `groupBy().agg()` + aliasing; bare non-key column fails
(window → **05**); three counts; `sum`/`avg` skip NULLs + `F.coalesce`;
exercise — per-`payment_method`.

### Expected state

- Input: `trip_enriched`
- Output: none (no write)

### Exercise

Per-`payment_method` aggregation.

### Next

`02 - Multi-column Keys, NULL Groups, and Filter Placement`

## Notebook 02 — Multi-column Keys, NULL Groups, and Filter Placement

### Context

NULL key groups vs `countDistinct`, and `WHERE` vs `HAVING`.

### Learning objectives

- Build aliased aggregates with composite keys
- Reason about NULL keys vs `countDistinct`
- Choose whether to filter input rows (`WHERE`) or aggregated groups
  (`HAVING`)

### Lesson flow

NULL key group vs `countDistinct`; composite grain; `WHERE` vs `HAVING`;
exercise — borough + HAVING, then composite key.

### Expected state

- Input: `trip_enriched`
- Output: none (no write)

### Exercise

Borough + HAVING, then composite key.

### Next

`03 - Collections, Percentiles, and Distinct Counts`

## Notebook 03 — Collections, Percentiles, and Distinct Counts

### Context

Collection aggregates, percentiles, and distinct counts.

### Learning objectives

- Use advanced aggregates (`collect_list` / `collect_set`, approximate
  percentiles, `countDistinct`)

### Lesson flow

`collect_list` / `collect_set`; `avg` vs approximate p50 / p90;
`countDistinct`.

### Expected state

- Input: `trip_enriched`, `trip_driver_assignment`
- Output: none (no write)

### Exercise

Short hands-on on the demonstrated aggregate pattern.

### Next

`04 - Pivot`

## Notebook 04 — Pivot

### Context

Reshape grouped results with `pivot`.

### Learning objectives

- Use pivoting to summarize and reshape data

### Lesson flow

`pivot` + explicit values.

### Expected state

- Input: `trip_enriched`
- Output: none (no write)

### Exercise

Short hands-on on `pivot`.

### Next

`05 - Window Functions Fundamentals`

## Notebook 05 — Window Functions Fundamentals

### Context

Windows preserve input rows — contrast with `groupBy`.

### Learning objectives

- Build windows for ranking and partition-only aggregates
- Preview Top-2 filter-after-rank (full Top-N → **07**)

### Lesson flow

`groupBy` vs `Window`; partition-only aggregates; ranking-API ties; Top-2
filter-after-rank preview → **07**.

### Expected state

- Input: `trip_enriched`, `trip_driver_assignment`
- Output: none (no write)

### Exercise

Short hands-on on window ranking.

### Next

`06 - Running Totals and Lag and Lead`

## Notebook 06 — Running Totals and Lag and Lead

### Context

Ordered frames: running totals and `lag` / `lead`. May first aggregate to
daily grain, then window over that.

### Learning objectives

- Build windows for running calculations and row-to-row comparisons

### Lesson flow

Default `RANGE` vs explicit `ROWS`; ordered `first_value` / `last_value`;
daily running totals; `lag` / `lead`.

### Expected state

- Input: `trip_enriched`
- Output: none (no write)

### Exercise

Short hands-on on running totals or `lag` / `lead`.

### Next

`07 - Top-N per Group and Sampling`

## Notebook 07 — Top-N per Group and Sampling

### Context

Top-N per group, NULL sort placement, and sampling.

### Learning objectives

- Control NULL sort placement with `nullsFirst` / `nullsLast`
- Select Top-N per group (including tie-selection policy) and draw
  reproducible samples with `sample` / `sampleBy` / `randomSplit`

### Lesson flow

Top-N per group (`row_number` + filter; extends **05** Top-2); Top-N
selection policy (`row_number <= N` vs `rank <= N`, secondary sort);
`nullsFirst` / `nullsLast` (standalone sort placement); `sample` /
`sampleBy` / `randomSplit`.

### Expected state

- Input: `trip_enriched`, `trip_driver_assignment`
- Output: none (no write)

### Exercise

Short hands-on on Top-N or sampling.

### Next

`08 - Build KPI Tables`

## Notebook 08 — Build KPI Tables

### Context

Write-only: three managed `kpi_*` Delta tables for Module 9.

### Learning objectives

- Apply the module patterns to write three managed `kpi_*` tables

### Lesson flow

Write-only: three managed `kpi_*` Delta tables (`saveAsTable`). Formulas:
see Shared paths and assets.

### Expected state

- Input: both managed tables (`trip_enriched`, `trip_driver_assignment`)
- Output: the three `kpi_*` tables at the Dataset grain/row counts.
  Formulas: Shared paths and assets.

### Exercise

An exercise does not apply — this notebook is write-only.

### Next

Module 9 — Spark SQL and DataFrame Interoperability.

## Minimum privileges required

- Unity Catalog (no catalog / external-location / volume DDL):
  - **`USE CATALOG`** on **`rideshare_dev`**
  - **`USE SCHEMA`** on **`rideshare_dev.processed`**
  - **`SELECT`** on **`rideshare_dev.processed.trip_enriched`** and
    **`rideshare_dev.processed.trip_driver_assignment`**
  - **`CREATE TABLE`** on **`rideshare_dev.processed`**
    (`08 - Build KPI Tables.py` only)
- Workspace: **`CAN ATTACH TO`** (or **`CAN RESTART`**) on the compute used here
