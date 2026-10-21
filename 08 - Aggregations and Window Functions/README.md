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
- Apply the module patterns in `09 - Build KPI Tables.py` to write three
  managed `kpi_*` tables for Module 9

## Prerequisites

Complete Module 7 notebooks **`01`–`07`**. You need:

| Asset | Rows / notes | Source |
|---|---|---|
| `rideshare_dev.processed.trip_enriched` | 106 — one per `trip_id`; 16 columns | Module 7 `07 - Build Unified Curated Tables.py` |
| `rideshare_dev.processed.trip_driver_assignment` | 100 — one per (`driver_id`, `trip_id`); 12 drivers, trips 1–100; 13 columns | Module 7 `07 - Build Unified Curated Tables.py` |

`trip_enriched` is the primary source for Notebooks **01–08**.
`trip_driver_assignment` appears where a **1:M** grain (many trips per driver)
makes a point that trip grain cannot. `01 - GroupBy and Basic Aggregations.py`
loads `trip_enriched` and summarizes it by `service_type`.
`02 - NULL Handling in Aggregations.py` owns the inherited-NULL map; later
notebooks load without re-describing it.

Also recall: Module 3 NULL / `F.coalesce`; Module 4 wide/`Exchange` stages;
Module 7 `02 - Join Cardinality, NULL Keys, and Cross Joins.py` (grain and key
profiling). `Window` + `row_number` dedup is introduced in **06**; Top-N
reuses the pattern in **08**.

Does **not** read `/Volumes/rideshare_dev/processed/output_files/practice/`
or Module 6 `/Volumes/rideshare_dev/processed/output_files/curated/` — the
managed tables already carry what this module needs.

## Dataset

Schemas, inherited NULLs, and group-key values:
[`docs/data/dataset-overview.md`](../docs/data/dataset-overview.md).
KPI column formulas: see Shared paths and assets below.

`09 - Build KPI Tables.py` writes Unity Catalog managed Delta tables with
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

Module-wide reused KPI formulas (consumed by notebook **09** and by Module 9).

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

Basic `groupBy()` syntax and aggregate functions on one key — no write.

### Learning objectives

- Explain how `groupBy()` turns individual rows into grouped summaries
- Calculate multiple aggregates with `agg()` and give each a clear name
- Tell the difference between counting rows and counting distinct values

### Lesson flow

Output grain (`distinct().count()` predicts, `groupBy().count()` confirms);
`groupBy().agg()` with multiple aggregates + aliasing; `countDistinct` on a
non-NULL column; bare non-key column is not allowed (window → **06**).

### Expected state

- Input: `trip_enriched`
- Output: none (no write)

### Next

`02 - NULL Handling in Aggregations`

## Notebook 02 — NULL Handling in Aggregations

### Context

How NULL values and NULL keys change aggregate results — no write.

### Learning objectives

- Compare `count("*")`, `count(column)`, and `countDistinct(column)` when
  NULL values are present
- Explain why `groupBy()` treats NULL as its own group, while
  `countDistinct()` ignores NULL
- Explain why `sum()`, `avg()`, `min()`, and `max()` skip NULLs, and why
  they return NULL when every value in a group is NULL
- Identify when to replace NULL with zero before aggregation, and how that
  changes the average

### Lesson flow

Inherited NULLs in `trip_enriched`; three counts (`count("*")`,
`count(column)`, `countDistinct`); NULL key group vs `countDistinct` on
`payment_method`; `sum` / `avg` / `min` / `max` skip NULLs; `avg` 104 vs 106
denominator; `F.coalesce` when NULL means zero; all-NULL group returns NULL.

### Expected state

- Input: `trip_enriched`
- Output: none (no write)

### Next

`03 - Filter Placement with WHERE and HAVING`

## Notebook 03 — Filter Placement with WHERE and HAVING

### Context

Filter rows before aggregation or groups after it — no write.

### Learning objectives

- Filter individual rows before aggregation using `.filter()`, equivalent to
  SQL `WHERE`
- Filter aggregated groups using `.filter()` after `agg()`, equivalent to SQL
  `HAVING`
- Decide whether a condition belongs before or after aggregation

### Lesson flow

Filter before aggregation (`WHERE`) on borough tips over $5; filter after
aggregation (`HAVING`) on borough total tips over $90; `WHERE` vs `HAVING`
comparison; `.where()` is an alias for `.filter()`.

### Expected state

- Input: `trip_enriched`
- Output: none (no write)

### Next

`04 - Collections and Distinct Counts`

## Notebook 04 — Collections and Distinct Counts

### Context

Collection aggregates and multi-column distinct counts, using a single driver
profile story — no write.

### Learning objectives

- Collect grouped values into an array, keeping duplicates, with
  `collect_list()`
- Collect distinct grouped values into an array with `collect_set()`
- Count distinct combinations of values across columns with `countDistinct()`

### Lesson flow

`collect_list` on driver service types (one value per trip, repeats retained);
`collect_set` on driver service types (each type once); `sort_array` applied to
both to produce consistent, comparable arrays; two-column `countDistinct` on
pickup-to-drop-off routes per driver.

### Expected state

- Input: `trip_driver_assignment`
- Output: none (no write)

### Next

`05 - Pivot`

## Notebook 05 — Pivot

### Context

Reshape grouped results with `pivot`.

### Learning objectives

- Use pivoting to summarize and reshape data

### Lesson flow

Multi-column grouping (one row per observed pair); `pivot` + explicit values.

### Expected state

- Input: `trip_enriched`
- Output: none (no write)

### Next

`06 - Window Functions Fundamentals`

## Notebook 06 — Window Functions Fundamentals

### Context

Windows preserve input rows — contrast with `groupBy`.

### Learning objectives

- Build windows for ranking and partition-only aggregates
- Preview Top-2 filter-after-rank (full Top-N → **08**)

### Lesson flow

`groupBy` vs `Window`; partition-only aggregates; ranking-API ties; Top-2
filter-after-rank preview → **08**.

### Expected state

- Input: `trip_enriched`, `trip_driver_assignment`
- Output: none (no write)

### Next

`07 - Running Totals and Window Frames`

## Notebook 07 — Running Totals and Window Frames

### Context

Ordered frames: running totals and `first_value` / `last_value`. May first
aggregate to daily grain, then window over that.

### Learning objectives

- Build windows for running calculations and partition edge values

### Lesson flow

Default `RANGE` vs explicit `ROWS`; ordered `first_value` / `last_value`;
daily running totals.

### Expected state

- Input: `trip_enriched`
- Output: none (no write)

### Next

`08 - Lag and Lead`

## Notebook 08 — Lag and Lead

### Context

Row-to-row comparisons across an ordered window using `lag()` and `lead()`.

### Learning objectives

- Compare adjacent rows using `lag()` and `lead()`

### Lesson flow

`lag()` and `lead()` on the 14-day daily summary; day-over-day difference;
`NULL` boundary rows and row-offset vs calendar-day behavior.

### Expected state

- Input: `trip_enriched`
- Output: none (no write)

### Next

`09 - Top-N per Group and NULL Ordering`

## Notebook 09 — Top-N per Group and NULL Ordering

### Context

Top-N per group and NULL sort placement.

### Learning objectives

- Select Top-N per group (including tie-selection policy)
- Control NULL sort placement with `nullsFirst` / `nullsLast`

### Lesson flow

Top-N per group (`row_number` + filter; extends **06** Top-2); Top-N
selection policy (`row_number <= N` vs `rank <= N`, secondary sort);
`nullsFirst` / `nullsLast` (standalone sort placement).

### Expected state

- Input: `trip_enriched`, `trip_driver_assignment`
- Output: none (no write)

### Next

`10 - Sampling`

## Notebook 10 — Sampling

### Context

Draw reproducible subsets of rows with `sample`, `sampleBy`, and `randomSplit` — no write.

### Learning objectives

- Draw reproducible samples with `sample` / `sampleBy` / `randomSplit`

### Lesson flow

`sample` with approximate fraction and seed reproducibility; `sampleBy`
stratified by `service_type` (omitted keys receive fraction 0); `randomSplit`
into weighted subsets with a seed.

### Expected state

- Input: `trip_enriched`, `trip_driver_assignment`
- Output: none (no write)

### Next

`11 - Build KPI Tables`

## Notebook 11 — Build KPI Tables

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

### Next

Module 9 — Spark SQL and DataFrame Interoperability.

## Minimum privileges required

- Unity Catalog (no catalog / external-location / volume DDL):
  - **`USE CATALOG`** on **`rideshare_dev`**
  - **`USE SCHEMA`** on **`rideshare_dev.processed`**
  - **`SELECT`** on **`rideshare_dev.processed.trip_enriched`** and
    **`rideshare_dev.processed.trip_driver_assignment`**
  - **`CREATE TABLE`** on **`rideshare_dev.processed`**
    (`09 - Build KPI Tables.py` only)
- Workspace: **`CAN ATTACH TO`** (or **`CAN RESTART`**) on the compute used here
