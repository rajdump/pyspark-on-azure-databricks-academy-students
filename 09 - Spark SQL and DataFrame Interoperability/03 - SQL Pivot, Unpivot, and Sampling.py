# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - SQL Pivot, Unpivot, and Sampling
# MAGIC
# MAGIC SQL `PIVOT` / `UNPIVOT` and a brief `TABLESAMPLE` contrast.
# MAGIC
# MAGIC `trip_enriched`.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Reshape with SQL `PIVOT` / `UNPIVOT` and contrast SQL `TABLESAMPLE` with
# MAGIC   seeded DataFrame sampling
# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — load `trip_enriched`
# MAGIC
# MAGIC We'll use `trip_enriched` throughout the notebook.
# MAGIC
# MAGIC The main columns are:
# MAGIC
# MAGIC - `pickup_borough` — the row grouping
# MAGIC - `service_type` — the values we'll pivot into columns
# MAGIC - `trip_id` — used to count trips
# MAGIC - `payment_method` — used later in the exercise
# MAGIC
# MAGIC The table contains **106 trips**.
# MAGIC
# MAGIC Module 8 `04 - Pivot` introduced the DataFrame `pivot` operation. Here we
# MAGIC use the SQL forms, including `UNPIVOT`.

# COMMAND ----------

trip_enriched = spark.table("rideshare_dev.processed.trip_enriched")  # noqa: F821

print(f"trip_enriched: {trip_enriched.count()} rows")  # expect 106

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Borough × service counts
# MAGIC
# MAGIC `GROUP BY pickup_borough, service_type` returns a row only when that
# MAGIC pair has trips. A missing pair does not appear at all.
# MAGIC
# MAGIC Example shape (toy — not the full result):
# MAGIC
# MAGIC | pickup_borough | service_type | trip_count |
# MAGIC |---|---|---:|
# MAGIC | Bronx | STANDARD | 3 |
# MAGIC | Bronx | XL | 1 |
# MAGIC
# MAGIC There is no Bronx / PREMIUM row — that pair is **absent**, not NULL.
# MAGIC
# MAGIC **Expected:** **18 rows** from the real query below.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   pickup_borough,
# MAGIC   service_type,
# MAGIC   COUNT(*) AS trip_count
# MAGIC FROM rideshare_dev.processed.trip_enriched
# MAGIC GROUP BY pickup_borough, service_type
# MAGIC ORDER BY pickup_borough, service_type

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. PIVOT service types into columns
# MAGIC
# MAGIC `PIVOT` makes one row per `pickup_borough`. The listed `service_type`
# MAGIC values become columns.
# MAGIC
# MAGIC Example shape (toy):
# MAGIC
# MAGIC | pickup_borough | STANDARD | PREMIUM | XL |
# MAGIC |---|---:|---:|---:|
# MAGIC | Bronx | 3 | NULL | 1 |
# MAGIC
# MAGIC PREMIUM is **NULL** because the column is required even when Bronx has
# MAGIC no PREMIUM trips — still not a zero.
# MAGIC
# MAGIC **Expected:** **5 borough rows** × **5 service columns**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC   SELECT pickup_borough, service_type, trip_id
# MAGIC   FROM rideshare_dev.processed.trip_enriched
# MAGIC )
# MAGIC PIVOT (
# MAGIC   COUNT(trip_id)
# MAGIC   FOR service_type IN (
# MAGIC     'STANDARD',
# MAGIC     'PREMIUM',
# MAGIC     'XL',
# MAGIC     'SHARED',
# MAGIC     'UNKNOWN'
# MAGIC   )
# MAGIC )
# MAGIC ORDER BY pickup_borough

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. COALESCE zeros + SQL temp view
# MAGIC
# MAGIC Turn those NULL cells into `0`, then store the result as
# MAGIC `borough_service_wide` so later `%sql` cells can query it by name.
# MAGIC
# MAGIC Example shape (toy):
# MAGIC
# MAGIC | pickup_borough | STANDARD | PREMIUM | XL |
# MAGIC |---|---:|---:|---:|
# MAGIC | Bronx | 3 | 0 | 1 |
# MAGIC
# MAGIC Same `COALESCE` idea as `02 - SQL Joins, Aggregations, and Filtering`,
# MAGIC now on pivot columns.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW borough_service_wide AS
# MAGIC SELECT
# MAGIC   pickup_borough,
# MAGIC   COALESCE(STANDARD, 0) AS STANDARD,
# MAGIC   COALESCE(PREMIUM, 0) AS PREMIUM,
# MAGIC   COALESCE(XL, 0) AS XL,
# MAGIC   COALESCE(SHARED, 0) AS SHARED,
# MAGIC   COALESCE(UNKNOWN, 0) AS UNKNOWN
# MAGIC FROM (
# MAGIC   SELECT *
# MAGIC   FROM (
# MAGIC     SELECT pickup_borough, service_type, trip_id
# MAGIC     FROM rideshare_dev.processed.trip_enriched
# MAGIC   )
# MAGIC   PIVOT (
# MAGIC     COUNT(trip_id)
# MAGIC     FOR service_type IN (
# MAGIC       'STANDARD',
# MAGIC       'PREMIUM',
# MAGIC       'XL',
# MAGIC       'SHARED',
# MAGIC       'UNKNOWN'
# MAGIC     )
# MAGIC   )
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM borough_service_wide
# MAGIC ORDER BY pickup_borough

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. UNPIVOT columns back to rows
# MAGIC
# MAGIC `UNPIVOT` turns the service columns back into `service_type` rows.
# MAGIC
# MAGIC Example shape (toy):
# MAGIC
# MAGIC | pickup_borough | service_type | trip_count |
# MAGIC |---|---|---:|
# MAGIC | Bronx | STANDARD | 3 |
# MAGIC | Bronx | PREMIUM | 0 |
# MAGIC | Bronx | XL | 1 |
# MAGIC
# MAGIC Step 1 omitted missing pairs (**18** rows). After `COALESCE` + `UNPIVOT`,
# MAGIC zero pairs are explicit rows (**25** = 5 boroughs × 5 services).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM borough_service_wide
# MAGIC UNPIVOT (
# MAGIC   trip_count FOR service_type IN (
# MAGIC     STANDARD,
# MAGIC     PREMIUM,
# MAGIC     XL,
# MAGIC     SHARED,
# MAGIC     UNKNOWN
# MAGIC   )
# MAGIC )
# MAGIC ORDER BY pickup_borough, service_type

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. TABLESAMPLE
# MAGIC
# MAGIC `TABLESAMPLE (25 PERCENT)` requests about **25%** of `trip_enriched`.
# MAGIC The sample is not seeded here, so re-runs can differ. The percentage is
# MAGIC approximate, not an exact row count.
# MAGIC
# MAGIC Module 8 `07 - Top-N per Group and Sampling` used seeded `.sample()`
# MAGIC when you need the same draw again.
# MAGIC
# MAGIC **Expected:** roughly **25–30 rows** from this 106-row table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM rideshare_dev.processed.trip_enriched TABLESAMPLE (25 PERCENT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC Repeat the same reshape using `payment_method`.
# MAGIC
# MAGIC ### Q1 — Pivot payment methods to columns
# MAGIC
# MAGIC One row per `pickup_borough`. Pivot these values into columns:
# MAGIC
# MAGIC - `card`, `cash`, `wallet`, `corporate`, `unknown`
# MAGIC
# MAGIC Count with `trip_id`.
# MAGIC
# MAGIC **Expected:** **5 borough rows × 5 payment-method columns**.
# MAGIC
# MAGIC ### Q2 — Replace NULLs and unpivot
# MAGIC
# MAGIC `COALESCE` NULL cells to `0`, then `UNPIVOT` back to
# MAGIC `pickup_borough` + `payment_method` rows (zeros included).
# MAGIC
# MAGIC Trip 106 has a NULL `payment_method`. NULL is not in the explicit
# MAGIC `PIVOT ... IN (...)` list, so it does not become its own column.
# MAGIC
# MAGIC **Expected:** explicit zero rows included after unpivot.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q1: PIVOT payment_method by pickup_borough (expect 5 borough rows × 5 methods)
# MAGIC -- TODO: wrap a subquery in PIVOT (COUNT(trip_id) FOR payment_method IN (...))
# MAGIC SELECT pickup_borough
# MAGIC FROM rideshare_dev.processed.trip_enriched
# MAGIC WHERE 1 = 0

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q2: COALESCE zeros, then UNPIVOT (expect explicit 0 rows)
# MAGIC -- Hint: UNPIVOT a subquery that applies COALESCE, or create a temp view
# MAGIC -- then UNPIVOT in the next cell (not both in one %sql cell)
# MAGIC SELECT pickup_borough
# MAGIC FROM rideshare_dev.processed.trip_enriched
# MAGIC WHERE 1 = 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC `GROUP BY` → `PIVOT` → `COALESCE` → `UNPIVOT`
# MAGIC
# MAGIC | Step | Missing pair |
# MAGIC |---|---|
# MAGIC | `GROUP BY` | absent row |
# MAGIC | `PIVOT` | NULL cell |
# MAGIC | `COALESCE` | 0 |
# MAGIC | `UNPIVOT` | row with 0 |
# MAGIC
# MAGIC A SQL temp view can hold the pivoted result for later `%sql` cells.
# MAGIC `TABLESAMPLE` is approximate; use seeded DataFrame `.sample()` when
# MAGIC reproducibility matters.
# MAGIC
# MAGIC **Next:** `04 - SQL Windows and QUALIFY` covers ranking, running totals,
# MAGIC and `LAG`.
