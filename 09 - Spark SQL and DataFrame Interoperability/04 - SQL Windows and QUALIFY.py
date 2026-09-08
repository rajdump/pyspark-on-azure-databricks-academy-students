# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 04 - SQL Windows and QUALIFY
# MAGIC
# MAGIC Window `OVER` + `QUALIFY`, and running totals / `LAG` on daily KPI grain.
# MAGIC
# MAGIC `kpi_zone_performance`, `kpi_daily_trip_summary`.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Rank and filter with window `OVER` + `QUALIFY`
# MAGIC - Compute running totals / `LAG` on daily KPI grain
# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — load the KPI tables
# MAGIC
# MAGIC This notebook uses two KPI tables created in Module 8.
# MAGIC
# MAGIC Module 8 `05 - Window Functions Fundamentals` introduced ranking windows,
# MAGIC and `06 - Running Totals and Lag and Lead` covered running totals and `LAG`.
# MAGIC Here we apply those patterns with Spark SQL.
# MAGIC
# MAGIC The `kpi_zone_performance` table contains one row for each combination of
# MAGIC `pickup_borough` and `pickup_zone` (**20** rows). Each row includes
# MAGIC aggregated zone metrics such as `total_tip` and `trip_count`. We will
# MAGIC compare zones within the same borough and rank them from highest to
# MAGIC lowest based on these metrics.
# MAGIC
# MAGIC | pickup_borough | pickup_zone | total_tip | trip_count |
# MAGIC |---|---|---:|---:|
# MAGIC | Bronx | Zone A | 50 | 8 |
# MAGIC | Bronx | Zone B | 30 | 5 |
# MAGIC
# MAGIC The `kpi_daily_trip_summary` table contains one row for each `trip_date`
# MAGIC (**14** rows). Each row includes `total_distance_miles` for that day. We
# MAGIC will order these daily records by date to calculate the running distance,
# MAGIC bring in the previous day's distance with `LAG`, and measure the
# MAGIC day-over-day change.
# MAGIC
# MAGIC | trip_date | total_distance_miles |
# MAGIC |---|---:|
# MAGIC | day 1 | 20 |
# MAGIC | day 2 | 10 |
# MAGIC | day 3 | 15 |

# COMMAND ----------

kpi_zone = spark.table(  # noqa: F821
    "rideshare_dev.processed.kpi_zone_performance"
)
kpi_daily = spark.table(  # noqa: F821
    "rideshare_dev.processed.kpi_daily_trip_summary"
)

print(f"kpi_zone_performance: {kpi_zone.count()} rows")  # expect 20
print(f"kpi_daily_trip_summary: {kpi_daily.count()} rows")  # expect 14

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 — Rank zones within each borough
# MAGIC
# MAGIC ### 1. Rank zones by total tip
# MAGIC
# MAGIC | pickup_borough | pickup_zone | total_tip | rn |
# MAGIC |---|---|---:|---:|
# MAGIC | Bronx | Zone A | 50 | 1 |
# MAGIC | Bronx | Zone B | 30 | 2 |
# MAGIC | Bronx | Zone C | 10 | 3 |
# MAGIC
# MAGIC **Expected:** **20 rows**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   pickup_borough,
# MAGIC   pickup_zone,
# MAGIC   total_tip,
# MAGIC   ROW_NUMBER() OVER (
# MAGIC     PARTITION BY pickup_borough
# MAGIC     ORDER BY total_tip DESC
# MAGIC   ) AS rn
# MAGIC FROM rideshare_dev.processed.kpi_zone_performance
# MAGIC ORDER BY pickup_borough, rn

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Keep the Top 2 zones with `QUALIFY`
# MAGIC
# MAGIC | pickup_borough | pickup_zone | total_tip | rn |
# MAGIC |---|---|---:|---:|
# MAGIC | Bronx | Zone A | 50 | 1 |
# MAGIC | Bronx | Zone B | 30 | 2 |
# MAGIC
# MAGIC Zone C (`rn` 3) is gone — no outer subquery needed.
# MAGIC
# MAGIC **Expected:** **9 rows** (Staten Island has only one zone).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   pickup_borough,
# MAGIC   pickup_zone,
# MAGIC   total_tip,
# MAGIC   ROW_NUMBER() OVER (
# MAGIC     PARTITION BY pickup_borough
# MAGIC     ORDER BY total_tip DESC
# MAGIC   ) AS rn
# MAGIC FROM rideshare_dev.processed.kpi_zone_performance
# MAGIC QUALIFY ROW_NUMBER() OVER (
# MAGIC   PARTITION BY pickup_borough
# MAGIC   ORDER BY total_tip DESC
# MAGIC ) <= 2
# MAGIC ORDER BY pickup_borough, rn

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Same Top-N with a subquery
# MAGIC
# MAGIC | form | filter |
# MAGIC |---|---|
# MAGIC | `QUALIFY` | in the same query |
# MAGIC | subquery | `WHERE rn <= 2` outside |
# MAGIC
# MAGIC In Databricks, the `QUALIFY` clause is a way to keep Top-N results. If your
# MAGIC SQL engine doesn't support `QUALIFY`, or if you need the `rn` column for
# MAGIC later use, consider using a subquery.
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ```sql
# MAGIC WITH ranked AS (
# MAGIC   SELECT ..., ROW_NUMBER() OVER (...) AS rn
# MAGIC   FROM kpi_zone_performance
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM ranked
# MAGIC WHERE rn <= 2
# MAGIC -- later you could also: JOIN other_table ON ... using ranked
# MAGIC ```
# MAGIC
# MAGIC **Expected:** same **9 rows**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT pickup_borough, pickup_zone, total_tip, rn
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     pickup_borough,
# MAGIC     pickup_zone,
# MAGIC     total_tip,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY pickup_borough
# MAGIC       ORDER BY total_tip DESC
# MAGIC     ) AS rn
# MAGIC   FROM rideshare_dev.processed.kpi_zone_performance
# MAGIC ) ranked
# MAGIC WHERE rn <= 2
# MAGIC ORDER BY pickup_borough, rn

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 — Track distance across days
# MAGIC
# MAGIC ### 4. Running distance total
# MAGIC
# MAGIC | trip_date | total_distance_miles | running_distance |
# MAGIC |---|---:|---:|
# MAGIC | day 1 | 20 | 20 |
# MAGIC | day 2 | 10 | 30 |
# MAGIC | day 3 | 5 | 35 |
# MAGIC
# MAGIC Frame: `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`.
# MAGIC
# MAGIC **Expected:** **14 rows**; running distance
# MAGIC **21.35 → 113.34 → … → 793.20**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   trip_date,
# MAGIC   total_distance_miles,
# MAGIC   SUM(total_distance_miles) OVER (
# MAGIC     ORDER BY trip_date
# MAGIC     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC   ) AS running_distance
# MAGIC FROM rideshare_dev.processed.kpi_daily_trip_summary
# MAGIC ORDER BY trip_date

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Previous day with `LAG`
# MAGIC
# MAGIC | trip_date | total_distance_miles | prev_day | distance_change |
# MAGIC |---|---:|---:|---:|
# MAGIC | day 1 | 20 | NULL | NULL |
# MAGIC | day 2 | 10 | 20 | −10 |
# MAGIC | day 3 | 15 | 10 | +5 |
# MAGIC
# MAGIC Day 1 has no previous row, so `prev_day` and `distance_change` are NULL.
# MAGIC
# MAGIC **Expected:** `distance_change` begins with NULL, then
# MAGIC **+70.64, −61.99, +25.52, ...**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   trip_date,
# MAGIC   total_distance_miles,
# MAGIC   SUM(total_distance_miles) OVER (
# MAGIC     ORDER BY trip_date
# MAGIC     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC   ) AS running_distance,
# MAGIC   LAG(total_distance_miles, 1) OVER (ORDER BY trip_date) AS prev_day,
# MAGIC   total_distance_miles
# MAGIC     - LAG(total_distance_miles, 1) OVER (ORDER BY trip_date) AS distance_change
# MAGIC FROM rideshare_dev.processed.kpi_daily_trip_summary
# MAGIC ORDER BY trip_date

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Direction from `distance_change`
# MAGIC
# MAGIC | `distance_change` | `direction` |
# MAGIC |---|---|
# MAGIC | NULL | `n/a` |
# MAGIC | greater than 0 | `up` |
# MAGIC | less than 0 | `down` |
# MAGIC | equal to 0 | `flat` |
# MAGIC
# MAGIC Check NULL first so day 1 is `n/a`. Inner query builds
# MAGIC `distance_change`; outer query applies `CASE`.
# MAGIC
# MAGIC **Expected:** same 14 daily rows with a `direction` label.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   trip_date,
# MAGIC   total_distance_miles,
# MAGIC   running_distance,
# MAGIC   prev_day,
# MAGIC   distance_change,
# MAGIC   CASE
# MAGIC     WHEN distance_change IS NULL THEN 'n/a'
# MAGIC     WHEN distance_change > 0 THEN 'up'
# MAGIC     WHEN distance_change < 0 THEN 'down'
# MAGIC     ELSE 'flat'
# MAGIC   END AS direction
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     trip_date,
# MAGIC     total_distance_miles,
# MAGIC     SUM(total_distance_miles) OVER (
# MAGIC       ORDER BY trip_date
# MAGIC       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC     ) AS running_distance,
# MAGIC     LAG(total_distance_miles, 1) OVER (ORDER BY trip_date) AS prev_day,
# MAGIC     total_distance_miles
# MAGIC       - LAG(total_distance_miles, 1) OVER (ORDER BY trip_date) AS distance_change
# MAGIC   FROM rideshare_dev.processed.kpi_daily_trip_summary
# MAGIC ) daily_change
# MAGIC ORDER BY trip_date

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC Within each borough, which zones rank highest by **`trip_count`**?
# MAGIC Keep only zones with at least **3** trips, then keep the Top **2**.
# MAGIC
# MAGIC | clause | job |
# MAGIC |---|---|
# MAGIC | `WHERE trip_count >= 3` | remove low-volume zones first |
# MAGIC | `ROW_NUMBER() ... ORDER BY trip_count DESC` | rank within each borough |
# MAGIC | `QUALIFY ... <= 2` | keep `rn` 1 and 2 |
# MAGIC
# MAGIC **Expected:** **8 rows** (Staten Island is removed by `WHERE`).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top-2 by trip_count per borough, only zones with trip_count >= 3 (expect 8)
# MAGIC SELECT
# MAGIC   pickup_borough,
# MAGIC   pickup_zone,
# MAGIC   trip_count
# MAGIC   -- TODO: ROW_NUMBER() ... AS rn
# MAGIC FROM rideshare_dev.processed.kpi_zone_performance
# MAGIC WHERE 1 = 0  -- TODO: trip_count >= 3
# MAGIC -- TODO: QUALIFY ... <= 2
# MAGIC ORDER BY pickup_borough, trip_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC **Part 1 — rank zones**
# MAGIC
# MAGIC | step | pattern |
# MAGIC |---|---|
# MAGIC | Rank within a borough | `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)` |
# MAGIC | Keep Top-N in one query | `QUALIFY ... <= N` |
# MAGIC | Same Top-N when you need `rn` later | subquery / CTE + `WHERE rn <= N` |
# MAGIC
# MAGIC **Part 2 — track distance**
# MAGIC
# MAGIC | step | pattern |
# MAGIC |---|---|
# MAGIC | Running total | windowed `SUM` + `ROWS` frame |
# MAGIC | Previous day | `LAG` → `distance_change` |
# MAGIC | Label the change | `CASE` → `up` / `down` / `flat` / `n/a` |
# MAGIC
# MAGIC **Next:** `05 - CTEs and Parameterized SQL` — named query steps and safe
# MAGIC SQL parameters.