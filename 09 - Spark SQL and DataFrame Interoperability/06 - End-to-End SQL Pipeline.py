# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - End-to-End SQL Pipeline
# MAGIC
# MAGIC Phase II synthesis: rebuild Module 8 KPI contracts in Spark SQL. Read-only.
# MAGIC
# MAGIC `trip_enriched`, `trip_driver_assignment`.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Rebuild Module 8 KPI contracts in Spark SQL (read-only; no writes)
# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — load source tables
# MAGIC
# MAGIC We'll use two managed tables:
# MAGIC
# MAGIC - `trip_enriched` — daily and pickup-zone KPIs (**106** trips)
# MAGIC - `trip_driver_assignment` — driver productivity KPI (**100** trips)

# COMMAND ----------

trip_enriched = spark.table("rideshare_dev.processed.trip_enriched")  # noqa: F821
trip_driver_assignment = spark.table(  # noqa: F821
    "rideshare_dev.processed.trip_driver_assignment"
)

print(f"trip_enriched: {trip_enriched.count()} rows")  # expect 106
print(f"trip_driver_assignment: {trip_driver_assignment.count()} rows")  # expect 100

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. What does daily trip activity look like?
# MAGIC
# MAGIC Finance wants a daily view of trip activity so it can compare volume,
# MAGIC revenue, payouts, distance, and ride duration across days.
# MAGIC
# MAGIC For each day, we will calculate:
# MAGIC
# MAGIC - number of trips
# MAGIC - total base fare
# MAGIC - total tips
# MAGIC - total driver payout
# MAGIC - total distance
# MAGIC - average trip distance
# MAGIC - average ride duration
# MAGIC
# MAGIC The output grain is **one row per `trip_date`**. The dataset covers
# MAGIC **14 dated days**, so the result should contain **14 rows**.
# MAGIC
# MAGIC Trips **101–106** have a NULL `trip_date`, so we remove them before
# MAGIC grouping.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1a — Keep dated trips
# MAGIC
# MAGIC First, keep only rows where `trip_date` is not NULL.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM rideshare_dev.processed.trip_enriched
# MAGIC WHERE trip_date IS NOT NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1b — Aggregate by day
# MAGIC
# MAGIC Same filter, now `GROUP BY trip_date` and calculate the daily measures.
# MAGIC
# MAGIC **Expected:** **14 rows**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   trip_date,
# MAGIC   COUNT(*) AS trip_count,
# MAGIC   SUM(base_fare_amount) AS total_base_fare,
# MAGIC   SUM(tip_amount) AS total_tip,
# MAGIC   SUM(driver_payout_amount) AS total_driver_payout,
# MAGIC   SUM(trip_distance_miles) AS total_distance_miles,
# MAGIC   ROUND(AVG(trip_distance_miles), 2) AS avg_distance_miles,
# MAGIC   ROUND(AVG(ride_duration_mins), 2) AS avg_ride_duration_mins
# MAGIC FROM rideshare_dev.processed.trip_enriched
# MAGIC WHERE trip_date IS NOT NULL
# MAGIC GROUP BY trip_date
# MAGIC ORDER BY trip_date

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Which pickup zones generate the most business?
# MAGIC
# MAGIC Operations wants to compare performance across pickup zones.
# MAGIC
# MAGIC For each pickup zone, we will calculate:
# MAGIC
# MAGIC - trip count
# MAGIC - total base fare
# MAGIC - total tips
# MAGIC - tip percentage (`tip_percent_of_base`)
# MAGIC - average trip distance
# MAGIC - average ride duration
# MAGIC
# MAGIC The output grain is **one row per (`pickup_borough`, `pickup_zone`)**.
# MAGIC There are **20 pickup zones**, so the result should contain **20 rows**.
# MAGIC
# MAGIC This calculation uses **all 106 trips**. `SUM` and `AVG` skip NULL
# MAGIC measure values rather than treating them as zero.
# MAGIC
# MAGIC `tip_percent_of_base` is a ratio of **sums**
# MAGIC (`total_tip / total_base_fare × 100`), not an average of trip-level
# MAGIC percentages. Apply it only when total base fare is greater than **0**;
# MAGIC otherwise the result is NULL.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2a — Aggregate by pickup zone
# MAGIC
# MAGIC Group by `pickup_borough` and `pickup_zone`. Calculate volume and fare
# MAGIC totals for each zone.
# MAGIC
# MAGIC **Expected:** **20 rows**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   pickup_borough,
# MAGIC   pickup_zone,
# MAGIC   MAX(pickup_location_id) AS pickup_location_id,
# MAGIC   COUNT(*) AS trip_count,
# MAGIC   SUM(base_fare_amount) AS total_base_fare,
# MAGIC   SUM(tip_amount) AS total_tip
# MAGIC FROM rideshare_dev.processed.trip_enriched
# MAGIC GROUP BY pickup_borough, pickup_zone
# MAGIC ORDER BY pickup_borough, pickup_zone

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b — Add tip percent and averages
# MAGIC
# MAGIC Keep the same grouping. Add `tip_percent_of_base` and the two averages.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   pickup_borough,
# MAGIC   pickup_zone,
# MAGIC   MAX(pickup_location_id) AS pickup_location_id,
# MAGIC   COUNT(*) AS trip_count,
# MAGIC   SUM(base_fare_amount) AS total_base_fare,
# MAGIC   SUM(tip_amount) AS total_tip,
# MAGIC   CASE
# MAGIC     WHEN SUM(base_fare_amount) > 0
# MAGIC     THEN ROUND(100 * SUM(tip_amount) / SUM(base_fare_amount), 1)
# MAGIC   END AS tip_percent_of_base,
# MAGIC   ROUND(AVG(trip_distance_miles), 2) AS avg_distance_miles,
# MAGIC   ROUND(AVG(ride_duration_mins), 2) AS avg_ride_duration_mins
# MAGIC FROM rideshare_dev.processed.trip_enriched
# MAGIC GROUP BY pickup_borough, pickup_zone
# MAGIC ORDER BY pickup_borough, pickup_zone

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Which drivers cover the most distance?
# MAGIC
# MAGIC Fleet operations wants a productivity summary for every driver.
# MAGIC
# MAGIC For each driver, we will calculate:
# MAGIC
# MAGIC - number of trips
# MAGIC - total distance driven
# MAGIC - average ride duration
# MAGIC - service types handled
# MAGIC
# MAGIC Then rank drivers across the fleet by **total distance driven**.
# MAGIC
# MAGIC The source is `trip_driver_assignment` (**100** trips, **12** drivers).
# MAGIC The output grain is **one row per `driver_id`**.
# MAGIC
# MAGIC A CTE holds the per-driver aggregation (`05 - CTEs and Parameterized SQL`).
# MAGIC The outer query adds `DENSE_RANK` (`04 - SQL Windows and QUALIFY`).

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3a — Aggregate by driver
# MAGIC
# MAGIC Name the per-driver aggregation `driver_agg`, then select from it.
# MAGIC
# MAGIC **Expected:** **12 rows**.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH driver_agg AS (
# MAGIC   SELECT
# MAGIC     driver_id,
# MAGIC     MAX(driver_name) AS driver_name,
# MAGIC     COUNT(*) AS trip_count,
# MAGIC     SUM(trip_distance_miles) AS total_distance_miles,
# MAGIC     ROUND(AVG(ride_duration_mins), 2) AS avg_ride_duration_mins,
# MAGIC     SORT_ARRAY(COLLECT_SET(service_type)) AS unique_service_types
# MAGIC   FROM rideshare_dev.processed.trip_driver_assignment
# MAGIC   GROUP BY driver_id
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM driver_agg
# MAGIC ORDER BY driver_id

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3b — Rank drivers by total distance
# MAGIC
# MAGIC Same CTE. The outer query adds a fleet-wide `DENSE_RANK` ordered by
# MAGIC `total_distance_miles` descending. Drivers with the same total distance
# MAGIC receive the same rank.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH driver_agg AS (
# MAGIC   SELECT
# MAGIC     driver_id,
# MAGIC     MAX(driver_name) AS driver_name,
# MAGIC     COUNT(*) AS trip_count,
# MAGIC     SUM(trip_distance_miles) AS total_distance_miles,
# MAGIC     ROUND(AVG(ride_duration_mins), 2) AS avg_ride_duration_mins,
# MAGIC     SORT_ARRAY(COLLECT_SET(service_type)) AS unique_service_types
# MAGIC   FROM rideshare_dev.processed.trip_driver_assignment
# MAGIC   GROUP BY driver_id
# MAGIC )
# MAGIC SELECT
# MAGIC   driver_id,
# MAGIC   driver_name,
# MAGIC   trip_count,
# MAGIC   total_distance_miles,
# MAGIC   avg_ride_duration_mins,
# MAGIC   unique_service_types,
# MAGIC   DENSE_RANK() OVER (
# MAGIC     ORDER BY total_distance_miles DESC
# MAGIC   ) AS distance_dense_rank
# MAGIC FROM driver_agg
# MAGIC ORDER BY distance_dense_rank, driver_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary — Module 9
# MAGIC
# MAGIC The three KPI contracts from Module 8 `08 - Build KPI Tables` can be
# MAGIC expressed in Spark SQL — **same logic, different API**.
# MAGIC
# MAGIC | Notebook | Focus |
# MAGIC |---|---|
# MAGIC | `01 - Dual API Foundations and When to Choose` | Bridges + row-level `CASE` |
# MAGIC | `02 - SQL Joins, Aggregations, and Filtering` | JOIN, `GROUP BY`, `HAVING` |
# MAGIC | `03 - SQL Pivot, Unpivot, and Sampling` | Reshape + `TABLESAMPLE` |
# MAGIC | `04 - SQL Windows and QUALIFY` | Ranking, running totals, `LAG` |
# MAGIC | `05 - CTEs and Parameterized SQL` | Named steps + `:params` |
# MAGIC | `06 - End-to-End SQL Pipeline` | KPI rebuild in SQL |
# MAGIC
# MAGIC **Next:** Module 10 — Delta Lake for Managed Tables.
