# Databricks notebook source
# MAGIC %md
# MAGIC # 08 - Build KPI Tables
# MAGIC
# MAGIC Write-only: three managed `kpi_*` Delta tables for Module 9.
# MAGIC
# MAGIC Both managed tables (`trip_enriched`, `trip_driver_assignment`).
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Apply the module patterns to write three managed `kpi_*` tables
# COMMAND ----------

# MAGIC %md
# MAGIC ##### LOAD: `trip_enriched` and `trip_driver_assignment`
# MAGIC
# MAGIC Load the two managed tables created in Module 7:
# MAGIC
# MAGIC - `trip_enriched` — used for the daily and pickup-zone KPIs
# MAGIC - `trip_driver_assignment` — used for the driver productivity KPI
# MAGIC
# MAGIC We also check the source row counts before building the KPI tables.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

trip_enriched = spark.table("rideshare_dev.processed.trip_enriched")  # noqa: F821
trip_driver_assignment = spark.table(  # noqa: F821
    "rideshare_dev.processed.trip_driver_assignment"
)

print(f"trip_enriched: {trip_enriched.count()} rows")  # expect 106
print(
    f"trip_driver_assignment: {trip_driver_assignment.count()} rows"
)  # expect 100

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. What does daily trip activity look like?
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
# MAGIC The output grain is:
# MAGIC
# MAGIC **one row per `trip_date`**
# MAGIC
# MAGIC The dataset covers **14 dated days**, from **2026-03-01 through
# MAGIC 2026-03-14**, so the final KPI should contain **14 rows**.
# MAGIC
# MAGIC Trips **101–106** have a NULL `trip_date`, so we remove them before
# MAGIC grouping. Those same rows contain the NULL measures in this dataset,
# MAGIC which means the remaining **100 dated trips** have complete values for
# MAGIC the measures used here.
# MAGIC
# MAGIC We keep `total_distance_miles` in the KPI because Module 9 will use the
# MAGIC daily values for calculations such as **running totals and `lag` across
# MAGIC dates**.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### `1a: Keep dated trips and aggregate by day`
# MAGIC
# MAGIC First, remove rows where `trip_date` is NULL.
# MAGIC
# MAGIC Then group the remaining trips by `trip_date` and calculate the daily
# MAGIC measures.

# COMMAND ----------

# explicit NULL-date removal — measures below are fully populated
dated_trip = trip_enriched.filter(F.col("trip_date").isNotNull())

kpi_daily_trip_summary = dated_trip.groupBy("trip_date").agg(
    F.count("*").alias("trip_count"),
    F.sum("base_fare_amount").alias("total_base_fare"),
    F.sum("tip_amount").alias("total_tip"),
    F.sum("driver_payout_amount").alias("total_driver_payout"),
    F.sum("trip_distance_miles").alias("total_distance_miles"),
    F.round(F.avg("trip_distance_miles"), 2).alias("avg_distance_miles"),
    F.round(F.avg("ride_duration_mins"), 2).alias("avg_ride_duration_mins"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### `1b: Preview the daily KPI`
# MAGIC
# MAGIC Sort the result by `trip_date` so we can inspect the daily values in
# MAGIC calendar order.
# MAGIC
# MAGIC We expect **14 rows — one for each active date**.

# COMMAND ----------

kpi_daily_trip_summary.orderBy("trip_date").show(14, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### `1c: Write the daily KPI table`
# MAGIC
# MAGIC Save the result as the managed table:
# MAGIC
# MAGIC `rideshare_dev.processed.kpi_daily_trip_summary`

# COMMAND ----------

kpi_daily_trip_summary.write.mode("overwrite").saveAsTable(
    "rideshare_dev.processed.kpi_daily_trip_summary"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### `1d: Verify the daily KPI`
# MAGIC
# MAGIC Read the saved table back and verify its row count.
# MAGIC
# MAGIC Because the grain is **one row per `trip_date`**, we expect **14 rows**.

# COMMAND ----------

daily_out = spark.table(  # noqa: F821
    "rideshare_dev.processed.kpi_daily_trip_summary"
)
print(f"kpi_daily_trip_summary: {daily_out.count()} rows")  # expect 14

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Which pickup zones generate the most business?
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
# MAGIC The output grain is:
# MAGIC
# MAGIC **one row per (`pickup_borough`, `pickup_zone`)**
# MAGIC
# MAGIC There are **20 pickup zones**, so the final KPI should contain **20 rows**.
# MAGIC
# MAGIC Unlike the daily KPI, this calculation uses **all 106 trips**. Some of the
# MAGIC additional trips contain NULL measure values, which gives us a useful
# MAGIC example of how Spark aggregates handle NULLs.
# MAGIC
# MAGIC | Pickup zone | NULL measure | Trip |
# MAGIC |---|---|---:|
# MAGIC | Manhattan / Financial District | `base_fare_amount` | 104 |
# MAGIC | Manhattan / Harlem | `base_fare_amount`, `tip_amount`, `trip_distance_miles` | 106 |
# MAGIC | Queens / Astoria | `tip_amount`, `trip_distance_miles` | 103 |
# MAGIC | Brooklyn / Williamsburg | `trip_distance_miles` | 105 |
# MAGIC
# MAGIC Spark's `sum` and `avg` ignore NULL values rather than treating them as
# MAGIC zero.
# MAGIC
# MAGIC For example, the Williamsburg trip with NULL distance still contributes to
# MAGIC `trip_count`, but it does not contribute to `avg_distance_miles`.
# MAGIC
# MAGIC We also calculate `tip_percent_of_base` from the **total tips for the
# MAGIC zone** and the **total base fare for the zone**:
# MAGIC
# MAGIC `total_tip / total_base_fare × 100`
# MAGIC
# MAGIC This is a ratio of aggregated totals, not an average of trip-level
# MAGIC percentages.
# MAGIC
# MAGIC The calculation is applied only when the total base fare is greater than
# MAGIC **0**. Otherwise, the result is NULL.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### `2a: Aggregate by pickup zone`
# MAGIC
# MAGIC Group the trips by:
# MAGIC
# MAGIC - `pickup_borough`
# MAGIC - `pickup_zone`
# MAGIC
# MAGIC Then calculate the volume, fare, tip, distance, and duration measures for
# MAGIC each zone.

# COMMAND ----------

kpi_zone_performance = trip_enriched.groupBy(
    "pickup_borough",
    "pickup_zone",
).agg(
    # one location_id per zone — max is deterministic
    F.max("pickup_location_id").alias("pickup_location_id"),
    F.count("*").alias("trip_count"),
    F.sum("base_fare_amount").alias("total_base_fare"),
    F.sum("tip_amount").alias("total_tip"),
    # tip percent only when base fare sum is positive; else NULL
    F.when(
        F.sum("base_fare_amount") > 0,
        F.round(
            F.lit(100) * F.sum("tip_amount") / F.sum("base_fare_amount"),
            1,
        ),
    )
    .otherwise(F.lit(None))
    .alias("tip_percent_of_base"),
    F.round(F.avg("trip_distance_miles"), 2).alias("avg_distance_miles"),
    F.round(F.avg("ride_duration_mins"), 2).alias("avg_ride_duration_mins"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### `2b: Preview the zone KPI`
# MAGIC
# MAGIC Sort by `pickup_borough` and `pickup_zone` so the output is easy to
# MAGIC inspect.
# MAGIC
# MAGIC We expect **20 rows — one for each pickup zone**.

# COMMAND ----------

kpi_zone_performance.orderBy(
    "pickup_borough",
    "pickup_zone",
).show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### `2c: Write the zone KPI table`
# MAGIC
# MAGIC Save the result as the managed table:
# MAGIC
# MAGIC `rideshare_dev.processed.kpi_zone_performance`

# COMMAND ----------

kpi_zone_performance.write.mode("overwrite").saveAsTable(
    "rideshare_dev.processed.kpi_zone_performance"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### `2d: Verify the zone KPI`
# MAGIC
# MAGIC Read the saved table back and verify its row count.
# MAGIC
# MAGIC Because the grain is **one row per pickup zone**, we expect **20 rows**.

# COMMAND ----------

zone_out = spark.table(  # noqa: F821
    "rideshare_dev.processed.kpi_zone_performance"
)
print(f"kpi_zone_performance: {zone_out.count()} rows")  # expect 20

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Which drivers cover the most distance?
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
# MAGIC We will then rank drivers across the fleet by **total distance driven**.
# MAGIC
# MAGIC The source is `trip_driver_assignment`, which contains trips **1–100** and
# MAGIC has complete values for the measures used here.
# MAGIC
# MAGIC The output grain is:
# MAGIC
# MAGIC **one row per `driver_id`**
# MAGIC
# MAGIC There are **12 drivers**, so the final KPI should contain **12 rows**.
# MAGIC
# MAGIC This calculation uses two steps:
# MAGIC
# MAGIC 1. aggregate the trip rows into one row per driver
# MAGIC 2. rank those driver-level rows by total distance
# MAGIC
# MAGIC The ranking therefore compares **driver totals**, not individual trips.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### `3a: Aggregate by driver`
# MAGIC
# MAGIC Group the trips by `driver_id` and calculate one productivity row for each
# MAGIC driver.
# MAGIC
# MAGIC We also collect the distinct `service_type` values handled by each driver
# MAGIC into an array.

# COMMAND ----------

driver_agg = trip_driver_assignment.groupBy("driver_id").agg(
    # max(driver_name) is deterministic — one name per driver_id
    F.max("driver_name").alias("driver_name"),
    F.count("*").alias("trip_count"),
    F.sum("trip_distance_miles").alias("total_distance_miles"),
    F.round(F.avg("ride_duration_mins"), 2).alias("avg_ride_duration_mins"),
    F.sort_array(F.collect_set("service_type")).alias("unique_service_types"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### `3b: Rank drivers by total distance`
# MAGIC
# MAGIC The DataFrame now contains **one row per driver**.
# MAGIC
# MAGIC Create a fleet-wide window ordered by `total_distance_miles` from highest
# MAGIC to lowest, then use `dense_rank` to assign each driver a distance rank.
# MAGIC
# MAGIC Drivers with the same total distance receive the same rank.

# COMMAND ----------

distance_rank_window = Window.orderBy(F.col("total_distance_miles").desc())

kpi_driver_productivity = driver_agg.withColumn(
    "distance_dense_rank",
    F.dense_rank().over(distance_rank_window),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### `3c: Preview the driver KPI`
# MAGIC
# MAGIC Sort by `distance_dense_rank` so the drivers with the greatest total
# MAGIC distance appear first.
# MAGIC
# MAGIC Use `driver_id` as a secondary display order when drivers share the same
# MAGIC rank.
# MAGIC
# MAGIC We expect **12 rows — one for each driver**.

# COMMAND ----------

kpi_driver_productivity.orderBy(
    "distance_dense_rank",
    "driver_id",
).show(12, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### `3d: Write the driver KPI table`
# MAGIC
# MAGIC Save the result as the managed table:
# MAGIC
# MAGIC `rideshare_dev.processed.kpi_driver_productivity`

# COMMAND ----------

kpi_driver_productivity.write.mode("overwrite").saveAsTable(
    "rideshare_dev.processed.kpi_driver_productivity"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### `3e: Verify the driver KPI`
# MAGIC
# MAGIC Read the saved table back and verify its row count.
# MAGIC
# MAGIC Because the grain is **one row per driver**, we expect **12 rows**.

# COMMAND ----------

driver_out = spark.table(  # noqa: F821
    "rideshare_dev.processed.kpi_driver_productivity"
)
print(f"kpi_driver_productivity: {driver_out.count()} rows")  # expect 12

# COMMAND ----------

# MAGIC %md
# MAGIC ## Output
# MAGIC
# MAGIC This notebook created three analytics-ready KPI tables:
# MAGIC
# MAGIC | Table | Grain | Expected rows |
# MAGIC |---|---|---:|
# MAGIC | `rideshare_dev.processed.kpi_daily_trip_summary` | one row per `trip_date` | 14 |
# MAGIC | `rideshare_dev.processed.kpi_zone_performance` | one row per (`pickup_borough`, `pickup_zone`) | 20 |
# MAGIC | `rideshare_dev.processed.kpi_driver_productivity` | one row per `driver_id` | 12 |
# MAGIC
# MAGIC Across the three KPIs, the workflow follows the same pattern:
# MAGIC
# MAGIC **prepare the input → aggregate → inspect → write → verify**
# MAGIC
# MAGIC The results are stored as **Unity Catalog managed Delta tables** using
# MAGIC `saveAsTable()`.
# MAGIC
# MAGIC In **Module 9** `06 - End-to-End SQL Pipeline`, we rebuild the same KPI
# MAGIC calculations in Spark SQL. That notebook does not write tables.
# MAGIC
# MAGIC **Next:** Module 9 — Spark SQL and DataFrame Interoperability
# MAGIC (`01 - Dual API Foundations and When to Choose`).
