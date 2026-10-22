# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC ## 11 - Build KPI Tables
# MAGIC
# MAGIC Earlier lessons showed how to calculate metrics such as the total tip amount and the average trip distance. Those results stayed in memory. This lesson writes them into three managed KPI tables for Module 9.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Write three managed `kpi_*` tables to store metrics that earlier lessons only calculated in memory.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach **all-purpose compute**.
# MAGIC
# MAGIC Load the two managed tables created in Module 7, **07 - Build Unified Curated Tables**:
# MAGIC
# MAGIC - `rideshare_dev.processed.trip_enriched` — 106 rows, one row per trip.
# MAGIC - `rideshare_dev.processed.trip_driver_assignment` — 100 rows, one row per `driver_id`, `trip_id`.
# MAGIC
# MAGIC The next cell checks those source row counts before building the KPI tables.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

trip_enriched = spark.table("rideshare_dev.processed.trip_enriched")
trip_driver_assignment = spark.table("rideshare_dev.processed.trip_driver_assignment")

print(f"trip_enriched: {trip_enriched.count()} rows")  # expect 106
print(f"trip_driver_assignment: {trip_driver_assignment.count()} rows")  # expect 100

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. What does daily trip activity look like?
# MAGIC
# MAGIC Finance wants a daily summary of trips. For each day, calculate:
# MAGIC
# MAGIC - number of trips
# MAGIC - total base fare
# MAGIC - total tips
# MAGIC - total driver payout
# MAGIC - total distance
# MAGIC - average trip distance
# MAGIC - average ride duration
# MAGIC
# MAGIC Some trips have no `trip_date`. If we group them as-is, Spark puts them in a separate `NULL` group, which isn't a real day.
# MAGIC
# MAGIC So remove those trips first, then group the rest by `trip_date`.

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
# MAGIC ### Preview the daily summary
# MAGIC
# MAGIC Sort by `trip_date` to see the days in calendar order.

# COMMAND ----------

kpi_daily_trip_summary.orderBy("trip_date").show(14, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the daily KPI table
# MAGIC
# MAGIC Save the result as `rideshare_dev.processed.kpi_daily_trip_summary`.

# COMMAND ----------

kpi_daily_trip_summary.write.mode("overwrite"). \
saveAsTable("rideshare_dev.processed.kpi_daily_trip_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify the daily KPI table
# MAGIC
# MAGIC Read the saved table back and check the row count. The data covers 14 days, so expect 14 rows.

# COMMAND ----------

daily_out = spark.table(  # noqa: F821
    "rideshare_dev.processed.kpi_daily_trip_summary"
)
print(f"kpi_daily_trip_summary: {daily_out.count()} rows")  # expect 14

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Which pickup zones generate the most business?
# MAGIC
# MAGIC Operations wants to compare pickup zones. For each zone, calculate:
# MAGIC
# MAGIC - number of trips
# MAGIC - total base fare
# MAGIC - total tips
# MAGIC - tip percentage
# MAGIC - average trip distance
# MAGIC - average ride duration
# MAGIC
# MAGIC Tip percentage is the zone's total tips divided by its total base fare, × 100. If a zone's total base fare is 0, the result is NULL.
# MAGIC
# MAGIC All trips are included in the zone summary, even when some values are `NULL`.
# MAGIC
# MAGIC `count("*")` counts every trip. `sum()` and `avg()` skip only the `NULL` values in the column they're calculating.

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
# MAGIC ### Preview the zone summary
# MAGIC
# MAGIC Sort by `pickup_borough` and `pickup_zone` to see the zones in order.

# COMMAND ----------

kpi_zone_performance.orderBy(
    "pickup_borough",
    "pickup_zone",
).show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the zone KPI table
# MAGIC
# MAGIC Save the result as `rideshare_dev.processed.kpi_zone_performance`.
# MAGIC

# COMMAND ----------

kpi_zone_performance.write.mode("overwrite").saveAsTable(
    "rideshare_dev.processed.kpi_zone_performance"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify the zone KPI table
# MAGIC
# MAGIC Read the saved table back and check the row count. There are 20 pickup zones, so expect 20 rows.

# COMMAND ----------

zone_out = spark.table(  # noqa: F821
    "rideshare_dev.processed.kpi_zone_performance"
)
print(f"kpi_zone_performance: {zone_out.count()} rows")  # expect 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Which drivers cover the most distance?
# MAGIC
# MAGIC Fleet operations wants a productivity summary for each driver. For each driver, calculate:
# MAGIC
# MAGIC - number of trips
# MAGIC - total distance driven
# MAGIC - average ride duration
# MAGIC - service types handled
# MAGIC
# MAGIC `collect_set` gathers each driver's distinct service types into a list, and `sort_array` puts them in alphabetical order.

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
# MAGIC ### Rank drivers by total distance
# MAGIC
# MAGIC Rank all drivers from highest to lowest total distance with `dense_rank`. Drivers with the same total get the same rank.
# MAGIC
# MAGIC You'll see a warning that no partition is defined. Without `partitionBy`, Spark moves all rows into one partition to rank them. That's fine here because there are only 12 drivers.

# COMMAND ----------

distance_rank_window = Window.orderBy(F.col("total_distance_miles").desc())

kpi_driver_productivity = driver_agg.withColumn(
    "distance_dense_rank",
    F.dense_rank().over(distance_rank_window),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preview the driver summary
# MAGIC
# MAGIC Sort by rank to see the drivers who covered the most distance first.

# COMMAND ----------

kpi_driver_productivity.orderBy(
    "distance_dense_rank",
    "driver_id",
).show(12, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the driver KPI table
# MAGIC
# MAGIC Save the result as `rideshare_dev.processed.kpi_driver_productivity`.

# COMMAND ----------

kpi_driver_productivity.write.mode("overwrite").saveAsTable(
    "rideshare_dev.processed.kpi_driver_productivity"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify the driver KPI table
# MAGIC
# MAGIC Read the saved table back and check the row count. There are 12 drivers, so expect 12 rows.

# COMMAND ----------

driver_out = spark.table(  # noqa: F821
    "rideshare_dev.processed.kpi_driver_productivity"
)
print(f"kpi_driver_productivity: {driver_out.count()} rows")  # expect 12

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC This notebook created three KPI tables:
# MAGIC
# MAGIC | Table | Answers | Rows |
# MAGIC |---|---|---:|
# MAGIC | `kpi_daily_trip_summary` | What does daily trip activity look like? | 14 days |
# MAGIC | `kpi_zone_performance` | Which pickup zones generate the most business? | 20 zones |
# MAGIC | `kpi_driver_productivity` | Which drivers cover the most distance? | 12 drivers |
# MAGIC
# MAGIC Each table followed the same steps: **prepare → aggregate → preview → write → verify**.
# MAGIC
# MAGIC `saveAsTable()` stored all three as managed tables in `rideshare_dev.processed`.
# MAGIC
# MAGIC **Next:** Module 9 — Spark SQL and DataFrame Interoperability (`01 - Dual API Foundations and When to Choose`)