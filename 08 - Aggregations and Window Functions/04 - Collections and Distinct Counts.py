# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 04 - Collections and Distinct Counts
# MAGIC
# MAGIC In earlier lessons, you learned how to use `groupBy()` with aggregate functions such as `count()`, `sum()`, `avg()`, `min()`, and `max()`. Each function returns a single summary value for each group.
# MAGIC
# MAGIC In this lesson, you'll learn how to use `groupBy()` with `collect_list()` and `collect_set()`. Instead of returning a single summary value, these functions collect the values within each group into an array.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Collect every value in a group, including duplicates, with `collect_list()`.
# MAGIC - Collect each distinct value in a group once with `collect_set()`.
# MAGIC - Sort a collected array with `sort_array()` so results are easy to compare.
# MAGIC - Count distinct combinations with `countDistinct()`.

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach **all-purpose compute**.
# MAGIC
# MAGIC This notebook uses the `trip_driver_assignment` table built in Module 7, **07 - Build Unified Curated Tables**.
# MAGIC Each row represents one driver-trip assignment: 100 assignments across 12 drivers.

# COMMAND ----------

from pyspark.sql import functions as F

trip_driver_assignment = spark.table("rideshare_dev.processed.trip_driver_assignment")  # noqa: F821

print("trip_driver_assignment rows:", trip_driver_assignment.count())


D001_D002 = trip_driver_assignment.filter(
    trip_driver_assignment["driver_id"].isin("D001", "D002")
).orderBy("driver_id", "trip_id")

D001_D002.select(
    "driver_id",
    "trip_id",
    "service_type"
).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Section 1 - collect_list
# MAGIC %md
# MAGIC ## 1. Collect every service type with `collect_list()`
# MAGIC
# MAGIC **Question:** Which service types did each driver handle?

# COMMAND ----------

D001_D002.groupBy("driver_id").agg(
    F.count("*").alias("trip_count"),
    F.collect_list("service_type").alias("all_service_types"),
).orderBy("driver_id").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC D001 handled 9 trips, so its array contains 9 values. `STANDARD` appears 6 times.

# COMMAND ----------

# DBTITLE 1,Section 2 - collect_set
# MAGIC %md
# MAGIC ## 2. Collect distinct service types with `collect_set()`
# MAGIC
# MAGIC **Question:** Which different service types did each driver handle?

# COMMAND ----------

D001_D002.groupBy("driver_id").agg(
    F.count("*").alias("trip_count"),
    F.collect_set("service_type").alias("unique_service_types"),
).orderBy("driver_id").show(3, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Neither `collect_list()` nor `collect_set()` guarantees the order of values in the resulting array.

# COMMAND ----------

# DBTITLE 1,Section 3 - sort_array
# MAGIC %md
# MAGIC ## 3. Sort collected arrays with `sort_array()`

# COMMAND ----------

driver_service_types = D001_D002.groupBy("driver_id").agg(
    F.sort_array(F.collect_list("service_type")).alias("all_service_types"),
    F.sort_array(F.collect_set("service_type")).alias("unique_service_types"),
)

driver_service_types.orderBy("driver_id").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Section 4 - countDistinct
# MAGIC %md
# MAGIC ## 4. Count Unique Pickup-to-Drop-off Routes with `countDistinct()`
# MAGIC
# MAGIC **Question:** How many distinct pickup-to-drop-off routes appear across all
# MAGIC driver-trip assignments?

# COMMAND ----------

trip_driver_assignment.agg(
    F.count("*").alias("total_assignments"),
    F.countDistinct(
        "pickup_location_id",
        "dropoff_location_id"
    ).alias("distinct_routes"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC The table contains 100 driver-trip assignments but only 91 distinct pickup-to-drop-off routes. Assignments with the same pickup and drop-off locations are counted as one distinct route, even if different drivers handled them.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC - `collect_list()` collects values from each group into an array, retaining duplicates.
# MAGIC - `collect_set()` collects distinct values from each group into an array.
# MAGIC - `sort_array()` sorts the elements within an array for consistent, readable output.
# MAGIC - `countDistinct()` counts distinct combinations of values when multiple columns are provided.
# MAGIC
# MAGIC **Next:** **`05 - Pivot`** — reshape grouped results with `pivot`.