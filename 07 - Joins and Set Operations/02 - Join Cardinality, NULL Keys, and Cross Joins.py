# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02 - Join Cardinality, NULL Keys, and Cross Joins
# MAGIC
# MAGIC Notebook 01 showed how to write a join. This notebook explains how many
# MAGIC rows a join returns, and why.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Understand grain and join relationships: 1:1, 1:M, M:1, and M:M.
# MAGIC - Explain how repeated join keys can increase the number of rows in a join result.
# MAGIC - Use `count()`, `countDistinct()`, and `NULL` counts to check join keys.
# MAGIC - Understand why `NULL` join keys do not match with standard equality and how `eqNullSafe()` changes this behavior.
# MAGIC - Understand how a cross join creates every possible combination of rows from two DataFrames.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup
# MAGIC
# MAGIC Attach **all-purpose compute**.
# MAGIC
# MAGIC Create three small DataFrames from the rideshare data:
# MAGIC
# MAGIC - `drivers`: one row per driver.
# MAGIC - `driver_trips`: each trip with its assigned driver. Trip 101 has no
# MAGIC   assigned driver, so its `driver_id` is `NULL`.
# MAGIC - `trips`: each trip with its service type and pickup location.

# COMMAND ----------

from pyspark.sql import functions as F

drivers = spark.createDataFrame(  # noqa: F821
    [("D001", "Ravi Kumar"), ("D002", "Priya Sharma")],
    ["driver_id", "driver_name"],
)

driver_trips = spark.createDataFrame(  # noqa: F821
    [("D001", 8), ("D001", 25), ("D001", 43), ("D002", 4), ("D002", 5), (None, 101)],
    ["driver_id", "trip_id"],
)

trips = spark.createDataFrame(  # noqa: F821
    [
        (4, "Standard", 10),
        (5, "Premium", 8),
        (8, "Premium", 1),
        (25, "Standard", 8),
        (43, "XL", 9),
        (101, "Premium", 5),
    ],
    ["trip_id", "service_type", "pickup_location_id"],
)

drivers.show()
driver_trips.orderBy("trip_id").show()
trips.orderBy("trip_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Grain and cardinality
# MAGIC
# MAGIC Grain describes what one row represents, while cardinality describes how many rows from each DataFrame can share the same join key.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1:1

# COMMAND ----------

driver_trips.join(trips, on="trip_id", how="inner").orderBy("trip_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1:M

# COMMAND ----------

drivers.join(driver_trips, on="driver_id", how="inner").orderBy(
    "driver_id", "trip_id"
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### M:1

# COMMAND ----------

driver_trips.join(drivers, on="driver_id", how="inner").orderBy("trip_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### M:M

# COMMAND ----------

driver_zones = (
    drivers.join(driver_trips, on="driver_id", how="inner")
    .join(trips, on="trip_id", how="inner")
    .select("driver_name", "pickup_location_id")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Key profiling
# MAGIC
# MAGIC Before joining, we can check whether the join keys contain repeated or NULL values.

# COMMAND ----------

print("drivers")
drivers.select(
    F.count("*").alias("row_count"),
    F.countDistinct("driver_id").alias("distinct_driver_id"),
    F.sum(F.when(F.col("driver_id").isNull(), 1).otherwise(0)).alias("null_driver_id"),
).show()

print("driver_trips")
driver_trips.select(
    F.count("*").alias("row_count"),
    F.countDistinct("driver_id").alias("distinct_driver_id"),
    F.sum(F.when(F.col("driver_id").isNull(), 1).otherwise(0)).alias("null_driver_id"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. NULL join keys
# MAGIC
# MAGIC Standard equality never matches `NULL`. `NULL = 'D001'` is not true, and
# MAGIC `NULL = NULL` is not true either.

# COMMAND ----------

drivers_with_unknown = spark.createDataFrame(  # noqa: F821
    [("D001", "Ravi Kumar"), ("D002", "Priya Sharma"), (None, "Unknown")],
    ["driver_id", "driver_name"],
)

# COMMAND ----------

drivers_with_unknown.join(driver_trips, on="driver_id", how="inner").orderBy(
    "trip_id"
).show()

# COMMAND ----------

drivers_with_unknown.join(
    driver_trips,
    on=drivers_with_unknown.driver_id.eqNullSafe(driver_trips.driver_id),
    how="inner",
).select(
    drivers_with_unknown.driver_id, "driver_name", "trip_id"
).orderBy("trip_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Cross joins
# MAGIC
# MAGIC A cross join pairs every row in one DataFrame with every row in the other, without a join key.
# MAGIC
# MAGIC A report must list every driver against every service type.

# COMMAND ----------

service_types = trips.select("service_type").distinct()
grid = drivers.crossJoin(service_types)

grid.orderBy("driver_id", "service_type").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - Grain describes what one row represents. Cardinality describes how many rows on each side of a join can share the same key: 1:1, 1:M, M:1, or M:M.
# MAGIC - When join keys repeat, matching rows can multiply, increasing the number of rows in the result.
# MAGIC - Use `count("*")`, `countDistinct()`, and `NULL` counts to identify repeated or missing join keys.
# MAGIC - Standard equality does not match `NULL` values. `eqNullSafe()` allows two `NULL` values to match.
# MAGIC - A cross join returns every possible combination of rows from two DataFrames, without using a join key.
# MAGIC
# MAGIC **Next:** `03 - Sort-Merge Join vs Broadcast Hash Join`