# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 06 - Window Functions Fundamentals
# MAGIC
# MAGIC Both `groupBy().agg()` and window functions organize rows with the same value into groups and apply aggregate functions such as `count()`, `sum()`, and `avg()` to calculate metrics like trip count, total trip distance, and average trip distance. The difference is in how they return the results.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Compare `groupBy().agg()` with window aggregations.
# MAGIC - Use `row_number()`, `rank()`, and `dense_rank()` to rank rows within each group.
# MAGIC - Filter ranked rows to select the top rows from each group.

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach **all-purpose compute**.
# MAGIC
# MAGIC This notebook uses the `trip_driver_assignment` table created in Module 7,
# MAGIC **07 - Build Unified Curated Tables**. Each row represents one driver-trip assignment: 100 assignments
# MAGIC across 12 drivers.
# MAGIC
# MAGIC The examples use drivers **D001** and **D010** only: 17 assignments.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

trip_driver_assignment = spark.table("rideshare_dev.processed.trip_driver_assignment")  # noqa: F821

D001_D010 = trip_driver_assignment.filter(F.col("driver_id").isin("D001", "D010"))

D001_D010.select(
    "driver_id",
    "trip_id",
    "trip_distance_miles",
).orderBy("driver_id", "trip_id").show(truncate=False)

print("Rows:", D001_D010.count())

# COMMAND ----------

# DBTITLE 1,Section 1 - groupBy versus window aggregations
# MAGIC %md
# MAGIC ## 1. Compare `groupBy().agg()` and window functions
# MAGIC
# MAGIC **Question:** For each driver, what are the trip count, total trip distance, and average trip distance?
# MAGIC
# MAGIC ### Using `groupBy().agg()`

# COMMAND ----------

driver_summary = D001_D010.groupBy("driver_id").agg(
    F.count("*").alias("driver_trip_count"),
    F.sum("trip_distance_miles").alias("driver_total_distance"),
    F.round(F.avg("trip_distance_miles"), 2).alias("driver_avg_distance"),
)

driver_summary.orderBy("driver_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using window functions
# MAGIC Equivalent SQL query:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC     driver_id,
# MAGIC     trip_id,
# MAGIC     trip_distance_miles,
# MAGIC     COUNT(*) OVER (
# MAGIC         PARTITION BY driver_id
# MAGIC     ) AS driver_trip_count,
# MAGIC     SUM(trip_distance_miles) OVER (
# MAGIC         PARTITION BY driver_id
# MAGIC     ) AS driver_total_distance,
# MAGIC     ROUND(
# MAGIC         AVG(trip_distance_miles) OVER (
# MAGIC             PARTITION BY driver_id
# MAGIC         ), 2
# MAGIC     ) AS driver_avg_distance
# MAGIC FROM rideshare_dev.processed.trip_driver_assignment
# MAGIC WHERE driver_id IN ('D001', 'D010');
# MAGIC ```

# COMMAND ----------

driver_window = Window.partitionBy("driver_id")

driver_metrics = (
    D001_D010
    .withColumn("driver_trip_count", F.count("*").over(driver_window))
    .withColumn("driver_total_distance", F.sum("trip_distance_miles").over(driver_window))
    .withColumn(
        "driver_avg_distance",
        F.round(F.avg("trip_distance_miles").over(driver_window), 2),
    )
)

driver_metrics.select(
    "driver_id",
    "trip_id",
    "trip_distance_miles",
    "driver_trip_count",
    "driver_total_distance",
    "driver_avg_distance",
).orderBy("driver_id", "trip_id").show(truncate=False)

print("driver_metrics rows:", driver_metrics.count())

# COMMAND ----------

# DBTITLE 1,Section 2 - Rank trips within each driver
# MAGIC %md
# MAGIC ## 2. Rank trips within each driver
# MAGIC
# MAGIC **Question:** For each driver, rank trips from longest to shortest distance.

# COMMAND ----------

distance_rank_window = Window.partitionBy("driver_id").orderBy(
    F.col("trip_distance_miles").desc()
)


ranked_trips = (
    D001_D010
    .withColumn("distance_row_number", F.row_number().over(distance_rank_window))
    .withColumn("distance_rank", F.rank().over(distance_rank_window))
    .withColumn("distance_dense_rank", F.dense_rank().over(distance_rank_window))
)

ranked_trips.select(
    "driver_id",
    "trip_id",
    "trip_distance_miles",
    "distance_row_number",
    "distance_rank",
    "distance_dense_rank",
).orderBy("driver_id", "distance_row_number").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Section 3 - Keep the top rows per driver
# MAGIC %md
# MAGIC ## 3. Select the four longest trips per driver
# MAGIC
# MAGIC **Question:** What are the four longest trips for each driver?

# COMMAND ----------


ranked_trips.filter(F.col("distance_row_number") <= 4).orderBy(
    "driver_id", "distance_row_number"
).select(
    "driver_id",
    "trip_id",
    "trip_distance_miles",
    "distance_row_number",
).show(truncate=False)




# COMMAND ----------

ranked_trips.filter(F.col("distance_rank") <= 4).orderBy(
    "driver_id", "distance_rank"
).select(
    "driver_id",
    "trip_id",
    "trip_distance_miles",
    "distance_rank",
).show(truncate=False)



# COMMAND ----------

ranked_trips.filter(F.col("distance_dense_rank") <= 4).orderBy(
    "driver_id", "distance_dense_rank"
).select(
    "driver_id",
    "trip_id",
    "trip_distance_miles",
    "distance_dense_rank",
).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Key Takeaways
# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC - `groupBy().agg()` returns one summary row per group, while window functions add group-level values to every row and keep the original grain.
# MAGIC - `Window.partitionBy()` defines the group of related rows, and `.over()` applies a function across that window.
# MAGIC - Ranking functions require `orderBy()` in the window specification.
# MAGIC - `row_number()` assigns a unique sequential number to each row; `rank()` gives tied rows the same rank and skips the next number; `dense_rank()` gives tied rows the same rank with no gap.
# MAGIC - Filtering on a ranking column selects the top rows per group: `row_number()` returns at most `N` rows per group, while `rank()` and `dense_rank()` include all rows tied at the cutoff.
# MAGIC
# MAGIC **Next:** `07 - Running Totals and Window Frames`