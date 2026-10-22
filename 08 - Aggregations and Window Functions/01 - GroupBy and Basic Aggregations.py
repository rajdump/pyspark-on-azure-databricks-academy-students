# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 - GroupBy and Basic Aggregations
# MAGIC
# MAGIC `groupBy()` function organizes rows with the same value into a single group. You can then use aggregate functions like count, sum, avg, etc to compute metrics for each group.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Understand how `groupBy()` combines individual rows with the same value into one summary row per group.
# MAGIC - Calculate multiple aggregates using `agg()` and assign meaningful column names.
# MAGIC - Understand the difference between counting rows and counting distinct values.

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach **all-purpose compute**.
# MAGIC
# MAGIC This notebook uses the `trip_enriched` table created in Module 7, **07 - Build Unified Curated Tables**. It contains **one row per trip**: a total of 106 trips across 16 columns.

# COMMAND ----------

from pyspark.sql import functions as F

trip_enriched = spark.table("rideshare_dev.processed.trip_enriched") 

print("trip_enriched rows:", trip_enriched.count())
trip_enriched.printSchema()

# COMMAND ----------

trip_enriched.show(106)

# COMMAND ----------

# DBTITLE 1,Section 1 - Output grain
# MAGIC %md
# MAGIC ## 1. Output grain Before and After Grouping
# MAGIC
# MAGIC Before grouping, each row represents one trip. After grouping, each row represents one service type and its total trip count.

# COMMAND ----------

# MAGIC %md
# MAGIC Equivalent SQL query:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT COUNT(DISTINCT service_type) AS service_type_count
# MAGIC FROM trip_enriched
# MAGIC ```

# COMMAND ----------

trip_enriched.select("service_type").distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Equivalent SQL query:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT service_type, COUNT(*) AS count 
# MAGIC FROM trip_enriched GROUP BY service_type
# MAGIC ```

# COMMAND ----------

trip_enriched.groupBy("service_type").count().orderBy(F.col("count").desc()).show()

# COMMAND ----------

# DBTITLE 1,Section 2 - groupBy and agg
# MAGIC %md
# MAGIC ## 2. Calculate Multiple Aggregates per Group
# MAGIC
# MAGIC The `agg()` method allows you to calculate multiple aggregates for each group, like the number of trips, total tips, and average distance.

# COMMAND ----------

# MAGIC %md
# MAGIC Apply aggregation's without naming the calculated columns.
# MAGIC
# MAGIC
# MAGIC Equivalent SQL query:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT service_type, COUNT(*), SUM(tip_amount), AVG(trip_distance_miles)
# MAGIC FROM trip_enriched
# MAGIC GROUP BY service_type;
# MAGIC ```

# COMMAND ----------

trip_enriched.groupBy("service_type").agg(
    F.count("*"),
    F.sum("tip_amount"),
    F.avg("trip_distance_miles"),
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Apply aggregation's naming the calculated columns.

# COMMAND ----------

service_summary = trip_enriched.groupBy("service_type").agg(
    F.count("*").alias("trip_count"),
    F.sum("tip_amount").alias("total_tip"),
    F.round(F.avg("trip_distance_miles"), 2).alias("avg_distance_miles"),
    F.min("ride_duration_mins").alias("shortest_ride_mins"),
    F.max("ride_duration_mins").alias("longest_ride_mins"),
)

service_summary.orderBy(F.col("trip_count").desc()).show()

# COMMAND ----------

# DBTITLE 1,Section 3 - Distinct counts
# MAGIC %md
# MAGIC ## 3. Counting rows vs. counting distinct values
# MAGIC
# MAGIC These functions answer different questions for each service type:
# MAGIC
# MAGIC | PySpark | SQL | Question |
# MAGIC |---|---|---|
# MAGIC | `F.count("*")` | `COUNT(*)` | Total number of trips in each service type |
# MAGIC | `F.countDistinct("pickup_zone")` | `COUNT(DISTINCT pickup_zone)` | Number of different pickup zones in each service type |

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Equivalent SQL query:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT service_type, COUNT(*) AS trip_count, COUNT(DISTINCT pickup_zone) AS distinct_pickup_zones
# MAGIC FROM trip_enriched
# MAGIC GROUP BY service_type
# MAGIC ORDER BY trip_count DESC;
# MAGIC ```

# COMMAND ----------

trip_enriched.groupBy("service_type").agg(
    F.count("*").alias("trip_count"),
    F.countDistinct("pickup_zone").alias("distinct_pickup_zones"),
).orderBy(F.col("trip_count").desc()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Key takeaways
# MAGIC
# MAGIC - Decide what one result row represents before writing a `groupBy`.
# MAGIC   Predict the number of groups with `.distinct().count()`, then verify it.
# MAGIC - Calculate multiple aggregates with `.agg()` and name them with `.alias()`.
# MAGIC - Grouped results contain grouping keys and aggregate results, not individual
# MAGIC   non-grouped columns such as `trip_id`.
# MAGIC - `count("*")` counts rows; `countDistinct(column)` counts different values.
# MAGIC
# MAGIC **Next:** **`02 - NULL Handling in Aggregations`** — how NULL values and
# MAGIC NULL grouping keys affect counts, sums, and averages.