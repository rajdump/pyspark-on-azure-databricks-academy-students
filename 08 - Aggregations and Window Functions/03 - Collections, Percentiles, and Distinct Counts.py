# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Collections, Percentiles, and Distinct Counts
# MAGIC
# MAGIC Collection aggregates, percentiles, and distinct counts.
# MAGIC
# MAGIC `trip_enriched`, `trip_driver_assignment`.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Use advanced aggregates (`collect_list` / `collect_set`, approximate
# MAGIC   percentiles, `countDistinct`)
# COMMAND ----------

# MAGIC %md
# MAGIC `groupBy` with `count`, `sum`, and `avg` covers many common aggregation needs.
# MAGIC This notebook adds `collect_list` / `collect_set`, `percentile_approx`, and
# MAGIC `countDistinct`.
# MAGIC
# MAGIC ## What this notebook teaches
# MAGIC
# MAGIC | Section | Concept | Why it matters |
# MAGIC |---|---|---|
# MAGIC | 1 | `collect_list` / `collect_set` | Build a driver profile showing all service types handled, or only the unique service types. |
# MAGIC | 2 | `avg` vs `percentile_approx` (p50 / p90) | Compare average trip distance with median and upper-range distance thresholds. |
# MAGIC | 3 | `countDistinct` | Count how many unique pickup-to-drop-off routes appear. |
# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC | DataFrame | Grain | Used for |
# MAGIC |---|---|---|
# MAGIC | `trip_driver_assignment` | One driver-trip assignment | Section 1 (collections) |
# MAGIC | `trip_enriched` | One trip | Sections 2–3 |

# COMMAND ----------

from pyspark.sql import functions as F

trip_enriched_table = "rideshare_dev.processed.trip_enriched"
trip_driver_assignment_table = "rideshare_dev.processed.trip_driver_assignment"

trip_enriched = spark.table(trip_enriched_table)  # noqa: F821
trip_driver_assignment = spark.table(trip_driver_assignment_table)  # noqa: F821

# COMMAND ----------

# DBTITLE 1,Which services did each driver handle?
# MAGIC %md
# MAGIC ## 1. Collect grouped values
# MAGIC
# MAGIC ### Which service types did each driver handle — all, or only unique?
# MAGIC
# MAGIC `collect_list` keeps repeats. `collect_set` keeps unique values only.
# MAGIC `sort_array` is for readable display, not trip order.

# COMMAND ----------

driver_service_collections = trip_driver_assignment.groupBy("driver_id").agg(
    F.sort_array(F.collect_list(F.col("service_type"))).alias("all_service_types"),
    F.sort_array(F.collect_set(F.col("service_type"))).alias("unique_service_types"),
)

driver_service_collections.orderBy("driver_id").show(12, truncate=False)

# COMMAND ----------

# DBTITLE 1,Which payment methods appear on STANDARD trips?
# MAGIC %md
# MAGIC ### Which payment methods appear on STANDARD trips?
# MAGIC
# MAGIC Build the payment-method list and unique set for `STANDARD`. Trip count can
# MAGIC be higher than the collected list size when a trip has no payment method —
# MAGIC `collect_list` and `collect_set` skip NULLs.

# COMMAND ----------

standard_payment_collections = trip_enriched.filter(F.col("service_type") == "STANDARD").agg(
    F.count("*").alias("standard_trip_count"),
    F.count(F.col("payment_method")).alias("known_payment_method_count"),
    F.size(F.collect_list(F.col("payment_method"))).alias("collected_list_size"),
    F.sort_array(F.collect_set(F.col("payment_method"))).alias("unique_payment_methods"),
)

standard_payment_collections.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Typical distance and upper-range threshold
# MAGIC %md
# MAGIC ## 2. Calculate percentiles
# MAGIC
# MAGIC ### What is a typical trip distance, and where does the upper range begin?
# MAGIC
# MAGIC Compare `avg` with approximate p50 (typical) and p90 (upper-range threshold).

# COMMAND ----------

trip_enriched.agg(
    F.round(F.avg(F.col("trip_distance_miles")), 2).alias("avg_distance_miles"),
    F.percentile_approx(F.col("trip_distance_miles"), 0.50).alias("p50_distance_miles"),
    F.percentile_approx(F.col("trip_distance_miles"), 0.90).alias("p90_distance_miles"),
).show()

# COMMAND ----------

# DBTITLE 1,Distance patterns by service type
# MAGIC %md
# MAGIC ### How do trip-distance patterns differ by service type?
# MAGIC
# MAGIC For each service type, calculate average, median (p50), and upper-range (p90)
# MAGIC trip distance.

# COMMAND ----------

service_distance_percentiles = trip_enriched.groupBy("service_type").agg(
    F.count(F.col("trip_distance_miles")).alias("known_distance_count"),
    F.round(F.avg(F.col("trip_distance_miles")), 2).alias("avg_distance_miles"),
    F.percentile_approx(F.col("trip_distance_miles"), 0.50).alias("p50_distance_miles"),
    F.percentile_approx(F.col("trip_distance_miles"), 0.90).alias("p90_distance_miles"),
)

service_distance_percentiles.orderBy("service_type").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Identify how many unique pickup-to-drop-off routes appear?
# MAGIC %md
# MAGIC ## 3. Count distinct values
# MAGIC
# MAGIC ### Identify how many unique pickup-to-drop-off routes appear?
# MAGIC
# MAGIC `countDistinct` on pickup and drop-off locations returns the unique route count.

# COMMAND ----------

trip_enriched.agg(
    F.countDistinct(
        F.col("pickup_location_id"),
        F.col("dropoff_location_id"),
    ).alias("unique_route_count"),
).show()

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Pattern | What it is used for |
# MAGIC |---|---|
# MAGIC | `collect_list` / `collect_set` | Per-group arrays — repeats vs unique values (NULLs skipped) |
# MAGIC | `percentile_approx` | p50 / p90 thresholds alongside `avg` |
# MAGIC | `countDistinct` | Unique / distinct counts |
# MAGIC
# MAGIC **Next:** **`04 - Pivot`** — reshape grouped results with `pivot`.
