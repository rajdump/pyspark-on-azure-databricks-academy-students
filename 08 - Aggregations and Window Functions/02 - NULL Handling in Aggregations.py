# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02 - NULL Handling in Aggregations
# MAGIC
# MAGIC This notebook shows how NULL values affect `groupBy()` results in two ways:
# MAGIC
# MAGIC - **When grouping:** Rows with NULL values are grouped together
# MAGIC   into one group, shown as `null` in the result.
# MAGIC - **When aggregating:** Functions such as `count(column)`,
# MAGIC   `sum()`, and `avg()` ignore NULL values when calculating results.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Compare the results of `count("*")`, `count(column)`, and `countDistinct(column)` when NULL values are present.
# MAGIC - Explain why `groupBy()` treats NULL as a separate group, while `countDistinct()` ignores NULL values.
# MAGIC - Explain why `sum()`, `avg()`, `min()`, and `max()` skip NULL values, and why they return NULL when every value in a group is NULL.
# MAGIC - Identify when to replace NULL values with zero before aggregation and how this affects averages.

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach **all-purpose compute**.
# MAGIC
# MAGIC This notebook uses the `trip_enriched` table created in Module 7 (Notebook `07`). It contains **one row per trip**: a total of 106 trips across 16 columns.

# COMMAND ----------

from pyspark.sql import functions as F

trip_enriched_table = "rideshare_dev.processed.trip_enriched"

trip_enriched = spark.table(trip_enriched_table)  # noqa: F821

print("trip_enriched rows:", trip_enriched.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Counting with NULL values
# MAGIC Spark provides three counting functions that handle NULL values differently.
# MAGIC
# MAGIC - `count("*")` counts all rows, including those containing NULL values.
# MAGIC - `count(column)` counts only non-NULL values in the specified column.
# MAGIC - `countDistinct(column)` counts distinct non-NULL values in the specified column.

# COMMAND ----------

trip_enriched.select(
    F.count("*").alias("rows"),
    F.count("trip_date").alias("trip_date"),
    F.countDistinct("trip_date").alias("distinct_trip_dates"),
    F.count("payment_method").alias("payment_method"),
    F.count("base_fare_amount").alias("base_fare"),
    F.count("tip_amount").alias("tip"),
    F.count("trip_distance_miles").alias("distance"),
    F.count("ride_duration_mins").alias("duration"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Now apply the same three counts to each service type:
# MAGIC
# MAGIC - How many trips belong to each service type? (`trip_count`)
# MAGIC - How many of those trips have a recorded trip date? (`dated_trip_count`)
# MAGIC - On how many distinct dates did those trips occur? (`distinct_dates`)

# COMMAND ----------

trip_enriched.groupBy("service_type").agg(
    F.count("*").alias("trip_count"),
    F.count("trip_date").alias("dated_trip_count"),
    F.countDistinct("trip_date").alias("distinct_dates"),
).orderBy(F.col("trip_count").desc()).show()

# COMMAND ----------

# DBTITLE 1,Section 2 - NULL grouping keys
# MAGIC %md
# MAGIC ## 2. NULL grouping keys — `groupBy()` vs. `countDistinct()`
# MAGIC
# MAGIC When a grouping column contains NULL values, `groupBy()` places all rows with NULL into a separate group. However, `countDistinct()` ignores NULL values.

# COMMAND ----------

# 1. Display every payment_method group (watch for a NULL row)
trip_enriched.groupBy("payment_method").agg(
    F.count("*").alias("trip_count"),
).orderBy(F.col("trip_count").desc()).show()

# COMMAND ----------

# 2. Distinct values — countDistinct skips NULL
trip_enriched.select(
    F.countDistinct("payment_method").alias("distinct_payment_method"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC `groupBy()` produces 6 groups, while `countDistinct()` returns 5 because it excludes NULL values.
# MAGIC
# MAGIC `unknown` and `NULL` are different: unknown is a text value, so `countDistinct()` includes it. `NULL` represents a missing value, so it is excluded.

# COMMAND ----------

# DBTITLE 1,Section 3 - Aggregates and NULL values
# MAGIC %md
# MAGIC ## 3. NULL values in aggregate functions
# MAGIC
# MAGIC Aggregate functions such as sum(), avg(), min(), and max() ignore NULL values and calculate results using only non-NULL values.

# COMMAND ----------

trip_enriched.select(
    F.count("*").alias("all_trips"),
    F.count("tip_amount").alias("trips_with_tip"),
    F.sum("tip_amount").alias("total_tip"),
    F.avg("tip_amount").alias("avg_tip_non_null"),
    # Round to 6 decimal places to match the avg output scale.
    F.round(F.sum("tip_amount") / F.count("*"), 6).alias("avg_tip_all_trips"),
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Replacing NULL with zero using `coalesce()`
# MAGIC
# MAGIC This is equivalent to the SQL query:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS all_trips,
# MAGIC     COUNT(COALESCE(tip_amount, CAST(0 AS DECIMAL(10, 2)))) AS trips_with_tip,
# MAGIC     SUM(COALESCE(tip_amount, CAST(0 AS DECIMAL(10, 2)))) AS total_tip,
# MAGIC     AVG(COALESCE(tip_amount, CAST(0 AS DECIMAL(10, 2)))) AS avg_tip_all_trips
# MAGIC FROM trip_enriched;
# MAGIC ```

# COMMAND ----------

tip_or_zero = F.coalesce(F.col("tip_amount"), F.lit(0).cast("decimal(10,2)"))

trip_enriched.select(
    F.count("*").alias("all_trips"),
    F.count(tip_or_zero).alias("trips_with_tip"),
    F.sum(tip_or_zero).alias("total_tip"),
    F.avg(tip_or_zero).alias("avg_tip_all_trips"),
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Edge case — When all values in a group are NULL
# MAGIC `trip_enriched` has no group where every value is NULL, so this small example creates one: both `cash` fares are NULL.

# COMMAND ----------

edge_case_df = spark.createDataFrame(  # noqa: F821
    [("card", 25.0), ("card", 30.0), ("cash", None), ("cash", None)],
    "payment_method string, base_fare_amount double",
)

edge_case_df.groupBy("payment_method").agg(
    F.count("*").alias("trips"),
    F.count("base_fare_amount").alias("known_fares"),
    F.sum("base_fare_amount").alias("total_fare"),
    F.avg("base_fare_amount").alias("avg_fare"),
    F.min("base_fare_amount").alias("min_fare"),
    F.max("base_fare_amount").alias("max_fare"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC When every value in a group is NULL, aggregate functions such as `sum`, `avg`, `min`, and `max` return NULL, not zero.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC - `count("*")` counts all rows, `count(column)` counts non-NULL values, and `countDistinct(column)` counts distinct non-NULL values.
# MAGIC - `groupBy()` creates a separate group for NULL values, while `countDistinct()` excludes NULL values.
# MAGIC - `sum()`, `avg()`, `min()`, and `max()` skip NULL values and return NULL when every value in a group is NULL.
# MAGIC - Replace NULL with zero before averaging only when zero matches the business meaning.
# MAGIC
# MAGIC **Next:** **`03 - Filter Placement with WHERE and HAVING`** — how filtering
# MAGIC rows before aggregation differs from filtering groups after it.