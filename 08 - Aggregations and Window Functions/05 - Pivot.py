# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 05 - Pivot
# MAGIC
# MAGIC In earlier lessons, you used `groupBy()` with aggregate functions to summarize data
# MAGIC and with collection functions to gather values into arrays.
# MAGIC
# MAGIC In this lesson, you'll learn how to use `groupBy()`, `pivot()`, and `agg()` together
# MAGIC to create a summary table:
# MAGIC
# MAGIC - `groupBy()` puts rows with the same value into one group.
# MAGIC - `pivot()` turns each distinct value of a column into its own column.
# MAGIC - `agg()` applies an aggregate function to calculate a summary for each cell.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC - Turn the values of a column into separate columns with `pivot()`.
# MAGIC - Choose a column that is suitable to pivot.
# MAGIC - Understand when pivoting is useful and when it should be avoided.

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach **all-purpose compute**.
# MAGIC
# MAGIC This notebook uses `rideshare_dev.processed.trip_enriched`, created in
# MAGIC Module 07. Each row represents one trip, and the table contains 106 trips.

# COMMAND ----------

from pyspark.sql import functions as F

trip_enriched_table = "rideshare_dev.processed.trip_enriched"
trip_enriched = spark.table(trip_enriched_table)  # noqa: F821

# COMMAND ----------

# DBTITLE 1,Section 1 - Count trips by borough and service type
# MAGIC %md
# MAGIC ## 1. Number of trips by borough and service type
# MAGIC
# MAGIC **Question:** How many trips of each service type are there in each pickup borough?

# COMMAND ----------

trip_enriched.groupBy("pickup_borough", "service_type") \
    .agg(F.count("trip_id").alias("trip_count")) \
    .orderBy("pickup_borough", "service_type") \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC There are **18 combinations of boroughs and service types**. 

# COMMAND ----------

# DBTITLE 1,Section 2 - Turn service types into columns
# MAGIC %md
# MAGIC ## 2. Turn service types into columns with `pivot()`
# MAGIC
# MAGIC **Question:** Can each borough appear on one row, with a separate column for each
# MAGIC service type?
# MAGIC
# MAGIC This is equivalent to the SQL query:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC     SELECT pickup_borough, service_type, trip_idFROM trip_enriched
# MAGIC )
# MAGIC PIVOT (
# MAGIC     COUNT(trip_id)
# MAGIC     FOR service_type IN ('STANDARD','PREMIUM','XL','SHARED','UNKNOWN')
# MAGIC )
# MAGIC ORDER BY pickup_borough;
# MAGIC ```
# MAGIC
# MAGIC In this example:
# MAGIC
# MAGIC - `groupBy("pickup_borough")` creates one row for each borough.
# MAGIC - `pivot("service_type", service_types)` creates one column for each service type.
# MAGIC - `agg(F.count("trip_id"))` fills each cell with the number of trips.
# MAGIC
# MAGIC The `service_types` list sets which columns appear and in what order. Without it,
# MAGIC Spark scans the data first to find the distinct values.

# COMMAND ----------

service_types = ["STANDARD", "PREMIUM", "XL", "SHARED", "UNKNOWN"]

trips_by_borough_wide = (
    trip_enriched.groupBy("pickup_borough")
    .pivot("service_type", service_types)
    .agg(F.count("trip_id"))
)

trips_by_borough_wide.orderBy("pickup_borough").show()

# COMMAND ----------

# DBTITLE 1,Section 3 - When not to use pivot
# MAGIC %md
# MAGIC ## 3. Choose a column to pivot
# MAGIC
# MAGIC Each distinct value of the pivot column becomes a separate output column. If the
# MAGIC column has many distinct values, the result becomes very wide.

# COMMAND ----------

print("service_type distinct:", trip_enriched.select("service_type").distinct().count())
print("trip_id distinct:", trip_enriched.select("trip_id").distinct().count())

# COMMAND ----------

# DBTITLE 1,Key Takeaways
# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC - `groupBy()` with multiple columns returns one row per combination that exists in the data.
# MAGIC - `pivot()` turns each distinct value of a column into its own column. It changes the
# MAGIC   layout, not the counts.
# MAGIC - `agg()` calculates the value for each cell.
# MAGIC - A `NULL` cell means that combination doesn't appear in the data, not a count of 0.
# MAGIC - Pass the pivot values explicitly to control the output columns, and avoid pivoting
# MAGIC   columns with many distinct values.
# MAGIC
# MAGIC **Next:** `06 - Window Functions Fundamentals`