# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 03 - Your First DataFrame
# MAGIC
# MAGIC Read **13 - Your First DataFrame** in the course Notion hub first. Then
# MAGIC attach classic all-purpose compute and run the cells below.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Build a small rideshare DataFrame from Python rows (no explicit schema)
# MAGIC - Inspect with `show` / `display` / `printSchema`
# MAGIC - Explain why an inferred schema is fine for demos, not for production

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach classic **all-purpose** compute before you run any code cells.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build a small rideshare DataFrame
# MAGIC
# MAGIC In practice, DataFrames are usually created from files or tables.

# COMMAND ----------

trips = [
    (1001, "Standard", 138, 12.4, 18),
    (1002, "Shared", 74, 3.1, 9),
    (1003, "Premium", 231, 22.7, 35),
    (1004, "Standard", 100, 5.6, 14),
]

columns = [
    "trip_id",
    "service_type",
    "pickup_location_id",
    "trip_distance_miles",
    "ride_duration_mins",
]

trips_df = spark.createDataFrame(trips, columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Look at the rows: `show()` vs `display()`
# MAGIC
# MAGIC `show()` is Spark text. `display()` is the Databricks interactive table.

# COMMAND ----------

trips_df.show(truncate=False)

# COMMAND ----------

display(trips_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Look at the structure: `printSchema()`
# MAGIC
# MAGIC Types were inferred. Fine for this demo — not for production.

# COMMAND ----------

trips_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - `spark.createDataFrame(rows, columns)` built the DataFrame.
# MAGIC - `show()` / `display()` show rows; `printSchema()` shows structure.
# MAGIC - Inferred schema is a demo default, not a production schema.
# MAGIC
# MAGIC Next up: Module 2 — DataFrame Fundamentals (`01 - Creating DataFrames`).