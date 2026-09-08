# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Your First DataFrame
# MAGIC
# MAGIC First DataFrame from in-notebook Python rows — not file reads from
# MAGIC `data/raw/`.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Build a small rideshare DataFrame from Python rows (no explicit schema)
# MAGIC - Inspect with `show` / `display` / `printSchema`
# MAGIC - Explain why an inferred schema is fine for demos, not for production
# COMMAND ----------

# MAGIC %md
# MAGIC ## Build a small rideshare DataFrame
# MAGIC
# MAGIC In production pipelines, DataFrames usually come from files or tables. For
# MAGIC a first step, create one directly from Python rows so you can focus on the
# MAGIC DataFrame itself — not schemas or file formats.
# MAGIC
# MAGIC `spark.createDataFrame(rows, columns)` turns a list of tuples into a
# MAGIC DataFrame and names the columns. The names match the course `trip` table
# MAGIC so the habit carries over later.

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
# MAGIC Before you transform data, look at it. Two common ways to view rows:
# MAGIC
# MAGIC | Method | What you get | When to use it |
# MAGIC |---|---|---|
# MAGIC | `show()` | Plain-text table in the cell output | Quick check; works in Spark anywhere |
# MAGIC | `display()` | Interactive Databricks table (sort, filter, chart) | Exploring in the notebook UI |
# MAGIC
# MAGIC `show()` is core Spark. `display()` is Databricks-only and better for
# MAGIC interactive exploration.

# COMMAND ----------

trips_df.show(truncate=False)

# COMMAND ----------

display(trips_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Look at the structure: `printSchema()`
# MAGIC
# MAGIC A DataFrame has rows **and** a structure: column names, types, and
# MAGIC nullability. `printSchema()` prints that structure.
# MAGIC
# MAGIC You did not define types yourself. Spark **inferred** them from the Python
# MAGIC values — this is the **default (inferred) schema**. Convenient for a first
# MAGIC look, but **not recommended for production**. Inference reflects the sample
# MAGIC values you passed in, not a deliberate data model (for example, whole
# MAGIC numbers may become `long` when your real table expects `int`). Later modules
# MAGIC cover explicit schemas when you need controlled types.

# COMMAND ----------

trips_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC Create a second small DataFrame named `my_trips_df` the same simple way
# MAGIC (rows + column names — no schema object):
# MAGIC
# MAGIC 1. Add at least 3 rows of your own rideshare-style values.
# MAGIC 2. Run `my_trips_df.show(truncate=False)`.
# MAGIC 3. Run `display(my_trips_df)`.
# MAGIC 4. Run `my_trips_df.printSchema()` and note that the types are inferred.

# COMMAND ----------

# Your code here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC You created and inspected your first DataFrame by:
# MAGIC
# MAGIC - Building rows in Python and naming columns with
# MAGIC   `spark.createDataFrame(rows, columns)`
# MAGIC - Viewing values with `show()` (quick text) and `display()` (interactive
# MAGIC   Databricks table)
# MAGIC - Checking structure with `printSchema()` — seeing Spark's **default
# MAGIC   inferred schema**, which is fine for demos but not recommended for
# MAGIC   production pipelines
# MAGIC
# MAGIC **Next:** Module 2 — DataFrame Fundamentals (`01 - Creating DataFrames`).
