# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Inspecting DataFrames
# MAGIC
# MAGIC Read **Inspecting DataFrames** in the course Notion hub first. Then attach
# MAGIC classic all-purpose compute and run the cells below.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Inspect contents with `show` options and `display`
# MAGIC - Inspect structure with `printSchema`, `schema`, `columns`, `dtypes`
# MAGIC - Check size with `count` and `isEmpty`; use `describe` / `summary`
# MAGIC - Distinguish metadata checks from methods that run Spark work

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach classic **all-purpose** compute before you run any code cells. One trip is intentionally bad — look for it as you inspect.

# COMMAND ----------

from decimal import Decimal

rows = [
    (1001, "Standard", 138, Decimal("12.40"), 18),
    (1002, "Shared", 74, Decimal("3.10"), 9),
    (1003, "Premium", 231, Decimal("22.70"), 35),
    (1004, "Standard", 100, Decimal("-4.00"), 30000),
    (1005, "Shared", 74, Decimal("2.20"), 7),
]

schema_ddl = (
    "trip_id bigint, service_type string, pickup_location_id int, "
    "trip_distance_miles decimal(8,2), ride_duration_mins int"
)

df = spark.createDataFrame(rows, schema_ddl)  # pyright: ignore[reportUndefinedVariable]  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect contents: `show()` and `display()`
# MAGIC
# MAGIC Find the negative `trip_distance_miles` and the huge `ride_duration_mins`.

# COMMAND ----------

df.show()

# COMMAND ----------

df.show(3, truncate=False)

# COMMAND ----------

df.show(2, vertical=True)

# COMMAND ----------

display(df)  # pyright: ignore[reportUndefinedVariable]  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect structure: `printSchema()`, `schema`, `columns`, `dtypes`
# MAGIC
# MAGIC `columns` and `dtypes` are metadata. They do not start a Spark job.

# COMMAND ----------

df.printSchema()

# COMMAND ----------

print(df.schema)

# COMMAND ----------

print("columns:", df.columns)
print("dtypes: ", df.dtypes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect size: `count()` and `isEmpty()`

# COMMAND ----------

print(f"Row count: {df.count()}")

# COMMAND ----------

print(f"Is DataFrame empty? {df.isEmpty()}")

# COMMAND ----------

# MAGIC %md
# MAGIC A filter can make a DataFrame empty even when the source is not.

# COMMAND ----------

empty_df = df.filter("trip_distance_miles > 1000")
print(f"Is filtered DataFrame empty? {empty_df.isEmpty()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect statistics: `describe()` and `summary()`
# MAGIC
# MAGIC Confirm min distance is negative and max duration is huge.

# COMMAND ----------

df.describe().show()

# COMMAND ----------

df.summary("count", "min", "25%", "50%", "75%", "max").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - Rows: `show` / `display`. Structure: `printSchema` / `schema` / `columns` /
# MAGIC   `dtypes`. Size: `count` / `isEmpty`. Profile: `describe` / `summary`.
# MAGIC - `columns` and `dtypes` are cheap. The rest run Spark work.
# MAGIC
# MAGIC Next up: `03 - Selecting and Transforming Columns`.