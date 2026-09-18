# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Querying DataFrames with SQL
# MAGIC
# MAGIC Query a DataFrame through a session temporary view with `%sql` and
# MAGIC `spark.sql`.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Explain why `%sql` and `spark.sql` cannot see a Python DataFrame variable
# MAGIC - Register a session temporary view with `createOrReplaceTempView`
# MAGIC - Query that view with `%sql` and with `spark.sql`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach classic **all-purpose** compute, 
# MAGIC Global temporary views are supported on classic compute, not serverless.

# COMMAND ----------

from decimal import Decimal

from pyspark.sql import functions as F

rows = [
    (1001, "Standard", 138, Decimal("12.40"), 18),
    (1002, "Shared", 74, Decimal("3.10"), 9),
    (1003, "Premium", 231, Decimal("22.70"), 35),
    (1004, "Standard", 100, Decimal("5.60"), 14),
    (1005, "Shared", 74, Decimal("2.20"), 7),
]

schema_ddl = (
    "trip_id bigint, service_type string, pickup_location_id int, "
    "trip_distance_miles decimal(8,2), ride_duration_mins int"
)

df = spark.createDataFrame(rows, schema_ddl)  # pyright: ignore[reportUndefinedVariable]  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC Confirm the sample rows.

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why `%sql` cannot see a Python DataFrame variable
# MAGIC
# MAGIC **`%sql`** and **`spark.sql(...)`** look up table and view names, not Python
# MAGIC variables.

# COMMAND ----------

try:
    spark.sql("SELECT trip_id FROM df").show()  # noqa: F821
except Exception as e:
    print(f"{type(e).__name__} — SQL has no table or view named df")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register a session temporary view
# MAGIC
# MAGIC **`createOrReplaceTempView`** gives the DataFrame a SQL name for this Spark
# MAGIC session.
# MAGIC
# MAGIC **Business question:** A SQL dashboard in this notebook needs the trip data
# MAGIC queryable under the name **`trips`**.

# COMMAND ----------

df.createOrReplaceTempView("trips")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query with `%sql`
# MAGIC
# MAGIC **Business question:** A mid-range trip report needs service types and
# MAGIC distances for trips between 3 and 15 miles.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT service_type, trip_distance_miles
# MAGIC FROM trips
# MAGIC WHERE trip_distance_miles BETWEEN 3 AND 15

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query with `spark.sql(...)`
# MAGIC
# MAGIC Use **`%sql`** when the whole cell is SQL. Use **`spark.sql(...)`** when
# MAGIC Python needs a DataFrame back.
# MAGIC
# MAGIC **Business question:** Downstream Python code needs long trips from SQL, then
# MAGIC a further DataFrame filter for Shared service type only.

# COMMAND ----------

long_trips = spark.sql(  # noqa: F821
    "SELECT trip_id, service_type, trip_distance_miles FROM trips WHERE trip_distance_miles > 10"
)

long_trips.filter(F.col("service_type") == "Shared").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Global temporary views (`global_temp`)
# MAGIC
# MAGIC `createOrReplaceGlobalTempView` registers a global temporary view that other sessions on the same classic cluster can query.
# MAGIC
# MAGIC A session temporary view is available only in the current Spark session.
# MAGIC
# MAGIC Global temporary views require **classic compute** and are not supported on serverless.
# MAGIC

# COMMAND ----------

 df.createOrReplaceGlobalTempView("trips_global")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS trip_count FROM global_temp.trips_global

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - **`%sql`** / **`spark.sql`** — view names, not Python variables like **`df`**
# MAGIC - **`createOrReplaceTempView`** — session SQL name **`trips`**
# MAGIC - **`%sql`** — SQL cell; **`spark.sql`** — SQL from Python, DataFrame back
# MAGIC - **`global_temp`** — classic compute; prefer session views
# MAGIC
# MAGIC Next up: **Module 3 — Data Cleaning, NULL Semantics, and Type Handling**.