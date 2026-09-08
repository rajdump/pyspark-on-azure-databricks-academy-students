# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Querying DataFrames with SQL
# MAGIC
# MAGIC Query a DataFrame through temp views and Spark SQL — including when
# MAGIC side-by-side APIs are the learning objective.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Express the same calculated column via `F.when`, `F.expr`, and `selectExpr`
# MAGIC - Register a session temporary view; query with `%sql` and `spark.sql`
# MAGIC - Recognize global temporary views on classic compute (not serverless)
# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup DataFrame for SQL query examples
# MAGIC
# MAGIC Notebook 05 kept rows with **`filter`** / **`where`**. To query the same
# MAGIC data with **`%sql`** or **`spark.sql`**, Spark needs a **SQL name** for the
# MAGIC DataFrame — a temporary view — not just a Python variable.
# MAGIC
# MAGIC Create one small DataFrame to reuse across every example. Column names and
# MAGIC types match the `trip` table; file-based reads begin in Module 5.

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

distance_band_expr = (
    F.when(F.col("trip_distance_miles") < 5, "short")
    .when(F.col("trip_distance_miles") <= 15, "medium")
    .otherwise("long")
)

distance_band_sql = """
CASE
    WHEN trip_distance_miles < 5 THEN 'short'
    WHEN trip_distance_miles <= 15 THEN 'medium'
    ELSE 'long'
END AS distance_band
"""

# COMMAND ----------

# MAGIC %md
# MAGIC Confirm the sample rows before SQL query examples — the same habit as
# MAGIC inspection in the previous notebook.

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Same derived column three ways
# MAGIC
# MAGIC **Business question:** Trip reporting needs distance-band labels — `short`,
# MAGIC `medium`, and `long` — on each trip row.
# MAGIC
# MAGIC Notebooks 03–04 already built this band with **`F.when`**, **`F.expr`**, and
# MAGIC **`selectExpr`**. The same calculation can start from one Python DataFrame
# MAGIC variable — no temp view required yet.

# COMMAND ----------

# MAGIC %md
# MAGIC ### With `F.when` / `otherwise`

# COMMAND ----------

df.select("trip_id", distance_band_expr.alias("distance_band")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### With `F.expr` (SQL `CASE WHEN` string)

# COMMAND ----------

df.select("trip_id", F.expr(distance_band_sql)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### With `selectExpr`

# COMMAND ----------

df.selectExpr("trip_id", distance_band_sql).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why `%sql` cannot see a Python DataFrame variable
# MAGIC
# MAGIC A **`%sql`** cell (and **`spark.sql(...)`**) resolves **SQL table and view
# MAGIC names** in the Spark catalog — not Python variable names. The Python name
# MAGIC **`df`** is invisible to SQL until you register a view.
# MAGIC
# MAGIC The next cell runs SQL **`FROM df`** before any view exists. Spark looks for a
# MAGIC table or view named **`df`** and raises an error — the same name resolution a
# MAGIC **`%sql`** cell would use.

# COMMAND ----------

try:
    spark.sql("SELECT trip_id FROM df").show()  # noqa: F821
except Exception as e:
    print(f"{type(e).__name__} — SQL has no table or view named df")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register a session temporary view
# MAGIC
# MAGIC **`createOrReplaceTempView(name)`** registers the DataFrame under a SQL name
# MAGIC for this Spark session. It does not copy rows or write a persisted table.
# MAGIC
# MAGIC **Business question:** A SQL dashboard in this notebook needs the trip data
# MAGIC queryable under the name **`trips`**.
# MAGIC
# MAGIC The Python variable stays **`df`**; the SQL name is **`trips`**. Running the
# MAGIC cell again replaces the existing **`trips`** view if you rebuild **`df`**.

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
# MAGIC The same filter with the DataFrame API on **`df`** — SQL used the view name
# MAGIC **`trips`**; Python still uses **`df`**:

# COMMAND ----------

df.select("service_type", "trip_distance_miles").filter(
    F.col("trip_distance_miles").between(3, 15)
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query with `spark.sql(...)`
# MAGIC
# MAGIC Use a **`%sql`** cell when the whole cell is SQL. Use **`spark.sql(...)`**
# MAGIC when a Python cell needs to run SQL and keep working in Python afterward.
# MAGIC
# MAGIC **`spark.sql(sql_text)`** returns a **DataFrame**.
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
# MAGIC **Business question:** Legacy pipelines on classic compute sometimes share
# MAGIC temporary data across notebooks attached to the same cluster.
# MAGIC
# MAGIC A **global temporary view** is registered with
# MAGIC **`createOrReplaceGlobalTempView`** and queried as **`global_temp.view_name`**.
# MAGIC This course prefers **session** temp views for new work; recognize
# MAGIC **`global_temp`** when you read existing code.
# MAGIC
# MAGIC > **Good to know:** Global temp views require **classic** all-purpose compute
# MAGIC > — not serverless. On unsupported compute, register or query may fail; the
# MAGIC > pattern below uses **`try`** / **`except`** so the rest of the notebook still
# MAGIC > runs.

# COMMAND ----------

try:
    df.createOrReplaceGlobalTempView("trips_global")
    spark.sql("SELECT COUNT(*) AS trip_count FROM global_temp.trips_global").show()  # noqa: F821
except Exception as e:
    print(f"{type(e).__name__}: {e}")
    print("Global temp views need classic all-purpose compute — not serverless.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Session view vs global view vs persisted table
# MAGIC
# MAGIC - **Session temp view** — current Spark session; this session only. Use to
# MAGIC   mix Python and SQL on the same in-memory data.
# MAGIC - **Global temp view** — until the cluster stops; **classic** compute,
# MAGIC   cross-session. Legacy code only; not serverless.
# MAGIC - **Persisted table** — survives session end; governed storage in later
# MAGIC   modules. Use when production jobs and other users must read the data later.
# MAGIC
# MAGIC This notebook stops at temporary views. Writing and governing tables comes in
# MAGIC later modules.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC Use a second small rideshare-style DataFrame named `my_df` and complete:
# MAGIC
# MAGIC 1. Create `my_df` with explicit `trip`-aligned column names and types.
# MAGIC 2. Register it as a session temporary view with a SQL name you choose.
# MAGIC 3. Query the view with **`%sql`** or **`spark.sql(...)`**.
# MAGIC 4. Show the result.
# MAGIC
# MAGIC Keep the DataFrame tiny (a handful of rows).

# COMMAND ----------

# Your code here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Recap this notebook's SQL query path:
# MAGIC
# MAGIC - **`F.when`**, **`F.expr`**, **`selectExpr`** — same derived column from a
# MAGIC   Python DataFrame without a temp view
# MAGIC - **`%sql`** / **`spark.sql`** — resolve **view/table names**, not Python
# MAGIC   variables like **`df`**
# MAGIC - **`createOrReplaceTempView`** — register a session SQL name (here,
# MAGIC   **`trips`**) for the same data as **`df`**
# MAGIC - **`%sql`** — run a full SQL cell; **`spark.sql`** — run SQL from Python and
# MAGIC   chain DataFrame methods on the result
# MAGIC - **`global_temp`** — recognize on classic compute; prefer session views for
# MAGIC   new work
# MAGIC - **Persisted tables** — for data that must outlive the session (later modules)
# MAGIC
# MAGIC Next up: **Module 3 — Data Cleaning, NULL Semantics, and Type Handling** —
# MAGIC three-valued logic and NULL-safe predicates on hand-built DataFrames,
# MAGIC then messy values, safe casting, and parsing.
