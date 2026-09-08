# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Inspecting DataFrames
# MAGIC
# MAGIC Inspect beyond a first look: contents, structure, size, and summary stats.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Inspect contents with `show` options and `display`
# MAGIC - Inspect structure with `printSchema`, `schema`, `columns`, `dtypes`
# MAGIC - Check size with `count` and `isEmpty`; use `describe` / `summary`
# MAGIC - Distinguish metadata checks from methods that run Spark work
# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup DataFrame for inspection
# MAGIC
# MAGIC Create one small DataFrame (for example 3-8 rows) to use throughout the
# MAGIC notebook so every inspection method runs against the same data.
# MAGIC
# MAGIC This sample intentionally includes one suspicious value pattern so you can
# MAGIC see why inspection matters before transformation logic.

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
# MAGIC Before you transform rideshare trips, look at the rows. The sample above
# MAGIC includes at least one suspicious trip (negative distance and a huge ride
# MAGIC duration). Spotting that early is a reliability habit — not just a demo of
# MAGIC print options.
# MAGIC
# MAGIC Use `show()` for a quick text sample and `display()` when you want an
# MAGIC interactive Databricks table.

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Look for the suspicious trip in the output (negative `trip_distance_miles`
# MAGIC and a very large `ride_duration_mins`). In a real pipeline, that is a signal
# MAGIC to investigate or quarantine — not to trust every row blindly.

# COMMAND ----------

df.show(3, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC `show(n)` is a display sample, not a contract for deterministic row order.
# MAGIC If row order matters, add an explicit `orderBy(...)` before inspection.

# COMMAND ----------

df.show(2, vertical=True)

# COMMAND ----------

display(df)  # pyright: ignore[reportUndefinedVariable]  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC `display()` is useful for interactive triage (sort/filter quickly), then
# MAGIC codify what you found in explicit checks so jobs can enforce the same logic.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect structure: `printSchema()`, `schema`, `columns`, `dtypes`
# MAGIC
# MAGIC Use both human-readable and programmatic schema views.

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC `printSchema()` is best for quick reading. The next cell shows the same
# MAGIC schema as a Python object (`StructType`) so code can inspect or reuse it.
# MAGIC
# MAGIC In production, schema inspection is a reliability check: if a key field
# MAGIC type is wrong (for example `double` instead of `decimal`), downstream logic
# MAGIC and quality rules may silently behave differently.

# COMMAND ----------

print(df.schema)

# COMMAND ----------

# MAGIC %md
# MAGIC `columns` and `dtypes` are **metadata** checks. They return ordinary Python
# MAGIC values and do **not** launch a Spark job, so you can validate expected
# MAGIC column names and types cheaply before running expensive actions.

# COMMAND ----------

print("columns:", df.columns)
print("dtypes: ", df.dtypes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect size and emptiness: `count()` and `isEmpty()`
# MAGIC
# MAGIC Compare total-row checks and empty-data checks.
# MAGIC
# MAGIC In production pipelines, this answers operational questions such as:
# MAGIC
# MAGIC - Did ingestion load any rows for this run?
# MAGIC - Should we skip downstream writes for an empty batch?
# MAGIC - Is a filter unexpectedly removing all data?

# COMMAND ----------

print(f"Row count: {df.count()}")

# COMMAND ----------

print(f"Is DataFrame empty? {df.isEmpty()}")

# COMMAND ----------

# MAGIC %md
# MAGIC A filtered DataFrame can become empty even when the original DataFrame is
# MAGIC not. This is a common sanity check before writing or aggregating results.

# COMMAND ----------

empty_df = df.filter("trip_distance_miles > 1000")
print(f"Is filtered DataFrame empty? {empty_df.isEmpty()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## First-pass statistics: `describe()` and `summary()`
# MAGIC
# MAGIC Profile numeric columns for a first-pass review. With this sample, watch
# MAGIC for an impossible minimum distance (negative) and an extreme max ride
# MAGIC duration — the same anomaly you may have spotted in `show()`.
# MAGIC
# MAGIC These methods are good for anomaly discovery, but they are exploratory
# MAGIC checks — not a substitute for explicit data-quality rules that a job can
# MAGIC enforce.

# COMMAND ----------

df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC In the `describe()` output, confirm the anomaly signal: min
# MAGIC `trip_distance_miles` should be negative, and max `ride_duration_mins`
# MAGIC should look unrealistically large. Next action in production is not
# MAGIC “trust the average” — it is investigate, filter, or quarantine bad rows.

# COMMAND ----------

# MAGIC %md
# MAGIC `summary()` can include approximate percentiles (25%, 50%, 75%), which are
# MAGIC often useful for quick distribution checks during exploratory inspection.

# COMMAND ----------

df.summary("count", "min", "25%", "50%", "75%", "max").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance note: metadata checks vs Spark execution
# MAGIC
# MAGIC For day-to-day inspection, separate lightweight schema lookups from methods
# MAGIC that execute Spark work:
# MAGIC
# MAGIC - **Metadata-oriented:** `schema`, `columns`, `dtypes`, `printSchema()`
# MAGIC - **Execute Spark work:** `show()`, `count()`, `isEmpty()`, `describe()`,
# MAGIC   `summary()`
# MAGIC
# MAGIC Practical production tips:
# MAGIC
# MAGIC - If you only need to know whether data exists, use `isEmpty()` rather than
# MAGIC   `count() == 0`.
# MAGIC - Avoid repeatedly running full-table inspections in the same notebook run.
# MAGIC - For large DataFrames, profile only the columns and statistics you need.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC Use a second small rideshare-style DataFrame named `my_df` and complete:
# MAGIC
# MAGIC 1. Show rows with one `show(...)` call and `display(my_df)`.
# MAGIC 2. Inspect structure with `printSchema()`, `schema`, and `dtypes`.
# MAGIC 3. Run `count()` and `isEmpty()`.
# MAGIC 4. Run one stats method (`describe()` or `summary()`).
# MAGIC 5. Write one short note: which method you used that triggers Spark work
# MAGIC    and which method you used that is metadata-oriented.

# COMMAND ----------

# Your code here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Recap this notebook's inspection path:
# MAGIC
# MAGIC - row content inspection (`show`, `display`)
# MAGIC - structure inspection (`printSchema`, `schema`, `columns`, `dtypes`)
# MAGIC - size/emptiness checks (`count`, `isEmpty`)
# MAGIC - quick statistical review (`describe`, `summary`)
# MAGIC - practical performance awareness (metadata lookups vs Spark execution)
# MAGIC
# MAGIC Next up: `03 - Selecting and Transforming Columns`.
