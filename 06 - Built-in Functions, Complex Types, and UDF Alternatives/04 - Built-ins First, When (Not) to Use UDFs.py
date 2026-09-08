# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Built-ins First, When (Not) to Use UDFs
# MAGIC
# MAGIC Built-ins as default; Python UDF contrast only — do not overwrite curated
# MAGIC outputs.
# MAGIC
# MAGIC A small column-rule demo (not a curated overwrite).
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Explain when to prefer built-ins over Python UDFs (and when Pandas/Arrow UDFs
# MAGIC   exist as an advanced fallback outside this course)
# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Read the curated `trip` and `payment` outputs written in Module 6
# MAGIC **`03 - Cleaning and Curated Outputs`**. Use `format("parquet").load(...)` — the
# MAGIC same recommended read pattern as Notebook 03 — rather than the `.parquet(path)`
# MAGIC shorthand.
# MAGIC
# MAGIC | Dataset | Source | Grain |
# MAGIC |---|---|---|
# MAGIC | `trip` | `…/curated/trip/` | 106 rows; one row per `trip_id` |
# MAGIC | `payment` | `…/curated/payment/` | 105 rows; one row per `trip_id` |
# MAGIC
# MAGIC This notebook's demo rule uses `payment`'s `tip_percent_of_base`; the exercise
# MAGIC uses `trip`'s `trip_distance_km`. Both columns were created in Notebook 03.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

curated_root = "/Volumes/rideshare_dev/processed/output_files/curated"
curated_trip_path = f"{curated_root}/trip"
curated_payment_path = f"{curated_root}/payment"

print(f"curated_trip_path = {curated_trip_path}")
print(f"curated_payment_path = {curated_payment_path}")

# COMMAND ----------

trip_curated = spark.read.format(  # pyright: ignore[reportUndefinedVariable]  # noqa: F821
    "parquet"
).load(curated_trip_path)
payment_curated = spark.read.format(  # pyright: ignore[reportUndefinedVariable]  # noqa: F821
    "parquet"
).load(curated_payment_path)

print(f"Curated trip rows: {trip_curated.count()}")
print(f"Curated payment rows: {payment_curated.count()}")

payment_curated.select(
    F.col("trip_id"),
    F.col("tip_percent_of_base"),
).orderBy(F.col("trip_id")).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Built-ins as the default
# MAGIC
# MAGIC The demo rule buckets `tip_percent_of_base` into a `tip_band` label:
# MAGIC
# MAGIC | `tip_percent_of_base` | `tip_band` |
# MAGIC |---|---|
# MAGIC | NULL | `no_tip` |
# MAGIC | `< 10` | `low` |
# MAGIC | `10` – `< 20` | `medium` |
# MAGIC | `>= 20` | `high` |
# MAGIC
# MAGIC `F.when` expresses this rule entirely with built-in expressions. Catalyst can
# MAGIC inspect those expressions and optimize them together with the surrounding query
# MAGIC plan.

# COMMAND ----------

payment_tip_band_builtin = payment_curated.withColumn(
    "tip_band",
    F.when(F.col("tip_percent_of_base").isNull(), F.lit("no_tip"))
    .when(F.col("tip_percent_of_base") < 10, F.lit("low"))
    .when(F.col("tip_percent_of_base") < 20, F.lit("medium"))
    .otherwise(F.lit("high")),
).select(
    F.col("trip_id"),
    F.col("tip_percent_of_base"),
    F.col("tip_band"),
)

print("tip_band from built-in F.when:")
payment_tip_band_builtin.orderBy(F.col("trip_id")).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The same rule as a Python UDF
# MAGIC
# MAGIC PySpark DataFrame code is written in Python, but built-in column expressions do
# MAGIC not process rows as ordinary Python code. Python builds Spark expressions; Spark
# MAGIC executes them in its JVM-based engine. A Python UDF is different: its logic is
# MAGIC regular Python, so Spark runs it in **Python worker** processes on the cluster.
# MAGIC
# MAGIC Spark transfers batches of values between Spark's JVM executor process and a
# MAGIC Python worker process. Inside the Python worker, the regular UDF still processes
# MAGIC one value at a time. That JVM–Python boundary adds serialization overhead
# MAGIC built-in expressions avoid. Catalyst cannot inspect or optimize the Python logic
# MAGIC inside the UDF.
# MAGIC
# MAGIC The demo below produces the same `tip_band` values as the built-in version; the
# MAGIC difference is where and how Spark executes the logic.

# COMMAND ----------


def tip_band_python(tip_percent):
    if tip_percent is None:
        return "no_tip"
    if tip_percent < 10:
        return "low"
    if tip_percent < 20:
        return "medium"
    return "high"


tip_band_udf = F.udf(tip_band_python, StringType())

payment_tip_band_udf = payment_curated.withColumn(
    "tip_band_udf",
    tip_band_udf(F.col("tip_percent_of_base")),
).select(
    F.col("trip_id"),
    F.col("tip_percent_of_base"),
    F.col("tip_band_udf"),
)

print("tip_band from a Python UDF (same values as the built-in version):")
payment_tip_band_udf.orderBy(F.col("trip_id")).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Which approach to reach for
# MAGIC
# MAGIC Both implementations above agree on `tip_band` because this rule is simple enough
# MAGIC for built-ins — that is the common case in production.
# MAGIC
# MAGIC | Approach | Execution | Use when |
# MAGIC |---|---|---|
# MAGIC | Built-in `F.*` | Spark JVM engine; Catalyst can inspect the expression | Default choice |
# MAGIC | Python UDF | Python worker; logic is opaque to Catalyst | No suitable Spark built-in exists |
# MAGIC | Pandas/Arrow UDF | Python worker using Arrow batches | Advanced fallback for vectorized Python logic |
# MAGIC
# MAGIC **Decision rule.**
# MAGIC
# MAGIC ```
# MAGIC Can Spark built-in functions express the rule?
# MAGIC         |
# MAGIC         +-- Yes → use built-in functions
# MAGIC         |
# MAGIC         +-- No  → consider a UDF
# MAGIC                     |
# MAGIC                     +-- Vectorized Python/Pandas logic → Pandas/Arrow UDF
# MAGIC                     |
# MAGIC                     +-- Otherwise → regular Python UDF as a last resort
# MAGIC ```
# MAGIC
# MAGIC A common mistake is reaching for a UDF out of familiarity with plain Python
# MAGIC before checking whether `pyspark.sql.functions` already covers the rule — most
# MAGIC of the transforms in Module 6 **`01 - Column Transforms with Built-in
# MAGIC Functions`** and **`03 - Cleaning and Curated Outputs`** did not need one.
# MAGIC
# MAGIC **Advanced note.** Pandas UDFs use Apache Arrow to transfer data between Spark
# MAGIC and Python in columnar batches and support vectorized Pandas operations. They can
# MAGIC be useful when Python, Pandas, or NumPy logic is genuinely required, but they
# MAGIC remain secondary to Spark built-in functions. This course does not cover them
# MAGIC further.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Exercise
# MAGIC
# MAGIC New requirement: bucket curated `trip` rows into a `trip_distance_band` from
# MAGIC `trip_distance_km`:
# MAGIC
# MAGIC | `trip_distance_km` | `trip_distance_band` |
# MAGIC |---|---|
# MAGIC | NULL | `unknown` |
# MAGIC | `< 5` | `short` |
# MAGIC | `5` – `< 15` | `medium` |
# MAGIC | `>= 15` | `long` |
# MAGIC
# MAGIC Using `trip_curated`:
# MAGIC
# MAGIC 1. Build `trip_distance_band` with built-in `F.when`.
# MAGIC 2. In a markdown cell or short comment, explain why a Python UDF is unnecessary
# MAGIC    for this rule.
# MAGIC 3. Name one transformation that could justify a UDF because it requires custom
# MAGIC    Python logic or a Python library and has no suitable Spark built-in function
# MAGIC    (you do not need to implement it).
# MAGIC
# MAGIC Do not write any result — this notebook only reads `curated/trip/` and
# MAGIC `curated/payment/`, never overwrites them.

# COMMAND ----------

# Your code here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - **Default to built-ins.** Transformations with `F.*` create Spark expressions
# MAGIC   that Spark runs in its JVM-based engine; Catalyst can inspect those expressions
# MAGIC   and optimize them with the surrounding query plan.
# MAGIC - **Python UDFs are a boundary crossing.** Regular Python logic runs in Python
# MAGIC   worker processes; Spark transfers batches of values between Spark's JVM executor
# MAGIC   process and those workers, and the UDF still processes one value at a time inside
# MAGIC   the worker. Catalyst cannot inspect or optimize code inside the UDF.
# MAGIC - **Reach for a UDF only when built-ins cannot express the rule** — for example
# MAGIC   when you need custom Python or a library with no Spark built-in equivalent.
# MAGIC   Pandas/Arrow UDFs are an advanced fallback for vectorized Python logic; this
# MAGIC   course does not cover them further.
# MAGIC
# MAGIC **Next:** Module 7 — Joins and Set Operations.
