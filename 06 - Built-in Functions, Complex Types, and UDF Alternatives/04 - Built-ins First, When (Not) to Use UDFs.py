# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Built-ins First, When (Not) to Use UDFs
# MAGIC
# MAGIC In this notebook, we apply the same labelling rule to the curated `payment`
# MAGIC data twice: once with built-in functions and once with a Python UDF.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Write a labelling rule with `F.when`
# MAGIC - Write the same rule as a Python UDF with `F.udf`
# MAGIC - Handle `NULL` values in built-in expressions and Python UDFs
# MAGIC - Explain how Spark runs built-in functions and Python UDFs differently
# MAGIC - Decide when a Python UDF is the right choice

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and source data
# MAGIC
# MAGIC Attach **all-purpose compute**.
# MAGIC
# MAGIC - The notebook reads the curated `payment` Parquet output written in
# MAGIC   `03 - Cleaning and Curated Outputs`. It has 105 records.
# MAGIC - This notebook only reads the curated data. It does not write any output.
# MAGIC - `demo_trip_ids` selects one record for each `tip_band` label.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Reading path
curated_root = "/Volumes/rideshare_dev/processed/output_files/curated"
curated_payment_path = f"{curated_root}/payment"

print(f"curated_payment_path = {curated_payment_path}")

# One trip_id per tip_band: none, low, medium, high, unknown
demo_trip_ids = [1, 2, 3, 102, 103]

# COMMAND ----------

payment_curated = spark.read.format(  # pyright: ignore[reportUndefinedVariable]  # noqa: F821
    "parquet"
).load(curated_payment_path)

print(f"Curated payment rows: {payment_curated.count()}")

payment_curated.filter(F.col("trip_id").isin(demo_trip_ids)).select(
    F.col("trip_id"),
    F.col("tip_amount"),
    F.col("base_fare_amount"),
    F.col("tip_percent_of_base"),
).orderBy(F.col("trip_id")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The labelling rule
# MAGIC
# MAGIC Both versions add a `tip_band` label based on `tip_percent_of_base`:
# MAGIC
# MAGIC | `tip_percent_of_base` | `tip_band` |
# MAGIC |---|---|
# MAGIC | `NULL` | `unknown` |
# MAGIC | `0` | `none` |
# MAGIC | Greater than `0` and less than `10` | `low` |
# MAGIC | `10` or greater and less than `20` | `medium` |
# MAGIC | `20` or greater | `high` |
# MAGIC
# MAGIC ## 3. Built-in version with `F.when`
# MAGIC
# MAGIC Spark checks the `F.when` conditions from top to bottom and returns the label of
# MAGIC the first condition that is true. The `isNull()` check comes first because this
# MAGIC rule needs a separate `unknown` label. Without it, `NULL` falls through to
# MAGIC `.otherwise("high")`.

# COMMAND ----------

payment_tip_band_builtin = payment_curated.withColumn(
    "tip_band",
    F.when(F.col("tip_percent_of_base").isNull(), F.lit("unknown"))
    .when(F.col("tip_percent_of_base") == 0, F.lit("none"))
    .when(F.col("tip_percent_of_base") < 10, F.lit("low"))
    .when(F.col("tip_percent_of_base") < 20, F.lit("medium"))
    .otherwise(F.lit("high")),
).select(
    F.col("trip_id"),
    F.col("tip_percent_of_base"),
    F.col("tip_band"),
)

print("tip_band from built-in F.when:")
payment_tip_band_builtin.filter(F.col("trip_id").isin(demo_trip_ids)).orderBy(
    F.col("trip_id")
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Python UDF version
# MAGIC
# MAGIC The same rule written as a regular Python function and registered with
# MAGIC `F.udf`. `StringType()` is the data type the function returns.
# MAGIC
# MAGIC Spark passes `NULL` to the function as Python `None`. The function handles
# MAGIC `None` before any numeric comparison, because Python raises a `TypeError` when
# MAGIC comparing `None` with a number.

# COMMAND ----------


def tip_band_python(tip_percent):
    if tip_percent is None:
        return "unknown"
    if tip_percent == 0:
        return "none"
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
payment_tip_band_udf.filter(F.col("trip_id").isin(demo_trip_ids)).orderBy(
    F.col("trip_id")
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. How Spark runs each version
# MAGIC
# MAGIC Both versions return the same `tip_band` values, but Spark runs them differently.
# MAGIC
# MAGIC | | Built-in functions (`F.*`) | Python UDF (`F.udf`) |
# MAGIC |---|---|---|
# MAGIC | Where logic runs | Executor JVM | Python worker process |
# MAGIC | JVM–Python data transfer | Not required | Required |
# MAGIC | Catalyst optimization | Can optimize the expression | Cannot inspect or optimize the Python function's internal logic |
# MAGIC
# MAGIC ## 6. When to use a Python UDF
# MAGIC
# MAGIC - Use built-in functions by default.
# MAGIC - Use a Python UDF only when built-in functions cannot express the rule, for
# MAGIC   example, when the logic needs a Python library that has no Spark equivalent.
# MAGIC - Pandas UDFs use Apache Arrow to process data in column batches. They are an
# MAGIC   advanced option that this course does not cover.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - The built-in `F.when` version and the Python UDF version return the same
# MAGIC   `tip_band` values for all five demo records.
# MAGIC - Built-in functions run inside the executor JVM, and Catalyst can optimize them.
# MAGIC - A Python UDF runs in a separate Python worker process, and Catalyst cannot
# MAGIC   optimize the Python code.
# MAGIC - Use a Python UDF only when built-in functions cannot express the rule.
# MAGIC
# MAGIC **Next:** Module 7 — `01 - Introduction to PySpark Joins`
