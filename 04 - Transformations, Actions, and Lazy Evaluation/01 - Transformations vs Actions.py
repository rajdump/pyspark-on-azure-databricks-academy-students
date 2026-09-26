# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Transformations vs Actions
# MAGIC
# MAGIC Write a zone fare list. Spark does not run it until you ask for a result.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC - See that transformations build a plan and return new DataFrames
# MAGIC - DataFrames are `immutable`
# MAGIC - Use the `show` action to run that plan

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach classic **all-purpose** compute

# COMMAND ----------

from decimal import Decimal

from pyspark.sql import functions as F

rows = [
    (1001, "Midtown East", Decimal("12.50")),
    (1002, "chelsea", Decimal("8.75")),
    (1003, "Astoria", Decimal("6.20")),
    (1004, "SoHo", None),
    (1005, "Williamsburg", Decimal("11.25")),
    (1006, "midtown west", Decimal("9.10")),
]

schema_ddl = (
    "trip_id bigint, pickup_zone string, base_fare_amount decimal(10,2)"
)

trips = spark.createDataFrame(  # pyright: ignore[reportUndefinedVariable]  # noqa: F821
    rows,
    schema_ddl,
)

# COMMAND ----------

# Step 1: Remove rows where fare is missing
trips_filter = trips.filter(F.col("base_fare_amount").isNotNull())


# COMMAND ----------

# Step 2: Normalize pickup zone names to uppercase
trips_upper = trips_filter.withColumn("pickup_zone", F.upper("pickup_zone"))


# COMMAND ----------

# Step 3: Aggregate total revenue by pickup zone
trip_summary = (
    trips_upper
    .groupBy("pickup_zone")
    .agg(F.sum("base_fare_amount").alias("total_revenue"))
)

# COMMAND ----------

trip_summary.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC * Transformations return a new DataFrame and extend the plan —
# MAGIC   `filter`, `withColumn`, `groupBy`, `agg`. 
# MAGIC * The source DataFrame does not change.
# MAGIC * Actions run the plan — `show`.
# MAGIC
# MAGIC **Next:** `02 - Lazy Evaluation and the Query Plan` — why Spark waits, and
# MAGIC how to inspect the plan.