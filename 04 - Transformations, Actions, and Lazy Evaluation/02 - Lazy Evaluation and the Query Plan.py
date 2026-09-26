# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Lazy Evaluation and the Query Plan
# MAGIC
# MAGIC Spark records transformations first and executes them only when an action
# MAGIC requests a result.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Understand lazy evaluation
# MAGIC - Inspect a query plan with `.explain()`
# MAGIC - See how Spark optimizes a logical plan before execution

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach classic **all-purpose** compute.
# MAGIC
# MAGIC This notebook uses the same trip data as `01 - Transformations vs Actions`.

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

# MAGIC %md
# MAGIC ## Lazy evaluation
# MAGIC
# MAGIC Spark uses **lazy evaluation** for DataFrame transformations.
# MAGIC
# MAGIC When we apply transformations, Spark records the operations in a logical
# MAGIC plan. It does not execute the transformation chain until an action
# MAGIC requests a result.
# MAGIC
# MAGIC In the previous notebook, we filtered the missing fare before converting
# MAGIC the pickup zone to uppercase.
# MAGIC
# MAGIC Here, we intentionally write the operations in a different order so we
# MAGIC can inspect what Spark does with the plan.

# COMMAND ----------

# Step 1: Normalize pickup zone names to uppercase
trips_upper = trips.withColumn("pickup_zone", F.upper("pickup_zone"))

# COMMAND ----------

# Step 2: Remove rows where fare is missing
trips_filter = trips_upper.filter(F.col("base_fare_amount").isNotNull())

# COMMAND ----------

# Step 3: Aggregate total revenue by pickup zone
trip_summary = (
    trips_filter.groupBy("pickup_zone")
    .agg(F.sum("base_fare_amount").alias("total_revenue"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC The transformation cells finished without producing the result.
# MAGIC
# MAGIC Spark has built a logical plan for `trip_summary`, but has not executed
# MAGIC it yet.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect the query plan
# MAGIC
# MAGIC `.explain(mode="extended")` lets us inspect how Spark understands and
# MAGIC prepares the DataFrame query.
# MAGIC
# MAGIC It does not execute the query or return the result rows.

# COMMAND ----------

trip_summary.explain(mode="extended")

# COMMAND ----------

# MAGIC %md
# MAGIC Read the plan from the bottom up.
# MAGIC
# MAGIC Focus on these three parts:
# MAGIC
# MAGIC - **Parsed / Analyzed Logical Plan** — shows the operations we wrote:
# MAGIC   `upper`, then the fare filter, then the aggregation.
# MAGIC - **Optimized Logical Plan** — Spark rewrites the logical plan before
# MAGIC   execution. In this small in-memory example, the `Project` and `Filter`
# MAGIC   are folded into the `LocalRelation`.
# MAGIC - **Physical Plan** — shows how Spark plans to execute the optimized
# MAGIC   query.
# MAGIC
# MAGIC Notice that the optimized relation contains only the rows needed for the
# MAGIC query. The trip with the missing fare has already been removed before
# MAGIC the physical `LocalTableScan`.
# MAGIC
# MAGIC The result has not changed. Spark changed the plan used to produce it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the plan
# MAGIC
# MAGIC `show()` is the action that requests the result.

# COMMAND ----------

trip_summary.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect the query in Spark UI
# MAGIC
# MAGIC After `show()` completes, open:
# MAGIC
# MAGIC **Spark UI** → **SQL / DataFrame** → **Completed Queries** → the query →
# MAGIC **Details for Query**
# MAGIC
# MAGIC The Spark UI shows the physical plan used to execute the query, along
# MAGIC with the jobs and stages created during execution.
# MAGIC
# MAGIC The physical plan does not have to match the order in which the Python
# MAGIC transformations were written.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - Spark lazily records DataFrame transformations in a logical plan.
# MAGIC - An action requests the result and triggers execution.
# MAGIC - `.explain(mode="extended")` shows the logical and physical query plans.
# MAGIC - Spark can optimize the logical plan before execution while preserving
# MAGIC   the same result.
# MAGIC
# MAGIC **Next:** `03 - Narrow vs Wide Transformations`