# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Jobs, Stages, and Tasks
# MAGIC
# MAGIC In the previous lesson, we saw that `filter` and `upper` are narrow transformations, while `groupBy` requires a shuffle.
# MAGIC
# MAGIC In this notebook, we will use the same transformations to see how Spark organizes execution into **Jobs, Stages, and Tasks**.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Understand how an action triggers a Spark **Job**
# MAGIC - Understand how a shuffle separates execution into **Stages**
# MAGIC - Understand how **Tasks** process partitions within each stage

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach classic **all-purpose** compute and Access mode should be `dedicated`

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", "false")  # noqa: F821
spark.conf.set("spark.sql.shuffle.partitions", "2")  # noqa: F821

# COMMAND ----------

from decimal import Decimal

from pyspark.sql import functions as F

rows = [
    (1001, "Midtown East", Decimal("12.50")),
    (1002, "midtown east", Decimal("4.90")),
    (1003, "williamsburg", Decimal("6.20")),
    (1004, "Williamsburg", Decimal("11.25")),
    (1005, "chelsea", Decimal("8.75")),
    (1006, "Chelsea", None),
    (1007, "midtown East", Decimal("9.10")),
    (1008, "williamsburg", Decimal("5.00")),
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
# MAGIC ## Before the shuffle
# MAGIC
# MAGIC The first two transformations are `filter` and `upper`.
# MAGIC
# MAGIC Both are narrow transformations. They process data within the existing partitions and do not require data to be redistributed across partitions.
# MAGIC
# MAGIC This work can continue without a shuffle.

# COMMAND ----------

# Step 1: Remove rows where fare is missing
trips_filter = trips.filter(F.col("base_fare_amount").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC The next transformation, `upper`, continues processing the records within the existing partitions.
# MAGIC
# MAGIC Like `filter`, it does not require data to move between partitions.
# MAGIC
# MAGIC Spark can therefore continue this work before reaching the shuffle.

# COMMAND ----------

# Step 2: Normalize pickup zone names to uppercase
trips_upper = trips_filter.withColumn("pickup_zone", F.upper("pickup_zone"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shuffle boundary
# MAGIC
# MAGIC After `upper`, records with the same `pickup_zone` can still exist in different partitions.
# MAGIC
# MAGIC To calculate the total revenue for each pickup zone, Spark must bring records with the same key together.
# MAGIC
# MAGIC `groupBy` therefore requires Spark to redistribute data across partitions.
# MAGIC
# MAGIC This redistribution is called a **shuffle**.
# MAGIC
# MAGIC A shuffle creates a boundary in the execution. Spark performs the work before and after that boundary in separate **stages**.

# COMMAND ----------

# Step 3: Aggregate total revenue by pickup zone
trip_summary = (
    trips_upper.groupBy("pickup_zone")
    .agg(F.sum("base_fare_amount").alias("total_revenue"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trigger the job
# MAGIC
# MAGIC So far, Spark has only built the transformations.
# MAGIC
# MAGIC `show()` is an **action**. When it runs, Spark starts executing the query to produce the requested result.
# MAGIC
# MAGIC This execution is organized as a **Jobs, Stages, and Tasks**.

# COMMAND ----------

trip_summary.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## After the shuffle
# MAGIC
# MAGIC During execution, Spark redistributes records by `pickup_zone`.
# MAGIC
# MAGIC The work before the shuffle is performed in one stage, and the work after the shuffle is performed in another stage.
# MAGIC
# MAGIC After matching keys have been brought together, Spark can calculate the final revenue for each pickup zone.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tasks
# MAGIC
# MAGIC Each stage contains **Tasks**.
# MAGIC
# MAGIC A task processes one partition for that stage.
# MAGIC
# MAGIC Because different partitions can be processed independently, multiple tasks can run in parallel.
# MAGIC
# MAGIC So the execution hierarchy is:
# MAGIC
# MAGIC **Job → Stages → Tasks**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC In this notebook, we learned how Spark organizes execution into Jobs, Stages, and Tasks.
# MAGIC
# MAGIC - An **action** such as `show()` triggers a **Job**.
# MAGIC - Narrow transformations can continue without requiring a shuffle.
# MAGIC - A **shuffle** creates a boundary between **Stages**.
# MAGIC - Each stage contains **Tasks** that process its partitions.
# MAGIC - A Spark execution can therefore be understood as:
# MAGIC
# MAGIC   **Job → Stages → Tasks**
# MAGIC
# MAGIC **Next:** `05 - Common DataFrame Actions`