# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Narrow and Wide Transformations — Shuffle
# MAGIC
# MAGIC `filter` and `upper` process data within the **existing partitions**. `groupBy` requires a **shuffle**.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Differentiate narrow from wide transformations

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

trips.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Input partitions and tasks
# MAGIC
# MAGIC Spark divides the input data into smaller logical units called input partitions.
# MAGIC Each input partition can be processed in parallel by a task.

# COMMAND ----------

trips.select(
    "pickup_zone",
    "base_fare_amount",
    F.spark_partition_id().alias("partition_id"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Narrow transformations
# MAGIC
# MAGIC Each task processes its input partition by applying this filter condition.
# MAGIC
# MAGIC This filter is a narrow transformation because each task can process its partition independently, without moving data to another partition.

# COMMAND ----------

# Step 1: Remove rows where fare is missing
trips_filter = trips.filter(F.col("base_fare_amount").isNotNull())

trips_filter.select(
    "pickup_zone",
    "base_fare_amount",
    F.spark_partition_id().alias("partition_id"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC The next transformation `upper` continues processing the data within the same partitions.
# MAGIC
# MAGIC Both transformations process records within the existing partitions and neither requires data to move between partitions.

# COMMAND ----------

# Step 2: Normalize pickup zone names to uppercase
trips_upper = trips_filter.withColumn("pickup_zone", F.upper("pickup_zone"))

trips_upper.select(
    "pickup_zone",
    "base_fare_amount",
    F.spark_partition_id().alias("partition_id"),
).show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Wide transformations
# MAGIC
# MAGIC At this point, records for the same pickup zone may exist in different partitions.
# MAGIC
# MAGIC To calculate revenue for each pickup zone, Spark must bring records with the same key together
# MAGIC
# MAGIC This process requires data movement between partitions. This data movement across the cluster is called a shuffle.

# COMMAND ----------

# Step 3: Aggregate total revenue by pickup zone
trip_summary = (
    trips_upper.groupBy("pickup_zone")
    .agg(F.sum("base_fare_amount").alias("total_revenue"))
)


trip_summary.select(
    "pickup_zone",
    "total_revenue",
    F.spark_partition_id().alias("partition_id"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC In this notebook, we learned how transformations affect Spark partitions.
# MAGIC
# MAGIC - Spark divides input data into **partitions**, and each partition can be processed by a **task**.
# MAGIC - **Narrow transformations** such as `filter` and `upper` process data within the existing partitions without moving data between partitions.
# MAGIC - **Wide transformations** such as `groupBy` require related records from different partitions to be brought together.
# MAGIC - This redistribution of data across partitions is called a shuffle.
# MAGIC
# MAGIC **Next:** `04 - Jobs, Stages, and Tasks`.