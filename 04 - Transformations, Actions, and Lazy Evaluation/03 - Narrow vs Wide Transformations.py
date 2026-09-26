# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Narrow vs Wide Transformations
# MAGIC
# MAGIC Filter and uppercase stay in one partition. `groupBy` shuffles. That split
# MAGIC is the stage DAG.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Differentiate narrow from wide transformations
# MAGIC - Identify `Exchange` in the physical plan
# MAGIC - Read jobs, stages, tasks, and the stage DAG
# MAGIC - Explain that a failed task retries from the lineage

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach classic **all-purpose** compute
# MAGIC
# MAGIC This notebook rebuilds the same trips as
# MAGIC `01 - Transformations vs Actions`. Run this setup. Do not depend on
# MAGIC notebook 01 still being in the session.
# MAGIC
# MAGIC Turn AQE off and set shuffle partitions to `2` so the wide job is easier
# MAGIC to read. Do not call `repartition`. Partition count is whatever Spark
# MAGIC gives this tiny frame — often one.

# COMMAND ----------

from decimal import Decimal

from pyspark.sql import functions as F

spark.conf.set("spark.sql.adaptive.enabled", "false")  # noqa: F821
spark.conf.set("spark.sql.shuffle.partitions", "2")  # noqa: F821

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

trips.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Partitions and tasks
# MAGIC
# MAGIC A **partition** is a chunk of rows Spark can process in parallel. A
# MAGIC **task** processes one partition.
# MAGIC
# MAGIC **Business question:** How many piles of rows does this hand-built table
# MAGIC have?

# COMMAND ----------

trips.select(
    "trip_id",
    "pickup_zone",
    F.spark_partition_id().alias("partition_id"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC You may see one `partition_id` for every row. That means one pile and one
# MAGIC task before any shuffle. The Notion page used four piles from files. This
# MAGIC lab uses whatever Spark assigned.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Narrow transformations
# MAGIC
# MAGIC A **narrow** transformation uses only the rows already in that partition.
# MAGIC No data moves. No shuffle.
# MAGIC
# MAGIC **Business question:** Operations needs trips that have a fare, with pickup
# MAGIC zone names in uppercase.

# COMMAND ----------

narrow = (
    trips.filter(F.col("base_fare_amount").isNotNull())
    .withColumn("pickup_zone", F.upper(F.col("pickup_zone")))
)

narrow.explain()
narrow.show()

# COMMAND ----------

# MAGIC %md
# MAGIC The physical plan should have no `Exchange`. `filter` and `upper` stay on
# MAGIC the same piles.
# MAGIC
# MAGIC `show()` starts a **job**. With no shuffle that job is **one stage**.
# MAGIC The stage runs **one task per partition**.
# MAGIC
# MAGIC ```text
# MAGIC show()
# MAGIC     ↓
# MAGIC 1 job
# MAGIC     ↓
# MAGIC 1 stage
# MAGIC     ↓
# MAGIC N tasks (one per partition)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wide transformations
# MAGIC
# MAGIC A **wide** transformation needs matching keys together. Spark **shuffles**
# MAGIC rows between partitions. That often appears as `Exchange` and starts a new
# MAGIC **stage**.
# MAGIC
# MAGIC **Business question:** Operations needs total fare by pickup zone.

# COMMAND ----------

trip_summary = (
    trips.filter(F.col("base_fare_amount").isNotNull())
    .withColumn("pickup_zone", F.upper(F.col("pickup_zone")))
    .groupBy("pickup_zone")
    .agg(F.sum("base_fare_amount").alias("total_revenue"))
)

trip_summary.explain()
trip_summary.show()

# COMMAND ----------

# MAGIC %md
# MAGIC The physical plan should show `Exchange`. Stage 1 runs the filter and
# MAGIC `upper` and partial sums. The shuffle gathers the same `pickup_zone`.
# MAGIC Stage 2 finishes `SUM`.
# MAGIC
# MAGIC ```text
# MAGIC show()
# MAGIC     ↓
# MAGIC 1 job
# MAGIC     ↓
# MAGIC 2 stages (split at Exchange)
# MAGIC     ↓
# MAGIC N tasks, then 2 tasks (shuffle partitions = 2)
# MAGIC ```
# MAGIC
# MAGIC That picture of stages is the **DAG**. In Spark UI, that `show()` is one
# MAGIC job with **two stages**. Do not also `collect()` — a second action starts
# MAGIC a second job.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Confirm the DAG in Spark UI
# MAGIC
# MAGIC **Spark UI** → **Jobs** — narrow run: `Stages: 1/1`. Wide run: more than
# MAGIC one stage.
# MAGIC
# MAGIC **Spark UI** → **SQL / DataFrame** → **Completed Queries** → the wide
# MAGIC query → **Details for Query**
# MAGIC
# MAGIC Find **Exchange**. That node is the shuffle boundary in the DAG.

# COMMAND ----------

# MAGIC %md
# MAGIC ## If a task fails
# MAGIC
# MAGIC Spark does not keep every intermediate table. It keeps the **lineage**
# MAGIC from notebook 02 — the plan back to `trips`.
# MAGIC
# MAGIC If a **task** fails, Spark retries it and recomputes that partition from
# MAGIC the plan. You do not rerun the notebook by hand. This notebook does not
# MAGIC crash an executor to prove it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common shuffle triggers
# MAGIC
# MAGIC A shuffle costs network (and often disk) time and adds a stage. Skip wide
# MAGIC steps you do not need.
# MAGIC
# MAGIC * `groupBy()` and aggregations — this notebook
# MAGIC * `orderBy()` and `sort()` — you used `orderBy` in notebook 01
# MAGIC * `distinct()` and `dropDuplicates()`
# MAGIC * `repartition()` — do not call it here
# MAGIC * Many joins (later modules)
# MAGIC
# MAGIC Deep shuffle tuning waits for Module 18.

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", "true")  # noqa: F821
spark.conf.set("spark.sql.shuffle.partitions", "200")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC * Partition — a chunk of rows. One task processes one partition.
# MAGIC * Narrow — `filter`, `upper`; no `Exchange`; one stage.
# MAGIC * Wide — `groupBy`; `Exchange`; a new stage; the stage DAG.
# MAGIC * An action starts a **job**. Stages run **tasks**.
# MAGIC * A failed task retries from the lineage.
# MAGIC
# MAGIC **Next:** `04 - Common DataFrame Actions` — `first`, `take`, `toPandas`,
# MAGIC and driver memory. Writes wait for Module 5.
