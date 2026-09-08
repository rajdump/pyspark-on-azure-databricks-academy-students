# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Narrow vs Wide Transformations
# MAGIC
# MAGIC Local work versus shuffles — `Exchange` as a stage boundary.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Differentiate narrow from wide transformations
# MAGIC - Identify `Exchange` in the physical plan
# MAGIC - Recognize common shuffle triggers such as `groupBy` and `orderBy`
# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up the payments example
# MAGIC
# MAGIC Build a small DataFrame with enough rows to land in more than one partition
# MAGIC and to aggregate by `payment_method` later. Course `payment` columns used
# MAGIC here: `trip_id` (`bigint`), `payment_method` (`string`),
# MAGIC `base_fare_amount` (`decimal(10,2)`), and `tip_amount` (`decimal(10,2)`).
# MAGIC
# MAGIC Prefer classic all-purpose compute (**Dedicated** access mode) for the
# MAGIC clearest partition and shuffle demos. The notebook also runs on Standard
# MAGIC and serverless, but those environments may collapse this hand-built sample
# MAGIC into a single partition, which weakens the narrow vs wide contrast.
# MAGIC Partition count follows the cluster (often tied to cores) — observe what
# MAGIC you get.
# MAGIC
# MAGIC The same DataFrame is used for both the narrow and wide demos.

# COMMAND ----------

from decimal import Decimal

from pyspark.sql import functions as F

payments = spark.createDataFrame(  # pyright: ignore[reportUndefinedVariable]  # noqa: F821
    [
        (1001, "card", Decimal("12.50"), Decimal("3.50")),
        (1002, "cash", Decimal("8.75"), Decimal("0.00")),
        (1003, "card", Decimal("6.20"), Decimal("2.00")),
        (1004, "cash", Decimal("9.10"), Decimal("1.25")),
        (1005, "card", Decimal("15.00"), Decimal("4.00")),
        (1006, "cash", Decimal("5.40"), Decimal("0.50")),
        (1007, "card", Decimal("7.80"), Decimal("1.00")),
        (1008, "cash", Decimal("11.25"), Decimal("2.75")),
        (1009, "card", Decimal("4.80"), Decimal("0.00")),
        (1010, "cash", Decimal("10.00"), Decimal("3.25")),
    ],
    """
    trip_id bigint,
    payment_method string,
    base_fare_amount decimal(10,2),
    tip_amount decimal(10,2)
    """,
)

payments.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect how rows are distributed across partitions
# MAGIC
# MAGIC A **partition** is a chunk of rows that Spark can process in parallel. One
# MAGIC task usually works on one partition.
# MAGIC
# MAGIC Print each partition separately so you can see which rows sit together.
# MAGIC Your partition count may differ from a classmate's — that is expected on
# MAGIC Dedicated all-purpose compute. You should still see more than one
# MAGIC partition for this sample.

# COMMAND ----------

with_part = payments.withColumn("partition_id", F.spark_partition_id())

for pid in sorted(
    r.partition_id for r in with_part.select("partition_id").distinct().collect()
):
    print(f"=== partition {pid} ===")
    with_part.filter(F.col("partition_id") == pid).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Narrow transformations
# MAGIC
# MAGIC A **narrow transformation** processes each partition independently.
# MAGIC
# MAGIC Spark uses only the rows already available in that partition, so it does not
# MAGIC need to move data between partitions. Therefore, no shuffle is required.
# MAGIC
# MAGIC Keep tips greater than `0`.

# COMMAND ----------

narrow_df = payments.filter(F.col("tip_amount") > F.lit(0))
narrow_df.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect partitions after the narrow transformation
# MAGIC
# MAGIC Print each partition again. Surviving rows should keep the same
# MAGIC `partition_id`.

# COMMAND ----------

narrow_part = narrow_df.withColumn("partition_id", F.spark_partition_id())

for pid in sorted(
    r.partition_id
    for r in narrow_part.select("partition_id").distinct().collect()
):
    print(f"=== partition {pid} ===")
    narrow_part.filter(F.col("partition_id") == pid).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect the narrow plan
# MAGIC
# MAGIC Call `explain`. Look at the physical plan — it should not show `Exchange`.
# MAGIC (Notebook 02 used `explain(mode="extended")`; here the goal is to spot
# MAGIC whether a shuffle operator is present.)

# COMMAND ----------

narrow_df.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read the narrow result
# MAGIC
# MAGIC Compare the partition prints before and after the filter:
# MAGIC
# MAGIC - Zero-tip rows (`1002`, `1009`) drop out
# MAGIC - Surviving rows keep the same `partition_id`
# MAGIC - No rows move between partitions, so no shuffle occurs
# MAGIC
# MAGIC Your `explain` output should match that story: a `Filter` on the local
# MAGIC source, with no `Exchange`.
# MAGIC
# MAGIC `collect()` triggers **one job** to return all rows. Because there is no
# MAGIC shuffle, that job contains **one stage**. The stage runs one **task** per
# MAGIC partition — so the task count matches the partition count you observed
# MAGIC above:
# MAGIC
# MAGIC ```text
# MAGIC collect()
# MAGIC     ↓
# MAGIC 1 job
# MAGIC     ↓
# MAGIC 1 stage
# MAGIC     ↓
# MAGIC N tasks (one per partition)
# MAGIC     ↓
# MAGIC N partitions processed independently
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wide transformations
# MAGIC
# MAGIC A **wide transformation** needs rows from more than one partition.
# MAGIC
# MAGIC Spark must move rows between partitions so matching keys can be processed
# MAGIC together. That movement is a **shuffle**. In the physical plan it often
# MAGIC appears as `Exchange`.
# MAGIC
# MAGIC Count rows by `payment_method`.

# COMMAND ----------

wide_df = payments.groupBy("payment_method").count()
wide_df.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect partitions after the wide transformation
# MAGIC
# MAGIC Print each partition of the `groupBy` result. Matching keys were gathered by
# MAGIC the shuffle, so the layout differs from the original payments partitions.

# COMMAND ----------

wide_part = wide_df.withColumn("partition_id", F.spark_partition_id())

for pid in sorted(
    r.partition_id
    for r in wide_part.select("partition_id").distinct().collect()
):
    print(f"=== partition {pid} ===")
    wide_part.filter(F.col("partition_id") == pid).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect the wide plan
# MAGIC
# MAGIC Call `explain`. The physical plan should show `Exchange`.

# COMMAND ----------

wide_df.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read the wide result
# MAGIC
# MAGIC Before `groupBy`, `card` and `cash` rows were spread across the input
# MAGIC partitions you printed earlier. Each partition can calculate only a
# MAGIC **partial** count of each `payment_method`.
# MAGIC
# MAGIC Spark then runs the wide transformation across a shuffle boundary.
# MAGIC
# MAGIC ### Stage 1 — before the shuffle
# MAGIC
# MAGIC One task per input partition calculates partial `card` and `cash` counts.
# MAGIC
# MAGIC ### Shuffle boundary
# MAGIC
# MAGIC Your `explain` output should show `Exchange` (hash partitioning by
# MAGIC `payment_method`). That operator moves partial counts with the same
# MAGIC `payment_method` together.
# MAGIC
# MAGIC ### Stage 2 — after the shuffle
# MAGIC
# MAGIC Spark combines the partial counts into the final totals:
# MAGIC
# MAGIC | payment_method | Final count |
# MAGIC |---|---:|
# MAGIC | card | 5 |
# MAGIC | cash | 5 |
# MAGIC
# MAGIC Look at your post-`groupBy` partition print — the two summary rows may land
# MAGIC in fewer partitions than the original payments layout.
# MAGIC
# MAGIC ```text
# MAGIC N input partitions
# MAGIC         ↓
# MAGIC N tasks calculate partial counts
# MAGIC         ↓
# MAGIC Exchange — shuffle
# MAGIC         ↓
# MAGIC later stage combines shuffled results
# MAGIC         ↓
# MAGIC card = 5 and cash = 5
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Confirm the shuffle in the Spark UI
# MAGIC
# MAGIC Open the query plan for the wide `groupBy` + `collect()` run:
# MAGIC
# MAGIC **Spark UI** → **SQL / DataFrame** → **Completed Queries** → select the
# MAGIC query → **Details for Query**
# MAGIC
# MAGIC In the plan visualization, find **Exchange**. That node is the shuffle
# MAGIC boundary between the partial-count stage and the final-count stage.
# MAGIC
# MAGIC Also open the Spark Jobs list for that run and compare stage counts: the
# MAGIC wide job should show **more than one stage**, unlike the narrow `filter`
# MAGIC job (`Stages: 1/1`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common shuffle triggers
# MAGIC
# MAGIC A shuffle happens when Spark must move rows between partitions so related
# MAGIC rows can be processed together. That costs network (and often disk) time and
# MAGIC splits work into extra stages — so avoid wide steps you do not need.
# MAGIC
# MAGIC Common shuffle triggers include:
# MAGIC
# MAGIC - `groupBy()` and aggregations
# MAGIC - `orderBy()` and `sort()`
# MAGIC - `distinct()` and `dropDuplicates()`
# MAGIC - `repartition()`
# MAGIC - Joins that require both sides to be redistributed
# MAGIC
# MAGIC In this notebook, `groupBy()` created the shuffle. You already used
# MAGIC `orderBy` as a transformation in Notebook 01 — now you know it can also
# MAGIC trigger a wide stage. Full join and aggregation APIs come in later modules;
# MAGIC deep shuffle and partition tuning wait for Module 17.
# MAGIC
# MAGIC > **Good to know:** These examples use `collect()` instead of `show()`.
# MAGIC >
# MAGIC > `show()` fetches only enough rows for display and may scan partitions in
# MAGIC > multiple passes. That can create extra jobs unrelated to the narrow or
# MAGIC > wide transformation, which confuses Spark UI reading for beginners.
# MAGIC >
# MAGIC > `collect()` requests the complete result, so the Spark UI is easier to
# MAGIC > follow on this tiny dataset. Use `collect()` only for small results — it
# MAGIC > returns all rows to the driver. Notebook 04 covers that risk.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC Build an isolated example (do not reuse DataFrames from earlier sections):
# MAGIC
# MAGIC 1. Create **`exercise_df`** with **`trip_id`** (`bigint`),
# MAGIC    **`payment_method`** (`string`), **`base_fare_amount`**
# MAGIC    (`decimal(10,2)`), and **`tip_amount`** (`decimal(10,2)`). Use four to
# MAGIC    six small rows with mixed `card` / `cash` and at least one zero tip.
# MAGIC 2. Build a **narrow** DataFrame with `filter` on tip amount. Predict: no
# MAGIC    `Exchange`. Verify with `explain()`, then `collect()`.
# MAGIC 3. Build a **wide** DataFrame with `groupBy("payment_method").count()`.
# MAGIC    Predict: `Exchange` present. Verify with `explain()`, then `collect()`.
# MAGIC 4. Add a one-line note comparing the stage story you expect in Spark UI for
# MAGIC    each action (`1/1` vs more than one stage).

# COMMAND ----------

# Your code here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Recap this notebook's path:
# MAGIC
# MAGIC - **Partition** — a chunk of rows; one task usually processes one partition
# MAGIC - **Narrow** — work stays inside each partition (`filter`); no `Exchange`
# MAGIC - **Wide** — matching keys must meet (`groupBy`); shuffle appears as
# MAGIC   `Exchange` and starts a new **stage**
# MAGIC - **`collect()`** starts a **job**; stages run **tasks** across partitions
# MAGIC - **Spark UI** — **Details for Query** confirms `Exchange` for the wide run
# MAGIC - **Shuffle triggers** — `groupBy`, `orderBy`, `distinct`, `repartition`,
# MAGIC   many joins; deep tuning is Module 17
# MAGIC
# MAGIC Next up: **Common DataFrame Actions** — `first`, `head`, `take`, `tail`,
# MAGIC `isEmpty`, and `toPandas`. Writing with `DataFrame.write` waits for
# MAGIC Module 5.
