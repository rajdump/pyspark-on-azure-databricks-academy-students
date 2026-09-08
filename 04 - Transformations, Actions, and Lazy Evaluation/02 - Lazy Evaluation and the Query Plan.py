# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Lazy Evaluation and the Query Plan
# MAGIC
# MAGIC Why Spark waits for an action, and how the optimizer can rewrite a plan.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Explain lazy evaluation
# MAGIC - Inspect logical and physical plans with `.explain()` and spot optimizer
# MAGIC   changes
# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up the payments example
# MAGIC
# MAGIC Build a small DataFrame with the course `payment` columns used in this
# MAGIC notebook: `trip_id` (`bigint`), `payment_method` (`string`),
# MAGIC `base_fare_amount` (`decimal(10,2)`), and `tip_amount` (`decimal(10,2)`).

# COMMAND ----------

from decimal import Decimal

from pyspark.sql import functions as F

payments = spark.createDataFrame(  # pyright: ignore[reportUndefinedVariable]  # noqa: F821
    [
        (1001, "card", Decimal("12.50"), Decimal("0.00")),
        (1002, "cash", Decimal("8.75"), Decimal("1.50")),
        (1003, "card", Decimal("6.20"), Decimal("2.00")),
        (1004, "card", Decimal("4.80"), Decimal("0.50")),
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
# MAGIC ## Why Spark waits for an action
# MAGIC
# MAGIC **Lazy evaluation** means Spark records each transformation in a logical
# MAGIC plan and does not process rows until an action asks for a result.
# MAGIC
# MAGIC **Business question:** Payments ops wants non-zero tips reviewed with a
# MAGIC derived INR value and a simple tip band. What happens when you define that
# MAGIC chain — and when does Spark actually run it?

# COMMAND ----------

review_payments = (
    payments.withColumn(
        "tip_inr",
        F.round(F.col("tip_amount") * F.lit(83), 2),
    )
    .withColumn(
        "tip_band",
        F.when(F.col("tip_amount") >= F.lit(2), F.lit("high")).otherwise(F.lit("low")),
    )
    .select(
        "trip_id",
        "payment_method",
        "base_fare_amount",
        "tip_amount",
        "tip_inr",
        "tip_band",
    )
    .filter(F.col("tip_amount") > F.lit(0))
)

# COMMAND ----------

# MAGIC %md
# MAGIC The cell finished with no printed rows. Spark has a plan for
# MAGIC **`review_payments`**, not a computed table in memory. An action is what
# MAGIC forces execution. **`show()`**, **`count()`**, and write/save operations
# MAGIC are actions.

# COMMAND ----------

review_payments.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Spark waits so it can see the full chain before choosing how to run it.
# MAGIC That is why the optimizer can move the late filter earlier: the full plan
# MAGIC is available before the action starts the job.
# MAGIC
# MAGIC > **Good to know:** After the action, open **Spark UI → Jobs** on classic
# MAGIC > all-purpose compute. This notebook's query should appear as a job with the
# MAGIC > optimized plan already in place.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect the query plan with `explain(mode="extended")`
# MAGIC
# MAGIC **`.explain()`** prints how Spark understands a DataFrame. It does not
# MAGIC return rows to your notebook the way **`show()`** does — it prints the
# MAGIC plan text.
# MAGIC
# MAGIC **Business question:** What plan did Spark build for the payments review
# MAGIC before any action ran?

# COMMAND ----------

review_payments.explain(mode="extended")

# COMMAND ----------

# MAGIC %md
# MAGIC Read the extended output from the bottom up, you do not need every operator name yet — look for the filter, the projected columns, and the local relation that holds these hand-built rows.
# MAGIC
# MAGIC - Parsed / Analyzed plan: your written chain is still visible.
# MAGIC - Optimized logical plan: Spark collapsed the whole chain into a LocalRelation.
# MAGIC - Physical plan: Spark reads that as a LocalTableScan.
# MAGIC
# MAGIC Spark optimized plan can place the
# MAGIC `tip_amount > 0` filter before the derived columns, so the zero-tip row
# MAGIC does not pay for `tip_inr` or `tip_band`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Confirm the optimized execution in Spark UI
# MAGIC
# MAGIC Open the query plan for the **`show()`** action:
# MAGIC
# MAGIC **Spark UI** → **SQL / DataFrame** → **Completed Queries** → select the
# MAGIC query → **Details for Query**
# MAGIC
# MAGIC In the plan visualization, look for the physical node that reads the final
# MAGIC rows for this in-memory DataFrame. The row
# MAGIC count should reflect only the non-zero-tip rows, not the full source.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC Build an isolated chain (do not reuse DataFrames from earlier sections):
# MAGIC
# MAGIC 1. Create **`exercise_df`** with **`trip_id`** (`bigint`),
# MAGIC    **`payment_method`** (`string`), **`base_fare_amount`**
# MAGIC    (`decimal(10,2)`), and **`tip_amount`** (`decimal(10,2)`). Use three or
# MAGIC    four small rows.
# MAGIC 2. Add one or two narrow transformations first, then put the filter last.
# MAGIC 3. Call **`.explain(mode="extended")`** on the chain.
# MAGIC 4. Add a short note about where the optimized plan differs from the written
# MAGIC    order.
# MAGIC 5. Call **`show()`** once after you finish reading the plan.

# COMMAND ----------

# Your code here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Recap this notebook's path:
# MAGIC
# MAGIC - **Lazy evaluation** — transformations accumulate in a logical plan;
# MAGIC   Spark processes rows when an action runs
# MAGIC - **Actions that trigger execution** — **`show`**, **`count`**, and write /
# MAGIC   save operations (building a writer alone is not enough)
# MAGIC - **`.explain(mode="extended")`** — prints the plan; compare logical and
# MAGIC   physical stages
# MAGIC - **Optimizer reordering** — Catalyst may move a late filter earlier while
# MAGIC   preserving the same result
# MAGIC - **Spark UI proof** — the job / SQL UI should show the optimized execution
# MAGIC
# MAGIC Next up: **Narrow vs Wide Transformations** — local work versus shuffles
# MAGIC and **`Exchange`** in the physical plan.
