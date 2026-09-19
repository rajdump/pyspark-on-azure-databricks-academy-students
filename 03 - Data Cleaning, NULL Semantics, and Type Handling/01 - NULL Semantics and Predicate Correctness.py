# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 01 - NULL Semantics and Predicate Correctness
# MAGIC
# MAGIC Three-valued logic and NULL-safe predicates — before messy-value cleanup.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Explain three-valued logic and why filters keep only `TRUE` rows
# MAGIC - Build NULL-safe predicates with `isNull` / `isNotNull`, and `eqNullSafe` / `<=>`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach classic **all-purpose** compute or serverless

# COMMAND ----------

from decimal import Decimal

from pyspark.sql import functions as F

rows = [
    (1001, "Card", Decimal("3.50"), 138),
    (1002, "Cash", None, 74),
    (1003, None, Decimal("2.00"), 231),
    (1004, "Card", Decimal("1.00"), None),
]

schema_ddl = """
    trip_id bigint,
    payment_method string,
    tip_amount decimal(10,2),
    pickup_location_id int
"""

df = spark.createDataFrame(  # pyright: ignore[reportUndefinedVariable]  # noqa: F821
    rows,
    schema_ddl,
)

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Understand three-valued logic
# MAGIC
# MAGIC **Business question:** Operations needs trips that qualify for a card-tip
# MAGIC reward.
# MAGIC
# MAGIC Qualifying rule: payment method is **`"Card"`** and tip is at least
# MAGIC **`$2.00`**.

# COMMAND ----------

is_card = F.col("payment_method") == "Card"
tip_at_least_2 = F.col("tip_amount") >= 2.00

qualifies_for_reward = is_card & tip_at_least_2

df.select(
    "trip_id",
    "payment_method",
    "tip_amount",
    is_card.alias("is_card"),
    tip_at_least_2.alias("tip_at_least_2"),
    qualifies_for_reward.alias("qualifies_for_reward"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## See what a filter keeps
# MAGIC
# MAGIC A filter keeps only rows whose condition is **`TRUE`** — not **`FALSE`**
# MAGIC or **`NULL`**.

# COMMAND ----------

df.filter(qualifies_for_reward).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use `isNull` and `isNotNull` for definite answers
# MAGIC
# MAGIC When a value is `NULL`, a normal comparison can return `NULL` instead of `TRUE` or `FALSE`. 
# MAGIC
# MAGIC `NULL` needs a separate rule; check it with **`isNull()`** / **`isNotNull()`** 
# MAGIC
# MAGIC **Business question:** A payment audit needs trips with a recorded payment
# MAGIC method.

# COMMAND ----------

df.filter(F.col("payment_method").isNotNull()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Business question:** A payment audit needs trips with no recorded
# MAGIC payment method.

# COMMAND ----------

df.filter(F.col("payment_method").isNull()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## `~isin` drops `NULL` column values

# COMMAND ----------

# MAGIC %md
# MAGIC **Business question:** Compliance wants pickup zones other than 74 and 231.

# COMMAND ----------

df.show()

# COMMAND ----------

blocked = [74, 231]
df.filter(~F.col("pickup_location_id").isin(blocked)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### `None` in the `isin` list empties the filter

# COMMAND ----------

# MAGIC %md
# MAGIC **Business question:** Compliance wants to exclude trips with pickup zone **74** or **231**, 
# MAGIC and trips with a missing pickup zone.
# MAGIC
# MAGIC PySpark treats Python **`None`** as `NULL`

# COMMAND ----------

blocked_with_null = [74, 231, None]
df.filter(~F.col("pickup_location_id").isin(blocked_with_null)).show()

# COMMAND ----------

location_allowed = F.col("pickup_location_id").isNull() | ~F.col("pickup_location_id").isin(74, 231)

df.filter(location_allowed).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compare `null` with `eqNullSafe`
# MAGIC
# MAGIC | Comparison | Result |
# MAGIC | --- | --- |
# MAGIC | `"Card"` vs `"Card"` | `TRUE` |
# MAGIC | `NULL` vs `"Card"` | `FALSE` |
# MAGIC | `NULL` vs `NULL` | `TRUE` |

# COMMAND ----------

df.select(
    "trip_id",
    "payment_method",
    (F.col("payment_method") == "Card").alias("is_card"),
    F.col("payment_method").eqNullSafe("Card").alias("is_card_null_safe"),
    F.expr("payment_method <=> 'Card'").alias("is_card_sql_null_safe"),
    F.col("payment_method").eqNullSafe(None).alias("is_missing_null_safe"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chain reward and pickup-zone rules
# MAGIC
# MAGIC **Business question:** Operations needs a per-trip report showing whether each trip passes the reward rule, the pickup-zone rule, and both rules together.
# MAGIC
# MAGIC * **Reward rule:** Payment method is `"Card"` and tip is at least `$2.00`.
# MAGIC * **Pickup-zone rule:** Exclude trips with pickup zone 74 or 231, and trips with a missing pickup zone.

# COMMAND ----------

reward_decisions = df.select(
    "trip_id",
    "payment_method",
    "tip_amount",
    "pickup_location_id"
).filter(location_allowed & qualifies_for_reward )

reward_decisions.show() 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC * A condition can return `TRUE`, `FALSE`, or `NULL`. A filter keeps only rows where the result is `TRUE`.
# MAGIC * Use `isNull()` and `isNotNull()` to check whether a value is missing.
# MAGIC * Remove Python `None` from an `isin(...)` list. Then state separately whether rows with a `NULL` column value should stay or go.
# MAGIC * Use `eqNullSafe(...)` or SQL `<=>` when comparing values that may be `NULL`.
# MAGIC * Name and inspect each rule before combining them. The combined result can also be `NULL`.
# MAGIC
# MAGIC **Next:** `02 - Missing, Blank, and Sentinel Values` covers other forms of missing data, such as blank strings, sentinel values, and `NaN`.
# MAGIC