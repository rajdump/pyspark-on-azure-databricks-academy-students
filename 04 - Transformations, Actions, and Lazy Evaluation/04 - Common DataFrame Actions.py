# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Common DataFrame Actions
# MAGIC
# MAGIC Pull a few rows to the driver. Know what each action returns and what it
# MAGIC costs.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Choose `first`, `head`, `take`, `tail`, `isEmpty`, and `toPandas`
# MAGIC - Know which of those pull a large result onto the driver

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach classic **all-purpose** compute
# MAGIC
# MAGIC This notebook rebuilds the same trips as
# MAGIC `01 - Transformations vs Actions`. Run this setup. Do not depend on an
# MAGIC earlier notebook still being in the session.
# MAGIC
# MAGIC > **Warning:** `collect()`, `toPandas()`, and large `head(n)` / `take(n)` /
# MAGIC > `tail(n)` pull rows onto the **driver**. Use them only on small results.

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

trips.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sort so first and last rows are predictable
# MAGIC
# MAGIC You already used `show()` and `count()`. This notebook demos the other
# MAGIC pull/check actions. Writes wait for Module 5.
# MAGIC
# MAGIC **Business question:** Operations wants the cheapest trips that have a
# MAGIC fare. Spark DataFrames have no guaranteed order until you sort.

# COMMAND ----------

ordered = (
    trips.filter(F.col("base_fare_amount").isNotNull())
    .orderBy(F.col("base_fare_amount"))
)

ordered.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Five trips. Lowest fare is trip `1003` (`6.20`). Highest is trip `1001`
# MAGIC (`12.50`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## `first` and `head`
# MAGIC
# MAGIC `first()` and `head()` with no argument each return one `Row`.

# COMMAND ----------

first_row = ordered.first()
print("first() returned:", type(first_row).__name__)
print(first_row)

# COMMAND ----------

head_row = ordered.head()
print("head() returned:", type(head_row).__name__)
print(head_row)

# COMMAND ----------

# MAGIC %md
# MAGIC Both are trip `1003`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## `head(n)` and `take(n)`
# MAGIC
# MAGIC Both return a Python `list` of the first `n` rows. Size risk grows with
# MAGIC `n`.

# COMMAND ----------

head_rows = ordered.head(3)
print("head(3) returned:", type(head_rows).__name__, "len =", len(head_rows))
head_rows  # noqa: B018

# COMMAND ----------

take_rows = ordered.take(3)
print("take(3) returned:", type(take_rows).__name__, "len =", len(take_rows))
take_rows  # noqa: B018

# COMMAND ----------

# MAGIC %md
# MAGIC Trips `1003`, `1002`, and `1006` — the three cheapest fares.

# COMMAND ----------

# MAGIC %md
# MAGIC ## `tail(n)`
# MAGIC
# MAGIC `tail(n)` returns the last `n` rows as a Python `list`. Because we sorted
# MAGIC by fare, those are the highest fares.

# COMMAND ----------

tail_rows = ordered.tail(3)
print("tail(3) returned:", type(tail_rows).__name__, "len =", len(tail_rows))
tail_rows  # noqa: B018

# COMMAND ----------

# MAGIC %md
# MAGIC Trips `1006`, `1005`, and `1001`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## `isEmpty`
# MAGIC
# MAGIC `isEmpty()` returns `True` or `False`. Prefer it over `count() == 0` when
# MAGIC you only need a yes/no check. `count()` walks every row.

# COMMAND ----------

print("ordered.isEmpty():", ordered.isEmpty())

empty_df = ordered.filter(F.col("base_fare_amount") < F.lit(0))
print("negative-fare isEmpty():", empty_df.isEmpty())

# COMMAND ----------

# MAGIC %md
# MAGIC ## `toPandas`
# MAGIC
# MAGIC `toPandas()` returns the complete Spark result as a pandas DataFrame on
# MAGIC the driver. Same size risk as `collect()`. Use it only on a small,
# MAGIC bounded result.
# MAGIC
# MAGIC `DataFrame.write` returns a writer. `.save()` / `.saveAsTable()` run the
# MAGIC write. Module 5 covers that.

# COMMAND ----------

pdf = ordered.toPandas()
print("toPandas() returned:", type(pdf).__name__, "shape =", pdf.shape)
pdf  # noqa: B018

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC * Sort first when `first` / `head` / `take` / `tail` must be predictable.
# MAGIC * `first()` and `head()` return one `Row`. `head(n)`, `take(n)`, and
# MAGIC   `tail(n)` return a `list`.
# MAGIC * `isEmpty()` is a yes/no check. Prefer it over `count() == 0`.
# MAGIC * `collect()` and `toPandas()` move the full result to the driver. Keep
# MAGIC   the DataFrame small.
# MAGIC * Writes are actions too. Module 5 covers `DataFrame.write`.
# MAGIC
# MAGIC **Next:** Module 5 — Reading, Writing, and Schemas.
