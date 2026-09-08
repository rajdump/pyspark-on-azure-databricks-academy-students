# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Common DataFrame Actions
# MAGIC
# MAGIC Return types and driver-side memory risk for common pull/check actions.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Choose common actions (`first`, `head`, `take`, `tail`, `isEmpty`, `toPandas`)
# MAGIC   and know their driver-side memory risks
# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up the payments example
# MAGIC
# MAGIC Build a small DataFrame with a few payment rows and different
# MAGIC `tip_amount` values. Course `payment` columns used here: `trip_id`
# MAGIC (`bigint`), `payment_method` (`string`), `base_fare_amount`
# MAGIC (`decimal(10,2)`), and `tip_amount` (`decimal(10,2)`).
# MAGIC
# MAGIC Later, sort by `tip_amount` so the first and last rows are predictable.
# MAGIC
# MAGIC > **Caution:** Actions such as `collect()`, `toPandas()`, and large
# MAGIC > `head(n)` / `take(n)` / `tail(n)` pull rows onto the **driver**. Use them
# MAGIC > only on small, bounded results — a large pull can exhaust driver memory.

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
# MAGIC ## Common DataFrame actions
# MAGIC
# MAGIC An **action** requests a result or writes the DataFrame, so Spark executes
# MAGIC the plan.
# MAGIC
# MAGIC | Action | Return type | Size risk |
# MAGIC |---|---|---|
# MAGIC | `show()` | Displays rows (returns `None`) | Low (display only) |
# MAGIC | `count()` | `int` | Low |
# MAGIC | `collect()` | `list` of all `Row`s | High if the Spark result is large |
# MAGIC | `first()` | One `Row` | Low |
# MAGIC | `head()` | One `Row` | Low |
# MAGIC | `head(n)` / `take(n)` | `list` of `n` `Row`s | Grows with `n` |
# MAGIC | `tail(n)` | `list` of `n` `Row`s | Grows with `n` |
# MAGIC | `isEmpty()` | `True` or `False` | Low (no row payload) |
# MAGIC | `toPandas()` | pandas `DataFrame` (all rows) | High if the Spark result is large |
# MAGIC | `write.save()` / `write.saveAsTable()` | Writes output | Storage, not driver memory |
# MAGIC
# MAGIC You already used `show()`, `count()`, and `collect()`. This notebook demos
# MAGIC the other pull/check actions. Writing is covered in Module 5.
# MAGIC
# MAGIC Prefer small `n` for `head` / `take` / `tail`. Use `collect()` and
# MAGIC `toPandas()` only when the returned result is small.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retrieve the first rows
# MAGIC
# MAGIC Sort by `tip_amount` ascending so the first rows are predictable. Then
# MAGIC compare `first()`, `head()`, `head(n)`, and `take(n)`.
# MAGIC
# MAGIC `first()` and `head()` return one `Row`; `head(n)` and `take(n)` return a
# MAGIC `list` of `Row`s.

# COMMAND ----------

ordered = payments.orderBy(F.col("tip_amount"))
ordered.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### `first()`
# MAGIC
# MAGIC Returns one `Row` — the first row after the sort.

# COMMAND ----------

first_row = ordered.first()
print("first() returned:", type(first_row).__name__)
print(first_row)

# COMMAND ----------

# MAGIC %md
# MAGIC ### `head()`
# MAGIC
# MAGIC With no argument, `head()` also returns one `Row`.

# COMMAND ----------

head_row = ordered.head()
print("head() returned:", type(head_row).__name__)
print(head_row)

# COMMAND ----------

# MAGIC %md
# MAGIC ### `head(n)`
# MAGIC
# MAGIC Returns a Python `list` of the first `n` rows.

# COMMAND ----------

head_rows = ordered.head(3)
print("head(3) returned:", type(head_rows).__name__, "len =", len(head_rows))
head_rows  # noqa: B018 -- bare expression triggers Databricks' rich cell display

# COMMAND ----------

# MAGIC %md
# MAGIC ### `take(n)`
# MAGIC
# MAGIC Returns a Python `list` of the first `n` rows (same idea as `head(n)`).

# COMMAND ----------

take_rows = ordered.take(3)
print("take(3) returned:", type(take_rows).__name__, "len =", len(take_rows))
take_rows  # noqa: B018 -- bare expression triggers Databricks' rich cell display

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retrieve the last rows with `tail()`
# MAGIC
# MAGIC `tail(n)` returns the last `n` rows as a Python `list`.
# MAGIC
# MAGIC Spark DataFrames have no guaranteed row order unless you sort. We already
# MAGIC sorted by `tip_amount`, so the last rows are the highest tips.

# COMMAND ----------

tail_rows = ordered.tail(3)
print("tail(3) returned:", type(tail_rows).__name__, "len =", len(tail_rows))
tail_rows  # noqa: B018 -- bare expression triggers Databricks' rich cell display

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check whether a DataFrame is empty
# MAGIC
# MAGIC `isEmpty()` returns `True` or `False`. Prefer it over `count() == 0` when
# MAGIC you only need a yes/no check — `count()` must count every row; `isEmpty()`
# MAGIC can stop after finding one.

# COMMAND ----------

print("ordered.isEmpty():", ordered.isEmpty())

empty_df = ordered.filter(F.col("tip_amount") < F.lit(0))
print("empty filter isEmpty():", empty_df.isEmpty())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert a small result to pandas
# MAGIC
# MAGIC `toPandas()` returns the complete Spark result as a pandas DataFrame on
# MAGIC the driver.
# MAGIC
# MAGIC Use it only for small, bounded DataFrames. A large result can exhaust
# MAGIC driver memory — the same risk as `collect()`. Writing results is also an
# MAGIC action, but `DataFrame.write` itself only returns a writer interface — a
# MAGIC terminal method such as `.save()` or `.saveAsTable()` triggers execution.
# MAGIC Module 5 covers it.

# COMMAND ----------

pdf = ordered.toPandas()
print("toPandas() returned:", type(pdf).__name__, "shape =", pdf.shape)
pdf  # noqa: B018 -- bare expression triggers Databricks' rich cell display

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC Build an isolated example (do not reuse DataFrames from earlier sections):
# MAGIC
# MAGIC 1. Create **`exercise_df`** with **`trip_id`** (`bigint`),
# MAGIC    **`payment_method`** (`string`), **`base_fare_amount`**
# MAGIC    (`decimal(10,2)`), and **`tip_amount`** (`decimal(10,2)`). Use four to
# MAGIC    six small rows with mixed tip amounts.
# MAGIC 2. Sort by **`tip_amount`** descending into **`exercise_ordered`**.
# MAGIC 3. Call **`first()`**, **`take(2)`**, and **`tail(2)`**. Print the return
# MAGIC    type of each.
# MAGIC 4. Call **`isEmpty()`** on **`exercise_ordered`**, then on a filter that
# MAGIC    matches no rows.
# MAGIC 5. Call **`toPandas()`** once on this small result. Add a one-line note
# MAGIC    about why you would not call it on a large production DataFrame.

# COMMAND ----------

# Your code here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Recap this notebook's path:
# MAGIC
# MAGIC - **Sort first** when you need predictable `first` / `head` / `take` /
# MAGIC   `tail` results — DataFrames have no guaranteed order otherwise
# MAGIC - **`first()`** and **`head()`** return one `Row`; **`head(n)`**,
# MAGIC   **`take(n)`**, and **`tail(n)`** return a `list` — size risk grows with
# MAGIC   `n`
# MAGIC - **`isEmpty()`** returns `True` or `False`; prefer it over
# MAGIC   `count() == 0` when you only need emptiness
# MAGIC - **`collect()`** and **`toPandas()`** move the full result to the driver
# MAGIC   — keep the DataFrame small
# MAGIC - **Writing** is also an action, but `DataFrame.write` itself returns a
# MAGIC   writer interface — a terminal method such as `.save()` or
# MAGIC   `.saveAsTable()` triggers it; Module 5 covers it
# MAGIC
# MAGIC **Module 4 complete.**
# MAGIC
# MAGIC Next up: **Module 5 — Reading, Writing, and Schemas** — bring files and
# MAGIC tables in, and use `DataFrame.write` to save results.
