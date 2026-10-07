# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Common DataFrame Actions
# MAGIC
# MAGIC Hands-on practice for common DataFrame actions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach classic **all-purpose** compute.
# MAGIC > **Warning:** `collect()`, `toPandas()`, and large values of `head(n)`,
# MAGIC > `take(n)`, or `tail(n)` can bring many rows to the Driver. Use them
# MAGIC > only when the result is small enough for Driver memory.

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
# MAGIC ## `show()` and `count()`
# MAGIC
# MAGIC Display rows and return the row count.

# COMMAND ----------

trips.show()
print("count():", trips.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## `first()` and `head()`
# MAGIC
# MAGIC Both return a single row.

# COMMAND ----------

first_row = trips.first()
first_row

# COMMAND ----------

head_row = trips.head()
head_row

# COMMAND ----------

# MAGIC %md
# MAGIC ## `head(n)` and `take(n)`
# MAGIC
# MAGIC Both return up to `n` rows.

# COMMAND ----------

head_rows = trips.head(3)
head_rows

# COMMAND ----------

take_rows = trips.take(3)
take_rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## `tail(n)`
# MAGIC
# MAGIC Returns the last `n` rows.

# COMMAND ----------

tail_rows = trips.tail(3)
tail_rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## `isEmpty()`
# MAGIC
# MAGIC Returns `True` if the DataFrame has no rows.

# COMMAND ----------

print("trips.isEmpty():", trips.isEmpty())

empty_df = trips.filter(F.col("base_fare_amount") < F.lit(0))

print("negative-fare isEmpty():", empty_df.isEmpty())

# COMMAND ----------

# MAGIC %md
# MAGIC ## `collect()`
# MAGIC
# MAGIC Returns all rows to the Driver.

# COMMAND ----------

collected_rows = trips.collect()

collected_rows  # noqa: B018

# COMMAND ----------

# MAGIC %md
# MAGIC ## `toPandas()`
# MAGIC
# MAGIC Converts the result to a pandas DataFrame on the Driver.

# COMMAND ----------

pdf = trips.toPandas()
pdf  # noqa: B018

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reduce the result before `collect()`
# MAGIC
# MAGIC Reduce with `filter`, `select`, and `limit` first.

# COMMAND ----------

small_result = (
    trips.filter(F.col("base_fare_amount").isNotNull())
    .select("trip_id", "pickup_zone", "base_fare_amount")
    .limit(3)
)

small_result.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC **Next:** Module 5 — Reading, Writing, and Schemas.