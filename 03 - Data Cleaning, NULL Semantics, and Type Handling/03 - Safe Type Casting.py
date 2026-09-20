# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Safe Type Casting
# MAGIC
# MAGIC Convert text fares to numbers without failing the job.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Use `cast` and see invalid text fail the job under ANSI
# MAGIC - Use `try_cast` so invalid text becomes `NULL` and the job continues
# MAGIC - Find rejected rows with `source.isNotNull() & casted.isNull()`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach classic **all-purpose** compute or serverless

# COMMAND ----------

from pyspark.sql import functions as F

rows = [
    (1001, "12.50"),
    (1002, "N/A"),
    (1003, "8.00"),
    (1004, None),
]

schema_ddl = "trip_id bigint, base_fare_amount string"

df = spark.createDataFrame(  # pyright: ignore[reportUndefinedVariable]  # noqa: F821
    rows,
    schema_ddl,
)

# COMMAND ----------

df.show()
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## `cast` fails the job
# MAGIC
# MAGIC **Business question:** Finance needs `base_fare_amount` as `decimal(10,2)`.
# MAGIC The column is text and includes `"N/A"`.
# MAGIC
# MAGIC `cast` converts a column to a new type. Under ANSI, invalid text fails the
# MAGIC job.

# COMMAND ----------

# Expected: CAST_INVALID_INPUT
df.select(
    "trip_id",
    F.col("base_fare_amount").cast("decimal(10,2)").alias("base_fare_amount"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Return `NULL` with `try_cast`
# MAGIC
# MAGIC **Business question:** Finance still needs the conversion. Keep values that
# MAGIC cannot convert as `NULL` so the job finishes.
# MAGIC
# MAGIC `try_cast` works like `cast`, but invalid text becomes `NULL`.
# MAGIC
# MAGIC > **Warning:** Do not turn off ANSI for the session. Use `try_cast` on the
# MAGIC > column.

# COMMAND ----------

typed = df.select(
    "trip_id",
    "base_fare_amount",
    F.col("base_fare_amount").try_cast("decimal(10,2)").alias("base_fare_amount_typed"),
)

typed.show()
typed.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC `"12.50"` and `"8.00"` convert. `"N/A"` and the original missing fare are
# MAGIC `NULL`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find rejected rows
# MAGIC
# MAGIC **Business question:** Operations needs the trips whose fare could not be
# MAGIC converted, not trips that were already missing.

# COMMAND ----------

rejected = typed.filter(
    F.col("base_fare_amount").isNotNull() & F.col("base_fare_amount_typed").isNull()
)

rejected.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Trip `1002` had `"N/A"`. Trip `1004` was already `NULL`. Only `1002` is
# MAGIC rejected.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC * `cast` converts valid text. Invalid text fails the job under ANSI.
# MAGIC * `try_cast` writes `NULL` for invalid text. The job continues.
# MAGIC * A rejected row has a source value and a `NULL` cast result. An original
# MAGIC   `NULL` is not a rejected row.
# MAGIC
# MAGIC **Next:** `04 - Numeric Overflow and Date-Timestamp Parsing` covers
# MAGIC overflow and unparseable dates with `try_sum`, `try_avg`, `try_to_date`,
# MAGIC and `try_to_timestamp`.
