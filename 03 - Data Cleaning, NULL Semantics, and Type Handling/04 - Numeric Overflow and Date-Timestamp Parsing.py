# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Numeric Overflow and Date-Timestamp Parsing
# MAGIC
# MAGIC Keep overflow and unparseable dates from failing the job.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Use `+` and see overflow fail the job under ANSI
# MAGIC - Use `try_add` so overflow becomes `NULL` and the job continues
# MAGIC - Parse dates with `try_to_date` / `try_to_timestamp` and find failed
# MAGIC   conversions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach classic **all-purpose** compute or serverless

# COMMAND ----------

from pyspark.sql import functions as F

rows = [
    (1001, 30, "2026-03-01"),
    (1002, 2147483647, "2026-03-02"),
    (1003, 8, "not-a-date"),
    (1004, 10, None),
]

schema_ddl = "trip_id bigint, ride_duration_mins int, trip_date string"

df = spark.createDataFrame(  # pyright: ignore[reportUndefinedVariable]  # noqa: F821
    rows,
    schema_ddl,
)

# COMMAND ----------

df.show()
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overflow fails the job
# MAGIC
# MAGIC **Business question:** Operations needs `ride_duration_mins * 2` for
# MAGIC capacity planning. Trip `1002` already holds the largest `int`.
# MAGIC
# MAGIC Under ANSI, adding that value to itself fails the job.

# COMMAND ----------

# Expected: ARITHMETIC_OVERFLOW
df.select(
    "trip_id",
    (F.col("ride_duration_mins") + F.col("ride_duration_mins")).alias(
        "doubled_duration_mins"
    ),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Return `NULL` with `try_add`
# MAGIC
# MAGIC **Business question:** Operations still needs the doubled duration. Keep
# MAGIC values that overflow as `NULL` so the job finishes.
# MAGIC
# MAGIC `try_add` works like `+`, but overflow becomes `NULL`.
# MAGIC
# MAGIC > **Warning:** Do not turn off ANSI for the session. Use `try_*` on the
# MAGIC > expression.

# COMMAND ----------

safe_durations = df.select(
    "trip_id",
    "ride_duration_mins",
    F.try_add(
        F.col("ride_duration_mins"),
        F.col("ride_duration_mins"),
    ).alias("doubled_duration_mins"),
)

safe_durations.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Trip `1001` doubles to `60`. Trip `1002` overflows, so the result is
# MAGIC `NULL`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## `to_date` fails the job
# MAGIC
# MAGIC **Business question:** Operations needs `trip_date` as a real date. The
# MAGIC column is text and includes `"not-a-date"`.
# MAGIC
# MAGIC `to_date` and `to_timestamp` need a format. Spark does not guess it.
# MAGIC
# MAGIC - `yyyy` — year; `MM` — month; `dd` — day
# MAGIC
# MAGIC Spark uses the session timezone when it builds a timestamp from date
# MAGIC text.

# COMMAND ----------

print("Session timezone:", spark.conf.get("spark.sql.session.timeZone"))  # noqa: F821

# COMMAND ----------

# Expected: CAST_INVALID_INPUT
df.select(
    "trip_id",
    F.to_date(F.col("trip_date"), "yyyy-MM-dd").alias("trip_date"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Return `NULL` with `try_to_date` and `try_to_timestamp`
# MAGIC
# MAGIC **Business question:** Operations still needs the conversion. Keep values
# MAGIC that cannot parse as `NULL` so the job finishes.

# COMMAND ----------

parsed = df.select(
    "trip_id",
    "trip_date",
    F.try_to_date(F.col("trip_date"), F.lit("yyyy-MM-dd")).alias("trip_date_typed"),
    F.try_to_timestamp(F.col("trip_date"), F.lit("yyyy-MM-dd")).alias(
        "trip_timestamp"
    ),
)

parsed.show(truncate=False)
parsed.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC `"2026-03-01"` and `"2026-03-02"` parse. `"not-a-date"` and the original
# MAGIC missing date are `NULL`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handle failed conversions
# MAGIC
# MAGIC **Business question:** We need to identify trips where a date was present,
# MAGIC but parsing failed.

# COMMAND ----------

rejected = parsed.filter(
    F.col("trip_date").isNotNull() & F.col("trip_date_typed").isNull()
)

rejected.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC * Overflow fails the job under ANSI. `try_add` writes `NULL` and the job
# MAGIC   continues.
# MAGIC * `to_date` / `to_timestamp` need an explicit format. Invalid text fails
# MAGIC   the job under ANSI.
# MAGIC * `try_to_date` / `try_to_timestamp` write `NULL` for invalid text.
# MAGIC * A failed conversion has a source value and a `NULL` result. An original
# MAGIC   `NULL` is not a failed conversion.
# MAGIC
# MAGIC **Next:** Module 4 — Transformations, Actions, and Lazy Evaluation.
