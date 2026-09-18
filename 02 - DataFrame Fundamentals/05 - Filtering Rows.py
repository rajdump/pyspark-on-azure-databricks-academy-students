# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 05 - Filtering Rows
# MAGIC
# MAGIC Read **Filtering Rows** in the course Notion hub first. Then attach
# MAGIC classic all-purpose compute and run the cells below.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Filter with Column ops and SQL strings; combine with `AND` vs `&`
# MAGIC - Use `|`, `~`, `isin`, `between`, `like`
# MAGIC - Apply intro NULL checks (`isNull` / `isNotNull`); empty string ≠ NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach classic **all-purpose** compute. This frame includes NULL, empty-string,
# MAGIC and negative-distance rows.

# COMMAND ----------

from decimal import Decimal

from pyspark.sql import functions as F

rows = [
    (1001, "Standard", 138, Decimal("12.40"), 18),
    (1002, "Shared", 74, Decimal("3.10"), 9),
    (1003, "Premium", 231, Decimal("22.70"), 35),
    (1004, "Standard", 100, Decimal("5.60"), 14),
    (1005, "Shared", 74, Decimal("2.20"), 7),
    (1006, "Premium", 138, Decimal("18.00"), None),
    (1007, None, 161, Decimal("8.30"), 19),
    (1008, "", 90, Decimal("4.50"), 12),
    (1009, "Standard", 100, Decimal("-1.00"), 10),
]

schema_ddl = (
    "trip_id bigint, service_type string, pickup_location_id int, "
    "trip_distance_miles decimal(8,2), ride_duration_mins int"
)

df = spark.createDataFrame(rows, schema_ddl)  # pyright: ignore[reportUndefinedVariable]  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC Confirm the sample rows.

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter with `filter` / `where`
# MAGIC
# MAGIC **Business question:** Dispatch planning needs trips longer than ten miles.

# COMMAND ----------

df.filter(F.col("trip_distance_miles") > 10).show()

# COMMAND ----------

df.where(F.col("trip_distance_miles") > 10).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine conditions
# MAGIC
# MAGIC **Business question:** A service-quality report needs Standard trips longer
# MAGIC than six miles.

# COMMAND ----------

df.filter("service_type = 'Standard' AND trip_distance_miles > 6").show()

# COMMAND ----------

standard_long_sql = "service_type = 'Standard' AND trip_distance_miles > 6"

df.filter(F.expr(standard_long_sql)).show()

# COMMAND ----------

df.filter((F.col("service_type") == "Standard") & (F.col("trip_distance_miles") > 6)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Do not use Python `and`, `or`, or `not`
# MAGIC
# MAGIC Python `and`, `or`, and `not` do not work with PySpark Column expressions. Use `&`, `|`, and `~` instead.
# MAGIC

# COMMAND ----------

try:
    df.filter((F.col("service_type") == "Standard") and (F.col("trip_distance_miles") > 6)).show()
except Exception as e:
    print(f"Python and on Columns — {type(e).__name__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter with `|`
# MAGIC
# MAGIC **Business question:** Peak-hour analysis needs Premium trips or any trip
# MAGIC longer than twenty miles.

# COMMAND ----------

df.filter((F.col("service_type") == "Premium") | (F.col("trip_distance_miles") > 20)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reverse with `~`
# MAGIC
# MAGIC **Business question:** A mix report needs every trip except Shared.

# COMMAND ----------

df.filter(~(F.col("service_type") == "Shared")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Match several values with `isin`
# MAGIC
# MAGIC **Business question:** A fare review needs Standard and Premium trips only.

# COMMAND ----------

df.filter(F.col("service_type").isin("Standard", "Premium")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inclusive range with `between`
# MAGIC
# MAGIC **Business question:** A mid-range report needs trips from 3 to 12 miles.

# COMMAND ----------

df.filter(F.col("trip_distance_miles").between(3, 12)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pattern match with `like`
# MAGIC **Business question:** `service_type` values that start with `S`.

# COMMAND ----------

df.filter(F.col("service_type").like("S%")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Intro NULL
# MAGIC
# MAGIC **Business question:** A data-quality review needs rows where `service_type`
# MAGIC was never captured.

# COMMAND ----------

print("== None row count:", df.filter(F.col("service_type") == None).count())  # noqa: E711

# COMMAND ----------

print("!= None row count:", df.filter(F.col("service_type") != None).count())  # noqa: E711

# COMMAND ----------

# MAGIC %md
# MAGIC `isNull()` finds the missing `service_type`.

# COMMAND ----------

df.filter(F.col("service_type").isNull()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC `isNotNull()` keeps a value, including an empty string.

# COMMAND ----------

df.filter(F.col("service_type").isNotNull()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Empty string is not NULL
# MAGIC
# MAGIC **Business question:** A validation report needs rows where `service_type` was
# MAGIC submitted as blank — not missing, but empty.

# COMMAND ----------

df.filter(F.col("service_type") == "").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chain a filter into one output
# MAGIC
# MAGIC **Business question:** A downstream dashboard needs trips with a known,
# MAGIC non-empty service type and positive distance.

# COMMAND ----------

is_usable = (
    F.col("service_type").isNotNull()
    & (F.col("service_type") != "")
    & F.col("trip_distance_miles").isNotNull()
    & (F.col("trip_distance_miles") > 0)
)

distance_band_expr = (
    F.when(F.col("trip_distance_miles") < 5, "short")
    .when(F.col("trip_distance_miles") <= 15, "medium")
    .otherwise("long")
)

usable_trips = (
    df.filter(is_usable)
    .withColumn("distance_band", distance_band_expr)
    .withColumn("source_system", F.lit("mobile_app"))
    .select(
        "trip_id",
        "service_type",
        "trip_distance_miles",
        "distance_band",
        "source_system",
    )
)

usable_trips.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Confirm `df` is unchanged.

# COMMAND ----------

print("usable_trips row count:", usable_trips.count())
print("original df row count: ", df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - `filter` / `where` — same method; new DataFrame
# MAGIC - Combine with SQL `AND` or Column `&` — not Python `and`
# MAGIC - `|`, `~`, `isin`, `between`, `like`
# MAGIC - `isNull()` / `isNotNull()` — not `== None`
# MAGIC - Empty string is not NULL
# MAGIC
# MAGIC Next up: `06 - Querying DataFrames with SQL`.