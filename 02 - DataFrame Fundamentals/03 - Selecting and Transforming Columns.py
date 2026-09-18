# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 03 - Selecting and Transforming Columns
# MAGIC
# MAGIC Read **Selecting and Transforming Columns** in the course Notion hub first.
# MAGIC Then attach classic all-purpose compute and run the cells below.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Select, add, rename, recalculate, and drop columns
# MAGIC - Transforms return a **new** DataFrame.
# MAGIC - Build Column expressions with `F.col`, `alias`, light `cast`, `F.lit`, and
# MAGIC   `F.when` / `otherwise`
# MAGIC - Choose `select` vs `withColumn` and chain into a small ops-style output

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach classic **all-purpose** compute.

# COMMAND ----------

from decimal import Decimal

from pyspark.sql import functions as F

rows = [
    (1001, "Standard", 138, Decimal("12.40"), 18),
    (1002, "Shared", 74, Decimal("3.10"), 9),
    (1003, "Premium", 231, Decimal("22.70"), 35),
    (1004, "Standard", 100, Decimal("5.60"), 14),
    (1005, "Shared", 74, Decimal("2.20"), 7),
]

schema_ddl = (
    "trip_id bigint, service_type string, pickup_location_id int, "
    "trip_distance_miles decimal(8,2), ride_duration_mins int"
)

df = spark.createDataFrame(rows, schema_ddl)  # pyright: ignore[reportUndefinedVariable]  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC Confirm the sample rows before reshaping.

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select and reorder columns
# MAGIC
# MAGIC Only the columns you name, in that order. Strings are enough here.

# COMMAND ----------

df.select("trip_id", "service_type", "trip_distance_miles").show()

# COMMAND ----------

# MAGIC %md
# MAGIC `select` also reorders columns.

# COMMAND ----------

df.select("service_type", "trip_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### `select` returns a new DataFrame
# MAGIC
# MAGIC `df` is unchanged. Assign the result to keep it.

# COMMAND ----------

selected = df.select("trip_id", "service_type")
print("new DataFrame columns:", selected.columns)
print("original df columns:  ", df.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build column expressions with `F.col`
# MAGIC
# MAGIC A column-name string cannot build expressions.
# MAGIC
# MAGIC Use `F.col("name")` when you need an expression, such as an alias, arithmetic, cast, or condition.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alias

# COMMAND ----------

df.select(
    "trip_id",
    # A column-name string cannot apply an alias inside select().
    # "trip_distance_miles as distance_mi",
    F.col("trip_distance_miles").alias("distance_mi"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Arithmetic
# MAGIC
# MAGIC Define the kilometre expression once, then reuse it.

# COMMAND ----------

km_expr = F.col("trip_distance_miles") * 1.60934

df.select(
    "trip_id",
    "trip_distance_miles",
    # A column-name string cannot perform arithmetic.
    # "trip_distance_miles" * 1.60934,
    km_expr.alias("trip_distance_km"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cast
# MAGIC
# MAGIC `cast` changes type. Cast with intent.

# COMMAND ----------

df.select(
    F.col("trip_id").cast("string").alias("trip_id_str"),
    "trip_distance_miles",
).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Constants with `F.lit`
# MAGIC
# MAGIC `F.lit` is the same constant on every row. It is not `F.col`.

# COMMAND ----------

df.select("trip_id", F.lit("mobile_app").alias("source_system")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conditional columns with `F.when`
# MAGIC
# MAGIC If/else on a column. Without `otherwise`, unmatched rows are `NULL`.

# COMMAND ----------

distance_band_expr = (
    F.when(F.col("trip_distance_miles") < 5, "short")
    .when(F.col("trip_distance_miles") <= 15, "medium")
    .otherwise("long")
)

df.select(
    "trip_id",
    "trip_distance_miles",
    distance_band_expr.alias("distance_band"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add column with `withColumn`
# MAGIC
# MAGIC New name → add. Reuses `km_expr`.

# COMMAND ----------

df_km = df.withColumn("trip_distance_km", km_expr)
df_km.show()

# COMMAND ----------

print("df columns:    ", df.columns)
print("df_km columns: ", df_km.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add a column with `select`
# MAGIC
# MAGIC To keep all existing columns plus one new column, use withColumn or select("*", expression).
# MAGIC
# MAGIC To keep only certain columns plus one new column, use select with the required columns and the expression.
# MAGIC
# MAGIC This avoids the extra step of adding a column first and then dropping unwanted columns.
# MAGIC

# COMMAND ----------

df_km_sel_cols = df.select(
    "trip_id",
    "trip_distance_miles",
    km_expr.alias("trip_distance_km"),
)

df_km_sel_cols.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Choose by the output shape you want, not by habit.

# COMMAND ----------

# MAGIC %md
# MAGIC `withColumn` can also build a true/false flag.

# COMMAND ----------

df.withColumn("is_long_trip", F.col("trip_distance_miles") > 15).select(
    "trip_id", "trip_distance_miles", "is_long_trip"
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recalculate existing column values with `withColumn` to prevent duplicate column names.

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumn
# MAGIC
# MAGIC Name already exists → recalculate that column. `Shared` becomes `Pool`.
# MAGIC One `service_type`. Other columns stay.

# COMMAND ----------

service_type_expr = F.when(F.col("service_type") == "Shared", F.lit("Pool")).otherwise(
    F.col("service_type")
)

df.withColumn("service_type", service_type_expr).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### select
# MAGIC
# MAGIC `select("*", expr.alias("service_type"))` keeps the original and adds a second
# MAGIC `service_type`. Look for two columns with that name.

# COMMAND ----------

df.select("*", service_type_expr.alias("service_type")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Several columns at once: `withColumns`
# MAGIC
# MAGIC Add several columns in one step.

# COMMAND ----------

df.withColumns(
    {
        "trip_distance_km": km_expr,
        "is_long_trip": distance_band_expr,
        "service_type": service_type_expr
    }
).select("trip_id", "trip_distance_km", "is_long_trip","service_type").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rename columns
# MAGIC
# MAGIC `withColumnRenamed` / `withColumnsRenamed` keep the other columns.

# COMMAND ----------

df.withColumnRenamed("ride_duration_mins", "duration_mins").show()

# COMMAND ----------

df.withColumnsRenamed(
    {
        "service_type": "ride_type",
        "trip_distance_miles": "distance_miles",
    }
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove columns with `drop`
# MAGIC
# MAGIC Missing names do not raise an error.

# COMMAND ----------

df_km = df.withColumns(
    {
        "trip_distance_km": km_expr,
        "is_long_trip": distance_band_expr,
        "service_type": service_type_expr
    }
)

# COMMAND ----------

df_km.show()

# COMMAND ----------

df_km.drop("trip_distance_miles", "pickup_location_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chain transforms into one output
# MAGIC
# MAGIC Reuse `km_expr`, `distance_band_expr`, and `service_type_expr`.

# COMMAND ----------

operations_dashboard = (
    df.select(
        F.col("trip_id").cast("string").alias("trip_id"),
        "service_type",
        "trip_distance_miles",
        "ride_duration_mins",
    )
    .withColumns(
        {
            "trip_distance_km": km_expr,
            "distance_band": distance_band_expr,
            "source_system": F.lit("mobile_app"),
        }
    )
    .withColumn("service_type", service_type_expr)
    .withColumnRenamed("ride_duration_mins", "duration_mins")
    .drop("trip_distance_miles")
)

operations_dashboard.show()
operations_dashboard.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Confirm `df` is unchanged.

# COMMAND ----------

print("operations_dashboard columns:", operations_dashboard.columns)
print("original df columns:         ", df.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - `select` projects and reorders. `F.col` builds expressions.
# MAGIC - `withColumn` / `withColumns` add or recalculate. Rename and `drop` shape output.
# MAGIC - Chain into one new DataFrame. Source `df` stays unchanged.
# MAGIC
# MAGIC Next up: `04 - SQL Expressions in DataFrame Code`.