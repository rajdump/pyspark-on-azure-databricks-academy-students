# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 04 - SQL Expressions in DataFrame Code
# MAGIC
# MAGIC Read **SQL Expressions in DataFrame Code** in the course Notion hub first.
# MAGIC Then attach classic all-purpose compute and run the cells below.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Use `F.expr` and `selectExpr`, including SQL `CASE WHEN`
# MAGIC - Reuse named SQL strings

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach classic **all-purpose** compute. No `%sql` — that is notebook 06.

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
# MAGIC Confirm the sample rows.

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build a Column with `F.expr`
# MAGIC
# MAGIC SQL string → Column, not a DataFrame. Reuse `mph_sql` later.

# COMMAND ----------

mph_sql = "round(trip_distance_miles / (ride_duration_mins / 60.0), 1) AS mph"

df.select(
    "trip_id",
    F.expr(mph_sql),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply several SQL strings with `selectExpr`
# MAGIC
# MAGIC SQL strings → new DataFrame. Pass `mph_sql` directly. No `F.expr`.

# COMMAND ----------

df.selectExpr(
    "trip_id",
    "upper(service_type) AS service_type_upper",
    mph_sql,
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conditional logic with SQL `CASE WHEN`
# MAGIC
# MAGIC Same bands as notebook 03 `F.when`. New name → add with `F.expr`.

# COMMAND ----------

distance_band_sql = """
CASE
    WHEN trip_distance_miles < 5 THEN 'short'
    WHEN trip_distance_miles <= 15 THEN 'medium'
    ELSE 'long'
END
"""

labelled_with_expr = df.select("trip_id", "trip_distance_miles", F.expr(distance_band_sql).alias("distance_band")) 

labelled_with_expr.show()

# COMMAND ----------

# MAGIC %md
# MAGIC `selectExpr` names the `CASE` in one step with `AS distance_band`.

# COMMAND ----------

labelled_with_select_expr = df.selectExpr(
    "trip_id",
    "trip_distance_miles",
    distance_band_sql + " AS distance_band",
)

labelled_with_select_expr.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the final output with selectExpr

# COMMAND ----------

operations_dashboard = df.selectExpr(
    "cast(trip_id as string) AS trip_id",
    "CASE WHEN service_type = 'Shared' THEN 'Pool' ELSE service_type END AS service_type",
    "trip_distance_miles * 1.60934 AS trip_distance_km",
    distance_band_sql + " AS distance_band",
    "'mobile_app' AS source_system",
    "ride_duration_mins AS duration_mins",
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
# MAGIC - `F.expr` — SQL string → Column
# MAGIC - `selectExpr` — SQL strings → new DataFrame
# MAGIC - `CASE WHEN` — SQL form of `F.when`
# MAGIC - Reuse named SQL strings
# MAGIC
# MAGIC Next up: `05 - Filtering Rows`.