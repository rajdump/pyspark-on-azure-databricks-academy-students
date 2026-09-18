# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 01 - Creating DataFrames
# MAGIC
# MAGIC Read **Creating DataFrames** in the course Notion hub first. Then attach
# MAGIC classic all-purpose compute and run the cells below.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Create DataFrames unnamed/inferred, named/inferred, named with DDL, and named
# MAGIC   with `StructType`
# MAGIC - Inspect each path and explain inferred vs production risk

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach classic **all-purpose** compute before you run any code cells.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Without columns and without schema
# MAGIC
# MAGIC Rows only. Check `_1`, `_2`, `_3` on `printSchema()`.

# COMMAND ----------

rows_basic = [
    (1001, "Standard", 12.4),
    (1002, "Shared", 3.1),
    (1003, "Premium", 22.7),
]

df_unnamed = spark.createDataFrame(rows_basic)  # noqa: F821

# COMMAND ----------

df_unnamed.printSchema()

# COMMAND ----------

df_unnamed.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) With columns and without schema
# MAGIC
# MAGIC Names you chose; types Spark inferred. Compare with the RideEase model
# MAGIC on Notion.

# COMMAND ----------

rows_named = [
    (1001, "Standard", 138, 12.4, 18),
    (1002, "Shared", 74, 3.1, 9),
    (1003, "Premium", 231, 22.7, 35),
]

columns_named = [
    "trip_id",
    "service_type",
    "pickup_location_id",
    "trip_distance_miles",
    "ride_duration_mins",
]

df_inferred = spark.createDataFrame(rows_named, columns_named)  # noqa: F821

# COMMAND ----------

df_inferred.printSchema()

# COMMAND ----------

df_inferred.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) With columns and an explicit DDL schema
# MAGIC
# MAGIC `printSchema()` should show `int` and `decimal(8,2)`, not inferred `long`
# MAGIC / `double`.

# COMMAND ----------

from decimal import Decimal  # noqa: E402

rows_typed = [
    (1001, "Standard", 138, Decimal("12.40"), 18),
    (1002, "Shared", 74, Decimal("3.10"), 9),
    (1003, "Premium", 231, Decimal("22.70"), 35),
]

schema_ddl = (
    "trip_id bigint NOT NULL, service_type string, pickup_location_id int, "
    "trip_distance_miles decimal(8,2), ride_duration_mins int"
)

df_ddl = spark.createDataFrame(rows_typed, schema_ddl)  # noqa: F821

# COMMAND ----------

df_ddl.printSchema()

# COMMAND ----------

df_ddl.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4) With columns and an explicit `StructType` schema
# MAGIC
# MAGIC Same contract as DDL, as Python objects. Schema should match path 3.

# COMMAND ----------

from pyspark.sql.types import (  # noqa: E402
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

schema_struct = StructType(
    [
        StructField("trip_id", LongType(), nullable=False),
        StructField("service_type", StringType(), nullable=True),
        StructField("pickup_location_id", IntegerType(), nullable=True),
        StructField("trip_distance_miles", DecimalType(8, 2), nullable=True),
        StructField("ride_duration_mins", IntegerType(), nullable=True),
    ]
)

df_struct = spark.createDataFrame(rows_typed, schema_struct)  # noqa: F821

# COMMAND ----------

df_struct.printSchema()

# COMMAND ----------

df_struct.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - Four create paths: unnamed, named+inferred, DDL, `StructType`.
# MAGIC - Inferred types follow sample values; explicit schemas follow the model.
# MAGIC
# MAGIC Next up: `02 - Inspecting DataFrames`.