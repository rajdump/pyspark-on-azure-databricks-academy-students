# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Creating DataFrames
# MAGIC
# MAGIC What a DataFrame is, and four ways to create one from Python rows.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Explain what a Spark DataFrame is
# MAGIC - Create DataFrames unnamed/inferred, named/inferred, named with DDL, and named
# MAGIC   with `StructType`
# MAGIC - Inspect each path and explain inferred vs production risk
# COMMAND ----------

# MAGIC %md
# MAGIC ## What a Spark DataFrame represents
# MAGIC
# MAGIC A Spark DataFrame is a distributed table-like dataset with:
# MAGIC
# MAGIC - **rows** (records)
# MAGIC - **named columns**
# MAGIC - a **schema** (column types and nullability metadata)
# MAGIC
# MAGIC Spark uses the schema to plan and optimize execution. In demos, Spark can
# MAGIC infer the schema from sample Python values. In production, you usually
# MAGIC define the schema intentionally so data types and nullability match the
# MAGIC business model you expect.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Without columns and without schema
# MAGIC
# MAGIC This is the most basic creation path. Spark infers types and assigns
# MAGIC default column names like `_1`, `_2`, `_3`.
# MAGIC
# MAGIC > **Good to know:** Databricks injects **`spark`** when you attach compute
# MAGIC > — it is not imported in the notebook. Local linters (`ruff`) analyze this
# MAGIC > file as plain Python and report rule **F821** (undefined name **`spark`**).
# MAGIC > The end-of-line **`noqa`** comment on the next code cell tells ruff to
# MAGIC > ignore that warning there. Without it, **`ruff check` fails locally**; the
# MAGIC > notebook still runs on Databricks. Later notebooks use the same comment
# MAGIC > without repeating this note.

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
# MAGIC Default names (`_1`, `_2`, ...) are fine for a quick experiment, but they
# MAGIC are risky in production pipelines. Joins, filters, and quality checks need
# MAGIC stable business names (`trip_id`, `service_type`). If those names are
# MAGIC positional (`_1`, `_2`), a later column reordering can silently break the
# MAGIC logic.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) With columns and without schema
# MAGIC
# MAGIC Here you provide column names, but Spark still infers data types from the
# MAGIC Python values.

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
# MAGIC This path is convenient and readable. Look at the `printSchema()` output
# MAGIC above: whole numbers often become `long`, and Python floats become
# MAGIC `double`.
# MAGIC
# MAGIC That is the production risk. The course rideshare model expects
# MAGIC `pickup_location_id` as `int` and `trip_distance_miles` as
# MAGIC `decimal(8,2)`. Inference followed the sample values, not that intended
# MAGIC data model — so types can drift before you notice.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) With columns and an explicit DDL schema
# MAGIC
# MAGIC A DDL schema string gives explicit names, types, and nullability. For this
# MAGIC course, we align the core fields to the rideshare model (`bigint`, `int`,
# MAGIC `decimal(8,2)`).

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
# MAGIC Compare this schema with the inferred one: `pickup_location_id` is `int`
# MAGIC and `trip_distance_miles` is `decimal(8,2)` — closer to the intended
# MAGIC rideshare model.
# MAGIC
# MAGIC **Gotcha — `NOT NULL`.** DDL `NOT NULL` (and `nullable=False` next) sets
# MAGIC schema metadata when you create the DataFrame from local Python rows. It
# MAGIC is not a lasting table constraint that will keep rejecting nulls forever
# MAGIC after writes to storage. Treat it as create-time schema intent, not as a
# MAGIC substitute for later data-quality checks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4) With columns and an explicit `StructType` schema
# MAGIC
# MAGIC `StructType` is the same idea as DDL schema, expressed as Python objects.

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
# MAGIC `StructType` is useful when schema fields need to be assembled or reused in
# MAGIC Python code. The outcome should look like the DDL example: explicit types
# MAGIC and nullability metadata, including `nullable=False` on `trip_id` with the
# MAGIC same create-time meaning as DDL `NOT NULL`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why inference is convenient, but risky in production
# MAGIC
# MAGIC Inferred schema is fast to start with because Spark decides types for you.
# MAGIC For production batch pipelines, that can be risky:
# MAGIC
# MAGIC - whole-number fields can become `long` when your model expects `int`
# MAGIC - decimal-like fields can become `double` when financial logic expects
# MAGIC   exact decimal types
# MAGIC - nullability may not reflect business rules
# MAGIC
# MAGIC Explicit schemas reduce ambiguity and make pipeline behavior more reliable.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC Build one small rideshare DataFrame in two ways, then compare:
# MAGIC
# MAGIC 1. Create `my_df_inferred` from 3 rows + column names (no explicit schema).
# MAGIC 2. Create `my_df_typed` from the same rows using an explicit DDL schema:
# MAGIC    - `trip_id bigint`
# MAGIC    - `service_type string`
# MAGIC    - `pickup_location_id int`
# MAGIC    - `trip_distance_miles decimal(8,2)`
# MAGIC    - `ride_duration_mins int`
# MAGIC 3. Run `printSchema()` on both DataFrames.
# MAGIC 4. Show either DataFrame rows with `show(truncate=False)`.
# MAGIC 5. In a short comment, note one schema difference you observe.

# COMMAND ----------

# Your code here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC You created DataFrames four ways and inspected each schema:
# MAGIC
# MAGIC - unnamed + inferred
# MAGIC - named + inferred
# MAGIC - named + explicit DDL schema
# MAGIC - named + explicit `StructType` schema
# MAGIC
# MAGIC The key production takeaway: inferred schemas are convenient for demos, but
# MAGIC explicit schemas are safer when type precision and nullability matter.
# MAGIC
# MAGIC Next up: `02 - Inspecting DataFrames` — deeper inspection with `columns`,
# MAGIC `dtypes`, `count`, and summary statistics.
