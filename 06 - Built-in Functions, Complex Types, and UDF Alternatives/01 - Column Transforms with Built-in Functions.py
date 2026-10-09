# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Column Transforms with Built-in Functions
# MAGIC
# MAGIC In the earlier modules, we learned how to create DataFrames and read and write
# MAGIC data in different file formats. In this notebook, we apply Spark's built-in
# MAGIC functions to transform DataFrame columns.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Apply string, numeric, date, and conditional built-in functions
# MAGIC - Load the same dataset from a Volume path and a managed table, then apply the
# MAGIC   same transformations to both

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Import Spark's built-in functions and define the paths and table name used in
# MAGIC this notebook.

# COMMAND ----------

from pyspark.sql import functions as F

landing_root = "/Volumes/rideshare_dev/landing/source_files"
trip_csv_path = f"{landing_root}/trip/trip.csv"
trip_time_parquet_path = f"{landing_root}/trip_time/trip_time.parquet"
payment_avro_path = f"{landing_root}/payment/payment.avro"
trip_time_table = "rideshare_dev.processed.trip_time_preview"

print(f"trip_csv_path = {trip_csv_path}")
print(f"trip_time_parquet_path = {trip_time_parquet_path}")
print(f"payment_avro_path = {payment_avro_path}")
print(f"trip_time_table = {trip_time_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Built-in functions create Column expressions
# MAGIC
# MAGIC Spark's built-in functions create Column expressions that describe
# MAGIC transformations on DataFrame columns. These expressions become part of the
# MAGIC logical plan and are evaluated during execution.
# MAGIC
# MAGIC In the following examples, we use built-in functions to transform dates,
# MAGIC strings, and numeric values.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load `trip_time` from two sources
# MAGIC
# MAGIC Load the `trip_time` dataset from the landing Volume using an explicit schema.

# COMMAND ----------

trip_time_schema_ddl = """
trip_id bigint,
trip_date date,
hour_of_day int
"""

trip_time_from_volume = (
    spark.read.format("parquet")
    .schema(trip_time_schema_ddl)
    .load(trip_time_parquet_path)
)

print("Volume DataFrame:")
trip_time_from_volume.printSchema()
trip_time_from_volume.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC Now load the same dataset from the managed table created in Module 5,
# MAGIC **07 - Write Patterns and Table Preview**.

# COMMAND ----------

trip_time_from_table = spark.table(trip_time_table)

print("Managed-table DataFrame:")
trip_time_from_table.printSchema()
trip_time_from_table.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Apply the same transformations after either load
# MAGIC
# MAGIC Apply built-in functions to create `trip_year`, `trip_month`, `trip_day_name`,
# MAGIC and `day_part` from the existing `trip_time` columns.

# COMMAND ----------

trip_time_volume_inline = trip_time_from_volume.select(
    F.col("trip_id"),
    F.col("trip_date"),
    F.col("hour_of_day"),
    F.year(F.col("trip_date")).alias("trip_year"),
    F.month(F.col("trip_date")).alias("trip_month"),
    F.date_format(F.col("trip_date"), "EEEE").alias("trip_day_name"),
    (
        F.when(F.col("hour_of_day") < 6, "overnight")
        .when(F.col("hour_of_day") < 12, "morning")
        .when(F.col("hour_of_day") < 18, "afternoon")
        .otherwise("evening")
        .alias("day_part")
    ),
)

print("Volume DataFrame with transforms applied inline:")
trip_time_volume_inline.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC The previous example applies the expressions directly inside `.select()`. To
# MAGIC avoid repeating them for both DataFrames, store the expressions in a Python
# MAGIC list and reuse them.
# MAGIC
# MAGIC The `*` operator unpacks the list and passes each Column expression to
# MAGIC `.select()`.

# COMMAND ----------

trip_time_transformations = [
    F.col("trip_id"),
    F.col("trip_date"),
    F.col("hour_of_day"),
    F.year(F.col("trip_date")).alias("trip_year"),
    F.month(F.col("trip_date")).alias("trip_month"),
    F.date_format(F.col("trip_date"), "EEEE").alias("trip_day_name"),
    (
        F.when(F.col("hour_of_day") < 6, "overnight")
        .when(F.col("hour_of_day") < 12, "morning")
        .when(F.col("hour_of_day") < 18, "afternoon")
        .otherwise("evening")
        .alias("day_part")
    ),
]

trip_time_volume_transformed = trip_time_from_volume.select(*trip_time_transformations)
trip_time_table_transformed = trip_time_from_table.select(*trip_time_transformations)

print("Transforms applied to the Volume source:")
trip_time_volume_transformed.show(5, truncate=False)

print("Same transforms applied to the managed-table source:")
trip_time_table_transformed.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Load `trip` with its explicit schema
# MAGIC
# MAGIC Load the `trip` CSV using an explicit schema. We will use this DataFrame for
# MAGIC string, numeric, and conditional transformations.

# COMMAND ----------

trip_schema_ddl = """
trip_id bigint,
service_type string,
pickup_location_id int,
dropoff_location_id int,
trip_distance_miles decimal(8,2),
request_to_pickup_mins int,
ride_duration_mins int,
driver_arrival_to_pickup_mins int
"""

trip = (
    spark.read.format("csv")
    .option("header", "true")
    .schema(trip_schema_ddl)
    .load(trip_csv_path)
)

trip.printSchema()
trip.show(3, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. String transformations
# MAGIC
# MAGIC Apply `F.trim()`, `F.upper()`, and `F.concat_ws()` to standardize
# MAGIC `service_type` and create a new `service_label` column.

# COMMAND ----------

trip_strings = trip.select(
    F.col("trip_id"),
    F.col("service_type"),
    F.upper(F.trim(F.col("service_type"))).alias("service_type_standardized"),
    F.concat_ws(
        "-",
        F.lit("SERVICE"),
        F.upper(F.trim(F.col("service_type"))),
    ).alias("service_label"),
)

trip_strings.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Numeric transformations
# MAGIC
# MAGIC Use arithmetic operations, `F.round()`, and `F.abs()` to derive new columns
# MAGIC from the trip's distance and time measurements.

# COMMAND ----------

trip_metrics = trip.select(
    F.col("trip_id"),
    F.col("trip_distance_miles"),
    F.round(
        F.col("trip_distance_miles") * F.lit(1.60934),
        2,
    ).alias("trip_distance_km"),
    F.col("request_to_pickup_mins"),
    F.col("driver_arrival_to_pickup_mins"),
    (F.col("request_to_pickup_mins") - F.col("driver_arrival_to_pickup_mins")).alias(
        "request_to_driver_arrival_mins"
    ),
    F.col("ride_duration_mins"),
    (F.col("ride_duration_mins") - F.col("request_to_pickup_mins")).alias(
        "ride_minus_wait_to_pickup_mins"
    ),
    F.abs(F.col("ride_duration_mins") - F.col("request_to_pickup_mins")).alias(
        "ride_wait_to_pickup_gap_mins"
    ),
)

trip_metrics.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Conditional transformations
# MAGIC
# MAGIC Use `F.when()` and `.otherwise()` to create `ride_duration_band`, categorizing
# MAGIC rides as `short`, `medium`, or `long` based on duration.

# COMMAND ----------

trip_duration_bands = trip.select(
    F.col("trip_id"),
    F.col("ride_duration_mins"),
    (
        F.when(F.col("ride_duration_mins") < 15, "short")
        .when(F.col("ride_duration_mins") < 30, "medium")
        .otherwise("long")
        .alias("ride_duration_band")
    ),
)

trip_duration_bands.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Decimal calculations with `payment`
# MAGIC
# MAGIC Load the `payment` Avro dataset using the expected schema. Calculate
# MAGIC `charge_before_tip` and `tip_percent_of_base` using the decimal amount columns.

# COMMAND ----------

payment_schema_ddl = """
trip_id bigint,
payment_method string,
base_fare_amount decimal(10,2),
surge_amount decimal(10,2),
tax_amount decimal(10,2),
tip_amount decimal(10,2),
discount_amount decimal(10,2),
driver_payout_amount decimal(10,2)
"""

payment = (
    spark.read.format("avro")
    .schema(payment_schema_ddl)
    .load(payment_avro_path)
)

payment.printSchema()

# COMMAND ----------

payment_amounts = payment.select(
    F.col("trip_id"),
    F.col("base_fare_amount"),
    F.col("surge_amount"),
    F.col("tax_amount"),
    F.col("discount_amount"),
    F.col("tip_amount"),
    F.round(
        F.col("base_fare_amount")
        + F.col("surge_amount")
        + F.col("tax_amount")
        - F.col("discount_amount"),
        2,
    ).alias("charge_before_tip"),
    (
        F.when(
            F.col("base_fare_amount") > 0,
            F.round(
                F.col("tip_amount") / F.col("base_fare_amount") * 100,
                1,
            ),
        )
        .otherwise(F.lit(None))
        .alias("tip_percent_of_base")
    ),
)

payment_amounts.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - Applied Spark built-in functions to date, string, numeric, and decimal columns.
# MAGIC - Reused the same Column expressions across DataFrames loaded from different sources.
# MAGIC - Created derived columns using arithmetic and conditional expressions.
# MAGIC
# MAGIC **Next:** `02 - Complex Types, Structs, Arrays, and explode`
