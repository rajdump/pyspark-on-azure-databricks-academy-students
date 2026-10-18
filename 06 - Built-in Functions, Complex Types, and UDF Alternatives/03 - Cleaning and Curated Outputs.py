# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03 - Cleaning and Curated Outputs
# MAGIC
# MAGIC In this notebook, we clean the `trip` and `payment` CSV files that contain
# MAGIC data-quality issues, add calculated columns, and write the results as Parquet
# MAGIC to curated storage.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Read CSV files with all-string schemas and convert columns with `try_cast`
# MAGIC - Remove records without a `trip_id` and duplicate trip records
# MAGIC - Standardize labels and validate numeric values
# MAGIC - Write cleaned DataFrames as Parquet and check them after reading back

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and source data
# MAGIC
# MAGIC Attach **all-purpose compute**.
# MAGIC
# MAGIC - `bad_trip_data.csv` has 108 records.
# MAGIC - `bad_payment_data.csv` has 106 records.
# MAGIC
# MAGIC Both files contain missing keys, invalid values, and inconsistent text labels.
# MAGIC Both schemas read every column as `string`, so invalid values such as
# MAGIC `not_a_number` can be inspected before conversion.

# COMMAND ----------

from pyspark.sql import functions as F

# Reading path
landing_root = "/Volumes/rideshare_dev/landing/source_files"
trip_source_path = f"{landing_root}/trip/bad_trip_data.csv"
payment_source_path = f"{landing_root}/payment/bad_payment_data.csv"

# Writing path
curated_root = "/Volumes/rideshare_dev/processed/output_files/curated"
curated_trip_path = f"{curated_root}/trip"
curated_payment_path = f"{curated_root}/payment"

print(f"trip_source_path = {trip_source_path}")
print(f"payment_source_path = {payment_source_path}")
print(f"curated_trip_path = {curated_trip_path}")
print(f"curated_payment_path = {curated_payment_path}")

# COMMAND ----------

trip_string_schema_ddl = """
trip_id string,
service_type string,
pickup_location_id string,
dropoff_location_id string,
trip_distance_miles string,
request_to_pickup_mins string,
ride_duration_mins string,
driver_arrival_to_pickup_mins string
"""

payment_string_schema_ddl = """
trip_id string,
payment_method string,
base_fare_amount string,
surge_amount string,
tax_amount string,
tip_amount string,
discount_amount string,
driver_payout_amount string
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Clean and enrich trip data
# MAGIC
# MAGIC Read the trip CSV and show the records with data-quality issues.

# COMMAND ----------

trip_source = (
    spark.read.format(  # pyright: ignore[reportUndefinedVariable]  # noqa: F821
        "csv"
    )
    .option("header", "true")
    .schema(trip_string_schema_ddl)
    .load(trip_source_path)
)

print("Bad records from trip source:")
trip_source.filter(
    F.col("trip_id").isin("101", "102", "103", "104", "105", "106")
    | F.col("trip_id").isNull()
    | (F.trim(F.col("trip_id")) == "")
).show(truncate=False)

trip_source.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2a. Convert the column types
# MAGIC
# MAGIC Use `try_cast` to convert the ID, distance, and time columns to `bigint`, `int`,
# MAGIC or `decimal(8,2)`. Values that cannot be converted become `NULL`.
# MAGIC `service_type` stays a `string`.

# COMMAND ----------

trip_cast = (
    trip_source.withColumn("trip_id", F.col("trip_id").try_cast("bigint"))
    .withColumn(
        "pickup_location_id",
        F.col("pickup_location_id").try_cast("int"),
    )
    .withColumn(
        "dropoff_location_id",
        F.col("dropoff_location_id").try_cast("int"),
    )
    .withColumn(
        "trip_distance_miles",
        F.col("trip_distance_miles").try_cast("decimal(8,2)"),
    )
    .withColumn(
        "request_to_pickup_mins",
        F.col("request_to_pickup_mins").try_cast("int"),
    )
    .withColumn(
        "ride_duration_mins",
        F.col("ride_duration_mins").try_cast("int"),
    )
    .withColumn(
        "driver_arrival_to_pickup_mins",
        F.col("driver_arrival_to_pickup_mins").try_cast("int"),
    )
)


print("Bad records from trip source after try_cast:")
trip_cast.filter(
    F.col("trip_id").isin("101", "102", "103", "104", "105", "106")
    | F.col("trip_id").isNull()
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Compare with the source records:
# MAGIC
# MAGIC - **Trip 105:** `not_a_number` in `trip_distance_miles` becomes `NULL`.
# MAGIC - **Trip 103:** `-1.00` converts successfully; it is handled in step 2e.
# MAGIC - **Trip 106:** `trip_distance_miles` was already `NULL`, so it stays `NULL`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b. Remove records without `trip_id`
# MAGIC
# MAGIC Show the record without a `trip_id`, then keep only records that have one.
# MAGIC The trip count goes from 108 to 107.

# COMMAND ----------

trip_rejected = trip_cast.filter(F.col("trip_id").isNull())

print("Rejected trip records:")
trip_rejected.select(
    F.col("trip_id"),
    F.col("service_type"),
    F.col("pickup_location_id"),
).show(truncate=False)

# Remove the row with a missing trip_id.
trip_key_filtered = trip_cast.filter(F.col("trip_id").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2c. Remove duplicate trip records
# MAGIC
# MAGIC Trip 101 appears twice with identical data. Keep one record per `trip_id`.
# MAGIC The trip count goes from 107 to 106.

# COMMAND ----------

trip_deduplicated = trip_key_filtered.dropDuplicates(["trip_id"])

print(f"Trip rows after key filter: {trip_key_filtered.count()}")
print(f"Trip rows after dropDuplicates: {trip_deduplicated.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2d. Standardize `service_type`
# MAGIC
# MAGIC Trim and lowercase `service_type`, then replace empty strings, `"n/a"`, and
# MAGIC `NULL` with `"unknown"`. Trip 101 becomes `premium`; trips 102 and 104 become
# MAGIC `unknown`.

# COMMAND ----------

trip_labels_normalized = trip_deduplicated.withColumn(
    "service_type",
    F.lower(F.trim(F.col("service_type"))),
).withColumn(
    "service_type",
    F.coalesce(
        F.when(
            ~F.col("service_type").isin("", "n/a"),
            F.col("service_type"),
        ),
        F.lit("unknown"),
    ),
)

print("Controlled trip labels after normalization:")
trip_labels_normalized.filter(F.col("trip_id").between(101, 106)).orderBy(F.col("trip_id")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2e. Validate numeric values
# MAGIC
# MAGIC - `trip_distance_miles` must be greater than `0`.
# MAGIC - `request_to_pickup_mins`, `ride_duration_mins`, and
# MAGIC   `driver_arrival_to_pickup_mins` must be `0` or greater.
# MAGIC
# MAGIC Values that fail become `NULL`; the records are kept. Trip 103's `-1.00`
# MAGIC distance becomes `NULL`.

# COMMAND ----------

trip_values_checked = (
    trip_labels_normalized.withColumn(
        "trip_distance_miles",
        F.when(
            F.col("trip_distance_miles") > 0,
            F.col("trip_distance_miles"),
        ).otherwise(F.lit(None).cast("decimal(8,2)")),
    )
    .withColumn(
        "request_to_pickup_mins",
        F.when(
            F.col("request_to_pickup_mins") >= 0,
            F.col("request_to_pickup_mins"),
        ),
    )
    .withColumn(
        "ride_duration_mins",
        F.when(
            F.col("ride_duration_mins") >= 0,
            F.col("ride_duration_mins"),
        ),
    )
    .withColumn(
        "driver_arrival_to_pickup_mins",
        F.when(
            F.col("driver_arrival_to_pickup_mins") >= 0,
            F.col("driver_arrival_to_pickup_mins"),
        ),
    )
)

print("Controlled trip distances after cleaning:")
trip_values_checked.filter(F.col("trip_id").between(101, 106)).orderBy(F.col("trip_id")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2f. Derive new columns
# MAGIC
# MAGIC Convert `service_type` to uppercase and derive `service_label`,
# MAGIC `trip_distance_km`, `request_to_driver_arrival_mins`,
# MAGIC `diff_ride_duration_wait_mins`, and `ride_duration_band`.
# MAGIC If `ride_duration_mins` is `NULL`, `ride_duration_band` is also `NULL`.

# COMMAND ----------

trip_clean = (
    trip_values_checked.withColumn(
        "service_type",
        F.upper(F.col("service_type")),
    )
    .withColumn(
        "service_label",
        F.concat_ws(
            "-",
            F.lit("SERVICE"),
            F.upper(F.col("service_type")),
        ),
    )
    .withColumn(
        "trip_distance_km",
        F.round(F.col("trip_distance_miles") * F.lit(1.60934), 2),
    )
    .withColumn(
        "request_to_driver_arrival_mins",
        F.col("request_to_pickup_mins") - F.col("driver_arrival_to_pickup_mins"),
    )
    .withColumn(
        "diff_ride_duration_wait_mins",
        F.col("ride_duration_mins") - F.col("request_to_pickup_mins"),
    )
    .withColumn(
        "ride_duration_band",
        F.when(F.col("ride_duration_mins").isNull(), F.lit(None).cast("string"))
        .when(F.col("ride_duration_mins") < 15, "short")
        .when(F.col("ride_duration_mins") < 30, "medium")
        .otherwise("long"),
    )
    .select(
        F.col("trip_id"),
        F.col("service_type"),
        F.col("service_label"),
        F.col("pickup_location_id"),
        F.col("dropoff_location_id"),
        F.col("trip_distance_miles"),
        F.col("trip_distance_km"),
        F.col("request_to_pickup_mins"),
        F.col("driver_arrival_to_pickup_mins"),
        F.col("request_to_driver_arrival_mins"),
        F.col("ride_duration_mins"),
        F.col("diff_ride_duration_wait_mins"),
        F.col("ride_duration_band"),
    )
)


trip_clean.orderBy(F.col("trip_id")).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Clean and enrich payment data
# MAGIC
# MAGIC Read the payment CSV and show the records with data-quality issues.

# COMMAND ----------

payment_source = (
    spark.read.format(  # pyright: ignore[reportUndefinedVariable]  # noqa: F821
        "csv"
    )
    .option("header", "true")
    .schema(payment_string_schema_ddl)
    .load(payment_source_path)
)

print("Bad records from payment source:")
payment_source.filter(
    F.col("trip_id").isin("101", "102", "103", "104", "105")
    | F.col("trip_id").isNull()
    | (F.trim(F.col("trip_id")) == "")
).show(truncate=False)

payment_source.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3a. Convert the column types
# MAGIC
# MAGIC Use `try_cast` to convert `trip_id` to `bigint` and the six amount columns to
# MAGIC `decimal(10,2)`. Values that cannot be converted become `NULL`.
# MAGIC `payment_method` stays a `string`.

# COMMAND ----------

payment_cast = (
    payment_source.withColumn("trip_id", F.col("trip_id").try_cast("bigint"))
    .withColumn(
        "base_fare_amount",
        F.col("base_fare_amount").try_cast("decimal(10,2)"),
    )
    .withColumn(
        "surge_amount",
        F.col("surge_amount").try_cast("decimal(10,2)"),
    )
    .withColumn(
        "tax_amount",
        F.col("tax_amount").try_cast("decimal(10,2)"),
    )
    .withColumn(
        "tip_amount",
        F.col("tip_amount").try_cast("decimal(10,2)"),
    )
    .withColumn(
        "discount_amount",
        F.col("discount_amount").try_cast("decimal(10,2)"),
    )
    .withColumn(
        "driver_payout_amount",
        F.col("driver_payout_amount").try_cast("decimal(10,2)"),
    )
)

print("Bad records from payment source after try_cast:")
payment_cast.filter(
    F.col("trip_id").isin("101", "102", "103", "104", "105")
    | F.col("trip_id").isNull()
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Compare with the source records:
# MAGIC
# MAGIC - **Payment 103:** `not_a_number` in `tip_amount` becomes `NULL`.
# MAGIC - **Payments 102 and 104:** `surge_amount = -1.50` and `base_fare_amount = -5.00`
# MAGIC   convert successfully; they are handled in step 3d.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3b. Remove records without `trip_id`
# MAGIC
# MAGIC Show the record without a `trip_id`, then keep only records that have one.
# MAGIC The payment count goes from 106 to 105. The payment file has no duplicate
# MAGIC `trip_id` values, so there is no deduplication step.

# COMMAND ----------

payment_rejected = payment_cast.filter(F.col("trip_id").isNull())

print("Rejected payment records:")
payment_rejected.select(
    F.col("trip_id"),
    F.col("payment_method"),
    F.col("base_fare_amount"),
).show(truncate=False)

# Remove the row with a missing trip_id.
payment_key_filtered = payment_cast.filter(F.col("trip_id").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3c. Standardize `payment_method`
# MAGIC
# MAGIC Trim and lowercase `payment_method`, then replace empty strings and `NULL`
# MAGIC with `"unknown"`. Payments 101 and 104 become `card`, payment 102 becomes
# MAGIC `cash`, and payment 105 becomes `unknown`.

# COMMAND ----------

payment_labels_normalized = payment_key_filtered.withColumn(
    "payment_method",
    F.lower(F.trim(F.col("payment_method"))),
).withColumn(
    "payment_method",
    F.coalesce(
        F.when(
            F.col("payment_method") != "",
            F.col("payment_method"),
        ),
        F.lit("unknown"),
    ),
)

print("Controlled payment labels after normalization:")
payment_labels_normalized.filter(F.col("trip_id").between(101, 105)).orderBy(
    F.col("trip_id")
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3d. Validate payment amounts
# MAGIC
# MAGIC All six amount columns must be `0` or greater. Negative values become `NULL`;
# MAGIC the records are kept. Payment 102's `surge_amount` and payment 104's
# MAGIC `base_fare_amount` become `NULL`.

# COMMAND ----------

payment_values_checked = (
    payment_labels_normalized.withColumn(
        "base_fare_amount",
        F.when(
            F.col("base_fare_amount") >= 0,
            F.col("base_fare_amount"),
        ).otherwise(F.lit(None).cast("decimal(10,2)")),
    )
    .withColumn(
        "surge_amount",
        F.when(
            F.col("surge_amount") >= 0,
            F.col("surge_amount"),
        ).otherwise(F.lit(None).cast("decimal(10,2)")),
    )
    .withColumn(
        "tax_amount",
        F.when(
            F.col("tax_amount") >= 0,
            F.col("tax_amount"),
        ).otherwise(F.lit(None).cast("decimal(10,2)")),
    )
    .withColumn(
        "tip_amount",
        F.when(
            F.col("tip_amount") >= 0,
            F.col("tip_amount"),
        ).otherwise(F.lit(None).cast("decimal(10,2)")),
    )
    .withColumn(
        "discount_amount",
        F.when(
            F.col("discount_amount") >= 0,
            F.col("discount_amount"),
        ).otherwise(F.lit(None).cast("decimal(10,2)")),
    )
    .withColumn(
        "driver_payout_amount",
        F.when(
            F.col("driver_payout_amount") >= 0,
            F.col("driver_payout_amount"),
        ).otherwise(F.lit(None).cast("decimal(10,2)")),
    )
)

print("Controlled payment amounts after cleaning:")
payment_values_checked.filter(F.col("trip_id").between(101, 105)).orderBy(
    F.col("trip_id")
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3e. Derive new columns
# MAGIC
# MAGIC - `charge_before_tip` = `base_fare_amount + surge_amount + tax_amount - discount_amount`.
# MAGIC   A `NULL` surge, tax, or discount counts as `0`; a `NULL` base fare gives `NULL`.
# MAGIC - `tip_percent_of_base` = `tip_amount / base_fare_amount * 100`, calculated only
# MAGIC   when `base_fare_amount > 0` and `tip_amount` is not `NULL`; otherwise `NULL`.

# COMMAND ----------

payment_clean = (
    payment_values_checked.withColumn(
        "charge_before_tip",
        F.round(
            F.col("base_fare_amount")
            + F.coalesce(F.col("surge_amount"), F.lit(0))
            + F.coalesce(F.col("tax_amount"), F.lit(0))
            - F.coalesce(F.col("discount_amount"), F.lit(0)),
            2,
        ),
    )
    .withColumn(
        "tip_percent_of_base",
        F.when(
            (F.col("base_fare_amount") > 0) & F.col("tip_amount").isNotNull(),
            F.round(
                F.col("tip_amount") / F.col("base_fare_amount") * 100,
                1,
            ),
        ).otherwise(F.lit(None)),
    )
    .select(
        F.col("trip_id"),
        F.col("payment_method"),
        F.col("base_fare_amount"),
        F.col("surge_amount"),
        F.col("tax_amount"),
        F.col("tip_amount"),
        F.col("discount_amount"),
        F.col("driver_payout_amount"),
        F.col("charge_before_tip"),
        F.col("tip_percent_of_base"),
    )
)

payment_clean.orderBy(F.col("trip_id")).filter(F.col("trip_id").between(101, 105)).orderBy(
    F.col("trip_id")
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write and review curated outputs
# MAGIC
# MAGIC Write `trip_clean` and `payment_clean` as Parquet. `overwrite` replaces the
# MAGIC previous output when the notebook is rerun.

# COMMAND ----------

trip_clean.write.format("parquet").mode("overwrite").save(curated_trip_path)
payment_clean.write.format("parquet").mode("overwrite").save(curated_payment_path)

print(f"Wrote curated trip data to {curated_trip_path}")
print(f"Wrote curated payment data to {curated_payment_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC Read both outputs back and check the schemas and record counts:
# MAGIC
# MAGIC - Curated trip: 106 records (108 − 1 missing `trip_id` − 1 duplicate).
# MAGIC - Curated payment: 105 records (106 − 1 missing `trip_id`).

# COMMAND ----------

trip_curated = spark.read.format(  # pyright: ignore[reportUndefinedVariable]  # noqa: F821
    "parquet"
).load(curated_trip_path)
payment_curated = spark.read.format(  # pyright: ignore[reportUndefinedVariable]  # noqa: F821
    "parquet"
).load(curated_payment_path)

trip_curated_count = trip_curated.count()
payment_curated_count = payment_curated.count()

print(f"Curated trip readback: {trip_curated_count} records")
print(f"Curated payment readback: {payment_curated_count} records")

trip_curated.printSchema()
payment_curated.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - Read both CSV files as strings and converted columns with `try_cast`.
# MAGIC - Removed records without a `trip_id` and the duplicate trip record.
# MAGIC - Standardized labels and set invalid numeric values to `NULL`.
# MAGIC - Wrote curated `trip` (106 records) and `payment` (105 records) as Parquet.
# MAGIC
# MAGIC **Next:** Module 6 — `04 - Built-ins First, When (Not) to Use UDFs`