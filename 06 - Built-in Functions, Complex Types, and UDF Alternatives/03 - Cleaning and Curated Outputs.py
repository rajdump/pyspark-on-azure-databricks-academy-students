# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Cleaning and Curated Outputs
# MAGIC
# MAGIC Full-size controlled-bad CSVs → curated **`trip`** and **`payment`**.
# MAGIC
# MAGIC Landing **`bad_trip_data.csv`** (108 source rows) and
# MAGIC **`bad_payment_data.csv`** (106 source rows).
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Review Module 3 cleaning patterns on full-size controlled-bad CSV variants and
# MAGIC   persist cleaned DataFrames to curated outputs
# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC This notebook reads all CSV columns as string intentionally.
# MAGIC In production, do not default every column to STRING.
# MAGIC Use explicit business data types when source format and data quality are reliable.
# MAGIC Use string-first ingestion only for unreliable inputs where raw values must be validated before casting to target types.
# MAGIC
# MAGIC The source files contain:
# MAGIC
# MAGIC - `bad_trip_data.csv`: 100 original trip records and 8 controlled bad records
# MAGIC - `bad_payment_data.csv`: 100 original payment records and 6 controlled bad records
# MAGIC
# MAGIC Each file contains one record that lacks a `trip_id`; that row is rejected.
# MAGIC The trip source also includes a duplicate `trip_id` 101, which `dropDuplicates`
# MAGIC removes so curated trip keeps one row per key. Remaining records stay after
# MAGIC their values are cleaned.

# COMMAND ----------

from pyspark.sql import functions as F

landing_root = "/Volumes/rideshare_dev/landing/source_files"
curated_root = "/Volumes/rideshare_dev/processed/output_files/curated"

trip_source_path = f"{landing_root}/trip/bad_trip_data.csv"
payment_source_path = f"{landing_root}/payment/bad_payment_data.csv"

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
# MAGIC ## 1. Clean and enrich trip data
# MAGIC
# MAGIC The trip source contains controlled examples of casing and whitespace, sentinel and
# MAGIC blank labels, malformed numeric text, a negative distance, a blank distance, a
# MAGIC duplicate `trip_id` 101, and a missing key.
# MAGIC
# MAGIC The pipeline will:
# MAGIC
# MAGIC - Use `try_cast` to convert each typed field.
# MAGIC - Identify and reject records where the `trip_id` is NULL.
# MAGIC - Drop duplicate `trip_id` values before label normalization.
# MAGIC - Trim and convert `service_type` to lowercase; map blank values and "n/a" to "unknown."
# MAGIC - Convert malformed or non-positive distance values to NULL.
# MAGIC - Convert negative duration and wait values to NULL, while keeping zero as a valid entry.

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
# MAGIC ### Safely cast typed fields

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
# MAGIC Compare the table above to **Bad records from trip source**. Only selected
# MAGIC values change at this step; the rest stay the same until later rules.
# MAGIC
# MAGIC | trip_id | Column | Source value | After `try_cast` | Why |
# MAGIC |--------:|--------|--------------|------------------|-----|
# MAGIC | 101 | `service_type` | ` Premium ` | Same | Cast does not normalize labels; fixed later. |
# MAGIC | 102 | `service_type` | ` n/a ` | Same | Same. |
# MAGIC | 103 | `trip_distance_miles` | `-1.00` | `-1.00` | Cast succeeds; but business won't accept negative distance fixed later (`> 0` rule). |
# MAGIC | 104 | `service_type` | NULL (blank) | Same | Cast does not normalize labels; fixed later. |
# MAGIC | 105 | `trip_distance_miles` | `not_a_number` | NULL | Invalid decimal text; `try_cast` returns NULL. |
# MAGIC | 106 | `trip_distance_miles` | NULL (blank) | NULL | Missing field, not a failed cast; compare to trip 105. |
# MAGIC | NULL | `trip_id` | NULL (missing key) | NULL | No key to cast; row rejected in the next step. |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reject records without a usable key
# MAGIC `trip_id` is necessary for joins. Remove the row with a missing trip_id.


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
# MAGIC ### Drop duplicate `trip_id` values
# MAGIC Keep one row per `trip_id` before normalization and downstream transforms.

# COMMAND ----------

trip_deduplicated = trip_key_filtered.dropDuplicates(["trip_id"])

print(f"Trip rows after key filter: {trip_key_filtered.count()}")
print(f"Trip rows after dropDuplicates: {trip_deduplicated.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Normalize service labels
# MAGIC
# MAGIC Raw `service_type` values arrive with inconsistent casing, extra whitespace,
# MAGIC and placeholder sentinels. The next cell applies a two-step normalization so
# MAGIC downstream joins and aggregations work reliably:
# MAGIC
# MAGIC | Step | What it does | Function used |
# MAGIC |------|-------------|---------------|
# MAGIC | 1. Trim & lowercase | Removes leading/trailing spaces, converts to lowercase | `F.lower(F.trim(...))` |
# MAGIC | 2. Replace sentinels | Replaces `""`, `"n/a"`, and NULL with `"unknown"` | `F.coalesce(F.when(...), F.lit("unknown"))` |
# MAGIC
# MAGIC **Example transformations:**
# MAGIC
# MAGIC | Raw input | After step 1 | After step 2 (final) |
# MAGIC |-----------|-------------|----------------------|
# MAGIC | `"  Premium "` | `"premium"` | `"premium"` |
# MAGIC | `" N/A "` | `"n/a"` | `"unknown"` |
# MAGIC | `""` (empty) | `""` | `"unknown"` |
# MAGIC | NULL | NULL | `"unknown"` |
# MAGIC | `"shared"` | `"shared"` | `"shared"` |
# MAGIC
# MAGIC > **Why two steps?** Trim and lowercase run first so that `" N/A "` becomes
# MAGIC > `"n/a"` before the sentinel check. Reversing the order would let dirty
# MAGIC > variants slip through uncaught.

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
# MAGIC ### Enforce numeric business rules and NULL handling
# MAGIC
# MAGIC `try_cast` has already converted incorrect numeric text to NULL. Now let's handle the business rules; distance must be positive, while duration and wait values may be zero but not negative.

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
# MAGIC ### Add trip enrichments and select the curated contract
# MAGIC
# MAGIC ### Trip timing columns at a glance
# MAGIC
# MAGIC | Column | Business meaning | Formula / Source |
# MAGIC |---|---|---|
# MAGIC | `request_to_pickup_mins` | Represents the total time from the moment the rider requests the trip until the passenger boards the vehicle. | Source column |
# MAGIC | `driver_arrival_to_pickup_mins` | Indicates the time taken for the passenger to board the vehicle after the driver arrives at the pickup point. | Source column |
# MAGIC | `request_to_driver_arrival_mins` | Refers to the estimated time taken for the driver to arrive at the pickup location. | `request_to_pickup_mins - driver_arrival_to_pickup_mins` |
# MAGIC | `ride_duration_mins` | Refers to the actual travel time from the moment the passenger is picked up until they are dropped off. | Source column |
# MAGIC | `diff_ride_duration_wait_mins` | Refers to the difference between the travel time during the ride and the waiting time before pickup. If the value is negative, it means the ride took less time than the wait before pickup. | `ride_duration_mins - request_to_pickup_mins` |
# MAGIC
# MAGIC The explicit NULL branch in `ride_duration_band` prevents a missing duration
# MAGIC from being mislabeled as a long ride.

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
# MAGIC ## 2. Clean and enrich payment data
# MAGIC
# MAGIC The payment source contains controlled examples of casing and whitespace, a blank
# MAGIC label, malformed optional numeric text, negative amounts, and a missing key.
# MAGIC
# MAGIC The pipeline will:
# MAGIC
# MAGIC - Use `try_cast` to convert each typed field.
# MAGIC - Identify and reject records where the `trip_id` is NULL.
# MAGIC - Trim and convert `payment_method` to lowercase; map blank values to "unknown."
# MAGIC - Convert malformed or negative amount values to NULL, while keeping zero as a valid entry.

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
# MAGIC ### Safely cast typed fields

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
# MAGIC Compare the table above to **Bad records from payment source**. Only selected
# MAGIC values change at this step; the rest stay the same until later rules.
# MAGIC
# MAGIC | trip_id | Column | Source value | After `try_cast` | Why |
# MAGIC |--------:|--------|--------------|------------------|-----|
# MAGIC | 101 | `payment_method` | ` Card ` | Same | Cast does not normalize labels; fixed later. |
# MAGIC | 102 | `payment_method` | ` cash ` | Same | Same. |
# MAGIC | 102 | `surge_amount` | `-1.50` | `-1.50` | Cast succeeds; negative amount fixed later (`>= 0` rule). |
# MAGIC | 103 | `tip_amount` | `not_a_number` | NULL | Invalid decimal text; `try_cast` returns NULL. |
# MAGIC | 104 | `base_fare_amount` | `-5.00` | `-5.00` | Cast succeeds; negative amount fixed later (`>= 0` rule). |
# MAGIC | 105 | `payment_method` | NULL (blank) | Same | Cast does not normalize labels; fixed later. |
# MAGIC | NULL | `trip_id` | NULL (missing key) | NULL | No key to cast; row rejected in the next step. |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reject records without a usable key
# MAGIC `trip_id` is necessary for joins. Remove the row with a missing trip_id.

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
# MAGIC ### Normalize payment labels
# MAGIC
# MAGIC Raw `payment_method` values arrive with inconsistent casing, extra whitespace,
# MAGIC and blank labels. The next cell applies a two-step normalization so downstream
# MAGIC joins and aggregations work reliably:
# MAGIC
# MAGIC | Step | What it does | Function used |
# MAGIC |------|-------------|---------------|
# MAGIC | 1. Trim & lowercase | Removes leading/trailing spaces, converts to lowercase | `F.lower(F.trim(...))` |
# MAGIC | 2. Replace blanks | Replaces `""` and NULL with `"unknown"` | `F.coalesce(F.when(...), F.lit("unknown"))` |
# MAGIC
# MAGIC **Example transformations:**
# MAGIC
# MAGIC | Raw input | After step 1 | After step 2 (final) |
# MAGIC |-----------|-------------|----------------------|
# MAGIC | `"  Card "` | `"card"` | `"card"` |
# MAGIC | `""` (empty) | `""` | `"unknown"` |
# MAGIC | NULL | NULL | `"unknown"` |
# MAGIC | `"cash"` | `"cash"` | `"cash"` |
# MAGIC
# MAGIC > **Why two steps?** Trim and lowercase run first so blank and dirty values are
# MAGIC > normalized consistently before the blank check.
# MAGIC

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
# MAGIC ### Enforce numeric business rules and NULL handling
# MAGIC
# MAGIC `try_cast` has already converted incorrect numeric text to NULL. Now let's handle the business rules; payment amounts may be zero but not negative.

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
# MAGIC ### Add payment enrichments and select the curated contract
# MAGIC
# MAGIC ### Payment amount columns at a glance
# MAGIC
# MAGIC | Column | Business meaning | Formula / Source |
# MAGIC |---|---|---|
# MAGIC | `base_fare_amount` | Core fare amount before surges, tax, discounts, and tip. | Source column |
# MAGIC | `surge_amount` | Dynamic pricing increment applied for demand conditions. | Source column |
# MAGIC | `tax_amount` | Tax component added to the ride charge. | Source column |
# MAGIC | `discount_amount` | Discount amount deducted from the charge. | Source column |
# MAGIC | `charge_before_tip` | Total rider charge before tip after adding surges and tax and subtracting discounts. | `base_fare_amount + coalesce(surge_amount, 0) + coalesce(tax_amount, 0) - coalesce(discount_amount, 0)` |
# MAGIC | `tip_percent_of_base` | Tip percentage relative to base fare when base fare is positive and tip exists. | `(tip_amount / base_fare_amount) * 100` |
# MAGIC
# MAGIC `charge_before_tip` uses zero only as a calculation fallback for optional amounts;
# MAGIC it does not overwrite a source NULL. `tip_percent_of_base` remains NULL when its
# MAGIC denominator is missing or zero.

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

payment_clean.orderBy(F.col("trip_id")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write and review curated outputs
# MAGIC
# MAGIC `.mode("overwrite")` makes this course workflow idempotent: rerunning the notebook
# MAGIC replaces the previous output instead of appending duplicate records. The same
# MAGIC `trip_clean` and `payment_clean` DataFrames prepared above are written here.

# COMMAND ----------

trip_clean.write.format("parquet").mode("overwrite").save(curated_trip_path)
payment_clean.write.format("parquet").mode("overwrite").save(curated_payment_path)

print(f"Wrote curated trip data to {curated_trip_path}")
print(f"Wrote curated payment data to {curated_payment_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC Read both folders back to check the actual files that downstream notebooks will
# MAGIC consume.

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
# MAGIC ## 4. Exercise
# MAGIC
# MAGIC New payment records arrive with the same categories of problems demonstrated
# MAGIC above, but different values. Build `payment_exercise` from
# MAGIC `payment_exercise_source`:
# MAGIC
# MAGIC 1. Safely cast `trip_id` and `base_fare_amount` with `try_cast` to their
# MAGIC    canonical types.
# MAGIC 2. Reject the record whose `trip_id` is NULL after casting.
# MAGIC 3. Trim and lowercase `payment_method`; replace a blank or NULL method with
# MAGIC    `unknown`.
# MAGIC 4. Convert a negative `base_fare_amount` to NULL.
# MAGIC 5. Select `trip_id`, `payment_method`, and `base_fare_amount`, then order by
# MAGIC    `trip_id` and show the result.
# MAGIC
# MAGIC Keep this as one forward-moving chain and do not write it. Your result should
# MAGIC have four records. Trip `202` should show `payment_method = "unknown"`
# MAGIC (blank source value). Trip `203` should show `base_fare_amount = NULL`
# MAGIC (negative source value).

# COMMAND ----------

payment_exercise_source = spark.createDataFrame(  # pyright: ignore[reportUndefinedVariable]  # noqa: F821
    [
        ("201", " Card ", "45.00", "5.00", "3.50", "6.00", "0.00", "38.00"),
        ("202", "", "30.00", "0.00", "2.40", "3.00", "0.00", "25.00"),
        ("203", "Cash", "-12.00", "0.00", "0.96", "1.50", "0.00", "10.00"),
        ("204", "wallet", "27.50", "2.00", "2.20", "3.00", "0.00", "22.00"),
        (None, "Card", "18.00", "0.00", "1.44", "2.00", "0.00", "15.00"),
    ],
    payment_string_schema_ddl,
)

print("New payment records for the exercise:")
payment_exercise_source.show(truncate=False)

# COMMAND ----------

# Your code here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - Read each full-size controlled-bad CSV with a string schema so `try_cast` could
# MAGIC   expose malformed values safely under ANSI mode.
# MAGIC - Used one staged chain per dataset to cast fields, diagnose failed conversions,
# MAGIC   reject missing keys, normalize labels, and convert invalid numeric values to
# MAGIC   NULL. On trip, also dropped duplicate `trip_id` values.
# MAGIC - Added the existing Module 6 enrichments to those same cleaned DataFrames.
# MAGIC - Wrote `trip_clean` and `payment_clean` to `curated/trip/` and
# MAGIC   `curated/payment/`, then printed curated readback counts for trip and payment.
# MAGIC
# MAGIC **Next:** Module 6 **`04 - Built-ins First: When (Not) to Use UDFs`** compares
# MAGIC built-in expressions with a Python UDF contrast and when to prefer built-ins.
