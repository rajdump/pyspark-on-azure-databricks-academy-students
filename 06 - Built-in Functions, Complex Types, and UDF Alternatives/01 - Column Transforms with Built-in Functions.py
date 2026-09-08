# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Column Transforms with Built-in Functions
# MAGIC
# MAGIC Same transforms after load from a Volume path vs a managed table — no curated
# MAGIC write.
# MAGIC
# MAGIC Landing **`trip_time`** Parquet and
# MAGIC **`rideshare_dev.processed.trip_time_preview`**; also landing **`trip`**
# MAGIC (and optional light **`payment`**).
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Apply string, numeric, date/time, and conditional `F.*` transforms
# MAGIC - Load the same logical dataset from a Volume path and a managed table, then
# MAGIC   apply identical chains after load
# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Import Spark's built-in functions as **`F`**, then define the source paths and
# MAGIC managed table used below.
# MAGIC
# MAGIC | Dataset | Source | Columns used |
# MAGIC |---|---|---|
# MAGIC | `trip` | Landing CSV | `service_type`, distance, and duration columns |
# MAGIC | `trip_time` | Landing Parquet | `trip_id`, `trip_date`, `hour_of_day` |
# MAGIC | `trip_time` | Managed table | Same three columns as landing Parquet |
# MAGIC | `payment` | Landing Avro | Payment method and decimal amount columns |

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
# MAGIC A regular Python function like `str.upper()` processes **one value at a time**
# MAGIC in your driver process. Spark built-in functions work differently:
# MAGIC
# MAGIC | | Python function | Spark built-in (`F.*`) |
# MAGIC |---|---|---|
# MAGIC | **What it produces** | A computed value | A **Column expression** (a plan node) |
# MAGIC | **When it runs** | Immediately | Only when an action triggers execution |
# MAGIC | **Where it runs** | Driver (single machine) | Executors (distributed, JVM-optimized) |
# MAGIC | **Optimizer visibility** | Opaque | Full — Spark can reorder, prune, or fuse |
# MAGIC
# MAGIC When you write `F.upper(F.col("service_type"))`, nothing executes yet. Spark
# MAGIC records the instruction in the DataFrame's **logical plan** and evaluates it
# MAGIC across all partitions when an action (`.show()`, `.collect()`, or a terminal
# MAGIC write method such as `.write.parquet()`) runs.
# MAGIC
# MAGIC > **Module production rule:** use built-ins first. They keep the optimizer
# MAGIC > informed, avoid Python-per-row overhead, and compose cleanly.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load `trip_time` from two source types
# MAGIC
# MAGIC A Volume path identifies files. A three-part table name identifies a Unity
# MAGIC Catalog table. The load syntax changes, but both operations return a DataFrame.
# MAGIC
# MAGIC First, read the landing Parquet file with the documented schema.

# COMMAND ----------

trip_time_schema_ddl = """
trip_id bigint,
trip_date date,
hour_of_day int
"""

trip_time_from_volume = (
    spark.read.format("parquet")  # noqa: F821
    .schema(trip_time_schema_ddl)
    .load(trip_time_parquet_path)
)

print("Volume DataFrame:")
trip_time_from_volume.printSchema()
trip_time_from_volume.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC Now load the managed table with **`spark.table`**. The table created in Module 5
# MAGIC contains the same logical `trip_time` dataset.
# MAGIC
# MAGIC Reference: Module 5 notebook
# MAGIC **`07 - Write Patterns and Table Preview`**, section
# MAGIC **`## 5 cell runs `saveAsTable(managed_table)`**).

# COMMAND ----------

trip_time_from_table = spark.table(trip_time_table)  # noqa: F821

print("Managed-table DataFrame:")
trip_time_from_table.printSchema()
trip_time_from_table.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Apply the same transformations after either load
# MAGIC
# MAGIC The expressions below add:
# MAGIC
# MAGIC - **`trip_year`** and **`trip_month`** from `trip_date`
# MAGIC - **`trip_day_name`** as a readable weekday name
# MAGIC - **`day_part`** from `hour_of_day`
# MAGIC
# MAGIC **`F.when(...).when(...).otherwise(...)`** is Spark's Column-expression form
# MAGIC for ordered conditional rules. Spark checks each condition from top to bottom
# MAGIC and uses the first match.

# COMMAND ----------

# Apply transforms directly — see each expression in action
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
# MAGIC **Production pattern: extract and reuse.** The inline approach above is
# MAGIC clear, but what if you load the same dataset from multiple sources (a file
# MAGIC today, a table tomorrow)? Copy-pasting the same expressions violates DRY
# MAGIC (Don't Repeat Yourself).
# MAGIC
# MAGIC **Solution:** store the expressions in a Python list, then unpack with `*` into
# MAGIC `.select()`. The list is just a plain `list[Column]` — Spark doesn't know about
# MAGIC it; it's purely a Python convenience.

# COMMAND ----------

# Extract the same expressions into a reusable list
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

# Apply the SAME list to both sources — one list, two loads, zero duplication
trip_time_volume_transformed = trip_time_from_volume.select(*trip_time_transformations)
trip_time_table_transformed = trip_time_from_table.select(*trip_time_transformations)

print("Transforms applied to the Volume source:")
trip_time_volume_transformed.show(5, truncate=False)

print("Same transforms applied to the managed-table source:")
trip_time_table_transformed.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Both results have identical columns — the same list drove both. In production,
# MAGIC only the load line changes per source; the transformation logic stays in one place.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Load `trip` with its explicit schema
# MAGIC
# MAGIC The `trip` CSV does not store type metadata. Reuse the explicit schema pattern
# MAGIC from Module 5 so numeric columns arrive as numeric types.
# MAGIC
# MAGIC The dataset has **no date columns**. Its examples therefore focus on strings,
# MAGIC numeric/decimal values, and conditional rules.

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
    spark.read.format("csv")  # noqa: F821
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
# MAGIC Consistent text labels prevent values that mean the same thing from being
# MAGIC treated as different categories.
# MAGIC
# MAGIC - **`F.trim`** removes leading and trailing spaces.
# MAGIC - **`F.upper`** changes text to uppercase.
# MAGIC - **`F.concat_ws`** combines values with a separator.
# MAGIC
# MAGIC The landing values may already be clean. The same expressions still document
# MAGIC the expected output format.

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
# MAGIC `service_type_standardized` makes comparisons reliable by removing extra spaces
# MAGIC and normalizing case (for example, `Premium`, ` premium `, and `PREMIUM` become
# MAGIC the same value).
# MAGIC `service_label` creates a readable tagged value such as `SERVICE-PREMIUM`,
# MAGIC which is useful for display, quick filtering, and grouped summaries.
# MAGIC It also shows function composition: output from **`F.trim`** feeds
# MAGIC **`F.upper`**, then **`F.concat_ws`** builds the final label.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Numeric and decimal transformations
# MAGIC
# MAGIC Built-in numeric functions let us create useful metrics.
# MAGIC
# MAGIC We will use these three time columns from `trip`:
# MAGIC
# MAGIC | Column | Plain meaning |
# MAGIC |---|---|
# MAGIC | `request_to_pickup_mins` | Wait from request until pickup, including boarding |
# MAGIC | `driver_arrival_to_pickup_mins` | Driver waits at pickup spot until passenger boards |
# MAGIC | `ride_duration_mins` | Time in the car from pickup to destination |
# MAGIC
# MAGIC These represent different parts of one trip timeline, so each subtraction
# MAGIC answers a different question:
# MAGIC
# MAGIC | Derived column | Meaning |
# MAGIC |---|---|
# MAGIC | `request_to_driver_arrival_mins` | Time to reach pickup, excluding boarding |
# MAGIC | `ride_minus_wait_to_pickup_mins` | Negative when pickup wait exceeds ride duration |
# MAGIC | `ride_wait_to_pickup_gap_mins` | Absolute gap size regardless of sign (always >= 0) |
# MAGIC
# MAGIC In this cell we also convert miles to kilometers with multiplication and
# MAGIC round to 2 decimals using `F.round`.

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
# MAGIC Conditional columns turn numeric measurements into business-friendly labels.
# MAGIC The following rule groups rides by duration:
# MAGIC
# MAGIC - Less than 15 minutes → `short`
# MAGIC - 15 to 29 minutes → `medium`
# MAGIC - 30 minutes or more → `long`
# MAGIC
# MAGIC The boundaries do not overlap because **`F.when`** checks them in order.

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
# MAGIC **Common mistake:** omitting **`.otherwise(...)`** leaves unmatched rows as
# MAGIC `NULL`. Use that intentionally only when `NULL` is the required result.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Light decimal examples with `payment`
# MAGIC
# MAGIC The landing `payment` dataset is Avro, so it carries its schema. An explicit
# MAGIC schema keeps the expected decimal types visible in the read contract.
# MAGIC
# MAGIC This example calculates:
# MAGIC
# MAGIC - **`charge_before_tip`** from base fare, surge, tax, and discount
# MAGIC - **`tip_percent_of_base`** only when the base fare is greater than zero

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
    spark.read.format("avro")  # noqa: F821
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
# MAGIC The condition prevents division by zero. If the base fare is zero or `NULL`,
# MAGIC `tip_percent_of_base` is `NULL`, which signals that the percentage could not be
# MAGIC calculated.
# MAGIC
# MAGIC These results remain DataFrames in this notebook. There is no curated write:
# MAGIC Module 6 **`03 - Cleaning and Curated Outputs`** owns persisted enrichment and
# MAGIC cleaning columns.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC Build **`trip_exercise`** from `trip` with these columns:
# MAGIC
# MAGIC 1. Keep `trip_id` and `service_type`.
# MAGIC 2. Create **`service_type_clean`** with `F.trim` and `F.upper`.
# MAGIC 3. Create **`duration_gap_mins`** as the absolute difference between
# MAGIC    `ride_duration_mins` and `request_to_pickup_mins`.
# MAGIC 4. Create **`distance_band`** with `F.when`:
# MAGIC    - Less than 3 miles → `short_distance`
# MAGIC    - Less than 8 miles → `medium_distance`
# MAGIC    - Otherwise → `long_distance`
# MAGIC 5. Display 10 rows. Do not write the result.
# MAGIC
# MAGIC Each required pattern was demonstrated above, but the columns and boundary
# MAGIC values are different.

# COMMAND ----------

trip_exercise = trip.select(
    F.col("trip_id"),
    F.col("service_type"),
    # Add service_type_clean, duration_gap_mins, and distance_band here.
)

trip_exercise.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - **Built-in functions** create Spark Column expressions that Spark can
# MAGIC   understand and optimize.
# MAGIC - **Source loading is separate from transformation logic.** A Volume path and
# MAGIC   a managed table use different load syntax but both return DataFrames.
# MAGIC - The same expression list added date, calendar, and day-part columns after
# MAGIC   either `trip_time` load.
# MAGIC - **`trip`** demonstrated string, numeric/decimal, and conditional transforms;
# MAGIC   its schema has no date columns.
# MAGIC - **`payment`** provided a light decimal calculation with a division guard.
# MAGIC - This notebook created no persisted output. Module 6
# MAGIC   **`03 - Cleaning and Curated Outputs`** will re-read landing data before
# MAGIC   writing curated datasets.
# MAGIC
# MAGIC **Next:** Module 6 **`02 - Complex Types: Structs, Arrays, and explode`** —
# MAGIC access nested struct fields, work with arrays, and flatten assigned trips.
