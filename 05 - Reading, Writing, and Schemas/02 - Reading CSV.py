# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Reading CSV
# MAGIC
# MAGIC Read **`trip`** dataset from `/Volumes/rideshare_dev/landing/source_files/trip/`.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Read CSV with an explicit schema vs **`inferSchema`**
# MAGIC - Apply light reshape; write a practice output

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach **all-purpose compute**.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Reading path
landing_root = "/Volumes/rideshare_dev/landing/source_files"
trip_csv_path = f"{landing_root}/trip/trip.csv"

# Writing paths
practice_root = "/Volumes/rideshare_dev/processed/output_files/practice"
practice_output_path = f"{practice_root}/trip_csv_roundtrip/"
malformed_demo_path = f"{practice_root}/malformed_csv_demo/"

print(f"trip_csv_path = {trip_csv_path}")
print(f"practice_output_path = {practice_output_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Source path
# MAGIC
# MAGIC **`trip.csv`** was copied into the landing volume in Notebook 01.

# COMMAND ----------

display(dbutils.fs.ls(f"{landing_root}/trip"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. CSV header
# MAGIC
# MAGIC ### 2a. Without header
# MAGIC
# MAGIC Read `trip.csv` without any CSV options set.
# MAGIC
# MAGIC Since `header=True` is not specified, Spark considers the first line as a data row rather than using it as column names.

# COMMAND ----------

trip_no_header = spark.read.csv(trip_csv_path)
trip_no_header.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b. With **`header=True`**
# MAGIC
# MAGIC Tell Spark to use the first row as column names. Schema inference is still off, so every column stays **`string`**.

# COMMAND ----------

trip_strings = (
    spark.read.format("csv").option("header", True).load(trip_csv_path)
)

trip_strings.printSchema()
trip_strings.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Another way to read CSV

# COMMAND ----------

trip_strings_shorthand = spark.read.option("header", True).csv(trip_csv_path)

trip_strings_shorthand.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Schema inference
# MAGIC
# MAGIC `inferSchema=True` asks Spark to inspect the CSV values and determine the column types.
# MAGIC
# MAGIC It is useful for exploration, but when the expected schema is already known, an explicit schema gives you more control.

# COMMAND ----------

trip_inferred = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(trip_csv_path)
)

trip_inferred.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Compare Spark's inferred type for **`trip_distance_miles`** with the expected type **`decimal(8,2)`** in the next section.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Explicit schema
# MAGIC
# MAGIC For a known schema, define the column names and types up front and pass the schema to **`.schema(...)`**.
# MAGIC
# MAGIC As introduced in Module 2, you can provide the schema as either a **DDL string** or a **StructType**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5a. DDL schema string

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
    .option("header", True)
    .schema(trip_schema_ddl)
    .load(trip_csv_path)
)

print("Read with DDL schema:")
trip.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5b. `StructType` schema

# COMMAND ----------

trip_schema = StructType(
    [
        StructField("trip_id", LongType()),
        StructField("service_type", StringType()),
        StructField("pickup_location_id", IntegerType()),
        StructField("dropoff_location_id", IntegerType()),
        StructField("trip_distance_miles", DecimalType(8, 2)),
        StructField("request_to_pickup_mins", IntegerType()),
        StructField("ride_duration_mins", IntegerType()),
        StructField("driver_arrival_to_pickup_mins", IntegerType()),
    ]
)

trip_via_struct = (
    spark.read.format("csv")
    .option("header", True)
    .schema(trip_schema)
    .load(trip_csv_path)
)

print("Same file read with StructType (schemas should match):")
trip_via_struct.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Malformed records
# MAGIC
# MAGIC A **malformed record** is a row that Spark cannot parse using the schema.
# MAGIC
# MAGIC For example, if **`trip_distance_miles`** is defined as **`decimal(8,2)`** but the value is **`not_a_distance`**, that row is malformed.
# MAGIC
# MAGIC The **`mode`** option controls what Spark does with malformed records:
# MAGIC
# MAGIC - **`FAILFAST`** — stop with an error
# MAGIC - **`PERMISSIVE`** — keep the row and set the invalid value to `null`
# MAGIC - **`DROPMALFORMED`** — drop the row
# MAGIC
# MAGIC `PERMISSIVE` is the default mode.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6a. Create a demo file
# MAGIC
# MAGIC Row 2 has **`not_a_distance`** in **`trip_distance_miles`** (**`decimal(8,2)`**).

# COMMAND ----------

malformed_csv_path = f"{malformed_demo_path}bad_trips.csv"
malformed_schema = "trip_id int, service_type string, trip_distance_miles decimal(8,2)"

dbutils.fs.mkdirs(malformed_demo_path)

dbutils.fs.put(
    malformed_csv_path,
    """trip_id,service_type,trip_distance_miles
1,Standard,5.54
2,Premium,not_a_distance
3,Standard,4.44
""",
    overwrite=True,
)

print(f"Wrote demo file to {malformed_csv_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6b. `FAILFAST` — stop on the first bad row
# MAGIC
# MAGIC The read fails as soon as an action reaches row 2. Use it when bad input
# MAGIC must stop the pipeline.

# COMMAND ----------

try:
    (
        spark.read.format("csv")
        .option("header", True)
        .option("mode", "FAILFAST")
        .schema(malformed_schema)
        .load(malformed_csv_path)
        .show()
    )
except Exception as exc:
    print(f"FAILFAST stopped the read: {type(exc).__name__}")
    print(str(exc)[:400])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6c. `PERMISSIVE` — keep the row, null the bad value
# MAGIC
# MAGIC Spark keeps the malformed row and sets the invalid `trip_distance_miles` value to **`null`**.
# MAGIC
# MAGIC To keep the original CSV line for inspection, add a **`_corrupt_record`** string column to the schema.

# COMMAND ----------

permissive_schema = f"{malformed_schema}, _corrupt_record string"

(
    spark.read.format("csv")
    .option("header", True)
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .schema(permissive_schema)
    .load(malformed_csv_path)
    .show(truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6d. `DROPMALFORMED` — drop the bad row

# COMMAND ----------

(
    spark.read.format("csv")
    .option("header", True)
    .option("mode", "DROPMALFORMED")
    .schema(malformed_schema)
    .load(malformed_csv_path)
    .show(truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Light reshape
# MAGIC
# MAGIC Before writing the CSV output, use **`select()`** to keep only the columns needed for this example.

# COMMAND ----------

trip_subset = trip.select(
    F.col("trip_id"),
    F.col("service_type"),
    F.col("pickup_location_id"),
    F.col("trip_distance_miles"),
)

trip_subset.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. CSV round trip
# MAGIC
# MAGIC Write the selected columns to **`practice/trip_csv_roundtrip/`** using **`mode("overwrite")`**, so each re-run replaces the existing data in that folder. Then read the CSV output back into Spark.
# MAGIC
# MAGIC CSV does not store Spark data types, so when the file is read again without a schema, `trip_distance_miles` is read as a **`string`** instead of `decimal(8,2)`.

# COMMAND ----------

trip_subset.write.format("csv").mode("overwrite").option("header", True).save(
    practice_output_path
)

print(f"Wrote CSV folder to {practice_output_path}")
display(dbutils.fs.ls(practice_output_path))

# COMMAND ----------

roundtrip_strings = (
    spark.read.format("csv").option("header", True).load(practice_output_path)
)

print("Re-read without an explicit schema (types revert to string):")
roundtrip_strings.printSchema()

# COMMAND ----------

trip_subset_schema_ddl = (
    "trip_id bigint, service_type string, pickup_location_id int, "
    "trip_distance_miles decimal(8,2)"
)

roundtrip_typed = (
    spark.read.format("csv")
    .option("header", True)
    .schema(trip_subset_schema_ddl)
    .load(practice_output_path)
)

print("Re-read with explicit schema (types restored):")
roundtrip_typed.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - **Default CSV read** — without **`header=True`**, Spark uses column names such as **`_c0`**, **`_c1`**, and treats the first line as data. With **`header=True`**, Spark uses the first line as column names. Without a schema, the columns are still read as **`string`**.
# MAGIC - **`inferSchema=True`** — Spark inspects the CSV values and determines the column types. This requires an extra pass over the data.
# MAGIC - **Explicit schema** — pass either a **DDL string** or a **`StructType`** to **`.schema(...)`** when the expected column names and types are already known.
# MAGIC - **Malformed records** — Spark treats a record as malformed when a value cannot be converted to its schema type or when the number of values does not match the schema. **`FAILFAST`** fails when an action reaches the record, **`PERMISSIVE`** keeps it with `null` values and can store the original line in **`_corrupt_record`**, and **`DROPMALFORMED`** drops it.
# MAGIC - **CSV round trip** — CSV does not store Spark data types. When the written CSV is read again without a schema, the columns are read as strings. Reapply the schema to restore the expected types.
# MAGIC
# MAGIC **Next:** **03 - Reading JSON** — read **`zone_lookup`** (JSON Lines) from
# MAGIC the landing volume.