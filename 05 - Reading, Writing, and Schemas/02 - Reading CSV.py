# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Reading CSV
# MAGIC
# MAGIC Read **`trip`** from landing with an explicit schema.
# MAGIC
# MAGIC `/Volumes/rideshare_dev/landing/source_files/trip/`.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Read CSV with an explicit schema vs **`inferSchema`**
# MAGIC - Apply light reshape; write a practice output
# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Import PySpark helpers and set paths for the **`trip`** dataset.
# MAGIC
# MAGIC Course **`trip`** columns (from `docs/data/dataset-overview.md`):
# MAGIC **`trip_id`** (bigint), **`service_type`** (string),
# MAGIC **`pickup_location_id`** (int), **`dropoff_location_id`** (int),
# MAGIC **`trip_distance_miles`** (decimal(8,2)), **`request_to_pickup_mins`**
# MAGIC (int), **`ride_duration_mins`** (int),
# MAGIC **`driver_arrival_to_pickup_mins`** (int).

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

landing_root = "/Volumes/rideshare_dev/landing/source_files"
trip_csv_path = f"{landing_root}/trip/trip.csv"
practice_root = "/Volumes/rideshare_dev/processed/output_files/practice"
practice_output_path = f"{practice_root}/trip_csv_roundtrip/"
malformed_demo_path = f"{practice_root}/malformed_csv_demo/"

print(f"trip_csv_path = {trip_csv_path}")
print(f"practice_output_path = {practice_output_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Source path
# MAGIC
# MAGIC **`trip/trip.csv`** was copied into the landing volume in Notebook 01.
# MAGIC Format notebooks in this module read through **`/Volumes/...`** paths — Unity
# MAGIC Catalog resolves those paths to your external storage without hardcoding
# MAGIC **`abfss://`** URLs in every cell.

# COMMAND ----------

display(dbutils.fs.ls(f"{landing_root}/trip"))

# COMMAND ----------

# MAGIC %md
# MAGIC You should see **`trip.csv`** and the full-size controlled-bad
# MAGIC **`bad_trip_data.csv`** variant in that folder. The path variable
# MAGIC **`trip_csv_path`** points specifically to **`trip.csv`** for the reads below.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Default CSV read
# MAGIC
# MAGIC Spark's CSV default is **`header=False`**: every row is treated as data and
# MAGIC columns get generic names (**`_c0`**, **`_c1`**, …). Only after you see that
# MAGIC behavior does **`header=True`** make sense.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2a. Without header
# MAGIC
# MAGIC Read **`trip.csv`** with no options — the first line of the file (the real
# MAGIC column names) is ingested as an ordinary data row.

# COMMAND ----------

trip_no_header = spark.read.csv(trip_csv_path)

print("Default read without header (generic column names):")
trip_no_header.printSchema()
trip_no_header.show(1, vertical=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Column names are **`_c0`**, **`_c1`**, … and row 1 contains
# MAGIC **`trip_id`**, **`service_type`**, … as **values** — not as schema. Legacy
# MAGIC feeds without a header row look like this on purpose; our file **does** have
# MAGIC a header, so we fix that next.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b. With **`header=True`** — two equivalent syntaxes
# MAGIC
# MAGIC Tell Spark the first row is column names. Still no schema inference — every
# MAGIC column stays **`string`**. Spark exposes two equivalent ways to read CSV:

# COMMAND ----------

trip_strings_shorthand = spark.read.option("header", True).csv(trip_csv_path)

trip_strings = (
    spark.read.format("csv").option("header", True).load(trip_csv_path)
)

print("Shorthand — .option(...).csv(path):")
trip_strings_shorthand.printSchema()

print("Generic — .format('csv').option(...).load(path):")
trip_strings.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Both builds return the same schema. **`.csv(path)`** is compact shorthand;
# MAGIC **`format("csv").load(path)`** is the generic DataSource API.
# MAGIC
# MAGIC **Recommended in this module:** **`format("csv").load(...)`** and
# MAGIC **`format("csv").save(...)`** — the same **`format(...).load(...)`** pattern
# MAGIC works for JSON, Parquet, Avro, and XML in the notebooks ahead, so pipelines
# MAGIC stay consistent. Shorthand is fine for quick CSV-only exploration.
# MAGIC
# MAGIC The cells below use the **`format("csv")`** form. **`trip_strings`** is the
# MAGIC DataFrame we carry forward.

# COMMAND ----------

print("Sample row from trip_strings:")
trip_strings.show(1, vertical=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Column names now match the file header, and every column is still **`string`**.
# MAGIC That is Spark's safe default for CSV — text files do not embed type metadata.
# MAGIC If you leave types as strings, numeric columns behave like text in filters and
# MAGIC aggregations (Module 3 showed why typing matters).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Schema inference
# MAGIC
# MAGIC **`inferSchema=True`** asks Spark to scan the file and guess column types.
# MAGIC Convenient for exploration; less predictable in production. Use
# MAGIC **`.show(1, vertical=True)`** on wide tables so one sample row is easy to
# MAGIC read alongside **`printSchema()`**.

# COMMAND ----------

trip_inferred = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(trip_csv_path)
)

print("Inferred schema:")
trip_inferred.printSchema()
trip_inferred.show(1, vertical=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Inference usually produces reasonable types on clean data, but it requires an
# MAGIC **extra pass over the file** to sample values. On large or messy feeds, guesses
# MAGIC can be wrong (for example, ID columns inferred as numbers when they should
# MAGIC stay strings). Module 4 also reminded you that anything that forces Spark to
# MAGIC inspect data — like **`count()`** — is an **action**. Treat inference as a
# MAGIC trade-off: less upfront work, less control.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Explicit schema
# MAGIC
# MAGIC The production pattern: declare the contract up front and pass it to
# MAGIC **`.schema(...)`**. Module 2 introduced two equivalent forms — a **DDL
# MAGIC schema string** and a **`StructType`**. File reads accept either.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4a. DDL schema string
# MAGIC
# MAGIC A comma-separated list of **`column_name type`** pairs — the same style used
# MAGIC in **`createDataFrame(..., schema_ddl)`** back in Module 2.

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
trip.show(1, vertical=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4b. `StructType` schema
# MAGIC
# MAGIC **`StructType`** is the same contract expressed as Python objects — useful
# MAGIC when a schema is built or reused programmatically.

# COMMAND ----------

trip_schema = StructType(
    [
        StructField("trip_id", LongType(), False),
        StructField("service_type", StringType(), False),
        StructField("pickup_location_id", IntegerType(), False),
        StructField("dropoff_location_id", IntegerType(), False),
        StructField("trip_distance_miles", DecimalType(8, 2), False),
        StructField("request_to_pickup_mins", IntegerType(), False),
        StructField("ride_duration_mins", IntegerType(), False),
        StructField("driver_arrival_to_pickup_mins", IntegerType(), False),
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
# MAGIC Use **`trip`** (DDL read) for the rest of this notebook. When the source
# MAGIC layout is stable, explicit schemas — DDL or **`StructType`** — make pipelines
# MAGIC repeatable and reviewable. Types apply at read time; no separate cast step
# MAGIC is needed when the file matches the contract.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Schema validation
# MAGIC
# MAGIC After any read, confirm three things before downstream steps: Spark's schema,
# MAGIC column names, and a quick row sample. On small landing files, a row count is
# MAGIC cheap sanity check too.

# COMMAND ----------

print("Spark schema:")
trip.printSchema()

print(f"\nColumn names ({len(trip.columns)} columns):")
print(trip.columns)

print("\nSample row:")
trip.show(1, vertical=True)

row_count = trip.count()
print(f"\nRow count: {row_count} (expect 100 for the course trip file)")

# COMMAND ----------

# MAGIC %md
# MAGIC **`printSchema()`** and **`.columns`** inspect metadata on the driver — they
# MAGIC do not modify data, add steps to the logical plan, or trigger a Spark job.
# MAGIC **`count()`** is an action — it executes the read plan and scans the file.
# MAGIC In production jobs, row-count checks often catch empty files or partial loads
# MAGIC early.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Malformed records
# MAGIC
# MAGIC Real feeds arrive with bad rows — missing fields, extra commas, truncated
# MAGIC lines. Do **not** edit the landed **`trip.csv`** to simulate this. Instead,
# MAGIC write a tiny demo file under **`practice/`** and compare **`FAILFAST`**,
# MAGIC **`PERMISSIVE`**, and **`DROPMALFORMED`**.

# COMMAND ----------

malformed_csv_path = f"{malformed_demo_path}bad_trips.csv"

dbutils.fs.mkdirs(malformed_demo_path)
dbutils.fs.put(
    malformed_csv_path,
    """trip_id,service_type,trip_distance_miles
1,Standard,5.54
2,Premium
3,Standard,4.44
""",
    True,
)

print(f"Wrote demo file to {malformed_csv_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC Row 2 is missing the distance field. **`FAILFAST`** stops the read at the
# MAGIC first bad row — useful when bad data should halt the pipeline.

# COMMAND ----------

try:
    (
        spark.read.format("csv")
        .option("header", True)
        .option("mode", "FAILFAST")
        .load(malformed_csv_path)
        .show()
    )
except Exception as exc:
    print(f"FAILFAST stopped the read: {type(exc).__name__}")
    print(str(exc)[:400])

# COMMAND ----------

# MAGIC %md
# MAGIC **`PERMISSIVE`** keeps good rows and parks corrupt lines in a
# MAGIC **`_corrupt_record`** column so you can inspect or quarantine them later.
# MAGIC Include **`_corrupt_record`** in your explicit schema — otherwise Spark
# MAGIC will not surface the corrupt lines in a dedicated column.

# COMMAND ----------

permissive_schema = "trip_id int, service_type string, trip_distance_miles decimal(8,2), _corrupt_record string"

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
# MAGIC **`DROPMALFORMED`** silently drops corrupt rows and returns only well-formed
# MAGIC records — useful when bad rows should disappear without failing the job.

# COMMAND ----------

(
    spark.read.format("csv")
    .option("header", True)
    .option("mode", "DROPMALFORMED")
    .load(malformed_csv_path)
    .show(truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Light reshape
# MAGIC
# MAGIC Before writing, **`select`** a small column set for a downstream preview.
# MAGIC This module stops at light reshape — deeper transforms belong in Module 6.

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
# MAGIC Write the subset to **`practice/trip_csv_roundtrip/`**, then read it back.
# MAGIC CSV is still text on disk — Spark does **not** remember that
# MAGIC **`trip_distance_miles`** was a decimal. Reads in section 2b showed both
# MAGIC syntaxes; the write below uses **`format("csv").save(...)`** (recommended).
# MAGIC The shorthand **`.csv(...)`** equivalent is shown as a comment only.

# COMMAND ----------

# Recommended — consistent with other formats in this module
trip_subset.write.format("csv").mode("overwrite").option("header", True).save(
    practice_output_path
)

# Shorthand equivalent:
# trip_subset.write.mode("overwrite").option("header", True).csv(practice_output_path)

print(f"Wrote CSV folder to {practice_output_path}")
display(dbutils.fs.ls(practice_output_path))

# COMMAND ----------

roundtrip_strings = (
    spark.read.format("csv").option("header", True).load(practice_output_path)
)

print("Re-read without a schema (types revert to string):")
roundtrip_strings.printSchema()
roundtrip_strings.show(1, vertical=True)

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
roundtrip_typed.show(1, vertical=True)

# COMMAND ----------

# MAGIC %md
# MAGIC We used **`.mode("overwrite")`** so re-running this cell replaces the prior
# MAGIC folder. Save modes in depth — append, error, ignore — are covered in
# MAGIC **07 - Write Patterns and Table Preview**. Parquet (next formats in this
# MAGIC module) preserves types without this round-trip loss.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC Build a small practice extract without reusing **`trip_subset`**:
# MAGIC
# MAGIC 1. Read **`trip.csv`** again with the full **`trip_schema_ddl`** (or
# MAGIC    **`trip_schema`**) into a new DataFrame (do not reuse **`trip`** or
# MAGIC    **`trip_subset`**).
# MAGIC 2. **`select`** exactly these three columns: **`trip_id`**,
# MAGIC    **`dropoff_location_id`**, **`ride_duration_mins`**.
# MAGIC 3. Write the result to
# MAGIC    **`/Volumes/rideshare_dev/processed/output_files/practice/trip_exercise/`**
# MAGIC    with **`header=True`** and **`.mode("overwrite")`** (use either
# MAGIC    **`format("csv").save(...)`** or **`.csv(...)`** — same as section 8).
# MAGIC 4. Re-read the written folder with an explicit schema (DDL or
# MAGIC    **`StructType`**) for those three columns and print the schema. Confirm
# MAGIC    **`trip_id`** is **`bigint`** and the two integer columns are **`int`**,
# MAGIC    not **`string`**.

# COMMAND ----------

# Your code here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - **CSV syntax** — **`.csv(path)`** / **`.csv(...)`** shorthand and
# MAGIC   **`format("csv").load(...)`** / **`format("csv").save(...)`** are
# MAGIC   equivalent; prefer **`format("csv")`** in this module for consistency
# MAGIC   across file formats
# MAGIC - **Volume paths** — reads use
# MAGIC   **`/Volumes/rideshare_dev/landing/source_files/...`**, not raw
# MAGIC   **`abfss://`** URLs
# MAGIC - **Default CSV read** — without **`header`**, Spark uses **`_c0`**, **`_c1`**, …
# MAGIC   and treats every row as data; **`header=True`** uses the first row as
# MAGIC   column names (still all **`string`** types without a schema)
# MAGIC - **`inferSchema=True`** — Spark guesses types after an extra data pass;
# MAGIC   fine for exploration, risky for production contracts
# MAGIC - **Explicit schema (DDL or `StructType`)** — recommended production
# MAGIC   pattern; types apply at read time
# MAGIC - **Validation** — check **`printSchema()`**, column names, samples, and
# MAGIC   row counts before trusting a landing file
# MAGIC - **Malformed rows** — **`FAILFAST`** halts early; **`PERMISSIVE`** +
# MAGIC   **`_corrupt_record`** quarantines bad lines; **`DROPMALFORMED`** drops them
# MAGIC - **CSV round trip** — writing CSV loses Spark types; re-apply a schema on
# MAGIC   read (Parquet avoids this — coming up next)
# MAGIC
# MAGIC **Next:** **03 - Reading JSON** — read **`zone_lookup`** (JSON Lines) from
# MAGIC the landing volume.
