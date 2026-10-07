# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Reading Parquet
# MAGIC
# MAGIC Read the **`trip_time`** dataset from `/Volumes/rideshare_dev/landing/source_files/trip_time/`.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Explain why Parquet does not need **`inferSchema`**
# MAGIC - Read Parquet without a schema and with an explicit schema
# MAGIC - Explain how Parquet matches schema columns to file columns
# MAGIC - Write Parquet and read it back with its types

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach **all-purpose compute**.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, LongType, StructField, StructType

# Reading path
landing_root = "/Volumes/rideshare_dev/landing/source_files"
trip_time_parquet_path = f"{landing_root}/trip_time/trip_time.parquet"

# Writing path
practice_root = "/Volumes/rideshare_dev/processed/output_files/practice"
practice_output_path = f"{practice_root}/trip_time_parquet_roundtrip/"

print(f"trip_time_parquet_path = {trip_time_parquet_path}")
print(f"practice_output_path = {practice_output_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Source path
# MAGIC
# MAGIC **`trip_time.parquet`** was copied into the landing volume in Notebook 01.

# COMMAND ----------

display(dbutils.fs.ls(f"{landing_root}/trip_time"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Parquet format
# MAGIC
# MAGIC Parquet is a **binary, column-oriented** format. Each file stores its schema
# MAGIC (column names and types) in the file **footer**.
# MAGIC
# MAGIC | Format | Types without a schema |
# MAGIC |--------|------------------------|
# MAGIC | CSV | All **`string`**, unless **`inferSchema=True`** |
# MAGIC | JSON | Spark infers them from the values |
# MAGIC | Parquet | Read from the file footer |
# MAGIC
# MAGIC Because the file is binary, **`dbutils.fs.head`** does not show readable rows.
# MAGIC Read it into a DataFrame to inspect it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Another way to read Parquet
# MAGIC
# MAGIC **`.parquet(path)`** is shorthand for **`format("parquet").load(path)`**.

# COMMAND ----------

trip_time_shorthand = spark.read.parquet(trip_time_parquet_path)

trip_time_shorthand.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Reading without a schema
# MAGIC
# MAGIC No **`inferSchema`** option is needed. Spark reads the column names and types
# MAGIC from the file footer.

# COMMAND ----------

trip_time_embedded = spark.read.format("parquet").load(trip_time_parquet_path)

trip_time_embedded.printSchema()
trip_time_embedded.show(1, vertical=True)

print(f"Row count: {trip_time_embedded.count()} (expect 100 for the course trip_time file)")

# COMMAND ----------

# MAGIC %md
# MAGIC **`trip_id`** is **`long`**, **`trip_date`** is a real **`date`**, and
# MAGIC **`hour_of_day`** is **`integer`**, exactly as stored in the file.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Explicit schema
# MAGIC
# MAGIC Define the column names and types, then pass them to **`.schema(...)`** as a
# MAGIC **DDL string** or a **`StructType`**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5a. DDL schema string

# COMMAND ----------

trip_time_schema_ddl = """
trip_id bigint,
trip_date date,
hour_of_day int
"""

trip_time = spark.read.format("parquet").schema(trip_time_schema_ddl).load(trip_time_parquet_path)

print("Read with DDL schema:")
trip_time.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5b. `StructType` schema

# COMMAND ----------

trip_time_schema = StructType(
    [
        StructField("trip_id", LongType()),
        StructField("trip_date", DateType()),
        StructField("hour_of_day", IntegerType()),
    ]
)

trip_time_via_struct = (
    spark.read.format("parquet").schema(trip_time_schema).load(trip_time_parquet_path)
)

print("Read with StructType:")
trip_time_via_struct.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Parquet columns are matched to the schema **by name**, not by position.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Light reshape
# MAGIC
# MAGIC Before writing the Parquet output, use **`select()`** and rename **`hour_of_day`** to **`pickup_hour`**.

# COMMAND ----------

trip_time_subset = trip_time.select(
    F.col("trip_id"),
    F.col("trip_date"),
    F.col("hour_of_day"),
).withColumnRenamed("hour_of_day", "pickup_hour")

trip_time_subset.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Parquet round trip
# MAGIC
# MAGIC Write the subset to **`practice/trip_time_parquet_roundtrip/`** using **`mode("overwrite")`**, so each re-run replaces the existing data in that folder. Then read the Parquet output back into Spark.
# MAGIC
# MAGIC Each output file stores its schema in its footer, so the types survive the round trip.

# COMMAND ----------

trip_time_subset.write.format("parquet").mode("overwrite").save(practice_output_path)

print(f"Wrote Parquet folder to {practice_output_path}")
display(dbutils.fs.ls(practice_output_path))

# COMMAND ----------

roundtrip_embedded = spark.read.format("parquet").load(practice_output_path)

print("Re-read without an explicit schema (types from the footer):")
roundtrip_embedded.printSchema()

# COMMAND ----------

trip_time_subset_schema_ddl = "trip_id bigint, trip_date date, pickup_hour int"

roundtrip_typed = (
    spark.read.format("parquet").schema(trip_time_subset_schema_ddl).load(practice_output_path)
)

print("Re-read with explicit schema:")
roundtrip_typed.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - **Parquet format** — binary and column-oriented; each file stores its schema in the footer.
# MAGIC - **Reading without a schema** — Spark reads column names and types from the footer. No **`inferSchema`** is needed.
# MAGIC - **Explicit schema** — pass either a **DDL string** or a **`StructType`** to **`.schema(...)`**. Parquet columns are matched **by name**.
# MAGIC - **Parquet round trip** — Spark writes a folder of **`part-`** files. Parquet keeps the Spark data types, unlike CSV and JSON.
# MAGIC
# MAGIC **Next:** **05 - Reading XML** — read **`drivers`** from the landing volume.
