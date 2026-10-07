# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 06 - Reading Avro
# MAGIC
# MAGIC Read the **`payment`** dataset from `/Volumes/rideshare_dev/landing/source_files/payment/`.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Read Avro and inspect the schema stored in the file
# MAGIC - Explain why Avro does not need **`inferSchema`**
# MAGIC - Read Avro with an explicit schema
# MAGIC - Explain how Avro matches schema fields to file fields
# MAGIC - Write Avro and read the output back

# COMMAND ----------

# MAGIC %md
# MAGIC ## Avro in this lesson
# MAGIC
# MAGIC In the earlier **Avro vs Parquet** lesson, we saw that Avro is a
# MAGIC row-oriented binary format and that each Avro file stores its writer schema
# MAGIC in the file header.
# MAGIC
# MAGIC In this lesson, we use that knowledge to read **`payment.avro`** with Spark.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach **all-purpose compute**.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Reading path
landing_root = "/Volumes/rideshare_dev/landing/source_files"
payment_avro_path = f"{landing_root}/payment/payment.avro"

# Writing path
practice_root = "/Volumes/rideshare_dev/processed/output_files/practice"
practice_output_path = f"{practice_root}/payment_avro_roundtrip/"

print(f"payment_avro_path = {payment_avro_path}")
print(f"practice_output_path = {practice_output_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Source path
# MAGIC
# MAGIC **`payment.avro`** was copied into the landing volume in Notebook 01.

# COMMAND ----------

display(dbutils.fs.ls(f"{landing_root}/payment"))

# COMMAND ----------

# MAGIC %md
# MAGIC You should see **`payment.avro`** in that folder. The folder also contains
# MAGIC **`bad_payment_data.csv`**, which is used later in Module 6.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Reading Avro
# MAGIC Spark gets the column names and data types from the writer schema in the
# MAGIC file header, so it does not need **`inferSchema`**.

# COMMAND ----------

payment_embedded = spark.read.format("avro").load(payment_avro_path)

print("Schema from Avro metadata (no inferSchema):")
payment_embedded.printSchema()

print("\nSample row:")
payment_embedded.show(1, vertical=True)

row_count = payment_embedded.count()
print(f"\nRow count: {row_count} (expect 100)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Explicit schema
# MAGIC
# MAGIC Avro already stores column names and data types, but you can still provide an
# MAGIC explicit schema with **`.schema(...)`**. The schema can be a DDL string or a
# MAGIC **`StructType`**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3a. DDL schema string

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
    spark.read.format("avro").schema(payment_schema_ddl).load(payment_avro_path)
)

print("Read with DDL schema:")
payment.printSchema()
payment.show(1, vertical=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3b. `StructType` schema

# COMMAND ----------

payment_schema = StructType(
    [
        StructField("trip_id", LongType()),
        StructField("payment_method", StringType()),
        StructField("base_fare_amount", DecimalType(10, 2)),
        StructField("surge_amount", DecimalType(10, 2)),
        StructField("tax_amount", DecimalType(10, 2)),
        StructField("tip_amount", DecimalType(10, 2)),
        StructField("discount_amount", DecimalType(10, 2)),
        StructField("driver_payout_amount", DecimalType(10, 2)),
    ]
)

payment_via_struct = (
    spark.read.format("avro").schema(payment_schema).load(payment_avro_path)
)

print("Same file read with StructType (schemas should match):")
payment_via_struct.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Avro fields are matched by name, not by position.
# MAGIC
# MAGIC Use **`payment`** (the DDL read) for the rest of this notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Light reshape
# MAGIC
# MAGIC Keep a smaller payment subset for the practice write.

# COMMAND ----------

payment_subset = payment.select(
    F.col("trip_id"),
    F.col("payment_method"),
    F.col("base_fare_amount"),
    F.col("tip_amount"),
    F.col("driver_payout_amount"),
)

payment_subset.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Avro round trip
# MAGIC
# MAGIC Write the subset to **`practice/payment_avro_roundtrip/`**, then read it
# MAGIC back. Spark writes one or more Avro data files under that folder. Each file
# MAGIC stores its writer schema in the header, so Spark can read the data types back
# MAGIC without **`inferSchema`**.

# COMMAND ----------

payment_subset.write.format("avro").mode("overwrite").save(practice_output_path)

print(f"Wrote Avro folder to {practice_output_path}")
display(dbutils.fs.ls(practice_output_path))

# COMMAND ----------

roundtrip_embedded = spark.read.format("avro").load(practice_output_path)

print("Re-read without explicit schema (types come from Avro metadata):")
roundtrip_embedded.printSchema()
roundtrip_embedded.show(1, vertical=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - **Avro schema** — each Avro file stores its writer schema in the file header.
# MAGIC - **Avro read** — use **`format("avro").load(...)`**; there is no **`spark.read.avro(...)`** shorthand.
# MAGIC - **No `inferSchema`** — Spark reads the column names and data types from the writer schema.
# MAGIC - **Explicit schema** — **`.schema(...)`** accepts a DDL string or a **`StructType`**.
# MAGIC - **Field matching** — Avro fields are matched by name, not by position.
# MAGIC - **Avro round trip** — Spark writes a folder of Avro part files. For the data types used in this lesson, the types are available again on read.
# MAGIC
# MAGIC **Next:** **07 - Write Patterns and Table Preview**