# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 05 - Reading XML
# MAGIC
# MAGIC Read the **`drivers`** dataset from `/Volumes/rideshare_dev/landing/source_files/drivers/`.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Explain why the XML reader needs **`rowTag`**
# MAGIC - Read XML and inspect the structure Spark infers
# MAGIC - Explain how nested XML elements become nested columns
# MAGIC - Select a field inside a nested column

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach **all-purpose compute**.

# COMMAND ----------

from pyspark.sql import functions as F

# Reading path
landing_root = "/Volumes/rideshare_dev/landing/source_files"
drivers_xml_path = f"{landing_root}/drivers/drivers.xml"

# Writing path
practice_root = "/Volumes/rideshare_dev/processed/output_files/practice"
practice_output_path = f"{practice_root}/drivers_json_roundtrip/"

print(f"drivers_xml_path = {drivers_xml_path}")
print(f"practice_output_path = {practice_output_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Source path
# MAGIC
# MAGIC **`drivers.xml`** was copied into the landing volume in Notebook 01.

# COMMAND ----------

display(dbutils.fs.ls(f"{landing_root}/drivers"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. XML layout and `rowTag`
# MAGIC
# MAGIC The outer **`<drivers>`** element wraps the whole file. Each **`<driver>`**
# MAGIC element inside it is one record. **`rowTag`** tells Spark which element
# MAGIC becomes one DataFrame row.

# COMMAND ----------

print(dbutils.fs.head(drivers_xml_path, 700))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Reading XML
# MAGIC
# MAGIC Without **`rowTag`**, the read fails. With **`.option("rowTag", "driver")`**,
# MAGIC every **`<driver>`** element becomes one row.

# COMMAND ----------

try:
    spark.read.format("xml").load(drivers_xml_path).show(1)
except Exception as exc:
    print(f"{type(exc).__name__}: {str(exc)[:400]}")

# COMMAND ----------

drivers = spark.read.format("xml").option("rowTag", "driver").load(drivers_xml_path)

drivers.show(1, vertical=True, truncate=False)

print(f"Row count: {drivers.count()} (expect 12 for the course drivers file)")

# COMMAND ----------

# MAGIC %md
# MAGIC **`.xml(path, rowTag=...)`** is shorthand for
# MAGIC **`format("xml").option("rowTag", ...).load(path)`**. This module uses
# MAGIC **`format("xml").load(...)`** so the read pattern matches the other file formats.

# COMMAND ----------

drivers_shorthand = spark.read.xml(drivers_xml_path, rowTag="driver")

drivers_shorthand.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Schema inference
# MAGIC
# MAGIC No schema was passed, so Spark inspected the XML records and inferred the
# MAGIC column names and types, including the nested structures.

# COMMAND ----------

drivers.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Nested columns
# MAGIC
# MAGIC **`driver_id`**, **`name`**, and **`license_number`** hold a single value, so
# MAGIC they are ordinary columns. **`vehicle`** and **`trips_assigned`** contain their
# MAGIC own child elements, so they stay nested.

# COMMAND ----------

drivers.select(
    F.col("driver_id"),
    F.col("vehicle"),
    F.col("trips_assigned"),
).show(2, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Selecting nested fields
# MAGIC
# MAGIC Use a dot between the column name and the field name. **`vehicle.make`** reads
# MAGIC the **`make`** field from the **`vehicle`** struct, and **`alias(...)`** gives the
# MAGIC new column a flat name.

# COMMAND ----------

drivers_subset = drivers.select(
    F.col("driver_id"),
    F.col("name"),
    F.col("vehicle.make").alias("vehicle_make"),
    F.col("vehicle.model").alias("vehicle_model"),
    F.col("vehicle.body_type").alias("vehicle_body_type"),
)

drivers_subset.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Practice write
# MAGIC
# MAGIC Write the flat subset as JSON to **`practice/drivers_json_roundtrip/`** using **`mode("overwrite")`**, so each re-run replaces the existing data in that folder. Then read the JSON output back. The original **`drivers.xml`** file is unchanged.

# COMMAND ----------

drivers_subset.write.format("json").mode("overwrite").save(practice_output_path)

print(f"Wrote JSON folder to {practice_output_path}")
display(dbutils.fs.ls(practice_output_path))

# COMMAND ----------

roundtrip = spark.read.format("json").load(practice_output_path)

roundtrip.printSchema()
roundtrip.show(3, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - **XML read** — use **`format("xml").option("rowTag", ...).load(path)`** or **`.xml(path, rowTag=...)`**. **`rowTag`** sets which element becomes one row; without it, the read fails.
# MAGIC - **Schema inference** — without a schema, Spark infers the column names and types, including nested structures.
# MAGIC - **Nested columns** — **`vehicle`** is a struct, and **`trips_assigned`** is a struct containing an array of trip IDs.
# MAGIC - **Nested field selection** — use dot notation such as **`vehicle.make`** to select a field inside a struct.
# MAGIC
# MAGIC **Next:** **06 - Reading Avro** — read **`payment`** from the landing volume.