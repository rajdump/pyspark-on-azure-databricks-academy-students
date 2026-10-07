# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Reading JSON
# MAGIC
# MAGIC Read the **`zone_lookup`** dataset from `/Volumes/rideshare_dev/landing/source_files/zone_lookup/`.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Explain what **JSON Lines** means
# MAGIC - Compare an inferred JSON schema with an explicit schema
# MAGIC - Handle missing and extra fields with an explicit schema
# MAGIC - Read multiline JSON with **`multiLine=True`**
# MAGIC - Write JSON and read it back with the schema

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach **all-purpose compute**.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# Reading path
landing_root = "/Volumes/rideshare_dev/landing/source_files"
zone_json_path = f"{landing_root}/zone_lookup/zone_lookup.json"

# Writing paths
practice_root = "/Volumes/rideshare_dev/processed/output_files/practice"
practice_output_path = f"{practice_root}/zone_lookup_json_roundtrip/"
schema_demo_dir = f"{practice_root}/zone_lookup_schema_demo/"
multiline_demo_dir = f"{practice_root}/zone_lookup_multiline_demo/"

print(f"zone_json_path = {zone_json_path}")
print(f"practice_output_path = {practice_output_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Source path
# MAGIC
# MAGIC **`zone_lookup.json`** was copied into the landing volume in Notebook 01.

# COMMAND ----------

display(dbutils.fs.ls(f"{landing_root}/zone_lookup"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. JSON Lines
# MAGIC
# MAGIC **JSON Lines** (newline-delimited JSON) means **one complete JSON object per
# MAGIC line**. Spark's JSON reader expects this layout by default, so the landing
# MAGIC file needs no extra option.

# COMMAND ----------

print(dbutils.fs.head(zone_json_path, 500))

# COMMAND ----------

# MAGIC %md
# MAGIC Each line is a complete `{...}` object. Section 7 shows the opposite: one
# MAGIC object spread across multiple lines.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Another way to read JSON
# MAGIC
# MAGIC **`.json(path)`** is shorthand for **`format("json").load(path)`**.

# COMMAND ----------

zone_shorthand = spark.read.json(zone_json_path)

zone_shorthand.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Schema inference
# MAGIC
# MAGIC Unlike CSV, a JSON read without a schema does not return every column as
# MAGIC **`string`**. Spark infers the field names and types from the records.

# COMMAND ----------

zone_inferred = spark.read.format("json").load(zone_json_path)

zone_inferred.printSchema()

print(f"Row count: {zone_inferred.count()} (expect 22 for the course zone_lookup file)")

# COMMAND ----------

# MAGIC %md
# MAGIC Compare with the expected schema in the next section: **`location_id`** is
# MAGIC inferred as **`long`** instead of **`int`**, and the fields are sorted
# MAGIC alphabetically, so **`borough_name`** comes first.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Explicit schema
# MAGIC
# MAGIC For a known schema, define the field names and types up front and pass the schema to **`.schema(...)`**.
# MAGIC
# MAGIC As introduced in Module 2, you can provide the schema as either a **DDL string** or a **`StructType`**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5a. DDL schema string

# COMMAND ----------

zone_schema_ddl = """
location_id int,
borough_name string,
zone_name string,
service_zone string
"""

zone = spark.read.format("json").schema(zone_schema_ddl).load(zone_json_path)

print("Read with DDL schema:")
zone.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5b. `StructType` schema

# COMMAND ----------

zone_schema = StructType(
    [
        StructField("location_id", IntegerType()),
        StructField("borough_name", StringType()),
        StructField("zone_name", StringType()),
        StructField("service_zone", StringType()),
    ]
)

zone_via_struct = spark.read.format("json").schema(zone_schema).load(zone_json_path)

print("Same file read with StructType (schemas should match):")
zone_via_struct.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Missing and extra fields
# MAGIC
# MAGIC JSON fields are matched to the schema **by name**. With an explicit schema, a
# MAGIC **missing** key becomes **`null`**, and an **extra** key that is not in the
# MAGIC schema is **ignored**.
# MAGIC
# MAGIC In the demo file below, row 2 has no **`service_zone`**, and row 3 has an
# MAGIC extra **`region`** key.

# COMMAND ----------

schema_demo_path = f"{schema_demo_dir}zones.json"

dbutils.fs.mkdirs(schema_demo_dir)
dbutils.fs.put(
    schema_demo_path,
    """{"location_id": 1, "borough_name": "Manhattan", "zone_name": "Midtown East", "service_zone": "Yellow Zone"}
{"location_id": 2, "borough_name": "Brooklyn", "zone_name": "Williamsburg"}
{"location_id": 3, "borough_name": "Queens", "zone_name": "Astoria", "service_zone": "Boro Zone", "region": "NYC"}
""",
    overwrite=True,
)

schema_demo = spark.read.format("json").schema(zone_schema_ddl).load(schema_demo_path)
schema_demo.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Multiline JSON
# MAGIC
# MAGIC Some JSON files spread one object across multiple lines (pretty-printed
# MAGIC JSON). By default, Spark expects each line to be a complete JSON object, so
# MAGIC this layout needs **`multiLine=True`**.

# COMMAND ----------

multiline_json_path = f"{multiline_demo_dir}zone_multiline.json"

dbutils.fs.mkdirs(multiline_demo_dir)
dbutils.fs.put(
    multiline_json_path,
    """{
  "location_id": 901,
  "borough_name": "Demo",
  "zone_name": "Multiline Example",
  "service_zone": "Demo Zone"
}
""",
    overwrite=True,
)

print(f"Wrote multiline demo to {multiline_json_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7a. Without `multiLine`
# MAGIC
# MAGIC Spark parses each line on its own. No single line is a complete JSON object,
# MAGIC so every line becomes a row with all columns **`null`**.

# COMMAND ----------

(
    spark.read.format("json")
    .schema(zone_schema_ddl)
    .load(multiline_json_path)
    .show(truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7b. With `multiLine=True`
# MAGIC
# MAGIC Spark parses the whole file as one JSON document, so the object becomes one row.

# COMMAND ----------

(
    spark.read.format("json")
    .option("multiLine", True)
    .schema(zone_schema_ddl)
    .load(multiline_json_path)
    .show(truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Light reshape
# MAGIC
# MAGIC Before writing the JSON output, use **`select()`** to keep only the columns needed for this example.

# COMMAND ----------

zone_subset = zone.select(
    F.col("location_id"),
    F.col("borough_name"),
    F.col("zone_name"),
)

zone_subset.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. JSON round trip
# MAGIC
# MAGIC Write the selected columns to **`practice/zone_lookup_json_roundtrip/`** using **`mode("overwrite")`**, so each re-run replaces the existing data in that folder. Then read the JSON output back into Spark.
# MAGIC
# MAGIC JSON keeps field names and values, but not Spark data types. Without a schema, Spark infers the types again, so **`location_id`** comes back as **`long`** instead of **`int`**.

# COMMAND ----------

zone_subset.write.format("json").mode("overwrite").save(practice_output_path)

print(f"Wrote JSON folder to {practice_output_path}")
display(dbutils.fs.ls(practice_output_path))

# COMMAND ----------

roundtrip_inferred = spark.read.format("json").load(practice_output_path)

print("Re-read without an explicit schema (types inferred again):")
roundtrip_inferred.printSchema()

# COMMAND ----------

zone_subset_schema_ddl = "location_id int, borough_name string, zone_name string"

roundtrip_typed = (
    spark.read.format("json").schema(zone_subset_schema_ddl).load(practice_output_path)
)

print("Re-read with explicit schema (types restored):")
roundtrip_typed.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - **JSON Lines** — one complete JSON object per line. Spark's JSON reader expects this layout by default.
# MAGIC - **Schema inference** — without a schema, Spark infers field names and types. For this file, **`location_id`** is inferred as **`long`**, and the fields are sorted alphabetically.
# MAGIC - **Explicit schema** — pass either a **DDL string** or a **`StructType`** to **`.schema(...)`** to control field order and types. JSON fields are matched **by name**, not by position.
# MAGIC - **Missing and extra fields** — a missing field becomes **`null`**; a field that is not in the schema is ignored.
# MAGIC - **Multiline JSON** — use **`multiLine=True`** when one JSON object spans multiple lines.
# MAGIC - **JSON round trip** — Spark writes a folder of **`part-`** files. JSON keeps field names and values but not Spark data types, so provide the schema again when reading it back.
# MAGIC
# MAGIC **Next:** **04 - Reading Parquet** — read **`trip_time`** from the landing
# MAGIC volume.
