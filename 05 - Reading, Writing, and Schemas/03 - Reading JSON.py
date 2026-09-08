# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Reading JSON
# MAGIC
# MAGIC Read **`zone_lookup`** (JSON Lines) from landing.
# MAGIC
# MAGIC `/Volumes/rideshare_dev/landing/source_files/zone_lookup/`.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Read JSON Lines with an explicit schema
# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Import PySpark helpers and set paths for the **`zone_lookup`** dataset.
# MAGIC
# MAGIC Course **`zone_lookup`** columns (from `docs/data/dataset-overview.md`):
# MAGIC **`location_id`** (int), **`borough_name`** (string), **`zone_name`**
# MAGIC (string), **`service_zone`** (string).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

landing_root = "/Volumes/rideshare_dev/landing/source_files"
zone_json_path = f"{landing_root}/zone_lookup/zone_lookup.json"
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
# MAGIC **`zone_lookup/zone_lookup.json`** was copied into the landing volume in
# MAGIC Notebook 01. Format notebooks in this module read through **`/Volumes/...`**
# MAGIC paths only.

# COMMAND ----------

display(dbutils.fs.ls(f"{landing_root}/zone_lookup"))

# COMMAND ----------

# MAGIC %md
# MAGIC You should see **`zone_lookup.json`** in that folder. The path variable
# MAGIC **`zone_json_path`** points to the full file for the reads below.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. JSON Lines format
# MAGIC
# MAGIC **JSON Lines** (newline-delimited JSON) means **one complete JSON object per
# MAGIC line**. Spark's JSON reader expects this layout by default — no
# MAGIC **`multiLine=True`** needed for the landing file.

# COMMAND ----------

print("First 500 characters of zone_lookup.json:")
print(dbutils.fs.head(zone_json_path, 500))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Notice how each line is a complete `{...}` object — that is JSON Lines format.
# MAGIC In section 7, you will see the opposite: a single record spread across
# MAGIC multiple lines (pretty-printed JSON), which requires a different read option.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Read syntax — shorthand vs generic
# MAGIC
# MAGIC Spark exposes two equivalent ways to read JSON:

# COMMAND ----------

zone_inferred_shorthand = spark.read.json(zone_json_path)

zone_inferred = spark.read.format("json").load(zone_json_path)

print("Shorthand — .json(path):")
zone_inferred_shorthand.printSchema()

print("Generic — .format('json').load(path):")
zone_inferred.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Both approaches give the same result. The difference is just syntax:
# MAGIC
# MAGIC | Syntax | When to use |
# MAGIC |--------|-------------|
# MAGIC | `.json(path)` | Quick exploration — short and convenient |
# MAGIC | `.format("json").load(path)` | Recommended — same pattern works for CSV, Parquet, Avro, and XML |
# MAGIC
# MAGIC This notebook uses `format("json")` from here onward. The variable
# MAGIC `zone_inferred` carries forward until section 5, where we apply an
# MAGIC explicit schema.
# MAGIC
# MAGIC > **Note:** Columns appear in alphabetical order (`borough_name` first,
# MAGIC > not `location_id`). When Spark infers a JSON schema, it sorts field
# MAGIC > names A–Z. Use an explicit schema (section 5) to control column order.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Inferred read and inspect
# MAGIC
# MAGIC Notebook 02 showed CSV columns defaulting to **`string`** until you infer or
# MAGIC declare types. JSON is different: without a schema, Spark **infers field
# MAGIC names and types** from the file.

# COMMAND ----------

print("Inferred schema:")
zone_inferred.printSchema()

print("\nSample row:")
zone_inferred.show(1, vertical=True)

row_count = zone_inferred.count()
print(f"\nRow count: {row_count} (expect 22 for the course zone_lookup file)")

# COMMAND ----------

# MAGIC %md
# MAGIC **`printSchema()`** inspects metadata on the driver — it does not modify
# MAGIC data or trigger a Spark job. **`count()`** is an action that executes the
# MAGIC read plan.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Explicit schema
# MAGIC
# MAGIC For stable pipelines, declare the contract up front. Module 2 introduced DDL
# MAGIC strings and **`StructType`**; file reads accept either via **`.schema(...)`**.

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
zone.show(1, vertical=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5b. `StructType` schema

# COMMAND ----------

zone_schema = StructType(
    [
        StructField("location_id", IntegerType(), False),
        StructField("borough_name", StringType(), False),
        StructField("zone_name", StringType(), False),
        StructField("service_zone", StringType(), False),
    ]
)

zone_via_struct = spark.read.format("json").schema(zone_schema).load(zone_json_path)

print("Same file read with StructType (schemas should match):")
zone_via_struct.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Use **`zone`** (DDL read) for the rest of this notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Missing and extra fields
# MAGIC
# MAGIC With an explicit schema, a **missing** JSON key becomes **`null`**; an
# MAGIC **extra** key not in the schema is **ignored**. That is read-time schema
# MAGIC contract behavior — not Delta schema evolution (Module 10). Use a tiny demo
# MAGIC file under **`practice/`**, not the landing source.

# COMMAND ----------

schema_demo_path = f"{schema_demo_dir}zones.json"

dbutils.fs.mkdirs(schema_demo_dir)
dbutils.fs.put(
    schema_demo_path,
    """{"location_id": 1, "borough_name": "Manhattan", "zone_name": "Midtown East", "service_zone": "Yellow Zone"}
{"location_id": 2, "borough_name": "Brooklyn", "zone_name": "Williamsburg"}
{"location_id": 3, "borough_name": "Queens", "zone_name": "Astoria", "service_zone": "Boro Zone", "region": "NYC"}
""",
    True,
)

print(f"Wrote demo file to {schema_demo_path}")

# COMMAND ----------

schema_demo = spark.read.format("json").schema(zone_schema_ddl).load(schema_demo_path)

print("Missing service_zone → null; extra region → not in schema:")
schema_demo.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Row 2 is missing **`service_zone`** → **`null`**. Row 3 includes **`region`**
# MAGIC in the file, but the explicit schema has no **`region`** column, so Spark
# MAGIC drops it on read.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Multiline JSON
# MAGIC
# MAGIC Pretty-printed exports put **one JSON object across multiple lines**. The
# MAGIC default reader (JSON Lines mode) treats **each line** as its own record, which
# MAGIC fails on split objects. Set **`multiLine=True`** to read the whole file as
# MAGIC one record (or one array). Landing **`zone_lookup.json`** does not need this.

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
    True,
)

print(f"Wrote multiline demo to {multiline_json_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC Read without **`multiLine`** — each line is parsed separately, so broken
# MAGIC partial lines fail or produce corrupt rows.

# COMMAND ----------

print("Without multiLine (JSON Lines mode — expect problems):")
try:
    spark.read.format("json").load(multiline_json_path).show(truncate=False)
except Exception as exc:
    print(f"Read failed or returned unusable rows: {type(exc).__name__}")
    print(str(exc)[:400])

# COMMAND ----------

print("With multiLine=True (whole file is one record):")
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
# MAGIC **`select`** columns useful for later joins to **`trip`** on
# MAGIC **`pickup_location_id`** / **`dropoff_location_id`**. Deeper transforms belong
# MAGIC in Module 6.

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
# MAGIC Write the subset to **`practice/zone_lookup_json_roundtrip/`**, then read it
# MAGIC back. Spark writes **`part-*.json`** files under that directory. Reads in
# MAGIC section 3 showed both syntaxes; the write below uses
# MAGIC **`format("json").save(...)`** (recommended). The shorthand **`.json(...)`**
# MAGIC equivalent is shown as a comment only.

# COMMAND ----------

zone_subset.write.format("json").mode("overwrite").save(practice_output_path)

# Shorthand equivalent:
# zone_subset.write.mode("overwrite").json(practice_output_path)

print(f"Wrote JSON folder to {practice_output_path}")
display(dbutils.fs.ls(practice_output_path))

# COMMAND ----------

roundtrip_inferred = spark.read.format("json").load(practice_output_path)

print("Re-read without explicit schema (Spark infers again):")
roundtrip_inferred.printSchema()
roundtrip_inferred.show(1, vertical=True)

# COMMAND ----------

zone_subset_schema_ddl = "location_id int, borough_name string, zone_name string"

roundtrip_typed = (
    spark.read.format("json").schema(zone_subset_schema_ddl).load(practice_output_path)
)

print("Re-read with explicit schema (production pattern):")
roundtrip_typed.printSchema()
roundtrip_typed.show(1, vertical=True)

# COMMAND ----------

# MAGIC %md
# MAGIC JSON re-infers types more readily than CSV, but explicit schema on read still
# MAGIC makes the contract clear. We used **`.mode("overwrite")`** so re-runs replace
# MAGIC the prior folder. Save modes in depth are in **07 - Write Patterns and Table
# MAGIC Preview**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC Build a small practice extract without reusing **`zone_subset`**:
# MAGIC
# MAGIC 1. Read **`zone_lookup.json`** again with the full **`zone_schema_ddl`** (or
# MAGIC    **`zone_schema`**) into a new DataFrame (do not reuse **`zone`** or
# MAGIC    **`zone_subset`**).
# MAGIC 2. **`select`** exactly these three columns: **`location_id`**, **`zone_name`**,
# MAGIC    **`service_zone`**.
# MAGIC 3. Write the result to
# MAGIC    **`/Volumes/rideshare_dev/processed/output_files/practice/zone_exercise/`**
# MAGIC    with **`.mode("overwrite")`** (use either **`format("json").save(...)`** or
# MAGIC    **`.json(...)`** — same as section 9).
# MAGIC 4. Re-read the written folder with an explicit schema for those three columns
# MAGIC    and print the schema. Confirm **`location_id`** is **`int`** and the string
# MAGIC    columns are **`string`**, not mis-inferred types.

# COMMAND ----------

# Your code here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - **JSON syntax** — **`.json(path)`** shorthand and
# MAGIC   **`format("json").load(...)`** / **`format("json").save(...)`** are
# MAGIC   equivalent; prefer **`format("json")`** in this module
# MAGIC - **JSON Lines** — one `{...}` object per line; default reader layout for
# MAGIC   **`zone_lookup.json`**
# MAGIC - **Inferred read** — JSON infers names and types without a schema (unlike
# MAGIC   CSV default strings)
# MAGIC - **Explicit schema (DDL or `StructType`)** — recommended production pattern
# MAGIC - **Missing / extra fields** — nulls and ignored keys at read time; not schema
# MAGIC   evolution (Module 10)
# MAGIC - **Multiline JSON** — **`multiLine=True`** for pretty-printed multi-line
# MAGIC   records
# MAGIC - **JSON round trip** — writes a directory of part files; re-apply explicit
# MAGIC   schema for a clear contract
# MAGIC
# MAGIC **Next:** **04 - Reading Parquet** — read **`trip_time`** from the landing
# MAGIC volume.
