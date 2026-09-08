# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Reading XML
# MAGIC
# MAGIC Read **`drivers`** with **`rowTag`** only — nested flatten is Module 6.
# MAGIC
# MAGIC `/Volumes/rideshare_dev/landing/source_files/drivers/`.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Read XML with **`rowTag`** only — no **`explode`**
# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Import PySpark helpers and set paths for the **`drivers`** dataset.
# MAGIC
# MAGIC Course **`drivers`** fields (from `docs/data/dataset-overview.md`):
# MAGIC **`driver_id`** (string), **`name`** (string), **`license_number`**
# MAGIC (string), **`vehicle`** (struct: make, model, year, body_type),
# MAGIC **`trips_assigned`** (repeated **`trip_id`** list).

# COMMAND ----------

from pyspark.sql import functions as F

landing_root = "/Volumes/rideshare_dev/landing/source_files"
drivers_xml_path = f"{landing_root}/drivers/drivers.xml"
practice_root = "/Volumes/rideshare_dev/processed/output_files/practice"
practice_output_path = f"{practice_root}/drivers_xml_roundtrip/"

print(f"drivers_xml_path = {drivers_xml_path}")
print(f"practice_output_path = {practice_output_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Source path
# MAGIC
# MAGIC **`drivers/drivers.xml`** was copied into the landing volume in Notebook 01.
# MAGIC Format notebooks in this module read through **`/Volumes/...`** paths only.

# COMMAND ----------

display(dbutils.fs.ls(f"{landing_root}/drivers"))

# COMMAND ----------

# MAGIC %md
# MAGIC You should see **`drivers.xml`** in that folder. The path variable
# MAGIC **`drivers_xml_path`** points to the full file for the reads below.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. XML layout and `rowTag`
# MAGIC
# MAGIC Peek at the file. The root element is **`<drivers>`**; each record is a
# MAGIC **`<driver>`** child. **`rowTag`** tells Spark which element is one row —
# MAGIC without it, the reader does not know where records start and end.

# COMMAND ----------

print("First 700 characters of drivers.xml:")
print(dbutils.fs.head(drivers_xml_path, 700))

# COMMAND ----------

# MAGIC %md
# MAGIC Notice the nesting: **`vehicle`** is a struct-like child element, and
# MAGIC **`trips_assigned`** repeats **`trip_id`**. We will inspect those nested
# MAGIC columns after the read — without flattening them with **`explode`**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Read with `rowTag`
# MAGIC
# MAGIC Use **`format("xml")`** and set **`.option("rowTag", "driver")`**. There is
# MAGIC no compact **`.xml(path)`** shorthand like CSV/JSON/Parquet in this module —
# MAGIC the generic DataSource API is the pattern.

# COMMAND ----------

print("Without rowTag (expect failure):")
try:
    spark.read.format("xml").load(drivers_xml_path).show(1)
except Exception as exc:
    print(f"{type(exc).__name__}: {str(exc)[:400]}")

# COMMAND ----------

drivers = (
    spark.read.format("xml").option("rowTag", "driver").load(drivers_xml_path)
)

print("With rowTag='driver':")
drivers.printSchema()
drivers.show(1, vertical=True, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **`rowTag`** is required for this feed. Use **`drivers`** for the rest of
# MAGIC this notebook. Nested columns are still nested — that is intentional for
# MAGIC Module 5.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Inspect schema and nested columns
# MAGIC
# MAGIC Confirm row count (expect **12**), then look at **`vehicle`** and
# MAGIC **`trips_assigned`** without calling **`explode`**.

# COMMAND ----------

print("Full schema (nested types expected):")
drivers.printSchema()

row_count = drivers.count()
print(f"\nRow count: {row_count} (expect 12 for the course drivers file)")

# COMMAND ----------

print("Top-level columns only:")
drivers.select(
    F.col("driver_id"),
    F.col("name"),
    F.col("license_number"),
).show(3, truncate=False)

print("\nNested vehicle struct (still one column):")
drivers.select(F.col("driver_id"), F.col("vehicle")).show(2, truncate=False)

print("\ntrips_assigned stays nested (no explode — Module 6):")
drivers.select(F.col("driver_id"), F.col("trips_assigned")).show(2, truncate=False)

# COMMAND ----------

# DBTITLE 1,Nested columns explained
# MAGIC %md
# MAGIC
# MAGIC Looking at the output above, notice two kinds of nested columns:
# MAGIC
# MAGIC | Column | Type | What it holds |
# MAGIC |--------|------|---------------|
# MAGIC | `vehicle` | struct | A single object with fields: `make`, `model`, `year`, `body_type` |
# MAGIC | `trips_assigned` | struct containing an array of longs | A wrapper with a `trip_id` field holding a list of trip IDs |
# MAGIC
# MAGIC You can access struct fields using dot notation (e.g. **`vehicle.make`**) —
# MAGIC we do this in section 5.
# MAGIC
# MAGIC Each driver row holds an **array** of
# MAGIC **`trip_id`** values under **`trips_assigned.trip_id`**. You cannot join
# MAGIC that array directly to the **`trip`** table's scalar **`trip_id`** —
# MAGIC Spark needs one **`trip_id` per row**. **`explode()`** turns the array
# MAGIC into separate rows so the join is possible. That pattern is Module 6;
# MAGIC here we only read and inspect without flattening.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Light reshape
# MAGIC
# MAGIC Pull a few useful fields for a practice extract. Nested field paths like
# MAGIC **`vehicle.make`** access struct members without exploding arrays.

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
# MAGIC ## 6. Practice write
# MAGIC
# MAGIC Write the flat subset to **`practice/drivers_xml_roundtrip/`** as JSON
# MAGIC (familiar from Notebook 03), then re-read. We are not writing XML here —
# MAGIC the lesson goal is the **read** with **`rowTag`**.

# COMMAND ----------

drivers_subset.write.format("json").mode("overwrite").save(practice_output_path)

print(f"Wrote JSON folder to {practice_output_path}")
display(dbutils.fs.ls(practice_output_path))

# COMMAND ----------

roundtrip = spark.read.format("json").load(practice_output_path)

print("Re-read practice output:")
roundtrip.printSchema()
roundtrip.show(3, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC We used **`.mode("overwrite")`** so re-runs replace the prior folder. Save
# MAGIC modes in depth are in **07 - Write Patterns and Table Preview**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC Build a small practice extract without reusing **`drivers_subset`**:
# MAGIC
# MAGIC 1. Read **`drivers.xml`** again with **`rowTag="driver"`** into a new
# MAGIC    DataFrame (do not reuse **`drivers`** or **`drivers_subset`**).
# MAGIC 2. **`select`** exactly these columns: **`driver_id`**, **`license_number`**,
# MAGIC    and **`vehicle.year`** aliased as **`vehicle_year`**. Do **not** call
# MAGIC    **`explode`**.
# MAGIC 3. Write the result to
# MAGIC    **`/Volumes/rideshare_dev/processed/output_files/practice/drivers_exercise/`**
# MAGIC    with **`.mode("overwrite")`** as JSON
# MAGIC    (**`format("json").save(...)`**).
# MAGIC 4. Re-read the written folder and print the schema. Confirm the three
# MAGIC    column names above are present.

# COMMAND ----------

# Your code here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - **XML read** — use **`format("xml")`** with **`.option("rowTag", ...)`**;
# MAGIC   **`rowTag`** is required for this feed
# MAGIC - **Nested columns** — **`vehicle`** (struct) and **`trips_assigned`** stay
# MAGIC   nested; inspect without **`explode`**
# MAGIC - **Struct field access** — paths like **`vehicle.make`** reshape without
# MAGIC   flattening arrays
# MAGIC - **`explode()`** on **`trips_assigned.trip_id`** — required before joining
# MAGIC   drivers to **`trip`** on **`trip_id`** → Module 6
# MAGIC
# MAGIC **Next:** **06 - Reading Avro** — read **`payment`** from the landing
# MAGIC volume.
