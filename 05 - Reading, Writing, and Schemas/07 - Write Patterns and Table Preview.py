# Databricks notebook source
# MAGIC %md
# MAGIC # 07 - Write Patterns and Table Preview
# MAGIC
# MAGIC Save modes, a brief partitioned write, Delta as a **file** format, and a
# MAGIC managed **`saveAsTable`** preview.
# MAGIC
# MAGIC Landing **`trip_time`** (and prior practice patterns).
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Use save modes and a brief partitioned write
# MAGIC - Preview Delta as a file format under `practice/` and create managed table
# MAGIC   **`rideshare_dev.processed.trip_time_preview`**
# MAGIC - Distinguish files vs managed tables
# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Import PySpark helpers and set the landing path, practice output paths,
# MAGIC and managed table name used below.
# MAGIC
# MAGIC Course **`trip_time`** columns (from `docs/data/dataset-overview.md`):
# MAGIC **`trip_id`** (bigint), **`trip_date`** (date), **`hour_of_day`** (int).
# MAGIC
# MAGIC Practice outputs for this notebook:
# MAGIC **`write_modes_demo/`**, **`trip_time_partitioned/`**,
# MAGIC **`trip_time_delta_file/`**, plus managed table
# MAGIC **`rideshare_dev.processed.trip_time_preview`**.

# COMMAND ----------

from pyspark.sql import functions as F

landing_root = "/Volumes/rideshare_dev/landing/source_files"
trip_time_parquet_path = f"{landing_root}/trip_time/trip_time.parquet"
practice_root = "/Volumes/rideshare_dev/processed/output_files/practice"

save_modes_path = f"{practice_root}/write_modes_demo/"
partitioned_path = f"{practice_root}/trip_time_partitioned/"
delta_file_path = f"{practice_root}/trip_time_delta_file/"
managed_table = "rideshare_dev.processed.trip_time_preview"

print(f"trip_time_parquet_path = {trip_time_parquet_path}")
print(f"practice_root = {practice_root}")
print(f"managed_table = {managed_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Source path
# MAGIC
# MAGIC The write demos below reuse **`trip_time`**. Notebook 01 copied
# MAGIC **`trip_time.parquet`** into the landing volume. You already read that
# MAGIC file in **04 - Reading Parquet**.
# MAGIC
# MAGIC Confirm the file is still there, then load it with an explicit schema.
# MAGIC Use the **`/Volumes/...`** path — do not hardcode an **`abfss://`** URL.

# COMMAND ----------

display(dbutils.fs.ls(f"{landing_root}/trip_time"))

# COMMAND ----------

# MAGIC %md
# MAGIC You should see **`trip_time.parquet`**. The next cell builds
# MAGIC **`write_source`** — the DataFrame every save-mode and write demo uses.

# COMMAND ----------

trip_time_schema_ddl = """
trip_id bigint,
trip_date date,
hour_of_day int
"""

trip_time = (
    spark.read.format("parquet")
    .schema(trip_time_schema_ddl)
    .load(trip_time_parquet_path)
)

write_source = trip_time.select(
    F.col("trip_id"),
    F.col("trip_date"),
    F.col("hour_of_day"),
)

print(f"write_source rows = {write_source.count()} (expect 100)")
write_source.printSchema()
write_source.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Save modes
# MAGIC
# MAGIC **`.mode(...)`** tells Spark what to do when the output path already
# MAGIC has data. **`write`** is an **action** (Module 4) — each cell below
# MAGIC runs the write as soon as you execute it.
# MAGIC
# MAGIC All four modes write to the same path:
# MAGIC **`practice/write_modes_demo/`**. Run the cells in order so the counts
# MAGIC match the notes.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2a. `overwrite`
# MAGIC
# MAGIC Delete existing files at the path, then write the new DataFrame.
# MAGIC After this cell, the folder should contain **5** rows.

# COMMAND ----------

(
    write_source.limit(5)
    .write.format("parquet")
    .mode("overwrite")
    .save(save_modes_path)
)

print("After overwrite (expect 5 rows):")
print(spark.read.format("parquet").load(save_modes_path).count())
display(dbutils.fs.ls(save_modes_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b. `append`
# MAGIC
# MAGIC Keep the existing files and add new ones. Row count increases. Useful
# MAGIC when each run adds a new batch; risky if you re-run the same batch by
# MAGIC mistake and double the rows.

# COMMAND ----------

(
    write_source.limit(5)
    .write.format("parquet")
    .mode("append")
    .save(save_modes_path)
)

print("After append (expect 10 rows if you ran overwrite once, then append once):")
print(spark.read.format("parquet").load(save_modes_path).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2c. `ignore`
# MAGIC
# MAGIC If the path already has data, Spark skips the write. No error. No
# MAGIC change to the existing files or row count.

# COMMAND ----------

count_before_ignore = spark.read.format("parquet").load(save_modes_path).count()

(
    write_source.limit(3)
    .write.format("parquet")
    .mode("ignore")
    .save(save_modes_path)
)

count_after_ignore = spark.read.format("parquet").load(save_modes_path).count()
print(f"Before ignore: {count_before_ignore}")
print(f"After ignore:  {count_after_ignore} (unchanged when path already exists)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2d. `errorifexists`
# MAGIC
# MAGIC If the path already has data, Spark raises an error and does not write.
# MAGIC Use this when a second write to the same path should fail loudly.
# MAGIC Spark also accepts **`"error"`** as the same mode (the default when you
# MAGIC omit **`.mode(...)`**).

# COMMAND ----------

print("errorifexists when path exists (expect failure):")
try:
    (
        write_source.limit(1)
        .write.format("parquet")
        .mode("errorifexists")
        .save(save_modes_path)
    )
except Exception as exc:
    print(f"{type(exc).__name__}: {str(exc)[:400]}")

# COMMAND ----------

# MAGIC %md
# MAGIC | Mode | If the path already has data |
# MAGIC |------|------------------------------|
# MAGIC | **`overwrite`** | Replace the existing files |
# MAGIC | **`append`** | Add more files (row count grows) |
# MAGIC | **`ignore`** | Skip the write; leave existing files |
# MAGIC | **`errorifexists`** / **`error`** | Raise an error (default) |
# MAGIC
# MAGIC Always set **`.mode(...)`** explicitly. Relying on the default
# MAGIC (**`error`**) or on habit (**`overwrite`**) without checking the path
# MAGIC is a common source of failed jobs or lost data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Brief partitioned write
# MAGIC
# MAGIC **`.partitionBy("hour_of_day")`** writes subfolders named
# MAGIC **`hour_of_day=<value>/`** under the output path — for example
# MAGIC **`hour_of_day=8/`**. Later jobs can read one folder instead of the
# MAGIC whole dataset. This module only shows the write layout.

# COMMAND ----------

(
    write_source.write.format("parquet")
    .mode("overwrite")
    .partitionBy("hour_of_day")
    .save(partitioned_path)
)

print(f"Wrote partitioned Parquet to {partitioned_path}")
display(dbutils.fs.ls(partitioned_path))

# COMMAND ----------

# MAGIC %md
# MAGIC You should see directories named **`hour_of_day=<value>`**. Reading the
# MAGIC parent folder still returns every row; Spark adds **`hour_of_day`**
# MAGIC from the folder names.

# COMMAND ----------

partitioned_read = spark.read.format("parquet").load(partitioned_path)

print("Schema after partitioned read:")
partitioned_read.printSchema()
print(f"Row count: {partitioned_read.count()} (expect 100)")
partitioned_read.groupBy("hour_of_day").count().orderBy("hour_of_day").show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Delta file write under `practice/`
# MAGIC
# MAGIC Write with **`format("delta")`** to a Volume path under **`practice/`**
# MAGIC — the same folder pattern as Parquet or JSON, different format name.
# MAGIC This notebook only creates and re-reads the folder. ACID transactions,
# MAGIC **`MERGE`**, and time travel are covered in Module 10.
# MAGIC
# MAGIC Delta is one storage format among others. Module 5 still lands and
# MAGIC writes CSV, JSON, Parquet, XML, and Avro where those formats fit.

# COMMAND ----------

(
    write_source.write.format("delta")
    .mode("overwrite")
    .save(delta_file_path)
)

print(f"Wrote Delta files to {delta_file_path}")
display(dbutils.fs.ls(delta_file_path))

# COMMAND ----------

delta_from_path = spark.read.format("delta").load(delta_file_path)

print("Re-read Delta from Volume path:")
delta_from_path.printSchema()
print(f"Row count: {delta_from_path.count()}")
delta_from_path.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC The directory listing should include **`_delta_log/`** — Delta’s
# MAGIC transaction log next to the data files. You still address this output
# MAGIC with a Volume path string, not a catalog table name.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Managed `saveAsTable` into `rideshare_dev.processed`
# MAGIC
# MAGIC **`saveAsTable`** creates (or replaces) a **managed** table in Unity
# MAGIC Catalog. Spark stores the files in the catalog’s **managed location**
# MAGIC from Notebook 01 — not under
# MAGIC **`/Volumes/rideshare_dev/processed/output_files/`**.

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {managed_table}")

(
    write_source.write.format("delta")
    .mode("overwrite")
    .saveAsTable(managed_table)
)

print(f"Created managed table {managed_table}")

# COMMAND ----------

display(spark.sql(f"DESCRIBE EXTENDED {managed_table}"))

# COMMAND ----------

# MAGIC %md
# MAGIC In the **`DESCRIBE EXTENDED`** output, check **`Type`** and
# MAGIC **`Location`** (labels can vary slightly by runtime). **`Location`**
# MAGIC should be the catalog managed storage path — not
# MAGIC **`/Volumes/rideshare_dev/processed/output_files/...`**.
# MAGIC
# MAGIC **`DROP TABLE IF EXISTS`** above lets you re-run this section cleanly.
# MAGIC Notebook **99** Level 1 deletes files under **`practice/`** only.
# MAGIC Level 4 drops the whole **`rideshare_dev`** catalog, including managed
# MAGIC tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Files vs tables
# MAGIC
# MAGIC Compare the two outputs you just created: Delta files at
# MAGIC **`delta_file_path`**, and the managed table **`managed_table`**. Same
# MAGIC columns and row count; different name and storage location.

# COMMAND ----------

print("Delta FILE on external volume:")
print(f"  path = {delta_file_path}")
print(f"  rows = {spark.read.format('delta').load(delta_file_path).count()}")

print("\nManaged TABLE in Unity Catalog:")
print(f"  name = {managed_table}")
print(f"  rows = {spark.table(managed_table).count()}")

print("\nQuery the table with the DataFrame API:")
spark.table(managed_table).show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC | | Delta file under `practice/` | Managed `saveAsTable` |
# MAGIC |---|------------------------------|------------------------|
# MAGIC | How you name it | Volume path string | `catalog.schema.table` |
# MAGIC | Where files are stored | External volume `output_files` | Catalog managed location |
# MAGIC | Who controls access | Volume / path permissions | Unity Catalog table privileges (Module 11) |
# MAGIC | Delta features (MERGE, time travel, …) | Module 10 | Module 10 |
# MAGIC
# MAGIC Both can use **`format("delta")`**. In Module 5, the point is which
# MAGIC name you use and which storage location holds the files.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC Write a second extract. Do not reuse the worked-output paths above
# MAGIC (**`write_modes_demo/`**, **`trip_time_partitioned/`**,
# MAGIC **`trip_time_delta_file/`**, or **`trip_time_preview`**).
# MAGIC
# MAGIC 1. From **`write_source`** (or a fresh read of **`trip_time.parquet`**),
# MAGIC    **`select`** **`trip_id`** and **`trip_date`** only.
# MAGIC 2. Write that extract as Parquet to
# MAGIC    **`/Volumes/rideshare_dev/processed/output_files/practice/trip_date_exercise/`**
# MAGIC    with **`.mode("overwrite")`** and **`.partitionBy("trip_date")`**.
# MAGIC 3. List the partition folders with **`dbutils.fs.ls`**.
# MAGIC 4. Write the same two-column extract as a managed Delta table named
# MAGIC    **`rideshare_dev.processed.trip_date_preview`** (drop it first if it
# MAGIC    exists), then **`show(3)`** via **`spark.table(...)`**.

# COMMAND ----------

# Your code here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - **Save modes** — **`overwrite`**, **`append`**, **`ignore`**, and
# MAGIC   **`errorifexists`** / **`error`** control what happens when the path
# MAGIC   already has data; **`write`** is an action
# MAGIC - **Partitioned write** — **`.partitionBy(...)`** creates
# MAGIC   **`column=value/`** folders under **`practice/`**
# MAGIC - **Delta file write** — **`format("delta").save(volume_path)`** writes
# MAGIC   Delta files on the external volume (Module 10 covers **`MERGE`**,
# MAGIC   time travel, and related features). Delta does not replace CSV,
# MAGIC   JSON, Parquet, or Avro for every job
# MAGIC - **Managed `saveAsTable`** — registers
# MAGIC   **`rideshare_dev.processed.<table>`**; files go to the catalog
# MAGIC   managed location, not the external **`output_files`** volume
# MAGIC - **Files vs tables** — Volume path vs **`catalog.schema.table`**
# MAGIC   (table privileges → Module 11)
# MAGIC
# MAGIC **Next:** Module 6 `01 - Column Transforms with Built-in Functions`.
# MAGIC Use **99 - Rideshare Project Cleanup and Reset** to clear **`practice/`**
# MAGIC or tear down the project — recovery only, not the successor.
