# Databricks notebook source
# MAGIC %md
# MAGIC # 07 - Write Patterns and Table Preview
# MAGIC
# MAGIC Write the **`trip_time`** dataset with save modes, a partition column,
# MAGIC Delta files, and a managed table.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Use the four save modes
# MAGIC - Write a partitioned output
# MAGIC - Write Delta files to a Volume path
# MAGIC - Create a managed table with **`saveAsTable`**
# MAGIC - Read files by path and a table by name

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach **all-purpose compute**. The next cell sets the source path, the
# MAGIC practice output paths, and the managed table name.

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
# MAGIC This notebook reuses **`trip_time.parquet`** from **04 - Reading Parquet**.

# COMMAND ----------

display(dbutils.fs.ls(f"{landing_root}/trip_time"))

# COMMAND ----------

# MAGIC %md
# MAGIC Load the file with an explicit schema into **`write_source`**. Every write
# MAGIC below uses this DataFrame.

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
# MAGIC **`.mode(...)`** tells Spark what to do when the output already exists.
# MAGIC A write operation such as **`.save(...)`** is an action. Spark executes
# MAGIC the write when that operation runs.
# MAGIC
# MAGIC All four demos write to **`practice/write_modes_demo/`**. Run them in
# MAGIC order so the row counts match.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2a. `overwrite`
# MAGIC
# MAGIC Replace the existing output with the new DataFrame. Expect **5** rows.

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
# MAGIC Add the new rows to the existing output. Expect **10** rows.

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
# MAGIC Skip the write when the output already exists. The row count stays the
# MAGIC same.

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
print(f"After ignore:  {count_after_ignore} (unchanged when output already exists)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2d. `errorifexists`
# MAGIC
# MAGIC Raise an error when the output already exists. Nothing is written.
# MAGIC **`"error"`** is the same mode, and it is the default when you omit
# MAGIC **`.mode(...)`**.

# COMMAND ----------

print("errorifexists when output exists (expect failure):")
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
# MAGIC | Mode | If the output already exists |
# MAGIC |------|------------------------------|
# MAGIC | **`overwrite`** | Replace the existing output |
# MAGIC | **`append`** | Add more files; the row count grows |
# MAGIC | **`ignore`** | Skip the write |
# MAGIC | **`errorifexists`** / **`error`** | Raise an error (default) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Partitioned write
# MAGIC
# MAGIC **`.partitionBy("hour_of_day")`** writes **`hour_of_day=<value>/`**
# MAGIC subfolders under the output path, for example **`hour_of_day=8/`**.

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
# MAGIC You should see folders named **`hour_of_day=<value>`**. Reading the parent
# MAGIC folder returns all **100** rows, and Spark adds **`hour_of_day`** back
# MAGIC from the folder names.

# COMMAND ----------

partitioned_read = spark.read.format("parquet").load(partitioned_path)

print("Schema after partitioned read:")
partitioned_read.printSchema()
print(f"Row count: {partitioned_read.count()} (expect 100)")
partitioned_read.groupBy("hour_of_day").count().orderBy("hour_of_day").show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Delta file write
# MAGIC
# MAGIC Write **`write_source`** with **`format("delta")`** to
# MAGIC **`practice/trip_time_delta_file/`**, then read it back by path.
# MAGIC ACID transactions, **`MERGE`**, and time travel are covered in a later
# MAGIC Delta lesson.
# MAGIC
# MAGIC Module 5 still works with CSV, JSON, Parquet, XML, and Avro where those
# MAGIC formats fit.

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
# MAGIC In the folder listing, the data files are Parquet files, and
# MAGIC **`_delta_log/`** sits next to them. You read this output by its Volume
# MAGIC path, not by a table name.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Managed table with `saveAsTable`
# MAGIC
# MAGIC Drop the table if it exists, so the cell can be re-run. Then create the
# MAGIC managed table **`rideshare_dev.processed.trip_time_preview`** with
# MAGIC **`saveAsTable`**.

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
# MAGIC Check **`Type`** and **`Location`**. **`Location`** should be the catalog
# MAGIC managed location, not **`/Volumes/rideshare_dev/processed/output_files/...`**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Files vs tables
# MAGIC
# MAGIC Read both outputs: the Delta files by path and the managed table by name.
# MAGIC Both have the same rows.

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
# MAGIC | | Delta files under `practice/` | Managed table from `saveAsTable` |
# MAGIC |---|------------------------------|------------------------|
# MAGIC | How you name it | Volume path | `catalog.schema.table` |
# MAGIC | Where the files are stored | External volume `output_files` | Catalog managed location |
# MAGIC | Who controls access | Unity Catalog volume privileges | Unity Catalog table privileges |
# MAGIC
# MAGIC Both use **`format("delta")`**. The difference is how you name the data
# MAGIC and where the files are stored. Privileges are covered in a later Unity
# MAGIC Catalog lesson.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - **Save modes** — **`overwrite`**, **`append`**, **`ignore`**, and **`errorifexists`** decide what happens when the output already exists.
# MAGIC - **Partitioned write** — **`.partitionBy(...)`** writes one **`column=value/`** folder for each value.
# MAGIC - **Delta file write** — **`format("delta").save(path)`** writes Parquet data files and a **`_delta_log/`** folder to a Volume path.
# MAGIC - **Managed `saveAsTable`** — creates a managed table whose files go to the catalog managed location.
# MAGIC - **Files vs tables** — files are read by path; tables are read by name.
# MAGIC
# MAGIC **Next:** Module 6 **01 - Column Transforms with Built-in Functions**.
# MAGIC Use **99 - Rideshare Project Cleanup and Reset** only to clear
# MAGIC **`practice/`** or reset the project.
