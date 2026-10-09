# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
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
# MAGIC Attach **all-purpose compute**.

# COMMAND ----------

from pyspark.sql import functions as F

# Reading path
landing_root = "/Volumes/rideshare_dev/landing/source_files"
trip_time_parquet_path = f"{landing_root}/trip_time/trip_time.parquet"

# Writing path
practice_root = "/Volumes/rideshare_dev/processed/output_files/practice"
save_modes_path = f"{practice_root}/write_modes_demo/"
partitioned_path = f"{practice_root}/trip_time_partitioned/"
delta_file_path = f"{practice_root}/trip_time_delta_file/"

# Managed table
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
# MAGIC The notebook reads `trip_time.parquet` using an explicit schema, then selects the required columns into a DataFrame named `write_source`. All write examples in this lesson use this DataFrame.

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
# MAGIC `.mode(...)` tells Spark what to do when the output already exists. 
# MAGIC
# MAGIC A write operation such as `.save(...)` is an action that triggers Spark execution.
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
# MAGIC Raise an error when the output already exists.

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
# MAGIC ## 3. Partitioned write
# MAGIC
# MAGIC `.partitionBy("hour_of_day")` creates a separate subfolder for each distinct value in the `hour_of_day` column.

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

partitioned_read = spark.read.format("parquet").load(partitioned_path)

print("Schema after partitioned read:")
partitioned_read.printSchema()
print(f"Row count: {partitioned_read.count()} (expect 100)")
partitioned_read.groupBy("hour_of_day").count().orderBy("hour_of_day").show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Delta file write
# MAGIC
# MAGIC Writing Delta files follows the same approach as Parquet and JSON. Only the format name changes

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
# MAGIC ## 5. Managed table with `saveAsTable`
# MAGIC
# MAGIC `saveAsTable` writes the DataFrame data and creates a managed table in Unity Catalog

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
# MAGIC ## Summary
# MAGIC
# MAGIC - **Save modes** — **`overwrite`**, **`append`**, **`ignore`**, and **`errorifexists`** decide what happens when the output already exists.
# MAGIC - **Partitioned write** — **`.partitionBy(...)`** writes one **`column=value/`** folder for each value.
# MAGIC - **Delta file write** — **`format("delta").save(path)`** writes Parquet data files and a **`_delta_log/`** folder to a Volume path.
# MAGIC - **Managed `saveAsTable`** — creates a managed table whose files go to the catalog managed location.
# MAGIC - **Files vs tables** — files are read by path; tables are read by name.
# MAGIC
# MAGIC **Next:** Module 6 **01 - Column Transforms with Built-in Functions**.
# MAGIC
# MAGIC Use notebook 99 - Rideshare Project Cleanup and Reset when you need to clear the practice/ folder or reset the project.