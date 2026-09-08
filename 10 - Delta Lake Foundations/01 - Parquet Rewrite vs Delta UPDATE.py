# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Parquet Rewrite vs Delta UPDATE
# MAGIC
# MAGIC Isolated folders: why a one-row fare correction is a full Parquet rewrite,
# MAGIC then the same correction as a Delta `UPDATE`.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Show why a one-row fare correction is a full Parquet rewrite
# MAGIC - Apply the same correction as a Delta `UPDATE` that records the change in
# MAGIC   `_delta_log`
# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Handmade dataset, reset two folders so the notebook can re-run.

# COMMAND ----------

from decimal import Decimal

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
)

parquet_path = (
    "/Volumes/rideshare_dev/processed/output_files/practice/"
    "fare_correction_parquet/"
)
delta_path = (
    "/Volumes/rideshare_dev/processed/output_files/practice/"
    "fare_correction_delta/"
)

extract_schema = StructType(
    [
        StructField("trip_id", LongType(), False),
        StructField("service_type", StringType(), False),
        StructField("payment_method", StringType(), False),
        StructField("base_fare_amount", DecimalType(10, 2), False),
        StructField("tip_amount", DecimalType(10, 2), False),
    ]
)

trips_extract = spark.createDataFrame(
    [
        (1001, "STANDARD", "card", Decimal("20.00"), Decimal("3.00")),
        (1002, "SHARED", "cash", Decimal("15.00"), Decimal("0.00")),
        (1003, "PREMIUM", "card", Decimal("40.00"), Decimal("6.00")),
        (1004, "STANDARD", "wallet", Decimal("25.00"), Decimal("2.50")),
    ],
    schema=extract_schema,
)

dbutils.fs.rm(parquet_path, True)
dbutils.fs.rm(delta_path, True)

print(f"parquet_path = {parquet_path}")
print(f"delta_path = {delta_path}")
print("rows in extract =", trips_extract.count())
display(trips_extract.orderBy("trip_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write four rows as Parquet
# MAGIC
# MAGIC Original extract: **1003** tip is **6.00**. After write: **4** rows, part
# MAGIC files, **no** `_delta_log`. Ignore `.crc` files in listings.

# COMMAND ----------

(
    trips_extract.write.format("parquet")
    .mode("overwrite")
    .save(parquet_path)
)

print("Parquet folder listing:")
display(dbutils.fs.ls(parquet_path))

parquet_trips = spark.read.format("parquet").load(parquet_path)
print(f"parquet rows = {parquet_trips.count()} (expect 4)")
display(parquet_trips.orderBy("trip_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC You should see Parquet part files and **no** `_delta_log/` folder.
# MAGIC Trip **1003** still has tip **6.00**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business requirement — correct trip 1003
# MAGIC
# MAGIC Operations needs trip **1003**'s tip changed from **6.00** to **10.00**.
# MAGIC Keep all **4** rows. No inserts. No deletes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parquet update — read, modify, overwrite
# MAGIC
# MAGIC Parquet has no row `UPDATE`. The pattern is: read the folder, change the
# MAGIC column with `when` / `otherwise`, overwrite the same path.

# COMMAND ----------

parquet_corrected = parquet_trips.withColumn(
    "tip_amount",
    F.when(
        F.col("trip_id") == 1003,
        F.lit("10.00").cast("decimal(10,2)"),
    ).otherwise(F.col("tip_amount")),
)

(
    parquet_corrected.write.format("parquet")
    .mode("overwrite")
    .save(parquet_path)
)

parquet_after = spark.read.format("parquet").load(parquet_path)
print(f"parquet rows after overwrite = {parquet_after.count()} (expect 4)")
display(parquet_after.orderBy("trip_id"))

print("Parquet folder after overwrite (still no _delta_log):")
display(dbutils.fs.ls(parquet_path))

# COMMAND ----------

# MAGIC %md
# MAGIC **4** rows remain. Trip **1003** is **10.00**. Spark rewrote Parquet files
# MAGIC at the folder. There is still no `_delta_log`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parquet limitations
# MAGIC
# MAGIC Two limits show up in production:
# MAGIC
# MAGIC - There is **no** transactional `UPDATE` on a Parquet folder. The next
# MAGIC   cell is expected to fail.
# MAGIC - In production, a failed or overlapping overwrite can leave a bad folder (mixed old and new part files) with no log to roll back.

# COMMAND ----------

spark.sql(
    f"""
    UPDATE parquet.`{parquet_path}`
    SET tip_amount = 10.00
    WHERE trip_id = 1003
    """
)  # Expected: AnalysisException

# COMMAND ----------

# MAGIC %md
# MAGIC ## Same data as Delta
# MAGIC
# MAGIC Write the **original** four rows (tip **6.00**, not the Parquet overwrite)
# MAGIC with `format("delta")`. Deletion vectors **off**. `ls`: data files plus
# MAGIC `_delta_log/`.

# COMMAND ----------

(
    trips_extract.write.format("delta")
    .mode("overwrite")
    .option("delta.enableDeletionVectors", "false")
    .save(delta_path)
)

print("Delta folder listing:")
display(dbutils.fs.ls(delta_path))

print("_delta_log listing:")
display(dbutils.fs.ls(f"{delta_path}_delta_log"))

delta_trips = spark.read.format("delta").load(delta_path)
print(f"delta rows = {delta_trips.count()} (expect 4)")
display(delta_trips.orderBy("trip_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC You should see Parquet data files **and** `_delta_log/`. Trip **1003** is
# MAGIC back to tip **6.00** — this folder started from the original extract.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta `UPDATE`
# MAGIC
# MAGIC Correct trip **1003** with `UPDATE` on `` delta.`<path>` ``. No
# MAGIC `saveAsTable`.
# MAGIC
# MAGIC Delta does not modify an existing Parquet file directly. Instead, it creates a new Parquet file that contains the updated rows. The changes are recorded in the `_delta_log`, which marks the new file as part of the current table state. While the old file may still be stored and show up when listing the contents, Delta no longer uses it for the current version.

# COMMAND ----------

spark.sql(
    f"""
    UPDATE delta.`{delta_path}`
    SET tip_amount = 10.00
    WHERE trip_id = 1003
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify
# MAGIC
# MAGIC **4** rows; **1003** is **10.00**.

# COMMAND ----------

delta_after = spark.read.format("delta").load(delta_path)
print(f"delta rows after UPDATE = {delta_after.count()} (expect 4)")
display(delta_after.orderBy("trip_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Glance at files
# MAGIC
# MAGIC After an UPDATE, the `ls` command may display multiple Parquet data files and several JSON commit files in the `_delta_log` directory. The data continues to be stored in Parquet format, but Delta uses the transaction log to identify which data files are part of the current table version, rather than reading every Parquet file in the folder. Older files may still be stored even if they are no longer part of the current table state.

# COMMAND ----------

print("Delta data files after UPDATE (leftover files may remain):")
display(dbutils.fs.ls(delta_path))

print("_delta_log after UPDATE:")
display(dbutils.fs.ls(f"{delta_path}_delta_log"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volume folders vs managed tables
# MAGIC
# MAGIC This lab uses Volume folders, not managed tables, so you can `ls` the files and see the proof. Managed-table files live in catalog storage (`abfss://`), which is harder to browse here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC Finance also needs trip **1001**'s tip changed from **3.00** to **4.00**
# MAGIC on the **Delta** folder you just updated.
# MAGIC
# MAGIC - Use `UPDATE` on `` delta.`<path>` `` (same path as the worked example)
# MAGIC - Do not rewrite Parquet
# MAGIC - Do not touch `fare_log_delta/`
# MAGIC
# MAGIC **Expected:** still **4** rows; trip **1001** tip is **4.00**; trip
# MAGIC **1003** stays **10.00**.

# COMMAND ----------

# Your code here.

# COMMAND ----------

# MAGIC %md
# MAGIC **Hint:** Copy the worked `UPDATE` cell. Change `trip_id` to **1001** and
# MAGIC `tip_amount` to **4.00**. Then `spark.read.format("delta").load(...)`
# MAGIC and confirm `.count()` is **4**.

# COMMAND ----------

# MAGIC %md
# MAGIC **Solution** (commented out — un-comment if you want to compare)

# COMMAND ----------

# spark.sql(
#     f"""
#     UPDATE delta.`{delta_path}`
#     SET tip_amount = 4.00
#     WHERE trip_id = 1001
#     """
# )
# exercise_check = spark.read.format("delta").load(delta_path)
# print(f"rows = {exercise_check.count()} (expect 4)")
# display(exercise_check.orderBy("trip_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - Parquet row fixes are **read → `when` → overwrite**. There is no
# MAGIC   transactional `UPDATE`, and a failed overwrite can leave a bad folder
# MAGIC - A Delta UPDATE does not modify the existing Parquet file in place. It writes a new Parquet file, and _delta_log records which files belong to the current table state. This is handled transactionally.
# MAGIC - Plain Parquet has no UPDATE. You must rewrite or overwrite the data yourself.
# MAGIC - Volume folders make `ls` easy; managed tables such as `trip_enriched`
# MAGIC   are also Delta under `abfss://`
# MAGIC - How to make a one-row change without rewriting the whole data file is Module 11.
# MAGIC
# MAGIC **Next:** `02 - Understanding the Delta Transaction Log` walks
# MAGIC `_delta_log` commit by commit.