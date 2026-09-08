# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Delta Time Travel and Restore
# MAGIC
# MAGIC Self-contained managed table: query a past snapshot and `RESTORE`.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Query a past snapshot (`VERSION AS OF`, one PySpark `versionAsOf` read) and
# MAGIC   `RESTORE` it
# MAGIC - Recognize `TIMESTAMP AS OF` syntax; explain that historical access is bounded
# MAGIC   by retention and that `VACUUM` removes eligible files
# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — create the lab table
# MAGIC We use a managed Delta table to keep the lesson focused on table versions, time travel, and RESTORE.
# MAGIC
# MAGIC In `02 - Understanding the Delta Transaction Log`, you learned how Delta
# MAGIC records table changes and creates new versions. This notebook builds on
# MAGIC that foundation to explore how those versions can be queried and restored.

# COMMAND ----------

from decimal import Decimal

lab_table = "rideshare_dev.processed.fare_timetravel_lab"

# Small source dataset used to create a controlled Delta history
trips_extract = spark.createDataFrame(
    [
        (1001, "STANDARD", "card", Decimal("20.00"), Decimal("3.00")),
        (1002, "SHARED", "cash", Decimal("15.00"), Decimal("0.00")),
        (1003, "PREMIUM", "card", Decimal("40.00"), Decimal("6.00")),
        (1004, "STANDARD", "wallet", Decimal("25.00"), Decimal("2.50")),
    ],
    "trip_id LONG, service_type STRING, payment_method STRING, "
    "base_fare_amount DECIMAL(10, 2), tip_amount DECIMAL(10, 2)",
)
# SQL statements below read from this temporary view
trips_extract.createOrReplaceTempView("trips_extract")

# Recreate the lab table so every run starts with a clean history
spark.sql(f"DROP TABLE IF EXISTS {lab_table}")
spark.sql(
    f"""
    CREATE TABLE {lab_table} (
      trip_id BIGINT,
      service_type STRING,
      payment_method STRING,
      base_fare_amount DECIMAL(10, 2),
      tip_amount DECIMAL(10, 2)
    )
    USING DELTA
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build table history
# MAGIC We will intentionally create five table versions to build a history that we can explore.

# COMMAND ----------

# First data version: trips 1001–1003
spark.sql(
    f"""
    INSERT INTO {lab_table}
    SELECT * FROM trips_extract
    WHERE trip_id <= 1003
    """
)
display(spark.table(lab_table).orderBy("trip_id"))

# COMMAND ----------

# Add trip 1004 — table state immediately before the update
spark.sql(
    f"""
    INSERT INTO {lab_table}
    SELECT * FROM trips_extract
    WHERE trip_id = 1004
    """
)
display(spark.table(lab_table).orderBy("trip_id"))

# COMMAND ----------

# Correct the tip for trip 1003
spark.sql(
    f"""
    UPDATE {lab_table}
    SET tip_amount = 10.00
    WHERE trip_id = 1003
    """
)
display(spark.table(lab_table).orderBy("trip_id"))

# COMMAND ----------

# Simulate an accidental delete of trip 1002
spark.sql(
    f"""
    DELETE FROM {lab_table}
    WHERE trip_id = 1002
    """
)
display(spark.table(lab_table).orderBy("trip_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explore history with `DESCRIBE HISTORY`
# MAGIC
# MAGIC `DESCRIBE HISTORY` lets you inspect the versions recorded for a Delta table.
# MAGIC
# MAGIC For this lesson, focus on:
# MAGIC
# MAGIC * **`version`** — identifies the table version
# MAGIC * **`timestamp`** — when the version was created
# MAGIC * **`operation`** — the type of operation that created the version
# MAGIC
# MAGIC Ignore the other columns for now.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT version, timestamp, operation FROM (DESCRIBE HISTORY rideshare_dev.processed.fare_timetravel_lab)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time travel by version
# MAGIC
# MAGIC Time travel lets you **read** an earlier version of a Delta table without changing its current state.
# MAGIC

# COMMAND ----------

# Table as it looked at version 2 (before the tip update)
before_update = spark.sql(
    f"""
    SELECT *
    FROM {lab_table}
    VERSION AS OF 2
    """
)
display(before_update.orderBy("trip_id"))

# Current table — time travel does not change it
current = spark.table(lab_table)
display(current.orderBy("trip_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time travel by timestamp
# MAGIC Sometimes you don't know the Delta version number. Instead, you know
# MAGIC approximately when the table was last correct, such as before a bad load.
# MAGIC
# MAGIC ```sql
# MAGIC SELECT *
# MAGIC FROM rideshare_dev.processed.fare_timetravel_lab
# MAGIC TIMESTAMP AS OF '<timestamp>'
# MAGIC ```
# MAGIC
# MAGIC Delta reads the table version at or before that time. Time travel does
# MAGIC not change the current table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time travel with PySpark
# MAGIC
# MAGIC Delta time travel is also available through the DataFrame reader. Use the `versionAsOf` option to load a specific historical version of the table.
# MAGIC

# COMMAND ----------

# Same version 2, through the DataFrame reader
historical_df = spark.read.option("versionAsOf", 2).table(lab_table)
display(historical_df.orderBy("trip_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Restore an earlier state
# MAGIC Time travel and `RESTORE` both use Delta history, but they do different
# MAGIC jobs.
# MAGIC
# MAGIC > **Note:** `RESTORE` creates a new table version that matches the version you choose. The older versions remain in the table history.

# COMMAND ----------

# Recover version 3: after the UPDATE, before the DELETE
spark.sql(
    f"""
    RESTORE TABLE {lab_table}
    TO VERSION AS OF 3
    """
)
# 1002 is back; the corrected tip remains
display(spark.table(lab_table).orderBy("trip_id"))

# HISTORY lists RESTORE as a new version after DELETE
display(spark.sql(f"DESCRIBE HISTORY {lab_table}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Restore by timestamp
# MAGIC
# MAGIC `RESTORE` can also use a timestamp instead of a version number.
# MAGIC
# MAGIC ```sql
# MAGIC RESTORE TABLE ... TO TIMESTAMP AS OF '<timestamp>'
# MAGIC ```
# MAGIC
# MAGIC Databricks finds the table state at that time and creates a new version with that state as the current table.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retention: how far back can you go?
# MAGIC
# MAGIC Time travel and `RESTORE` work only while Delta retains the information needed to access an earlier version:
# MAGIC
# MAGIC - **Transaction history** — identifies the table version.
# MAGIC - **Data files** — contain the data needed to read that version.
# MAGIC
# MAGIC ```text
# MAGIC Historical version
# MAGIC        │
# MAGIC        ├── Transaction history      default: 30 days
# MAGIC        │
# MAGIC        └── Required data files      VACUUM retention: 7 days
# MAGIC ```
# MAGIC
# MAGIC Delta keeps table history for 30 days by default.
# MAGIC
# MAGIC Data files that are no longer needed by the current table become eligible for removal after 7 days by default. They remain in storage until VACUUM runs and deletes them.
# MAGIC
# MAGIC After the required data files are removed, `VERSION AS OF` may no longer be able to read that historical version, even if `DESCRIBE HISTORY` still lists it.
# MAGIC
# MAGIC > **Warning:** Do not run `VACUUM` in this notebook

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exercise
# MAGIC
# MAGIC The table has already been restored, so trip **1002** is back in the current table.
# MAGIC
# MAGIC Use PySpark `versionAsOf` to read version **4**, where trip **1002** was deleted.
# MAGIC
# MAGIC Then compare version **4** with the current table.
# MAGIC
# MAGIC **Expected:**
# MAGIC
# MAGIC - Version **4** → **3 rows**
# MAGIC - Current table → **4 rows**

# COMMAND ----------

# Your code here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - Every committed change creates a new version of the Delta table.
# MAGIC - You can use `DESCRIBE HISTORY` to inspect the version history of the table.
# MAGIC - Time travel allows you to read a previous state of the table by version or timestamp, without altering the current table.
# MAGIC - The `RESTORE` command creates a new version that makes an earlier state of the table current again.
# MAGIC - Access to historical versions relies on the availability of the necessary transaction history and data files.
# MAGIC
# MAGIC **Next:** Module 11 `00 - Copy Fare DV Lab File`, then
# MAGIC `01 - Deletion Vectors, REORG TABLE, and VACUUM` — cleanup of obsolete
# MAGIC data and its effect on historical access..