# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Introductory MERGE
# MAGIC
# MAGIC Matched update and not-matched insert — not production incremental `MERGE`.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Apply an introductory `MERGE` (matched update and not-matched insert)
# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Drop `rideshare_dev.processed.fare_maint_lab` and delete leftover files at
# MAGIC `{url}/external-tables/fare_maint_lab`. `DROP TABLE` does not delete those
# MAGIC files.
# MAGIC
# MAGIC Handmade extract columns (no `driver_payout_amount`):
# MAGIC `trip_id`, `service_type`, `payment_method`, `base_fare_amount`,
# MAGIC `tip_amount`. Deletion vectors off on first write.

# COMMAND ----------

# lab_table = "rideshare_dev.processed.fare_maint_lab"
# lab_path = "{url}/external-tables/fare_maint_lab"  # DESCRIBE EXTERNAL LOCATION el_rideshare_dev
# TODO: DROP TABLE IF EXISTS lab_table
# TODO: dbutils.fs.rm(lab_path, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0 — Create the table and insert three rows
# MAGIC
# MAGIC `CREATE` + `INSERT` **1001–1003** only (**3** rows; 1003 tip **6.00**;
# MAGIC **1004** absent).

# COMMAND ----------

# TODO: CREATE TABLE with extract columns; delta.enableDeletionVectors = false
# TODO: INSERT 1001–1003 only (1003 tip 6.00; 1004 absent)
# TODO: SELECT — expect 3 rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — MERGE matched update and not-matched insert
# MAGIC
# MAGIC `MERGE` from a source with 1003 tip **10.00** and extract row **1004**:
# MAGIC `WHEN MATCHED` update tip; `WHEN NOT MATCHED` insert.

# COMMAND ----------

# TODO: source with 1003 tip 10.00 and extract row 1004
# TODO: MERGE WHEN MATCHED update tip; WHEN NOT MATCHED insert

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Confirm four rows
# MAGIC
# MAGIC `SELECT` **4** rows; 1003 is **10.00**; 1004 present.

# COMMAND ----------

# TODO: SELECT — 4 rows; 1003 tip is 10.00; 1004 present

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC `MERGE` **1001** **3.00 → 4.00**. Still **4** rows; 1003 stays **10.00**.

# COMMAND ----------

# TODO: MERGE 1001 tip 3.00 → 4.00
# TODO: SELECT — 4 rows; 1001 tip is 4.00; 1003 stays 10.00

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC TODO: recap MERGE matched update and not-matched insert.
# MAGIC
# MAGIC **Next:** `04 - ACID and Optimistic Concurrency`.
