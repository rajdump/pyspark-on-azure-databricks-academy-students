# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Schema Enforcement and Evolution
# MAGIC
# MAGIC Enforce schema, add a column, and apply `NOT NULL` / `CHECK` on
# MAGIC `fare_maint_lab`.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Enforce a table schema, add a column, and apply `NOT NULL` / `CHECK`
# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Drop `rideshare_dev.processed.fare_maint_lab` and delete leftover files at
# MAGIC `{url}/external-tables/fare_maint_lab`. `DROP TABLE` does not delete those
# MAGIC files.
# MAGIC
# MAGIC Handmade extract columns only (no `driver_payout_amount` yet):
# MAGIC `trip_id`, `service_type`, `payment_method`, `base_fare_amount`,
# MAGIC `tip_amount`. Deletion vectors off on first write.

# COMMAND ----------

# lab_table = "rideshare_dev.processed.fare_maint_lab"
# lab_path = "{url}/external-tables/fare_maint_lab"  # DESCRIBE EXTERNAL LOCATION el_rideshare_dev
# TODO: DROP TABLE IF EXISTS lab_table
# TODO: dbutils.fs.rm(lab_path, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0 — Create the table and insert four rows
# MAGIC
# MAGIC `CREATE` extract columns only (no `driver_payout_amount`). `INSERT`
# MAGIC trips **1001–1004**. Expect **4** rows.

# COMMAND ----------

# TODO: CREATE TABLE with extract columns; delta.enableDeletionVectors = false
# TODO: INSERT 1001–1004
# TODO: SELECT — expect 4 rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Schema enforcement
# MAGIC
# MAGIC Write or append a DataFrame that includes `driver_payout_amount`.
# MAGIC Expected: fail.

# COMMAND ----------

# TODO: DataFrame with driver_payout_amount (leave payout NULL — do not invent amounts)
# TODO: write/append → expected fail

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Add a column and mergeSchema
# MAGIC
# MAGIC `ALTER TABLE ADD COLUMN driver_payout_amount DECIMAL(10, 2)`.
# MAGIC `mergeSchema` write succeeds. `SELECT` still **4** rows; payout is **NULL**.

# COMMAND ----------

# TODO: ALTER TABLE ADD COLUMN driver_payout_amount DECIMAL(10, 2)
# TODO: mergeSchema write
# TODO: SELECT — 4 rows; driver_payout_amount is NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — NOT NULL and CHECK
# MAGIC
# MAGIC `NOT NULL` on `trip_id`. `CHECK (tip_amount >= 0)`. One insert that
# MAGIC violates `CHECK` → expected fail.

# COMMAND ----------

# TODO: NOT NULL on trip_id
# TODO: CHECK (tip_amount >= 0)
# TODO: insert that violates CHECK → expected fail

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC TODO: recap schema enforcement, ADD COLUMN / mergeSchema, NOT NULL, CHECK.
# MAGIC
# MAGIC **Next:** `03 - Introductory MERGE`.
