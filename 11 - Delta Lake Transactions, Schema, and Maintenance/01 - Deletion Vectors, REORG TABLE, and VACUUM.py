# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Deletion Vectors, REORG TABLE, and VACUUM
# MAGIC
# MAGIC First cleanup of obsolete physical data on **00**'s large external table,
# MAGIC after Module 10's retention/`VACUUM` warning.
# MAGIC
# MAGIC **00**'s folder `{url}/external-tables/fare_dv_lab`.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Compare `UPDATE` with and without deletion vectors
# MAGIC - Physically remove old row bytes with `REORG TABLE ... APPLY (PURGE)` and
# MAGIC   `VACUUM`
# COMMAND ----------

lab_table = "rideshare_dev.processed.fare_dv_lab"

external_location_url = (
    spark.sql("DESCRIBE EXTERNAL LOCATION el_rideshare_dev")
    .select("url")
    .first()["url"]
    .rstrip("/")
)

lab_path = f"{external_location_url}/external-tables/fare_dv_lab"

display(spark.sql(f"LIST '{lab_path}'"))

# COMMAND ----------

# DBTITLE 1,Step 0
# MAGIC %md
# MAGIC ## Step 0 — Create the table with deletion vectors OFF
# MAGIC
# MAGIC Create an external Delta table with deletion vectors and auto-compaction disabled.

# COMMAND ----------

spark.sql(
    f"""
    DROP TABLE IF EXISTS {lab_table}
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {lab_table}
    USING DELTA
    LOCATION '{lab_path}'
    """
)

spark.sql(
    f"""
    ALTER TABLE {lab_table}
    SET TBLPROPERTIES (
      'delta.enableDeletionVectors' = 'false',
      'delta.autoOptimize.autoCompact' = 'false'
    )
    """
)

display(spark.sql(f"LIST '{lab_path}'"))

# COMMAND ----------

# DBTITLE 1,Track 4 rows
# MAGIC %md
# MAGIC Pick 4 rows to track throughout the lab. Note their original `passenger_fare` values.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT row_id,passenger_fare
# MAGIC FROM rideshare_dev.processed.fare_dv_lab
# MAGIC WHERE row_id IN (210049714452023,210049714452029,210049714452046,210049714452050)
# MAGIC ORDER BY row_id

# COMMAND ----------

# DBTITLE 1,Step 1
# MAGIC %md
# MAGIC ## Step 1 — Update one row WITHOUT deletion vectors
# MAGIC
# MAGIC With DVs off, Delta rewrites the entire Parquet file just to change one row. Watch the file listing grow.
# MAGIC

# COMMAND ----------

spark.sql(
    f"""
    UPDATE {lab_table}
    SET passenger_fare = 55.00
    WHERE row_id = 210049714452023
    """
)
display(spark.sql(f"LIST '{lab_path}'"))

# COMMAND ----------

# DBTITLE 1,Check history — no DVs
# MAGIC %md
# MAGIC Check history. Notice `numCopiedRows` — Delta copied all 11 million rows just to update one.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT version, operation, operationParameters,operationMetrics
# MAGIC FROM (DESCRIBE HISTORY rideshare_dev.processed.fare_dv_lab)

# COMMAND ----------

# DBTITLE 1,Step 2
# MAGIC %md
# MAGIC ## Step 2 — Enable deletion vectors
# MAGIC
# MAGIC Turn on DVs, then update the same row. This time Delta writes a tiny file + a `.bin` marker instead of rewriting everything.

# COMMAND ----------

spark.sql(
    f"""
    ALTER TABLE {lab_table}
    SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
    """
)

# COMMAND ----------

# DBTITLE 1,Update with DVs on
# MAGIC %md
# MAGIC Update the same row again. Compare the new file sizes to Step 1 — much smaller this time.

# COMMAND ----------

spark.sql(
    f"""
    UPDATE {lab_table}
    SET passenger_fare = 60.00
    WHERE row_id = 210049714452023
    """
)
display(spark.sql(f"LIST '{lab_path}'"))

# COMMAND ----------

# DBTITLE 1,Check history — with DVs
# MAGIC %md
# MAGIC Check history. `numCopiedRows` is now **0** — no full rewrite happened.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT version, operation, operationParameters,operationMetrics
# MAGIC FROM (DESCRIBE HISTORY rideshare_dev.processed.fare_dv_lab)

# COMMAND ----------

# DBTITLE 1,Step 3
# MAGIC %md
# MAGIC ## Step 3 — Delete a row with deletion vectors
# MAGIC
# MAGIC Delete a row. Delta adds a `.bin` marker — no files rewritten, zero bytes added.

# COMMAND ----------

spark.sql(
    f"""
    DELETE FROM {lab_table}
    WHERE row_id = 210049714452046
    """
)
display(spark.sql(f"LIST '{lab_path}'"))

# COMMAND ----------

# DBTITLE 1,Check history — delete
# MAGIC %md
# MAGIC Check history. `numAddedFiles = 0`, `numAddedBytes = 0`. The row is hidden, not physically removed.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT version, operation, operationParameters,operationMetrics
# MAGIC FROM (DESCRIBE HISTORY rideshare_dev.processed.fare_dv_lab)

# COMMAND ----------

# DBTITLE 1,Step 4
# MAGIC %md
# MAGIC ## Step 4 — VACUUM
# MAGIC
# MAGIC `VACUUM` removes old unused files only. It does **not** touch deletion-vector-marked rows in live files.
# MAGIC
# MAGIC ⚠️ `RETAIN 0 HOURS` skips the safety window — never use on production tables.

# COMMAND ----------

spark.conf.set(
    "spark.databricks.delta.retentionDurationCheck.enabled", "false"
)
spark.sql(f"VACUUM {lab_table} RETAIN 0 HOURS")
display(spark.sql(f"LIST '{lab_path}'"))

# COMMAND ----------

# DBTITLE 1,Check history — vacuum
# MAGIC %md
# MAGIC Check history. VACUUM deleted 2 obsolete files (∼315 MB). Live files with DV marks are untouched.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT version, operation, operationParameters,operationMetrics
# MAGIC FROM (DESCRIBE HISTORY rideshare_dev.processed.fare_dv_lab)

# COMMAND ----------

# DBTITLE 1,Step 5
# MAGIC %md
# MAGIC ## Step 5 — Purge with REORG TABLE
# MAGIC
# MAGIC `REORG TABLE APPLY (PURGE)` rewrites live files to physically remove marked rows. Old files stick around until the next `VACUUM`.

# COMMAND ----------

spark.sql(f"REORG TABLE {lab_table} APPLY (PURGE)")
display(spark.sql(f"LIST '{lab_path}'"))

# COMMAND ----------

# DBTITLE 1,Check history — reorg
# MAGIC %md
# MAGIC Check history. REORG replaced 1 file and removed 1 deletion vector.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT version, operation, operationParameters,operationMetrics
# MAGIC FROM (DESCRIBE HISTORY rideshare_dev.processed.fare_dv_lab)

# COMMAND ----------

# DBTITLE 1,Step 6 — Second VACUUM
# MAGIC %md
# MAGIC ## Step 6 — Second VACUUM
# MAGIC
# MAGIC Run `VACUUM` again to clean up the old file that REORG replaced.

# COMMAND ----------

spark.sql(f"VACUUM {lab_table} RETAIN 0 HOURS")
display(spark.sql(f"LIST '{lab_path}'"))

# COMMAND ----------

# DBTITLE 1,Final history
# MAGIC %md
# MAGIC Final history — all operations logged end to end.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT version, operation, operationParameters,operationMetrics
# MAGIC FROM (DESCRIBE HISTORY rideshare_dev.processed.fare_dv_lab)

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Step | What happened |
# MAGIC | ---- | ------------- |
# MAGIC | 0 | Created table with deletion vectors OFF |
# MAGIC | 1 | UPDATE without DV → rewrote entire file (~315 MB copied) |
# MAGIC | 2 | UPDATE with DV → wrote only ~5 KB (tiny file + `.bin` marker) |
# MAGIC | 3 | DELETE with DV → just a `.bin` marker, no new data files |
# MAGIC | 4 | VACUUM → removed old unused files, did NOT touch live DV marks |
# MAGIC | 5 | REORG PURGE → rewrote live files, physically removed marked rows |
# MAGIC | 6 | Second VACUUM → cleaned up the file REORG replaced |
# MAGIC
# MAGIC **Deletion vectors avoid expensive rewrites. `REORG PURGE` removes marked rows from live files. `VACUUM` cleans up the leftovers.**
# MAGIC
# MAGIC **Next:** `02 - Schema Enforcement and Evolution`