# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - ACID and Optimistic Concurrency
# MAGIC
# MAGIC ACID and optimistic concurrency, including a write conflict and retry.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Explain ACID and optimistic concurrency, including a write conflict and retry
# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Drop `rideshare_dev.processed.fare_maint_lab` and delete leftover files at
# MAGIC `{url}/external-tables/fare_maint_lab`. `DROP TABLE` does not delete those
# MAGIC files.

# COMMAND ----------

import threading

from delta.exceptions import (
    ConcurrentAppendException,
    ConcurrentDeleteReadException,
    DeltaConcurrentModificationException,
)

lab_table = "rideshare_dev.processed.fare_maint_lab"

external_location_url = (
    spark.sql("DESCRIBE EXTERNAL LOCATION el_rideshare_dev")
    .select("url")
    .first()["url"]
    .rstrip("/")
)
lab_path = f"{external_location_url}/external-tables/fare_maint_lab"

spark.sql(f"DROP TABLE IF EXISTS {lab_table}")
dbutils.fs.rm(lab_path, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0 — Create the table and insert four rows
# MAGIC
# MAGIC Create the Delta table with **deletion vectors disabled** and insert the
# MAGIC four extract rows. This is the starting snapshot: **1001** card **3.00**,
# MAGIC **1002** cash **0.00**, **1003** PREMIUM **6.00**, **1004** wallet **2.50**.

# COMMAND ----------

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
    LOCATION '{lab_path}'
    TBLPROPERTIES (
      'delta.enableDeletionVectors' = 'false'
    )
    """
)
spark.sql(
    f"""
    INSERT INTO {lab_table} VALUES
      (1001, 'STANDARD', 'card', 20.00, 3.00),
      (1002, 'SHARED', 'cash', 15.00, 0.00),
      (1003, 'PREMIUM', 'card', 40.00, 6.00),
      (1004, 'STANDARD', 'wallet', 25.00, 2.50)
    """
)
display(spark.sql(f"SELECT * FROM {lab_table} ORDER BY trip_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — UPDATE and DESCRIBE HISTORY
# MAGIC
# MAGIC One job corrects the PREMIUM trip: tip **6.00 → 10.00**.
# MAGIC
# MAGIC - **Atomic** — 1003 is fully **10.00**, not a half write.
# MAGIC - **Consistent** — `SELECT` still has **4** rows and the extract columns.
# MAGIC - **Durable** — the next cell's `DESCRIBE HISTORY` is that commit in the
# MAGIC   transaction log. Module 10 used this command; here it is this fare
# MAGIC   correction as its own version.

# COMMAND ----------

spark.sql(
    f"""
    UPDATE {lab_table}
    SET tip_amount = 10.00
    WHERE trip_id = 1003
    """
)
display(spark.sql(f"SELECT * FROM {lab_table} ORDER BY trip_id"))

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {lab_table}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Optimistic concurrency
# MAGIC
# MAGIC Two jobs might correct a card tip and a wallet tip on **this same table**
# MAGIC at the same time.
# MAGIC
# MAGIC **Optimistic concurrency** is how Delta handles that on `fare_maint_lab`:
# MAGIC
# MAGIC 1. A reader or writer sees a **snapshot** at a table version.
# MAGIC 2. A writer prepares new data files for its change.
# MAGIC 3. At commit, Delta **validates** that the files this writer used have
# MAGIC    not changed since that snapshot.
# MAGIC 4. If another writer already rewrote those files, the commit fails with
# MAGIC    a **concurrent-modification** error. The failed write did not commit,
# MAGIC    so the writer **retries** against the latest snapshot.
# MAGIC
# MAGIC Two writers that touch the **same files** conflict, even when they
# MAGIC update different rows. Deletion vectors are **off**, so an `UPDATE`
# MAGIC rewrites the Parquet file that holds the row.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Overlapping-write conflict and retry
# MAGIC
# MAGIC Two jobs overlap: trip **1001** **3.00 → 4.00** and trip **1004**
# MAGIC **2.50 → 3.50**. They change different rows, but both rewrite the same
# MAGIC files, so one writer loses with a concurrent-modification error
# MAGIC (`DeltaConcurrentModificationException`, often `ConcurrentAppendException`
# MAGIC or `ConcurrentDeleteReadException`).
# MAGIC
# MAGIC - **Isolated** — overlapping writers on the same files; one loses.
# MAGIC - **Atomic** — the loser does not commit (that tip stays old until retry).
# MAGIC - **Consistent** — `SELECT` still has four trips, not a mixed broken row.
# MAGIC
# MAGIC The next cell runs those two `UPDATE`s as two threads so they can overlap.
# MAGIC
# MAGIC > **Note:** If both writes commit, the next cell fails. Re-run it so the
# MAGIC > two `UPDATE`s overlap.

# COMMAND ----------

conflict_errors = []
committed = []
results_lock = threading.Lock()
start_together = threading.Barrier(2)
conflict_types = (
    ConcurrentAppendException,
    ConcurrentDeleteReadException,
    DeltaConcurrentModificationException,
)


def update_tip(trip_id, tip_amount):
    start_together.wait()
    try:
        spark.sql(
            f"""
            UPDATE {lab_table}
            SET tip_amount = {tip_amount}
            WHERE trip_id = {trip_id}
            """
        )  # Expected: DeltaConcurrentModificationException
    except conflict_types as exc:
        with results_lock:
            conflict_errors.append((trip_id, tip_amount, type(exc).__name__))
        print(f"trip_id {trip_id} did not commit: {type(exc).__name__}")
    else:
        with results_lock:
            committed.append(trip_id)
        print(f"trip_id {trip_id} committed tip_amount = {tip_amount}")


writers = [
    threading.Thread(target=update_tip, args=(1001, "4.00")),
    threading.Thread(target=update_tip, args=(1004, "3.50")),
]
for writer in writers:
    writer.start()
for writer in writers:
    writer.join()

if not conflict_errors or len(conflict_errors) + len(committed) != 2:
    raise RuntimeError(  # Expected: RuntimeError
        "Expected one UPDATE to lose with a concurrent-modification "
        "error. Re-run this cell so the two writes overlap."
    )

print("conflict_errors =", conflict_errors)
display(spark.sql(f"SELECT * FROM {lab_table} ORDER BY trip_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC Retry the write that lost. That retry is a new **atomic** commit.
# MAGIC **Consistent:** still **4** rows — **1001** tip **4.00**, **1002** tip
# MAGIC **0.00**, **1003** tip **10.00**, **1004** tip **3.50**.

# COMMAND ----------

for trip_id, tip_amount, error_name in conflict_errors:
    print(f"Retrying trip_id {trip_id} after {error_name}")
    spark.sql(
        f"""
        UPDATE {lab_table}
        SET tip_amount = {tip_amount}
        WHERE trip_id = {trip_id}
        """
    )

display(spark.sql(f"SELECT * FROM {lab_table} ORDER BY trip_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Table properties glance
# MAGIC
# MAGIC `SHOW TBLPROPERTIES` confirms `delta.enableDeletionVectors` is **false**.
# MAGIC
# MAGIC > **Note:** Deletion vectors can allow **row-level** concurrency when two
# MAGIC > writers update different rows in the same file. This notebook does not
# MAGIC > lab that behavior.

# COMMAND ----------

display(
    spark.sql(
        f"SHOW TBLPROPERTIES {lab_table} ('delta.enableDeletionVectors')"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC On these four trips:
# MAGIC
# MAGIC - **Atomic** — the 1003 `UPDATE` fully committed; the overlapping loser
# MAGIC   did not, then retried as a new full write.
# MAGIC - **Consistent** — after each successful commit, `SELECT` still has
# MAGIC   **4** extract rows.
# MAGIC - **Isolated** — two overlapping `UPDATE`s on 1001 and 1004; one lost.
# MAGIC - **Durable** — `DESCRIBE HISTORY` kept the 1003 commit as a new version.
# MAGIC
# MAGIC Deletion vectors are off here. They can allow row-level concurrency
# MAGIC for non-overlapping rows — that is not part of this lab.
# MAGIC
# MAGIC **Next:** Module 12 — Unity Catalog and Data Governance.
