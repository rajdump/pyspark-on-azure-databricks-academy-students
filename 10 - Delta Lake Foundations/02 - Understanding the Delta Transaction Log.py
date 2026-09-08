# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Understanding the Delta Transaction Log
# MAGIC
# MAGIC Walk `_delta_log` commit by commit on a Volume folder — no catalog table.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Walk `_delta_log` commit by commit (`protocol` / `metaData` / `commitInfo` /
# MAGIC   `add` / `remove`), reconstruct the current snapshot, and read `DESCRIBE
# MAGIC   HISTORY`
# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Handmade extract, reset `fare_log_delta/` so the notebook can re-run.

# COMMAND ----------

import json
from decimal import Decimal

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
)

delta_path = (
    "/Volumes/rideshare_dev/processed/output_files/practice/"
    "fare_log_delta/"
)
log_path = f"{delta_path}_delta_log"

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

dbutils.fs.rm(delta_path, True)

print(f"delta_path = {delta_path}")
print("rows in extract =", trips_extract.count())
display(trips_extract.orderBy("trip_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Version 0 — Empty folder
# MAGIC
# MAGIC Write `_delta_log` from `extract_schema`. Do not write `trips_extract`
# MAGIC yet. Deletion vectors **off**.
# MAGIC
# MAGIC **0** rows. Typically no data `.parquet`.

# COMMAND ----------

(
    spark.createDataFrame([], extract_schema).write.format("delta")
    .mode("overwrite")
    .option("delta.enableDeletionVectors", "false")
    .save(delta_path)
)

print("Delta folder listing:")
display(dbutils.fs.ls(delta_path))

print("_delta_log listing:")
display(dbutils.fs.ls(log_path))

delta_now = spark.read.format("delta").load(delta_path)
print(f"delta rows = {delta_now.count()} (expect 0)")

# COMMAND ----------

# MAGIC %md
# MAGIC The first commit is `00000000000000000000.json`.

# COMMAND ----------

v0_log = spark.read.json(f"{log_path}/00000000000000000000.json")
print("v0 columns:", v0_log.columns)
display(v0_log)

# COMMAND ----------

# MAGIC %md
# MAGIC Find `commitInfo`, `metaData`, and `protocol`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Version 1 — `add` trips 1001–1003
# MAGIC
# MAGIC Append **1001–1003** from `trips_extract` (1003 tip still **6.00**).
# MAGIC Leave **1004** for the next commit. Delta read: **3** rows.

# COMMAND ----------

trips_1001_to_1003 = trips_extract.filter(
    F.col("trip_id").isin(1001, 1002, 1003)
)

(
    trips_1001_to_1003.write.format("delta")
    .mode("append")
    .save(delta_path)
)

delta_now = spark.read.format("delta").load(delta_path)
print(f"delta rows = {delta_now.count()} (expect 3)")
display(delta_now.orderBy("trip_id"))

print("_delta_log after v1:")
display(dbutils.fs.ls(log_path))

# COMMAND ----------

v1_log = spark.read.json(f"{log_path}/00000000000000000001.json")
print("v1 columns:", v1_log.columns)
display(v1_log)

# COMMAND ----------

# MAGIC %md
# MAGIC Look for `add`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Version 2 — `add` trip 1004
# MAGIC
# MAGIC Append **1004** from `trips_extract`. Delta read: **4** rows.

# COMMAND ----------

trip_1004 = trips_extract.filter(F.col("trip_id") == 1004)

(
    trip_1004.write.format("delta")
    .mode("append")
    .save(delta_path)
)

delta_now = spark.read.format("delta").load(delta_path)
print(f"delta rows = {delta_now.count()} (expect 4)")
display(delta_now.orderBy("trip_id"))

print("_delta_log after v2:")
display(dbutils.fs.ls(log_path))

# COMMAND ----------

v2_log = spark.read.json(f"{log_path}/00000000000000000002.json")
print("v2 columns:", v2_log.columns)
display(v2_log)

# COMMAND ----------

# MAGIC %md
# MAGIC Look for a second `add`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Version 3 — `UPDATE` trip 1003 (`remove` + `add`)
# MAGIC
# MAGIC Operations needs trip **1003**'s tip changed from **6.00** to **10.00**.
# MAGIC Keep **4** rows. Delta `remove`s the old file and `add`s a new one. The
# MAGIC old file may stay on disk.

# COMMAND ----------

spark.sql(
    f"""
    UPDATE delta.`{delta_path}`
    SET tip_amount = 10.00
    WHERE trip_id = 1003
    """
)

delta_now = spark.read.format("delta").load(delta_path)
print(f"delta rows = {delta_now.count()} (expect 4)")
display(delta_now.orderBy("trip_id"))

print("Delta data files after UPDATE (leftover files may remain):")
display(dbutils.fs.ls(delta_path))

print("_delta_log after v3:")
display(dbutils.fs.ls(log_path))

# COMMAND ----------

v3_log = spark.read.json(f"{log_path}/00000000000000000003.json")
print("v3 columns:", v3_log.columns)
display(v3_log)

# COMMAND ----------

# MAGIC %md
# MAGIC Look for `remove` and `add`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Version 4 — `DELETE` trip 1002 (`remove` + `add`)
# MAGIC
# MAGIC Remove trip **1002** the same way. Delta read: **3** rows.

# COMMAND ----------

spark.sql(
    f"""
    DELETE FROM delta.`{delta_path}`
    WHERE trip_id = 1002
    """
)

delta_now = spark.read.format("delta").load(delta_path)
print(f"delta rows = {delta_now.count()} (expect 3)")
display(delta_now.orderBy("trip_id"))

print("Delta data files after DELETE (leftover files may remain):")
display(dbutils.fs.ls(delta_path))

print("_delta_log after v4:")
display(dbutils.fs.ls(log_path))

# COMMAND ----------

v4_log = spark.read.json(f"{log_path}/00000000000000000004.json")
print("v4 columns:", v4_log.columns)
display(v4_log)

# COMMAND ----------

# MAGIC %md
# MAGIC Look for `remove` and `add`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Replay `add` / `remove` vs `ls`
# MAGIC
# MAGIC A Delta table snapshot contains the data files that are currently active.
# MAGIC
# MAGIC * `add` — makes a data file part of the table.
# MAGIC * `remove` — removes a data file from the current table state.
# MAGIC * `ls` — shows the physical files that still exist in storage, including files Delta no longer uses.
# MAGIC
# MAGIC > **Warning:** Reading all Parquet files directly from the folder is not the same as reading the Delta table.
# MAGIC

# COMMAND ----------

snapshot_files = set()
commit_files = sorted(
    file_info.path
    for file_info in dbutils.fs.ls(log_path)
    if file_info.path.endswith(".json")
)

for commit_file in commit_files:
    file_name = commit_file.rsplit("/", 1)[-1]
    version = int(file_name.replace(".json", ""))
    actions = []
    for line in dbutils.fs.head(commit_file).splitlines():
        if not line.strip():
            continue
        action = json.loads(line)
        if "add" in action:
            snapshot_files.add(action["add"]["path"])
            actions.append("add")
        elif "remove" in action:
            snapshot_files.discard(action["remove"]["path"])
            actions.append("remove")
        elif "protocol" in action:
            actions.append("protocol")
        elif "metaData" in action:
            actions.append("metaData")
        elif "commitInfo" in action:
            actions.append("commitInfo")
    print(f"v{version} actions: {actions}")

print("current snapshot files from add/remove:")
for name in sorted(snapshot_files):
    print(f"  {name}")
print(f"snapshot file count = {len(snapshot_files)}")

parquet_on_disk = [
    file_info.name
    for file_info in dbutils.fs.ls(delta_path)
    if file_info.name.endswith(".parquet")
]
print(f"parquet files on disk = {len(parquet_on_disk)}")
print("ls of the folder (ignore .crc and _delta_log):")
display(dbutils.fs.ls(delta_path))

# COMMAND ----------

# MAGIC %md
# MAGIC Snapshot file count can be **smaller** than `.parquet` files on disk. A
# MAGIC Delta read still returns **3** rows.

# COMMAND ----------

# MAGIC %md
# MAGIC ## `DESCRIBE HISTORY`
# MAGIC
# MAGIC Stop here. Do not query a past version.

# COMMAND ----------

history = spark.sql(f"DESCRIBE HISTORY delta.`{delta_path}`")
display(history)

# COMMAND ----------

# MAGIC %md
# MAGIC You should see versions **0**–**4**. Time travel is notebook 04.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - Version **0** is empty: schema in the log, **0** rows
# MAGIC - Writes `add` a file; `UPDATE` and `DELETE` `remove` one and `add`
# MAGIC   another. Leftover files may stay on disk
# MAGIC - The snapshot is `add` minus `remove`; `ls` is not the snapshot
# MAGIC - `DESCRIBE HISTORY` lists the commits; time travel is next
# MAGIC
# MAGIC **Next:** `03 - Managed vs External Delta Tables`