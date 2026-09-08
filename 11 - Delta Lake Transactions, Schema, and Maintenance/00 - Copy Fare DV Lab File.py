# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Copy Fare DV Lab File
# MAGIC
# MAGIC Setup for notebook **01**. Open from the course Git folder.
# MAGIC
# MAGIC `data/lab/fare_dv_lab.parquet`.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Copy the lab Parquet, `LIST`, and `CONVERT TO DELTA` so notebook **01** can
# MAGIC   register an external table
# COMMAND ----------

# MAGIC %md
# MAGIC **Next:** `01 - Deletion Vectors, REORG TABLE, and VACUUM`
# COMMAND ----------

from pathlib import Path

lab_table = "rideshare_dev.processed.fare_dv_lab"
lab_path = (
    spark.sql("DESCRIBE EXTERNAL LOCATION el_rideshare_dev")
    .select("url")
    .first()["url"]
    .rstrip("/")
    + "/external-tables/fare_dv_lab"
)
dest_file = f"{lab_path}/fare_dv_lab.parquet"

spark.sql(f"DROP TABLE IF EXISTS {lab_table}")
dbutils.fs.rm(lab_path, True)

repo_root = next(
    (
        path
        for path in [Path.cwd(), *Path.cwd().parents]
        if (path / "data" / "raw").is_dir()
    ),
    None,
)
if repo_root is None:
    raise FileNotFoundError("Open this notebook from the course Git folder.")
src = repo_root / "data" / "lab" / "fare_dv_lab.parquet"
if not src.is_file():
    raise FileNotFoundError(f"Missing {src}")

dbutils.fs.mkdirs(lab_path)
dbutils.fs.cp(f"file:{src.resolve()}", dest_file)
display(spark.sql(f"LIST '{lab_path}'"))

# COMMAND ----------

# DBTITLE 1,Convert Parquet to Delta
spark.sql(f"CONVERT TO DELTA parquet.`{lab_path}`")
display(spark.sql(f"LIST '{lab_path}'"))