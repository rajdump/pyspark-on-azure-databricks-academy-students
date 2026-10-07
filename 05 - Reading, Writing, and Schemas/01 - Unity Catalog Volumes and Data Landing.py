# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Unity Catalog Volumes and Data Landing
# MAGIC
# MAGIC This lab creates the storage environment used by the course.
# MAGIC
# MAGIC We set up the ADLS storage layer and create the required Unity Catalog
# MAGIC objects so the later notebooks have a consistent place to read source
# MAGIC files and write processed outputs.
# MAGIC
# MAGIC Read the **Unity Catalog objects** and **Unity Catalog Volumes and Data
# MAGIC Landing** lessons before running this lab. They explain the concepts used
# MAGIC in the steps below.
# MAGIC
# MAGIC > **Prerequisite**
# MAGIC >
# MAGIC > The Azure storage access setup must already exist before running this lab:
# MAGIC > - an Azure Access Connector with a Managed Identity
# MAGIC > - the required Azure RBAC permissions on the storage
# MAGIC > - a Unity Catalog Storage Credential that uses that identity
# MAGIC >
# MAGIC > Set the existing Storage Credential name in the `storage_credential`
# MAGIC > variable in the lab configuration.
# MAGIC
# MAGIC Before running the lab, replace the example Azure values in the
# MAGIC configuration cell with your own values.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Create the project root folder in ADLS
# MAGIC - Register the ADLS storage path as an External Location in Unity Catalog
# MAGIC - Create the Catalog, Schemas, and Volumes used by the course
# MAGIC - Copy the course sample files into the landing Volume
# MAGIC - Verify that the storage setup is ready for later lessons

# COMMAND ----------

# Lab config — overwrite with YOUR Azure values before running.
# Author defaults are examples only.

storage_account = "sadevdbxeus2"
container = "container-dev-dbx"
storage_credential = "ac_dev_dbx_eus2"
adls_folder = "rideshare"

abfss_root = (
    f"abfss://{container}@{storage_account}.dfs.core.windows.net/{adls_folder}"
)

print(f"abfss_root = {abfss_root}")
print(f"storage_credential = {storage_credential}")

# COMMAND ----------

# MAGIC %md
# MAGIC > #### 1. Create the project folder in ADLS
# MAGIC >
# MAGIC > In the Azure Portal, create the project folder inside the ADLS
# MAGIC > container from your lab configuration. This folder becomes the storage
# MAGIC > root used by the lab.
# MAGIC >
# MAGIC > ```text
# MAGIC > {container}/
# MAGIC > └── {adls_folder}/
# MAGIC > ```

# COMMAND ----------

# MAGIC %md
# MAGIC > #### 2. Create an External Location for the `rideshare` ADLS path
# MAGIC >
# MAGIC > Register the `rideshare` ADLS path with Unity Catalog as External
# MAGIC > Location `el_rideshare_dev`. Unity Catalog uses the existing Storage
# MAGIC > Credential to access this ADLS path.

# COMMAND ----------

spark.sql(f"""
CREATE EXTERNAL LOCATION IF NOT EXISTS el_rideshare_dev
    URL '{abfss_root}'
    WITH (STORAGE CREDENTIAL {storage_credential})
    COMMENT 'External location for the rideshare development project'
""")

# COMMAND ----------

# MAGIC %sql DESCRIBE EXTERNAL LOCATION el_rideshare_dev;

# COMMAND ----------

# MAGIC %md
# MAGIC > #### 3. Test the External Location
# MAGIC >
# MAGIC > Confirm that Databricks can reach the `rideshare` path before the later
# MAGIC > steps use it.
# MAGIC >
# MAGIC > 1. Open **Catalog Explorer** → **External Locations** → `el_rideshare_dev`
# MAGIC > 2. Click **Test connection**
# MAGIC > 3. Confirm that the storage-access checks succeed
# MAGIC >
# MAGIC > A **File Events Read** check may also appear. This lab does not use File
# MAGIC > Events, so that check does not need to succeed.
# MAGIC
# MAGIC <details>
# MAGIC <summary><strong>Troubleshooting: Refer Step 3 — Test the External Location in notion page Unity Catalog Volumes and Data Landing
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC > #### 4. Create the `rideshare_dev` Catalog and `landing` Schema
# MAGIC >
# MAGIC > `rideshare_dev` is the Catalog used by the course. Its managed storage
# MAGIC > location is `{abfss_root}/uc-managed`.
# MAGIC >
# MAGIC > `rideshare_dev.landing` is the Schema used for source-data objects.

# COMMAND ----------

spark.sql(f"""
CREATE CATALOG IF NOT EXISTS rideshare_dev
MANAGED LOCATION '{abfss_root}/uc-managed'
COMMENT 'Catalog for the rideshare development project'
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG rideshare_dev;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog();

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS rideshare_dev.landing
# MAGIC COMMENT 'Incoming rideshare source files';

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN rideshare_dev;

# COMMAND ----------

# MAGIC %md
# MAGIC > #### 5. Create the landing external Volume
# MAGIC >
# MAGIC > `rideshare_dev.landing.source_files` represents the `rideshare/landing`
# MAGIC > ADLS folder.
# MAGIC >
# MAGIC > Later notebooks access the source files through:
# MAGIC >
# MAGIC > `/Volumes/rideshare_dev/landing/source_files`
# MAGIC >
# MAGIC > This step also creates the dataset folders used to organize the source
# MAGIC > files under the Volume.

# COMMAND ----------

spark.sql(f"""
CREATE EXTERNAL VOLUME IF NOT EXISTS rideshare_dev.landing.source_files
LOCATION '{abfss_root}/landing'
COMMENT 'Landing volume for original rideshare source files'
""")

# COMMAND ----------

# Create one folder per dataset inside the landing volume

volume_path = "/Volumes/rideshare_dev/landing/source_files"

source_folders = [
    "trip",
    "trip_time",
    "zone_lookup",
    "payment",
    "drivers",
]

for folder in source_folders:
    dbutils.fs.mkdirs(f"{volume_path}/{folder}")

# COMMAND ----------

# Confirm the folders were created
display(dbutils.fs.ls(volume_path))

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE VOLUME rideshare_dev.landing.source_files;

# COMMAND ----------

# MAGIC %md
# MAGIC > #### 6. Copy the course source files into the landing Volume
# MAGIC >
# MAGIC > Copy the sample source files from `data/raw` into their corresponding
# MAGIC > dataset folders in the landing Volume.
# MAGIC >
# MAGIC > Run this notebook from the course **Git folder** so the `data/raw`
# MAGIC > path is available.

# COMMAND ----------

import shutil
from pathlib import Path

def find_repo_root(start: Path) -> Path:
    for path in [start, *start.parents]:
        if (path / "data" / "raw").is_dir():
            return path
    raise FileNotFoundError(
        "Could not find a folder containing data/raw. "
        "Open this notebook from the course Git folder and try again."
    )

repo_root = find_repo_root(Path.cwd())
volume_root = Path("/Volumes/rideshare_dev/landing/source_files")

file_map = {
    "data/raw/csv/trip.csv": "trip/trip.csv",
    "data/raw/csv/bad_trip_data.csv": "trip/bad_trip_data.csv",
    "data/raw/parquet/trip_time.parquet": "trip_time/trip_time.parquet",
    "data/raw/json/zone_lookup.json": "zone_lookup/zone_lookup.json",
    "data/raw/avro/payment.avro": "payment/payment.avro",
    "data/raw/csv/bad_payment_data.csv": "payment/bad_payment_data.csv",
    "data/raw/xml/drivers.xml": "drivers/drivers.xml",
}

for src_rel, dst_rel in file_map.items():
    src = repo_root / src_rel
    dst = volume_root / dst_rel
    shutil.copy2(src, dst)
    print(f"✓ {src_rel} → {dst_rel}")

print("\n--- Verification ---")
for dst_rel in file_map.values():
    dst = volume_root / dst_rel
    print(f"{dst_rel}: exists={dst.exists()}, size={dst.stat().st_size} bytes")

# COMMAND ----------

# MAGIC %md
# MAGIC > #### 7. Create the processed output area
# MAGIC >
# MAGIC > `rideshare_dev.processed` is the Schema used for processed-data objects.
# MAGIC >
# MAGIC > `rideshare_dev.processed.output_files` represents the
# MAGIC > `rideshare/processed` ADLS folder where later lessons write processed
# MAGIC > files.
# MAGIC >
# MAGIC > The Volume may be empty at the end of this lab. That is expected.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS rideshare_dev.processed
# MAGIC COMMENT 'Processed file outputs for the rideshare project';

# COMMAND ----------

spark.sql(f"""
CREATE EXTERNAL VOLUME IF NOT EXISTS rideshare_dev.processed.output_files
LOCATION '{abfss_root}/processed'
COMMENT 'Destination volume for processed rideshare files'
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE VOLUME rideshare_dev.processed.output_files;

# COMMAND ----------

# Confirm the processed volume exists (may be empty — that is expected)
output_root = "/Volumes/rideshare_dev/processed/output_files"

try:
    display(dbutils.fs.ls(output_root))
except Exception as e:
    print(f"Processed volume is empty or not listable yet (OK): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Setup complete
# MAGIC
# MAGIC | Object | Purpose |
# MAGIC |---|---|
# MAGIC | `el_rideshare_dev` | External Location for the `rideshare` ADLS path |
# MAGIC | `rideshare_dev` | Catalog used by the course, with managed storage at `{abfss_root}/uc-managed` |
# MAGIC | `rideshare_dev.landing.source_files` | Landing Volume for the course source files |
# MAGIC | `rideshare_dev.processed.output_files` | Processed Volume for files written by later lessons |
# MAGIC
# MAGIC **Next:** `02 - Reading CSV`