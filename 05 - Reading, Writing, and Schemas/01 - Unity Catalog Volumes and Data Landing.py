# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Unity Catalog Volumes and Data Landing
# MAGIC
# MAGIC Create the course catalog, volumes, and land repo source files — including
# MAGIC controlled-bad CSVs for Module 6.
# MAGIC
# MAGIC Repo `data/raw` (open from the Git folder) plus config-cell Azure
# MAGIC values.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Set Tier 1 lab config and create `rideshare_dev` landing/processed volumes
# MAGIC - Copy canonical + controlled-bad sources into landing and verify
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
# MAGIC > #### 1. Create the rideshare project folder manually in the Azure Portal
# MAGIC >
# MAGIC > In **your** storage account / container (from the config cell), create:
# MAGIC >
# MAGIC > ```text
# MAGIC > {container}/
# MAGIC > └── {adls_folder}/
# MAGIC > ```

# COMMAND ----------

# MAGIC %md
# MAGIC > #### 2. Create a Unity Catalog External Location pointing to that ADLS folder

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
# MAGIC > #### 3. Test the external location
# MAGIC >
# MAGIC > 1. Open **Catalog Explorer** → **External Locations** → `el_rideshare_dev`
# MAGIC > 2. Click **Test connection** (top-right)
# MAGIC > 3. All checks should show green: Read, List, Write, Delete, Path Exists,
# MAGIC >    Hierarchical Namespace Enabled, **File Events Read**
# MAGIC
# MAGIC <details>
# MAGIC <summary><strong>Troubleshooting: File Events Read Failed (click to expand)</strong></summary>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Step 1 — Verify the four required Azure roles are assigned**
# MAGIC
# MAGIC Go to Azure Portal → Storage Account (**your** `storage_account` from the
# MAGIC config cell) → Access Control (IAM). Confirm the access connector's
# MAGIC managed identity behind **your** `storage_credential` has:
# MAGIC
# MAGIC 1. Storage Account Contributor
# MAGIC 2. Storage Blob Data Contributor
# MAGIC 3. EventGrid EventSubscription Contributor
# MAGIC 4. Storage Queue Data Contributor
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Step 2 — Check for ABAC conditions on role assignments**
# MAGIC
# MAGIC Look at the **Condition** column in the role assignments list.
# MAGIC If any role shows "Add" (instead of "None"), it has a restricting condition.
# MAGIC
# MAGIC **Fix:** Delete the conditioned role assignment, then re-add the same role
# MAGIC **without** conditions (select "Not constrained" on the Conditions tab).
# MAGIC
# MAGIC *Note: The conditions editor won't let you save with zero conditions —
# MAGIC you must delete and re-create the assignment.*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Step 3 — Check storage account networking**
# MAGIC
# MAGIC Go to Storage Account → Networking. Confirm:
# MAGIC - **Public network access** = "Enabled from all networks", OR
# MAGIC - If firewalled: "Allow Azure services on the trusted services list" is checked
# MAGIC
# MAGIC The queue endpoint (`*.queue.core.windows.net`) must be reachable from
# MAGIC the Databricks control plane.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Step 4 — Wait for role propagation and re-test**
# MAGIC
# MAGIC Azure role changes can take **5–10 minutes** to propagate.
# MAGIC After fixing roles, wait a few minutes, then click **Test connection** again.
# MAGIC The UI shows the cached last result until you explicitly re-run it.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC > #### 4. Create the `rideshare_dev` catalog with a dedicated managed storage path

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
# MAGIC > #### 5. Create an external volume for the landing area, then create five dataset folders inside it

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
# MAGIC > #### 6. Copy source files from the Git repository to the landing volume
# MAGIC
# MAGIC Open this notebook from the course **Git folder** so the copy cell can
# MAGIC find `data/raw` by walking up from the working directory.

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
# MAGIC > #### 7. Create a separate processed schema and destination external volume

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
# MAGIC ## Exercise
# MAGIC
# MAGIC Confirm the landing volume looks right before you move on:
# MAGIC
# MAGIC 1. List `/Volumes/rideshare_dev/landing/source_files/` and check that you
# MAGIC    see five dataset folders (`trip`, `trip_time`, `zone_lookup`,
# MAGIC    `payment`, `drivers`).
# MAGIC 2. List inside `trip/` and confirm `trip.csv` and `bad_trip_data.csv`
# MAGIC    are present.
# MAGIC 3. Print how many items are in the `payment/` folder (expect **2** files:
# MAGIC    `payment.avro` and `bad_payment_data.csv`).
# MAGIC
# MAGIC Use `dbutils.fs.ls` (same pattern as the verification cells above).

# COMMAND ----------

# Your code here



# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Setup complete
# MAGIC
# MAGIC You now have:
# MAGIC
# MAGIC | Object | Name | Purpose |
# MAGIC |--------|------|--------|
# MAGIC | External Location | `el_rideshare_dev` | Connects Databricks to your ADLS project folder |
# MAGIC | Catalog | `rideshare_dev` | Top-level container for all rideshare data |
# MAGIC | Schema | `rideshare_dev.landing` | Holds raw source files as-is |
# MAGIC | Schema | `rideshare_dev.processed` | Holds file outputs (and later managed-table previews) |
# MAGIC | Volume | `landing.source_files` | 5 source datasets + 2 bad-data CSV files |
# MAGIC | Volume | `processed.output_files` | Outputs under `practice/` (Module 5) and `curated/` (Module 6+) |
# MAGIC
# MAGIC `practice/` and `curated/` appear under `output_files` on **first write** —
# MAGIC this notebook does not create those folders.
# MAGIC
# MAGIC Reading notebooks (CSV → … → write patterns) use Volume paths only.
# MAGIC Governance of these objects is Module 11.
# MAGIC
# MAGIC **Next:** `02 - Reading CSV`