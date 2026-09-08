# Databricks notebook source
# MAGIC %md
# MAGIC # 99 - Rideshare Project Cleanup and Reset
# MAGIC
# MAGIC Utility reset if something goes wrong. All cleanup actions are off by default.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Clear `practice/` without touching `curated/`
# MAGIC - Clear `curated/` (wide blast radius)
# MAGIC - Clear landing and recopy from notebook **01**
# MAGIC - Fully tear down catalog, external location, and ADLS folder while leaving the
# MAGIC   storage credential in place
# COMMAND ----------

# Lab config — must match Notebook 01 (overwrite with YOUR values).

storage_account = "sadevdbxeus2"
container = "container-dev-dbx"
storage_credential = "ac_dev_dbx_eus2"
adls_folder = "rideshare"

abfss_root = (
    f"abfss://{container}@{storage_account}.dfs.core.windows.net/{adls_folder}"
)

landing_volume = "/Volumes/rideshare_dev/landing/source_files"
processed_volume = "/Volumes/rideshare_dev/processed/output_files"
practice_path = f"{processed_volume}/practice"
curated_path = f"{processed_volume}/curated"

print(f"abfss_root = {abfss_root}")
print(f"storage_credential = {storage_credential}")

# COMMAND ----------

# This function deletes all files and folders inside a volume path.
# It does NOT delete the volume itself — only its contents.

def clear_volume_contents(volume_path: str) -> None:
    try:
        items = dbutils.fs.ls(volume_path)
    except Exception:
        print(f"  Path not found or empty: {volume_path}")
        return

    for item in items:
        dbutils.fs.rm(item.path, recurse=True)

    print(f"  Cleared: {volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Cleanup options
# MAGIC
# MAGIC **How to use:**
# MAGIC 1. Run the config cell, then the helper-function cell
# MAGIC 2. Find the level that matches your problem
# MAGIC 3. Change the flag from `False` to `True`
# MAGIC 4. Run **only that cell** (not the whole notebook)
# MAGIC 5. Change the flag back to `False` when done
# MAGIC
# MAGIC > If you already ran this once and some steps show "not found", that's
# MAGIC > normal — it means those items were already removed.

# COMMAND ----------

# -------------------------------------------------------
# Level 1 — Clear practice outputs (Module 5 write practice)
#
# What happens when True:
#   - Deletes contents of .../processed/output_files/practice/
#   - Does NOT touch curated/ or managed tables
# -------------------------------------------------------

RESET_PRACTICE_OUTPUTS = False  # ← Change to True, then run this cell

if RESET_PRACTICE_OUTPUTS:
    clear_volume_contents(practice_path)
    print("\n✓ Done. Re-run Module 5 write cells to regenerate practice outputs.")
    print("\nRemember: set RESET_PRACTICE_OUTPUTS back to False.")
else:
    print("Skipped (RESET_PRACTICE_OUTPUTS = False).")
    print(f"  Would clear: {practice_path}")

# COMMAND ----------

# -------------------------------------------------------
# Level 2 — Clear curated outputs (Module 6 curated Parquet)
#
# BLAST RADIUS: deletes ALL curated folders (cleaned datasets, joins, KPIs).
# You must re-run Module 6 onward to rebuild.
#
# What happens when True:
#   - Deletes contents of .../processed/output_files/curated/
#   - Does NOT touch practice/ or managed tables
# -------------------------------------------------------

RESET_CURATED_OUTPUTS = False  # ← Change to True, then run this cell

if RESET_CURATED_OUTPUTS:
    clear_volume_contents(curated_path)
    print("\n✓ Done. Re-run Module 6+ notebooks to regenerate curated outputs.")
    print("\nRemember: set RESET_CURATED_OUTPUTS back to False.")
else:
    print("Skipped (RESET_CURATED_OUTPUTS = False).")
    print(f"  Would clear: {curated_path}")

# COMMAND ----------

# -------------------------------------------------------
# Level 3 — Clear landing source files
#
# What happens when True:
#   - Deletes all dataset folders inside the landing volume
#   - The volume and schema stay in place
# -------------------------------------------------------

RESET_LANDING_FILES = False  # ← Change to True, then run this cell

if RESET_LANDING_FILES:
    clear_volume_contents(landing_volume)
    print("\n✓ Done. Re-run folder creation and file copy cells in Notebook 01.")
    print("\nRemember: set RESET_LANDING_FILES back to False.")
else:
    print("Skipped (RESET_LANDING_FILES = False).")
    print(f"  Would clear: {landing_volume}")

# COMMAND ----------

# -------------------------------------------------------
# Level 4 — Full project teardown (start over from scratch)
#
# What happens when True:
#   1. Delete files inside external volumes
#   2. Drop the rideshare_dev catalog (and everything in it, including
#      managed tables from saveAsTable previews)
#   3. Drop the el_rideshare_dev external location
#   4. Delete the ADLS project folder from storage
#
# What is NOT touched:
#   - Storage credential named in the config cell (stays for reuse)
# -------------------------------------------------------

FULL_PROJECT_TEARDOWN = False  # ← Change to True, then run this cell

if FULL_PROJECT_TEARDOWN:
    print("Step 1: Clearing volume files...")
    for vol in [landing_volume, processed_volume]:
        try:
            clear_volume_contents(vol)
        except Exception as e:
            print(f"  Could not clear {vol}: {e}")

    print("\nStep 2: Dropping rideshare_dev catalog...")
    spark.sql("DROP CATALOG IF EXISTS rideshare_dev CASCADE")
    print("  Done.")

    print("\nStep 3: Dropping external location...")
    spark.sql("DROP EXTERNAL LOCATION IF EXISTS el_rideshare_dev FORCE")
    print("  Done.")

    print(f"\nStep 4: Removing {adls_folder}/ folder from ADLS...")
    try:
        dbutils.fs.rm(abfss_root, recurse=True)
        print("  Done.")
    except Exception as e:
        if "LOCATION_OVERLAP" in str(e):
            print("  Could not delete — Unity Catalog still protects this path.")
            print("  Manual step: Azure Portal → Storage Account → Containers")
            print(f"  → {container} → select '{adls_folder}' folder → Delete")
        else:
            print(f"  Error: {e}")

    print("\n✓ Teardown complete. Run Notebook 01 from the top to rebuild.")
    print("\nRemember: set FULL_PROJECT_TEARDOWN back to False.")
else:
    print("Skipped (FULL_PROJECT_TEARDOWN = False).")
    print("  Would drop: rideshare_dev catalog")
    print("  Would drop: el_rideshare_dev external location")
    print(f"  Would delete: {adls_folder}/ folder in ADLS")
    print(f"  Would NOT touch: {storage_credential} storage credential")

# COMMAND ----------

# MAGIC %md
# MAGIC **Next:** `01 - Unity Catalog Volumes and Data Landing` (recovery returns
# MAGIC to the workflow that invokes this notebook).
