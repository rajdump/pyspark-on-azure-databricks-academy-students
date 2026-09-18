# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Databricks Workspace and Notebook
# MAGIC
# MAGIC Read **12 - Databricks workspace and Notebook** in the course Notion hub
# MAGIC first. Then attach classic all-purpose compute and run the cells below.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Print a Python value defined in an earlier cell
# MAGIC - Run a `%sql` cell and confirm SQL cannot see a Python local
# MAGIC - Run `%sh` on the driver for a quick check
# MAGIC - List files with `%fs` and `dbutils.fs`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach classic **all-purpose** compute before you run any code cells.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared Python state
# MAGIC
# MAGIC Run the next two cells **in order**. One `%sql` cell later is **expected
# MAGIC to fail** — run it separately, or accept that **Run All** shows that error.

# COMMAND ----------

# A rideshare-flavored value — shared with later Python cells in this session.
base_fare = 2.50

# COMMAND ----------

print(f"Base fare: {base_fare}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Worked example: `%sql`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Hello from a SQL cell' AS message

# COMMAND ----------

# MAGIC %md
# MAGIC The next cell is **expected to fail**: SQL cannot see the Python local
# MAGIC `base_fare`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT base_fare  -- # Expected: AnalysisException

# COMMAND ----------

# MAGIC %md
# MAGIC ## Worked example: `%sh`
# MAGIC
# MAGIC `%sh` runs on the **driver**, not in the Python process that holds
# MAGIC `base_fare`.

# COMMAND ----------

# MAGIC %sh
# MAGIC python3 --version

# COMMAND ----------

# MAGIC %sh
# MAGIC pwd

# COMMAND ----------

# MAGIC %md
# MAGIC ## Worked example: `%fs`
# MAGIC
# MAGIC Sample listing only — not this course's rideshare files.

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /databricks-datasets

# COMMAND ----------

# MAGIC %md
# MAGIC Same path in Python, as objects you can count and loop over:

# COMMAND ----------

files = dbutils.fs.ls("/databricks-datasets")
print(f"Found {len(files)} items. First 5:")
for f in files[:5]:
    print(f" - {f.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - Python cells share state by run order.
# MAGIC - SQL cannot read a Python local like `base_fare`.
# MAGIC - `%fs` is a quick look; `dbutils.fs` returns a Python list.
# MAGIC
# MAGIC Next up: `03 - Your First DataFrame`.