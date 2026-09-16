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
# MAGIC Before you run any code cells:
# MAGIC
# MAGIC 1. Open this notebook in your Azure Databricks workspace (via the Git folder
# MAGIC    that tracks this course repository).
# MAGIC 2. In the notebook toolbar, open the **Connect** (compute) dropdown.
# MAGIC 3. Select classic **all-purpose** compute, or start it if it is stopped.
# MAGIC 4. Wait until the notebook shows that it is attached (connected).
# MAGIC
# MAGIC If a cell fails with a message about no cluster or compute, attach compute
# MAGIC and try again.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared Python state
# MAGIC
# MAGIC Python cells share a running **state**. A variable created in one Python
# MAGIC cell is available to another Python cell **after** the defining cell has
# MAGIC run. Run the next two cells in order.

# COMMAND ----------

# A rideshare-flavored value — shared with later Python cells in this session.
base_fare = 2.50

# COMMAND ----------

print(f"Base fare: {base_fare}")

# COMMAND ----------

# MAGIC %md
# MAGIC If you run the print cell before `base_fare = 2.50`, Python raises
# MAGIC `NameError` because the variable does not exist yet.
# MAGIC
# MAGIC One `%sql` cell later is **expected to fail**. Run that cell separately,
# MAGIC read the error, then continue — or accept that **Run All** will show that
# MAGIC error mid-notebook by design.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Worked example: `%sql`
# MAGIC
# MAGIC The cell below runs a simple Spark SQL statement. The result renders as a
# MAGIC table in the notebook output.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Hello from a SQL cell' AS message

# COMMAND ----------

# MAGIC %md
# MAGIC The SQL greeting worked — so the SQL cell is running correctly. Now ask
# MAGIC SQL for the Python variable `base_fare`.
# MAGIC
# MAGIC Run the next cell **separately**. It is **expected to fail**: SQL treats
# MAGIC `base_fare` as a column name and cannot find that column. Python locals
# MAGIC are not visible to SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT base_fare  -- # Expected: AnalysisException

# COMMAND ----------

# MAGIC %md
# MAGIC ### What you should see
# MAGIC
# MAGIC Another Python cell can read `base_fare`, but a SQL cell cannot. Python
# MAGIC and SQL keep separate language state.
# MAGIC
# MAGIC Next, `%sh` reaches the **driver shell** — another environment (still not
# MAGIC Python variable state).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Worked example: `%sh`
# MAGIC
# MAGIC `%sh` runs a bash command on the **driver node** — not on executors, and
# MAGIC not inside the Python process that holds `base_fare`. Useful for quick
# MAGIC checks (Python version, working directory). Not a substitute for Spark
# MAGIC file operations on large data.

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
# MAGIC `%fs` is shorthand for `dbutils.fs` commands. The path below is a
# MAGIC **read-only sample collection** Databricks provides on supported compute.
# MAGIC It is only a demo for listing — this course's rideshare files are read
# MAGIC later from the shared dataset paths, not from here.

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /databricks-datasets

# COMMAND ----------

# MAGIC %md
# MAGIC You listed the path with a magic — a quick look. When your **Python code**
# MAGIC needs that listing (count items, loop, filter), use `dbutils.fs` instead.

# COMMAND ----------

files = dbutils.fs.ls("/databricks-datasets")
print(f"Found {len(files)} items. First 5:")
for f in files[:5]:
    print(f" - {f.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC Same listing as the `%fs` cell, now as a list in a variable. Magics are
# MAGIC handy for a quick look; use `dbutils.fs` when your code must work with
# MAGIC the result.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - Python cells share state by **run order** — define before use.
# MAGIC - Each language keeps its **own** state — a SQL cell cannot read a Python
# MAGIC   local like `base_fare`.
# MAGIC - **`%fs`** is a quick look; **`dbutils.fs`** gives you a Python result
# MAGIC   you can count and loop over.
# MAGIC
# MAGIC Next up: `03 - Your First DataFrame`.
