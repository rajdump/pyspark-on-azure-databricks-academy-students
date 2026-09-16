# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Apache Spark Architecture
# MAGIC
# MAGIC Read **10-Apache Spark architecture** in the course Notion hub first. Then
# MAGIC attach classic all-purpose compute and confirm the live `spark` session.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Print that `spark` is a SparkSession
# MAGIC - Print the Spark engine version
# MAGIC - Print this session's Spark application id

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Before you run any code cells:
# MAGIC
# MAGIC 1. Open this notebook in your Azure Databricks workspace.
# MAGIC 2. In the notebook toolbar, open the **Connect** (compute) dropdown.
# MAGIC 3. Select classic **all-purpose** compute, or start it if it is stopped.
# MAGIC 4. Wait until the notebook shows that it is attached (connected).
# MAGIC
# MAGIC If a cell fails with a message about no cluster or compute, attach compute
# MAGIC and try again. You do not need any data files for this notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Worked example: confirm the live session
# MAGIC
# MAGIC Azure Databricks already provides `spark` when this notebook is attached.
# MAGIC You do not create a SparkSession yourself.
# MAGIC
# MAGIC `spark` is the SparkSession (Notion 10). `spark.version` is the engine on
# MAGIC this compute. `spark.app.id` labels this Spark application.

# COMMAND ----------

print(f"spark is a: {type(spark).__name__}")
print(f"Spark version: {spark.version}")
print(f"Spark application id: {spark.conf.get('spark.app.id')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### What you should see
# MAGIC
# MAGIC - `spark is a: SparkSession`
# MAGIC - A Spark version string (for DBR 17.3 LTS this is in the Spark 4.0 line)
# MAGIC - An application id like `app-...` (the exact id varies)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - `spark` is a **SparkSession**.
# MAGIC - `spark.version` is the Spark engine on this compute.
# MAGIC - `spark.app.id` labels this Spark application.
# MAGIC
# MAGIC Architecture roles (Driver, SparkContext, cluster manager, workers,
# MAGIC executors) are on Notion 10. Jobs, stages, and tasks are on Notion 11.
# MAGIC
# MAGIC Next up: `02 - Databricks Workspace and Notebook`.