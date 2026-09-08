# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Introduction to Azure Databricks and the Workspace
# MAGIC
# MAGIC First orientation in the Azure Databricks workspace, before Spark architecture
# MAGIC or DataFrames.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Navigate the workspace browser, notebook editor, and compute attach
# MAGIC - Explain Databricks Runtime / LTS, including the classic vs serverless gotcha
# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Before you run any code cells:
# MAGIC
# MAGIC 1. Open this notebook in your Azure Databricks workspace (via the Git folder
# MAGIC    that tracks this course repository).
# MAGIC 2. In the notebook toolbar, open the **Connect** (compute) dropdown.
# MAGIC 3. Select compute that your workspace provides for this course, or start it
# MAGIC    if it is stopped.
# MAGIC 4. Wait until the notebook shows that it is attached (connected).
# MAGIC
# MAGIC If a cell fails with a message about no cluster or compute, attach compute
# MAGIC and try again. You do not need any data files for this notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Concept: the Azure Databricks workspace
# MAGIC
# MAGIC An Azure Databricks **workspace** is the web environment where you organize
# MAGIC notebooks, attach compute, and run batch data engineering work. Think of it
# MAGIC as the home base for this course — not a place you install Spark yourself.
# MAGIC
# MAGIC Three areas matter most right now:
# MAGIC
# MAGIC | Area | What it is | Why you care |
# MAGIC |---|---|---|
# MAGIC | **Workspace browser** | Folders and notebooks (including this Git-backed course) | Where you find and open lessons |
# MAGIC | **Notebook editor** | Cells you read, edit, and run | Where you write and execute PySpark |
# MAGIC | **Compute** | The machines that actually run your code | Notebooks do nothing useful until attached |
# MAGIC
# MAGIC In production, the same idea holds: your logic lives in notebooks or jobs,
# MAGIC and **compute** is what executes it. Choosing and attaching the right
# MAGIC compute is platform literacy every later module depends on.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Concept: attaching compute
# MAGIC
# MAGIC **Compute** is the cluster (or serverless environment) that runs notebook
# MAGIC cells. **Attaching** (connecting) a notebook to compute links this editor
# MAGIC to that runtime so `spark` and Python cells can execute.
# MAGIC
# MAGIC Common gotcha: you open a notebook and click Run before anything is
# MAGIC attached. The cell cannot run until compute is connected and ready.
# MAGIC
# MAGIC For this course, start with **classic all-purpose** compute in **Standard**
# MAGIC access mode when your workspace offers it. Other compute types appear in
# MAGIC later modules when a topic needs them — you do not need every option yet.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Concept: Databricks Runtime and LTS
# MAGIC
# MAGIC **Databricks Runtime (DBR)** is the software stack on your compute: Apache
# MAGIC Spark, Python, and supporting libraries, packaged as one versioned
# MAGIC environment.
# MAGIC
# MAGIC **LTS** means **Long Term Support** — a Runtime line that receives fixes
# MAGIC for a longer window than non-LTS releases. Teams pin an LTS version so
# MAGIC jobs stay predictable instead of silently changing underneath them.
# MAGIC
# MAGIC This course targets **Databricks Runtime 17.3 LTS** (Spark 4.0, Python
# MAGIC 3.12). When you create or pick compute for these notebooks, prefer that
# MAGIC Runtime (or the LTS version your workspace admin has standardized for the
# MAGIC course).
# MAGIC
# MAGIC You do not need to memorize every library on the Runtime. Remember the
# MAGIC idea: **the Runtime version defines what your code runs on.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Worked example: confirm compute is ready
# MAGIC
# MAGIC With this notebook attached to compute, run the next cell. Azure Databricks
# MAGIC already provides a `spark` session in the notebook — you do not create one
# MAGIC yourself here.
# MAGIC
# MAGIC The cell prints the Spark version and the Databricks Runtime version from
# MAGIC your attached compute. That is a simple proof that the workspace, notebook,
# MAGIC and compute are connected.

# COMMAND ----------

import os

# A notebook on Databricks already has `spark`. These prints confirm that this
# notebook is attached to compute and show which Runtime you are on.
spark_version = spark.version
dbr_version = os.environ.get(
    "DATABRICKS_RUNTIME_VERSION",
    spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", "unknown"),
)

print(f"Spark version on this compute: {spark_version}")
print(f"Databricks Runtime version: {dbr_version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### What you should see
# MAGIC
# MAGIC - A Spark version string (for DBR 17.3 LTS this is in the Spark 4.0 line).
# MAGIC - A Databricks Runtime version string (for this course, look for **17.3** LTS)
# MAGIC   when you are on **classic all-purpose** compute — the baseline for this
# MAGIC   notebook.
# MAGIC - If the cell errors because compute is missing, use **Connect**, attach
# MAGIC   compute, wait until it is ready, and run the cell again.
# MAGIC
# MAGIC You can also confirm Runtime in the UI: open the compute details for the
# MAGIC cluster you attached and check the Databricks Runtime version shown there.
# MAGIC
# MAGIC **Gotcha — serverless.** Serverless compute does **not** use Databricks
# MAGIC Runtime version pins. It uses its own environment versions. On serverless,
# MAGIC `spark.version` usually still prints, but the DBR lookup above may return
# MAGIC `unknown` or be missing. Prefer classic all-purpose **Standard** for this
# MAGIC lesson when you want to see Runtime / LTS clearly.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC Practice the attach-and-run loop — this time confirming Runtime from the
# MAGIC **compute UI**, not by re-running the lookup cell above.
# MAGIC
# MAGIC 1. If compute is connected, disconnect it from the notebook toolbar, then
# MAGIC    attach (connect) again and wait until it is ready. Prefer classic
# MAGIC    all-purpose **Standard** for this exercise.
# MAGIC 2. Open the compute details for the cluster you attached and find the
# MAGIC    **Databricks Runtime** version shown there (classic compute only).
# MAGIC 3. Run the cell below. It prints only `spark.version`. Fill in
# MAGIC    `dbr_version_from_ui` with the Runtime string you saw in the UI, then
# MAGIC    re-run so both values print.
# MAGIC
# MAGIC On serverless, the UI may not show a DBR pin — use classic Standard for
# MAGIC this exercise. Exact suffix matching is not the goal; the goal is knowing
# MAGIC where Runtime appears in the UI after you reconnect.

# COMMAND ----------

# After re-attaching compute, run this cell.
print(f"Spark version on this compute: {spark.version}")

# Replace with the Runtime version from the compute UI (classic), e.g. "17.3.x".
dbr_version_from_ui = "REPLACE_ME"
print(f"Databricks Runtime from compute UI: {dbr_version_from_ui}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - The **workspace** is where you browse notebooks and do your work.
# MAGIC - **Compute** runs your cells; attach (connect) before you expect output.
# MAGIC - **Databricks Runtime** is the versioned Spark/Python stack on that compute;
# MAGIC   **LTS** is the supported line this course pins (**17.3 LTS**).
# MAGIC
# MAGIC Next up: `02 - Apache Spark Architecture and PySpark` — how Spark executes
# MAGIC work once compute is attached (driver, executors, jobs, stages, and tasks).
