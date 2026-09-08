# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Apache Spark Architecture and PySpark
# MAGIC
# MAGIC Build the mental model of how Spark executes a request once compute is
# MAGIC attached.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Explain why Spark distributes work and how PySpark relates to Spark
# MAGIC - Use the provided `SparkSession` (`spark`)
# MAGIC - Describe driver/executors and jobs → stages → tasks
# MAGIC - Observe one live request in the Spark UI on classic compute
# COMMAND ----------

# MAGIC %md
# MAGIC ## The one idea behind Spark
# MAGIC
# MAGIC Some workloads are too large for one machine to finish in a reasonable
# MAGIC time. Spark's answer is to **split the work into pieces and process those
# MAGIC pieces at the same time across many machines**, then combine the results.
# MAGIC
# MAGIC Imagine counting every book in a library. One person walking every aisle
# MAGIC would take all day. With many people each counting one shelf, the job
# MAGIC finishes much faster. Spark is the system that divides the shelves,
# MAGIC assigns the work, and adds up the totals.
# MAGIC
# MAGIC In production batch data engineering — including rideshare pipelines that
# MAGIC clean, join, and aggregate trip data — you rely on that same idea.
# MAGIC
# MAGIC To use that engine from Python on Databricks, you talk to it through one
# MAGIC object.

# COMMAND ----------

# Prove the Spark engine is available on the compute you attached.
print(f"Spark is running. Engine version: {spark.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## PySpark and the `spark` variable (SparkSession)
# MAGIC
# MAGIC You just talked to a live Spark engine. Two things to notice:
# MAGIC
# MAGIC - You never installed Spark or started it yourself. When this notebook
# MAGIC   attached to compute, Databricks provided a ready-to-use object named
# MAGIC   **`spark`**.
# MAGIC - You wrote plain Python. Spark itself runs on the JVM, but **PySpark**
# MAGIC   lets you drive Spark from Python. That is all "PySpark" means: Python
# MAGIC   talking to Spark.
# MAGIC
# MAGIC ### What `spark` is
# MAGIC
# MAGIC `spark` is a **SparkSession** — your entry point (and connection) to this
# MAGIC Spark application on the cluster.
# MAGIC
# MAGIC Picture a hotel reception desk. For a room, a taxi, or dinner, you do not
# MAGIC visit every department yourself. You ask reception, and they route the
# MAGIC request. **`spark` is reception for the Spark engine.**
# MAGIC
# MAGIC Through a SparkSession you can later:
# MAGIC
# MAGIC - read data → `spark.read...`
# MAGIC - create DataFrames → `spark.createDataFrame(...)`
# MAGIC - run SQL → `spark.sql(...)`
# MAGIC - read or set supported session settings → `spark.conf...`
# MAGIC
# MAGIC You will practice DataFrames in a later notebook. Here the point is only:
# MAGIC **requests go through `spark`.**
# MAGIC
# MAGIC Outside Databricks (for example a standalone script) you would create the
# MAGIC session yourself. You will not run this in a Databricks notebook — it is
# MAGIC shown so you recognize it elsewhere:
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql import SparkSession
# MAGIC spark = SparkSession.builder.appName("my-app").getOrCreate()
# MAGIC ```
# MAGIC
# MAGIC Next, prove you are connected to a real Spark application by reading its
# MAGIC application id — a label Spark (and the Spark UI) use for this session.

# COMMAND ----------

# Application id labels the Spark application behind this session.
# On serverless, Spark Connect often does not expose spark.app.id — that is expected.
from pyspark.errors import PySparkException

try:
    app_id = spark.conf.get("spark.app.id")
    print(f"Connected to Spark application: {app_id}")
except PySparkException:
    print("spark.app.id is not available on this compute type.")
    print("That is expected on serverless. Prefer classic all-purpose for this lesson.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### What you should see
# MAGIC
# MAGIC - On **classic all-purpose** compute: a line like
# MAGIC   `Connected to Spark application: app-...` (the exact id varies).
# MAGIC - That id is the label for **this** Spark application — the same idea you
# MAGIC   will look up later in the Spark UI.
# MAGIC
# MAGIC **Gotcha — serverless.** Serverless uses Spark Connect and does **not**
# MAGIC expose `spark.app.id`. The cell above prints a short note instead of an
# MAGIC id. Prefer classic all-purpose **Standard** for this lesson so you can
# MAGIC see the application id and later open the Spark UI.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Who does the work
# MAGIC
# MAGIC Connection is only the start. When you **submit a request** through
# MAGIC `spark`, several roles work together. Think of a kitchen during a dinner
# MAGIC rush:
# MAGIC
# MAGIC | Role | Analogy | What it does |
# MAGIC |---|---|---|
# MAGIC | **SparkSession (`spark`)** | Reception / ticket window | Where your code submits Spark requests |
# MAGIC | **Driver** | Head chef | Plans the distributed work, schedules tasks, tracks progress |
# MAGIC | **Executors** | Line cooks | Run those tasks on their assigned data pieces, in parallel |
# MAGIC | **Cluster manager** | The kitchen building and staffing | Supplies CPU/memory and launches executors. On Databricks this is handled for you |
# MAGIC
# MAGIC You write the request once. The driver plans. The executors run. The
# MAGIC cluster manager makes sure workers exist.
# MAGIC
# MAGIC Here is the same idea as a picture (Diagram A).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diagram A — runtime roles
# MAGIC
# MAGIC Who plans, who allocates machines, who runs the work (plain text so it
# MAGIC renders without external diagram scripts):
# MAGIC
# MAGIC ```text
# MAGIC Driver  (holds SparkSession `spark`; plans work and tracks progress)
# MAGIC    |
# MAGIC    | requests resources
# MAGIC    v
# MAGIC Cluster manager  (Databricks handles this)
# MAGIC    |
# MAGIC    +--> Executor  (runs tasks)
# MAGIC    +--> Executor  (runs tasks)
# MAGIC ```
# MAGIC
# MAGIC **Same story on this diagram — counting trips (stand-in request):**
# MAGIC
# MAGIC 1. You will call a count through **`spark`** (SparkSession on the driver).
# MAGIC 2. The **driver** plans how to count across the cluster.
# MAGIC 3. The **cluster manager** has already provided **executors** on your compute.
# MAGIC 4. **Executors** run the actual counting work and send results back to the driver.
# MAGIC
# MAGIC Planning roles is half the story. Spark also nests the work into smaller
# MAGIC units.

# COMMAND ----------

# MAGIC %md
# MAGIC ## How Spark splits a submitted request
# MAGIC
# MAGIC When a request needs distributed processing, Spark organizes the work into
# MAGIC nested units:
# MAGIC
# MAGIC | Unit | Meaning |
# MAGIC |---|---|
# MAGIC | **Application** | This notebook plus its SparkSession — the whole Spark app on your compute |
# MAGIC | **Job** | One unit of work created when you submit a request that must actually run (an *action*) |
# MAGIC | **Stage** | A phase inside a job |
# MAGIC | **Task** | The smallest unit; typically one task per data partition, run on an executor |
# MAGIC
# MAGIC So: **application → job → stage(s) → task(s)**. Tasks are what executors
# MAGIC run (Diagram A). Jobs are what the driver plans for your request.
# MAGIC
# MAGIC Later modules cover why stage counts change (partitioning, shuffles,
# MAGIC adaptive query execution). For now: stage and task counts depend on how
# MAGIC data is partitioned and how Spark optimizes the plan — **not** on how big
# MAGIC your cluster is.
# MAGIC
# MAGIC Diagram B shows how those units nest for our count request.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diagram B — execution hierarchy
# MAGIC
# MAGIC How one submitted request nests (plain text, no external diagram scripts):
# MAGIC
# MAGIC ```text
# MAGIC Application  (this notebook + SparkSession)
# MAGIC    |
# MAGIC    v
# MAGIC Job  (created by the count request)
# MAGIC    |
# MAGIC    v
# MAGIC Stage(s)
# MAGIC    |
# MAGIC    v
# MAGIC Task(s)  (run on executors)
# MAGIC ```
# MAGIC
# MAGIC **Same story on this diagram:**
# MAGIC
# MAGIC 1. **Application** — this notebook and its SparkSession.
# MAGIC 2. **Job** — created when the count request runs.
# MAGIC 3. **Stage(s)** — phases Spark needs for that job.
# MAGIC 4. **Task(s)** — work pieces executors run (link back to Diagram A).
# MAGIC
# MAGIC Now submit that request live and find it in the Spark UI.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Worked example: submit a count request
# MAGIC
# MAGIC Production idea: count today's rideshare trips. You will build real trip
# MAGIC DataFrames in `04 - Your First DataFrame`. Here we submit a **stand-in
# MAGIC count request** that follows the same Spark path:
# MAGIC
# MAGIC ```python
# MAGIC spark.range(1000).count()
# MAGIC ```
# MAGIC
# MAGIC After the cell finishes (on classic compute):
# MAGIC
# MAGIC 1. Open the **Spark UI** for this compute (or the notebook's Spark UI entry).
# MAGIC 2. Open **Jobs** and find the newest job for this run.
# MAGIC 3. Open that job → stages → tasks.
# MAGIC
# MAGIC **Gotcha — serverless.** Serverless may not expose the classic Spark UI the
# MAGIC same way. Prefer classic all-purpose **Standard** for this lesson.
# MAGIC
# MAGIC You are walking Diagram A (who) and Diagram B (nesting) for one request.

# COMMAND ----------

# Stand-in for "count today's trips" — same request path, no DataFrame lesson yet.
trip_stand_in_count = spark.range(1000).count()
print(f"Count result: {trip_stand_in_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### What you should see
# MAGIC
# MAGIC - Printed count `1000` in the notebook.
# MAGIC - In the Spark UI (classic), a recent **job** for that count, with stage and
# MAGIC   task detail underneath.
# MAGIC
# MAGIC Your turn — change the request and find the new job.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC 1. Keep classic all-purpose compute attached.
# MAGIC 2. Run the cell below (a different count size).
# MAGIC 3. In the Spark UI, find the **new** job for this run.
# MAGIC 4. Note how many **stages** and roughly how many **tasks** that job shows.
# MAGIC
# MAGIC Exact stage and task counts depend on partitioning and query optimization —
# MAGIC not on how big your cluster is. What matters is that you can find the job
# MAGIC and read its stage/task breakdown.

# COMMAND ----------

# Change the number if you like, re-run, then inspect the new job in the Spark UI.
trip_stand_in_count = spark.range(5000).count()
print(f"Count result: {trip_stand_in_count}")

# After checking the Spark UI, note what you saw, for example:
# stages_seen = ?
# tasks_seen = ?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - **Spark** distributes work across machines; **PySpark** is how you drive
# MAGIC   it from Python.
# MAGIC - **`spark`** is a **SparkSession** — your entry point to the Spark
# MAGIC   application.
# MAGIC - **Diagram A:** driver plans, cluster manager supplies executors, executors
# MAGIC   run tasks.
# MAGIC - **Diagram B:** application → job → stage(s) → task(s) for one submitted
# MAGIC   request.
# MAGIC - You observed that path with a stand-in count request; real trip DataFrames
# MAGIC   come next in the module sequence.
# MAGIC
# MAGIC Next up: `03 - Working with Notebooks` — cells, magic commands, and
# MAGIC `dbutils`. After that, `04 - Your First DataFrame` builds rideshare rows
# MAGIC for real — using the same request path you learned here.
