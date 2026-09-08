# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Working with Notebooks
# MAGIC
# MAGIC How notebook cells, languages, magics, and `dbutils` share — and do not share
# MAGIC — a live session.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Explain cell run order and shared Python state
# MAGIC - Explain that languages keep separate state
# MAGIC - Use magics (`%md`, `%sql`, `%fs`, `%sh`) and `%fs` vs `dbutils.fs`
# COMMAND ----------

# MAGIC %md
# MAGIC ## Cells and notebook state
# MAGIC
# MAGIC A notebook is made of **cells**. You can run them in any order, but later
# MAGIC cells often depend on values created earlier — so top-to-bottom is the
# MAGIC safe habit.
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
# MAGIC For predictable results:
# MAGIC
# MAGIC - Put definitions before the cells that use them.
# MAGIC - Run notebooks from top to bottom (**Run All**).
# MAGIC - **Exception:** one `%sql` cell later is **expected to fail** (language
# MAGIC   state demo). Run that cell separately, read the error, then continue —
# MAGIC   or accept that **Run All** will show that error mid-notebook by design.
# MAGIC - If results look inconsistent, clear the notebook state (for example
# MAGIC   **Detach & Re-attach**) and rerun from the top.
# MAGIC
# MAGIC Same-language sharing is only half the story. Other cell languages do
# MAGIC **not** automatically see Python variables like `base_fare`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Each language has its own state
# MAGIC
# MAGIC Both Python cells above can use `base_fare` because Python cells share
# MAGIC variables.
# MAGIC
# MAGIC SQL, Scala, and R cells **cannot** use variables created in Python. Each
# MAGIC language keeps its own state.
# MAGIC
# MAGIC Later in the course you will share data across languages with Spark
# MAGIC tables or temporary views — not by reading a Python local from SQL.
# MAGIC
# MAGIC To prove the boundary here, you need a non-Python cell. That is what
# MAGIC **magic commands** are for.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Magic commands
# MAGIC
# MAGIC By default, a cell runs **Python**. To switch one cell to another
# MAGIC language (or mode), put a **magic command** on its **first line** — or
# MAGIC use the language selector on the cell.
# MAGIC
# MAGIC | Cell type / mode | Magic (first line) | What it does |
# MAGIC |---|---|---|
# MAGIC | **Python** | *(none — default)* or `%python` | Runs Python / PySpark on the driver |
# MAGIC | **Markdown** | `%md` | Renders formatted text, headings, tables |
# MAGIC | **SQL** | `%sql` | Runs a Spark SQL statement; shows a result table |
# MAGIC | **Filesystem** | `%fs` | Quick filesystem commands (shorthand for `dbutils.fs`) |
# MAGIC | **Shell** | `%sh` | Bash on the driver node |
# MAGIC
# MAGIC Two other magics are useful later but are **not** cell languages:
# MAGIC
# MAGIC | Magic | Purpose |
# MAGIC |---|---|
# MAGIC | `%run` | Run another notebook and import its variables into this session |
# MAGIC | `%pip` | Install a Python package for this notebook session |
# MAGIC
# MAGIC Magics are a **Databricks notebook** feature, not part of Apache Spark
# MAGIC itself. A `%sql` cell uses the SQL supported by Databricks Runtime
# MAGIC (including Spark SQL behavior).
# MAGIC
# MAGIC First magic to try: `%sql`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Worked example: `%sql`
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
# MAGIC SELECT base_fare

# COMMAND ----------

# MAGIC %md
# MAGIC ### What that error confirms
# MAGIC
# MAGIC Another Python cell can read `base_fare`, but a SQL cell cannot. Python
# MAGIC and SQL keep separate language state.
# MAGIC
# MAGIC Magics are not only for SQL. Next, try `%sh` — it reaches the **driver
# MAGIC shell**, which is another environment (still not Python variable state).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Worked example: `%sh`
# MAGIC
# MAGIC `%sh` runs a bash command on the **driver node** — not on executors, and
# MAGIC not inside the Python process that holds `base_fare`. Useful for quick
# MAGIC checks (Python version, working directory). Not a substitute for Spark
# MAGIC file operations on large data.
# MAGIC
# MAGIC **Gotcha — serverless.** `%sh` may be restricted or unavailable on
# MAGIC serverless compute. Prefer classic all-purpose **Standard** if you need
# MAGIC it.

# COMMAND ----------

# MAGIC %sh
# MAGIC python3 --version

# COMMAND ----------

# MAGIC %sh
# MAGIC pwd

# COMMAND ----------

# MAGIC %md
# MAGIC **Tip — Web Terminal.** For interactive work (for example `vim`, `htop`,
# MAGIC or multi-step command-line sessions), prefer the Databricks **Web
# MAGIC Terminal** over stacking many `%sh` cells.
# MAGIC
# MAGIC For checking files in storage, prefer filesystem tools next — `%fs` and
# MAGIC `dbutils.fs` — instead of ad-hoc shell listing.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Worked example: `%fs`
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
# MAGIC needs that listing (count items, loop, filter), use `dbutils` instead.

# COMMAND ----------

# MAGIC %md
# MAGIC ## `dbutils`
# MAGIC
# MAGIC `dbutils` is a Databricks-provided toolbox available in this notebook
# MAGIC without an import. The utilities you will meet most often:
# MAGIC
# MAGIC | Utility | Access | What it does |
# MAGIC |---|---|---|
# MAGIC | **File system** | `dbutils.fs` | List, copy, move, delete files |
# MAGIC | **Widgets** | `dbutils.widgets` | Notebook parameters (later in the course) |
# MAGIC | **Notebook** | `dbutils.notebook` | Run or exit notebooks programmatically (later) |
# MAGIC | **Secrets** | `dbutils.secrets` | Read secrets from a scope (later) |
# MAGIC
# MAGIC Today you need only `dbutils.fs`. As a data engineer, that is how you
# MAGIC confirm files landed in storage before a job tries to read them.
# MAGIC
# MAGIC The next cell does what `%fs ls` did, but returns Python objects you can
# MAGIC count and loop over.

# COMMAND ----------

files = dbutils.fs.ls("/databricks-datasets")
print(f"Found {len(files)} items. First 5:")
for f in files[:5]:
    print(f" - {f.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC Same listing as the `%fs` cell, now as a list in a variable. Magics are
# MAGIC handy for a quick look; use `dbutils` when your code must work with the
# MAGIC result.
# MAGIC
# MAGIC Practice that distinction in the exercise below — same path, but filter
# MAGIC the names so the task is not a copy of the worked example.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC 1. Use `dbutils.fs.ls` on `/databricks-datasets`.
# MAGIC 2. Keep only entries whose **name** contains the letters `data`
# MAGIC    (case-insensitive is fine). If nothing matches in your workspace,
# MAGIC    try another short keyword that appears in the listing (for example
# MAGIC    `csv` or `taxi`).
# MAGIC 3. Print how many matched, then print those names.

# COMMAND ----------

# Your code here.
# Hint: filter with `"data" in e.name.lower()` (or your chosen keyword).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC One thread for this lesson:
# MAGIC
# MAGIC - Python cells share state by **run order** — define before use; prefer
# MAGIC   **Run All** (except the intentional failing `%sql` cell).
# MAGIC - Each language keeps its **own** state — a SQL cell cannot read a Python
# MAGIC   local like `base_fare`.
# MAGIC - **Magic commands** switch how a cell runs (`%sql`, `%sh`, `%fs`, …).
# MAGIC - **`%fs`** is a quick look; **`dbutils.fs`** gives you a Python result
# MAGIC   you can count and loop over.
# MAGIC
# MAGIC Next up: `04 - Your First DataFrame` — building and inspecting a small
# MAGIC rideshare DataFrame with `show()`, `display()`, and `printSchema()`.
