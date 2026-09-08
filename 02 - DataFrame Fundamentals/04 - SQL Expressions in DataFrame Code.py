# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - SQL Expressions in DataFrame Code
# MAGIC
# MAGIC Express the same column logic as SQL strings inside DataFrame code.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Use `F.expr` and `selectExpr`, including SQL `CASE WHEN`
# MAGIC - Recognize misspelled-column `AnalysisException` across styles
# MAGIC - Distinguish Python `SyntaxError` from Spark SQL parse errors and choose a
# MAGIC   consistent style
# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup DataFrame for SQL expression examples
# MAGIC
# MAGIC Notebook 03 built derived columns with the Column API — `F.col`,
# MAGIC `F.when`, `F.lit`, and related helpers. The same calculations can be
# MAGIC written as **SQL expression strings** inside DataFrame methods (`F.expr`,
# MAGIC `selectExpr`). For example, a distance band you built with `F.when` can
# MAGIC be expressed as SQL `CASE WHEN`.
# MAGIC
# MAGIC Create one small DataFrame to reuse across every example. SQL strings
# MAGIC here still start from a Python DataFrame variable — this notebook does
# MAGIC not use `%sql` cells or temporary views (those come in notebook 06).

# COMMAND ----------

from decimal import Decimal

from pyspark.sql import functions as F

rows = [
    (1001, "Standard", 138, Decimal("12.40"), 18),
    (1002, "Shared", 74, Decimal("3.10"), 9),
    (1003, "Premium", 231, Decimal("22.70"), 35),
    (1004, "Standard", 100, Decimal("5.60"), 14),
    (1005, "Shared", 74, Decimal("2.20"), 7),
]

schema_ddl = (
    "trip_id bigint, service_type string, pickup_location_id int, "
    "trip_distance_miles decimal(8,2), ride_duration_mins int"
)

df = spark.createDataFrame(rows, schema_ddl)  # pyright: ignore[reportUndefinedVariable]  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC Confirm the sample rows before SQL expression examples — the same habit
# MAGIC as inspection in the previous notebook.

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build a Column with `F.expr`
# MAGIC
# MAGIC **`F.expr(sql_text)`** parses a SQL expression string and returns a
# MAGIC **Column** — not a DataFrame.
# MAGIC
# MAGIC **Business question:** Dispatch review needs average trip speed in miles
# MAGIC per hour.
# MAGIC
# MAGIC Store the SQL in a variable, then reuse that one definition later in this
# MAGIC notebook — no copies that can drift apart in a pipeline.

# COMMAND ----------

mph_sql = "round(trip_distance_miles / (ride_duration_mins / 60.0), 1) AS mph"

df.select(
    "trip_id",
    F.expr(mph_sql),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply several SQL strings with `selectExpr`
# MAGIC
# MAGIC **`selectExpr(*sql_strings)`** applies one or more SQL expression strings
# MAGIC and returns a new DataFrame. Each string can pass through an existing
# MAGIC column or define a calculated column with `AS`.
# MAGIC
# MAGIC The next example keeps `trip_id`, uppercases `service_type`, and reuses
# MAGIC `mph_sql`. The previous section passed `mph_sql` to `F.expr` inside
# MAGIC `select`; here the same string goes directly to `selectExpr`.

# COMMAND ----------

df.selectExpr(
    "trip_id",
    "upper(service_type) AS service_type_upper",
    mph_sql,
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC > **Good to know:** Plain `select("col_name")` and `select(F.col(...))` do
# MAGIC > **not** parse SQL expression strings — they treat the string as a literal
# MAGIC > column name lookup against the schema.
# MAGIC >
# MAGIC > `df.select("upper(service_type)")` does **not** call the `upper()`
# MAGIC > function. It looks for a column literally named `upper(service_type)` in
# MAGIC > the schema and fails with `AnalysisException`.
# MAGIC >
# MAGIC > `df.select(F.col("trip_distance_miles * 2"))` does **not** perform
# MAGIC > multiplication. It looks for a column literally named
# MAGIC > `trip_distance_miles * 2` — which does not exist — and fails the same way.
# MAGIC >
# MAGIC > **`F.col("name")` references one column.** In notebook 03, operators and
# MAGIC > PySpark functions apply to the Column object — for example
# MAGIC > `F.upper(F.col("service_type"))` or `F.col("trip_distance_miles") * 2`.
# MAGIC > They do not go inside the column-name string.
# MAGIC >
# MAGIC > To parse SQL **inside** a string, use `F.expr` or `selectExpr`:
# MAGIC >
# MAGIC > ```python
# MAGIC > # ✗ literal column names — AnalysisException
# MAGIC > df.select("upper(service_type)")
# MAGIC > df.select(F.col("upper(service_type)"))
# MAGIC > df.select(F.col("trip_distance_miles * 2"))
# MAGIC >
# MAGIC > # ✓ Column API (notebook 03)
# MAGIC > df.select(F.upper(F.col("service_type")))
# MAGIC > df.select(F.col("trip_distance_miles") * 2)
# MAGIC >
# MAGIC > # ✓ SQL strings parsed by Spark
# MAGIC > df.select(F.expr("upper(service_type)"))
# MAGIC > df.selectExpr("upper(service_type) AS service_type_upper")
# MAGIC > df.selectExpr("trip_distance_miles * 2 AS doubled")
# MAGIC > ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conditional logic with SQL `CASE WHEN`
# MAGIC
# MAGIC **Business question:** Trip reporting needs the same distance-band labels
# MAGIC from notebook 03 — `short`, `medium`, and `long`.
# MAGIC
# MAGIC Notebook 03 used `F.when` / `otherwise`. Here, the same thresholds appear
# MAGIC as SQL `CASE WHEN`. Both forms are common in production pipelines.

# COMMAND ----------

distance_band_sql = """
CASE
    WHEN trip_distance_miles < 5 THEN 'short'
    WHEN trip_distance_miles <= 15 THEN 'medium'
    ELSE 'long'
END
"""

labelled = df.withColumn("distance_band", F.expr(distance_band_sql))
labelled.select("trip_id", "trip_distance_miles", "distance_band").show()

# COMMAND ----------

# MAGIC %md
# MAGIC **`selectExpr`** can apply the same `CASE` and name the output column in
# MAGIC one step with `AS distance_band`.

# COMMAND ----------

labelled_with_select_expr = df.selectExpr(
    "trip_id",
    "trip_distance_miles",
    distance_band_sql + " AS distance_band",
)

labelled_with_select_expr.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compare misspelled column names
# MAGIC
# MAGIC Both `F.col` and `F.expr` must resolve column names against the DataFrame
# MAGIC schema. A typo in either style raises **`AnalysisException`** when Spark
# MAGIC analyzes the query.

# COMMAND ----------

try:
    df.select(F.col("trip_distnace_miles")).show()
except Exception as e:
    print(f"F.col typo — {type(e).__name__}")

# COMMAND ----------

try:
    df.select(F.expr("trip_distnace_miles")).show()
except Exception as e:
    print(f"F.expr typo — {type(e).__name__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Python syntax errors vs Spark SQL parse errors
# MAGIC
# MAGIC Python validates the **Python cell** before it runs. Spark validates **SQL
# MAGIC strings** when PySpark parses them. That controls whether a `try` / `except`
# MAGIC in the same cell can catch the error.
# MAGIC
# MAGIC Three cases — do not mix them up:
# MAGIC
# MAGIC - **Case A** — mistake in the **cell body** (Python syntax). Python fails
# MAGIC   before the cell runs; **`try` / `except` cannot help**.
# MAGIC - **Case B** — mistake in a **Python string** you parse with `compile()`.
# MAGIC   The cell body is valid; **`try` / `except` can catch `SyntaxError`** —
# MAGIC   a different pattern from Case A.
# MAGIC - **Case C** — mistake in a **SQL string** passed to Spark. The cell body
# MAGIC   is valid; **`try` / `except` can catch Spark's parse error** (see below).
# MAGIC
# MAGIC ### Case A — invalid Python in the cell body
# MAGIC
# MAGIC If you paste the snippet below **directly into a code cell** (not inside a
# MAGIC string), Python fails while parsing the cell:
# MAGIC
# MAGIC 1. Python finds the missing `)`.
# MAGIC 2. Python raises **`SyntaxError`** and does not execute the cell.
# MAGIC 3. The `try` block never starts — **`except` cannot catch it**.
# MAGIC
# MAGIC Fix the Python syntax in the cell itself first — no Spark work runs. The
# MAGIC fenced snippet is shown for reading only; it is **not executed** in this
# MAGIC notebook (a broken cell would block the rest of the lesson).

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC # Case A — broken cell body (do not run as-is in this notebook)
# MAGIC try:
# MAGIC     df.select(F.col("trip_distance_miles").cast("double")
# MAGIC except SyntaxError as e:
# MAGIC     print("never reached")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Case B — invalid Python inside a string (different from Case A)
# MAGIC
# MAGIC The next cell holds broken Python **inside a string** and passes it to
# MAGIC `compile()`. That is valid Python in the cell body, so the cell runs and
# MAGIC **`except` can catch `SyntaxError`**. This does **not** mean Case A is
# MAGIC catchable — it means inspecting or compiling string-held code is a separate
# MAGIC pattern from writing broken syntax directly in the cell.

# COMMAND ----------

bad_python = 'df.select(F.col("trip_distance_miles").cast("double")'
try:
    compile(bad_python, "<broken-snippet>", "exec")
except SyntaxError:
    print("Case B — SyntaxError caught (invalid Python was inside a string)")
print("Case A — same missing ')' in the cell body would fail before try runs.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Case C — invalid SQL inside a string (Spark parse error)
# MAGIC
# MAGIC The next cell is valid Python. The malformed SQL lives inside a string passed
# MAGIC to `selectExpr`, so Python starts the cell, Spark parses the SQL, and Spark
# MAGIC raises **`ParseException`**. Python can catch that inside `try`.

# COMMAND ----------

try:
    df.selectExpr("CAST(trip_distance_miles AS DOUBLE").show()
except Exception as e:
    print(f"Case C — Spark SQL raised {type(e).__name__} — caught at Python runtime")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Choose the clearer expression form
# MAGIC
# MAGIC SQL expression strings and Column expressions can describe the same column
# MAGIC logic. Neither style is always better — you will see both in production
# MAGIC code.
# MAGIC
# MAGIC - Use a **SQL expression string** when the calculation is easier to read
# MAGIC   as SQL.
# MAGIC - Use **`F.col` / `F.when`** when the calculation is easier to build with
# MAGIC   PySpark functions.
# MAGIC
# MAGIC Define a repeated calculation once, store it in a variable, and reuse it.
# MAGIC Keep related calculations in a consistent style so the pipeline stays
# MAGIC readable.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reuse SQL expressions in an operations summary
# MAGIC
# MAGIC **Business question:** A published trip summary needs formatted service
# MAGIC names, average speed, and a distance band.
# MAGIC
# MAGIC Reuse `distance_band_sql` with `F.expr` / `withColumn` and `mph_sql` with
# MAGIC `selectExpr` in one chain.

# COMMAND ----------

operations_summary = df.withColumn("distance_band", F.expr(distance_band_sql)).selectExpr(
    "trip_id",
    "upper(service_type) AS service_type_upper",
    mph_sql,
    "distance_band",
)

operations_summary.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC Use a second small rideshare-style DataFrame named `my_df` and complete:
# MAGIC
# MAGIC 1. Create `my_df` with explicit `trip`-aligned column names and types.
# MAGIC 2. Store one SQL expression string in a variable (for example average
# MAGIC    speed or a `CASE WHEN` distance band).
# MAGIC 3. Add a derived column with `F.expr` or `selectExpr` using that string.
# MAGIC 4. Show the result with the derived column visible.
# MAGIC
# MAGIC Keep the DataFrame tiny (a handful of rows).

# COMMAND ----------

# Your code here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Recap this notebook's SQL-in-DataFrame path:
# MAGIC
# MAGIC - **`F.expr`** — SQL string → Column; use inside `select`, `withColumn`, etc.
# MAGIC - **`selectExpr`** — SQL strings → new DataFrame in one call
# MAGIC - **`CASE WHEN`** — conditional columns in SQL (parallel to `F.when`)
# MAGIC - **`AnalysisException`** — unresolved column names in either style
# MAGIC - **`SyntaxError` vs `ParseException`** — Python cell errors vs bad SQL
# MAGIC   strings at Spark parse time
# MAGIC - **Reuse named SQL strings**; choose SQL or Column style for clarity
# MAGIC
# MAGIC Next up: `05 - Filtering Rows` — keep only the rows you need with row
# MAGIC conditions (Column operators and SQL predicate strings).
