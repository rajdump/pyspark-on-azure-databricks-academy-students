# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Selecting and Transforming Columns
# MAGIC
# MAGIC Reshape columns with the DataFrame API — the transforms later notebooks reuse.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Select, add, rename, recalculate, and drop columns
# MAGIC - Build Column expressions with `F.col`, `alias`, light `cast`, `F.lit`, and
# MAGIC   `F.when` / `otherwise`
# MAGIC - Choose `select` vs `withColumn` and chain into a small ops-style output
# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup DataFrame for column transforms
# MAGIC
# MAGIC Most batch work reshapes columns: keep what downstream needs, compute
# MAGIC derived fields, rename for clarity, drop the rest.
# MAGIC
# MAGIC Create one small DataFrame to reuse across every example. In production,
# MAGIC you reshape into a **new** DataFrame and leave the source unchanged until
# MAGIC you deliberately write results — that immutability habit prevents silent
# MAGIC overwrites in long pipelines.

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
# MAGIC Confirm the sample rows before reshaping — the same habit as inspection
# MAGIC in the previous notebook.

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Project and reorder with `select`
# MAGIC
# MAGIC **Business question:** A first dashboard needs trip identity, service type,
# MAGIC and distance — not every pickup zone column.
# MAGIC
# MAGIC `select` returns a new DataFrame with only the columns you name, in the
# MAGIC order you list them. The simplest form uses column names as strings.

# COMMAND ----------

df.select("trip_id", "service_type", "trip_distance_miles").show()

# COMMAND ----------

# MAGIC %md
# MAGIC `select` also **reorders** columns. Downstream tools (BI exports, CSV
# MAGIC writers) often expect a stable column order — define it explicitly rather
# MAGIC than relying on source layout.

# COMMAND ----------

df.select("service_type", "trip_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## `select` returns a new DataFrame
# MAGIC
# MAGIC Spark DataFrames are **immutable**. `select` does not change `df`; it
# MAGIC returns a new DataFrame. Assign the result when you need to keep it.

# COMMAND ----------

selected = df.select("trip_id", "service_type")
print("new DataFrame columns:", selected.columns)
print("original df columns:  ", df.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Column-name strings vs `F.col`
# MAGIC
# MAGIC Use a **column-name string** when you only need the column unchanged.
# MAGIC
# MAGIC Use **`F.col("name")`** when you need a **Column expression** — alias,
# MAGIC arithmetic, cast, comparison, or conditional logic.
# MAGIC
# MAGIC The next example renames `trip_distance_miles` to `distance_mi`, so it
# MAGIC needs `F.col`, not a plain string.

# COMMAND ----------

df.select(
    "trip_id",
    # A column-name string cannot apply an alias inside select().
    # "trip_distance_miles as distance_mi",
    F.col("trip_distance_miles").alias("distance_mi"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Business question:** Regional teams need trip distance reported in
# MAGIC kilometres alongside miles.
# MAGIC
# MAGIC `F.col` also supports calculations. Define the expression once in a
# MAGIC variable, then reuse it — one definition, no copies that can drift apart
# MAGIC in a pipeline.

# COMMAND ----------

km_expr = F.col("trip_distance_miles") * 1.60934

df.select(
    "trip_id",
    "trip_distance_miles",
    # A column-name string cannot perform arithmetic.
    # "trip_distance_miles" * 1.60934,
    km_expr.alias("trip_distance_km"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Light `cast` and constants with `F.lit`
# MAGIC
# MAGIC **Business question:** A BI export needs trip IDs as text, and downstream
# MAGIC audits need every row tagged with a source system.
# MAGIC
# MAGIC **`.cast("type")`** converts a column's type. Cast with intent — some
# MAGIC conversions lose precision. Deeper casting rules and failure modes come in
# MAGIC Module 3; here, cast only when the target type is clear (for example
# MAGIC formatting an ID as text for a dashboard export).
# MAGIC
# MAGIC **`F.lit(value)`** wraps a plain Python value as a Column — the same
# MAGIC constant on every row (for example a source-system tag). You do not need
# MAGIC `F.lit` for a comparison like `F.col("x") > 10`; Spark wraps the literal.

# COMMAND ----------

df.select(
    F.col("trip_id").cast("string").alias("trip_id_str"),
    "trip_distance_miles",
).printSchema()

# COMMAND ----------

df.select("trip_id", F.lit("mobile_app").alias("source_system")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conditional columns: `F.when` / `otherwise`
# MAGIC
# MAGIC **Business question:** Trip reporting and dashboards need distance-band
# MAGIC labels — `short`, `medium`, and `long`.
# MAGIC
# MAGIC **`F.when(condition, value)`** is column-level if/else logic. Chain more
# MAGIC `.when(...)`, then finish with `.otherwise(...)`. Without `otherwise`,
# MAGIC unmatched rows get `NULL`.
# MAGIC
# MAGIC Store the rule once and reuse it — the same pattern you will see again
# MAGIC with SQL expression strings in the next notebook.

# COMMAND ----------

distance_band_expr = (
    F.when(F.col("trip_distance_miles") < 5, "short")
    .when(F.col("trip_distance_miles") <= 15, "medium")
    .otherwise("long")
)

df.select(
    "trip_id",
    "trip_distance_miles",
    distance_band_expr.alias("distance_band"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add or replace columns: `withColumn`
# MAGIC
# MAGIC **`withColumn(name, expression)`** returns a new DataFrame:
# MAGIC
# MAGIC - If `name` does not exist → adds a derived column.
# MAGIC - If `name` already exists → recalculates that column from the expression.
# MAGIC
# MAGIC The first example adds `trip_distance_km` by reusing `km_expr`.

# COMMAND ----------

df_km = df.withColumn("trip_distance_km", km_expr)
df_km.select("trip_id", "trip_distance_miles", "trip_distance_km").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Choose `select` vs `withColumn` when adding a column
# MAGIC
# MAGIC The cell above added `trip_distance_km` with `withColumn`. The same
# MAGIC result can use `select("*", expression)` — keep every existing column and
# MAGIC append the derived one.

# COMMAND ----------

df.select("*", km_expr.alias("trip_distance_km")).select(
    "trip_id", "trip_distance_miles", "trip_distance_km"
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC For one new column, either style is fine — pick the one that reads more
# MAGIC clearly in your pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC A `withColumn` expression can also produce a true/false flag — useful for
# MAGIC downstream filters or quality checks.

# COMMAND ----------

df.withColumn("is_long_trip", F.col("trip_distance_miles") > 15).select(
    "trip_id", "trip_distance_miles", "is_long_trip"
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC When the column name already exists, `withColumn` **replaces** values from
# MAGIC the expression. Here, `Shared` becomes `Pool`; other service types stay the
# MAGIC same. The original `df` is still unchanged.

# COMMAND ----------

service_type_expr = F.when(F.col("service_type") == "Shared", F.lit("Pool")).otherwise(
    F.col("service_type")
)

df.withColumn("service_type", service_type_expr).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recalculating with `select` can duplicate column names
# MAGIC
# MAGIC `withColumn` replaces a column by name. **`select("*", expr.alias("service_type"))`**
# MAGIC instead keeps the original **and** adds a second `service_type` — later
# MAGIC references become ambiguous. With `select`, list every column you want and
# MAGIC put the recalculated expression in the correct position.

# COMMAND ----------

df.select("*", service_type_expr.alias("service_type")).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC > **Good to know:** `select` and `withColumn` both accept Column
# MAGIC > expressions and return a new DataFrame. Use `withColumn` when you mean
# MAGIC > to replace an existing column by name; use `select` when you are
# MAGIC > projecting the full output shape explicitly.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Several columns at once: `withColumns`
# MAGIC
# MAGIC Prefer **`withColumns({name: expr, ...})`** when you add several derived
# MAGIC columns in one step. Each separate `withColumn` call adds another
# MAGIC projection to the logical plan; repeating that many times (especially in
# MAGIC a loop) can bloat the plan.

# COMMAND ----------

df.withColumns(
    {
        "trip_distance_km": km_expr,
        "is_long_trip": F.col("trip_distance_miles") > 15,
    }
).select("trip_id", "trip_distance_km", "is_long_trip").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rename columns
# MAGIC
# MAGIC **`.alias()`** renames inside a `select`. To rename while keeping all other
# MAGIC columns, use **`withColumnRenamed(old, new)`** or rename several at once
# MAGIC with **`withColumnsRenamed({old: new, ...})`**.

# COMMAND ----------

df.withColumnRenamed("ride_duration_mins", "duration_mins").show()

# COMMAND ----------

df.withColumnsRenamed(
    {
        "service_type": "ride_type",
        "trip_distance_miles": "distance_miles",
    }
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Gotcha:** `withColumnRenamed` is a silent no-op when the old name does not
# MAGIC match a real column. A typo means the rename never happened — check
# MAGIC `printSchema()` when output columns look wrong.

# COMMAND ----------

typo_rename = df.withColumnRenamed("ride_duration_min", "duration_mins")
print("columns after typo rename:", typo_rename.columns)
print("unchanged from original? ", typo_rename.columns == df.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove columns with `drop`
# MAGIC
# MAGIC **`drop`** returns a new DataFrame without the named columns. In production,
# MAGIC drop temporary or internal columns before writing so consumers do not
# MAGIC inherit fields meant only for intermediate checks.
# MAGIC
# MAGIC If a column name is missing, `drop` does not raise an error.

# COMMAND ----------

df_km.drop("trip_distance_miles", "pickup_location_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chain transforms into an operations-style output
# MAGIC
# MAGIC **Business question:** A mobile-app dashboard needs trip data prepared
# MAGIC with:
# MAGIC
# MAGIC - trip IDs as text
# MAGIC - standardized service names (`Shared` → `Pool`)
# MAGIC - kilometre distances and distance bands
# MAGIC - a source tag
# MAGIC - only the columns the dashboard reads
# MAGIC
# MAGIC Chain `select`, `withColumns`, `withColumn`, rename, and `drop` into one
# MAGIC readable pipeline. Reuse the expressions defined earlier so the rules
# MAGIC stay consistent.

# COMMAND ----------

operations_dashboard = (
    df.select(
        F.col("trip_id").cast("string").alias("trip_id"),
        "service_type",
        "trip_distance_miles",
        "ride_duration_mins",
    )
    .withColumns(
        {
            "trip_distance_km": km_expr,
            "distance_band": distance_band_expr,
            "source_system": F.lit("mobile_app"),
        }
    )
    .withColumn("service_type", service_type_expr)
    .withColumnRenamed("ride_duration_mins", "duration_mins")
    .drop("trip_distance_miles")
)

operations_dashboard.show()
operations_dashboard.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Confirm the source DataFrame is unchanged — the chain above built a new
# MAGIC output shape without mutating `df`.

# COMMAND ----------

print("operations_dashboard columns:", operations_dashboard.columns)
print("original df columns:         ", df.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC Use a second small rideshare-style DataFrame named `my_df` and complete:
# MAGIC
# MAGIC 1. Project and reorder a few columns with `select`.
# MAGIC 2. Add at least one derived column with `withColumn` or `withColumns`
# MAGIC    using `F.col` (and `F.when` or `F.lit` if useful).
# MAGIC 3. Rename one column and drop one column you no longer need.
# MAGIC 4. Confirm the original `my_df` columns are unchanged after your chain.
# MAGIC
# MAGIC Keep the DataFrame tiny (a handful of rows). Use explicit column names
# MAGIC from the `trip` table where possible.

# COMMAND ----------

# Your code here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Recap this notebook's reshape path:
# MAGIC
# MAGIC - **`select`** — project, reorder; returns a new DataFrame; does not
# MAGIC   mutate the input
# MAGIC - **Column-name strings vs `F.col`** — strings for unchanged columns;
# MAGIC   `F.col` for expressions (`alias`, arithmetic, light `cast`, `F.lit`,
# MAGIC   `F.when`)
# MAGIC - **`select` vs `withColumn`** — either can add a column; `withColumn`
# MAGIC   replaces by name when recalculating
# MAGIC - **`withColumn` / `withColumns`** — add or recalculate columns; prefer
# MAGIC   `withColumns` for several additions at once
# MAGIC - **`withColumnRenamed` / `withColumnsRenamed`** — rename without listing
# MAGIC   every column in `select`
# MAGIC - **`drop`** — remove columns from the output shape
# MAGIC - **Chain transforms** — reuse named expressions; build a clear
# MAGIC   downstream-ready DataFrame
# MAGIC
# MAGIC Next up: `04 - SQL Expressions in DataFrame Code` — write the same kind of
# MAGIC column logic as SQL expression strings (`F.expr`, `selectExpr`).
