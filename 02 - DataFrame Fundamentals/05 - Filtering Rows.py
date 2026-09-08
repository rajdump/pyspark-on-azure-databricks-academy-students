# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Filtering Rows
# MAGIC
# MAGIC Keep rows with `filter` / `where`, including intro NULL and blank traps.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Filter with Column ops and SQL strings; combine with `AND` vs `&`
# MAGIC - Use `|`, `~`, `isin`, `between`, `like`
# MAGIC - Apply intro NULL checks (`isNull` / `isNotNull`); empty string ≠ NULL
# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup DataFrame for filter examples
# MAGIC
# MAGIC Notebook 04 built column logic with SQL expression strings. The same
# MAGIC predicates can appear inside **`filter`** / **`where`** to keep only matching
# MAGIC rows — or you can combine **`F.col`** comparisons with `&`, `|`, and `~`.
# MAGIC
# MAGIC Create one small DataFrame with deliberate quality issues to filter against:
# MAGIC
# MAGIC - one `NULL` `service_type`
# MAGIC - one empty-string `service_type`
# MAGIC - one `NULL` `ride_duration_mins`
# MAGIC - one negative `trip_distance_miles`
# MAGIC
# MAGIC Deeper NULL semantics and full quality pipelines stay for Module 3;
# MAGIC here you learn the intro checks only.

# COMMAND ----------

from decimal import Decimal

from pyspark.sql import functions as F

rows = [
    (1001, "Standard", 138, Decimal("12.40"), 18),
    (1002, "Shared", 74, Decimal("3.10"), 9),
    (1003, "Premium", 231, Decimal("22.70"), 35),
    (1004, "Standard", 100, Decimal("5.60"), 14),
    (1005, "Shared", 74, Decimal("2.20"), 7),
    (1006, "Premium", 138, Decimal("18.00"), None),
    (1007, None, 161, Decimal("8.30"), 19),
    (1008, "", 90, Decimal("4.50"), 12),
    (1009, "Standard", 100, Decimal("-1.00"), 10),
]

schema_ddl = (
    "trip_id bigint, service_type string, pickup_location_id int, "
    "trip_distance_miles decimal(8,2), ride_duration_mins int"
)

df = spark.createDataFrame(rows, schema_ddl)  # pyright: ignore[reportUndefinedVariable]  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC Confirm the sample rows before filtering — the same habit as inspection
# MAGIC in the previous notebook.

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter rows with `filter` / `where`
# MAGIC
# MAGIC **`filter(condition)`** returns a new DataFrame whose plan includes the row
# MAGIC condition. It does not change the original DataFrame. **`where`** is an alias
# MAGIC for **`filter`** — same method, different name.
# MAGIC
# MAGIC **Business question:** Dispatch planning needs trips longer than ten miles.

# COMMAND ----------

df.filter(F.col("trip_distance_miles") > 10).show()

# COMMAND ----------

# MAGIC %md
# MAGIC The same row condition with **`where`**:

# COMMAND ----------

df.where(F.col("service_type") == "Shared").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine conditions: Column operators vs SQL strings
# MAGIC
# MAGIC **Business question:** A service-quality report needs Standard trips
# MAGIC longer than six miles.
# MAGIC
# MAGIC **`AND`** in a SQL predicate string and **`&`** between Column expressions
# MAGIC perform the same logical AND — but they belong to different styles. Wrap each
# MAGIC Column comparison in parentheses when you use **`&`**.

# COMMAND ----------

df.filter("service_type = 'Standard' AND trip_distance_miles > 6").show()

# COMMAND ----------

# MAGIC %md
# MAGIC The same predicate as a SQL string passed through **`F.expr`**:

# COMMAND ----------

standard_long_sql = "service_type = 'Standard' AND trip_distance_miles > 6"

df.filter(F.expr(standard_long_sql)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC The Column-expression form with **`&`**:

# COMMAND ----------

df.filter((F.col("service_type") == "Standard") & (F.col("trip_distance_miles") > 6)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Do not use Python `and` with Column conditions
# MAGIC
# MAGIC Python **`and`**, **`or`**, and **`not`** expect plain Python booleans. A
# MAGIC Column condition is not a boolean — PySpark raises an error when Python tries
# MAGIC to treat it as one. The next cell demonstrates **`and`**; **`or`** and **`not`**
# MAGIC fail the same way. Use **`&`**, **`|`**, and **`~`** between Column
# MAGIC conditions instead.

# COMMAND ----------

try:
    df.filter((F.col("service_type") == "Standard") and (F.col("trip_distance_miles") > 6)).show()
except Exception as e:
    print(f"Python and on Columns — {type(e).__name__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## More filter helpers: `|`, `~`, `isin`, `between`, `like`
# MAGIC
# MAGIC **Business question:** Peak-hour analysis needs Premium trips or any trip
# MAGIC longer than twenty miles.
# MAGIC
# MAGIC Use **`|`** when a row can match either condition (wrap each side in
# MAGIC parentheses).

# COMMAND ----------

df.filter((F.col("service_type") == "Premium") | (F.col("trip_distance_miles") > 20)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Use **`~`** to reverse a Column condition. The next filter excludes Shared
# MAGIC trips. Rows whose `service_type` is **`NULL`** are also excluded — a
# MAGIC comparison with **`NULL`** is unknown, not true (see the NULL section below).

# COMMAND ----------

df.filter(~(F.col("service_type") == "Shared")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **`isin(...)`** checks whether a value matches one of several options — cleaner
# MAGIC than chaining many **`==`** comparisons with **`|`**.

# COMMAND ----------

df.filter(F.col("service_type").isin("Standard", "Premium")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **`between(lower, upper)`** is an inclusive range — values equal to either
# MAGIC boundary match.

# COMMAND ----------

df.filter(F.col("trip_distance_miles").between(3, 12)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **`like(pattern)`** is SQL-style pattern matching on strings. The first
# MAGIC example uses **`'%'`** — it matches any non-**`NULL`** string, including an
# MAGIC **empty string** (trip **`1008`**). Trip **`1007`** (`NULL`) still will not
# MAGIC appear.

# COMMAND ----------

df.filter(F.col("service_type").like("%")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **`S%`** means “starts with **`S`**” — **`%`** is a wildcard for the rest of
# MAGIC the name. Trip **`1008`** (empty string) will not match; trip **`1007`**
# MAGIC (`NULL`) still will not appear.

# COMMAND ----------

df.filter(F.col("service_type").like("S%")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC > **Good to know:** Compare the two **`like`** results above — **`LIKE`** never
# MAGIC > matches **`NULL`** (trip **`1007`** missing from both). An **empty string**
# MAGIC > can match **`'%'`** but not **`S%`** (trip **`1008`**).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Intro NULL: `isNull` / `isNotNull`
# MAGIC
# MAGIC **Business question:** A data-quality review needs rows where
# MAGIC `service_type` was never captured.
# MAGIC
# MAGIC Comparing a column to **`None`** with **`==`** or **`!=`** does **not** find
# MAGIC NULLs in SQL semantics — the result is **unknown**, and a filter keeps only
# MAGIC rows where the condition is **true**. The next two cells print a **count of
# MAGIC zero**; use **`isNull()`** / **`isNotNull()`** instead.

# COMMAND ----------

print("== None row count:", df.filter(F.col("service_type") == None).count())  # noqa: E711

# COMMAND ----------

print("!= None row count:", df.filter(F.col("service_type") != None).count())  # noqa: E711

# COMMAND ----------

df.filter(F.col("service_type").isNull()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **`isNotNull()`** keeps rows where the column has a value (including an empty
# MAGIC string — empty is not NULL).

# COMMAND ----------

df.filter(F.col("ride_duration_mins").isNotNull()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC > **Good to know:** Three-valued logic (`TRUE`, `FALSE`, `UNKNOWN`) and
# MAGIC > NULL-safe predicates are covered in depth in Module 3. Here, reach
# MAGIC > for **`isNull()`** / **`isNotNull()`** whenever the requirement mentions
# MAGIC > missing values.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Empty string is not NULL
# MAGIC
# MAGIC **Business question:** A validation report needs rows where `service_type`
# MAGIC was submitted as blank — not missing, but empty.
# MAGIC
# MAGIC Trip **`1008`** has an empty string for `service_type`. **`isNull()`** does not
# MAGIC find it — check **`""`** separately when blank and missing mean different
# MAGIC things.

# COMMAND ----------

df.filter(F.col("service_type") == "").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Trip **`1007`** has a **`NULL`** `service_type`. It does not appear in Shared,
# MAGIC empty-string, or **`~Shared`** results above because comparisons involving
# MAGIC **`NULL`** are unknown. When the requirement is truly “missing value”, use
# MAGIC **`isNull()`**, not **`== ""`**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chain a filter into an operations-style output
# MAGIC
# MAGIC **Business question:** A downstream dashboard needs trips with a known,
# MAGIC non-empty service type and positive distance.
# MAGIC
# MAGIC Define one reusable filter expression, then filter, add columns, and project
# MAGIC the shape the dashboard reads. Reuse named expressions from earlier notebooks
# MAGIC where they fit — one definition, no copies that can drift apart.

# COMMAND ----------

is_usable = (
    F.col("service_type").isNotNull()
    & (F.col("service_type") != "")
    & F.col("trip_distance_miles").isNotNull()
    & (F.col("trip_distance_miles") > 0)
)

distance_band_expr = (
    F.when(F.col("trip_distance_miles") < 5, "short")
    .when(F.col("trip_distance_miles") <= 15, "medium")
    .otherwise("long")
)

usable_trips = (
    df.filter(is_usable)
    .withColumn("distance_band", distance_band_expr)
    .withColumn("source_system", F.lit("mobile_app"))
    .select(
        "trip_id",
        "service_type",
        "trip_distance_miles",
        "distance_band",
        "source_system",
    )
)

usable_trips.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Confirm the source DataFrame is unchanged — filtering built a new output
# MAGIC without mutating `df`.

# COMMAND ----------

print("usable_trips row count:", usable_trips.count())
print("original df row count: ", df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC Use a second small rideshare-style DataFrame named `my_df` and complete:
# MAGIC
# MAGIC 1. Create `my_df` with explicit `trip`-aligned column names and types
# MAGIC    (include at least one `NULL` or empty string if useful).
# MAGIC 2. Store one reusable filter condition in a variable (for example
# MAGIC    `is_usable` above, or a distance / service-type rule).
# MAGIC 3. Filter with `filter` or `where` using that condition.
# MAGIC 4. Show the filtered result.
# MAGIC
# MAGIC Keep the DataFrame tiny (a handful of rows).

# COMMAND ----------

# Your code here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Recap this notebook's filter path:
# MAGIC
# MAGIC - **`filter` / `where`** — same method; returns a new DataFrame; does not
# MAGIC   mutate the input
# MAGIC - **Combine conditions** — SQL `AND` in strings or **`F.expr`**; Column
# MAGIC   **`&`** with parentheses; not Python **`and`**
# MAGIC - **`|`**, **`~`**, **`isin`**, **`like`**, **`between`** — common row
# MAGIC   helpers on Column expressions
# MAGIC - **Intro NULL** — **`== None`** / **`!= None`** do not find NULLs; use
# MAGIC   **`isNull()`** / **`isNotNull()`**
# MAGIC - **Empty string** — not NULL; check **`== ""`** separately when blank
# MAGIC   matters
# MAGIC - **Reusable filter** — name one condition; chain filter with transforms
# MAGIC   for downstream output
# MAGIC
# MAGIC Next up: `06 - Querying DataFrames with SQL` — give a DataFrame a temporary
# MAGIC SQL name and query it with `%sql` and `spark.sql`.
