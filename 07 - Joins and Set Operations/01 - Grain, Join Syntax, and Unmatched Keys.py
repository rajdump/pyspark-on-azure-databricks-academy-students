# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Grain, Join Syntax, and Unmatched Keys
# MAGIC
# MAGIC Grain, cardinality, and join syntax — skill-building only (no write).
# MAGIC
# MAGIC Landing `trip`, `trip_time` (+ constructed frames). No `payment`.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Define grain and cardinality and predict join row counts before running
# MAGIC - Write equi-joins in string, list, and Boolean form
# COMMAND ----------

# MAGIC %md
# MAGIC ## The problem: wrong joins cost more than syntax errors
# MAGIC
# MAGIC A syntax error stops your code immediately — you fix it and move on. A wrong
# MAGIC join decision **silently produces wrong data** that passes all downstream code
# MAGIC without a single error. You only discover it when a report is wrong or a
# MAGIC stakeholder notices impossible numbers.
# MAGIC
# MAGIC **Course note:** landing **`payment`** is **one row per `trip_id`** (wide fare
# MAGIC columns). The example below uses **line-level** rows for teaching only — same
# MAGIC pattern as constructed **`trip_charges`** in Section 3, not landing **`payment`**.
# MAGIC
# MAGIC **Trip level — 1 row per `trip_id`**
# MAGIC
# MAGIC | trip_id | service_type |
# MAGIC |---|---|
# MAGIC | 1 | Standard |
# MAGIC | 2 | Premium |
# MAGIC
# MAGIC **Line-level billing (teaching sketch) — 3 rows per `trip_id`**
# MAGIC
# MAGIC | trip_id | charge_type | amount |
# MAGIC |---|---|---|
# MAGIC | 1 | base_fare | 8.00 |
# MAGIC | 1 | surge | 3.00 |
# MAGIC | 1 | tip | 1.50 |
# MAGIC | 2 | base_fare | 18.00 |
# MAGIC | 2 | surge | 5.00 |
# MAGIC | 2 | tip | 2.00 |
# MAGIC
# MAGIC Inner join on **`trip_id` only** → **6 rows** (not 2). No error. No warning.
# MAGIC Trip columns repeat on every charge line — so **`count(*)`** and any metric
# MAGIC computed on trip grain without deduping can be wrong.
# MAGIC
# MAGIC Why? The left table has **1 row per trip**, the right has **3 rows per trip**.
# MAGIC Spark matched every left row to every right row with the same key. That's a
# MAGIC **1:M fanout** — invisible until the damage is done.
# MAGIC
# MAGIC Section 3.2 shows a related case: `trip_charges` ↔ `rate_card` — **both**
# MAGIC sides have multiple rows per trip (**M:M** on `trip_id` → 12 rows). Same
# MAGIC lesson: joining on too few columns produces wrong results.
# MAGIC
# MAGIC ## What this notebook teaches
# MAGIC
# MAGIC | Section | Concept | Why it matters |
# MAGIC |---|---|---|
# MAGIC | 1. Grain | What one row represents | Detect duplicates before joining |
# MAGIC | 2. Cardinality | 1:1, 1:M, M:1, M:M labels | Predict output row count |
# MAGIC | 3. Join syntax | String, List, Boolean forms | Write correct PySpark join conditions |
# MAGIC | Exercise | Unmatched keys — join types | Control which rows survive |
# MAGIC
# MAGIC **Core habit:** predict row count → run the join → verify with `count()`.
# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %md
# MAGIC ## Setup — load landing `trip` and `trip_time`
# MAGIC
# MAGIC | Table | Format | Grain | Key column | Rows |
# MAGIC |---|---|---|---|---|
# MAGIC | **`trip`** | CSV | One rideshare trip | `trip_id` (bigint) | 100 |
# MAGIC | **`trip_time`** | Parquet | Date/hour per trip | `trip_id` (bigint) | 100 |
# MAGIC
# MAGIC **Why explicit schemas?** CSV has no type metadata. Without a schema, Spark
# MAGIC infers `trip_id` as a *string*. Later joins against Parquet (where `trip_id` is
# MAGIC `bigint`) would silently return 0 rows — types don't match, so nothing joins.
# MAGIC The DDL schemas below force correct types from the start.

# COMMAND ----------

from pyspark.sql import functions as F

landing_root = "/Volumes/rideshare_dev/landing/source_files"
trip_csv_path = f"{landing_root}/trip/trip.csv"
trip_time_parquet_path = f"{landing_root}/trip_time/trip_time.parquet"

print(f"trip_csv_path = {trip_csv_path}")
print(f"trip_time_parquet_path = {trip_time_parquet_path}")

# COMMAND ----------

trip_schema_ddl = """
trip_id bigint,
service_type string,
pickup_location_id int,
dropoff_location_id int,
trip_distance_miles decimal(8,2),
request_to_pickup_mins int,
ride_duration_mins int,
driver_arrival_to_pickup_mins int
"""

trip_time_schema_ddl = """
trip_id bigint,
trip_date date,
hour_of_day int
"""

trip = (
    spark.read.format("csv")  # noqa: F821
    .option("header", True)
    .schema(trip_schema_ddl)
    .load(trip_csv_path)
)

trip_time = (
    spark.read.format("parquet")  # noqa: F821
    .schema(trip_time_schema_ddl)
    .load(trip_time_parquet_path)
)

print("trip rows:", trip.count())
print("trip_time rows:", trip_time.count())

# COMMAND ----------

# DBTITLE 1,Section 1 - Grain
# MAGIC %md
# MAGIC ## 1. Grain — know your data before you join it
# MAGIC
# MAGIC If you don't know what one row represents, you can't predict what a join will
# MAGIC do.
# MAGIC
# MAGIC **Grain** = what one row represents.
# MAGIC
# MAGIC - `trip`: one row = one completed trip
# MAGIC - `trip_time`: one row = one time record for one trip
# MAGIC
# MAGIC | Table | Grain statement | Key |
# MAGIC |---|---|---|
# MAGIC | `trip` | One completed rideshare trip | `trip_id` |
# MAGIC | `trip_time` | One time record per trip | `trip_id` |
# MAGIC
# MAGIC **The one check that prevents most join bugs:**
# MAGIC
# MAGIC ```
# MAGIC row_count = total rows in the table
# MAGIC distinct_key = number of unique trip_id values
# MAGIC
# MAGIC row_count == distinct_key → safe to join (one row per key)
# MAGIC row_count >  distinct_key → STOP. Duplicates will multiply your rows.
# MAGIC ```
# MAGIC
# MAGIC This takes 5 seconds to run and saves hours of debugging wrong aggregations.
# MAGIC The next cell demonstrates it.

# COMMAND ----------

trip_grain = trip.agg(
    F.count("*").alias("row_count"),
    F.countDistinct("trip_id").alias("distinct_trip_id"),
).collect()[0]

print(
    f"trip: {trip_grain.row_count} rows, "
    f"{trip_grain.distinct_trip_id} distinct trip_id values"
)
print(
    "Grain check:",
    "one row per trip_id"
    if trip_grain.row_count == trip_grain.distinct_trip_id
    else "NOT unique on trip_id",
)

# COMMAND ----------

# DBTITLE 1,Section 2 - Cardinality vocabulary
# MAGIC %md
# MAGIC ## 2. Cardinality — predict the damage before it happens
# MAGIC
# MAGIC Grain tells you what one row is. **Cardinality** tells you what will happen
# MAGIC when two tables meet on a join key:
# MAGIC
# MAGIC | Label | What it means | Row-count prediction |
# MAGIC |---|---|---|
# MAGIC | **1:1** | Each left key matches at most one right key | Output ≈ input count |
# MAGIC | **1:M** | One left key matches multiple right rows | Output grows (fanout) |
# MAGIC | **M:1** | Multiple left rows match one right row | Output follows left count |
# MAGIC | **M:M** | Duplicates on both sides | Output can multiply sharply |
# MAGIC
# MAGIC Landing **`trip`** ↔ **`trip_time`** on **`trip_id`** should be **1:1** —
# MAGIC The grain check above confirmed `trip` has 100 rows and 100 distinct
# MAGIC `trip_id` values (no duplicates); the profile cell below verifies
# MAGIC `trip_time` too. Once both pass,
# MAGIC the 1:1 label is safe. 
# MAGIC
# MAGIC The intro sketch is **1:M** (trip grain ↔ charge lines). Section 3.2 is
# MAGIC **M:M** on `trip_id` (`trip_charges` ↔ `rate_card` → 12 rows) — motivation
# MAGIC for composite keys. **M:1** is the same join with tables swapped — no extra
# MAGIC demo needed. A fuller **M:M** construct is in Notebook **`02`**.
# MAGIC
# MAGIC You don't need to memorize this table. The point is simple: **if you know the
# MAGIC grain of both sides, you already know what the join will do.** No surprises.

# COMMAND ----------

# DBTITLE 1,Profile both tables
# MAGIC %md
# MAGIC ### Verify both sides — not just one
# MAGIC
# MAGIC The grain check above confirmed `trip` is unique on `trip_id`. That's half
# MAGIC the story. If `trip_time` has duplicates, the join still multiplies rows.
# MAGIC **Both sides must pass the grain
# MAGIC check.** One clean table joined to one dirty table = dirty output.

# COMMAND ----------

for name, df in [("trip", trip), ("trip_time", trip_time)]:
    stats = df.agg(
        F.count("*").alias("rows"),
        F.countDistinct("trip_id").alias("distinct_trip_id"),
    ).collect()[0]
    print(f"{name}: rows={stats.rows}, distinct trip_id={stats.distinct_trip_id}")

# COMMAND ----------

# DBTITLE 1,Section 3 - Join syntax
# MAGIC %md
# MAGIC ## 3. Join-condition syntax — three ways to say "match these keys"
# MAGIC
# MAGIC Grain and cardinality tell you *whether* to join. Now: *how* to write it.
# MAGIC
# MAGIC PySpark gives you three syntactic forms for equi-joins (key on left = key on
# MAGIC right):
# MAGIC
# MAGIC | # | Form | Syntax | Keys in output | Use when |
# MAGIC |---|---|---|---|---|
# MAGIC | 1 | **String** | `"trip_id"` | 1 merged | Same name, single key |
# MAGIC | 2 | **List** | `["trip_id", "charge_type"]` | 1 per key merged | Same names, multi-key |
# MAGIC | 3 | **Boolean** | `F.col("l.key") == F.col("r.other")` | Both kept | Names differ |
# MAGIC
# MAGIC **The key difference:** String/List merge the key columns into one clean output
# MAGIC column. Boolean keeps both key columns separately — since names typically
# MAGIC differ (e.g., `trip_id` and `trip_no`), that's fine. 
# MAGIC
# MAGIC But if you mistakenly use
# MAGIC Boolean when names are the same, you get two columns with the same name —
# MAGIC Section 3.3 covers that trap.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3.1 String form — single shared column name
# MAGIC
# MAGIC ```python
# MAGIC left.join(right, "trip_id", "inner")
# MAGIC ```
# MAGIC
# MAGIC **What happens:**
# MAGIC - Spark finds `trip_id` on both sides, matches equal values
# MAGIC - **Merges** the two `trip_id` columns into one in the output (called "coalescing")
# MAGIC - Result has cleaner schema — no duplicate columns
# MAGIC
# MAGIC **Predict:** `trip` (100 rows) joined to `trip_time` (100 rows), both unique on
# MAGIC `trip_id`, all keys match → expect **100** output rows.

# COMMAND ----------

join_string = trip.join(trip_time, "trip_id", "inner")
print("Columns after string join on trip_id:", join_string.columns)
print("Row count:", join_string.count())
join_string.select("*").show(1, truncate=False,vertical=True)

# COMMAND ----------

# DBTITLE 1,3.2 List form
# MAGIC %md
# MAGIC ### 3.2 List form — composite equi-join
# MAGIC
# MAGIC ```python
# MAGIC left.join(right, ["trip_id", "charge_type"], "inner")
# MAGIC ```
# MAGIC
# MAGIC If a single column matches too broadly, add more columns to narrow it down.
# MAGIC Spark requires **ALL** columns in the list to match before producing a row.
# MAGIC
# MAGIC **Scenario:** you want to compare actual charges against expected rates.
# MAGIC
# MAGIC - `trip_charges` — 6 rows (3 charge types × 2 trips): what was actually charged
# MAGIC - `rate_card` — 4 rows (2 charge types × 2 trips, no tip): what should have been charged
# MAGIC
# MAGIC Both tables have `trip_id` and `charge_type`. You expect **4 rows** (one
# MAGIC comparison per charge type per trip). Watch what each key gives you:
# MAGIC
# MAGIC | Join key | What happens | Result |
# MAGIC |---|---|---|
# MAGIC | `"trip_id"` only | Every charge × every rate per trip | **12** (wrong) |
# MAGIC | `["trip_id", "charge_type"]` | Exact charge-type match only | **4** (correct) |
# MAGIC
# MAGIC The first code cell below shows the mistake (expect 4, get 12). The second
# MAGIC shows the fix (add `charge_type` to the key → get 4).

# COMMAND ----------

# DBTITLE 1,trip_id-only join
trip_charges = spark.createDataFrame(  # noqa: F821
    [
        (1, "base_fare", 8.00),
        (1, "surge", 3.00),
        (1, "tip", 1.50),
        (2, "base_fare", 18.00),
        (2, "surge", 5.00),
        (2, "tip", 2.00),
    ],
    ["trip_id", "charge_type", "amount"],
)

rate_card = spark.createDataFrame(  # noqa: F821
    [
        (1, "base_fare", 7.50),
        (1, "surge", 2.50),
        (2, "base_fare", 16.00),
        (2, "surge", 4.50),
    ],
    ["trip_id", "charge_type", "expected_amount"],
)

# THE MISTAKE: join on trip_id only
# Business expectation: 4 rows (one actual vs expected per charge type)
expected_rows = 4
single_key = trip_charges.join(rate_card, "trip_id", "inner")
actual_rows = single_key.count()
print(f"Expected: {expected_rows} rows (one comparison per charge type)")
print(f"Actual:   {actual_rows} rows — join key too broad!")
print(f"\nSomething is wrong. We got {actual_rows - expected_rows} extra rows.")
single_key.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Composite key fix
# MAGIC %md
# MAGIC **Now the fix — same two tables, just add `charge_type` to the key.**
# MAGIC
# MAGIC Spark now requires BOTH `trip_id` AND `charge_type` to match. No more broad
# MAGIC matching where every charge pairs with every rate for the same trip.
# MAGIC
# MAGIC Tip rows in `trip_charges` have no matching `charge_type` in `rate_card` →
# MAGIC inner join drops them. **Predict: 4 rows.**
# MAGIC
# MAGIC Compare the output below to the previous cell — same columns, same tables,
# MAGIC just fewer (correct) rows.

# COMMAND ----------

# THE FIX: join on BOTH trip_id and charge_type
predicted_composite = 4
composite_key = trip_charges.join(rate_card, ["trip_id", "charge_type"], "inner")
actual_composite = composite_key.count()
print(
    f"[trip_id, charge_type]: predicted={predicted_composite}, "
    f"actual={actual_composite}"
)
composite_key.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,3.3 Boolean form
# MAGIC %md
# MAGIC ### 3.3 Boolean form — when column names differ
# MAGIC
# MAGIC Your trip table has `trip_id`. An external system has `trip_no`. Same data,
# MAGIC different name. String/list forms can't handle this — use Boolean:
# MAGIC
# MAGIC ```python
# MAGIC trips.alias("t").join(
# MAGIC     feedback.alias("f"),
# MAGIC     F.col("t.trip_id") == F.col("f.trip_no"),
# MAGIC     "inner",
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Two things to know:**
# MAGIC 1. You must `.alias()` both sides — otherwise Spark can't tell which table's column you mean
# MAGIC 2. Both key columns stay in the output — use `.select()` to drop the extra one
# MAGIC
# MAGIC **Below:** `trips` has trip_id [1, 2]. `feedback` has trip_no [1, 3].
# MAGIC Only trip 1 matches → **predict 1 row.**

# COMMAND ----------

# Internal trip data — key is "trip_id"
trips = spark.createDataFrame(  # noqa: F821
    [(1, "standard"), (2, "premium")],
    ["trip_id", "service_type"],
)

# External feedback system — same trips, but key is "trip_no"
feedback = spark.createDataFrame(  # noqa: F821
    [(1, 4.8), (3, 3.2)],
    ["trip_no", "driver_rating"],
)

# String form would fail — names don't match. Boolean is required.
join_diff_names = trips.alias("t").join(
    feedback.alias("f"),
    F.col("t.trip_id") == F.col("f.trip_no"),
    "inner",
)
print("Output columns:", join_diff_names.columns)
print("  → 'trip_id' AND 'trip_no' both appear — Boolean form never merges keys")
print(f"\nRow count: {join_diff_names.count()} (only trip 1 exists on both sides)")
join_diff_names.show()

# COMMAND ----------

# DBTITLE 1,Boolean same names
# MAGIC %md
# MAGIC ### Common mistake: Boolean form when names already match
# MAGIC
# MAGIC You *can* do it — but you're creating a problem for yourself:
# MAGIC
# MAGIC ```python
# MAGIC # String form result: ['trip_id', 'trip_date', ...]      ← clean, one trip_id
# MAGIC # Boolean form result: ['trip_id', 'trip_date', ..., 'trip_id']  ← broken, two trip_id
# MAGIC ```
# MAGIC
# MAGIC Now every `.select("trip_id")` downstream throws an **ambiguous column error**.
# MAGIC You're forced to prefix everything with aliases: `F.col("t.trip_id")`.
# MAGIC
# MAGIC **Don't make extra work for yourself.** Use string form when names match.
# MAGIC Boolean is for when you have no choice (names differ). The cell below shows
# MAGIC exactly what the duplicate-column problem looks like.

# COMMAND ----------

join_bool_raw = trip.alias("t").join(
    trip_time.alias("tt"),
    F.col("t.trip_id") == F.col("tt.trip_id"),
    "inner",
)
print("Column names after Boolean join (note two trip_id columns):")
print(join_bool_raw.columns)
join_bool_raw.select(
    # F.col("trip_id"),  # This would throw ambiguous column error!
    F.col("t.trip_id"),
    F.col("tt.trip_date"),
).show(3, truncate=False)

# COMMAND ----------

# DBTITLE 1,Exercise
# MAGIC %md
# MAGIC ## Exercise — unmatched keys
# MAGIC
# MAGIC Sections 1–3 covered grain, cardinality, and syntax. One more decision:
# MAGIC **what happens when keys don't fully overlap?** The join type decides which
# MAGIC unmatched rows survive.
# MAGIC
# MAGIC | Join type | Plain English |
# MAGIC |---|---|
# MAGIC | **inner** | Keep only rows that match on both sides |
# MAGIC | **left** | Keep all left rows; NULLs where right has no match |
# MAGIC | **right** | Keep all right rows; NULLs where left has no match |
# MAGIC | **full outer** | Keep everything; NULLs on both sides where needed |
# MAGIC
# MAGIC Constructed frames in the next cell:
# MAGIC
# MAGIC ```
# MAGIC Left  trip_id: [1, 2, 3, 4, 5]
# MAGIC Right trip_id: [3, 4, 5, 6, 7]
# MAGIC ```
# MAGIC
# MAGIC 1. Split the keys into left-only, overlap, and right-only.
# MAGIC 2. Predict **inner**, **left**, **right**, and **full** row counts.
# MAGIC 3. Replace each `None` below with your prediction, then run and verify.

# COMMAND ----------

left_unmatched = spark.createDataFrame(  # noqa: F821
    [(1,), (2,), (3,), (4,), (5,)],
    ["trip_id"],
)
right_unmatched = spark.createDataFrame(  # noqa: F821
    [(3,), (4,), (5,), (6,), (7,)],
    ["trip_id"],
)

# YOUR PREDICTIONS — replace None with the row count you expect
predictions = {
    "inner": None,
    "left": None,
    "right": None,
    "full": None,
}

for join_type, predicted in predictions.items():
    actual = left_unmatched.join(right_unmatched, "trip_id", join_type).count()
    match = "✓" if predicted == actual else "✗"
    print(f"{match} {join_type:6} → predicted={predicted}, actual={actual}")

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Concept | Key takeaway |
# MAGIC |---|---|
# MAGIC | **Grain** | `count` == `countDistinct(key)` → one row per key before joining |
# MAGIC | **Cardinality** | Labels predict row count; M:1 = 1:M with tables swapped |
# MAGIC | **String join** | `"trip_id"` — merged key column when names match |
# MAGIC | **List join** | `["trip_id", "charge_type"]` — all listed columns must match |
# MAGIC | **Boolean join** | Different names; both key columns kept unless you `select` |
# MAGIC | **Unmatched keys** | inner = overlap; left/right keep one side; full = all rows |
# MAGIC
# MAGIC **The habit that prevents 90% of join bugs:** predict the row count BEFORE
# MAGIC you run. If actual ≠ predicted, something is wrong with your understanding
# MAGIC of the data — fix that before you build anything on top of it.
# MAGIC
# MAGIC **Next:** **`02 - Silent Join Failures and Validation`** — M:M fanout, NULL
# MAGIC keys, accidental Cartesians, and the **profile → predict → run → verify**
# MAGIC habit before you trust a join.