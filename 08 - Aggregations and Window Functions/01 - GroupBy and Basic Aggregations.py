# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - GroupBy and Basic Aggregations
# MAGIC
# MAGIC Output grain and basic `groupBy().agg()` — no write.
# MAGIC
# MAGIC `trip_enriched`.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Name and verify the output grain of grouped calculations
# MAGIC - Build aliased aggregates and reason about NULL values and count semantics
# COMMAND ----------

# MAGIC %md
# MAGIC ## Two traps every data engineer faces in aggregations
# MAGIC
# MAGIC ### Trap 1: Spark skips NULLs silently
# MAGIC Your stakeholder asks for the **average tip per trip** across 106 trips:
# MAGIC
# MAGIC ```python
# MAGIC trip_enriched.agg(F.avg("tip_amount")).show()
# MAGIC ```
# MAGIC
# MAGIC - **Spark calculates:** `2.955673` ($307.39 total ÷ **104** known tips)
# MAGIC - **Business asks for:** `2.899906` ($307.39 total ÷ **106** total trips)
# MAGIC
# MAGIC Because 2 trips have `NULL` tips, `F.avg` skips them. Spark gives no error,
# MAGIC but calculated the **average of the known tip values** instead of the **average
# MAGIC tip across all trips**. A wrong aggregate still returns a reasonable number.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Trap 2: `groupBy` changes what one row represents
# MAGIC
# MAGIC Module 7 taught you to preserve data grain during joins (1 row per trip).
# MAGIC A `groupBy` deliberately **reduces** that grain.
# MAGIC
# MAGIC `groupBy("service_type")` changes the data from one row per trip to one row
# MAGIC per service type.
# MAGIC
# MAGIC **Core habit:** Before writing a `groupBy`, determine what **one row in
# MAGIC the result should represent**. Then perform the aggregation and confirm
# MAGIC that the number of output rows aligns with your expectations.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What this notebook teaches
# MAGIC
# MAGIC | Section | Concept | Why it matters |
# MAGIC |---|---|---|
# MAGIC | 1. Output grain | One row per group | Predict summary row count before running |
# MAGIC | 2. `groupBy().agg()` | Syntax and aliasing | Name aggregate columns explicitly |
# MAGIC | 3. Counting | 3 counts, 3 answers | Match count function to business question |
# MAGIC | 4. Aggregates skip NULLs | `sum` / `avg` ignore NULLs | Control denominator with `F.coalesce` |  # noqa: E501
# MAGIC | Exercise | Per-`payment_method` summary | Apply all four habits on a new key |
# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %md
# MAGIC ## Setup — load `trip_enriched`
# MAGIC
# MAGIC Module 7 Notebook **`07`** wrote this managed table: one row per `trip_id`,
# MAGIC **106** rows, 16 columns.
# MAGIC
# MAGIC | Role | Columns |
# MAGIC |---|---|
# MAGIC | Key | `trip_id` |
# MAGIC | Join keys (retained) | `pickup_location_id`, `dropoff_location_id` |
# MAGIC | Group keys | `service_type`, `payment_method`, `trip_date`, `hour_of_day` |
# MAGIC | Group keys (zone) | `pickup_borough`, `pickup_zone`, `dropoff_borough`, `dropoff_zone` |
# MAGIC | Measures | `trip_distance_miles`, `ride_duration_mins` |
# MAGIC | Measures (money) | `base_fare_amount`, `tip_amount`, `driver_payout_amount` |
# MAGIC
# MAGIC Money and distance columns are `decimal`; `ride_duration_mins` and
# MAGIC `hour_of_day` are `int`; `trip_date` is a `date`.

# COMMAND ----------

from pyspark.sql import functions as F

trip_enriched_table = "rideshare_dev.processed.trip_enriched"

trip_enriched = spark.table(trip_enriched_table)  # noqa: F821

print("trip_enriched rows:", trip_enriched.count())
trip_enriched.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### The NULLs you inherited — read this before Section 3
# MAGIC
# MAGIC In `trip_enriched`, these columns are NULL on these `trip_id` values.
# MAGIC Both causes are deliberate (Module 7 left joins + Module 6 value rejection):
# MAGIC
# MAGIC | Column(s) | NULL on `trip_id` | Rows | Cause |
# MAGIC |---|---|---|---|
# MAGIC | `trip_date`, `hour_of_day` | 101–106 | 6 | `trip_time` had 100 rows (Module 7 left join) |
# MAGIC | `payment_method`, `driver_payout_amount` | 106 | 1 | `payment` had 105 rows (left join) |
# MAGIC | `base_fare_amount` | 104, 106 | 2 | Left join; Module 6: trip 104 (negative fare) |
# MAGIC | `tip_amount` | 103, 106 | 2 | Left join; Module 6 rejected trip 103's `not_a_number` tip |
# MAGIC | `trip_distance_miles` | 103, 105, 106 | 3 | Module 6: `-1.00`, `not_a_number`, blank |
# MAGIC
# MAGIC The next cell proves the counts — not the individual `trip_id`s.

# COMMAND ----------

trip_enriched.select(
    F.count("*").alias("rows"), # Counts all rows (NULLs don’t matter)
    F.count("trip_date").alias("trip_date"), # Exclude NULL rows
    F.count("payment_method").alias("payment_method"),
    F.count("base_fare_amount").alias("base_fare"),
    F.count("tip_amount").alias("tip"),
    F.count("trip_distance_miles").alias("distance"),
    F.count("ride_duration_mins").alias("duration"),
).show()

# Expected: rows=106, trip_date=100, payment_method=105, base_fare=104,
# tip=104, distance=103, duration=106

# COMMAND ----------

# DBTITLE 1,Section 1 - Output grain
# MAGIC %md
# MAGIC ## 1. Output grain — one row per group
# MAGIC
# MAGIC **Output grain** refers to what each row of your *result* represents.
# MAGIC
# MAGIC Module 7 processed *input* grain with one row per `trip_id`. A `groupBy`
# MAGIC operation replaces it with a new structure:
# MAGIC
# MAGIC | | Grain | Rows |
# MAGIC |---|---|---|
# MAGIC | Input `trip_enriched` | One completed trip | 106 |
# MAGIC | `groupBy("service_type")` | One service type | ? |
# MAGIC
# MAGIC You can fill in that `?` **before running the aggregate**, because the output
# MAGIC row count of a `groupBy` is just the number of distinct group-key values:
# MAGIC
# MAGIC ```
# MAGIC output rows == countDistinct(group key)
# MAGIC ```
# MAGIC
# MAGIC The prediction step is straightforward and only requires a single inexpensive
# MAGIC query. If the actual result returns a different row count, then your mental
# MAGIC model of the data is incorrect, and any calculations derived from it will
# MAGIC also be flawed.
# MAGIC
# MAGIC Notebook `02` covers this caveat: `countDistinct` **excludes NULLs**, while
# MAGIC `groupBy` **includes them as a separate group**. `service_type` has no NULLs,
# MAGIC so both methods agree here.
# MAGIC
# MAGIC **Performance Note:** The `groupBy` operation is considered a **wide**
# MAGIC transformation, which involves an `Exchange` (shuffle) as outlined in
# MAGIC Module 4. This means that rows with the same key need to be processed by the
# MAGIC same executor. Tuning the shuffle process will be covered in Module 17.

# COMMAND ----------

# How many groups will groupBy("service_type") produce?
trip_enriched.select("service_type").distinct().count()  # 5 groups expected

# COMMAND ----------

trip_enriched.select("service_type").distinct().show() 

# COMMAND ----------

# MAGIC %md
# MAGIC `UNKNOWN` is Module 6's normalized sentinel for a blank or `n/a`
# MAGIC service type; it is a real string, **not** a NULL.

# COMMAND ----------

# Note the uppercase values: Module 6 normalized service_type with F.upper
trip_enriched.groupBy("service_type").count().orderBy(F.col("count").desc()).show()

# Expected: STANDARD 55, SHARED 21, PREMIUM 16, XL 12, UNKNOWN 2  (sum = 106)

# COMMAND ----------

# DBTITLE 1,Section 2 - groupBy and agg
# MAGIC %md
# MAGIC ## 2. `groupBy().agg()` — multiple aggregates in one pass
# MAGIC Two rules:
# MAGIC
# MAGIC
# MAGIC **1. Multiple Aggregates, One Scan:** You can include as many aggregate
# MAGIC expressions as needed inside the `.agg(...)` function. Spark processes all
# MAGIC of these expressions in a single pass, eliminating the need for separate
# MAGIC queries for operations like count, sum, and average.
# MAGIC
# MAGIC **2. Always Use Aliases:** If you don't use `.alias(...)`, Spark will
# MAGIC automatically generate column names such as `count(1)`, `sum(tip_amount)`,
# MAGIC and `avg(CAST(...))`. These generated names can be difficult to read and
# MAGIC cumbersome to use later on, often requiring backtick escaping like
# MAGIC ``F.col("`sum(tip_amount)`")``. To avoid confusion, always assign each
# MAGIC aggregate a clear, descriptive name.
# MAGIC
# MAGIC The first cell below illustrates the default names that can be hard to
# MAGIC interpret, while the second cell shows a more readable version with aliases.

# COMMAND ----------

# Aggregates without alias — legal, but look at the column names
trip_enriched.groupBy("service_type").agg(
    F.count("*"),
    F.sum("tip_amount"),
    F.avg("trip_distance_miles"),
).show(truncate=False)

# COMMAND ----------

service_summary = trip_enriched.groupBy("service_type").agg(
    F.count("*").alias("trip_count"),
    F.sum("tip_amount").alias("total_tip"),
    F.round(F.avg("trip_distance_miles"), 2).alias("avg_distance_miles"),
    F.min("ride_duration_mins").alias("shortest_ride_mins"),
    F.max("ride_duration_mins").alias("longest_ride_mins"),
)

service_summary.orderBy(F.col("trip_count").desc()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## UNKNOWN has 2 trips — which ones? Let's try adding trip_id to the result.

# COMMAND ----------

try:
    service_summary = trip_enriched.groupBy("service_type").agg(
    F.count("*").alias("trip_count"),
    F.sum("tip_amount").alias("total_tip"),
    F.round(F.avg("trip_distance_miles"), 2).alias("avg_distance_miles"),
    F.min("ride_duration_mins").alias("shortest_ride_mins"),
    F.max("ride_duration_mins").alias("longest_ride_mins"),
    F.col("trip_id"),  # This is not allowed
).filter(F.col("service_type") == "UNKNOWN")

    service_summary.show()
except Exception:
    print(
        "groupBy collapsed multiple rows into one row per group"
        " — there is no single trip_id to display."
    )
    print(
        "To keep individual rows with group-level summaries,"
        " use a window function (Notebook 05)."
    )


# COMMAND ----------

# DBTITLE 1,Section 3 - Counting correctly
# MAGIC %md
# MAGIC ## 3. Counting correctly — three counts, three answers
# MAGIC
# MAGIC | Expression | Question it answers | On `trip_date` |
# MAGIC |---|---|---|
# MAGIC | `F.count("*")` | How many **rows**? | **106** |
# MAGIC | `F.count("trip_date")` | How many rows **have a value**? | **100** |
# MAGIC | `F.countDistinct("trip_date")` | How many **different values**? | **14** |
# MAGIC
# MAGIC These are three different business questions:
# MAGIC
# MAGIC - **How many trips did we operate?** — count all trips.
# MAGIC - **How many trips have a valid trip date?** — count only trips where the date is available.
# MAGIC - **How many days does the dataset cover?** — count the distinct trip dates.
# MAGIC
# MAGIC All three questions are valid, but the appropriate question depends on
# MAGIC the **business metric you are trying to measure**.
# MAGIC
# MAGIC **Cost note:** `countDistinct` is expensive at scale — use it when you need
# MAGIC the exact distinct count. Notebook `03` continues with collections,
# MAGIC percentiles, and route-level `countDistinct`.

# COMMAND ----------

trip_enriched.select(
    F.count("*").alias("all_trips"),
    F.count("trip_date").alias("trips_with_a_date"),
    F.countDistinct("trip_date").alias("distinct_dates"),
).show()

# Expected: all_trips=106, trips_with_a_date=100 (6 NULLs), distinct_dates=14

# COMMAND ----------

# MAGIC %md
# MAGIC Now apply the same three counts **per service type** — this is where
# MAGIC the difference becomes visible.

# COMMAND ----------

trip_enriched.groupBy("service_type").agg(
    F.count("*").alias("trip_count"),
    F.count("trip_date").alias("dated_trip_count"),
    F.countDistinct("trip_date").alias("distinct_dates"),
).orderBy(F.col("trip_count").desc()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC | `service_type` | `trip_count` | `dated_trip_count` | Gap | `distinct_dates` |
# MAGIC |---|---|---|---|---|
# MAGIC | STANDARD | 55 | 52 | 3 | 14 |
# MAGIC | SHARED | 21 | 21 | 0 | 13 |
# MAGIC | PREMIUM | 16 | 15 | 1 | 9 |
# MAGIC | XL | 12 | 12 | 0 | 8 |
# MAGIC | UNKNOWN | 2 | **0** | 2 | **0** |
# MAGIC
# MAGIC Notice the gap between `trip_count` and `dated_trip_count` — that's where NULLs hide.
# MAGIC STANDARD is short by 3, but every row still looks conceivable. Only by
# MAGIC showing both counts side by side does the gap become evident.
# MAGIC
# MAGIC UNKNOWN is the extreme: 2 trips, **0** dated — the entire group has no date information.

# COMMAND ----------

# DBTITLE 1,Section 4 - Aggregates skip NULLs
# MAGIC %md
# MAGIC ## 4. Aggregates skip NULLs
# MAGIC
# MAGIC In row arithmetic, `NULL` propagates: `a + NULL = NULL`.
# MAGIC Aggregates do the opposite — they **skip** NULLs silently.

# COMMAND ----------

trip_enriched.select(
    F.count("*").alias("all_trips"),
    F.count("tip_amount").alias("trips_with_tip"),
    F.sum("tip_amount").alias("total_tip"),
    F.avg("tip_amount").alias("avg_skips_nulls"),
    # Round to 6 d.p. to match F.avg's decimal(14,6) output — raw division
    # widens to decimal(38,20) and prints 20 digits without the round.
    F.round(F.sum("tip_amount") / F.count("*"), 6).alias("avg_per_trip"),
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC `F.avg` divided by **104** (non-NULL tips), not **106** (all trips).
# MAGIC Two valid interpretations — choose deliberately:
# MAGIC
# MAGIC | NULL rows are… | Use… |
# MAGIC |---|---|
# MAGIC | **Not valid for the metric** — exclude from the average | `F.avg` — denominator is non-NULL count only |  # noqa: E501
# MAGIC | **Valid but zero** — include in the average as 0.00 | `F.coalesce(col, F.lit(0))` before aggregating |  # noqa: E501
# MAGIC
# MAGIC The same skip rule applies to `F.sum`, `F.min`, and `F.max`.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Edge case — what if ALL values in a group are NULL?

# COMMAND ----------

# Edge case: when ALL values in a group are NULL, F.sum returns NULL — not 0
# Note: trip_enriched has no multi-row group where ALL fares are NULL,
# so we use a small handmade dataset that follows the same schema.
from decimal import Decimal  # noqa: E402

from pyspark.sql.types import DecimalType, StringType, StructField, StructType  # noqa: E402

edge_case_schema = StructType([
    StructField("payment_method", StringType()),
    StructField("base_fare_amount", DecimalType(10, 2)),
])

edge_case_data = [
    ("card", Decimal("25.00")),
    ("card", Decimal("30.00")),
    ("cash", None),
    ("cash", None),
]

edge_case_df = spark.createDataFrame(edge_case_data, edge_case_schema)  # noqa: F821

edge_case_df.groupBy("payment_method").agg(
    F.count("*").alias("trips"),
    F.count("base_fare_amount").alias("known_fares"),
    F.sum("base_fare_amount").alias("total_fare"),
    F.avg("base_fare_amount").alias("avg_fare"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **The fix:** `F.coalesce` replaces NULLs with 0 *before* aggregating —
# MAGIC now the denominator is 106.

# COMMAND ----------

tip_or_zero = F.coalesce(F.col("tip_amount"), F.lit(0).cast("decimal(10,2)"))

trip_enriched.select(
    F.sum(tip_or_zero).alias("total_tip_unchanged"),
    F.count(tip_or_zero).alias("now_counts_all_106"),
    F.avg(tip_or_zero).alias("avg_with_zeros"),
).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Exercise
# MAGIC %md
# MAGIC ## Exercise — a per-`payment_method` summary
# MAGIC
# MAGIC Sections 1–4 grouped mostly on `service_type`. Apply the same habits on
# MAGIC **`payment_method`**.
# MAGIC
# MAGIC **1. Aggregate.** One row per `payment_method`, with these aliased columns:
# MAGIC
# MAGIC | Alias | Aggregate |
# MAGIC |---|---|
# MAGIC | `trip_count` | All trips (`count("*")`) |
# MAGIC | `trips_with_fare` | Trips with a non-NULL `base_fare_amount` |
# MAGIC | `total_base_fare` | Sum of `base_fare_amount`, rounded to 2 |
# MAGIC | `avg_fare_skips_null` | `F.avg("base_fare_amount")`, rounded to 2 |
# MAGIC | `avg_fare_per_trip` | `F.sum / F.count("*")`, rounded to 2 |
# MAGIC
# MAGIC **2. Check the row count.** After you run, set `predicted_method_groups` to
# MAGIC the number of rows you got and confirm it matches `method_summary.count()`.
# MAGIC (You may see more groups than `countDistinct("payment_method")` — Notebook
# MAGIC `02` explains why.)
# MAGIC
# MAGIC **3. Explain (Section 4).** For the row where `payment_method` is NULL, why
# MAGIC are `total_base_fare` and `avg_fare_skips_null` both NULL, while
# MAGIC `trip_count` is 1?
# MAGIC
# MAGIC **Self-check:** `trip_count` should sum to **106**; `trips_with_fare`
# MAGIC should sum to **104** (matches the NULL map at the top of this notebook).

# COMMAND ----------

# 1. YOUR CODE — build the per-payment_method summary described above
method_summary = trip_enriched.groupBy("payment_method").agg(
    F.count("*").alias("trip_count"),
    # TODO: trips_with_fare, total_base_fare, avg_fare_skips_null, avg_fare_per_trip
)

# 2. YOUR CHECK — set this to the row count you observed
predicted_method_groups = None

actual = method_summary.count()
match = "✓" if predicted_method_groups == actual else "✗"
print(f"{match} predicted={predicted_method_groups}, actual={actual}")

method_summary.orderBy(F.col("trip_count").desc()).show()

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Concept | Key takeaway |
# MAGIC |---|---|
# MAGIC | **Output grain** | One row per group; predict with `.distinct().count()` |
# MAGIC | **Aliasing** | Always alias — ugly default names break downstream code |
# MAGIC | **Only keys and aggregates** | Want a per-row column too? That's a window (Notebook `05`) |  # noqa: E501
# MAGIC | **Three counts** | `count("*")`=106, `count("trip_date")`=100, `countDistinct`=14 |
# MAGIC | **NULL skipping** | `avg` divides by 104 (non-NULL), not 106 (all rows) |
# MAGIC | **`F.coalesce` first** | Use it when NULL means 0, not *unknown* |
# MAGIC | **Predict, then verify** | A `groupBy`'s row count can differ from what you expect — always check `.count()` before trusting the shape |  # noqa: E501
# MAGIC
# MAGIC **Next:** **`02 - Multi-column Keys, NULL Groups, and Filter Placement`** —
# MAGIC grouping on composite keys, why NULL becomes its own group, and how
# MAGIC `WHERE` vs `HAVING` produce different numbers from the same aggregation.