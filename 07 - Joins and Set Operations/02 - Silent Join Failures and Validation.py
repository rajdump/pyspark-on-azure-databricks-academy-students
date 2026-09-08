# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silent Join Failures and Validation
# MAGIC
# MAGIC M:M fanout, key profiling, and NULL-key traps — still no write.
# MAGIC
# MAGIC Landing `trip`, `trip_time`, `payment` (+ frames).
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Profile keys and resolve duplicates with `Window` + `row_number` (not
# MAGIC   `dropDuplicates`) when payloads differ
# MAGIC - Explain NULL-key behavior and use `eqNullSafe` when NULLs must match
# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %md
# MAGIC ## Setup — load and verify the clean 1:1 baseline
# MAGIC
# MAGIC | Table | Format | Grain | Key | Rows |
# MAGIC |---|---|---|---|---|
# MAGIC | `trip` | CSV | one completed trip | `trip_id` | 100 |
# MAGIC | `trip_time` | Parquet | one time record per trip | `trip_id` | 100 |
# MAGIC | `payment` | Avro | one fare record per trip | `trip_id` | 100 |
# MAGIC
# MAGIC All three are 1:1 on `trip_id`. `payment` is new (not used in Notebook 01);
# MAGIC it adds fare columns per trip.
# MAGIC
# MAGIC After loading, the second code cell runs all four join types on both pairs.
# MAGIC All should return 100 — a baseline **signal**, not proof of clean grain.
# MAGIC Matching join counts suggest no unmatched keys and no obvious fanout; Section 2
# MAGIC profiles `rows` / `distinct` / `nulls` to confirm uniqueness.

# COMMAND ----------

# DBTITLE 1,Load trip, trip_time, and payment with explicit schemas
from pyspark.sql import functions as F

landing_root = "/Volumes/rideshare_dev/landing/source_files"
trip_csv_path = f"{landing_root}/trip/trip.csv"
trip_time_parquet_path = f"{landing_root}/trip_time/trip_time.parquet"
payment_avro_path = f"{landing_root}/payment/payment.avro"

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

payment_schema_ddl = """
trip_id bigint,
payment_method string,
base_fare_amount decimal(10,2),
surge_amount decimal(10,2),
tax_amount decimal(10,2),
tip_amount decimal(10,2),
discount_amount decimal(10,2),
driver_payout_amount decimal(10,2)
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

payment = (
    spark.read.format("avro")  # noqa: F821
    .schema(payment_schema_ddl)
    .load(payment_avro_path)
)

print("trip rows:", trip.count())
print("trip_time rows:", trip_time.count())
print("payment rows:", payment.count())

# COMMAND ----------

# DBTITLE 1,Run all four join types on both table pairs
# trip ↔ trip_time — all four types should return 100
print("trip ↔ trip_time:")
print("  inner:", trip.join(trip_time, "trip_id", "inner").count())
print("  left: ", trip.join(trip_time, "trip_id", "left").count())
print("  right:", trip.join(trip_time, "trip_id", "right").count())
print("  full: ", trip.join(trip_time, "trip_id", "full").count())

print()

# trip ↔ payment — same expectation
print("trip ↔ payment:")
print("  inner:", trip.join(payment, "trip_id", "inner").count())
print("  left: ", trip.join(payment, "trip_id", "left").count())
print("  right:", trip.join(payment, "trip_id", "right").count())
print("  full: ", trip.join(payment, "trip_id", "full").count())

print("\n→ Counts match — profile in Section 2 to confirm grain.")
print("  The rest of this notebook shows what happens when data isn't this clean.")

# COMMAND ----------

# DBTITLE 1,1. M:M fanout
# MAGIC %md
# MAGIC ## 1. M:M — when both sides have duplicates
# MAGIC
# MAGIC Notebook 01 Section 3.2 showed what happens when you join on too few columns
# MAGIC — one side had extra rows per key. M:M is worse: BOTH sides have duplicates.
# MAGIC
# MAGIC You'll usually hit this when an ETL job ran twice, the source system sent
# MAGIC a retry, or two fact tables were joined before either one was aggregated.
# MAGIC
# MAGIC ```
# MAGIC Left trip_id:  [1, 1, 2]   — key 1 appears 2×
# MAGIC Right trip_id: [1, 1, 3]   — key 1 appears 2×
# MAGIC
# MAGIC Key 1: 2 left × 2 right = 4 output rows
# MAGIC Key 2: 1 left × 0 right = 0 (no match)
# MAGIC Key 3: 0 left × 1 right = 0 (no match)
# MAGIC Inner total: 4
# MAGIC ```
# MAGIC
# MAGIC The formula: for each key, count(left) × count(right).

# COMMAND ----------

# DBTITLE 1,M:M fanout — and how profiling would have caught it
left_mm = spark.createDataFrame([(1,), (1,), (2,)], ["trip_id"])  # noqa: F821
right_mm = spark.createDataFrame([(1,), (1,), (3,)], ["trip_id"])

predicted_mm = 4
actual_mm = left_mm.join(right_mm, "trip_id", "inner").count()
print(f"Inner M:M: predicted={predicted_mm}, actual={actual_mm}")
print("Each row below came from key 1 matching 2×2:")
left_mm.join(right_mm, "trip_id", "inner").show()

# This is exactly what Section 2's key-profiling habit catches BEFORE the
# join — profile both sides on the key first.
for name, frame in [("left_mm", left_mm), ("right_mm", right_mm)]:
    key_stats = frame.select(
        F.count("*").alias("rows"),
        F.countDistinct("trip_id").alias("distinct"),
    ).collect()[0]
    flag = "duplicates!" if key_stats["rows"] != key_stats["distinct"] else "unique"
    print(f"{name}: rows={key_stats['rows']}, distinct={key_stats['distinct']} \u2192 {flag}")

# COMMAND ----------

# DBTITLE 1,2. Key profiling
# MAGIC %md
# MAGIC ## 2. Key profiling — detect duplicates before joining
# MAGIC
# MAGIC The constructed frames in Section 1 both had duplicate keys — profiling would
# MAGIC have caught that before the join ever ran. Before any join, profile the key
# MAGIC on both sides:
# MAGIC
# MAGIC 1. **Row count** — total rows in the table
# MAGIC 2. **Distinct non-NULL keys** — `countDistinct(key)` (this ignores NULLs)
# MAGIC 3. **NULL key count** — rows where the key is NULL
# MAGIC
# MAGIC If rows == distinct and nulls == 0 → the key is unique and complete.
# MAGIC If not:
# MAGIC * rows ≠ distinct, but data is correct → the table's grain is finer than
# MAGIC   your key → find the composite key (Notebook 01 Section 3.2: `[trip_id,
# MAGIC   charge_type]` instead of just `trip_id`)
# MAGIC * rows ≠ distinct, data has true duplicates → resolve before joining
# MAGIC   (Section 3 below)
# MAGIC * nulls > 0 → inner joins will silently drop those rows (Section 4)

# COMMAND ----------

# DBTITLE 1,Profile trip, trip_time, and payment on trip_id
# Profile trip
trip_stats = trip.select(
    F.count("*").alias("rows"),
    F.countDistinct("trip_id").alias("distinct"),
    F.sum(F.when(F.col("trip_id").isNull(), 1).otherwise(0)).alias("nulls"),
).collect()[0]
print(f"trip       rows={trip_stats['rows']}, distinct={trip_stats['distinct']}, nulls={trip_stats['nulls']}")

# Profile trip_time
tt_stats = trip_time.select(
    F.count("*").alias("rows"),
    F.countDistinct("trip_id").alias("distinct"),
    F.sum(F.when(F.col("trip_id").isNull(), 1).otherwise(0)).alias("nulls"),
).collect()[0]
print(f"trip_time  rows={tt_stats['rows']}, distinct={tt_stats['distinct']}, nulls={tt_stats['nulls']}")

# Profile payment
pay_stats = payment.select(
    F.count("*").alias("rows"),
    F.countDistinct("trip_id").alias("distinct"),
    F.sum(F.when(F.col("trip_id").isNull(), 1).otherwise(0)).alias("nulls"),
).collect()[0]
print(f"payment    rows={pay_stats['rows']}, distinct={pay_stats['distinct']}, nulls={pay_stats['nulls']}")

# COMMAND ----------

# DBTITLE 1,3. Duplicate resolution
# MAGIC %md
# MAGIC ## 3. Duplicate resolution — don't let Spark decide
# MAGIC
# MAGIC You profiled and found duplicates. Two approaches:
# MAGIC
# MAGIC | Method | Behavior |
# MAGIC |---|---|
# MAGIC | `dropDuplicates(["trip_id"])` | Keeps one row per key — survivor is non-deterministic (different plans may keep different rows) |
# MAGIC | Window + `row_number` | Ranks rows per key by your rule (e.g. latest `updated_at`), keeps rank 1 — entire row stays intact |
# MAGIC
# MAGIC `dropDuplicates` is fine when all rows for a key are truly identical. When
# MAGIC they differ (different timestamps, amounts, statuses), use a **window
# MAGIC function** to rank rows and keep the one you want.
# MAGIC
# MAGIC Most common real-world pattern: same trip appears twice because ETL ran twice
# MAGIC or the source sent a retry. The rows differ only in `updated_at` timestamp.
# MAGIC Resolution: rank by `updated_at` descending, keep rank 1.
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.window import Window
# MAGIC
# MAGIC w = Window.partitionBy("trip_id").orderBy(F.col("updated_at").desc())
# MAGIC resolved = (dup_trips
# MAGIC     .withColumn("rn", F.row_number().over(w))
# MAGIC     .filter("rn = 1")
# MAGIC     .drop("rn"))
# MAGIC ```
# MAGIC
# MAGIC This keeps the **entire row** intact — no risk of mixing columns from
# MAGIC different records.

# COMMAND ----------

# DBTITLE 1,dropDuplicates vs window ranking on an ETL retry
from pyspark.sql.window import Window

# ETL ran twice — same trip, different updated_at timestamps
dup_trips = spark.createDataFrame(  # noqa: F821
    [
        (1, "Premium", 25.00, "2024-01-15 10:00:00"),
        (1, "Premium", 25.00, "2024-01-15 14:30:00"),  # later retry
        (2, "Standard", 12.50, "2024-01-15 09:00:00"),
    ],
    ["trip_id", "service_type", "fare", "updated_at"],
)

print("Before — trip_id=1 appears twice (ETL retry):")
dup_trips.show(truncate=False)

print("dropDuplicates — which row survived? Non-deterministic:")
dup_trips.dropDuplicates(["trip_id"]).orderBy("trip_id").show(truncate=False)

# Window function: rank by updated_at descending, keep rank 1 (latest)
w = Window.partitionBy("trip_id").orderBy(F.col("updated_at").desc())
resolved = (dup_trips
    .withColumn("rn", F.row_number().over(w))
    .filter("rn = 1")
    .drop("rn"))

print("Window function — keeps the latest complete row:")
resolved.orderBy("trip_id").show(truncate=False)

# Verify grain after resolution
stats = resolved.select(
    F.count("*").alias("rows"),
    F.countDistinct("trip_id").alias("distinct"),
).collect()[0]
print(f"Resolved: rows={stats['rows']}, distinct={stats['distinct']} \u2192 grain is clean")

# Did dropDuplicates happen to agree with the deterministic window result?
naive_survivor = dup_trips.dropDuplicates(["trip_id"]).filter("trip_id = 1").collect()[0]["updated_at"]
correct_survivor = resolved.filter("trip_id = 1").collect()[0]["updated_at"]
print(f"\ndropDuplicates kept updated_at={naive_survivor} for trip_id=1")
print(f"Window ranking kept updated_at={correct_survivor} for trip_id=1 (the true latest)")
print("\u2192 Same row this run?", naive_survivor == correct_survivor, "\u2014 don't rely on that holding next run.")

# COMMAND ----------

# DBTITLE 1,4. NULL keys
# MAGIC %md
# MAGIC ## 4. NULL keys — silent data loss
# MAGIC
# MAGIC In SQL and Spark, `NULL = NULL` evaluates to `NULL` (not TRUE). Standard
# MAGIC equality never matches NULL to NULL.
# MAGIC
# MAGIC **Scenario:** A batch of trip records arrived with a corrupt `trip_id` (NULL)
# MAGIC — the system failed to assign an ID. Separately, a payment record came in
# MAGIC that couldn't be linked to any trip (also NULL `trip_id`).
# MAGIC
# MAGIC You try to join trips with payments on `trip_id`:
# MAGIC
# MAGIC ```
# MAGIC trips   trip_id: [1, 2, NULL]   ← trip 3 has corrupt/missing ID
# MAGIC payments trip_id: [2, 3, NULL]   ← payment with no linked trip
# MAGIC ```
# MAGIC
# MAGIC The NULL trip and the NULL payment look like they should match — but they
# MAGIC don't. `NULL = NULL` is not TRUE in Spark. Both rows silently disappear
# MAGIC from an inner join.
# MAGIC
# MAGIC Predict each join type — replace `None` below, then run.

# COMMAND ----------

# DBTITLE 1,Predict join counts with NULL keys, then verify
# Trips — one record has corrupt/missing trip_id (NULL)
trips_with_null = spark.createDataFrame(  # noqa: F821
    [(1, "Premium"), (2, "Standard"), (None, "XL")],
    ["trip_id", "service_type"],
)

# Payments — one payment couldn't be linked to a trip (NULL)
payments_with_null = spark.createDataFrame(
    [(2, 15.00), (3, 22.50), (None, 8.00)],
    ["trip_id", "fare"],
)

print("Trips:")
trips_with_null.show()
print("Payments:")
payments_with_null.show()

# YOUR PREDICTIONS — replace None with expected row count
predictions = {
    "inner": None,
    "left": None,
    "right": None,
    "full": None,
}

for join_type, predicted in predictions.items():
    actual = trips_with_null.join(payments_with_null, "trip_id", join_type).count()
    mark = "✓" if predicted == actual else "✗"
    print(f"{mark} {join_type:6} → predicted={predicted}, actual={actual}")

# COMMAND ----------

# DBTITLE 1,4b. eqNullSafe
# MAGIC %md
# MAGIC ### When you WANT NULL to match NULL
# MAGIC
# MAGIC Sometimes NULL represents a known category ("unassigned trips") and you need
# MAGIC them to group together. Use `.eqNullSafe()` in a Boolean join condition —
# MAGIC it treats NULL = NULL as TRUE.
# MAGIC
# MAGIC This requires Boolean form (Notebook 01 Section 3.3) and produces both key
# MAGIC columns in the output — the same ambiguous-column trap Notebook 01 warned
# MAGIC about. Select explicitly if you need a single clean `trip_id` downstream.
# MAGIC
# MAGIC **Warning:** `eqNullSafe` can itself fan out when both sides contain multiple
# MAGIC NULL-key rows. Two NULLs left × three NULLs right = six rows. Profile NULL
# MAGIC counts on both sides before using this.
# MAGIC
# MAGIC Predict: inner with eqNullSafe on the data above → key 2 matches + NULL
# MAGIC matches = **2 rows**.

# COMMAND ----------

# DBTITLE 1,Standard equality vs eqNullSafe on NULL keys
# Standard inner join — NULL rows lost
standard = trips_with_null.join(payments_with_null, "trip_id", "inner")
print(f"Standard inner: {standard.count()} row (NULL didn't match)")
standard.show()

# eqNullSafe inner join — NULL rows matched
trips_a = trips_with_null.alias("t")
payments_a = payments_with_null.alias("p")

eq_safe = trips_a.join(
    payments_a,
    F.col("t.trip_id").eqNullSafe(F.col("p.trip_id")),
    "inner",
)
print(f"eqNullSafe inner: {eq_safe.count()} rows (NULL matched NULL)")
eq_safe.show()

print(f"eq_safe columns: {eq_safe.columns}")
print("\u2192 Two trip_id columns \u2014 Boolean form never merges keys, same as Notebook 01.")

# COMMAND ----------

# DBTITLE 1,5. Cartesian products
# MAGIC %md
# MAGIC ## 5. Cartesian products — intentional versus accidental
# MAGIC
# MAGIC A Cartesian product is every row on the left paired with every row on the
# MAGIC right. Think of it as M:M taken to the extreme — every key "matches" every
# MAGIC key.
# MAGIC
# MAGIC **Why it happens:** the join condition evaluates to TRUE for every
# MAGIC combination. No filtering, no key comparison — just "pair everything."
# MAGIC
# MAGIC **Intentional use case:** You're building a pricing grid. You have 3 service
# MAGIC types and 3 zones. You need a row for every combination so you can assign
# MAGIC a base rate to each. That's `crossJoin` — explicit, readable, intentional.
# MAGIC After the crossJoin, you add a rate column based on service type.
# MAGIC
# MAGIC **Accidental anti-pattern:** You write a Boolean join condition but use
# MAGIC `F.lit(True)` instead of an actual column comparison. The condition is always
# MAGIC true, so every left row matches every right row. Same 9 rows, but it's a bug.
# MAGIC
# MAGIC The danger isn't on small data (3 × 3 = 9). It's on production:
# MAGIC 100K trips × 100K payments = **10 billion rows**. No error message — your
# MAGIC cluster just runs out of memory or hangs for hours.
# MAGIC
# MAGIC **How to spot it:** if your join output is dramatically larger than either
# MAGIC input and you didn't expect it, check the join condition. An always-true
# MAGIC condition or a missing key is usually the cause.

# COMMAND ----------

# DBTITLE 1,Intentional crossJoin vs accidental Cartesian
# Intentional: generate a pricing grid (all service × zone combinations)
service_types = spark.createDataFrame(  # noqa: F821
    [("Premium",), ("Standard",), ("XL",)], ["service_type"]
)
zones = spark.createDataFrame(
    [("Manhattan",), ("Brooklyn",), ("Queens",)], ["zone"]
)

pricing_grid = service_types.crossJoin(zones).withColumn(
    "base_rate",
    F.when(F.col("service_type") == "Premium", 25.00)
    .when(F.col("service_type") == "XL", 20.00)
    .otherwise(12.00),
)
print(f"Intentional crossJoin: 3 service types × 3 zones = {pricing_grid.count()} rows")
print("Every combination gets a base rate:")
pricing_grid.show()

# Accidental: forgot the join key, used always-true condition
print("Accidental — F.lit(True) instead of 'trip_id':")
accidental = service_types.join(zones, F.lit(True), "inner")
print(f"Same result: {accidental.count()} rows")
print("\u2192 On production (100K × 100K) this would be 10 billion rows.")
print("\u2192 Rule of thumb: if row count is close to len(left) * len(right), suspect a missing or always-true join condition.")

# COMMAND ----------

# DBTITLE 1,6. Exercise
# MAGIC %md
# MAGIC ## 6. Exercise — full pre-join and post-join validation
# MAGIC
# MAGIC **Scenario:** A `driver_payouts` table arrived from the finance system. It
# MAGIC should have one payout per trip, but the extract has issues you need to
# MAGIC detect.
# MAGIC
# MAGIC Your task:
# MAGIC 1. Profile `driver_payouts` on `trip_id` (rows, distinct, nulls)
# MAGIC 2. Look at the profile result — is it safe to inner join with `trip`?
# MAGIC 3. Predict: if you inner join `trip` (100 rows, unique) with
# MAGIC    `driver_payouts` as-is, how many rows will you get?
# MAGIC 4. Replace `None` with your prediction, run, and verify
# MAGIC
# MAGIC **Think about:** Does the profile show duplicates? NULLs? What failure mode
# MAGIC from this notebook would you hit?

# COMMAND ----------

# DBTITLE 1,Exercise: profile driver_payouts, predict, then verify
# Finance extract — has issues you need to detect
driver_payouts = spark.createDataFrame(  # noqa: F821
    [
        (1, 18.50),
        (2, 10.00),
        (2, 10.00),   # duplicate — payout processed twice
        (3, 30.00),
        (None, 5.00), # NULL — couldn't link to a trip
    ],
    ["trip_id", "payout_amount"],
)

# Step 1: Profile driver_payouts
print("driver_payouts:")
driver_payouts.show()

payout_stats = driver_payouts.select(
    F.count("*").alias("rows"),
    F.countDistinct("trip_id").alias("distinct"),
    F.sum(F.when(F.col("trip_id").isNull(), 1).otherwise(0)).alias("nulls"),
).collect()[0]
print(f"Profile: rows={payout_stats['rows']}, distinct={payout_stats['distinct']}, nulls={payout_stats['nulls']}")

print()

# Step 2: YOUR PREDICTION — inner join trip (100 unique) with driver_payouts (as-is)
# Think: trip has keys [1..100]. driver_payouts has keys [1, 2, 2, 3, NULL].
# Which keys overlap? What about the duplicate? What about the NULL?
predicted_inner = None

# Step 3: Verify
actual_inner = trip.join(driver_payouts, "trip_id", "inner").count()
mark = "✓" if predicted_inner == actual_inner else "✗"
print(f"{mark} inner \u2192 predicted={predicted_inner}, actual={actual_inner}")
print("\nWhy 4? trip_id 1 \u2192 1 match, trip_id 2 \u2192 2 matches (the duplicate payout),")
print("trip_id 3 \u2192 1 match, NULL \u2192 0 matches (standard equality never matches NULL).")

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Summary — what to do before every join
# MAGIC
# MAGIC **The workflow:**
# MAGIC
# MAGIC 1. **Profile** both sides on the join key (rows, distinct, nulls)
# MAGIC 2. **Decide** — is it safe to join as-is?
# MAGIC    * rows ≠ distinct → resolve duplicates (window ranking, not dropDuplicates)
# MAGIC    * nulls > 0 → handle before inner join (or use eqNullSafe)
# MAGIC 3. **Predict** the output row count based on what you know
# MAGIC 4. **Run** the join
# MAGIC 5. **Verify** — actual == predicted? If not, investigate.
# MAGIC
# MAGIC **What this catches:**
# MAGIC * M:M fanout (rows multiply because duplicates exist on both sides)
# MAGIC * NULL-key loss (rows vanish from inner joins)
# MAGIC * Accidental Cartesian (output explodes because join condition is wrong)
# MAGIC
# MAGIC **What this does NOT catch:** value-level errors. Your row count can be
# MAGIC correct but the joined values can still be wrong (e.g., matching to the
# MAGIC wrong record). That requires checking output columns against business
# MAGIC expectations (Notebook 07).
# MAGIC
# MAGIC **Next:** `03 - Lookup Joins, Columns, and Broadcast`