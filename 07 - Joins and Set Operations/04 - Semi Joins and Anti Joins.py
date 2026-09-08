# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Semi Joins and Anti Joins
# MAGIC
# MAGIC `left_semi` / `left_anti` on curated trip vs payment.
# MAGIC
# MAGIC Curated `trip/` (106), curated `payment/` (105).
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Use `left_semi` / `left_anti`, and contrast anti-join with `subtract()`
# MAGIC   (bridge to **06**)
# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %md
# MAGIC ## Setup — load curated trip and payment
# MAGIC
# MAGIC | Table | Format | Grain | Key | Rows | Used in |
# MAGIC |---|---|---|---|---|---|
# MAGIC | `curated/trip` | Parquet | one completed trip | `trip_id` | 106 | Sections 1–4 |
# MAGIC | `curated/payment` | Parquet | one fare record per trip | `trip_id` | 105 | Sections 1–2 |
# MAGIC | Landing `trip_time` | Parquet | one time record per trip | `trip_id` | 100 | Practice (Section 3 only) |
# MAGIC
# MAGIC `curated/payment` has 105 rows — one fewer than `curated/trip`. Trip 106
# MAGIC exists in trip but has no payment record. That gap drives the anti-join
# MAGIC demo in Section 2.
# MAGIC
# MAGIC Trips 101–106 have no record in `trip_time` (which covers only the original
# MAGIC 100 trips). That 6-row gap drives the Section 3 practice.

# COMMAND ----------

# DBTITLE 1,Load curated trip, payment, and landing trip_time
curated_root = "/Volumes/rideshare_dev/processed/output_files/curated"
landing_root = "/Volumes/rideshare_dev/landing/source_files"

trip = spark.read.format("parquet").load(f"{curated_root}/trip")  # noqa: F821
payment = spark.read.format("parquet").load(f"{curated_root}/payment")  # noqa: F821
trip_time = spark.read.format("parquet").load(  # noqa: F821
    f"{landing_root}/trip_time/trip_time.parquet"
)

print("trip rows:", trip.count())
print("payment rows:", payment.count())
print("trip_time rows:", trip_time.count())

# COMMAND ----------

# DBTITLE 1,1. left_semi
# MAGIC %md
# MAGIC ## 1. `left_semi` — keep trips that have a payment
# MAGIC
# MAGIC **Question:** which of the 106 trips already have a payment record?

# COMMAND ----------

# DBTITLE 1,1. Run left_semi and verify
trips_with_payment = trip.join(payment, "trip_id", "left_semi")

print("left_semi row count:", trips_with_payment.count())
trips_with_payment.show(1, truncate=False, vertical=True)

# COMMAND ----------

# MAGIC %md
# MAGIC The key difference from an inner join: **semi returns only left-side
# MAGIC columns**. No payment columns appear in the result.

# COMMAND ----------

# DBTITLE 1,1. Contrast with inner join column count
inner_result = trip.join(payment, "trip_id", "inner")

print(f"inner join: {inner_result.count()} rows, {len(inner_result.columns)} columns")

print(f"left_semi:  {trips_with_payment.count()} rows, {len(trips_with_payment.columns)} columns")

print(f"\n\u2192 Same qualifying rows, but semi keeps only the {len(trip.columns)} trip columns")

# COMMAND ----------

# DBTITLE 1,2. left_anti
# MAGIC %md
# MAGIC ## 2. `left_anti` — find trips without a payment
# MAGIC
# MAGIC **Question:** which trips have no payment record at all?
# MAGIC
# MAGIC **Production use cases:**
# MAGIC - **Completeness audits** — find orders without shipments, users without logins
# MAGIC - **Incremental loads** — identify new records that haven't been processed yet
# MAGIC - **Orphan detection** — fact rows referencing deleted dimension keys

# COMMAND ----------

# DBTITLE 1,2. Run left_anti and verify
trips_without_payment = trip.join(payment, "trip_id", "left_anti")

print("left_anti row count:", trips_without_payment.count())

# Verify: semi + anti must account for every row in the driving table
semi_count = trips_with_payment.count()

print("\nPayment missing for this trip:")
trips_without_payment.show(2, truncate=False, vertical=True)

anti_count = trips_without_payment.count()
print(f"Verify: semi({semi_count}) + anti({anti_count}) = {semi_count + anti_count} == trip.count({trip.count()})")

# COMMAND ----------

# DBTITLE 1,2c. Reverse anti
# MAGIC %md
# MAGIC ### 2c. Reverse anti — flip the driving side
# MAGIC
# MAGIC Same two tables, different question: are there any payments that have **no
# MAGIC matching trip**?

# COMMAND ----------

# DBTITLE 1,2c. Reverse anti — payments without a trip
payments_without_trip = payment.join(trip, "trip_id", "left_anti")

print("Payments without a matching trip:", payments_without_trip.count(), "rows")

# COMMAND ----------

# DBTITLE 1,3. Practice
# MAGIC %md
# MAGIC ## 3. Practice — a second intentional gap (trip_time)
# MAGIC
# MAGIC Landing `trip_time` has 100 rows covering trip_ids 1–100. The curated trip
# MAGIC table has 106 rows (trip_ids 1–106). Trips 101–106 were added during Module 6
# MAGIC cleaning and have **no time record**.
# MAGIC
# MAGIC **Your task:** apply semi and anti joins to this second pair using Boolean
# MAGIC form with aliases (Notebook 01 Section 3.3).
# MAGIC
# MAGIC | Join | Driving side | Right side | Predicted rows |
# MAGIC |---|---|---|---|
# MAGIC | `left_semi` | `trip` (106) | `trip_time` (100) | ? |
# MAGIC | `left_anti` | `trip` (106) | `trip_time` (100) | ? |

# COMMAND ----------

# DBTITLE 1,3. Practice - left_semi on trip_time
# TODO (practice): Write a left_semi join from `trip` to `trip_time`
# using Boolean form with aliases.
#
# Steps:
#   1. Alias trip as "t" and trip_time as "tt"
#   2. Join: t.join(tt, <Boolean condition>, "left_semi")
#   3. Print the count — does it match your prediction?


# COMMAND ----------

# DBTITLE 1,3b. Now find what's missing
# MAGIC %md
# MAGIC ### Now find what’s missing
# MAGIC
# MAGIC Predict before running: how many trips have **no** time record? Which
# MAGIC trip_ids are they?

# COMMAND ----------

# DBTITLE 1,3. Practice - left_anti on trip_time
# TODO (practice): Write a left_anti join from `trip` to `trip_time`
# using Boolean form with aliases.
#
# Steps:
#   1. Alias trip as "t" and trip_time as "tt"
#   2. Join: t.join(tt, <Boolean condition>, "left_anti")
#   3. Print the count and display the trip_ids
#   4. Also confirm: semi count + anti count = 106


# COMMAND ----------

# DBTITLE 1,4. Bridge to subtract()
# MAGIC %md
# MAGIC ## 4. Bridge to `subtract()`
# MAGIC
# MAGIC `subtract()` returns rows from the first DataFrame that do not appear in the second DataFrame. It compares all columns in the DataFrames passed to it.
# MAGIC
# MAGIC Therefore, use `select()` on both sides when you want to compare only specific columns.
# MAGIC
# MAGIC ```python
# MAGIC trip.select("trip_id").subtract(
# MAGIC     payment.select("trip_id")
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC Here, Spark compares only `trip_id` because that is the only column present after the projection.
# MAGIC
# MAGIC Unlike `left_anti`, `subtract()` removes duplicate results.
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,4. subtract on trip_id — same result as anti-join
subtract_result = trip.select("trip_id").subtract(payment.select("trip_id"))

print("subtract on trip_id:", subtract_result.count(), "row(s)")
subtract_result.show()

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC 1. **`left_semi`** keeps left rows for which a match exists on the right.
# MAGIC    No right-side columns appear.
# MAGIC
# MAGIC 2. **`left_anti`** keeps left rows for which no match exists on the right.
# MAGIC    The result also contains only left-side columns.
# MAGIC
# MAGIC 3. **Direction matters** — the left DataFrame is always the driving side.
# MAGIC    Swapping which DataFrame is left changes which gaps you detect.
# MAGIC
# MAGIC 4. **Exhaustive-split check** — when both joins use the same key:
# MAGIC
# MAGIC    ```text
# MAGIC    semi count + anti count = left DataFrame count
# MAGIC    ```
# MAGIC
# MAGIC    Every left row appears in exactly one of the two results.
# MAGIC
# MAGIC 5. **Anti join and `subtract()` overlap but differ:**
# MAGIC
# MAGIC    * `left_anti` evaluates a join key and preserves duplicate left rows.
# MAGIC    * `subtract()` compares all selected columns and returns only distinct
# MAGIC      rows.
# MAGIC
# MAGIC    Full set-operation coverage → Notebook 06.
# MAGIC
# MAGIC **Next:** **`05 - Union and unionByName`**