# Databricks notebook source
# MAGIC %md
# MAGIC # 10 - Sampling
# MAGIC
# MAGIC Draw reproducible subsets of rows with `sample`, `sampleBy`, and `randomSplit`.
# MAGIC
# MAGIC `trip_enriched`, `trip_driver_assignment`.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Draw reproducible samples with `sample` / `sampleBy` / `randomSplit`
# COMMAND ----------

# DBTITLE 1,Setup — load both managed tables
# MAGIC %md
# MAGIC ## Setup — load both managed tables
# MAGIC
# MAGIC | DataFrame | Rows | Used for |
# MAGIC |---|---:|---|
# MAGIC | `trip_enriched` | 106 | Sections 1–2 (`sample` and `sampleBy`) |
# MAGIC | `trip_driver_assignment` | 100 | Section 3 (`randomSplit`) |

# COMMAND ----------

# DBTITLE 1,Load and verify the managed tables
trip_enriched_table = "rideshare_dev.processed.trip_enriched"
trip_driver_assignment_table = "rideshare_dev.processed.trip_driver_assignment"

trip_enriched = spark.table(trip_enriched_table)  # noqa: F821
trip_driver_assignment = spark.table(trip_driver_assignment_table)  # noqa: F821

trip_enriched_rows = trip_enriched.count()
trip_driver_assignment_rows = trip_driver_assignment.count()

print(f"trip_enriched: {trip_enriched_rows} rows")
print(f"trip_driver_assignment: {trip_driver_assignment_rows} rows")

# COMMAND ----------

# DBTITLE 1,How do we draw a reproducible subset of rows?
# MAGIC %md
# MAGIC ## 1. `sample` — approximate fraction and seed reproducibility
# MAGIC
# MAGIC Before running a fare audit across the full trip dataset, you may want a
# MAGIC smaller subset for quick spot-checks. Spark provides three useful options:
# MAGIC
# MAGIC | API | Use it when |
# MAGIC |---|---|
# MAGIC | `sample` | Select an approximate fraction of rows |
# MAGIC | `sampleBy` | Sample different fractions for different key values |
# MAGIC | `randomSplit` | Split the DataFrame into multiple subsets |
# MAGIC
# MAGIC Use a **`seed`** when you want the random selection to be repeatable for the
# MAGIC same input data and sampling configuration.
# MAGIC
# MAGIC `fraction` is a per-row probability, not a guaranteed row count — so
# MAGIC `0.2` means "about 20%," not exactly one-fifth of the table.
# MAGIC
# MAGIC The next cells draw a seeded sample, then confirm the same seed on the
# MAGIC same input returns the same `trip_id` set.

# COMMAND ----------

# DBTITLE 1,sample with seed 42
trip_sample_a = trip_enriched.sample(
    withReplacement=False,  # each row can appear at most once
    fraction=0.2,  # aim for about 20% of rows (not an exact count)
    seed=42,  # fixed starting number so a rerun keeps the same rows
)

trip_sample_a_rows = trip_sample_a.count()

print(f"sample (~0.2, seed=42): {trip_sample_a_rows} rows from {trip_enriched_rows}")

trip_sample_a.select(
    "trip_id",
    "service_type",
    "pickup_borough",
    "trip_distance_miles",
).show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Verify the same seed reproduces the sample
trip_sample_b = trip_enriched.sample(
    withReplacement=False,  # each row can appear at most once
    fraction=0.2,  # aim for about 20% of rows (not an exact count)
    seed=42,  # fixed starting number so a rerun keeps the same rows
)

trip_sample_b_rows = trip_sample_b.count()

print(f"Sample B: {trip_sample_b_rows} rows (same as A)")

only_in_a = (
    trip_sample_a.select("trip_id")
    .subtract(trip_sample_b.select("trip_id"))
    .count()
)
only_in_b = (
    trip_sample_b.select("trip_id")
    .subtract(trip_sample_a.select("trip_id"))
    .count()
)
print(
    f"Trips only in A: {only_in_a}, only in B: {only_in_b} — "
    "same seed produces identical subsets"
)

# COMMAND ----------

# DBTITLE 1,Which service types get sampled?
# MAGIC %md
# MAGIC ## 2. `sampleBy` — stratified sampling by key
# MAGIC
# MAGIC `sampleBy` samples rows **within each key** using the fraction assigned to that
# MAGIC key.
# MAGIC
# MAGIC Any key **not included** in the fractions map gets a sampling fraction of **0**
# MAGIC and is therefore excluded.
# MAGIC
# MAGIC Here, the map includes four service types but omits **`UNKNOWN`**. The full
# MAGIC dataset contains **2 UNKNOWN rows**, but the sampled result should contain
# MAGIC **0**.

# COMMAND ----------

# DBTITLE 1,Draw stratified sample without UNKNOWN
service_type_fractions = {
    "STANDARD": 0.2,
    "SHARED": 0.2,
    "PREMIUM": 0.2,
    "XL": 0.2,
}

trip_sample_by_service = trip_enriched.sampleBy(
    "service_type",
    fractions=service_type_fractions,  # rate per service_type; missing keys are skipped
    seed=42,  # fixed starting number so a rerun keeps the same sample
)

print("full counts by service_type:")
trip_enriched.groupBy("service_type").count().orderBy("service_type").show(
    truncate=False,
)

print("sampleBy counts by service_type (UNKNOWN omitted from fractions):")
trip_sample_by_service.groupBy("service_type").count().orderBy("service_type").show(
    truncate=False,
)

# COMMAND ----------

# DBTITLE 1,Why can XL be missing from the sample counts?
# MAGIC %md
# MAGIC ### Why can `XL` be missing from the sample counts?
# MAGIC
# MAGIC `XL` is in the fractions map at **0.2**, and the full table has **12** XL
# MAGIC trips. About 20% of 12 is roughly **2** rows — not a guaranteed count.
# MAGIC
# MAGIC With this seed, the sample kept **0** XL rows, so `groupBy` does not show
# MAGIC that key.

# COMMAND ----------

# DBTITLE 1,How do we split rows into seeded subsets?
# MAGIC %md
# MAGIC ## 3. `randomSplit` — split rows into seeded subsets
# MAGIC
# MAGIC Use **`trip_driver_assignment`** here because it has **100 rows** and no NULL
# MAGIC trip dates, making the split easier to inspect.
# MAGIC
# MAGIC Sections **1–2** used **`trip_enriched`** because `sampleBy` needed the
# MAGIC **`service_type`** column.
# MAGIC
# MAGIC With `randomSplit([0.7, 0.3], seed=...)`, Spark aims for an approximately
# MAGIC **70/30 split**. The exact number of rows in each subset can vary.
# MAGIC
# MAGIC For the same input and seed, the split is reproducible.

# COMMAND ----------

# DBTITLE 1,Split assignment rows ~70/30 with seed 42
subset_a, subset_b = trip_driver_assignment.randomSplit(
    [0.7, 0.3],  # aim for about 70% then 30% (counts can vary)
    seed=42,  # fixed starting number so a rerun keeps the same split
)

subset_a_rows = subset_a.count()
subset_b_rows = subset_b.count()

print(f"\nsubset A ({subset_a_rows} rows):")
subset_a.select(
    "driver_id",
    "trip_id",
    "trip_distance_miles",
).show(5, truncate=False)

print(f"subset B ({subset_b_rows} rows):")
subset_b.select(
    "driver_id",
    "trip_id",
    "trip_distance_miles",
).show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | API | Takeaway |
# MAGIC |---|---|
# MAGIC | `sample` | Selects an approximate fraction of rows; use a `seed` for repeatable results |
# MAGIC | `sampleBy` | Samples each key value at its own fraction; omitted keys receive fraction `0` |
# MAGIC | `randomSplit` | Splits a DataFrame into weighted subsets; use a `seed` for repeatable splits |
# MAGIC
# MAGIC **Next:** Module 8 **`11 - Build KPI Tables`** — write three managed
# MAGIC `kpi_*` Delta tables for Module 9.
