# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 10 - Sampling
# MAGIC
# MAGIC Production datasets can contain millions or billions of rows. During development, validation, or data-quality checks, processing the entire dataset every time may be unnecessary. 
# MAGIC
# MAGIC A smaller, random subset is often enough to inspect the data, test pipeline logic, or prepare data for downstream analysis.
# MAGIC
# MAGIC In this lesson, you'll draw random subsets from `trip_enriched`
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Use `sample()` to select a random portion of rows from a DataFrame, such as about 10%.
# MAGIC - Use `sampleBy()` to select a different portion from each group, and see that any group you don't list is left out.
# MAGIC - Use `randomSplit()` to divide a DataFrame into separate parts, such as about 70% and 30%, with no row appearing in both.
# MAGIC - Use a `seed` so you get the same random rows each time you rerun the code.

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach **all-purpose compute**.
# MAGIC
# MAGIC This notebook uses `rideshare_dev.processed.trip_enriched` created in Module 7, **07 - Build Unified Curated Tables**, which contains 106 rows (one row per trip).

# COMMAND ----------

trip_enriched = spark.table("rideshare_dev.processed.trip_enriched")  # noqa: F821

print("trip_enriched rows:", trip_enriched.count())

# COMMAND ----------

# DBTITLE 1,Section 1 - sample()
# MAGIC %md
# MAGIC ## 1. `sample()` — Pick a random portion of rows
# MAGIC
# MAGIC **Question:** How do we pick about 10% of trips at random for a quick look, and get the same trips every time we rerun the code?
# MAGIC
# MAGIC - `withReplacement=False` — a trip can appear only once in the sample.
# MAGIC - `fraction=0.1` — each row has a 10% chance of being picked, so the result is *about* 10%, not exactly 10%.
# MAGIC - `seed=42` — returns the same rows on every run, as long as the input data hasn't changed.

# COMMAND ----------

sampled_trips = trip_enriched.sample(
    withReplacement=False,
    fraction=0.1,
    seed=42,
)

print("Sampled rows:", sampled_trips.count())

sampled_trips.select(
    "trip_id",
    "service_type",
    "pickup_borough",
    "trip_distance_miles",
).orderBy("trip_id").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify that the same `seed` returns the same rows
# MAGIC
# MAGIC Running `sample()` again with `seed=42` returns the exact same set of `trip_id` values.

# COMMAND ----------

sampled_trips_rerun = trip_enriched.sample(
    withReplacement=False,
    fraction=0.1,
    seed=42,
)

sampled_trips_rerun.select(
    "trip_id",
    "service_type",
    "pickup_borough",
    "trip_distance_miles",
).orderBy("trip_id").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Section 2 - sampleBy()
# MAGIC %md
# MAGIC ## 2. `sampleBy()` — Pick a different portion from each group
# MAGIC
# MAGIC **Question:** `sample()` picks rows from the whole table without considering `service_type`. How do we pick a different percentage from each `service_type`, and leave out `UNKNOWN` trips?
# MAGIC
# MAGIC First, count the trips in each `service_type`.

# COMMAND ----------

trip_enriched.groupBy("service_type").count().orderBy("service_type").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Pass a `fractions` dictionary to `sampleBy()`. Each key is a `service_type`, and its value specifies the probability of selecting each trip of that type. For example, `0.2` means each trip has a 20% chance of being selected, not that exactly 20% of trips will be returned.
# MAGIC
# MAGIC Any `service_type` missing from the dictionary, such as `"UNKNOWN"`, has a sampling fraction of `0`, so its trips are excluded.

# COMMAND ----------

service_fractions = {
    "STANDARD": 0.2,
    "SHARED": 0.3,
    "PREMIUM": 0.3,
    "XL": 0.5,
}

stratified_sample = trip_enriched.sampleBy(
    "service_type",
    fractions=service_fractions,
    seed=42,
)

stratified_sample.groupBy("service_type").count().orderBy("service_type").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Section 3 - randomSplit()
# MAGIC %md
# MAGIC ## 3. `randomSplit()` — Split rows into separate parts
# MAGIC
# MAGIC **Question:** `sample()` and `sampleBy()` each return one set of rows. How do we split `trip_enriched` into two separate parts, about 70% and 30%, with no trip appearing in both?

# COMMAND ----------

trips_70, trips_30 = trip_enriched.randomSplit([0.7, 0.3], seed=42)

print("Total rows:", trip_enriched.count())
print("70% subset rows:", trips_70.count())
print("30% subset rows:", trips_30.count())

# COMMAND ----------

# DBTITLE 1,Key Takeaways
# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC - Use `sample()` to pick a random portion of rows, for example about 10%.
# MAGIC - Use `sampleBy()` to pick a different portion from each group. Groups not listed in `fractions` are skipped.
# MAGIC - Use `randomSplit()` to divide rows into separate parts, for example about 70% and 30%. Each row lands in exactly one part.
# MAGIC - All three pick rows by chance, so counts are close to your target but not exact.
# MAGIC - Add a `seed` to get the same rows every time you rerun, as long as the data hasn't changed.
# MAGIC
# MAGIC **Next:** `11 - Build KPI Tables`