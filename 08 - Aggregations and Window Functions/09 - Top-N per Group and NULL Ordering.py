# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 09 - Top-N per Group and NULL Ordering
# MAGIC
# MAGIC In the previous lessons, you used window functions to rank rows, calculate running totals, and compare adjacent rows with `lag()` and `lead()`.
# MAGIC
# MAGIC In this lesson, you will rank trips by `trip_distance_miles` to select the Top-N rows per group, handle ties at the cutoff, and control where `NULL` distances appear when sorting.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Select the Top-N rows per group by ranking with a window and filtering on the rank column.
# MAGIC - Choose between `row_number()` and `rank()` when rows tie at the Top-N cutoff, and add a tie-breaker column for a consistent result.
# MAGIC - Control `NULL` placement in `orderBy()` using `asc_nulls_last()`, `desc_nulls_first()`, and `desc_nulls_last()`.

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach **all-purpose compute**.
# MAGIC
# MAGIC This notebook uses `rideshare_dev.processed.trip_enriched` created in Module 7, **07 - Build Unified Curated Tables**, which contains 106 rows (one row per trip).
# MAGIC
# MAGIC Three trips (`103`, `105`, and `106`) have a `NULL` `trip_distance_miles`.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

trip_enriched = spark.table("rideshare_dev.processed.trip_enriched")  # noqa: F821

print("trip_enriched rows:", trip_enriched.count())

# COMMAND ----------

# DBTITLE 1,Section 1 - Top-N per group
# MAGIC %md
# MAGIC ## 1. Keep the Top-N rows per group
# MAGIC
# MAGIC **Question:** What are the 2 longest trips (`trip_distance_miles`) for each `service_type`?
# MAGIC
# MAGIC First rank trips by `trip_distance_miles` within each `service_type`, then filter to keep rows where the rank is `2` or less.

# COMMAND ----------

service_distance_window = Window.partitionBy("service_type").orderBy(
    F.col("trip_distance_miles").desc()
)

ranked_by_distance = trip_enriched.withColumn(
    "distance_row_number",
    F.row_number().over(service_distance_window),
)

top2_per_service = ranked_by_distance.filter(F.col("distance_row_number") <= 2)

top2_per_service.select(
    "service_type",
    "trip_id",
    "trip_distance_miles",
    "distance_row_number",
).orderBy("service_type", "distance_row_number").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Section 2 - Ties at the Top-N cutoff
# MAGIC %md
# MAGIC ## 2. Handle ties at the Top-N cutoff
# MAGIC
# MAGIC **Question:** In the `SHARED` service type, trips `20` and `96` are tied for 1st place at `8.87` miles. What happens when we ask for the **Top 1** longest `SHARED` trip?
# MAGIC
# MAGIC First, inspect the top 3 `SHARED` trips with both `row_number()` and `rank()`.

# COMMAND ----------

shared_ranked = (
    trip_enriched
    .filter(F.col("service_type") == "SHARED")
    .withColumn("distance_row_number", F.row_number().over(service_distance_window))
    .withColumn("distance_rank", F.rank().over(service_distance_window))
)

shared_ranked.select(
    "service_type",
    "trip_id",
    "trip_distance_miles",
    "distance_row_number",
    "distance_rank",
).orderBy("distance_row_number").show(3, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter with `row_number() <= 1` (keeps strictly 1 row)

# COMMAND ----------

shared_ranked.filter(F.col("distance_row_number") <= 1).select(
    "service_type",
    "trip_id",
    "trip_distance_miles",
    "distance_row_number",
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter with `rank() <= 1` (keeps all rows tied for 1st place)

# COMMAND ----------

shared_ranked.filter(F.col("distance_rank") <= 1).select(
    "service_type",
    "trip_id",
    "trip_distance_miles",
    "distance_rank",
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add a tie-breaker so `row_number()` returns a consistent result
# MAGIC
# MAGIC Because trips `20` and `96` have the same distance (`8.87` miles), `row_number()` has no rule for which trip gets `1` and which gets `2`. Add `F.col("trip_id").asc()` as a second `orderBy()` column so trip `20` always gets `distance_row_number = 1`.

# COMMAND ----------

consistent_distance_window = Window.partitionBy("service_type").orderBy(
    F.col("trip_distance_miles").desc(),
    F.col("trip_id").asc(),
)

(
    trip_enriched
    .filter(F.col("service_type") == "SHARED")
    .withColumn("distance_row_number", F.row_number().over(consistent_distance_window))
    .filter(F.col("distance_row_number") <= 1)
    .select("service_type", "trip_id", "trip_distance_miles", "distance_row_number")
    .show(truncate=False)
)

# COMMAND ----------

# DBTITLE 1,Section 3 - NULL sort placement
# MAGIC %md
# MAGIC ## 3. Control `NULL` placement when sorting
# MAGIC
# MAGIC **Question:** In the `STANDARD` service type, trips `103`, `105`, and `106` have a `NULL` `trip_distance_miles`. Where do those `NULL` rows appear when we rank from shortest to longest (`asc()`) vs longest to shortest (`desc()`)?
# MAGIC
# MAGIC | Method | Sort order | `NULL` placement |
# MAGIC |---|---|---|
# MAGIC | `asc()` | Smallest to largest | **First** (default) |
# MAGIC | `asc_nulls_last()` | Smallest to largest | **Last** |
# MAGIC | `desc()` | Largest to smallest | **Last** (default) |
# MAGIC | `desc_nulls_first()` | Largest to smallest | **First** |
# MAGIC | `desc_nulls_last()` | Largest to smallest | **Last** (explicit) |
# MAGIC
# MAGIC ### Default `asc()` — `NULL` distances appear first
# MAGIC
# MAGIC If you want the shortest `STANDARD` trips and use `.asc()`, the 3 `NULL` rows take ranks `1`, `2`, and `3`.

# COMMAND ----------

asc_default_window = Window.partitionBy("service_type").orderBy(
    F.col("trip_distance_miles").asc(),
    F.col("trip_id").asc(),
)

(
    trip_enriched
    .filter(F.col("service_type") == "STANDARD")
    .withColumn("distance_row_number", F.row_number().over(asc_default_window))
    .select("service_type", "trip_id", "trip_distance_miles", "distance_row_number")
    .orderBy("distance_row_number")
    .show(6, truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### `asc_nulls_last()` — shortest known distances appear first
# MAGIC
# MAGIC Use `asc_nulls_last()` to push `NULL` distances to the bottom so real shortest trips rank first.

# COMMAND ----------

asc_nulls_last_window = Window.partitionBy("service_type").orderBy(
    F.col("trip_distance_miles").asc_nulls_last(),
    F.col("trip_id").asc(),
)

(
    trip_enriched
    .filter(F.col("service_type") == "STANDARD")
    .withColumn("distance_row_number", F.row_number().over(asc_nulls_last_window))
    .select("service_type", "trip_id", "trip_distance_miles", "distance_row_number")
    .orderBy("distance_row_number")
    .show(6, truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### `desc_nulls_last()` — longest known distances appear first
# MAGIC
# MAGIC Both `desc()` and `desc_nulls_last()` place `NULL` distances at the bottom.

# COMMAND ----------

desc_nulls_last_window = Window.partitionBy("service_type").orderBy(
    F.col("trip_distance_miles").desc_nulls_last(),
    F.col("trip_id").asc(),
)

(
    trip_enriched
    .filter(F.col("service_type") == "STANDARD")
    .withColumn("distance_row_number", F.row_number().over(desc_nulls_last_window))
    .select("service_type", "trip_id", "trip_distance_miles", "distance_row_number")
    .orderBy("distance_row_number")
    .show(6, truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### `desc_nulls_first()` — `NULL` distances appear first

# COMMAND ----------

desc_nulls_first_window = Window.partitionBy("service_type").orderBy(
    F.col("trip_distance_miles").desc_nulls_first(),
    F.col("trip_id").asc(),
)

(
    trip_enriched
    .filter(F.col("service_type") == "STANDARD")
    .withColumn("distance_row_number", F.row_number().over(desc_nulls_first_window))
    .select("service_type", "trip_id", "trip_distance_miles", "distance_row_number")
    .orderBy("distance_row_number")
    .show(6, truncate=False)
)

# COMMAND ----------

# DBTITLE 1,Key Takeaways
# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC - Rank rows within each group using a window, then `.filter()` on the rank column to keep the Top-N rows.
# MAGIC - `row_number() <= N` keeps at most `N` rows per group; `rank() <= N` keeps all rows tied at `N`.
# MAGIC - Add a unique column (like `trip_id.asc()`) to `orderBy()` to break ties consistently with `row_number()`.
# MAGIC - By default, `asc()` puts `NULL`s first and `desc()` puts `NULL`s last. Use `asc_nulls_last()`, `desc_nulls_first()`, or `desc_nulls_last()` to change where `NULL`s appear.
# MAGIC
# MAGIC **Next:** `10 - Sampling`