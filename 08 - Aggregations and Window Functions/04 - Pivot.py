# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Pivot
# MAGIC
# MAGIC Reshape grouped results with `pivot`.
# MAGIC
# MAGIC `trip_enriched`.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Use pivoting to summarize and reshape data
# COMMAND ----------

# MAGIC %md
# MAGIC ## Pivot reshapes grouped results
# MAGIC
# MAGIC A stakeholder asks: for each pickup borough, show trip counts for service types
# MAGIC `STANDARD`, `PREMIUM`, `XL`, `SHARED` and `UNKNOWN` as separate columns.
# MAGIC
# MAGIC A normal `groupBy` can already calculate those counts. Its long result is
# MAGIC still valid for filtering, joining, and writing.
# MAGIC
# MAGIC `pivot` does not add a new calculation. It reshapes those grouped values into
# MAGIC a wider layout that is easier to compare in reports and dashboards.
# MAGIC
# MAGIC ## What this notebook teaches
# MAGIC
# MAGIC One business question, two result shapes:
# MAGIC
# MAGIC | Section | Concept | Why it matters |
# MAGIC |---|---|---|
# MAGIC | 1 | Long `groupBy` | Calculate the result without changing its shape |
# MAGIC | 2 | `pivot` + explicit values | Turn known category values into report columns |
# MAGIC | 3 | When not to pivot | Avoid creating too many columns from high-cardinality data |
# MAGIC | Exercise | Pivot `payment_method` | Apply the same pattern to another small category set |
# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Load `trip_enriched` (one row per trip). Used for every section in this notebook.

# COMMAND ----------

from pyspark.sql import functions as F

trip_enriched_table = "rideshare_dev.processed.trip_enriched"
trip_enriched = spark.table(trip_enriched_table)  # noqa: F821

# COMMAND ----------

# DBTITLE 1,How many trips does each pickup borough have by service type?
# MAGIC %md
# MAGIC ## 1. How many trips does each pickup borough have by service type?
# MAGIC
# MAGIC Answer the stakeholder question in **long format** first — one row per existing
# MAGIC (`pickup_borough`, `service_type`) pair.
# MAGIC
# MAGIC | pickup_borough | service_type | trip_count |
# MAGIC |---|---|---:|
# MAGIC | Manhattan | STANDARD | … |
# MAGIC | Manhattan | PREMIUM | … |
# MAGIC | Queens | STANDARD | … |
# MAGIC
# MAGIC This result already contains the numbers the stakeholder needs. You can filter,
# MAGIC join, re-aggregate, or write it without using `pivot`.
# MAGIC
# MAGIC Section 2 will keep the same counts and only reshape them into columns.
# MAGIC
# MAGIC `groupBy` decides the **rows**.

# COMMAND ----------

trips_by_borough_service = (
    trip_enriched.groupBy("pickup_borough", "service_type")
    .agg(F.count("trip_id").alias("trip_count"))
)

trips_by_borough_service.orderBy("pickup_borough", "service_type").show(50)

# COMMAND ----------

# DBTITLE 1,How can we show service types as columns?
# MAGIC %md
# MAGIC ## 2. How can we show service types as columns?
# MAGIC
# MAGIC Answer the same business question as Section 1 using the same `count(trip_id)`
# MAGIC values, but reshape the result into **wide format** so each service type becomes
# MAGIC a separate column.
# MAGIC
# MAGIC | pickup_borough | STANDARD | PREMIUM | XL | SHARED | UNKNOWN |
# MAGIC |---|---:|---:|---:|---:|---:|
# MAGIC | Manhattan | … | … | … | … | … |
# MAGIC | Queens | … | … | … | … | … |
# MAGIC
# MAGIC Mental model:
# MAGIC
# MAGIC - `groupBy` → rows (`pickup_borough`)
# MAGIC - `pivot` → columns (`service_type` values)
# MAGIC - `agg` → cell values (`count(trip_id)`)
# MAGIC
# MAGIC Pass an **explicit list of pivot values** so the output columns stay predictable.
# MAGIC
# MAGIC A NULL cell means that no row exists for that borough × service type combination
# MAGIC in this dataset. It does not mean the combination can never occur.

# COMMAND ----------

service_types = ["STANDARD", "PREMIUM", "XL", "SHARED", "UNKNOWN"]

trips_by_borough_wide = (
    trip_enriched.groupBy("pickup_borough")
    .pivot("service_type", service_types)
    .agg(F.count("trip_id"))
)

trips_by_borough_wide.orderBy("pickup_borough").show()

# COMMAND ----------

# DBTITLE 1,When should we avoid pivoting?
# MAGIC %md
# MAGIC ## 3. When should we avoid pivoting?
# MAGIC
# MAGIC Keep the data in **long format** while you still need to filter, join,
# MAGIC re-aggregate, or write it.
# MAGIC
# MAGIC Use `pivot` when a **small, known set of categories** needs to become columns
# MAGIC for comparison or reporting.
# MAGIC
# MAGIC - Good: `pivot("service_type")` — a small set of known values
# MAGIC - Avoid: `pivot("trip_id")` — each distinct ID could become a separate column
# MAGIC
# MAGIC As the number of pivot values grows, the output becomes wider and harder to
# MAGIC manage.
# MAGIC
# MAGIC **Rule:** Pivot small, known category sets. Otherwise, keep the long `groupBy`
# MAGIC result.

# COMMAND ----------

print("service_type distinct:", trip_enriched.select("service_type").distinct().count())
print("trip_id distinct:", trip_enriched.select("trip_id").distinct().count())

# COMMAND ----------

# DBTITLE 1,How do payment methods look as columns by pickup borough?
# MAGIC %md
# MAGIC ## Exercise — How do payment methods look as columns by pickup borough?
# MAGIC
# MAGIC Apply the same pivot pattern from Section 2.
# MAGIC
# MAGIC Build one row per `pickup_borough`, turn payment methods into columns, and use
# MAGIC `count(trip_id)` for the cell values.
# MAGIC
# MAGIC Use these explicit pivot values:
# MAGIC
# MAGIC `card`, `wallet`, `cash`, `corporate`, `unknown`
# MAGIC
# MAGIC Notebook 02 showed that one source row has a NULL `payment_method`. Because the
# MAGIC pivot uses an explicit values list, that NULL does not become its own payment
# MAGIC column.
# MAGIC
# MAGIC Predict the number of borough rows, complete the TODOs, then verify the result.

# COMMAND ----------

predicted_borough_groups = None  # TODO: replace with your prediction

payment_methods = None  # TODO: ["card", "wallet", "cash", "corporate", "unknown"]

borough_payment_wide = (
    trip_enriched.groupBy("pickup_borough")
    .pivot("payment_method", payment_methods)
    .agg(F.count("trip_id"))
)

actual = borough_payment_wide.count()
match = "✓" if predicted_borough_groups == actual else "✗"
print(f"{match} predicted={predicted_borough_groups}, actual={actual}")

borough_payment_wide.orderBy("pickup_borough").show()

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Idea | Takeaway |
# MAGIC |---|---|
# MAGIC | Long `groupBy` | Better for processing — filter, join, re-aggregate, write |
# MAGIC | `pivot` | Reshape for reports — categories become columns |
# MAGIC | Explicit values | Keep the column set predictable |
# MAGIC | High cardinality | Do not pivot keys like `trip_id` |
# MAGIC
# MAGIC **Next:** `05 - Window Functions Fundamentals` — keep input rows while adding
# MAGIC ranking and window aggregates.
