# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 04 - Semi Joins and Anti Joins
# MAGIC
# MAGIC A semi join keeps the left rows that have a match on the right. 
# MAGIC
# MAGIC An anti join keeps the left rows that have no match. Both return only the left DataFrame's columns.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Use `left_semi` to keep left rows that have at least one match on the right.
# MAGIC - Use `left_anti` to find left rows with no match on the right.
# MAGIC - Confirm that the combined row counts from left_semi and left_anti equal the left DataFrame's row count.
# MAGIC - Explain how swapping the left and right DataFrames changes which unmatched keys you find.
# MAGIC - Compare `left_anti` with `subtract()`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and source data
# MAGIC
# MAGIC Attach **all-purpose compute**.
# MAGIC
# MAGIC - `trip` contains 106 rows, with one completed trip per `trip_id`.
# MAGIC - `payment` contains 105 rows, with one fare record per `trip_id`. Trip 106 has no matching payment record.

# COMMAND ----------

curated_root = "/Volumes/rideshare_dev/processed/output_files/curated"

trip = spark.read.format("parquet").load(f"{curated_root}/trip")  # noqa: F821
payment = spark.read.format("parquet").load(f"{curated_root}/payment")  # noqa: F821

print("trip rows:", trip.count())
print("payment rows:", payment.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Left Semi Join
# MAGIC
# MAGIC `left_semi` returns rows from `trip` that have at least one matching row in `payment`.
# MAGIC Each matching row from `trip` appears once, regardless of how many payment rows match.
# MAGIC The result contains only columns from `trip`.

# COMMAND ----------

trips_with_payment = trip.join(payment, "trip_id", "left_semi")

print("left_semi rows:", trips_with_payment.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compare with an inner join
# MAGIC
# MAGIC Both joins return 105 rows in this example because each trip has at most one matching payment row.
# MAGIC An inner join also includes columns from `payment`, so it returns 22 columns compared with 13 for `left_semi`.
# MAGIC If a trip had two matching payment rows, the inner join would return that trip twice, while `left_semi` would return it once.

# COMMAND ----------

inner_result = trip.join(payment, "trip_id", "inner")

print("inner join rows:", inner_result.count(), "columns:", len(inner_result.columns))

print("left_semi rows:",trips_with_payment.count(),"columns:",len(trips_with_payment.columns),)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Left Anti Join
# MAGIC
# MAGIC `left_anti` returns rows from `trip` that have no matching row in `payment`. In this example, trip 106 has no payment.
# MAGIC
# MAGIC Every trip appears in either the `left_semi` or `left_anti` result, but not both.
# MAGIC
# MAGIC Therefore, the combined row counts from both joins equal the total row count of `trip`.

# COMMAND ----------

trips_without_payment = trip.join(payment, "trip_id", "left_anti")

trips_without_payment.select("trip_id").show()

semi_count = trips_with_payment.count()
anti_count = trips_without_payment.count()

print(f"semi ({semi_count}) + anti ({anti_count}) = {semi_count + anti_count}")
print("trip rows:", trip.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Anti Join vs `subtract()`
# MAGIC
# MAGIC `subtract()` can also find trip IDs with no matching payment. It is a set operation, not a join.
# MAGIC
# MAGIC Here, we select `trip_id` from both DataFrames because `subtract()` compares all columns in its input.
# MAGIC
# MAGIC Both operations identify trip 106, but they differ in two ways:
# MAGIC
# MAGIC | | `left_anti` | `subtract()` |
# MAGIC |---|---|---|
# MAGIC | Columns returned in this example | All `trip` columns | Only `trip_id` |
# MAGIC | Duplicate rows | Keeps duplicate left rows | Removes duplicates |
# MAGIC
# MAGIC The duplicate difference is not visible here because each `trip_id` appears once in `trip`.
# MAGIC
# MAGIC Set operations continue in notebook 06.

# COMMAND ----------

subtract_result = trip.select("trip_id").subtract(payment.select("trip_id"))

print("subtract on trip_id:", subtract_result.count())
subtract_result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Key Takeaway
# MAGIC
# MAGIC - `left_semi` returns left rows that have at least one match on the right, without duplicating them based on right-side matches.
# MAGIC - `left_anti` returns left rows that have no match on the right.
# MAGIC - Both joins return only columns from the left DataFrame.
# MAGIC - The combined row counts from `left_semi` and `left_anti` equal the left DataFrame's row count when using the same join condition.
# MAGIC - `subtract()` returns distinct unmatched rows based on the selected columns, while `left_anti` preserves duplicate left rows.
# MAGIC
# MAGIC **Next:** `05 - Union and unionByName`