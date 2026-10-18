# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Union and unionByName
# MAGIC
# MAGIC Combine DataFrame rows using `union()` (column position) and `unionByName()` (column name). This lab also covers missing columns and duplicate rows.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and source data
# MAGIC
# MAGIC Attach **all-purpose compute**. Create `morning` (trips 1–2) and `afternoon` (trips 3–4) with the same columns.

# COMMAND ----------

from pyspark.sql import functions as F

morning = spark.createDataFrame(  # noqa: F821
    [(1, 10, 20), (2, 11, 21)],
    ["trip_id", "pickup_location_id", "dropoff_location_id"],
)
afternoon = spark.createDataFrame(  # noqa: F821
    [(3, 12, 22), (4, 13, 23)],
    ["trip_id", "pickup_location_id", "dropoff_location_id"],
)

morning.show()
afternoon.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `union()` — Match Columns by Position
# MAGIC
# MAGIC `union()` matches columns by position, requires the same number of columns, and preserves duplicate rows (like SQL `UNION ALL`). Corresponding column types must be compatible.

# COMMAND ----------

combined = morning.union(afternoon)

print("union rows:", combined.count())
combined.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Column order matters
# MAGIC
# MAGIC Swap the pickup and dropoff columns in `afternoon`, then use `union()`. Check trips 3 and 4: values appear under the wrong names without an error.

# COMMAND ----------

afternoon_reordered = afternoon.select(
    "trip_id", "dropoff_location_id", "pickup_location_id"
)
afternoon_reordered.show()

bad_union = morning.union(afternoon_reordered)
bad_union.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `unionByName()` — Match Columns by Name
# MAGIC
# MAGIC `unionByName()` matches columns by name, so the reordered pickup and dropoff values stay correct.

# COMMAND ----------

good_union = morning.unionByName(afternoon_reordered)
good_union.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Handle Missing Columns
# MAGIC
# MAGIC `morning` has `dropoff_location_id`; `afternoon_with_tip` has `tip_amount` instead. Without `allowMissingColumns=True`, `unionByName()` raises an `AnalysisException` (caught below).

# COMMAND ----------

from pyspark.errors import AnalysisException

afternoon_with_tip = afternoon.select("trip_id", "pickup_location_id").withColumn(
    "tip_amount", F.lit(5.0)
)

try:
    morning.unionByName(afternoon_with_tip).show()  # Expected: AnalysisException
except AnalysisException as e:
    print("unionByName failed:", str(e).splitlines()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fill missing columns with `NULL`
# MAGIC
# MAGIC `allowMissingColumns=True` includes both sets of columns and fills missing values with `NULL`.

# COMMAND ----------

result = morning.unionByName(afternoon_with_tip, allowMissingColumns=True)
result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Remove Duplicate Rows
# MAGIC
# MAGIC Combining `morning` with itself doubles its rows. `distinct()` removes duplicate rows.

# COMMAND ----------

doubled = morning.union(morning)

print("morning rows:", morning.count())
print("union rows:", doubled.count())
print("distinct rows:", doubled.distinct().count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaway
# MAGIC
# MAGIC - `union()` matches by column position and preserves duplicate rows.
# MAGIC - `unionByName()` matches by column name; `allowMissingColumns=True` fills missing columns with `NULL`.
# MAGIC - `distinct()` removes duplicate rows.
# MAGIC
# MAGIC **Next:** `06 - Intersect, subtract, and exceptAll`
