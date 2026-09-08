# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Union and unionByName
# MAGIC
# MAGIC Stack frames with `union` / `unionByName` — constructed frames only.
# MAGIC
# MAGIC Constructed frames (no landing read).
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Combine frames with `union` / `unionByName` (column-order trap;
# MAGIC   `allowMissingColumns`)
# COMMAND ----------

# DBTITLE 1,Union rules
# MAGIC %md
# MAGIC ## Union rules
# MAGIC
# MAGIC 1. `union()` matches columns by **position**.
# MAGIC 2. `unionByName()` matches columns by **name**.
# MAGIC 3. `union()` requires the same **number** of columns; `unionByName()` requires the same column **names** (unless `allowMissingColumns=True`).
# MAGIC 4. Corresponding columns must have **compatible data types**.
# MAGIC 5. `union()` keeps the **left DataFrame's column names** in the result — this is why mismatched column order produces silent corruption.
# MAGIC 6. `unionByName(..., allowMissingColumns=True)` fills missing columns with NULL.
# MAGIC 7. Union keeps duplicate rows. Use `distinct()` only when duplicates are unintended.

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %md
# MAGIC ## Setup — constructed frames
# MAGIC
# MAGIC This notebook uses tiny handmade DataFrames — the same pattern as Notebook 01
# MAGIC and the official PySpark `union` / `unionByName` examples. No landing or
# MAGIC curated read is required.
# MAGIC
# MAGIC | Frame | Rows | Columns |
# MAGIC |---|---:|---|
# MAGIC | `morning` | 2 | `trip_id`, `pickup_location_id`, `dropoff_location_id` |
# MAGIC | `afternoon` | 2 | same as `morning` |

# COMMAND ----------

# DBTITLE 1,Create morning and afternoon
from pyspark.sql import functions as F

morning = spark.createDataFrame(  # noqa: F821
    [(1, 10, 20), (2, 11, 21)],
    ["trip_id", "pickup_location_id", "dropoff_location_id"],
)
afternoon = spark.createDataFrame(  # noqa: F821
    [(3, 12, 22), (4, 13, 23)],
    ["trip_id", "pickup_location_id", "dropoff_location_id"],
)

print("morning:")
morning.show()
print("afternoon:")
afternoon.show()

# COMMAND ----------

# DBTITLE 1,1. union
# MAGIC %md
# MAGIC ## 1. `union` — Combining DataFrames by Position
# MAGIC
# MAGIC
# MAGIC The `union()` function appends the rows of a second DataFrame below those of the first. It matches columns based on their position rather than their names; therefore, the first column in the second DataFrame aligns with the first column in the first DataFrame, and the resulting DataFrame retains the column names from the first DataFrame.
# MAGIC
# MAGIC Both DataFrames must contain the same number of columns, and the data types in matching positions must be compatible. While Spark may convert compatible types, mismatched types will cause the union to fail.
# MAGIC
# MAGIC The `union()` function keeps duplicate rows. It’s important to be aware of the column-order issue. If the two DataFrames have the same columns in a different order, the `union()` function can assign values to the wrong column names without raising any errors. Always verify the column order or consider using `unionByName()`.

# COMMAND ----------

# DBTITLE 1,1. Basic union — same schema and order
combined = morning.union(afternoon)

print("rows:", combined.count())
combined.show()

# COMMAND ----------

# DBTITLE 1,1b. Column-order trap demo
# Same rows as afternoon; pickup and dropoff columns swapped in position
afternoon_reordered = afternoon.select(
    "trip_id", "dropoff_location_id", "pickup_location_id"
)

print("afternoon_reordered column names still correct; but order differs:")
afternoon_reordered.show()

bad_union = morning.union(afternoon_reordered)
print("After union — afternoon pickup/dropoff values are corrupted:")
bad_union.show()

# COMMAND ----------

# DBTITLE 1,2. unionByName
# MAGIC %md
# MAGIC ## 2. `unionByName()` — match by name
# MAGIC
# MAGIC `unionByName()` aligns columns by **name**, so column order does not matter.
# MAGIC This is safer when the same columns may appear in a different order.
# MAGIC
# MAGIC By default, both DataFrames must contain the same column names, and matching
# MAGIC columns must have compatible data types. Missing columns raise an error.
# MAGIC Section 3 shows how to handle them with `allowMissingColumns=True`.
# MAGIC

# COMMAND ----------

# DBTITLE 1,2. unionByName fixes the reorder
good_union = morning.unionByName(afternoon_reordered)

print("unionByName on the same reordered frame:")
good_union.show()
print("→ Pickup and dropoff are correct because columns matched by name")

# COMMAND ----------

# DBTITLE 1,2b. Column mismatch error
# Same morning grain — drop dropoff, add tip_amount (missing on the other side)
full_cols = morning  # trip_id, pickup_location_id, dropoff_location_id
partial_cols = morning.select("trip_id", "pickup_location_id").withColumn(
    "tip_amount", F.lit(5.0)
)

# Default unionByName requires matching column names on both sides
try:
    full_cols.unionByName(partial_cols).show()
except Exception as e:
    print("unionByName fails when column sets differ:")
    print(f"  {str(e).splitlines()[0]}")

# COMMAND ----------

# DBTITLE 1,3. allowMissingColumns
# MAGIC %md
# MAGIC ## 3. `allowMissingColumns` — handle different schemas
# MAGIC
# MAGIC When combining DataFrames from different sources or schema versions, one side
# MAGIC may have columns the other does not. Setting `allowMissingColumns=True` fills
# MAGIC the missing columns with NULL instead of raising an error.

# COMMAND ----------

# DBTITLE 1,3. allowMissingColumns demo
result = full_cols.unionByName(partial_cols, allowMissingColumns=True)

print("allowMissingColumns=True — NULLs fill columns each side is missing:")
result.show()

# COMMAND ----------

# DBTITLE 1,4. distinct after union
# MAGIC %md
# MAGIC ## 4. Remove duplicate rows after a union
# MAGIC
# MAGIC `union()` keeps every row from both DataFrames, including duplicate rows. It
# MAGIC does not perform deduplication.
# MAGIC
# MAGIC Use `distinct()` when the union creates **`exact` duplicate rows**, such as when
# MAGIC the same source file or batch is processed more than once. `distinct()` removes
# MAGIC a row only when every column value is identical.
# MAGIC
# MAGIC Do not apply `distinct()` by default because identical rows may represent
# MAGIC separate valid business events.
# MAGIC
# MAGIC For example, the same passenger may take multiple trips with the same driver,
# MAGIC from the same pickup location, for the same fare on the same day. If the
# MAGIC dataset does not contain a unique `trip_id` or exact pickup timestamp, those
# MAGIC trips may appear identical.
# MAGIC
# MAGIC | passenger_id | driver_id | pickup_location_id | trip_date  | fare |
# MAGIC |---|---|---|---|---|
# MAGIC | P101 | D205 | L010 | 2026-08-04 | 250 |
# MAGIC | P101 | D205 | L010 | 2026-08-04 | 250 |
# MAGIC
# MAGIC `distinct()` has the same effect as `dropDuplicates()` without specifying
# MAGIC columns. For business-level duplicates, use `dropDuplicates()` with the
# MAGIC appropriate key columns.
# MAGIC
# MAGIC Notebook 02 used `dropDuplicates()` on selected key columns before joins. That
# MAGIC is key-level deduplication and serves a different purpose.
# MAGIC

# COMMAND ----------

# DBTITLE 1,4. distinct demo — double-union morning
doubled = morning.union(morning)

print("morning rows:", morning.count())
print("After union with itself:", doubled.count())
print("After distinct():", doubled.distinct().count())
print("\n→ distinct() removed the extra copies")

# COMMAND ----------

# DBTITLE 1,Practice
# MAGIC %md
# MAGIC ## Practice — predict and verify
# MAGIC
# MAGIC Create two overlapping frames (`trip_id` only) and stack them:
# MAGIC
# MAGIC | Frame | trip_ids |
# MAGIC |---|---|
# MAGIC | `group_a` | 1, 2, 3 |
# MAGIC | `group_b` | 3, 4, 5 |
# MAGIC
# MAGIC Predict **union** count and **distinct** count before you run.
# MAGIC
# MAGIC | Step | Your prediction |
# MAGIC |---|---:|
# MAGIC | unionByName count | ? |
# MAGIC | distinct count | ? |

# COMMAND ----------

# DBTITLE 1,Practice — overlapping frames
# TODO (practice):
# 1. Create group_a: trip_ids 1, 2, 3 (column: trip_id only)
# 2. Create group_b: trip_ids 3, 4, 5 (column: trip_id only)
# 3. unionByName the two frames — does the count match your prediction?
# 4. Apply distinct() — does the count match your prediction?
#
# Hint: trip_id 3 appears in both groups.


# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC 1. **`union`** matches columns by position — fast, but silently corrupts data
# MAGIC    if column order differs between sides.
# MAGIC
# MAGIC 2. **`unionByName`** matches columns by name — safer when column order may
# MAGIC    vary.
# MAGIC
# MAGIC 3. **`allowMissingColumns=True`** fills missing columns with NULL when schemas
# MAGIC    differ between sides.
# MAGIC
# MAGIC 4. **`distinct()` after union** removes exact whole-row duplicates. Use it
# MAGIC    intentionally when extra copies are unintended, not as a default.
# MAGIC
# MAGIC **Next:** **`06 - Intersect, subtract, and exceptAll`**