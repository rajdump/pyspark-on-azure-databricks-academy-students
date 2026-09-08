# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Intersect, subtract, and exceptAll
# MAGIC
# MAGIC Whole-row set ops on constructed frames.
# MAGIC
# MAGIC Constructed frames (no landing read).
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Use `intersect` / `intersectAll` / `subtract` / `exceptAll` and SQL `EXCEPT`
# MAGIC   naming
# COMMAND ----------

# DBTITLE 1,Set-op rules
# MAGIC %md
# MAGIC ## Set-op rules
# MAGIC
# MAGIC | API | Returns | Duplicates |
# MAGIC |---|---|---|
# MAGIC | `intersect` | rows in both sides | removed (distinct) |
# MAGIC | `intersectAll` | rows in both sides | pairs one-to-one |
# MAGIC | `subtract` | left-only rows | removed (distinct) |
# MAGIC | `exceptAll` | left-only rows | pairs one-to-one |
# MAGIC
# MAGIC 1. Set ops compare **whole rows** (every selected column), not a join key.
# MAGIC 2. `intersect` / `subtract` return distinct results.
# MAGIC 3. `intersectAll` / `exceptAll` preserve duplicate **counts** (one-to-one pairing).
# MAGIC 4. Columns align by **position** (same as `union`), not by name.
# MAGIC 5. Use `select()` to narrow columns before a set op when you want key-only
# MAGIC    comparison instead of whole-row.
# MAGIC
# MAGIC **SQL name mapping:**
# MAGIC
# MAGIC | SQL | DataFrame API |
# MAGIC |---|---|
# MAGIC | `INTERSECT` | `intersect()` |
# MAGIC | `INTERSECT ALL` | `intersectAll()` |
# MAGIC | `EXCEPT` / `EXCEPT DISTINCT` | `subtract()` |
# MAGIC | `EXCEPT ALL` | `exceptAll()` |
# MAGIC
# MAGIC There is no DataFrame method named `except`. Use `subtract()`.

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %md
# MAGIC ## Setup — constructed frames
# MAGIC
# MAGIC Handmade DataFrames with two columns — set ops compare the **whole row**.
# MAGIC
# MAGIC | Frame | Rows |
# MAGIC |---|---|
# MAGIC | `left_ids` | (1, Standard), (2, Premium), (3, Standard) |
# MAGIC | `right_ids` | (2, Premium), (3, Standard), (4, XL) |
# MAGIC
# MAGIC Overlap: `(2, Premium)`, `(3, Standard)`. Left-only: `(1, Standard)`. Right-only: `(4, XL)`.

# COMMAND ----------

# DBTITLE 1,Create left_ids and right_ids
left_ids = spark.createDataFrame(  # noqa: F821
    [(1, "Standard"), (2, "Premium"), (3, "Standard")],
    ["trip_id", "service_type"],
)
right_ids = spark.createDataFrame(  # noqa: F821
    [(2, "Premium"), (3, "Standard"), (4, "XL")],
    ["trip_id", "service_type"],
)

print("left_ids:")
left_ids.show()
print("right_ids:")
right_ids.show()

# COMMAND ----------

# DBTITLE 1,1. intersect vs intersectAll
# MAGIC %md
# MAGIC ## 1. `intersect` vs `intersectAll`
# MAGIC
# MAGIC `intersect()` returns rows present in **both** DataFrames. Duplicates are
# MAGIC removed from the result.
# MAGIC
# MAGIC `intersectAll()` also returns shared rows, but pairs them one-to-one. If a
# MAGIC row appears 3 times on the left and 2 times on the right, only 2 can be
# MAGIC paired — the third has no partner, so it is excluded.

# COMMAND ----------

# DBTITLE 1,1. intersect — shared trip_ids
shared = left_ids.intersect(right_ids)

print("intersect rows:", shared.count())  # 2 and 3 → 2
shared.orderBy("trip_id").show()

# COMMAND ----------

# DBTITLE 1,1b. intersect vs intersectAll with duplicates
left_dup = spark.createDataFrame(  # noqa: F821
    [(1, "Standard"), (1, "Standard"), (2, "Premium"), (3, "XL")],
    ["trip_id", "service_type"],
)
right_dup = spark.createDataFrame(  # noqa: F821
    [(1, "Standard"), (1, "Standard"), (2, "Premium")],
    ["trip_id", "service_type"],
)

print("intersect (distinct):")
left_dup.intersect(right_dup).orderBy("trip_id").show()

print("intersectAll (keeps overlapping copies):")
left_dup.intersectAll(right_dup).orderBy("trip_id").show()
print("→ trip_id 1 appears twice on both sides → intersectAll keeps 2 copies")

# COMMAND ----------

# DBTITLE 1,2. subtract vs exceptAll
# MAGIC %md
# MAGIC ## 2. `subtract` vs `exceptAll`
# MAGIC
# MAGIC `subtract()` returns rows in the **left** DataFrame that do not appear in
# MAGIC the right. Duplicates are removed from the result.
# MAGIC
# MAGIC `exceptAll()` is the multiset version — it subtracts matching copies from
# MAGIC the right, one for one, and keeps any remaining left-side copies.
# MAGIC
# MAGIC **intersect vs subtract:** 
# MAGIC Together they partition the left DataFrame:
# MAGIC
# MAGIC `left.intersect(right)` → which of my left rows also exist in right?
# MAGIC
# MAGIC `left.subtract(right)` → which of my left rows do NOT exist in right?

# COMMAND ----------

# DBTITLE 1,2. subtract — direction matters
only_left = left_ids.subtract(right_ids)  # 1
only_right = right_ids.subtract(left_ids)  # 4

print("left_ids.subtract(right_ids):")
only_left.show()
print("right_ids.subtract(left_ids):")
only_right.show()
print("→ Which DataFrame is left decides which leftover you get")

# COMMAND ----------

# DBTITLE 1,2b. subtract vs exceptAll with duplicates
batch_a = spark.createDataFrame(  # noqa: F821
    [(1, "Standard"), (1, "Standard"), (1, "Standard"), (2, "Premium"), (3, "XL"), (4, "Shared")],
    ["trip_id", "service_type"],
)
batch_b = spark.createDataFrame(  # noqa: F821
    [(1, "Standard"), (3, "XL")],
    ["trip_id", "service_type"],
)

print("subtract (distinct difference):")
batch_a.subtract(batch_b).orderBy("trip_id").show()

print("exceptAll (multiset difference):")
batch_a.exceptAll(batch_b).orderBy("trip_id").show()
print("→ three copies of trip_id 1 minus one copy → two remain under exceptAll")

# COMMAND ----------

# DBTITLE 1,Practice
# MAGIC %md
# MAGIC ## Practice — predict and verify
# MAGIC
# MAGIC Create a new multiset pair and apply all four APIs:
# MAGIC
# MAGIC | Frame | Rows |
# MAGIC |---|---|
# MAGIC | `left_ms` | (5, Shared), (5, Shared), (5, Shared), (6, Premium), (7, Standard) |
# MAGIC | `right_ms` | (5, Shared), (5, Shared), (7, Standard), (8, XL) |
# MAGIC
# MAGIC Predict before you run:
# MAGIC
# MAGIC | Step | Your prediction |
# MAGIC |---|---:|
# MAGIC | `intersect` count | ? |
# MAGIC | `intersectAll` count | ? |
# MAGIC | `subtract` count | ? |
# MAGIC | `exceptAll` count | ? |
# MAGIC
# MAGIC Hint: shared rows are `(5, Shared)` and `(7, Standard)`. Count how many
# MAGIC copies survive under distinct vs multiset rules.

# COMMAND ----------

# DBTITLE 1,Practice — multiset set ops
# TODO (practice):
# 1. Create left_ms: (5,Shared),(5,Shared),(5,Shared),(6,Premium),(7,Standard)
# 2. Create right_ms: (5,Shared),(5,Shared),(7,Standard),(8,XL)
#    Columns: trip_id, service_type
# 3. Print counts for intersect, intersectAll, subtract, exceptAll
# 4. Do the four counts match your predictions?


# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Set ops compare **whole rows** — not a join key. Use `select` to narrow
# MAGIC columns before calling set ops when you want key-only comparison, or use
# MAGIC semi/anti joins (Notebook **04**) for key-based filtering.
# MAGIC
# MAGIC **Next:** **`07 - Build Unified Curated Tables`**