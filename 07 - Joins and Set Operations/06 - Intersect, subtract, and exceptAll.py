# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 06 - Intersect, subtract, and exceptAll
# MAGIC
# MAGIC Compare complete rows in two DataFrames. Columns are matched by position, not by name.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and source data
# MAGIC
# MAGIC Attach **all-purpose compute**. Create two DataFrames with repeated rows.

# COMMAND ----------

left_df = spark.createDataFrame(  # noqa: F821
    [
        (8, "Premium"),
        (8, "Premium"),
        (9, "Standard"),
        (9, "Standard"),
        (10, "XL"),
    ],
    ["trip_id", "service_type"],
)
right_df = spark.createDataFrame(  # noqa: F821
    [(8, "Premium"), (8, "Premium"), (9, "Standard")],
    ["trip_id", "service_type"],
)

print("left_df:")
left_df.show()
print("right_df:")
right_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `intersect()` and `intersectAll()`
# MAGIC
# MAGIC Both methods return shared rows from the two DataFrames, but they handle duplicates differently

# COMMAND ----------

print("intersect:")
left_df.intersect(right_df).orderBy("trip_id").show()

print("intersectAll:")
left_df.intersectAll(right_df).orderBy("trip_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `subtract()` and `exceptAll()`
# MAGIC
# MAGIC Both methods return rows remaining in the first DataFrame after removing rows that appear in the second. They differ in how they handle duplicate rows

# COMMAND ----------

print("subtract:")
left_df.subtract(right_df).orderBy("trip_id").show()

print("exceptAll:")
left_df.exceptAll(right_df).orderBy("trip_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaway
# MAGIC
# MAGIC - `intersect()` and `subtract()` return distinct rows.
# MAGIC - `intersectAll()` and `exceptAll()` keep duplicates, based on how many copies appear in each DataFrame.
# MAGIC
# MAGIC **Next:** `07 - Build Unified Curated Tables`