# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03 - Filter Placement with WHERE and HAVING
# MAGIC
# MAGIC Filtering before `groupBy()` removes individual rows before aggregation, like SQL's `WHERE`. Filtering after `agg()` removes groups based on their aggregated results, like SQL's `HAVING`.
# MAGIC
# MAGIC The filter's position determines what changes: filtering before aggregation changes which rows contribute to each total, while filtering afterward determines which groups appear in the final result.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Filter individual rows before aggregation using `.filter()`, equivalent to SQL `WHERE`.
# MAGIC - Filter aggregated groups using `.filter()` after `agg()`, equivalent to SQL `HAVING`.
# MAGIC - Decide whether a condition belongs before or after aggregation.

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach **all-purpose compute**.
# MAGIC
# MAGIC This notebook uses the `trip_enriched` table created in Module 7, **07 - Build Unified Curated Tables**.
# MAGIC It contains **one row per trip**: a total of 106 trips.

# COMMAND ----------

from pyspark.sql import functions as F

trip_enriched_table = "rideshare_dev.processed.trip_enriched"

trip_enriched = spark.table(trip_enriched_table)  # noqa: F821

print("trip_enriched rows:", trip_enriched.count())

# COMMAND ----------

# DBTITLE 1,Section 1 - Filter before aggregation
# MAGIC %md
# MAGIC ## 1. Filter before aggregation — SQL `WHERE`
# MAGIC
# MAGIC **Question:** What is the total tip amount for each pickup borough, considering only trips where `tip_amount > 5`?
# MAGIC
# MAGIC ```sql
# MAGIC SELECT pickup_borough, SUM(tip_amount) AS total_tip
# MAGIC FROM trip_enriched
# MAGIC WHERE tip_amount > 5
# MAGIC GROUP BY pickup_borough
# MAGIC ORDER BY total_tip DESC;
# MAGIC ```
# MAGIC
# MAGIC In PySpark, place `.filter()` before `groupBy()`:

# COMMAND ----------

trip_enriched.filter(F.col("tip_amount") > 5).groupBy("pickup_borough").agg(
    F.round(F.sum("tip_amount"), 2).alias("total_tip"),
).orderBy(F.col("total_tip").desc()).show()

# COMMAND ----------

# DBTITLE 1,Section 2 - Filter after aggregation
# MAGIC %md
# MAGIC ## 2. Filter after aggregation — SQL `HAVING`
# MAGIC
# MAGIC **Question:** Which pickup boroughs have a total tip amount greater than $90?
# MAGIC
# MAGIC ```sql
# MAGIC SELECT pickup_borough, SUM(tip_amount) AS total_tip
# MAGIC FROM trip_enriched
# MAGIC GROUP BY pickup_borough
# MAGIC HAVING SUM(tip_amount) > 90
# MAGIC ORDER BY total_tip DESC;
# MAGIC ```
# MAGIC
# MAGIC The DataFrame API has no `.having()` method. Instead, place `.filter()` after
# MAGIC `agg()` and refer to the alias created there:

# COMMAND ----------

trip_enriched.groupBy("pickup_borough").agg(
    F.round(F.sum("tip_amount"), 2).alias("total_tip"),
).filter(F.col("total_tip") > 90).orderBy(F.col("total_tip").desc()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC In the first example, only trips with tips greater than $5 contribute to each
# MAGIC borough's total. In the second, all trips contribute to each borough's total, but
# MAGIC only boroughs with total tips greater than $90 appear in the result.

# COMMAND ----------

# DBTITLE 1,WHERE vs HAVING
# MAGIC %md
# MAGIC ## 3. `WHERE` vs `HAVING`
# MAGIC
# MAGIC | | SQL `WHERE` | SQL `HAVING` |
# MAGIC |---|---|---|
# MAGIC | Filters | Individual rows | Aggregated groups |
# MAGIC | Applied | Before `GROUP BY` | After `GROUP BY` |
# MAGIC | PySpark | `.filter()` before `groupBy()` | `.filter()` after `agg()` |
# MAGIC
# MAGIC In PySpark, `.where()` is an alias for `.filter()`. Both can filter rows before
# MAGIC grouping or aggregated results afterward, depending on where they are used.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC - **SQL `WHERE`:** Filters individual rows before `groupBy()`, such as trips where `tip_amount > 5`.
# MAGIC - **SQL `HAVING`:** Filters groups after `agg()`, such as boroughs where `total_tip > 90`.
# MAGIC - **PySpark `.filter()`:** Works both before and after aggregation. Its position determines whether it behaves like SQL `WHERE` or `HAVING`.
# MAGIC
# MAGIC **Next:** **`04 - Collections and Distinct Counts`** — collecting a driver's service
# MAGIC types with `collect_list()` and `collect_set()`, and counting distinct routes with
# MAGIC `countDistinct()`.