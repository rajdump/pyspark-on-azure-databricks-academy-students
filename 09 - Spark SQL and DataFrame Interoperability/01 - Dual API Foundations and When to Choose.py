# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Dual API Foundations and When to Choose
# MAGIC
# MAGIC Choose among `%sql`, `spark.table`, `spark.sql`→DataFrame, and DF→temp view.
# MAGIC No `GROUP BY`.
# MAGIC
# MAGIC `trip_enriched`.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Choose among direct `%sql`, `spark.table`, `spark.sql`→DataFrame, and
# MAGIC   DF→`createOrReplaceTempView` for a given task
# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — load `trip_enriched`
# MAGIC
# MAGIC We'll use `rideshare_dev.processed.trip_enriched` throughout — **106** trips,
# MAGIC one row per `trip_id`. Load it as a DataFrame now so it is ready when we
# MAGIC switch to the DataFrame API.
# MAGIC
# MAGIC Module 2 `06 - Querying DataFrames with SQL` introduced `%sql`,
# MAGIC `spark.sql`, and temp views. Here we apply those patterns to Unity Catalog
# MAGIC tables and compare the entry points side by side.

# COMMAND ----------

from pyspark.sql import functions as F

trip_enriched = spark.table("rideshare_dev.processed.trip_enriched")  # noqa: F821

print(f"trip_enriched: {trip_enriched.count()} rows")  # expect 106

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Direct SQL on a Unity Catalog table
# MAGIC
# MAGIC Because `trip_enriched` is a Unity Catalog table, `%sql` can query its
# MAGIC three-part name (`catalog.schema.table`) directly — no DataFrame or temp
# MAGIC view:
# MAGIC
# MAGIC `rideshare_dev.processed.trip_enriched`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT trip_id, service_type, tip_amount, pickup_borough
# MAGIC FROM rideshare_dev.processed.trip_enriched
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List tables in the processed schema
# MAGIC SHOW TABLES IN rideshare_dev.processed

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inspect columns and data types
# MAGIC DESCRIBE TABLE rideshare_dev.processed.trip_enriched

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Same projection via DataFrame API
# MAGIC
# MAGIC Return the same four columns through the Setup DataFrame. The data has not
# MAGIC changed — only the API used to express the query has changed.
# MAGIC
# MAGIC SQL used `SELECT` and `LIMIT`; the DataFrame API uses `.select()` and
# MAGIC `.limit()`.

# COMMAND ----------

trip_enriched.select(
    "trip_id",
    "service_type",
    "tip_amount",
    "pickup_borough",
).limit(5).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. SQL → DataFrame bridge
# MAGIC
# MAGIC `%sql` is convenient when the entire cell stays in SQL. Use `spark.sql(...)`
# MAGIC when you want to write the query in SQL and continue with the result in
# MAGIC Python — it returns a DataFrame.
# MAGIC
# MAGIC Keep only trips with a known `tip_amount`.
# MAGIC
# MAGIC **Expected:** **104** rows.

# COMMAND ----------

known_tips = spark.sql(  # noqa: F821
    """
    SELECT trip_id, tip_amount, pickup_borough
    FROM rideshare_dev.processed.trip_enriched
    WHERE tip_amount IS NOT NULL
    """
)

print(f"known_tips: {known_tips.count()} rows")  # expect 104
known_tips.limit(5).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Row-level `CASE WHEN`
# MAGIC
# MAGIC We'll classify `tip_amount` into a new `tip_amount_band` column. This is a
# MAGIC **row-level transformation**, so the dataset remains at **106** rows.
# MAGIC
# MAGIC These are absolute tip-amount bands, not the percentage-based `tip_band`
# MAGIC used earlier in Module 6.
# MAGIC
# MAGIC | Condition | `tip_amount_band` |
# MAGIC |---|---|
# MAGIC | `tip_amount IS NULL` | `no_data` |
# MAGIC | `tip_amount = 0` | `zero` |
# MAGIC | `tip_amount <= 3` | `low` |
# MAGIC | `tip_amount <= 6` | `medium` |
# MAGIC | Otherwise | `high` |
# MAGIC
# MAGIC **Expected distribution:** `zero` **26** · `low` **40** · `medium` **20** ·
# MAGIC `high` **18** · `no_data` **2**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   trip_id,
# MAGIC   tip_amount,
# MAGIC   CASE
# MAGIC     WHEN tip_amount IS NULL THEN 'no_data'
# MAGIC     WHEN tip_amount = 0 THEN 'zero'
# MAGIC     WHEN tip_amount <= 3 THEN 'low'
# MAGIC     WHEN tip_amount <= 6 THEN 'medium'
# MAGIC     ELSE 'high'
# MAGIC   END AS tip_amount_band
# MAGIC FROM rideshare_dev.processed.trip_enriched
# MAGIC ORDER BY tip_amount_band, trip_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. DataFrame → SQL bridge
# MAGIC
# MAGIC Create the same `tip_amount_band` with `F.when(...)`, then register the
# MAGIC DataFrame as the session temp view `trip_tip_band`. `%sql` can query that
# MAGIC name even though the transform was built in PySpark (`%sql` resolves names,
# MAGIC not Python variables).

# COMMAND ----------

trip_tip_band = trip_enriched.withColumn(
    "tip_amount_band",
    F.when(F.col("tip_amount").isNull(), "no_data")
    .when(F.col("tip_amount") == 0, "zero")
    .when(F.col("tip_amount") <= 3, "low")
    .when(F.col("tip_amount") <= 6, "medium")
    .otherwise("high"),
)

trip_tip_band.createOrReplaceTempView("trip_tip_band")
print("Temp view registered: trip_tip_band")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the temp view registered from the PySpark DataFrame
# MAGIC SELECT trip_id, tip_amount, tip_amount_band
# MAGIC FROM trip_tip_band
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. When to choose
# MAGIC
# MAGIC | You want to… | Use | Why |
# MAGIC |---|---|---|
# MAGIC | Explore a UC table interactively | `%sql` + UC name | Table already has a SQL name |
# MAGIC | Load then chain DF transforms | `spark.table("...")` | Load once; DF API for the rest |
# MAGIC | Write SQL; keep result in Python | `spark.sql(...)` → DF | SQL query; DF for pipeline |
# MAGIC | Query a DF with no catalog name | temp view → `%sql` | Session SQL name for the DF |
# MAGIC
# MAGIC ### Broader: SQL vs DataFrame for your logic
# MAGIC
# MAGIC | SQL works especially well for | DataFrame API works especially well for |
# MAGIC |---|---|
# MAGIC | Ad-hoc exploration; stakeholder collaboration | Dynamic columns; programmatic pipelines |
# MAGIC | `QUALIFY`, CTEs, `PIVOT`/`UNPIVOT` (concise SQL) | Runtime logic (loops over columns) |
# MAGIC | Quick validation against known contracts | Refactoring and IDE-assisted development |
# MAGIC | Shared with analysts who do not write Python | Unit-testable transforms (Module 16) |
# MAGIC
# MAGIC Both APIs can express equivalent Spark transformations.
# MAGIC
# MAGIC `06 - End-to-End SQL Pipeline` rebuilds the
# MAGIC Module 8 KPI contracts in Spark SQL.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC Return Manhattan trips that have a known tip and assign the same
# MAGIC `tip_amount_band` used in Section 4.
# MAGIC
# MAGIC ### Requirements
# MAGIC
# MAGIC - Use SQL
# MAGIC - Keep only `pickup_borough = 'Manhattan'`
# MAGIC - Exclude rows where `tip_amount` is NULL
# MAGIC - Reuse the Section 4 `CASE WHEN` bands
# MAGIC - Add one SQL comment explaining why `%sql` is a good entry point here
# MAGIC
# MAGIC **Expected:** **43 rows**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- %sql because ...
# MAGIC SELECT
# MAGIC   trip_id,
# MAGIC   tip_amount,
# MAGIC   CASE
# MAGIC     WHEN tip_amount IS NULL THEN 'no_data'
# MAGIC     -- TODO: finish absolute tip_amount_band CASE (same bands as Section 4)
# MAGIC     ELSE 'TODO'
# MAGIC   END AS tip_amount_band
# MAGIC FROM rideshare_dev.processed.trip_enriched
# MAGIC WHERE 1 = 0  -- TODO: replace with Manhattan + tip_amount IS NOT NULL
# MAGIC -- Expected: 43 rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC The same Spark data can move between SQL and the DataFrame API without
# MAGIC changing the underlying dataset.
# MAGIC
# MAGIC - `%sql` → work directly in SQL
# MAGIC - `spark.table(...)` → start from a catalog table in the DataFrame API
# MAGIC - `spark.sql(...)` → write SQL and keep the result as a DataFrame
# MAGIC - `createOrReplaceTempView(...)` → expose a DataFrame to SQL
# MAGIC
# MAGIC You also used SQL `CASE WHEN` for a row-level transformation without
# MAGIC changing the dataset grain.
# MAGIC
# MAGIC **Next:** `02 - SQL Joins, Aggregations, and Filtering` adds SQL joins,
# MAGIC `GROUP BY`, and `HAVING`.
