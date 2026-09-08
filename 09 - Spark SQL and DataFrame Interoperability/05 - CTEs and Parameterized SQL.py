# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 05 - CTEs and Parameterized SQL
# MAGIC
# MAGIC CTEs and safe named `:params` — not f-string SQL.
# MAGIC
# MAGIC `trip_enriched`.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Compose multi-step logic with CTEs and safe named `:params` (not f-string SQL)
# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — load `trip_enriched`
# MAGIC
# MAGIC We'll use `rideshare_dev.processed.trip_enriched` throughout — **106** trips,
# MAGIC one row per `trip_id`. Each row includes `pickup_borough`, `tip_amount`,
# MAGIC and `trip_date`.
# MAGIC
# MAGIC | pickup_borough | tip_amount | trip_date |
# MAGIC |---|---:|---|
# MAGIC | Borough A | 5.00 | day 1 |
# MAGIC | Borough B | 3.00 | day 1 |
# MAGIC | Borough A | 2.00 | day 2 |
# MAGIC
# MAGIC We will first compare each borough's total tip with the fleet total.
# MAGIC Later we will apply the same idea by date, for a borough passed as a
# MAGIC SQL parameter.

# COMMAND ----------

trip_enriched = spark.table("rideshare_dev.processed.trip_enriched")  # noqa: F821

print(f"trip_enriched: {trip_enriched.count()} rows")  # expect 106

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Single CTE
# MAGIC
# MAGIC **How much total tip did each borough collect?**
# MAGIC
# MAGIC The aggregation is familiar: `GROUP BY pickup_borough` and `SUM(tip_amount)`.
# MAGIC
# MAGIC A CTE names that result `borough_tips`. The final `SELECT` can then read
# MAGIC from `borough_tips` instead of repeating the aggregation.
# MAGIC
# MAGIC **Expected:** **5 borough rows**.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH borough_tips AS (
# MAGIC   SELECT
# MAGIC     pickup_borough,
# MAGIC     SUM(tip_amount) AS total_tip
# MAGIC   FROM rideshare_dev.processed.trip_enriched
# MAGIC   GROUP BY pickup_borough
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM borough_tips
# MAGIC ORDER BY pickup_borough

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Multi-CTE composition
# MAGIC
# MAGIC **What percentage of the fleet's total tip came from each borough?**
# MAGIC
# MAGIC | name | meaning |
# MAGIC |---|---|
# MAGIC | `borough_tips` | tip per borough |
# MAGIC | `fleet_total` | tip across all trips (one row) |
# MAGIC | `tip_share_pct` | borough / fleet × 100 |
# MAGIC
# MAGIC `fleet_total` returns a single row, so that value can be combined with
# MAGIC every borough row.
# MAGIC
# MAGIC **Expected:** **5 rows** with `tip_share_pct`.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH borough_tips AS (
# MAGIC   SELECT
# MAGIC     pickup_borough,
# MAGIC     SUM(tip_amount) AS total_tip
# MAGIC   FROM rideshare_dev.processed.trip_enriched
# MAGIC   GROUP BY pickup_borough
# MAGIC ),
# MAGIC fleet_total AS (
# MAGIC   SELECT SUM(tip_amount) AS fleet_tip
# MAGIC   FROM rideshare_dev.processed.trip_enriched
# MAGIC )
# MAGIC SELECT
# MAGIC   b.pickup_borough,
# MAGIC   b.total_tip,
# MAGIC   f.fleet_tip,
# MAGIC   ROUND(100 * b.total_tip / NULLIF(f.fleet_tip, 0), 1) AS tip_share_pct
# MAGIC FROM borough_tips AS b
# MAGIC CROSS JOIN fleet_total AS f
# MAGIC ORDER BY b.pickup_borough

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Nested subquery contrast
# MAGIC
# MAGIC The same calculation can be written without CTEs. Borough total and fleet
# MAGIC total sit inside the `FROM` clause as nested subqueries.
# MAGIC
# MAGIC The result is unchanged: same totals, same `tip_share_pct`, same **5 rows**.
# MAGIC
# MAGIC The difference is organization. CTEs give each step a name, which usually
# MAGIC makes multi-step SQL easier to follow and change.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   b.pickup_borough,
# MAGIC   b.total_tip,
# MAGIC   f.fleet_tip,
# MAGIC   ROUND(100 * b.total_tip / NULLIF(f.fleet_tip, 0), 1) AS tip_share_pct
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     pickup_borough,
# MAGIC     SUM(tip_amount) AS total_tip
# MAGIC   FROM rideshare_dev.processed.trip_enriched
# MAGIC   GROUP BY pickup_borough
# MAGIC ) AS b
# MAGIC CROSS JOIN (
# MAGIC   SELECT SUM(tip_amount) AS fleet_tip
# MAGIC   FROM rideshare_dev.processed.trip_enriched
# MAGIC ) AS f
# MAGIC ORDER BY b.pickup_borough

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Named parameters (`:borough`)
# MAGIC
# MAGIC Calculate tip-share for each pickup borough by reusing the same SQL
# MAGIC and passing `:borough` — not by writing a new `WHERE` clause each time.
# MAGIC
# MAGIC Pass each name from a short list into the same SQL:
# MAGIC `spark.sql(sql, args={"borough": borough})`.
# MAGIC Prefer `:params` over an f-string that pastes the value into the SQL.

# COMMAND ----------

borough_sql = """
SELECT
  b.pickup_borough,
  b.total_tip,
  f.fleet_tip,
  ROUND(100 * b.total_tip / NULLIF(f.fleet_tip, 0), 1) AS tip_share_pct
FROM (
  SELECT
    pickup_borough,
    SUM(tip_amount) AS total_tip
  FROM rideshare_dev.processed.trip_enriched
  WHERE pickup_borough = :borough
  GROUP BY pickup_borough
) AS b
CROSS JOIN (
  SELECT SUM(tip_amount) AS fleet_tip
  FROM rideshare_dev.processed.trip_enriched
) AS f
"""

manhattan = spark.sql(borough_sql, args={"borough": "Manhattan"})  # noqa: F821
manhattan.show()

# COMMAND ----------

boroughs = ["Manhattan", "Queens"]

for borough in boroughs:
    result = spark.sql(borough_sql, args={"borough": borough})  # noqa: F821
    result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. CTE + params combined
# MAGIC
# MAGIC Same metric as section 2: borough tip ÷ fleet tip × 100, applied on
# MAGIC date instead of pickup borough.
# MAGIC
# MAGIC The first CTE sums tip per `trip_date` for one pickup borough
# MAGIC (`:borough`). The second CTE sums tip per date across all boroughs.
# MAGIC Then divide: borough daily ÷ fleet daily × 100.
# MAGIC
# MAGIC Parameters:
# MAGIC
# MAGIC - `:borough` — which borough
# MAGIC - `:min_tip >= 0`
# MAGIC
# MAGIC **Expected:** **14 rows**. Share can be **100%** when all tip that day is
# MAGIC in the borough.

# COMMAND ----------

daily_share_sql = """
WITH borough_daily AS (
  SELECT
    trip_date,
    max(pickup_borough) as pickup_borough,
    SUM(tip_amount) AS borough_tip
  FROM rideshare_dev.processed.trip_enriched
  WHERE pickup_borough = :borough
    AND trip_date IS NOT NULL
    AND COALESCE(tip_amount, 0) >= :min_tip
  GROUP BY trip_date
),
fleet_daily AS (
  SELECT
    trip_date,
    SUM(tip_amount) AS fleet_tip
  FROM rideshare_dev.processed.trip_enriched
  WHERE trip_date IS NOT NULL
    AND COALESCE(tip_amount, 0) >= :min_tip
  GROUP BY trip_date
)
SELECT
  f.trip_date,
  pickup_borough,
  b.borough_tip,
  f.fleet_tip,
  ROUND(100 * b.borough_tip / NULLIF(f.fleet_tip, 0), 1) AS tip_share_pct
FROM fleet_daily AS f
LEFT JOIN borough_daily AS b
  ON f.trip_date = b.trip_date
ORDER BY f.trip_date
"""

manhattan_daily = spark.sql(  # noqa: F821
    daily_share_sql,
    args={"borough": "Manhattan", "min_tip": 0},
)
print(f"manhattan_daily: {manhattan_daily.count()} rows")  # expect 14
manhattan_daily.show(14, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC Reuse the daily tip-share pattern for a borough of your choice.
# MAGIC
# MAGIC Your query should:
# MAGIC
# MAGIC - use one CTE for the borough's daily tip
# MAGIC - use another CTE for the fleet's daily tip
# MAGIC - exclude rows where `trip_date` is NULL
# MAGIC - use `:borough` instead of hard-coding the borough name
# MAGIC - use `:min_tip = 0`
# MAGIC - execute with `spark.sql(..., args={...})`
# MAGIC
# MAGIC For each date, return:
# MAGIC
# MAGIC - `trip_date`
# MAGIC - borough daily tip
# MAGIC - fleet daily tip
# MAGIC - borough `tip_share_pct`
# MAGIC
# MAGIC **Hint:** Reuse section 5. Change `:borough` in `args`; keep `min_tip` at
# MAGIC `0`.
# MAGIC
# MAGIC **Expected:** **14 dated rows**.

# COMMAND ----------

# Write a parameterized CTE query: borough daily tip / fleet daily tip.
# Use spark.sql(..., args={"borough": "<your borough>", "min_tip": 0})
# TODO: result = spark.sql("""...""", args={...})  # noqa: F821
# TODO: print count (expect 14) and result.show(14, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Two SQL patterns help when a query starts doing more than one thing:
# MAGIC
# MAGIC - **CTEs** organize several SQL operations into clear, named steps.
# MAGIC - **Named parameters** let the same SQL query run with different input
# MAGIC   values without rewriting the query.
# MAGIC
# MAGIC **Next:** `06 - End-to-End SQL Pipeline` rebuilds
# MAGIC the Module 8 KPI contracts in Spark SQL.