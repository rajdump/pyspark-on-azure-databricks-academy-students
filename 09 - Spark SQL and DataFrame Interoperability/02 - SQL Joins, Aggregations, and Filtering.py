# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - SQL Joins, Aggregations, and Filtering
# MAGIC
# MAGIC Layered SQL: projection through `HAVING`, including a deliberate ambiguous
# MAGIC reference.
# MAGIC
# MAGIC `trip_enriched`, `trip_driver_assignment`.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Write SQL joins with qualified aliases, `CASE WHEN`, `COALESCE`, `GROUP BY`,
# MAGIC   and `HAVING` (including compound predicates)
# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — load both tables
# MAGIC
# MAGIC We'll use two managed tables:
# MAGIC
# MAGIC - `trip_enriched` — **106 trips**
# MAGIC - `trip_driver_assignment` — driver assignments for **100 trips**
# MAGIC
# MAGIC Trips **101–106** have no driver assignment.
# MAGIC
# MAGIC Both tables contain `service_type`. Once we join them, we'll need to make
# MAGIC clear which table that column comes from.
# MAGIC
# MAGIC Module 7 covered join patterns. Module 8 `01 - GroupBy and Basic
# MAGIC Aggregations` covered aggregations. Here we apply those ideas with Spark SQL.

# COMMAND ----------

trip_enriched = spark.table("rideshare_dev.processed.trip_enriched")  # noqa: F821
trip_driver_assignment = spark.table(  # noqa: F821
    "rideshare_dev.processed.trip_driver_assignment"
)

print(f"trip_enriched: {trip_enriched.count()} rows")  # expect 106
print(f"trip_driver_assignment: {trip_driver_assignment.count()} rows")  # expect 100

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main arc — build the query step by step
# MAGIC
# MAGIC We'll start with trip-level data and add one SQL concept at a time until we
# MAGIC reach a driven-trip tier summary.
# MAGIC
# MAGIC Each step keeps the previous logic and adds one new piece. The final step
# MAGIC uses `HAVING` to filter the aggregated result.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 — Start with the base columns
# MAGIC
# MAGIC Begin with the trip-level columns we'll need throughout the query.
# MAGIC
# MAGIC For now, keep the NULL values visible:
# MAGIC
# MAGIC - `tip_amount` is NULL for trips **103** and **106**
# MAGIC - `base_fare_amount` is NULL for trips **104** and **106**
# MAGIC
# MAGIC No filtering, joining, or grouping yet — the result remains at **106 rows**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT trip_id, service_type, tip_amount, base_fare_amount
# MAGIC FROM rideshare_dev.processed.trip_enriched

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 — Classify `service_type` into a tier
# MAGIC
# MAGIC Add a row-level `CASE WHEN`, using the same conditional pattern introduced in
# MAGIC `01 - Dual API Foundations and When to Choose`.
# MAGIC
# MAGIC Classify each `service_type` into a broader `tier`:
# MAGIC
# MAGIC | `service_type` | `tier` |
# MAGIC |---|---|
# MAGIC | `PREMIUM` | `high` |
# MAGIC | `STANDARD`, `XL` | `standard` |
# MAGIC | Anything else | `other` |
# MAGIC
# MAGIC Each trip gets a `tier` label — still **106 rows**. Grain changes only when
# MAGIC we `GROUP BY` later.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   trip_id,
# MAGIC   service_type,
# MAGIC   tip_amount,
# MAGIC   base_fare_amount,
# MAGIC   CASE
# MAGIC     WHEN service_type = 'PREMIUM' THEN 'high'
# MAGIC     WHEN service_type IN ('STANDARD', 'XL') THEN 'standard'
# MAGIC     ELSE 'other'
# MAGIC   END AS tier
# MAGIC FROM rideshare_dev.processed.trip_enriched

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 — Replace NULL tips with `COALESCE`
# MAGIC
# MAGIC Add `COALESCE(tip_amount, 0) AS safe_tip`.
# MAGIC
# MAGIC `COALESCE` returns the first non-NULL value. Here that means:
# MAGIC
# MAGIC - keep the original `tip_amount` when it has a value
# MAGIC - use `0` when `tip_amount` is NULL
# MAGIC
# MAGIC Look at trips **103** and **106**:
# MAGIC
# MAGIC - `tip_amount` remains NULL — the original value
# MAGIC - `safe_tip` shows `0` — the substituted value
# MAGIC
# MAGIC `COALESCE` does not remove rows, so the result remains at **106 rows**.
# MAGIC Apply it before the JOIN while the NULL-tip rows are still present.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   trip_id,
# MAGIC   service_type,
# MAGIC   tip_amount,
# MAGIC   base_fare_amount,
# MAGIC   CASE
# MAGIC     WHEN service_type = 'PREMIUM' THEN 'high'
# MAGIC     WHEN service_type IN ('STANDARD', 'XL') THEN 'standard'
# MAGIC     ELSE 'other'
# MAGIC   END AS tier,
# MAGIC   COALESCE(tip_amount, 0) AS safe_tip
# MAGIC FROM rideshare_dev.processed.trip_enriched

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 — Join the driver assignment table
# MAGIC
# MAGIC Restrict the result to **driven trips** by joining
# MAGIC `trip_driver_assignment` on `trip_id`.
# MAGIC
# MAGIC We need the assignment table to keep only trips that have a matching
# MAGIC driver assignment; the selected columns still come from `trip_enriched`.
# MAGIC
# MAGIC Both tables contain `service_type`. After the JOIN, a bare `service_type`
# MAGIC is no longer specific enough — Spark cannot tell which table we mean.
# MAGIC
# MAGIC The next query intentionally leaves `service_type` unqualified so you can
# MAGIC see the ambiguous column reference error.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   t.trip_id,
# MAGIC   tip_amount,
# MAGIC   base_fare_amount,
# MAGIC   CASE
# MAGIC     WHEN service_type = 'PREMIUM' THEN 'high'
# MAGIC     WHEN service_type IN ('STANDARD', 'XL') THEN 'standard'
# MAGIC     ELSE 'other'
# MAGIC   END AS tier,
# MAGIC   COALESCE(tip_amount, 0) AS safe_tip
# MAGIC FROM rideshare_dev.processed.trip_enriched AS t
# MAGIC INNER JOIN rideshare_dev.processed.trip_driver_assignment AS d
# MAGIC   ON t.trip_id = d.trip_id

# COMMAND ----------

# MAGIC %md
# MAGIC #### Fix the ambiguous reference
# MAGIC
# MAGIC Qualify the column with the table alias:
# MAGIC
# MAGIC `t.service_type`
# MAGIC
# MAGIC Qualify the other trip columns with `t.` as well so their source is clear.
# MAGIC
# MAGIC The `INNER JOIN` keeps only trips that have a matching driver assignment.
# MAGIC
# MAGIC **Expected:** **100 rows** — trips **101–106** have no assignment and are
# MAGIC removed.
# MAGIC
# MAGIC Trips **103** and **106** (NULL tips earlier) are among those undriven
# MAGIC trips. After this JOIN, `COALESCE` no longer changes any remaining tip
# MAGIC values.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   t.trip_id,
# MAGIC   t.tip_amount,
# MAGIC   t.base_fare_amount,
# MAGIC   CASE
# MAGIC     WHEN t.service_type = 'PREMIUM' THEN 'high'
# MAGIC     WHEN t.service_type IN ('STANDARD', 'XL') THEN 'standard'
# MAGIC     ELSE 'other'
# MAGIC   END AS tier,
# MAGIC   COALESCE(t.tip_amount, 0) AS safe_tip
# MAGIC FROM rideshare_dev.processed.trip_enriched AS t
# MAGIC INNER JOIN rideshare_dev.processed.trip_driver_assignment AS d
# MAGIC   ON t.trip_id = d.trip_id

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 — Aggregate by tier
# MAGIC
# MAGIC Until now, the query returned one row per driven trip.
# MAGIC
# MAGIC `GROUP BY tier` changes the result to one row per **tier**.
# MAGIC
# MAGIC For each tier, calculate trip count, average tip, and total base fare.
# MAGIC
# MAGIC Spark SQL allows the `tier` alias from the `SELECT` list in `GROUP BY`, so
# MAGIC we write `GROUP BY tier`.
# MAGIC
# MAGIC **Expected:**
# MAGIC
# MAGIC | `tier` | `trip_count` | `total_base_fare` |
# MAGIC |---|---:|---:|
# MAGIC | `high` | 15 | 966.75 |
# MAGIC | `standard` | 64 | 1970.15 |
# MAGIC | `other` | 21 | 308.68 |

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN t.service_type = 'PREMIUM' THEN 'high'
# MAGIC     WHEN t.service_type IN ('STANDARD', 'XL') THEN 'standard'
# MAGIC     ELSE 'other'
# MAGIC   END AS tier,
# MAGIC   COUNT(*) AS trip_count,
# MAGIC   ROUND(AVG(COALESCE(t.tip_amount, 0)), 2) AS avg_tip,
# MAGIC   SUM(t.base_fare_amount) AS total_base_fare
# MAGIC FROM rideshare_dev.processed.trip_enriched AS t
# MAGIC INNER JOIN rideshare_dev.processed.trip_driver_assignment AS d
# MAGIC   ON t.trip_id = d.trip_id
# MAGIC GROUP BY tier

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6 — Filter aggregated groups with `HAVING`
# MAGIC
# MAGIC `WHERE` filters individual rows **before** aggregation.
# MAGIC
# MAGIC `HAVING` filters the grouped result **after** aggregation.
# MAGIC
# MAGIC Keep only tiers whose total base fare is greater than `500`.
# MAGIC
# MAGIC **Expected:** **2 rows** — `other` is removed (total base fare **308.68**).
# MAGIC
# MAGIC Module 8 `02 - Multi-column Keys, NULL Groups, and Filter
# MAGIC Placement` covered the same filter-placement idea with the DataFrame API.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN t.service_type = 'PREMIUM' THEN 'high'
# MAGIC     WHEN t.service_type IN ('STANDARD', 'XL') THEN 'standard'
# MAGIC     ELSE 'other'
# MAGIC   END AS tier,
# MAGIC   COUNT(*) AS trip_count,
# MAGIC   ROUND(AVG(COALESCE(t.tip_amount, 0)), 2) AS avg_tip,
# MAGIC   SUM(t.base_fare_amount) AS total_base_fare
# MAGIC FROM rideshare_dev.processed.trip_enriched AS t
# MAGIC INNER JOIN rideshare_dev.processed.trip_driver_assignment AS d
# MAGIC   ON t.trip_id = d.trip_id
# MAGIC GROUP BY tier
# MAGIC HAVING total_base_fare > 500

# COMMAND ----------

# MAGIC %md
# MAGIC ## Side path — find trips with no driver assignment
# MAGIC
# MAGIC Not every question needs aggregation.
# MAGIC
# MAGIC To find trips that have **no matching driver assignment**, use
# MAGIC `NOT EXISTS`. The condition keeps a trip when no matching `trip_id` exists
# MAGIC in `trip_driver_assignment`.
# MAGIC
# MAGIC **Expected:** **6 trips** — `101` through `106`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT t.trip_id
# MAGIC FROM rideshare_dev.processed.trip_enriched AS t
# MAGIC WHERE NOT EXISTS (
# MAGIC   SELECT 1
# MAGIC   FROM rideshare_dev.processed.trip_driver_assignment AS d
# MAGIC   WHERE d.trip_id = t.trip_id
# MAGIC )
# MAGIC ORDER BY t.trip_id

# COMMAND ----------

# MAGIC %md
# MAGIC `LEFT ANTI JOIN` expresses the same business question: keep left-side rows
# MAGIC that have no match on the right. Module 7 used the anti-join pattern in the
# MAGIC DataFrame API; here `NOT EXISTS` is the SQL form.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC ### Q1 — Filter aggregated tiers
# MAGIC
# MAGIC Using **driven trips only**, return tiers that meet both conditions:
# MAGIC
# MAGIC - more than **20 trips**
# MAGIC - total base fare greater than **300**
# MAGIC
# MAGIC Use table aliases, `JOIN`, `CASE WHEN`, `GROUP BY`, and a compound `HAVING`
# MAGIC condition.
# MAGIC
# MAGIC **Expected:** **2 rows**
# MAGIC
# MAGIC - `standard` — 64 trips
# MAGIC - `other` — 21 trips
# MAGIC
# MAGIC ### Q2 — Find undriven trips
# MAGIC
# MAGIC Use `NOT EXISTS` to return trips that have no driver assignment.
# MAGIC
# MAGIC **Expected:** **6 `trip_id`s** — `101–106`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q1: compound HAVING on driven trips (expect 2 rows: standard, other)
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN t.service_type = 'PREMIUM' THEN 'high'
# MAGIC     WHEN t.service_type IN ('STANDARD', 'XL') THEN 'standard'
# MAGIC     ELSE 'other'
# MAGIC   END AS tier,
# MAGIC   COUNT(*) AS trip_count,
# MAGIC   SUM(t.base_fare_amount) AS total_base_fare
# MAGIC FROM rideshare_dev.processed.trip_enriched AS t
# MAGIC INNER JOIN rideshare_dev.processed.trip_driver_assignment AS d
# MAGIC   ON t.trip_id = d.trip_id
# MAGIC GROUP BY tier
# MAGIC HAVING 1 = 0  -- TODO: compound HAVING (trip count and total base fare)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q2: undriven trip_ids via NOT EXISTS (expect 6 rows: 101–106)
# MAGIC SELECT t.trip_id
# MAGIC FROM rideshare_dev.processed.trip_enriched AS t
# MAGIC WHERE 1 = 0  -- TODO: NOT EXISTS (... trip_driver_assignment ...)
# MAGIC ORDER BY t.trip_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC We built one SQL query from trip-level data into a driven-trip tier summary:
# MAGIC
# MAGIC `SELECT` → `CASE WHEN` → `COALESCE` → `JOIN` → `GROUP BY` → `HAVING`
# MAGIC
# MAGIC Key takeaways:
# MAGIC
# MAGIC - `CASE WHEN` adds row-level categories without changing the grain
# MAGIC - `COALESCE` substitutes a value without removing the row
# MAGIC - An `INNER JOIN` keeps trips with matching driver assignments
# MAGIC - Qualify shared column names after a JOIN, such as `t.service_type`
# MAGIC - `GROUP BY tier` changes the result from trip grain to tier grain
# MAGIC - `HAVING` filters aggregated groups
# MAGIC - `NOT EXISTS` finds rows that have no matching record
# MAGIC
# MAGIC **Next:** `03 - SQL Pivot, Unpivot, and Sampling` — `PIVOT` / `UNPIVOT`
# MAGIC reshape and `TABLESAMPLE`.
