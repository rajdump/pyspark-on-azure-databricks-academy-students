# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Running Totals and Lag and Lead
# MAGIC
# MAGIC Ordered frames: running totals and `lag` / `lead`. May first aggregate to
# MAGIC daily grain, then window over that.
# MAGIC
# MAGIC `trip_enriched`.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Build windows for running calculations and row-to-row comparisons
# COMMAND ----------

# MAGIC %md
# MAGIC ## The problem: a running total shows future revenue
# MAGIC
# MAGIC Finance wants to see Manhattan's **cumulative fare revenue after each trip**.
# MAGIC
# MAGIC You already know how to create a window. This time, you add `orderBy()` so
# MAGIC the trips are processed in sequence and then calculate a running `sum()`.
# MAGIC
# MAGIC But the result is wrong.
# MAGIC
# MAGIC For Manhattan's first three trips on `2026-03-01`:
# MAGIC
# MAGIC | `hour_of_day` | `trip_id` | `base_fare_amount` | `revenue_so_far` |
# MAGIC | ------------: | --------: | -----------------: | ---------------: |
# MAGIC |             2 |        36 |              15.09 |        **95.20** |
# MAGIC |             5 |        52 |              73.62 |        **95.20** |
# MAGIC |            17 |        46 |               6.49 |        **95.20** |
# MAGIC
# MAGIC The 2 a.m. trip should show only **15.09**, but it already shows the full
# MAGIC day's **95.20**.
# MAGIC
# MAGIC The correct running totals are:
# MAGIC
# MAGIC **15.09 → 88.71 → 95.20**
# MAGIC
# MAGIC ### Why?
# MAGIC
# MAGIC All three trips have the same `trip_date`.
# MAGIC
# MAGIC So `orderBy("trip_date")` does not give Spark a clear row-by-row trip
# MAGIC sequence.
# MAGIC
# MAGIC Because no frame was specified, Spark uses its default `RANGE` frame.
# MAGIC `RANGE` includes rows with the same sort value together, so all three
# MAGIC `2026-03-01` trips receive **95.20**.
# MAGIC
# MAGIC For a running total, we need two things:
# MAGIC
# MAGIC 1. A clear **row order**
# MAGIC 2. A rule that defines **how far the calculation should look from the
# MAGIC    current row**
# MAGIC
# MAGIC That second rule is called the **window frame**.
# MAGIC
# MAGIC For this running total, we want Spark to read:
# MAGIC
# MAGIC **first row → current row**
# MAGIC
# MAGIC ```python
# MAGIC .rowsBetween(
# MAGIC     Window.unboundedPreceding,
# MAGIC     Window.currentRow
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC `unboundedPreceding` points to the first row in the group.
# MAGIC
# MAGIC `currentRow` points to the row Spark is currently calculating.
# MAGIC
# MAGIC As Spark moves through the ordered rows, `currentRow` moves with it, while
# MAGIC `unboundedPreceding` stays fixed at the beginning.
# MAGIC
# MAGIC The frame fixes which rows are included. Section 1c adds the complete trip
# MAGIC order needed for reproducible results.
# MAGIC
# MAGIC ## What this notebook teaches
# MAGIC
# MAGIC | Section | The question it answers | Concept |
# MAGIC |---|---|---|
# MAGIC | 1 | How do we calculate reliable running revenue for each borough? | Default `RANGE`; unstable vs stable `ROWS` |  # noqa: E501
# MAGIC | 2 | What were each borough's first and final ordered fares? | `first_value` / `last_value`; current-row vs full frame |  # noqa: E501
# MAGIC | 3 | How much has accumulated through each date? | Running totals on a 14-row daily series |  # noqa: E501
# MAGIC | 4 | Did today beat yesterday? | `lag` / `lead` |
# MAGIC | Exercise | Which service type's tips are growing? | Running tip totals and previous-row change |  # noqa: E501
# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %md
# MAGIC ## Setup — load `trip_enriched` and keep the dated trips
# MAGIC
# MAGIC Every example in this notebook sorts rows by `trip_date`.
# MAGIC
# MAGIC Six trips (`101`–`106`) have no `trip_date`. Because Spark sorts NULL
# MAGIC values **first** by default, those rows can appear before the dated trips
# MAGIC and distort the ordering used in later examples.
# MAGIC
# MAGIC In Sections 1 and 2, the window partitions by `pickup_borough`. Manhattan
# MAGIC has three undated trips (`101`, `104`, `106`), Brooklyn has two (`102`,
# MAGIC `105`), and Queens has one (`103`). As a result, Manhattan's "first trip of
# MAGIC the period" could be trip `101` — even though it has no date.
# MAGIC
# MAGIC In Sections 3 and 4, the data is grouped by `trip_date`. The six undated
# MAGIC trips form a separate NULL group, which sorts before `2026-03-01`.
# MAGIC
# MAGIC To keep the examples focused on window ordering, this notebook uses only
# MAGIC the **100 dated trips**.
# MAGIC
# MAGIC | DataFrame       | Rows | Used for                      |
# MAGIC | --------------- | ---: | ----------------------------- |
# MAGIC | `trip_enriched` |  106 | Source table                  |
# MAGIC | `dated_trip`    |  100 | Sections 1–4 and the exercise |
# MAGIC
# MAGIC Filtering these six rows also removes the inherited NULLs relevant to this
# MAGIC notebook. In `dated_trip`, the columns used here — `base_fare_amount`,
# MAGIC `tip_amount`, `hour_of_day`, `pickup_borough`, and `service_type` — are
# MAGIC fully populated.
# MAGIC
# MAGIC This keeps the notebook focused on **row order and window behavior**,
# MAGIC rather than NULL handling.
# MAGIC
# MAGIC Column roles, data types, and the full NULL map are covered in Notebook
# MAGIC **01**.
# MAGIC
# MAGIC Controlling where NULLs appear in a sort with `nullsFirst` and `nullsLast`
# MAGIC is covered in Notebook **07**.

# COMMAND ----------

# DBTITLE 1,Load and filter
from pyspark.sql import functions as F
from pyspark.sql.window import Window

trip_enriched_table = "rideshare_dev.processed.trip_enriched"

trip_enriched = spark.table(trip_enriched_table)  # noqa: F821
dated_trip = trip_enriched.filter(F.col("trip_date").isNotNull())

print(f"trip_enriched: observed={trip_enriched.count()}, expected=106")
print(f"dated_trip: observed={dated_trip.count()}, expected=100")

# COMMAND ----------

# DBTITLE 1,Section 1 - Reliable running revenue per borough
# MAGIC %md
# MAGIC ## 1. How do we calculate reliable running revenue for each borough?
# MAGIC
# MAGIC In Notebook 05, the calculation used **all trips in each borough**. Every
# MAGIC Manhattan trip kept its own fare while also showing Manhattan's average
# MAGIC fare, so the same average appeared on every Manhattan row.
# MAGIC
# MAGIC Finance's question is different. Each trip should now show the borough's
# MAGIC **running revenue from the first trip through the current trip**.
# MAGIC
# MAGIC The partition stays the same:
# MAGIC
# MAGIC ```python
# MAGIC partitionBy("pickup_borough")
# MAGIC ```
# MAGIC
# MAGIC The same calculation is applied to every borough, but the trips are
# MAGIC processed separately within each borough.
# MAGIC
# MAGIC To calculate running revenue, Spark needs to know **the order of the trips
# MAGIC within each borough** and **which trips to include when calculating the
# MAGIC current row** — the frame.
# MAGIC
# MAGIC The next cells compare three scenarios: Spark's default `RANGE` frame, an
# MAGIC explicit `ROWS` frame with tied dates, and an explicit `ROWS` frame with a
# MAGIC stable trip order.
# MAGIC
# MAGIC | Subsection | Result | What it shows |
# MAGIC | --- | --- | --- |
# MAGIC | 1a — Default `RANGE` | `running_fare_revenue_default_range` | Same-date trips are included together |  # noqa: E501
# MAGIC | 1b — Unstable `ROWS` | `running_fare_revenue_unstable` | One total per row, but same-date trip order is undefined |  # noqa: E501
# MAGIC | 1c — Stable `ROWS` | `running_fare_revenue_stable` | One total per trip in a reproducible order |  # noqa: E501
# MAGIC
# MAGIC We display Manhattan so the three `2026-03-01` trips from the introduction
# MAGIC appear again with all three results side by side.

# COMMAND ----------

# DBTITLE 1,Section 1a - Default RANGE
# MAGIC %md
# MAGIC ### 1a. Why does every trip on the date receive the same total?
# MAGIC
# MAGIC First reproduce the result from the introduction. Order by `trip_date` and
# MAGIC do not specify a frame. Spark therefore uses its default `RANGE` frame,
# MAGIC which includes rows sharing the current date together.

# COMMAND ----------

# DBTITLE 1,Default RANGE running revenue
# Calculate each borough independently.
borough_partition = Window.partitionBy("pickup_borough")

# Leaving the frame unspecified reproduces the default RANGE behavior.
default_range_window = borough_partition.orderBy("trip_date")

trip_with_running_revenue = dated_trip.withColumn(
    "running_fare_revenue_default_range",
    F.round(
        F.sum(F.col("base_fare_amount")).over(default_range_window),
        2,
    ),
)

trip_with_running_revenue.filter(
    (F.col("pickup_borough") == "Manhattan") & (F.col("trip_date") == F.lit("2026-03-01"))
).select(
    "trip_date",
    "hour_of_day",
    "trip_id",
    "base_fare_amount",
    "running_fare_revenue_default_range",  # derived column
).orderBy(
    "hour_of_day",
    "trip_id",
).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Section 1b - Add a ROWS frame
# MAGIC %md
# MAGIC ### 1b. Does `rowsBetween` fix the running total?
# MAGIC
# MAGIC `rowsBetween` changes the calculation from date groups to physical rows.
# MAGIC Spark now calculates one running total per trip.
# MAGIC
# MAGIC However, ordering only by `trip_date` still leaves same-date trip order
# MAGIC undefined.

# COMMAND ----------

# DBTITLE 1,Unstable ROWS running revenue
# The date-only order still leaves same-date trips tied.
unstable_running_window = borough_partition.orderBy(
    "trip_date",
).rowsBetween(
    Window.unboundedPreceding,
    Window.currentRow,
)

trip_with_running_revenue = trip_with_running_revenue.withColumn(
    "running_fare_revenue_unstable",
    F.round(
        F.sum(F.col("base_fare_amount")).over(unstable_running_window),
        2,
    ),
)

trip_with_running_revenue.filter(
    (F.col("pickup_borough") == "Manhattan") & (F.col("trip_date") == F.lit("2026-03-01"))
).select(
    "trip_date",
    "hour_of_day",
    "trip_id",
    "base_fare_amount",
    "running_fare_revenue_default_range",  # derived column
    "running_fare_revenue_unstable",  # derived column
).orderBy(
    "hour_of_day",
    "trip_id",
).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Section 1c - Add a stable trip order
# MAGIC %md
# MAGIC ### 1c. How do we make the running total reproducible?
# MAGIC
# MAGIC Add `hour_of_day` to order trips within each date. Add the unique `trip_id`
# MAGIC last to resolve trips that share both a date and an hour.

# COMMAND ----------

# DBTITLE 1,Stable ROWS running revenue
# Define the complete order before attaching the cumulative frame.
stable_running_window = borough_partition.orderBy(
    "trip_date",
    "hour_of_day",
    "trip_id",
).rowsBetween(
    Window.unboundedPreceding,
    Window.currentRow,
)

trip_with_running_revenue = trip_with_running_revenue.withColumn(
    "running_fare_revenue_stable",
    F.round(
        F.sum(F.col("base_fare_amount")).over(stable_running_window),
        2,
    ),
)

trip_with_running_revenue.filter(
    (F.col("pickup_borough") == "Manhattan") & (F.col("trip_date") == F.lit("2026-03-01"))
).select(
    "trip_date",
    "hour_of_day",
    "trip_id",
    "base_fare_amount",
    "running_fare_revenue_default_range",  # derived column
    "running_fare_revenue_unstable",  # derived column
    "running_fare_revenue_stable",  # derived column
).orderBy(
    "hour_of_day",
    "trip_id",
).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Section 1 result
# MAGIC %md
# MAGIC - **Default `RANGE`:** **95.20, 95.20, 95.20**
# MAGIC - **Unstable `ROWS`:** one total per trip, but assignment may vary
# MAGIC - **Stable `ROWS`:** **15.09 → 88.71 → 95.20**

# COMMAND ----------

# DBTITLE 1,Verify the window preserved every trip
# Window columns add values without removing any of the 100 dated trips.
running_revenue_rows = trip_with_running_revenue.count()

print(f"window input/output: observed={running_revenue_rows}, expected=100")

# COMMAND ----------

# DBTITLE 1,Section 2 - First and final ordered fares
# MAGIC %md
# MAGIC ## 2. What were each borough's first and final ordered fares?
# MAGIC
# MAGIC An analyst wants every trip to show two reference values for its borough:
# MAGIC the first fare and the final fare in the 14-day period.
# MAGIC
# MAGIC Reuse Section 1's stable order:
# MAGIC `trip_date`, `hour_of_day`, then `trip_id`.
# MAGIC
# MAGIC | Subsection | Explicit `ROWS` frame | What happens |
# MAGIC | --- | --- | --- |
# MAGIC | 2a — Current-row frame | First row → current row | `first_value` stays fixed; `last_value` returns the current trip |  # noqa: E501
# MAGIC | 2b — Full frame | First row → final row | `last_value` returns the borough's final ordered fare |  # noqa: E501

# COMMAND ----------

# DBTITLE 1,Section 2a - Current-row frame
# MAGIC %md
# MAGIC ### 2a. Why does `last_value` return the current trip's fare?
# MAGIC
# MAGIC Build one stable trip order and explicitly define the frame as:
# MAGIC
# MAGIC **first row → current row**

# COMMAND ----------

# DBTITLE 1,First and last value through the current row
# Reuse the complete ordering pattern established in Section 1c.
borough_trip_order_window = borough_partition.orderBy(
    "trip_date",
    "hour_of_day",
    "trip_id",
)

# Both functions can read from the borough's first row through the current row.
borough_current_row_window = borough_trip_order_window.rowsBetween(
    Window.unboundedPreceding,
    Window.currentRow,
)

trip_with_borough_edge_fares = dated_trip.withColumn(
    "borough_first_base_fare",
    F.first_value(F.col("base_fare_amount")).over(borough_current_row_window),
).withColumn(
    "borough_last_base_fare_current_row",
    F.last_value(F.col("base_fare_amount")).over(borough_current_row_window),
)

trip_with_borough_edge_fares.filter(F.col("pickup_borough") == "Bronx").select(
    "pickup_borough",
    "trip_date",
    "hour_of_day",
    "trip_id",
    "base_fare_amount",
    "borough_first_base_fare",  # derived column
    "borough_last_base_fare_current_row",  # derived column
).orderBy(
    "trip_date",
    "hour_of_day",
    "trip_id",
).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Section 2b - Full frame
# MAGIC %md
# MAGIC ### 2b. How do we return the borough's final ordered fare?
# MAGIC
# MAGIC The 2a frame starts at the borough's first row but stops at the current
# MAGIC row.
# MAGIC
# MAGIC That works for `first_value`: the first row never changes.
# MAGIC
# MAGIC It does not work for `last_value`: the last row currently visible is the
# MAGIC current trip itself.
# MAGIC
# MAGIC Extend the frame through `Window.unboundedFollowing` so every trip can read
# MAGIC the borough's final ordered row.

# COMMAND ----------

# DBTITLE 1,Last value with the full frame
# Open the frame from the borough's first row through its final ordered row.
borough_full_frame_window = borough_trip_order_window.rowsBetween(
    Window.unboundedPreceding,
    Window.unboundedFollowing,
)

trip_with_borough_edge_fares = trip_with_borough_edge_fares.withColumn(
    "borough_last_base_fare_full_frame",
    F.last_value(F.col("base_fare_amount")).over(borough_full_frame_window),
)

trip_with_borough_edge_fares.filter(F.col("pickup_borough") == "Bronx").select(
    "pickup_borough",
    "trip_date",
    "hour_of_day",
    "trip_id",
    "base_fare_amount",
    "borough_first_base_fare",  # derived column
    "borough_last_base_fare_current_row",  # derived column
    "borough_last_base_fare_full_frame",  # derived column
).orderBy(
    "trip_date",
    "hour_of_day",
    "trip_id",
).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Summarize first and final fare by borough
# The full-frame values repeat within each borough, so one distinct row
# summarizes the starting and ending fare for each partition.
borough_edge_fare_summary = trip_with_borough_edge_fares.select(
    "pickup_borough",
    "borough_first_base_fare",
    "borough_last_base_fare_full_frame",
).distinct()

borough_edge_fare_summary_rows = borough_edge_fare_summary.count()
print(
    "borough summary:",
    f"observed={borough_edge_fare_summary_rows},",
    "expected=5",
)

borough_edge_fare_summary.orderBy("pickup_borough").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Section 2 result
# MAGIC %md
# MAGIC - `first_value` works with the current-row frame because the frame
# MAGIC   always begins at the borough's first row.
# MAGIC - `last_value` needs the full frame to read the borough's final ordered row.
# MAGIC - Staten Island has one dated trip, so its first and final values are the
# MAGIC   same. That is the correct result.

# COMMAND ----------

# DBTITLE 1,Section 3 - Accumulate through each date
# MAGIC %md
# MAGIC ## 3. How much has accumulated through each date?
# MAGIC
# MAGIC The input contains **100 trips across 14 dates**.
# MAGIC
# MAGIC For each `trip_date`, show:
# MAGIC
# MAGIC - Number of trips on that date
# MAGIC - Fare revenue on that date
# MAGIC - Trips accumulated from the first date through that date
# MAGIC - Revenue accumulated from the first date through that date
# MAGIC
# MAGIC First collapse the trips into **14 daily rows**.
# MAGIC
# MAGIC Then order those rows by `trip_date` and calculate from:
# MAGIC
# MAGIC **first date → current date**
# MAGIC
# MAGIC No `partitionBy` is needed because all 14 dates belong to one continuous
# MAGIC reporting period.

# COMMAND ----------

# DBTITLE 1,Build and verify the daily series
# Collapse the trip-level input into one row per date.
daily_summary = dated_trip.groupBy("trip_date").agg(
    F.count("*").alias("trip_count"),
    F.round(
        F.sum(F.col("base_fare_amount")),
        2,
    ).alias("daily_base_fare"),
)

daily_summary_rows = daily_summary.count()
print(f"daily summary: observed={daily_summary_rows}, expected=14")

daily_summary.orderBy("trip_date").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Add running totals to the daily series
# Accumulate values from the first date through the current date.
daily_running_window = Window.orderBy("trip_date").rowsBetween(
    Window.unboundedPreceding,
    Window.currentRow,
)

daily_with_running_totals = daily_summary.withColumn(
    "running_base_fare",
    F.round(
        F.sum(F.col("daily_base_fare")).over(daily_running_window),
        2,
    ),
).withColumn(
    "running_trip_count",
    F.sum(F.col("trip_count")).over(daily_running_window),
)

daily_with_running_totals.orderBy("trip_date").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Section 3 result
# MAGIC %md
# MAGIC The output has **14 rows — one per `trip_date`**.
# MAGIC
# MAGIC - `trip_count` and `daily_base_fare` describe that date.
# MAGIC - `running_trip_count` and `running_base_fare` describe the period from the
# MAGIC   first date through that date.

# COMMAND ----------

# DBTITLE 1,Section 4 - Compare with neighbouring days
# MAGIC %md
# MAGIC ## 4. Did today beat the previous day?
# MAGIC
# MAGIC A running total answers **"how much so far?"** Finance now wants each daily
# MAGIC fare total beside the values from the rows immediately before and after it.
# MAGIC
# MAGIC - `lag(column, 1)` reads the column from one earlier row.
# MAGIC - `lead(column, 1)` reads the column from one later row.
# MAGIC
# MAGIC Reuse the 14-row `daily_with_running_totals` series from Section 3 and order
# MAGIC it by `trip_date`. No `partitionBy` is needed because all 14 dates belong to
# MAGIC the same reporting period.
# MAGIC
# MAGIC The worked data contains every date from `2026-03-01` through
# MAGIC `2026-03-14`, so the previous row is also the previous calendar day here.
# MAGIC That is a property of this dataset—not a guarantee made by `lag`.
# MAGIC
# MAGIC Unlike a running aggregate, `lag` and `lead` use a fixed row offset. Their
# MAGIC ordered window does not need `rowsBetween`.

# COMMAND ----------

# DBTITLE 1,Add previous, next, and change columns
# lag and lead use row offsets in this ordered series; they do not use a
# running ROWS frame.
daily_order_window = Window.orderBy("trip_date")

daily_with_comparisons = (
    daily_with_running_totals.withColumn(
        "previous_day_base_fare",
        F.lag(F.col("daily_base_fare"), 1).over(daily_order_window),
    )
    .withColumn(
        "next_day_base_fare",
        F.lead(F.col("daily_base_fare"), 1).over(daily_order_window),
    )
    .withColumn(
        "base_fare_change_vs_previous_day",
        F.round(
            F.col("daily_base_fare") - F.col("previous_day_base_fare"),
            2,
        ),
    )
)

daily_with_comparisons.select(
    "trip_date",
    "trip_count",
    "daily_base_fare",
    "previous_day_base_fare",  # derived column
    "base_fare_change_vs_previous_day",  # derived column
    "next_day_base_fare",  # derived column
).orderBy(
    "trip_date",
).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Interpret the boundaries and the offset
# MAGIC %md
# MAGIC - The first date has no previous row, so `previous_day_base_fare` and the
# MAGIC   change column are NULL.
# MAGIC - The final date has no later row, so `next_day_base_fare` is NULL.
# MAGIC - A positive change means the current date earned more base fare than the
# MAGIC   previous row; a negative change means it earned less.
# MAGIC
# MAGIC The offset is measured in **rows**, not calendar days. If `2026-03-08`
# MAGIC were missing, the `lag` value for `2026-03-09` would come from
# MAGIC `2026-03-07`, not from an automatically created March 8 row.

# COMMAND ----------

# DBTITLE 1,Exercise
# MAGIC %md
# MAGIC ## Exercise — Track daily tip trends by service type
# MAGIC
# MAGIC Finance wants tip trends **per service type**, not across the whole fleet.
# MAGIC
# MAGIC Work in three steps:
# MAGIC
# MAGIC 1. Collapse `dated_trip` to one row per (`service_type`, `trip_date`) with
# MAGIC    `daily_tip_amount`.
# MAGIC 2. Add `running_tip_amount` (Section 3 frame, partitioned by `service_type`).
# MAGIC 3. Add `previous_row_tip_amount` and `tip_change_vs_previous_row` (Section 4
# MAGIC    `lag`).
# MAGIC
# MAGIC | Column | Reuse from |
# MAGIC |---|---|
# MAGIC | `running_tip_amount` | Section 3 — `rowsBetween(unboundedPreceding, currentRow)` |
# MAGIC | `previous_row_tip_amount` | Section 4 — `lag(..., 1)` |
# MAGIC | `tip_change_vs_previous_row` | Section 4 — current minus lagged |

# COMMAND ----------

# DBTITLE 1,Exercise step 1 - Service-date grain
# MAGIC %md
# MAGIC ### Step 1 — One row per (`service_type`, `trip_date`)
# MAGIC
# MAGIC 1. Predict how many **distinct `service_type` values** appear in
# MAGIC    `dated_trip` (`UNKNOWN` sits on undated trips only).
# MAGIC 2. Predict how many rows the grouped result has.
# MAGIC 3. Build `service_daily_tip` with `daily_tip_amount` = rounded sum of
# MAGIC    `tip_amount`.
# MAGIC 4. Compare predictions to the actual distinct service types and row count.

# COMMAND ----------

# DBTITLE 1,Build and verify service-date grain
predicted_service_type_count = None  # TODO: replace with your prediction
predicted_service_date_rows = None  # TODO: replace with your prediction

service_daily_tip = dated_trip.groupBy(
    "service_type",
    "trip_date",
).agg(
    F.round(
        F.sum(F.col("tip_amount")),
        2,
    ).alias("daily_tip_amount"),
)

actual_service_type_count = (
    service_daily_tip.select(
        "service_type",
    )
    .distinct()
    .count()
)
actual_service_date_rows = service_daily_tip.count()

service_type_match = "✓" if predicted_service_type_count == actual_service_type_count else "✗"
service_date_match = "✓" if predicted_service_date_rows == actual_service_date_rows else "✗"
print(
    f"{service_type_match} service types:",
    f"predicted={predicted_service_type_count},",
    f"actual={actual_service_type_count}",
)
print(
    f"{service_date_match} service-date rows:",
    f"predicted={predicted_service_date_rows},",
    f"actual={actual_service_date_rows}",
)

service_daily_tip.orderBy("service_type", "trip_date").show(20, truncate=False)

# COMMAND ----------

# DBTITLE 1,Exercise step 2 - Running tip total
# MAGIC %md
# MAGIC ### Step 2 — Running tip total within each service type
# MAGIC
# MAGIC Partition by `service_type`, order by `trip_date`, and use the same
# MAGIC cumulative **ROWS** frame as Section 3.
# MAGIC
# MAGIC Each service type's running total should **restart** on its first date.

# COMMAND ----------

# DBTITLE 1,Add running tip amount
# TODO: Window.partitionBy("service_type").orderBy("trip_date").rowsBetween(
# Window.unboundedPreceding, Window.currentRow)
service_running_window = None

service_with_running_tip = service_daily_tip.withColumn(
    "running_tip_amount",
    F.round(
        F.sum(F.col("daily_tip_amount")).over(service_running_window),
        2,
    ),
)

service_with_running_tip.filter(
    F.col("service_type") == "XL",
).orderBy("trip_date").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Exercise step 3 - Lag and row-over-row change
# MAGIC %md
# MAGIC ### Step 3 — Change vs the previous row in that series
# MAGIC
# MAGIC Use the same partition and order **without** `rowsBetween`.
# MAGIC
# MAGIC `lag` reads the previous **row** in the ordered partition, not yesterday on
# MAGIC the calendar. `XL` appears on only 8 of the 14 dates.

# COMMAND ----------

# DBTITLE 1,Add lag and tip change
# TODO: Window.partitionBy("service_type").orderBy("trip_date")
service_order_window = None

service_tip_trend = service_with_running_tip.withColumn(
    "previous_row_tip_amount",
    F.lag(F.col("daily_tip_amount"), 1).over(service_order_window),
).withColumn(
    "tip_change_vs_previous_row",
    F.round(
        F.col("daily_tip_amount") - F.col("previous_row_tip_amount"),
        2,
    ),
)

service_tip_trend.filter(
    F.col("service_type") == "XL",
).orderBy("trip_date").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Exercise interpretation
# MAGIC %md
# MAGIC - The running tip amount restarts for each service type.
# MAGIC - The first row in each service type has no previous row, so its lag and
# MAGIC   change values are NULL.
# MAGIC - `XL` has rows on 8 of the 14 dates. Where dates are skipped, `lag` reads
# MAGIC   the previous available XL row rather than necessarily yesterday.
# MAGIC
# MAGIC The grouped input contains **4 service types** and **44 service-date rows**.

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Idea | Takeaway |
# MAGIC |---|---|
# MAGIC | Default ordered frame | With `orderBy` and no frame, aggregates use `RANGE` — rows that share the sort key are evaluated together |  # noqa: E501
# MAGIC | Running totals | Use `rowsBetween(unboundedPreceding, currentRow)` when each row should accumulate **physical** rows, not tied dates |  # noqa: E501
# MAGIC | Trip-level order | After `trip_date`, add `hour_of_day` and `trip_id` so same-day trips sort the same way every run |  # noqa: E501
# MAGIC | Edge values | `first_value` reads the partition start; `last_value` for the true final value needs `unboundedFollowing` |  # noqa: E501
# MAGIC | Row offsets | `lag` / `lead` step by row within the partition — not calendar days; first/last rows in the partition get NULL |  # noqa: E501
# MAGIC
# MAGIC **Frame rule of thumb:** set an explicit `ROWS` frame for running aggregates
# MAGIC and for `last_value`; `lag` and `lead` only need `partitionBy` + `orderBy`.
# MAGIC
# MAGIC NULL placement in ordered windows: Notebook **07** (`nullsFirst` /
# MAGIC `nullsLast`).
# MAGIC
# MAGIC **Next:** Module 8 **`07 - Top-N per Group and Sampling`** — Top-N with
# MAGIC `row_number`, NULL sort placement, and sampling.
