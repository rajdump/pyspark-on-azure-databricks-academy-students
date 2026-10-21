# Databricks notebook source
# MAGIC %md
# MAGIC # 08 - Lag and Lead
# MAGIC
# MAGIC Row-to-row comparisons across an ordered window using `lag()` and `lead()`.
# MAGIC
# MAGIC `trip_enriched`.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Compare adjacent rows using `lag()` and `lead()`
# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %md
# MAGIC ## Setup — load `trip_enriched` and build the 14-day daily summary
# MAGIC
# MAGIC Attach **all-purpose compute**.
# MAGIC
# MAGIC Six trips (`101`–`106`) have no `trip_date`. Keep the **100 dated trips**
# MAGIC and aggregate them into the **14-row daily summary** (`daily_summary`)
# MAGIC from Notebook **07**.

# COMMAND ----------

# DBTITLE 1,Load and build daily_summary
from pyspark.sql import functions as F
from pyspark.sql.window import Window

trip_enriched_table = "rideshare_dev.processed.trip_enriched"

trip_enriched = spark.table(trip_enriched_table)  # noqa: F821
dated_trip = trip_enriched.filter(F.col("trip_date").isNotNull())

daily_summary = dated_trip.groupBy("trip_date").agg(
    F.count("*").alias("trip_count"),
    F.round(
        F.sum(F.col("base_fare_amount")),
        2,
    ).alias("daily_base_fare"),
)

daily_summary.orderBy("trip_date").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Section 1 - Compare with neighbouring days
# MAGIC %md
# MAGIC ## 1. Did today beat the previous day?
# MAGIC
# MAGIC A running total answers **"how much so far?"** Finance now wants each daily
# MAGIC fare total beside the values from the rows immediately before and after it.
# MAGIC
# MAGIC - `lag(column, 1)` reads the column from one earlier row.
# MAGIC - `lead(column, 1)` reads the column from one later row.
# MAGIC
# MAGIC Order the 14-row `daily_summary` series by `trip_date`. No `partitionBy` is
# MAGIC needed because all 14 dates belong to the same reporting period.
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
    daily_summary.withColumn(
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

# DBTITLE 1,Key Takeaways
# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC - `lag(column, n)` reads a value from `n` rows before the current row within the ordered window.
# MAGIC - `lead(column, n)` reads a value from `n` rows after the current row within the ordered window.
# MAGIC - `lag()` and `lead()` step by **rows**, not calendar dates, and do not need `rowsBetween()`.
# MAGIC - Rows at the start or end of a partition have no previous or next row, so `lag()` and `lead()` return `NULL`.
# MAGIC
# MAGIC **Next:** Module 8 **`09 - Top-N per Group and Sampling`** — Top-N with
# MAGIC `row_number`, NULL sort placement, and sampling.
