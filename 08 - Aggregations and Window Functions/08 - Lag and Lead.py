# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 08 - Lag and Lead
# MAGIC
# MAGIC In the previous lesson, you used window frames to calculate running totals and to read the first and last fare in each borough.
# MAGIC
# MAGIC In this lesson, you will use `lag()` and `lead()` to compare daily base fare totals with the previous and next available dates. 
# MAGIC
# MAGIC `lag()` reads a value from the previous row in the window, and `lead()` reads a value from the next row. 
# MAGIC
# MAGIC You can then answer questions such as "How much did the daily base fare change since the previous date?" without joining the table to itself.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Read the previous trip's fare with `lag()` and the next trip's fare with `lead()`.
# MAGIC - Calculate the change in fare between consecutive trips.
# MAGIC - Explain how `orderBy()` determines which rows `lag()` and `lead()` access.
# MAGIC - Handle the `null` returned for the first row (`lag()`) and the last row (`lead()`) in each partition.

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach **all-purpose compute**.
# MAGIC
# MAGIC This notebook uses the `trip_enriched` table created in Module 7, **07 - Build Unified Curated Tables**, which contains 106 rows, one row per trip.
# MAGIC
# MAGIC Six trips (`101`–`106`) have no `trip_date`. The notebook keeps the 100 dated trips and calculates the total `base_fare_amount` for each day.

# COMMAND ----------

# DBTITLE 1,Load and build daily_summary
from pyspark.sql import functions as F
from pyspark.sql.window import Window

trip_enriched = spark.table("rideshare_dev.processed.trip_enriched")  # noqa: F821
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
# MAGIC The Finance team wants to compare daily base fare totals to see whether revenue increased or decreased.
# MAGIC
# MAGIC `lag()` retrieves the previous date's total, while `lead()` retrieves the next date's total. Subtracting the previous total from the current total gives the daily change.

# COMMAND ----------

# DBTITLE 1,Add previous, next, and change columns
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
        "daily_change_in_revenue",
        F.round(
            F.col("daily_base_fare") - F.col("previous_day_base_fare"),
            2,
        ),
    )
)

daily_with_comparisons.select(
    "trip_date",
    "daily_base_fare",
    "previous_day_base_fare",  # derived column
    "daily_change_in_revenue",  # derived column
    "next_day_base_fare",  # derived column
).orderBy(
    "trip_date",
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC - The first date has no previous row, so `previous_day_base_fare` and the
# MAGIC  change column are NULL.
# MAGIC - The final date has no later row, so `next_day_base_fare` is NULL.
# MAGIC - A positive change means the current day earned more than the
# MAGIC  previous day; a negative change means it earned less.

# COMMAND ----------

# DBTITLE 1,Key Takeaways
# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC - `lag(column, n)` retrieves a value from `n` rows before the current row.
# MAGIC - `lead(column, n)` retrieves a value from `n` rows after the current row.
# MAGIC - `orderBy()` determines the row sequence. Offsets refer to row positions.
# MAGIC - By default, `lag()` and `lead()` return `NULL` when the requested previous or next row does not exist within the partition.
# MAGIC
# MAGIC **Next:** `09 - Top-N per Group and NULL Ordering`
