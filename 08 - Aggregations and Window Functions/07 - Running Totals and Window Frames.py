# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 07 - Running Totals and Window Frames
# MAGIC
# MAGIC In the previous lesson, you learned how to use window functions to summarize data for each driver and rank individual trips. 
# MAGIC
# MAGIC In this lesson, you will use ordered windows to calculate running totals. You will also see how window frames affect which rows are included in these calculations.
# MAGIC
# MAGIC You will also read the first fare and the last fare in each partition.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Calculate a running total with `rowsBetween(Window.unboundedPreceding, Window.currentRow)`.
# MAGIC - Explain why an `orderBy()` window without `rowsBetween()` gives tied rows the same total.
# MAGIC - Read the first and last fare with `first_value()`, `last_value()`, and `Window.unboundedFollowing`.

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach **all-purpose compute**.
# MAGIC
# MAGIC This notebook uses `rideshare_dev.processed.trip_enriched` filtered to **Brooklyn** and **Manhattan** trips on **`2026-03-01`** and **`2026-03-02`** (9 rows: 2 in Brooklyn and 7 in Manhattan).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

trip_enriched = spark.table("rideshare_dev.processed.trip_enriched")  # noqa: F821

sample_trips = trip_enriched.filter(
    F.col("pickup_borough").isin("Brooklyn", "Manhattan")
    & F.col("trip_date").isin("2026-03-01", "2026-03-02")
)

sample_trips.select(
    "pickup_borough",
    "trip_date",
    "hour_of_day",
    "trip_id",
    "base_fare_amount",
).orderBy("pickup_borough", "trip_date", "hour_of_day", "trip_id").show(truncate=False)

print("Rows:", sample_trips.count())

# COMMAND ----------

# DBTITLE 1,Section 1 - Total Fare per Borough
# MAGIC %md
# MAGIC ## 1. Total fare per borough
# MAGIC
# MAGIC **Question:** What is the total `base_fare_amount` for each borough across all trips?

# COMMAND ----------

partition_window = Window.partitionBy("pickup_borough")

running_totals = sample_trips.withColumn(
    "borough_total_fare",
    F.round(F.sum("base_fare_amount").over(partition_window), 2),
)

running_totals.select(
    "pickup_borough",
    "trip_date",
    "hour_of_day",
    "trip_id",
    "base_fare_amount",
    "borough_total_fare",
).orderBy("pickup_borough", "trip_date", "hour_of_day", "trip_id").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Section 2 - Running total with unique orderBy
# MAGIC %md
# MAGIC ## 2. Running total after each trip
# MAGIC
# MAGIC **Question:** What is the cumulative `base_fare_amount` after each trip when ordered by `trip_date`, `hour_of_day`, and `trip_id`?

# COMMAND ----------

partition_window = Window.partitionBy("pickup_borough")

running_window = (
    Window.partitionBy("pickup_borough")
    .orderBy("trip_date", "hour_of_day", "trip_id")
)

running_totals = (
    sample_trips
    .withColumn(
        "borough_total_fare",
        F.round(F.sum("base_fare_amount").over(partition_window), 2),
    )
    .withColumn(
        "running_fare",
        F.round(F.sum("base_fare_amount").over(running_window), 2),
    )
)

running_totals.select(
    "pickup_borough",
    "trip_date",
    "hour_of_day",
    "trip_id",
    "base_fare_amount",
    "borough_total_fare",
    "running_fare",
).orderBy("pickup_borough", "trip_date", "hour_of_day", "trip_id").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC The previous example uses Spark's default window frame. We can explicitly define a row-based frame using `rowsBetween(Window.unboundedPreceding, Window.currentRow)`.
# MAGIC
# MAGIC Both examples produce the same running totals because the combination of `trip_date`, `hour_of_day`, and `trip_id` is unique for each trip.

# COMMAND ----------

partition_window = Window.partitionBy("pickup_borough")

running_window = (
    Window.partitionBy("pickup_borough")
    .orderBy("trip_date", "hour_of_day", "trip_id")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

running_totals = (
    sample_trips
    .withColumn(
        "borough_total_fare",
        F.round(F.sum("base_fare_amount").over(partition_window), 2),
    )
    .withColumn(
        "running_fare",
        F.round(F.sum("base_fare_amount").over(running_window), 2),
    )
)

running_totals.select(
    "pickup_borough",
    "trip_date",
    "hour_of_day",
    "trip_id",
    "base_fare_amount",
    "borough_total_fare",
    "running_fare",
).orderBy("pickup_borough", "trip_date", "hour_of_day", "trip_id").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Section 3 - Tied orderBy values without rowsBetween
# MAGIC %md
# MAGIC ## 3. Running total when multiple rows share the same ordering value
# MAGIC
# MAGIC **Question:** How does the running total change when trips are ordered only by `trip_date`?

# COMMAND ----------

# MAGIC %md
# MAGIC ### Default frame: same date, same cumulative total

# COMMAND ----------

partition_window = Window.partitionBy("pickup_borough")

running_window = (
    Window.partitionBy("pickup_borough")
    .orderBy("trip_date")
)

running_totals = (
    sample_trips
    .withColumn(
        "borough_total_fare",
        F.round(F.sum("base_fare_amount").over(partition_window), 2),
    )
    .withColumn(
        "running_fare",
        F.round(F.sum("base_fare_amount").over(running_window), 2),
    )
)

running_totals.select(
    "pickup_borough",
    "trip_date",
    "hour_of_day",
    "trip_id",
    "base_fare_amount",
    "borough_total_fare",
    "running_fare",
).orderBy("pickup_borough", "trip_date", "hour_of_day", "trip_id").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC When an aggregate window uses `orderBy()` without an explicit frame, Spark uses a `RANGE` frame by default. Rows with the same ordering value receive the same cumulative total.
# MAGIC
# MAGIC For Manhattan, all three trips on `2026-03-01` show `95.20`, the total fare for that date.
# MAGIC
# MAGIC ### Explicit row frame: one trip at a time

# COMMAND ----------

date_rows_window = (
    Window.partitionBy("pickup_borough")
    .orderBy("trip_date")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

ordered_rows_window = (
    Window.partitionBy("pickup_borough")
    .orderBy("trip_date", "hour_of_day", "trip_id")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

running_totals = (
    sample_trips
    .withColumn(
        "running_fare_by_date",
        F.round(F.sum("base_fare_amount").over(date_rows_window), 2),
    )
    .withColumn(
        "running_fare_by_trip_order",
        F.round(F.sum("base_fare_amount").over(ordered_rows_window), 2),
    )
)

running_totals_ =  running_totals.select(
    "pickup_borough",
    "trip_date",
    "hour_of_day",
    "trip_id",
    "base_fare_amount",
    "running_fare_by_date",
    "running_fare_by_trip_order",
).orderBy("pickup_borough", "trip_date", "hour_of_day", "trip_id")

display(running_totals_)

# COMMAND ----------

# MAGIC %md
# MAGIC Both columns use `rowsBetween()`, so each total adds one trip at a time.
# MAGIC
# MAGIC `running_fare_by_date` is ordered only by `trip_date`. Trips that share a date have no fixed order, so trip `36` shows `21.58` and trip `46` shows `6.49`.
# MAGIC
# MAGIC `running_fare_by_trip_order` also orders by `hour_of_day` and `trip_id`, so Manhattan grows `15.09` → `88.71` → `95.20`.

# COMMAND ----------

# DBTITLE 1,Section 4 - First and last fare in each borough
# MAGIC %md
# MAGIC ## 4. First and last fare in each borough
# MAGIC
# MAGIC **Question:** For each borough, what were the first trip's fare and the final trip's fare in the ordered trip sequence?

# COMMAND ----------

current_row_window = (
    Window.partitionBy("pickup_borough")
    .orderBy("trip_date", "hour_of_day", "trip_id")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

full_partition_window = (
    Window.partitionBy("pickup_borough")
    .orderBy("trip_date", "hour_of_day", "trip_id")
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
)

first_and_last = (
    sample_trips
    .withColumn("first_fare", F.first_value(F.col("base_fare_amount")).over(current_row_window))
    .withColumn("last_fare_current_row", F.last_value(F.col("base_fare_amount")).over(current_row_window))
    .withColumn(
        "last_fare_full_partition",
        F.last_value(F.col("base_fare_amount")).over(full_partition_window),
    )
)

first_and_last.select(
    "pickup_borough",
    "trip_date",
    "hour_of_day",
    "trip_id",
    "base_fare_amount",
    "first_fare",
    "last_fare_current_row",
    "last_fare_full_partition",
).orderBy("pickup_borough", "trip_date", "hour_of_day", "trip_id").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC - **`first_fare`:** Returns the first fare in the borough (`55.65` for Brooklyn, `15.09` for Manhattan).
# MAGIC - **`last_fare_current_row`:** Ends at `Window.currentRow`, so it returns the current row's own fare.
# MAGIC - **`last_fare_full_partition`:** Extends to `Window.unboundedFollowing`, so it returns the final fare in the borough (`92.60` for Brooklyn, `43.16` for Manhattan).

# COMMAND ----------

# DBTITLE 1,Key Takeaways
# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC - `Window.partitionBy()` without `orderBy()` writes the full partition total on every row in that partition.
# MAGIC - With `orderBy()`, `rowsBetween(Window.unboundedPreceding, Window.currentRow)` adds one row at a time, from the first row in the partition through the current row.
# MAGIC - When an aggregate window uses `orderBy()` without `rowsBetween()`, Spark uses a `RANGE` frame. Rows that share the same ordering value receive the same cumulative total.
# MAGIC - `rowsBetween()` still adds one row at a time when ordering values tie, but the order of those tied rows is fixed only when `orderBy()` makes each row unique. Otherwise, the running totals for tied rows can change between runs.
# MAGIC - `first_value()` reads the first value in the frame, and `last_value()` reads the last value in the frame.
# MAGIC - A frame that ends at `Window.currentRow` makes `last_value()` return the current row's value. Extend the frame to `Window.unboundedFollowing` to return the final value in the partition.
# MAGIC
# MAGIC **Next:** `08 - Lag and Lead`