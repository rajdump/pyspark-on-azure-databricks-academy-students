# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Window Functions Fundamentals
# MAGIC
# MAGIC Windows preserve input rows — contrast with `groupBy`.
# MAGIC
# MAGIC `trip_enriched`, `trip_driver_assignment`.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Build windows for ranking and partition-only aggregates
# MAGIC - Preview Top-2 filter-after-rank (full Top-N → **07**)
# COMMAND ----------

# MAGIC %md
# MAGIC A **window function** lets you add group-level information to each row while
# MAGIC keeping the row-level details.
# MAGIC
# MAGIC For example, each driver-trip row can keep its own trip distance while also
# MAGIC showing the driver's total trip count.
# MAGIC
# MAGIC **Qualified rule:** a window function adds group-level values to each detail
# MAGIC row without collapsing the rows. A later `filter()` can still remove rows and
# MAGIC change the result grain.
# MAGIC
# MAGIC ## What this notebook teaches
# MAGIC
# MAGIC | Section | Concept | Why it matters |
# MAGIC |---|---|---|
# MAGIC | 1 | `groupBy` vs window | Add group-level values without collapsing detail rows |
# MAGIC | 2 | Window aggregates | Add counts, totals, and averages to each detail row |
# MAGIC | 3 | Ranking functions | Rank rows within each group and handle ties |
# MAGIC | 4 | Filter after rank | Keep the top rows per group |
# MAGIC | Exercise | Service windows | Add service totals and a duration rank to each trip |
# MAGIC
# MAGIC Run Module 8 **01–04** and Module 7 **01–07**, especially
# MAGIC **`07 - Build Unified Curated Tables`** (managed tables used here).
# COMMAND ----------

# DBTITLE 1,Setup and baseline grain
# MAGIC %md
# MAGIC ## Setup — load both managed tables
# MAGIC
# MAGIC Shared schemas and inherited NULL details remain in Module 8
# MAGIC **`01 - GroupBy and Basic Aggregations`** and `docs/data/dataset-overview.md`.
# MAGIC
# MAGIC | DataFrame | Grain | Used for |
# MAGIC |---|---|---|
# MAGIC | `trip_enriched` | One row per `trip_id` (106) | Section 1, exercise |
# MAGIC | `trip_driver_assignment` | One (`driver_id`, `trip_id`) row (100) | Sections 2–4 |
# MAGIC
# MAGIC `trip_driver_assignment` already contains `trip_distance_miles` and
# MAGIC `ride_duration_mins` on every row, so Sections 2–4 do not need a join to
# MAGIC `trip_enriched`.
# MAGIC
# MAGIC Both columns are non-NULL across the dataset. This lets the ranking examples
# MAGIC focus on **ties**—what happens when two rows have the same value—without
# MAGIC introducing NULL ordering rules.

# COMMAND ----------

# DBTITLE 1,Load and verify the managed tables
from pyspark.sql import functions as F
from pyspark.sql.window import Window

trip_enriched_table = "rideshare_dev.processed.trip_enriched"
trip_driver_assignment_table = "rideshare_dev.processed.trip_driver_assignment"

trip_enriched = spark.table(trip_enriched_table)  # noqa: F821
trip_driver_assignment = spark.table(trip_driver_assignment_table)  # noqa: F821

trip_enriched_rows = trip_enriched.count()
trip_driver_assignment_rows = trip_driver_assignment.count()

print(f"trip_enriched: observed={trip_enriched_rows}, expected=106")
print(f"trip_driver_assignment: observed={trip_driver_assignment_rows}, expected=100")

# COMMAND ----------

# DBTITLE 1,How is a window different from groupBy?
# MAGIC %md
# MAGIC ## 1. How is a window different from `groupBy`?
# MAGIC
# MAGIC A borough report needs the average `base_fare_amount` for each
# MAGIC `pickup_borough`.
# MAGIC
# MAGIC With `groupBy`, the result contains **5 rows**—one row per borough.
# MAGIC
# MAGIC With a window function, the same borough average can be added to every trip
# MAGIC row. The result still contains **106 rows** because no trip rows are
# MAGIC collapsed.
# MAGIC
# MAGIC `F.avg` ignores NULL values, so trips 104 and 106 do not contribute to the
# MAGIC average `base_fare_amount`. Both approaches therefore calculate the average
# MAGIC from the same non-NULL values.

# COMMAND ----------

# DBTITLE 1,Calculate one average per borough
borough_avg_fare = trip_enriched.groupBy("pickup_borough").agg(
    F.round(
        F.avg(F.col("base_fare_amount")),
        2,
    ).alias("borough_avg_base_fare"),
)

borough_avg_fare.orderBy("pickup_borough").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Window specification anatomy
# MAGIC %md
# MAGIC ### Window specification anatomy
# MAGIC
# MAGIC A **window specification** defines which related rows participate in a window
# MAGIC calculation.
# MAGIC
# MAGIC - `Window.partitionBy("pickup_borough")` groups trips by `pickup_borough` for
# MAGIC   the calculation.
# MAGIC - `.over(borough_aggregate_window)` applies the window function to those
# MAGIC   related rows for each input row.
# MAGIC
# MAGIC Unlike `groupBy`, `partitionBy` does not collapse the rows. It only defines
# MAGIC the group used by the window calculation. The borough average is repeated on
# MAGIC every trip row in that borough.
# MAGIC
# MAGIC Spark may shuffle data to bring rows with the same partition key together.
# MAGIC Module 17 covers shuffle behavior and window performance; this notebook
# MAGIC focuses on how windows work.

# COMMAND ----------

# DBTITLE 1,Add the borough average to every trip
borough_aggregate_window = Window.partitionBy("pickup_borough")

trip_with_borough_avg = trip_enriched.withColumn(
    "borough_avg_base_fare",
    F.round(
        F.avg(F.col("base_fare_amount")).over(borough_aggregate_window),
        2,
    ),
)

trip_with_borough_avg.select(
    "trip_id",
    "pickup_borough",
    "base_fare_amount",
    "borough_avg_base_fare",
).orderBy(
    "pickup_borough",
    "trip_id",
).show(30, truncate=False)

# COMMAND ----------

# DBTITLE 1,Verify grouped and windowed grain
borough_group_rows = borough_avg_fare.count()
trip_with_borough_avg_rows = trip_with_borough_avg.count()

print(f"groupBy output: observed={borough_group_rows}, expected=5")
print(f"window input: observed={trip_enriched_rows}, expected=106")
print(f"window output: observed={trip_with_borough_avg_rows}, expected=106")
print("window preserved trip rows:", trip_with_borough_avg_rows == trip_enriched_rows)

# COMMAND ----------

# DBTITLE 1,How do we add driver trip count, total distance, and average duration to each row?
# MAGIC %md
# MAGIC ## 2. How do we add driver trip count, total distance, and average duration to each row?
# MAGIC
# MAGIC The borough example kept every trip row while adding a borough-level value.
# MAGIC Use the same window pattern on `trip_driver_assignment` to add each driver's
# MAGIC trip count, total distance, and average ride duration to every driver-trip
# MAGIC row.
# MAGIC
# MAGIC For driver `D001`, the expected metrics are:
# MAGIC
# MAGIC - **9** trips
# MAGIC - **78.50 miles** total trip distance
# MAGIC - **33.67 minutes** average ride duration
# MAGIC
# MAGIC These driver-level values should repeat across all nine D001 rows, while each
# MAGIC row keeps its own trip distance and ride duration.

# COMMAND ----------

# DBTITLE 1,Add partition-level driver metrics
driver_aggregate_window = Window.partitionBy("driver_id")

driver_with_metrics = (
    trip_driver_assignment.withColumn(
        "driver_trip_count",
        F.count(F.col("trip_id")).over(driver_aggregate_window),
    )
    .withColumn(
        "driver_total_distance_miles",
        F.round(
            F.sum(F.col("trip_distance_miles")).over(driver_aggregate_window),
            2,
        ),
    )
    .withColumn(
        "driver_avg_ride_duration_mins",
        F.round(
            F.avg(F.col("ride_duration_mins")).over(driver_aggregate_window),
            2,
        ),
    )
)

# COMMAND ----------

# DBTITLE 1,Inspect D001 driver metrics
driver_with_metrics.filter(
    F.col("driver_id") == "D001",
).select(
    "driver_id",
    "trip_id",
    "trip_distance_miles",
    "ride_duration_mins",
    "driver_trip_count",  # derived column
    "driver_total_distance_miles",  # derived column
    "driver_avg_ride_duration_mins",  # derived column
).orderBy(
    "trip_id",
).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Verify driver-trip grain
driver_with_metrics_rows = driver_with_metrics.count()

print(f"driver input: observed={trip_driver_assignment_rows}, expected=100")
print(f"driver output: observed={driver_with_metrics_rows}, expected=100")
print(
    "window preserved driver-trip rows:",
    driver_with_metrics_rows == trip_driver_assignment_rows,
)

# COMMAND ----------

# DBTITLE 1,How do we rank trips within each driver?
# MAGIC %md
# MAGIC ## 3. How do we rank trips within each driver?
# MAGIC
# MAGIC Partitioning decides which driver's rows belong together. Ranking also needs
# MAGIC an order within each driver. For `D010`, trip 64 has the longest distance at
# MAGIC **13.99 miles**, so it should rank first.
# MAGIC
# MAGIC | Function | What happens when values tie |
# MAGIC |---|---|
# MAGIC | `row_number` | Gives each row a unique sequence number |
# MAGIC | `rank` | Gives tied rows the same rank, then leaves a gap |
# MAGIC | `dense_rank` | Gives tied rows the same rank, with no gap afterward |
# MAGIC
# MAGIC All three ranking functions use the same ordering rule: trip distance
# MAGIC descending.
# MAGIC
# MAGIC - `rank` and `dense_rank` keep equal distances tied (same rank value).
# MAGIC - `row_number` still assigns a unique position even when distances match.
# MAGIC
# MAGIC `trip_distance_miles` is non-NULL in this dataset, so NULL ordering does not
# MAGIC affect these rankings. Module 8 **`07 - Top-N per Group and Sampling`**
# MAGIC covers `nullsFirst` and `nullsLast` when the ranking column can contain
# MAGIC NULLs.

# COMMAND ----------

# DBTITLE 1,Add three distance rankings
distance_rank_window = Window.partitionBy("driver_id").orderBy(
    F.col("trip_distance_miles").desc(),
)

driver_ranked = (
    driver_with_metrics.withColumn(
        "distance_row_number",
        F.row_number().over(distance_rank_window),
    )
    .withColumn(
        "distance_rank",
        F.rank().over(distance_rank_window),
    )
    .withColumn(
        "distance_dense_rank",
        F.dense_rank().over(distance_rank_window),
    )
)

# COMMAND ----------

# DBTITLE 1,Inspect D010 distance rankings
driver_ranked.filter(
    F.col("driver_id") == "D010",
).select(
    "driver_id",
    "trip_id",
    "trip_distance_miles",
    "distance_row_number",  # derived column
    "distance_rank",  # derived column
    "distance_dense_rank",  # derived column
).orderBy(
    "distance_row_number",
).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Interpret the D010 tie
# MAGIC %md
# MAGIC The three ranking columns **do not** all agree for D010.
# MAGIC
# MAGIC Trips 22 and 79 both have **8.81 miles**:
# MAGIC
# MAGIC - `rank` and `dense_rank` both stay at **4** — the tie is preserved.
# MAGIC - `row_number` uses **4** and **5** — unique positions, but distance alone
# MAGIC   does not decide which trip comes first.
# MAGIC - The next trip (7.65 miles) shows the gap: `rank` **6**, `dense_rank` **5**.

# COMMAND ----------

# DBTITLE 1,How do we keep the top rows per group?
# MAGIC %md
# MAGIC ## 4. How do we keep the top rows per group?
# MAGIC
# MAGIC Ranking keeps every row. A later `filter()` on the ranking column keeps only
# MAGIC the rows you want and changes the grain.
# MAGIC
# MAGIC For example, to keep the **top 2 longest trips per driver**, filter
# MAGIC `distance_row_number <= 2`. With 12 drivers, that yields **24** rows.
# MAGIC
# MAGIC For D001, that should be trip **8** (12.75 miles) and trip **81**
# MAGIC (12.31 miles).
# MAGIC
# MAGIC Module 8 **`07 - Top-N per Group and Sampling`** goes deeper on Top-N
# MAGIC patterns.

# COMMAND ----------

# DBTITLE 1,Keep the top 2 longest trips per driver
top2_trips_per_driver = driver_ranked.filter(
    F.col("distance_row_number") <= 2,
)

# COMMAND ----------

# DBTITLE 1,Inspect top 2 trips
top2_trips_per_driver.select(
    "driver_id",
    "trip_id",
    "trip_distance_miles",
    "distance_row_number",  # derived column
).orderBy(
    "driver_id",
    "distance_row_number",
).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Verify Top-2 grain
top2_trips_per_driver_rows = top2_trips_per_driver.count()

print(f"top-2 input: observed={driver_with_metrics_rows}, expected=100")
print(f"top-2 output: observed={top2_trips_per_driver_rows}, expected=24")
print("filter reduced driver-trip rows:", top2_trips_per_driver_rows < driver_with_metrics_rows)

# COMMAND ----------

# DBTITLE 1,Exercise - Add service metrics and duration rank
# MAGIC %md
# MAGIC ## Exercise — Add service totals and a duration rank to each trip
# MAGIC
# MAGIC Repeat Sections 2 and 3 on `trip_enriched`, partitioned by `service_type`.
# MAGIC
# MAGIC Add these columns to every trip row (keep all **106** rows):
# MAGIC
# MAGIC | Column | Window pattern |
# MAGIC |---|---|
# MAGIC | `service_trip_count` | Count of trips per service type |
# MAGIC | `service_avg_ride_duration_mins` | Average `ride_duration_mins` per service type, rounded to 2 |
# MAGIC | `ride_duration_dense_rank` | `dense_rank` of duration within service type (longest = 1) |
# MAGIC
# MAGIC Fill in the two window specs and your row-count prediction. Every `STANDARD`
# MAGIC row should show `service_trip_count` **55**.

# COMMAND ----------

# DBTITLE 1,Exercise - Build, verify, and inspect
predicted_output_rows = None  # TODO: replace with your prediction

# TODO: Window.partitionBy("service_type")
service_aggregate_window = None

# TODO: Window.partitionBy("service_type").orderBy(F.col("ride_duration_mins").desc())
service_duration_rank_window = None

service_window_summary = (
    trip_enriched.withColumn(
        "service_trip_count",
        F.count(F.col("trip_id")).over(service_aggregate_window),
    )
    .withColumn(
        "service_avg_ride_duration_mins",
        F.round(
            F.avg(F.col("ride_duration_mins")).over(service_aggregate_window),
            2,
        ),
    )
    .withColumn(
        "ride_duration_dense_rank",
        F.dense_rank().over(service_duration_rank_window),
    )
)

actual = service_window_summary.count()
match = "✓" if predicted_output_rows == actual else "✗"
print(f"{match} predicted={predicted_output_rows}, actual={actual}")

service_window_summary.filter(
    F.col("service_type") == "STANDARD",
).select(
    "service_type",
    "trip_id",
    "ride_duration_mins",
    "service_trip_count",  # derived column
    "service_avg_ride_duration_mins",  # derived column
    "ride_duration_dense_rank",  # derived column
).orderBy(
    "ride_duration_dense_rank",
    "trip_id",
).show(30, truncate=False)

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Idea | Takeaway |
# MAGIC |---|---|
# MAGIC | `groupBy` vs window | `groupBy` returns one row per group; a window repeats the group metric on every detail row |
# MAGIC | Partition-only aggregates | Add trip count, total distance, and average duration per driver without collapsing rows |
# MAGIC | Ranking + ties | `row_number` unique positions; `rank` gaps on ties; `dense_rank` no gaps |
# MAGIC | Filter after rank | `distance_row_number <= 2` keeps Top-2 per driver and changes grain (100 → 24) |
# MAGIC
# MAGIC **Next:** Module 8 **`06 - Running Totals and lag/lead`** adds ordered frames,
# MAGIC running calculations, `first_value`, `last_value`, `lag`, and `lead`.
# MAGIC Module 8 **`07 - Top-N per Group and Sampling`** goes deeper on Top-N and
# MAGIC introduces sampling.