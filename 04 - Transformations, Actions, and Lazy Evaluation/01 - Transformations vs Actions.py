# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Transformations vs Actions
# MAGIC
# MAGIC Distinguish transformations from actions on chains learners already write.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Distinguish transformations (new DataFrame, build logical plan) from actions
# MAGIC   (execute the plan)
# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up the rideshare example
# MAGIC
# MAGIC Modules 2 and 3 built chains of **`select`**, **`filter`**, and
# MAGIC **`withColumn`** without asking when Spark ran them. One scenario runs
# MAGIC through this notebook: a nightly operations review of standard-service trips
# MAGIC that covered at least five miles.
# MAGIC
# MAGIC Build a small DataFrame with four columns from the course **`trip`** schema:
# MAGIC **`trip_id`**, **`service_type`**, **`trip_distance_miles`**, and
# MAGIC **`ride_duration_mins`**.

# COMMAND ----------

from decimal import Decimal

from pyspark.sql import functions as F

trips = spark.createDataFrame(  # pyright: ignore[reportUndefinedVariable]  # noqa: F821
    [
        (1001, "standard", Decimal("2.40"), 12),
        (1002, "premium", Decimal("8.75"), 26),
        (1003, "standard", Decimal("6.20"), 21),
        (1004, "shared", Decimal("3.10"), 18),
        (1005, "standard", Decimal("11.50"), 34),
        (1006, "premium", Decimal("4.80"), 16),
    ],
    """
    trip_id bigint,
    service_type string,
    trip_distance_miles decimal(8,2),
    ride_duration_mins int
    """,
)

trips.show()

# COMMAND ----------

# MAGIC %md
# MAGIC The **`show()`** call is what displayed those rows — it is an action, and the
# MAGIC sections below explain why that distinction matters.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Building a chain produces no output
# MAGIC
# MAGIC **Business question:** The nightly review needs standard-service trips of at
# MAGIC least five miles, with each distance also expressed in kilometers. What
# MAGIC happens when you write those steps and run the cell?

# COMMAND ----------

review_trips = (
    trips.filter((F.col("service_type") == "standard") & (F.col("trip_distance_miles") >= 5))
    .withColumn(
        "trip_distance_km",
        F.round(F.col("trip_distance_miles") * F.lit(1.60934), 2),
    )
    .select(
        "trip_id",
        "service_type",
        "trip_distance_miles",
        "trip_distance_km",
        "ride_duration_mins",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC The cell finished without printing a single row. Spark recorded the three
# MAGIC steps in the DataFrame's **logical plan** — its internal description of the
# MAGIC requested result — and processed no data.
# MAGIC
# MAGIC That is the behavior this notebook explains: **`filter`**, **`withColumn`**,
# MAGIC and **`select`** are transformations, and transformations on their own never
# MAGIC produce output.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformations return new DataFrames
# MAGIC
# MAGIC A **transformation** defines a processing step and returns a new DataFrame.
# MAGIC Run the first two steps of the review separately to see what each call hands
# MAGIC back.
# MAGIC
# MAGIC **Business question:** The review adds a kilometer column. Does that change
# MAGIC the shared **`trips`** DataFrame that other reports read?

# COMMAND ----------

filtered_trips = trips.filter(
    (F.col("service_type") == "standard") & (F.col("trip_distance_miles") >= 5)
)

trips_with_km = filtered_trips.withColumn(
    "trip_distance_km",
    F.round(F.col("trip_distance_miles") * F.lit(1.60934), 2),
)

print("filter returned:    ", type(filtered_trips).__name__)
print("withColumn returned:", type(trips_with_km).__name__)

# COMMAND ----------

print("Source columns:     ", trips.columns)
print("Transformed columns:", trips_with_km.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC Both calls returned a **`DataFrame`**, and neither printed rows. The source
# MAGIC **`trips`** still has its original four columns — **`trip_distance_km`**
# MAGIC exists only in the plan held by **`trips_with_km`**.
# MAGIC
# MAGIC A transformation never modifies the DataFrame it was called on, so other
# MAGIC reports reading **`trips`** are unaffected by this review.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Actions run the plan and produce output
# MAGIC
# MAGIC An **action** asks Spark to execute a DataFrame's plan. Depending on the
# MAGIC API, an action displays rows, returns a Python value, or writes data to
# MAGIC storage.
# MAGIC
# MAGIC **Business question:** The nightly review has to list its trips and report
# MAGIC how many it found. Which calls make that happen?

# COMMAND ----------

show_result = review_trips.show(truncate=False)

print("show() returned:", type(show_result).__name__)

# COMMAND ----------

review_count = review_trips.count()

print("count() returned:", type(review_count).__name__)
print("Number of records:", review_count)

# COMMAND ----------

# MAGIC %md
# MAGIC Trips **`1003`** and **`1005`** are the only standard-service trips of at
# MAGIC least five miles. **`show()`** displayed them and returned **`None`**;
# MAGIC **`count()`** returned the row count as a Python **`int`**. Both executed the
# MAGIC plan that produced no output when it was built.
# MAGIC
# MAGIC > **Good to know:** Each action executes the plan again. The two cells above
# MAGIC > ran the same filter, kilometer conversion, and column selection twice —
# MAGIC > once for **`show()`** and once for **`count()`**. The next notebook looks at
# MAGIC > the plan itself and at why Spark waits for an action.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Let's see more examples
# MAGIC
# MAGIC Each example below builds its own DataFrame and its own transformation
# MAGIC chain, then calls one action. Classify every step as you read.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chain transformations before a single action — example 1
# MAGIC
# MAGIC **Business question:** Which standard-service trips lasted 20 minutes or
# MAGIC more, and what is each trip's distance in kilometers?

# COMMAND ----------

ops_review = spark.createDataFrame(  # pyright: ignore[reportUndefinedVariable]  # noqa: F821
    [
        (2001, "standard", Decimal("4.10"), 15),
        (2002, "standard", Decimal("7.80"), 24),
        (2003, "premium", Decimal("9.20"), 28),
        (2004, "standard", Decimal("5.50"), 22),
        (2005, "shared", Decimal("3.40"), 19),
    ],
    """
    trip_id bigint,
    service_type string,
    trip_distance_miles decimal(8,2),
    ride_duration_mins int
    """,
)

long_standard = (
    ops_review.filter((F.col("service_type") == "standard") & (F.col("ride_duration_mins") >= 20))
    .withColumn(
        "trip_distance_km",
        F.round(F.col("trip_distance_miles") * F.lit(1.60934), 2),
    )
    .select(
        "trip_id",
        "service_type",
        "ride_duration_mins",
        "trip_distance_km",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC **`filter`**, **`withColumn`**, and **`select`** are transformations — the
# MAGIC chain built a plan and printed nothing. Call one action to execute it.

# COMMAND ----------

long_standard.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Trips **`2002`** and **`2004`** meet both conditions. One **`show()`** ran
# MAGIC the full chain.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chain transformations before a single action — example 2
# MAGIC
# MAGIC **Business question:** For premium trips, what are the two longest rides by
# MAGIC distance?

# COMMAND ----------

premium_trips = spark.createDataFrame(  # pyright: ignore[reportUndefinedVariable]  # noqa: F821
    [
        (3001, "premium", Decimal("6.40"), 18),
        (3002, "standard", Decimal("12.00"), 40),
        (3003, "premium", Decimal("11.10"), 32),
        (3004, "premium", Decimal("3.90"), 14),
        (3005, "premium", Decimal("8.75"), 26),
    ],
    """
    trip_id bigint,
    service_type string,
    trip_distance_miles decimal(8,2),
    ride_duration_mins int
    """,
)

top_premium = (
    premium_trips.filter(F.col("service_type") == "premium")
    .orderBy(F.col("trip_distance_miles").desc())
    .limit(2)
    .select("trip_id", "trip_distance_miles", "ride_duration_mins")
)

# COMMAND ----------

# MAGIC %md
# MAGIC **`filter`**, **`orderBy`**, **`limit`**, and **`select`** are all
# MAGIC transformations. **`orderBy`** and **`limit`** read as if they sort and
# MAGIC fetch immediately, but neither processes rows until an action runs.

# COMMAND ----------

top_premium.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Trips **`3003`** (**`11.10`** miles) and **`3005`** (**`8.75`** miles) are
# MAGIC the two longest premium rides. Classify by whether a call extends the plan
# MAGIC or executes it — not by how urgent the method name sounds.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC Build a similar isolated chain with columns from the course **`payment`**
# MAGIC schema:
# MAGIC
# MAGIC 1. Create **`exercise_df`** with **`trip_id`** (`bigint`),
# MAGIC    **`payment_method`** (`string`), **`base_fare_amount`**
# MAGIC    (`decimal(10,2)`), and **`tip_amount`** (`decimal(10,2)`). Use three or
# MAGIC    four small rows.
# MAGIC 2. Filter to rows where **`payment_method`** is **`"card"`**.
# MAGIC 3. Add **`fare_and_tip_amount`** as **`base_fare_amount + tip_amount`**.
# MAGIC 4. Select **`trip_id`**, **`payment_method`**, and
# MAGIC    **`fare_and_tip_amount`**.
# MAGIC 5. Before you run anything, label each of steps 2–4 as a transformation or
# MAGIC    an action.
# MAGIC 6. Call **`show()`** once after the chain is complete.
# MAGIC
# MAGIC Do not reuse DataFrames from earlier sections.

# COMMAND ----------

# Your code here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Recap this notebook's path:
# MAGIC
# MAGIC - **Transformations** return a new DataFrame and extend its logical plan
# MAGIC   without processing rows — **`select`**, **`filter`** / **`where`**,
# MAGIC   **`withColumn`**, **`orderBy`**, **`limit`**
# MAGIC - **Actions** trigger execution and return a result, display output, or
# MAGIC   write data — **`show`**, **`count`**
# MAGIC - **Source DataFrames are never modified** — each transformation builds a
# MAGIC   new plan
# MAGIC - **Chain transformations, then call one action** — every action re-executes
# MAGIC   the plan behind it
# MAGIC - **Method names can mislead** — **`orderBy`** and **`limit`** are still
# MAGIC   transformations until an action runs
# MAGIC
# MAGIC Next up: **Lazy Evaluation and the Query Plan** — why Spark waits for an
# MAGIC action and how to inspect the plan it builds.
