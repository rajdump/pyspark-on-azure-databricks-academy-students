# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Missing, Blank, and Sentinel Values
# MAGIC
# MAGIC Normalize missing data to actual `NULL` values before dropping or filling them in.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Identify `NULL`, blanks, sentinels, and `NaN`
# MAGIC - Use `na.drop` / `na.fill` / `na.replace` and `F.coalesce` (not partition
# MAGIC   coalesce)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach classic **all-purpose** compute or serverless
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

rows = [
    (1001, "Card", 3.50, 5),
    (1002, "N/A", 0.00, 8),
    (1003, "", None, -1),
    (1004, "   ", 2.00, 3),
    (1005, None, 1.25, -1),
    (1006, "Cash", float("nan"), 4),
    (1007, "Card", None, None),
]

schema_ddl = "trip_id bigint, payment_method string, tip_amount double, request_to_pickup_mins int"

df = spark.createDataFrame(  # pyright: ignore[reportUndefinedVariable]  # noqa: F821
    rows,
    schema_ddl,
)

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## `NULL` vs other missing values
# MAGIC
# MAGIC **Business question:** Operations needs a clear profile of true `NULL` values versus other missing values.

# COMMAND ----------

df.select(
    "trip_id",
    "payment_method",
    "tip_amount",
    F.col("payment_method").isNull().alias("payment_method_is_null"),
    F.col("tip_amount").isNull().alias("tip_amount_is_null"),
    F.isnan(F.col("tip_amount")).alias("tip_amount_is_nan"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Blank strings, strings containing only spaces, sentinel values such as `"N/A"`, and values such as `-1` do not appear as missing in these checks.
# MAGIC
# MAGIC Spark treats them as regular values until you clean and standardize them.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## `Trim` blank payment methods
# MAGIC
# MAGIC **Business question:** Operations wants empty payment strings and strings containing only spaces to be treated as the same missing value.

# COMMAND ----------

trim_blank_payments = df.select(
    "trip_id",
    F.trim(F.col("payment_method")).alias("payment_method"),
    "tip_amount",
    "request_to_pickup_mins",
)

trim_blank_payments.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Replace sentinel values
# MAGIC
# MAGIC **Business question:** Operations needs `N/A` in payment method and `-1` in pickup wait stored as NULL.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 — text sentinel ("N/A"):

# COMMAND ----------

without_na = trim_blank_payments.select(
    "trip_id",
    F.when(F.col("payment_method") == "N/A", F.lit(None))
    .otherwise(F.col("payment_method"))
    .alias("payment_method"),
    "tip_amount",
    "request_to_pickup_mins",
)

without_na.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 — numeric sentinel (-1), on without_na:

# COMMAND ----------

without_sentinels = without_na.select(
    "trip_id",
    "payment_method",
    "tip_amount",
    F.when(F.col("request_to_pickup_mins") == -1, F.lit(None).cast("int"))
    .otherwise(F.col("request_to_pickup_mins"))
    .alias("request_to_pickup_mins"),
)

without_sentinels.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Turn `NaN` tip into `NULL`
# MAGIC **Business question:** Operations needs tip stored as NaN converted to NULL.
# MAGIC

# COMMAND ----------

without_NaN = without_sentinels.select(
    "trip_id",
    "payment_method",
    F.when(F.isnan("tip_amount"), F.lit(None).cast("double"))
    .otherwise(F.col("tip_amount"))
    .alias("tip_amount"),
    "request_to_pickup_mins",
)

without_NaN.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Turn `empty payment` method into `NULL`
# MAGIC **Business question:** Operations needs a blank payment method stored as NULL.
# MAGIC

# COMMAND ----------

normalized = without_NaN.select(
    "trip_id",
    F.when(F.col("payment_method") == "", F.lit(None))
    .otherwise(F.col("payment_method"))
    .alias("payment_method"),
    "tip_amount",
    "request_to_pickup_mins",
)

normalized.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Count missing payment methods
# MAGIC **Business question:** Operations needs to know how many trips have no payment method

# COMMAND ----------

normalized.filter(F.col("payment_method").isNull()).show()

print(
    "Missing payment methods:",
    normalized.filter(F.col("payment_method").isNull()).count(),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `Fill NULL` with default values
# MAGIC **Business question:** Payment team need each trip in the report must include a payment method. If the payment method is NULL, use "unknown." 
# MAGIC
# MAGIC

# COMMAND ----------

filled_payment = normalized.na.fill("unknown", subset=["payment_method"])
filled_payment.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Business question:** Operations needs missing tip shown as 0.00 on the report.

# COMMAND ----------

filled_payment_tip_amount = normalized.na.fill({"payment_method": "unknown", "tip_amount": 0.0})
filled_payment_tip_amount.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fill default values by type
# MAGIC

# COMMAND ----------

normalized.show()

# COMMAND ----------

# Fill every numeric **`NULL`** or **`NaN`** with **`0`**. Spark applies this replacement only to numeric columns.
normalized.na.fill(0).show()

# COMMAND ----------

# Fill every string **`NULL`** with **`"unknown"`**. Spark applies this only to string columns.
normalized.na.fill("unknown").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use `na.replace` to normalize values

# COMMAND ----------

trim_blank_payments.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Replace a text sentinel with **`NULL`**.

# COMMAND ----------

without_na_ = trim_blank_payments.na.replace("N/A", None, subset=["payment_method"])

without_na_.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Replace a numeric sentinel with `NULL`

# COMMAND ----------

without_sentinels_ = without_na_.na.replace(-1, None, subset=["request_to_pickup_mins"])

without_sentinels_.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Replace `NaN` tip into `NULL`

# COMMAND ----------

without_NaN_ = without_sentinels_.na.replace(float("nan"), None, subset=["tip_amount"])

without_NaN_.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Replace an empty string with a label.

# COMMAND ----------

normalized_ = without_NaN_.na.replace("", None, subset=["payment_method"])

normalized_.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop rows with `na.drop`
# MAGIC
# MAGIC **`df.na.drop()`** / **`df.dropna(...)`** returns a new DataFrame after
# MAGIC removing rows that contain **`NULL`** or **`NaN`**
# MAGIC
# MAGIC **Business question:** Operations needs only trips with every column filled in.

# COMMAND ----------

normalized_.na.drop().show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Business question:** Operations needs only trips that have a payment method.

# COMMAND ----------

normalized_.na.drop(subset=["payment_method"]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Business question:** Operations needs to drop a trip if the tip or pickup wait is missing.

# COMMAND ----------

normalized_.na.drop(how="any", subset=["tip_amount", "request_to_pickup_mins"]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Business question:** Operations needs to drop a trip only when both the tip and the pickup wait are missing

# COMMAND ----------

normalized_.na.drop(how="all", subset=["tip_amount", "request_to_pickup_mins"]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set a fallback with `F.coalesce`
# MAGIC
# MAGIC `F.coalesce(...)` returns the first value that is not NULL.

# COMMAND ----------

payment_sources = spark.createDataFrame(
    [
        (1001, "Card", "Cash"),
        (1002, None, "Cash"),
        (1003, None, None),
    ],
    "trip_id bigint, payment_method string, payment_method_backup string",
)

payment_sources.select(
    "trip_id",
    F.coalesce(
        F.col("payment_method"),
        F.col("payment_method_backup"),
        F.lit("unknown"),
    ).alias("payment_method"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Normalize, decide, and validate
# MAGIC
# MAGIC **Business question:** Operations needs a pickup-wait report from the raw trips. Treat `blanks`, `N/A`, `-1`, and `NaN` as missing. Drop trips with no wait time. Show missing tip as 0.00. Show missing payment as "unknown."

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 — normalize missing values to NULL

# COMMAND ----------

normalized_trips = (
    df.select(
        "trip_id",
        F.trim(F.col("payment_method")).alias("payment_method"),
        "tip_amount",
        "request_to_pickup_mins",
    )
    .na.replace("N/A", None, subset=["payment_method"])
    .na.replace(-1, None, subset=["request_to_pickup_mins"])
    .na.replace(float("nan"), None, subset=["tip_amount"])
    .na.replace("", None, subset=["payment_method"])
)

normalized_trips.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 — drop or fill per column

# COMMAND ----------

pickup_wait_report = (
    normalized_trips.na.drop(subset=["request_to_pickup_mins"]).na.fill(
        {"tip_amount": 0.0, "payment_method": "unknown"}
    )
)

pickup_wait_report.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 — validate the cleaned results

# COMMAND ----------

pickup_wait_report.select(
    "trip_id",
    "payment_method",
    "tip_amount",
    "request_to_pickup_mins",
    F.col("payment_method").isNull().alias("payment_method_is_null"),
    F.col("tip_amount").isNull().alias("tip_amount_is_null"),
    F.col("request_to_pickup_mins").isNull().alias("request_to_pickup_mins_is_null"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ```
# MAGIC 1. Inspect the source values.
# MAGIC         ↓
# MAGIC 2. Define which values are considered "missing."
# MAGIC         ↓
# MAGIC 3. Normalize those values to NULL.
# MAGIC         ↓
# MAGIC 4. Decide for each column whether to keep, drop, or fill.
# MAGIC         ↓
# MAGIC 5. Validate the cleaned results.
# MAGIC ```
# MAGIC
# MAGIC **Next:** `03 - Safe Type Casting` — convert text to typed columns with
# MAGIC `cast` and `try_cast` under Spark 4 / ANSI mode, and detect rows rejected
# MAGIC by a cast.