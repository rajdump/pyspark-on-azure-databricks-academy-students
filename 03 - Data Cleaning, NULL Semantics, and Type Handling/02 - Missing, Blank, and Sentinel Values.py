# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Missing, Blank, and Sentinel Values
# MAGIC
# MAGIC Normalize missing shapes to real `NULL` before drop/fill.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Identify `NULL`, blanks, sentinels, and `NaN`
# MAGIC - Use `na.drop` / `na.fill` / `na.replace` and `F.coalesce` (not partition
# MAGIC   coalesce)
# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup DataFrame for missing-value examples
# MAGIC
# MAGIC Notebook 01 explained how **`NULL`** behaves in comparisons and filters.
# MAGIC Real sources rarely store clean **`NULL`** values — missing information
# MAGIC often appears as blank strings, spaces, sentinels such as **`"N/A"`** or
# MAGIC **`-1`**, or **`NaN`** in floating-point columns.
# MAGIC
# MAGIC Create one small DataFrame with deliberate quality issues:
# MAGIC
# MAGIC - **`payment_method`** — `NULL`, empty text, spaces, and **`"N/A"`**
# MAGIC - **`tip_amount`** — `NULL` and **`NaN`**
# MAGIC - **`request_to_pickup_mins`** — **`-1`** as a missing sentinel
# MAGIC - one row with both tip and request-to-pickup time missing
# MAGIC
# MAGIC > **Note:** **`tip_amount`** is intentionally **`double`** in this notebook
# MAGIC > so we can demonstrate **`NaN`** behavior with **`na.drop`** /
# MAGIC > **`na.fill`**.

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

# MAGIC %md
# MAGIC Confirm the sample rows before cleaning — the same habit as inspection in
# MAGIC Notebook 01.

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recognize `NULL`, blanks, sentinels, and `NaN`
# MAGIC
# MAGIC **Business question:** Operations needs a quick profile of which values are
# MAGIC true **`NULL`** values and which are missing-value disguises.
# MAGIC
# MAGIC When PySpark creates this DataFrame, each Python **`None`** becomes a Spark
# MAGIC **`NULL`**. Empty strings, spaces, **`"N/A"`**, and **`-1`** remain regular
# MAGIC values because Spark does not know they represent missing data. Trip
# MAGIC **`1006`** uses **`float("nan")`** for **`tip_amount`** — **`NaN`** is not
# MAGIC **`NULL`**, but **`na.drop`** and **`na.fill`** treat it as missing in
# MAGIC numeric columns.
# MAGIC
# MAGIC Add detection columns so each form of missing data is visible before
# MAGIC cleaning.

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
# MAGIC Trip **`1003`** has an empty string in **`payment_method`** — not
# MAGIC **`NULL`**, so **`payment_method_is_null`** is **`FALSE`**. Trip
# MAGIC **`1005`** has a real **`NULL`** there.
# MAGIC
# MAGIC Trip **`1006`** shows the **`NaN`** trap: **`tip_amount_is_null`** is
# MAGIC **`FALSE`** because **`NaN`** is not **`NULL`**, but **`tip_amount_is_nan`**
# MAGIC is **`TRUE`**. Blank strings, spaces, sentinels, and **`-1`** do not
# MAGIC appear in these checks at all — they look like regular values until you
# MAGIC normalize them.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop rows with `na.drop`
# MAGIC
# MAGIC **`df.na.drop()`** / **`df.dropna(...)`** returns a new DataFrame after
# MAGIC removing rows that contain **`NULL`** or **`NaN`** in the columns it checks.
# MAGIC It does not remove blank strings, spaces, sentinels, or **`-1`** until those
# MAGIC values are normalized.
# MAGIC
# MAGIC **Business question:** Operations needs to drop trips when any checked
# MAGIC column is missing.

# COMMAND ----------

df.na.drop().show()

# COMMAND ----------

# MAGIC %md
# MAGIC Check only **`payment_method`**. Spark removes rows where this column is
# MAGIC **`NULL`**. Values in the other columns do not affect this operation.

# COMMAND ----------

df.na.drop(subset=["payment_method"]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Check **`tip_amount`** and **`request_to_pickup_mins`**. With the default
# MAGIC **`how="any"`**, Spark removes a row when either checked column contains
# MAGIC **`NULL`** or **`NaN`**.

# COMMAND ----------

df.na.drop(how="any", subset=["tip_amount", "request_to_pickup_mins"]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Check the same two columns with **`how="all"`**. Spark removes a row only
# MAGIC when **both** checked columns contain **`NULL`** or **`NaN`**. Only trip
# MAGIC **`1007`** meets that condition.

# COMMAND ----------

df.na.drop(how="all", subset=["tip_amount", "request_to_pickup_mins"]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fill missing values with `na.fill`
# MAGIC
# MAGIC **`df.na.fill(...)`** / **`df.fillna(...)`** replaces **`NULL`** and
# MAGIC **`NaN`** with default values. It does not replace blank strings or
# MAGIC sentinels — normalize those first.

# COMMAND ----------

# MAGIC %md
# MAGIC Fill every numeric **`NULL`** or **`NaN`** with **`0`**. Spark applies this
# MAGIC replacement only to numeric columns.

# COMMAND ----------

df.na.fill(0).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Fill every string **`NULL`** with **`"unknown"`**. Spark applies this only
# MAGIC to string columns.

# COMMAND ----------

df.na.fill("unknown").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Fill **`NULL`** values in **`payment_method`** only.

# COMMAND ----------

df.na.fill("unknown", subset=["payment_method"]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Pass a dictionary when each column needs its own default.

# COMMAND ----------

df.na.fill({"tip_amount": 0.0, "payment_method": "unknown"}).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Replace known values with `na.replace`
# MAGIC
# MAGIC **`df.na.replace(...)`** swaps one known value for another. Pass Python
# MAGIC **`None`** as the replacement to write Spark **`NULL`** (for example,
# MAGIC replace **`"N/A"`** with **`None`**). It does not replace existing
# MAGIC **`NULL`** values — use **`na.fill`** for those.

# COMMAND ----------

# MAGIC %md
# MAGIC Replace a text sentinel with **`NULL`**.

# COMMAND ----------

df.na.replace("N/A", None, subset=["payment_method"]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Replace an empty string with a label.

# COMMAND ----------

df.na.replace("", "unknown", subset=["payment_method"]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Trip **`1003`** becomes **`"unknown"`**, but trip **`1004`** still shows
# MAGIC spaces in **`payment_method`**. **`na.replace`** matches exact values — it
# MAGIC does not trim whitespace. The normalize pipeline later uses **`trim`** +
# MAGIC **`nullif`** to handle spaces and empty strings together.

# COMMAND ----------

# MAGIC %md
# MAGIC Replace a numeric sentinel.

# COMMAND ----------

df.na.replace(-1, 0, subset=["request_to_pickup_mins"]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Replace several values at once.

# COMMAND ----------

df.na.replace(["Card", "Cash"], ["card", "cash"], "payment_method").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Normalize first, then drop or fill
# MAGIC
# MAGIC **Business question:** Operations needs every missing-value disguise turned
# MAGIC into a real **`NULL`** before choosing drop vs fill per column.
# MAGIC
# MAGIC This pipeline converts:
# MAGIC
# MAGIC - spaces and empty strings to **`NULL`** (`trim` + `nullif`)
# MAGIC - **`"N/A"`** to **`NULL`**
# MAGIC - **`-1`** to **`NULL`**
# MAGIC - **`NaN`** to **`NULL`**

# COMMAND ----------

normalized = (
    df.withColumn("payment_method", F.expr("nullif(trim(payment_method), '')"))
    .na.replace("N/A", None, subset=["payment_method"])
    .withColumn("request_to_pickup_mins", F.expr("nullif(request_to_pickup_mins, -1)"))
    .withColumn(
        "tip_amount",
        F.when(F.isnan(F.col("tip_amount")), F.lit(None).cast("double")).otherwise(
            F.col("tip_amount")
        ),
    )
)

normalized.show()

# COMMAND ----------

# MAGIC %md
# MAGIC All disguises are now **`NULL`**. From here, **`na.drop`** and **`na.fill`**
# MAGIC behave as expected.
# MAGIC
# MAGIC > **Good to know:** Most pipelines convert missing-value markers to
# MAGIC > **`NULL`** first, then decide per column: drop the row, keep the
# MAGIC > **`NULL`**, or replace it with **`na.fill`** or **`F.coalesce`**. Use a
# MAGIC > label such as **`"unknown"`** only when the column requires a value — it
# MAGIC > is not suitable for every column.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set a fallback with `F.coalesce`
# MAGIC
# MAGIC **`F.coalesce(...)`** returns the first non-**`NULL`** value from left to
# MAGIC right. Here, Spark keeps the cleaned **`payment_method`** when it exists.
# MAGIC If it is **`NULL`**, Spark uses **`"unknown"`**.
# MAGIC
# MAGIC > **Caution:** **`F.coalesce`** returns the first non-**`NULL`** **column
# MAGIC > value** (for example a payment-method fallback in **`withColumn`**).
# MAGIC > **`DataFrame.coalesce(n)`** reduces **partition count** — it does not
# MAGIC > replace missing values. Same name, different API; use **`F.coalesce`**
# MAGIC > for cleaning here.

# COMMAND ----------

normalized.withColumn(
    "payment_method_final",
    F.coalesce(F.col("payment_method"), F.lit("unknown")),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chain cleaning into an operations-style output
# MAGIC
# MAGIC **Business question:** Operations needs a reporting-ready payment view with
# MAGIC standardized missing-value handling for each column.
# MAGIC
# MAGIC Start with **`normalized`**, where blanks and sentinels are already
# MAGIC **`NULL`**. Remove trips without a request-to-pickup time, fill a missing tip
# MAGIC with **`0.0`**, and use **`"unknown"`** when the payment method is missing.

# COMMAND ----------

payment_ready = (
    normalized.na.drop(subset=["request_to_pickup_mins"])
    .na.fill({"tip_amount": 0.0})
    .withColumn(
        "payment_method_final",
        F.coalesce(F.col("payment_method"), F.lit("unknown")),
    )
    .select(
        "trip_id",
        "payment_method_final",
        "tip_amount",
        "request_to_pickup_mins",
    )
)

payment_ready.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Confirm the source DataFrame is unchanged — cleaning built a new output
# MAGIC without mutating **`df`**.

# COMMAND ----------

print("payment_ready row count:", payment_ready.count())
print("original df row count: ", df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC Use a second small rideshare-style DataFrame named **`exercise_df`** and
# MAGIC complete:
# MAGIC
# MAGIC 1. Create **`exercise_df`** with **`trip_id`**, **`service_type`**, and
# MAGIC    **`ride_duration_mins`** (`trip` table columns). Include at least one
# MAGIC    blank string, one sentinel, and one **`NULL`**.
# MAGIC 2. Normalize missing disguises to real **`NULL`** values.
# MAGIC 3. Drop rows where **`ride_duration_mins`** is missing after normalization.
# MAGIC 4. Fill a missing **`service_type`** with a label such as **`"unknown"`**
# MAGIC    using **`F.coalesce`** or **`na.fill`**.
# MAGIC 5. Show the final cleaned result.
# MAGIC
# MAGIC Keep the DataFrame tiny (four or five rows).

# COMMAND ----------

# Your code here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Recap this notebook's missing-value path:
# MAGIC
# MAGIC - **Disguises** — `NULL`, blanks, sentinels, and **`NaN`** are not
# MAGIC   interchangeable
# MAGIC - **`na.drop`** — removes rows with **`NULL`** / **`NaN`** in checked
# MAGIC   columns; **`how="any"`** vs **`how="all"`**; **`subset`**
# MAGIC - **`na.fill`** — replaces **`NULL`** / **`NaN`**; does not fix disguises
# MAGIC - **`na.replace`** — swaps known values; does not replace existing
# MAGIC   **`NULL`**
# MAGIC - **Normalize first** — then **`na.drop`** / **`na.fill`** behave as
# MAGIC   expected
# MAGIC - **`F.coalesce`** — first non-**`NULL`** column fallback; not partition
# MAGIC   **`DataFrame.coalesce(n)`**
# MAGIC
# MAGIC Next up: **`03 - Safe Type Casting`** — convert text to typed columns with
# MAGIC **`cast`** and **`try_cast`** under Spark 4 / ANSI mode, and detect rows
# MAGIC rejected by a cast.
