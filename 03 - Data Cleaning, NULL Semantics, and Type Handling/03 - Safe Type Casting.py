# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Safe Type Casting
# MAGIC
# MAGIC `cast` vs `try_cast` under Spark 4 / ANSI, and rejected-row detection.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Cast with `cast` and `try_cast`
# MAGIC - Detect rows rejected by a cast
# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup DataFrame for casting examples
# MAGIC
# MAGIC Notebook 02 normalized missing-value disguises to real **`NULL`** values.
# MAGIC You should already know intro **`cast`** from Module 2
# MAGIC **`03 - Selecting and Transforming Columns`**. Module 2 deferred the deeper
# MAGIC casting rules to here.
# MAGIC
# MAGIC The next problem is **wrong types**: numbers stored as text that
# MAGIC arithmetic operations cannot use until they are converted.
# MAGIC
# MAGIC Create one small DataFrame where fare and duration values are stored as
# MAGIC **`string`** columns:

# COMMAND ----------

from pyspark.sql import functions as F

rows = [
    (1001, "12.50", "18"),
    (1002, "8.00", "24"),
    (1003, "22.75", "35"),
]

schema_ddl = "trip_id bigint, base_fare_amount string, ride_duration_mins string"

df = spark.createDataFrame(rows, schema_ddl)  # pyright: ignore[reportUndefinedVariable]  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC Confirm the sample rows and schema before casting — the same inspection
# MAGIC habit as the earlier notebooks.

# COMMAND ----------

df.show()
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cast columns with `cast`
# MAGIC
# MAGIC **`F.col("x").cast("type")`** converts a column to a new Spark type when
# MAGIC every source value is valid. Pass a type name such as **`"decimal(10,2)"`**
# MAGIC or **`"int"`**.
# MAGIC
# MAGIC **Business question:** Finance needs **`base_fare_amount`** as
# MAGIC **`decimal(10,2)`** and **`ride_duration_mins`** as **`int`** before
# MAGIC running fare calculations.

# COMMAND ----------

typed = df.select(
    "trip_id",
    F.col("base_fare_amount").cast("decimal(10,2)").alias("base_fare_amount"),
    F.col("ride_duration_mins").cast("int").alias("ride_duration_mins"),
)

typed.printSchema()
typed.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Casting a decimal to an integer **truncates toward zero** — it does not
# MAGIC round. **`2.9`** becomes **`2`**.

# COMMAND ----------

typed.select(F.lit(2.9).cast("int").alias("truncated")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark 4 / ANSI mode and invalid casts
# MAGIC
# MAGIC On Databricks Runtime 17.3, **ANSI mode** is enabled by default. When an
# MAGIC invalid **`cast`** runs — for example, parsing a non-numeric string as a
# MAGIC number — Spark raises a **`[CAST_INVALID_INPUT]`** error instead of
# MAGIC silently returning **`NULL`** as it did before ANSI mode.
# MAGIC
# MAGIC This is a safer default: silent **`NULL`** returns used to hide data
# MAGIC quality problems that only surfaced later in the pipeline.

# COMMAND ----------

print("ANSI mode enabled:", spark.conf.get("spark.sql.ansi.enabled"))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC **Business question:** What happens when **`base_fare_amount`** contains
# MAGIC text that is not a number, such as **`"N/A"`**?
# MAGIC
# MAGIC Create a DataFrame that mixes valid fares with a sentinel and an
# MAGIC overflowing value, then attempt a plain **`cast`**.

# COMMAND ----------

bad = spark.createDataFrame(  # pyright: ignore[reportUndefinedVariable]  # noqa: F821
    [
        (1001, "12.50"),
        (1002, "N/A"),
        (1003, "8.00"),
        (1004, "999999999.99"),
    ],
    "trip_id bigint, base_fare_amount string",
)

try:
    bad.select(F.col("base_fare_amount").cast("decimal(10,2)")).show()
except Exception as e:
    print(f"{type(e).__name__}: {str(e).splitlines()[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC The error is typically **`[CAST_INVALID_INPUT]`** — **`"N/A"`** cannot be
# MAGIC converted to a **`decimal`**. Spark points to the fix: **`try_cast`**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Return `NULL` for invalid values with `try_cast`
# MAGIC
# MAGIC **`try_cast`** works like **`cast`**, with one difference: when Spark
# MAGIC supports the source-to-target conversion, **`try_cast`** returns
# MAGIC **`NULL`** for malformed or overflowing values instead of raising an
# MAGIC error. The job continues processing the remaining rows.
# MAGIC
# MAGIC **Business question:** Convert **`base_fare_amount`** to
# MAGIC **`decimal(10,2)`**, keeping bad values as **`NULL`** for later review
# MAGIC rather than stopping the job.

# COMMAND ----------

cleaned = bad.select(
    "trip_id",
    "base_fare_amount",
    F.col("base_fare_amount").try_cast("decimal(10,2)").alias("base_fare_amount_clean"),
)

cleaned.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **`"12.50"`** and **`"8.00"`** fit **`decimal(10,2)`** and convert cleanly.
# MAGIC
# MAGIC - **`"N/A"`** — invalid text → **`NULL`**
# MAGIC - **`"999999999.99"`** — valid number but too large for **`decimal(10,2)`**
# MAGIC   → **`NULL`**
# MAGIC
# MAGIC > **Good to know:** Apply **`try_cast`** to the affected column
# MAGIC > expressions. Do not turn off ANSI for the whole session — that hides
# MAGIC > bad casts and overflows across every operation in the job.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find rows that `try_cast` rejected
# MAGIC
# MAGIC A rejected row has a source value that is not **`NULL`** but a cast result
# MAGIC that is **`NULL`**. The source had a value; **`try_cast`** could not convert
# MAGIC it and wrote **`NULL`** instead.
# MAGIC
# MAGIC **`source.isNotNull() & casted.isNull()`**
# MAGIC
# MAGIC **Business question:** Which trips have a fare value that could not be
# MAGIC parsed to the target type and need data-quality review?

# COMMAND ----------

rejected = cleaned.filter(
    F.col("base_fare_amount").isNotNull() & F.col("base_fare_amount_clean").isNull()
)

rejected.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Trip **`1002`** has text that cannot be parsed. Trip **`1004`** overflows the
# MAGIC target precision. Both appear here because their source was not **`NULL`**
# MAGIC but the cast result is.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chain cast results into an operations-style output
# MAGIC
# MAGIC **Business question:** Operations needs two views from the same source —
# MAGIC fare values ready for calculations, and records that need data-quality
# MAGIC review because the conversion failed.

# COMMAND ----------

valid_fares = cleaned.filter(F.col("base_fare_amount_clean").isNotNull())

print("Ready for calculations")
valid_fares.show()

print("Requires data-quality review")
rejected.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unsupported type conversions
# MAGIC
# MAGIC **`try_cast`** handles invalid **values** only when Spark supports the
# MAGIC source-to-target type conversion. For example, Spark knows how to convert
# MAGIC a string to a number, so **`try_cast`** can catch the cases where a
# MAGIC particular string is not a valid number.
# MAGIC
# MAGIC Some type pairs cannot be converted at all. An **array** column, for
# MAGIC example, has no meaningful representation as an **integer**. Spark
# MAGIC rejects this at the schema level — before it reads a single row —
# MAGIC and **`try_cast`** cannot prevent that error.
# MAGIC
# MAGIC **Business question:** What happens when the source type is structurally
# MAGIC incompatible with the target type?

# COMMAND ----------

try:
    spark.range(1).select(F.array(F.lit(1)).try_cast("int")).show()  # noqa: F821
except Exception as e:
    print(f"{type(e).__name__}: {str(e).splitlines()[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC The error appears before any rows are processed. **`try_cast`** applies
# MAGIC only to valid conversion paths — when the source-to-target pair is
# MAGIC unsupported, fix the target type or transform the source column into a
# MAGIC compatible form first.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC Use a second small rideshare-style DataFrame named **`exercise_df`** and
# MAGIC complete:
# MAGIC
# MAGIC 1. Create **`exercise_df`** with **`trip_id`** and **`trip_distance_miles`**
# MAGIC    as **`string`** (aligned with the `trip` table). Include at least one
# MAGIC    valid decimal string and one invalid value such as **`"unknown"`**.
# MAGIC 2. Add **`trip_distance_miles_clean`** using **`try_cast`** to
# MAGIC    **`decimal(8,2)`**.
# MAGIC 3. Filter to rows where the source was not **`NULL`** but the cast result
# MAGIC    is **`NULL`** — the rejected-row pattern.
# MAGIC 4. Show the rejected rows.
# MAGIC
# MAGIC Keep the DataFrame tiny (four or five rows).

# COMMAND ----------

# Your code here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Recap this notebook's casting path:
# MAGIC
# MAGIC - **`cast`** — converts when values are valid; invalid input raises under
# MAGIC   ANSI mode
# MAGIC - **`try_cast`** — returns **`NULL`** for bad values when the conversion
# MAGIC   is supported; the job continues
# MAGIC - **Rejected rows** — **`source.isNotNull() & casted.isNull()`** isolates
# MAGIC   values that could not be converted
# MAGIC - **Unsupported type pairs** — schema-level error that **`try_cast`**
# MAGIC   cannot prevent; fix the target type or transform the source first
# MAGIC - **Do not disable ANSI globally** — use **`try_cast`** on the affected
# MAGIC   expression instead
# MAGIC
# MAGIC Next up: **`04 - Numeric Overflow and Date-Timestamp Parsing`** —
# MAGIC **`try_add`**, **`try_sum`**, **`try_to_date`**, and invalid source vs
# MAGIC invalid format patterns.
