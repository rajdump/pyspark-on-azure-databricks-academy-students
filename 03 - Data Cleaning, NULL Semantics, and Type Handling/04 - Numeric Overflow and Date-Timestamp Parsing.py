# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Numeric Overflow and Date-Timestamp Parsing
# MAGIC
# MAGIC Overflow and unparseable dates/timestamps with Spark 4 / ANSI `try_*` helpers.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Handle numeric overflow (`try_sum` / `try_avg`)
# MAGIC - Parse dates/timestamps with formats and session timezone; use `try_to_date` /
# MAGIC   `try_to_timestamp`
# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup context for overflow and parsing examples
# MAGIC
# MAGIC Notebook 03 handled malformed **casts** with **`try_cast`**. Overflow can
# MAGIC still appear during **casts**, **arithmetic**, and **aggregations** under
# MAGIC ANSI mode. Date and timestamp columns often arrive as **text** before
# MAGIC analytics can use them.
# MAGIC
# MAGIC Each section below builds a tiny DataFrame for one failure mode. Column
# MAGIC names align with **`docs/data/dataset-overview.md`**.

# COMMAND ----------

from decimal import Decimal

from pyspark.sql import functions as F

print("ANSI mode enabled:", spark.conf.get("spark.sql.ansi.enabled"))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handle integer cast overflow
# MAGIC
# MAGIC The course stores **`trip_id`** as **`bigint`**. The largest value an
# MAGIC **`int`** can hold is **`2147483647`**. Casting a larger trip ID to
# MAGIC **`int`** raises **`[CAST_OVERFLOW]`** under ANSI mode.
# MAGIC
# MAGIC **Business question:** What happens when a downstream system expects
# MAGIC **`trip_id`** as **`int`** but the source value exceeds the **`int`** range?

# COMMAND ----------

big_trip_id = spark.createDataFrame(  # pyright: ignore[reportUndefinedVariable]  # noqa: F821
    [(2147483648,)],
    "trip_id bigint",
)

try:
    big_trip_id.select(F.col("trip_id").cast("int")).show()
except Exception as e:
    print(f"{type(e).__name__}: {str(e).splitlines()[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC **`try_cast`** returns **`NULL`** instead of raising — the same pattern as
# MAGIC Notebook 03 for invalid text, applied here to overflow.

# COMMAND ----------

big_trip_id.select(F.col("trip_id").try_cast("int").alias("trip_id_int")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Choose a decimal type that fits
# MAGIC
# MAGIC In **`decimal(p, s)`**, **`p`** is the total number of digits and **`s`**
# MAGIC is digits after the decimal point. **`decimal(4, 2)`** max is **`99.99`**.
# MAGIC
# MAGIC **Business question:** Finance needs **`base_fare_amount`** as a decimal —
# MAGIC what happens when the target precision is too narrow?

# COMMAND ----------

fares = spark.createDataFrame(  # pyright: ignore[reportUndefinedVariable]  # noqa: F821
    [(12345.67,)],
    "base_fare_amount double",
)

try:
    fares.select(F.col("base_fare_amount").cast("decimal(4,2)")).show()
except Exception as e:
    print(f"{type(e).__name__}: {str(e).splitlines()[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC The course **`payment`** schema uses **`decimal(10, 2)`** for
# MAGIC **`base_fare_amount`** — wide enough for normal fares. Choose a target
# MAGIC type that fits the business range, not the smallest type that works today.

# COMMAND ----------

fares.select(
    F.col("base_fare_amount").cast("decimal(10,2)").alias("base_fare_amount")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Return `NULL` for arithmetic overflow
# MAGIC
# MAGIC Overflow can occur during a calculation, not only in a cast. Set
# MAGIC **`ride_duration_mins`** to the largest **`int`** value — adding it to
# MAGIC itself exceeds the **`int`** range.
# MAGIC
# MAGIC **Business question:** Operations needs **`ride_duration_mins * 2`** for
# MAGIC capacity planning. What happens when doubling the largest possible duration
# MAGIC overflows?

# COMMAND ----------

max_duration = spark.createDataFrame(  # pyright: ignore[reportUndefinedVariable]  # noqa: F821
    [(2147483647,)],
    "ride_duration_mins int",
)

try:
    max_duration.select(
        (F.col("ride_duration_mins") + F.col("ride_duration_mins")).alias("doubled_duration_mins")
    ).show()
except Exception as e:
    print(f"{type(e).__name__}: {str(e).splitlines()[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC **`try_add`** returns **`NULL`** instead of raising. Related helpers include
# MAGIC **`try_subtract`**, **`try_multiply`**, and **`try_divide`** — **`try_divide`**
# MAGIC also returns **`NULL`** for division by zero.

# COMMAND ----------

max_duration.select(
    F.try_add(
        F.col("ride_duration_mins"),
        F.col("ride_duration_mins"),
    ).alias("doubled_duration_mins")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handle aggregation overflow
# MAGIC
# MAGIC **`sum`** and **`avg`** can overflow when the **result** exceeds what the
# MAGIC target type can store — even when every individual value fits fine.
# MAGIC
# MAGIC Start with normal fares in the course **`decimal(10, 2)`** type. Each
# MAGIC value is realistic; the aggregation succeeds.

# COMMAND ----------

daily_fares = spark.createDataFrame(  # pyright: ignore[reportUndefinedVariable]  # noqa: F821
    [
        (Decimal("42.50"),),
        (Decimal("38.75"),),
        (Decimal("25.00"),),
    ],
    "base_fare_amount decimal(10,2)",
)

daily_fares.show()

# COMMAND ----------

daily_fares.select(
    F.sum("base_fare_amount").alias("total_fare"),
    F.avg("base_fare_amount").alias("avg_fare"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Business question:** Finance needs a total fare, but a downstream report
# MAGIC expects **`decimal(4, 2)`** — at most **`99.99`**. Each trip fare fits that
# MAGIC type on its own. What happens when the **sum** is **`100.00`**?

# COMMAND ----------

narrow_fares = spark.createDataFrame(  # pyright: ignore[reportUndefinedVariable]  # noqa: F821
    [
        (Decimal("45.00"),),
        (Decimal("55.00"),),
    ],
    "base_fare_amount decimal(4,2)",
)

narrow_fares.show()

# COMMAND ----------

try:
    narrow_fares.select(F.sum("base_fare_amount").cast("decimal(4,2)")).show()
except Exception as e:
    print(f"{type(e).__name__}: {str(e).splitlines()[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC **`45.00`** and **`55.00`** each fit **`decimal(4, 2)`**. Their sum is
# MAGIC **`100.00`**, which needs three digits before the decimal — one more than
# MAGIC the type allows. Casting the total back to the narrow type raises
# MAGIC **`[CAST_OVERFLOW]`**.
# MAGIC
# MAGIC **`try_sum`** and **`try_avg`** are the safe aggregation equivalents of
# MAGIC **`try_add`**. Use them when the aggregation itself might overflow; use
# MAGIC **`try_cast`** when you must narrow an already-computed total to a smaller
# MAGIC type.

# COMMAND ----------

narrow_fares.select(
    F.expr("try_sum(base_fare_amount)").alias("safe_sum"),
    F.expr("try_avg(base_fare_amount)").alias("safe_avg"),
    F.expr("try_cast(sum(base_fare_amount) AS decimal(4,2))").alias("safe_narrow_total"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC > **Good to know:** Apply **`try_*`** helpers to the affected expressions.
# MAGIC > Do not turn off ANSI for the whole session — that hides overflow and
# MAGIC > parsing failures across every operation in the job.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert text to dates and timestamps
# MAGIC
# MAGIC **`trip_time`** stores **`trip_date`** and **`hour_of_day`**. In raw
# MAGIC feeds, both often arrive as text before analytics can use them. Build
# MAGIC timestamp text from both columns, then parse with format patterns:
# MAGIC
# MAGIC - **`yyyy`** — year; **`MM`** — month; **`dd`** — day
# MAGIC - **`HH`** — hour (24-hour); **`mm`** — minute; **`ss`** — second
# MAGIC
# MAGIC **Business question:** Operations needs each trip as a typed
# MAGIC **`timestamp`** for time-of-day reporting. How do you parse date text and
# MAGIC hour together?

# COMMAND ----------

events = spark.createDataFrame(  # pyright: ignore[reportUndefinedVariable]  # noqa: F821
    [(1001, "2026-07-16", 14)],
    "trip_id bigint, trip_date string, hour_of_day int",
)

timestamp_text = F.expr(
    "concat(trip_date, ' ', lpad(cast(hour_of_day AS string), 2, '0'), ':00:00')"
)

events.show()

# COMMAND ----------

parsed = events.select(
    "trip_id",
    F.to_date(F.col("trip_date"), "yyyy-MM-dd").alias("trip_date"),
    "hour_of_day",
    F.to_timestamp(timestamp_text, "yyyy-MM-dd HH:mm:ss").alias("trip_timestamp"),
)

parsed.printSchema()
parsed.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **`to_date`** converts date text to a **`date`** column.
# MAGIC **`to_timestamp`** parses the combined date-and-hour string into a
# MAGIC **`timestamp`**. Both require an explicit format pattern — Spark does not
# MAGIC guess the layout of your source text.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Session timezone
# MAGIC
# MAGIC Spark uses a **session timezone** when working with timestamps.
# MAGIC
# MAGIC If the source value does not include timezone information, such as
# MAGIC **`2026-07-16 14:00:00`**, Spark uses the current session timezone to
# MAGIC interpret that value.
# MAGIC
# MAGIC First, check which timezone the Spark session is using. If the project
# MAGIC requires a specific timezone, set it explicitly before processing
# MAGIC timestamps.

# COMMAND ----------

print(spark.conf.get("spark.sql.session.timeZone"))  # noqa: F821

spark.conf.set("spark.sql.session.timeZone", "Asia/Kolkata")  # noqa: F821
print(spark.conf.get("spark.sql.session.timeZone"))  # noqa: F821

spark.conf.set("spark.sql.session.timeZone", "UTC")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Return `NULL` for invalid timestamps
# MAGIC
# MAGIC Malformed **`trip_date`** or out-of-range **`hour_of_day`** produces text
# MAGIC that **`to_timestamp`** cannot parse — it raises under ANSI mode.
# MAGIC
# MAGIC **Business question:** Which trip timestamps fail to parse, and can the
# MAGIC job continue without stopping on the first bad row?

# COMMAND ----------

messy = spark.createDataFrame(  # pyright: ignore[reportUndefinedVariable]  # noqa: F821
    [
        (1001, "2026-07-16", 9),
        (1002, "not-a-date", 14),
        (1003, "2026-13-40", 99),
    ],
    "trip_id bigint, trip_date string, hour_of_day int",
)

messy.show()

# COMMAND ----------

try:
    messy.select(
        F.to_timestamp(timestamp_text, "yyyy-MM-dd HH:mm:ss").alias("trip_timestamp")
    ).show()
except Exception as e:
    print(f"{type(e).__name__}: {str(e).splitlines()[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC **`try_to_timestamp`** returns **`NULL`** for unparseable source text.
# MAGIC Keep source columns beside the result so rejected values stay visible.

# COMMAND ----------

parsed_messy = messy.select(
    "trip_id",
    "trip_date",
    "hour_of_day",
    F.try_to_timestamp(timestamp_text, F.lit("yyyy-MM-dd HH:mm:ss")).alias("trip_timestamp"),
)

parsed_messy.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Trip **`1001`** parses cleanly. Trip **`1002`** has text that is not a
# MAGIC date. Trip **`1003`** has an impossible month and hour — both produce
# MAGIC **`NULL`** in **`trip_timestamp`**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Return `NULL` for invalid dates
# MAGIC
# MAGIC For ISO-shaped text such as **`yyyy-MM-dd`**, **`try_cast(... AS DATE)`**
# MAGIC or **`try_to_date`** returns **`NULL`** instead of raising for invalid
# MAGIC source strings.
# MAGIC
# MAGIC **Business question:** Which **`trip_date`** values cannot be parsed to a
# MAGIC real calendar date?

# COMMAND ----------

dates = spark.createDataFrame(  # pyright: ignore[reportUndefinedVariable]  # noqa: F821
    [("2026-07-16",), ("not-a-date",), ("2026-02-30",)],
    "trip_date string",
)

dates.select(
    "trip_date",
    F.expr("try_cast(trip_date AS DATE)").alias("parsed_trip_date"),
    F.try_to_date(F.col("trip_date"), F.lit("yyyy-MM-dd")).alias("parsed_with_try_to_date"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **`"2026-07-16"`** is valid. **`"not-a-date"`** is not a date at all.
# MAGIC **`"2026-02-30"`** looks like a date but February 30 does not exist —
# MAGIC both helpers return **`NULL`**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distinguish invalid source from invalid format pattern
# MAGIC
# MAGIC An invalid **source value** is a data problem — tolerant parsing can
# MAGIC return **`NULL`**. An invalid **format pattern** is a code problem — fix
# MAGIC the pattern in your code; **`try_*`** cannot make an unsupported pattern
# MAGIC valid.
# MAGIC
# MAGIC **Business question:** A parse fails — is the source text wrong, or is
# MAGIC the format string in your code wrong?

# COMMAND ----------

try:
    dates.select(F.to_date(F.col("trip_date"), "YYYY-QQ-DD")).show()
except Exception as e:
    print(f"{type(e).__name__}: {str(e).splitlines()[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC Spark datetime patterns use lowercase **`yyyy`** for a calendar year.
# MAGIC The pattern **`YYYY-QQ-DD`** is invalid — Spark rejects it before parsing
# MAGIC any rows. That is a code fix, not a data-quality review.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chain safe parsing into an operations-style output
# MAGIC
# MAGIC **Business question:** Which trip timestamps parsed successfully, and
# MAGIC which rows need review because parsing returned **`NULL`** while source
# MAGIC text was present?

# COMMAND ----------

accepted_timestamps = parsed_messy.filter(F.col("trip_timestamp").isNotNull())

rejected_timestamps = parsed_messy.filter(
    F.col("trip_date").isNotNull() & F.col("trip_timestamp").isNull()
)

print("Parsed successfully")
accepted_timestamps.show(truncate=False)

print("Requires data-quality review")
rejected_timestamps.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Trip **`1001`** is ready for time-of-day reporting. Trips **`1002`** and
# MAGIC **`1003`** need review — same rejected-row pattern as Notebook 03:
# MAGIC **`source.isNotNull() & parsed.isNull()`**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine safe arithmetic and parsing in one review table
# MAGIC
# MAGIC **Business question:** How can operations review overflow and parsing
# MAGIC failures in one output without stopping the pipeline when either occurs?

# COMMAND ----------

operations_input = spark.createDataFrame(  # pyright: ignore[reportUndefinedVariable]  # noqa: F821
    [
        (1001, "2026-07-16", 9, 30),
        (1002, "not-a-date", 14, 2147483647),
        (1003, "2026-13-40", 99, 45),
    ],
    "trip_id bigint, trip_date string, hour_of_day int, ride_duration_mins int",
)

operations_review = operations_input.select(
    "trip_id",
    "trip_date",
    "hour_of_day",
    "ride_duration_mins",
    F.try_add(
        F.col("ride_duration_mins"),
        F.col("ride_duration_mins"),
    ).alias("doubled_duration_mins"),
    F.try_to_timestamp(timestamp_text, F.lit("yyyy-MM-dd HH:mm:ss")).alias("trip_timestamp"),
)

operations_review.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Trip **`1001`** has usable values in both safe columns. Trip **`1002`**
# MAGIC overflows when **`ride_duration_mins`** is doubled and fails timestamp
# MAGIC parsing. Trip **`1003`** fails parsing only. A **`NULL`** in either
# MAGIC safe column marks a row for review; non-**`NULL`** results remain usable
# MAGIC in the same batch.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC Use a second small rideshare-style DataFrame named **`exercise_df`** and
# MAGIC complete:
# MAGIC
# MAGIC 1. Create **`exercise_df`** with **`trip_id`** and **`trip_date`** as
# MAGIC    **`string`** (aligned with the `trip_time` table). Include at least one
# MAGIC    valid ISO-shaped date and one invalid date string.
# MAGIC 2. Add **`trip_date_parsed`** with **`try_to_date`** (or
# MAGIC    **`try_cast(... AS DATE)`**) and a matching format pattern.
# MAGIC 3. Filter to rows where source **`trip_date`** is not **`NULL`** but
# MAGIC    **`trip_date_parsed`** is **`NULL`** — the rejected-row pattern from
# MAGIC    Notebook 03.
# MAGIC 4. Show the rejected rows.
# MAGIC
# MAGIC Keep the DataFrame tiny (three or four rows).

# COMMAND ----------

# Your code here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Recap this notebook's overflow and parsing path:
# MAGIC
# MAGIC - **Cast overflow** — use **`try_cast`** when a too-large value should
# MAGIC   become **`NULL`**
# MAGIC - **Arithmetic overflow** — plain **`+`** raises; **`try_add`** and
# MAGIC   related **`try_*`** operators return **`NULL`**
# MAGIC - **Aggregation overflow** — **`try_sum`** / **`try_avg`** return
# MAGIC   **`NULL`** instead of stopping the aggregation
# MAGIC - **Date/timestamp parsing** — **`to_date`** / **`to_timestamp`** with
# MAGIC   explicit patterns; **`try_to_*`** for bad source text
# MAGIC - **Session timezone** — check **`spark.sql.session.timeZone`**; set it
# MAGIC   before parsing timestamp text that has no zone
# MAGIC - **Bad data vs bad pattern** — fix patterns in code; use **`try_*`** for
# MAGIC   bad source values
# MAGIC - **Review output** — keep source columns beside safe results; split
# MAGIC   accepted vs rejected rows
# MAGIC - **Do not disable ANSI globally** — use **`try_*`** on the affected
# MAGIC   expression instead
# MAGIC
# MAGIC Next up: **Module 4 — Transformations, Actions, and Lazy Evaluation** —
# MAGIC how Spark builds and executes the transformation chains you already write.
