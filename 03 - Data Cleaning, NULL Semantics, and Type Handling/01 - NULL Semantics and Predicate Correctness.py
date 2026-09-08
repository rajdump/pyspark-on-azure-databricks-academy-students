# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - NULL Semantics and Predicate Correctness
# MAGIC
# MAGIC Three-valued logic and NULL-safe predicates — before messy-value cleanup.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Explain three-valued logic and why filters keep only `TRUE` rows
# MAGIC - Build NULL-safe predicates with `isNull` / `isNotNull`, the `isin` + NULL
# MAGIC   trap, and `eqNullSafe` / `<=>`
# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup DataFrame for NULL semantics examples
# MAGIC
# MAGIC Module 2 introduced NULL checks in **`05 - Filtering Rows`**. This notebook
# MAGIC goes deeper: how **`NULL`** behaves in comparisons, why filters silently drop
# MAGIC **`NULL`** results, and how to write predicates that stay correct when values
# MAGIC are missing.
# MAGIC
# MAGIC Create one small DataFrame with deliberate missing values in
# MAGIC **`payment_method`**, **`tip_amount`**, and **`pickup_location_id`**:

# COMMAND ----------

from decimal import Decimal

from pyspark.sql import functions as F

rows = [
    (1001, "Card", Decimal("3.50"), 138),
    (1002, "Cash", None, 74),
    (1003, None, Decimal("2.00"), 231),
    (1004, "Card", Decimal("1.00"), None),
]

schema_ddl = """
    trip_id bigint,
    payment_method string,
    tip_amount decimal(10,2),
    pickup_location_id int
"""

df = spark.createDataFrame(  # pyright: ignore[reportUndefinedVariable]  # noqa: F821
    rows,
    schema_ddl,
)

# COMMAND ----------

# MAGIC %md
# MAGIC Confirm the sample rows before building conditions — the same inspection
# MAGIC habit as Module 2.

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Understand three-valued logic
# MAGIC
# MAGIC **Business question:** Operations needs trips that qualify for a card-tip
# MAGIC reward.
# MAGIC
# MAGIC Qualifying rule: payment method is **`"Card"`** and tip is at least
# MAGIC **`$2.00`**. The next result shows each comparison **before** combining them
# MAGIC with **`&`**.

# COMMAND ----------

is_card = F.col("payment_method") == "Card"
tip_at_least_2 = F.col("tip_amount") >= 2.00
qualifies_for_reward = is_card & tip_at_least_2

df.select(
    "trip_id",
    "payment_method",
    "tip_amount",
    is_card.alias("is_card"),
    tip_at_least_2.alias("tip_at_least_2"),
    qualifies_for_reward.alias("qualifies_for_reward"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC The result depends on what Spark knows about each trip:
# MAGIC
# MAGIC - Trip **`1001`**: `TRUE AND TRUE` produces **`TRUE`**.
# MAGIC - Trip **`1002`**: `FALSE AND NULL` produces **`FALSE`**. Its payment
# MAGIC   method already disqualifies it.
# MAGIC - Trip **`1003`**: `NULL AND TRUE` produces **`NULL`**. Its payment
# MAGIC   method is **`NULL`**, so Spark cannot produce a definite answer.
# MAGIC - Trip **`1004`**: `TRUE AND FALSE` produces **`FALSE`**.
# MAGIC
# MAGIC Spark follows SQL's **three-valued logic**. A condition can produce
# MAGIC **`TRUE`**, **`FALSE`**, or **`NULL`**. In a condition result, **`NULL`**
# MAGIC means Spark cannot determine whether the answer is true or false.
# MAGIC
# MAGIC The same rule applies to other boolean operators. **`TRUE OR NULL`**
# MAGIC becomes **`TRUE`**, while **`FALSE OR NULL`** remains **`NULL`**.
# MAGIC **`NOT NULL`** also remains **`NULL`**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## See what a filter keeps
# MAGIC
# MAGIC **Business question:** Operations needs only the trips that qualify for the
# MAGIC card-tip reward.
# MAGIC
# MAGIC A filter keeps only rows whose condition is **`TRUE`** — not **`FALSE`**
# MAGIC or **`NULL`**.

# COMMAND ----------

df.filter(qualifies_for_reward).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Only trip **`1001`** remains. Trips **`1002`** and **`1004`** produced
# MAGIC **`FALSE`**, while trip **`1003`** produced **`NULL`**. The filter removes
# MAGIC all three because it keeps only **`TRUE`**.
# MAGIC
# MAGIC When a **`NULL`** value needs its own rule, test for **`NULL`** explicitly
# MAGIC instead of relying on a normal comparison alone.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use `isNull` and `isNotNull` for definite answers
# MAGIC
# MAGIC Module 2 showed that comparing a column with Python **`None`** does not find
# MAGIC **`NULL`** values — use **`isNull()`** / **`isNotNull()`** when the
# MAGIC requirement mentions missing values. Use that pattern here after seeing why
# MAGIC comparisons can produce **`NULL`**.
# MAGIC
# MAGIC **Business question:** A payment audit needs trips with a recorded payment
# MAGIC method.

# COMMAND ----------

df.filter(F.col("payment_method").isNotNull()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Business question:** A payment audit needs trips with no recorded
# MAGIC payment method.

# COMMAND ----------

df.filter(F.col("payment_method").isNull()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handle `None` in an `isin` list
# MAGIC
# MAGIC **`isNull()`** and **`isNotNull()`** check whether a **column value** is
# MAGIC **`NULL`**. A **`NULL`** can also enter a condition through a Python list
# MAGIC passed to **`isin(...)`**.
# MAGIC
# MAGIC **Business question:** Compliance needs trips whose pickup zone is not **74**
# MAGIC or **231**.
# MAGIC
# MAGIC Use **`~F.col("pickup_location_id").isin(blocked)`** to keep trips whose
# MAGIC pickup zone is not in the blocklist.
# MAGIC
# MAGIC Check **`pickup_location_id`** values before you filter.

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **`isin(blocked)`** is **`TRUE`** when **`pickup_location_id`** is on the
# MAGIC blocklist. **`~isin(...)`** is then **`FALSE`**, so the filter drops zones
# MAGIC **74** and **231**. For every other known zone, **`isin(...)`** is
# MAGIC **`FALSE`**, **`~isin(...)`** is **`TRUE`**, and the filter keeps the row.

# COMMAND ----------

blocked = [74, 231]
df.filter(~F.col("pickup_location_id").isin(blocked)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Only trip **`1001`** remains. Trips **`1002`** and **`1003`** are on the
# MAGIC blocklist. Trip **`1004`** has a **`NULL`** pickup zone, so **`isin(...)`**
# MAGIC is **`NULL`**, **`~isin(...)`** is **`NULL`**, and the filter drops that row
# MAGIC too.

# COMMAND ----------

# MAGIC %md
# MAGIC **Business question:** Compliance needs trips whose pickup zone is not **74**
# MAGIC or **231**.
# MAGIC
# MAGIC The upstream blocklist includes Python **`None`**. Run the same filter.
# MAGIC Why does it return no rows? PySpark treats Python **`None`** as **`NULL`**
# MAGIC inside the **`isin(...)`** condition.

# COMMAND ----------

blocked_with_null = [74, 231, None]
df.filter(~F.col("pickup_location_id").isin(blocked_with_null)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Zone **`138`** is not on the blocklist, but the filter returns **no rows**.
# MAGIC
# MAGIC **`isin(...)`** compares the column to **each** list value and joins the
# MAGIC results with **`OR`**. For zone **`138`**: **`138 == 74`** is **`FALSE`**,
# MAGIC **`138 == 231`** is **`FALSE`**, and **`138 == NULL`** is **`NULL`** —
# MAGIC Python **`None`** in the list becomes **`NULL`**. So **`isin(...)`** is
# MAGIC **`FALSE OR FALSE OR NULL`**, which is **`NULL`**.
# MAGIC
# MAGIC **`~`** applies **`NOT`**. **`NOT NULL`** is still **`NULL`**, not
# MAGIC **`TRUE`**, so the filter drops the row. For blocked zones, **`~isin(...)`**
# MAGIC is **`FALSE`** as intended. For every other row, the condition is
# MAGIC **`NULL`**, not **`TRUE`**.

# COMMAND ----------

# MAGIC %md
# MAGIC **Business question:** Compliance needs trips whose pickup zone is not **74**
# MAGIC or **231**, plus trips with an unknown pickup zone.
# MAGIC
# MAGIC Strip **`None`** from the blocklist first. The filter below keeps:
# MAGIC
# MAGIC - trips whose **`pickup_location_id`** is **`NULL`**
# MAGIC - trips whose **`pickup_location_id`** is known and not **74** or **231**

# COMMAND ----------

safe_blocked = [zone_id for zone_id in blocked_with_null if zone_id is not None]

location_allowed = F.col("pickup_location_id").isNull() | (
    ~F.col("pickup_location_id").isin(safe_blocked)
)

df.filter(location_allowed).select("trip_id", "pickup_location_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC The **`isNull()`** branch keeps trips with an unknown pickup zone. For known
# MAGIC zones, **`~isin(safe_blocked)`** excludes **74** and **231**.
# MAGIC
# MAGIC To drop trips with an unknown pickup zone instead, remove the **`isNull()`**
# MAGIC branch.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compare values that may be `NULL`: `eqNullSafe`
# MAGIC
# MAGIC **`isNull()`** tests whether a value is missing. **`eqNullSafe(...)`**
# MAGIC (SQL **`<=>`**) compares two values and always returns **`TRUE`** or
# MAGIC **`FALSE`** — even when either side is **`NULL`**.
# MAGIC
# MAGIC **Business question:** A payment audit needs to compare payment methods even
# MAGIC when some values are missing.
# MAGIC
# MAGIC Normal **`==`** returns **`NULL`** when either side is **`NULL`**.
# MAGIC **`eqNullSafe(...)`** rules:
# MAGIC
# MAGIC - two **`NULL`** values → **`TRUE`**
# MAGIC - **`NULL`** vs a known value → **`FALSE`**
# MAGIC - two known values → same as **`==`**

# COMMAND ----------

df.select(
    "trip_id",
    "payment_method",
    (F.col("payment_method") == "Card").alias("plain_eq"),
    F.col("payment_method").eqNullSafe("Card").alias("null_safe_vs_card"),
    F.expr("payment_method <=> 'Card'").alias("sql_null_safe_eq"),
    F.col("payment_method").eqNullSafe(None).alias("null_safe_vs_null"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC For trip **`1003`**, **`payment_method`** is **`NULL`**:
# MAGIC
# MAGIC - normal equality with **`"Card"`** returns **`NULL`**
# MAGIC - **`eqNullSafe("Card")`** and SQL **`<=>`** both return **`FALSE`**
# MAGIC - **`eqNullSafe(None)`** returns **`TRUE`**
# MAGIC
# MAGIC Use **`isNull()`** to check whether a column value is **`NULL`**. Use
# MAGIC **`eqNullSafe(...)`** (or SQL **`<=>`**) when you must compare values that
# MAGIC may be **`NULL`** and still get **`TRUE`** or **`FALSE`**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chain reward and pickup-zone rules
# MAGIC
# MAGIC **Business question:** Operations needs a per-trip report showing whether
# MAGIC each trip passes the reward rule, the blocklist rule, and final eligibility.
# MAGIC
# MAGIC Reward rule: payment method is **`"Card"`** and tip is at least **`$2.00`**.
# MAGIC Blocklist rule: pickup zone is not **74** or **231**, or the pickup zone is
# MAGIC unknown (same as **`location_allowed`** above).

# COMMAND ----------

reward_decisions = df.select(
    "trip_id",
    "payment_method",
    "tip_amount",
    "pickup_location_id",
    qualifies_for_reward.alias("qualifies_for_reward"),
    location_allowed.alias("location_allowed"),
    (qualifies_for_reward & location_allowed).alias("eligible_for_reward"),
)
reward_decisions.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Only trip **`1001`** has **`eligible_for_reward`** equal to **`TRUE`**. Trip
# MAGIC **`1003`** shows the trap: **`qualifies_for_reward`** is **`NULL`**, so
# MAGIC **`qualifies_for_reward & location_allowed`** is not **`TRUE`** even though
# MAGIC **`location_allowed`** is **`TRUE`**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC Use a second small rideshare-style DataFrame named **`exercise_df`** and
# MAGIC complete:
# MAGIC
# MAGIC 1. Create **`exercise_df`** with **`trip_id`**, **`service_type`**, and
# MAGIC    **`dropoff_location_id`** (aligned with the `trip` table). Include at
# MAGIC    least one **`NULL`** **`service_type`** and one **`NULL`**
# MAGIC    **`dropoff_location_id`**.
# MAGIC 2. Define a reusable condition **`is_premium`** where **`service_type`**
# MAGIC    equals **`"Premium"`**. For missing service types, the intermediate
# MAGIC    column shows **`NULL`**.
# MAGIC 3. Build a blocklist **`restricted_zones`** that accidentally includes Python
# MAGIC    **`None`**, then derive **`safe_restricted`** without **`None`**.
# MAGIC 4. Define **`zone_ok`** — keep rows whose **`dropoff_location_id`** is
# MAGIC    **`NULL`** **or** not in **`safe_restricted`** (same pattern as
# MAGIC    **`location_allowed`** above).
# MAGIC 5. Filter to rows where **`is_premium & zone_ok`** is **`TRUE`** and show
# MAGIC    the result.
# MAGIC
# MAGIC Keep the DataFrame tiny (four or five rows).

# COMMAND ----------

# Your code here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Recap this notebook's NULL predicate path:
# MAGIC
# MAGIC - **Three-valued logic** — conditions can produce **`TRUE`**, **`FALSE`**,
# MAGIC   or **`NULL`**; show intermediate columns before filtering
# MAGIC - **Filters keep only `TRUE`** — **`FALSE`** and **`NULL`** rows drop out
# MAGIC - **`isNull()` / `isNotNull()`** — definite answers when a column value may
# MAGIC   be missing (Module 2 intro; applied here after 3VL)
# MAGIC - **`isin` + Python `None`** — strip **`None`** from lists; decide separately
# MAGIC   how to treat **`NULL`** column values
# MAGIC - **`eqNullSafe` / `<=>`** — comparisons that must always return
# MAGIC   **`TRUE`** or **`FALSE`**
# MAGIC - **Eligibility chain** — name intermediate predicates; combine them with
# MAGIC   **`&`** into a final pass/fail column
# MAGIC
# MAGIC Next up: **`02 - Missing, Blank, and Sentinel Values`** — how missing data
# MAGIC hides as blanks, sentinels, and **`NaN`**, and how to normalize before
# MAGIC **`na.drop`** / **`na.fill`**.
