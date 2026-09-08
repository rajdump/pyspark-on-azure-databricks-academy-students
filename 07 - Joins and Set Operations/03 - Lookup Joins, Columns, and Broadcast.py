# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Lookup Joins, Columns, and Broadcast
# MAGIC
# MAGIC Repeated zone lookup, column cleanup, and broadcast — reused in notebook
# MAGIC **07**.
# MAGIC
# MAGIC `zone_lookup` (22);
# MAGIC `/Volumes/rideshare_dev/processed/output_files/curated/trip/` (106).
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Run a repeated lookup join (`zone_lookup` for pickup and dropoff)
# MAGIC - Clean columns with `select` / rename and `F.broadcast` a small dimension
# MAGIC   (confirm in `.explain()`)
# COMMAND ----------

# MAGIC %md
# MAGIC ## The problem: location IDs aren't names — and a tiny lookup table shouldn't cost a big shuffle
# MAGIC
# MAGIC This module **01**–**02** and Module 6 **01**–**04** must already have run so
# MAGIC **`curated/trip`** exists.
# MAGIC
# MAGIC Every trip in `curated/trip` includes a `pickup_location_id` and a `dropoff_location_id`, represented by numeric codes. To make this information clearer, you can join the `zone_lookup` table on `location_id` twice: once for pickup and once for dropoff.
# MAGIC
# MAGIC The sizes of the tables matter; the `zone_lookup` has 22 rows while the `trip` table has 106 rows. However, in a production environment, larger datasets like a fact table may contain millions of rows compared to a smaller dimension table, which could lead to inefficient shuffling of the larger table.
# MAGIC
# MAGIC A **broadcast join** solves this issue by sending the smaller table to every executor while keeping the larger table in its original location. This notebook focuses on implementing a broadcast join after establishing a repeated lookup pattern.
# MAGIC
# MAGIC Key topics covered include:
# MAGIC
# MAGIC 1. **Repeated Lookup Join** - Joining the same dimension table twice with different aliases for pickup and dropoff.
# MAGIC    
# MAGIC 2. **Column Cleanup** - Handling duplicate or ambiguous names from the repeated lookup.
# MAGIC
# MAGIC 3. **Unmatched Dimension Rows** - Identifying zones in `zone_lookup` that have no trip references and discussing join behaviours.
# MAGIC
# MAGIC 4. **Broadcasting** - Hinting to Spark to avoid shuffle operations and verifying this in the physical plan.
# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %md
# MAGIC ## Setup — fact vs dimension
# MAGIC
# MAGIC | Table | Format | Grain | Key | Rows |
# MAGIC |---|---|---|---|---|
# MAGIC | `curated/trip` | Parquet | one completed trip | `trip_id` | 106 |
# MAGIC | `zone_lookup` | JSON Lines | one taxi zone | `location_id` | 22 |
# MAGIC
# MAGIC Three signals determine the roles of tables:
# MAGIC
# MAGIC 1. **Grain and Content**: This indicates what each row represents. The `curated/trip` table's grain is a completed trip, containing measures like `trip_distance_miles`. In contrast, `zone_lookup` represents taxi zones with attributes like `borough_name` and has no measures.
# MAGIC
# MAGIC 2. **Foreign Key Direction**: This describes which table references the other. `curated/trip` contains `pickup_location_id` and `dropoff_location_id`, which point to `zone_lookup.location_id`. In a structure, fact tables reference dimension tables, not the other way around.
# MAGIC
# MAGIC 3. **Growth Pattern**: This refers to how data changes over time. `curated/trip` updates with each new trip, while `zone_lookup` remains static as it holds reference data.
# MAGIC
# MAGIC Based on these signals, `curated/trip` is the **fact** table as it references zones by ID, and `zone_lookup` is the **dimension** table with one row per zone.
# MAGIC
# MAGIC Before joining, check the lookup key: if `rows == distinct` and `nulls == 0`, `location_id` is a reliable join key. Only the dimension key requires this check, as duplicates in the `trip` foreign keys are common.

# COMMAND ----------

# DBTITLE 1,Setup - load trip and zone_lookup
from pyspark.sql import functions as F

landing_root = "/Volumes/rideshare_dev/landing/source_files"
curated_root = "/Volumes/rideshare_dev/processed/output_files/curated"

zone_json_path = f"{landing_root}/zone_lookup/zone_lookup.json"
curated_trip_path = f"{curated_root}/trip"

zone_lookup_schema_ddl = """
location_id int,
borough_name string,
zone_name string,
service_zone string
"""

zone_lookup = (
    spark.read.format("json")  # noqa: F821
    .schema(zone_lookup_schema_ddl)
    .load(zone_json_path)
)

trip = spark.read.format("parquet").load(curated_trip_path)  # noqa: F821

print("trip rows:", trip.count())
print("zone_lookup rows:", zone_lookup.count())

# COMMAND ----------

# DBTITLE 1,Setup - profile zone_lookup key
zone_stats = zone_lookup.select(
    F.count("*").alias("rows"),
    F.countDistinct("location_id").alias("distinct"),
    F.sum(F.when(F.col("location_id").isNull(), 1).otherwise(0)).alias("nulls"),
).collect()[0]
print(
    f"zone_lookup profile: rows={zone_stats['rows']}, "
    f"distinct={zone_stats['distinct']}, nulls={zone_stats['nulls']}"
)
print("\u2192 unique, no NULLs \u2014 safe lookup key")

# COMMAND ----------

# DBTITLE 1,1. Repeated lookup join
# MAGIC %md
# MAGIC ## 1. Repeating a lookup join results in multiple shuffles
# MAGIC
# MAGIC `zone_lookup` joins to `trip` twice: once for `pickup_location_id` and once for `dropoff_location_id`. Each join triggers a shuffle, which is costly as it involves redistributing rows across partitions based on join keys. 
# MAGIC
# MAGIC This process incurs significant disk I/O and serialization costs, regardless of cluster size. Additionally, network transfer costs arise when executors span multiple nodes, but these are secondary to the shuffle cost itself.
# MAGIC
# MAGIC In the physical plan, Spark defaults to a `SortMergeJoin` for each join, which involves shuffling both sides. With two joins, this results in four `Exchange` operations. 
# MAGIC
# MAGIC To optimize, use a broadcast join instead — Section 4 covers this topic.
# MAGIC
# MAGIC First, disable automatic broadcasting, so the plan below shows a
# MAGIC `SortMergeJoin`. Then you can force broadcasting with `F.broadcast()` while
# MAGIC keeping the setting at `-1`.

# COMMAND ----------

# DBTITLE 1,1. Disable auto-broadcast for the shuffle demo
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)  # noqa: F821

# COMMAND ----------

# DBTITLE 1,1. Double lookup join without broadcast
t = trip.alias("t")
pz = zone_lookup.alias("pz")
dz = zone_lookup.alias("dz")

trip_with_zones = (
    t.join(
        pz,
        F.col("t.pickup_location_id") == F.col("pz.location_id"),
        "left",
    )
    .join(
        dz,
        F.col("t.dropoff_location_id") == F.col("dz.location_id"),
        "left",
    )
)

trip_with_zones.explain()

# COMMAND ----------

# DBTITLE 1,1. Show messy duplicate columns
trip_with_zones.show(1, truncate=False, vertical=True)

# COMMAND ----------

# DBTITLE 1,1b. Duplicate columns after the double join
# MAGIC %md
# MAGIC ## Duplicate columns after the double join
# MAGIC
# MAGIC Take a look at the row printed above: `location_id`, `borough_name`, `zone_name`, and `service_zone` each appear twice—once from the pickup join and once from the dropoff join. 
# MAGIC
# MAGIC As a result, joining `zone_lookup` twice causes every column to appear twice in the output, which means we need to differentiate the pickup value from the dropoff value explicitly.
# MAGIC
# MAGIC This is expected behaviour and not an error, but the data is not yet usable. The columns need to be selected and renamed to reflect their business meaning accurately. 
# MAGIC
# MAGIC For example, we should rename the columns to `pickup_borough` and `dropoff_borough` instead of having two identically named `borough_name` columns. Section 2 below will address this cleanup.

# COMMAND ----------

# DBTITLE 1,2. Explicit select and rename
# MAGIC %md
# MAGIC ## 2. Explicit select and rename — clean up before downstream use
# MAGIC
# MAGIC Any downstream `.select("borough_name")` throws an
# MAGIC ambiguous column error, because Spark cannot tell which one you mean.
# MAGIC
# MAGIC The fix is to use `.select()` with alias-qualified references, renaming
# MAGIC each attribute to reflect its role: `pickup_borough`, `pickup_zone`,
# MAGIC `dropoff_borough`, and `dropoff_zone`. This produces one clean, unambiguous
# MAGIC schema that is ready for aggregation or export.

# COMMAND ----------

# DBTITLE 1,2. Select and rename
trip_with_zones.select(
    F.col("t.trip_id"),
    F.col("t.service_type"),
    F.col("t.pickup_location_id"),
    F.col("pz.borough_name").alias("pickup_borough"),
    F.col("pz.zone_name").alias("pickup_zone"),
    F.col("t.dropoff_location_id"),
    F.col("dz.borough_name").alias("dropoff_borough"),
    F.col("dz.zone_name").alias("dropoff_zone"),
    F.col("t.trip_distance_miles"),
).show(1, truncate=False, vertical=True)

# COMMAND ----------

# DBTITLE 1,3. Unmatched dimension rows
# MAGIC %md
# MAGIC ## 3. Unmatched dimension rows — practice
# MAGIC
# MAGIC Work through a small concrete example before writing any code.
# MAGIC
# MAGIC **A few `trip` rows:**
# MAGIC
# MAGIC | trip_id | pickup_location_id | dropoff_location_id |
# MAGIC |---|---|---|
# MAGIC | 1 | 1 | 9 |
# MAGIC | 2 | 18 | 14 |
# MAGIC | 3 | 12 | 20 |
# MAGIC
# MAGIC **A few `zone_lookup` rows:**
# MAGIC
# MAGIC | location_id | borough_name | zone_name |
# MAGIC |---|---|---|
# MAGIC | 1 | Manhattan | Midtown East |
# MAGIC | 9 | Brooklyn | Downtown Brooklyn |
# MAGIC | 21 | New Jersey | Newark Airport |
# MAGIC | 22 | New Jersey | Hoboken Terminal |
# MAGIC
# MAGIC Notice `location_id` 21 and 22 never show up as anyone's
# MAGIC `pickup_location_id` or `dropoff_location_id` above. This holds across the
# MAGIC full dataset too: `zone_lookup` has 22 rows, but no trip among all 106
# MAGIC curated rows ever references `location_id` 21 (`Newark Airport`) or 22
# MAGIC (`Hoboken Terminal`) — see
# MAGIC [`docs/data/dataset-overview.md`](../docs/data/dataset-overview.md).
# MAGIC
# MAGIC **Practice, using Notebook 01's left/right/full join behavior:**
# MAGIC
# MAGIC 1. Write a `trip`-driven **left join** to `zone_lookup` on `location_id`.
# MAGIC    Predict first: will `location_id` 21 or 22 ever appear in the result?
# MAGIC    Then run it and check.
# MAGIC 2. Flip it to a **right or full outer join** from `zone_lookup`'s side.
# MAGIC    Predict first: what changes? Then run it and inspect what the trip
# MAGIC    columns look like for `location_id` 21 and 22.

# COMMAND ----------

# DBTITLE 1,3. Practice - left join
# TODO (practice): write a trip-driven LEFT JOIN from `trip` to
# `zone_lookup`, matching `dropoff_location_id` to `location_id`.
# Use .alias() on both sides (Notebook 01 Section 3.3).
#
# Then check: does location_id 21 or 22 ever appear in the result?
# Filter your joined DataFrame down to just those two location_id values
# and count the rows — does the count match your prediction?

# COMMAND ----------

# DBTITLE 1,3b. Flip the driving side
# MAGIC %md
# MAGIC ### Now flip the driving side
# MAGIC
# MAGIC Before writing the next join, predict: which side needs to drive for an
# MAGIC unreferenced zone to have any chance of showing up at all?

# COMMAND ----------

# DBTITLE 1,3. Practice - right or full join
# TODO (practice): write a RIGHT (or FULL) outer join from `zone_lookup`'s
# side to `trip`, matching `dropoff_location_id` to `location_id`.
#
# Then check: filter the result down to location_id 21 and 22 — what do
# the trip columns (e.g. trip_id) look like for those rows? Compare the
# total row count to the left join from the cell above.

# COMMAND ----------

# DBTITLE 1,4. Broadcast
# MAGIC %md
# MAGIC ## 4. Broadcast — avoid shuffling the fact table for a 22-row dimension
# MAGIC
# MAGIC **Without a hint (Section 1):** With `autoBroadcastJoinThreshold = -1`,
# MAGIC Spark does not auto-broadcast. The double join used a shuffle strategy
# MAGIC (`SortMergeJoin` / `Exchange`s).
# MAGIC
# MAGIC **With a hint (below):** `F.broadcast()` forces a broadcast join even while
# MAGIC the threshold is still `-1`. Auto-broadcast stays off so the difference you
# MAGIC see in `.explain()` comes from the hint, not from the 10 MB default.
# MAGIC
# MAGIC **Note on Serverless compute:** Photon renames plan operators with a
# MAGIC `Photon` prefix, so you may see `PhotonBroadcastHashJoin` instead of
# MAGIC `BroadcastHashJoin`. Same optimization — Photon's vectorized form.

# COMMAND ----------

# DBTITLE 1,4. Double lookup join with F.broadcast
# Same double join as Section 1, but both zone_lookup sides use F.broadcast().
# Threshold is still -1 — the hint alone forces the broadcast plan.
t = trip.alias("t")
pz = F.broadcast(zone_lookup.alias("pz"))
dz = F.broadcast(zone_lookup.alias("dz"))

trip_broadcast_join = t.join(
    pz,
    F.col("t.pickup_location_id") == F.col("pz.location_id"),
    "left",
).join(
    dz,
    F.col("t.dropoff_location_id") == F.col("dz.location_id"),
    "left",
)

trip_broadcast_join.explain()

# COMMAND ----------

# DBTITLE 1,4. Restore auto-broadcast default
# Lesson complete — restore the usual 10 MB auto-broadcast threshold.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # noqa: F821

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Summary — lookup joins, in one workflow
# MAGIC
# MAGIC 1. **Profile the Dimension Key** - Ensure that `location_id` is unique and has no NULL values before performing the join. This confirms that it is safe to use.
# MAGIC
# MAGIC 2. **Join Twice with Aliases** - When the same dimension serves two purposes, such as pickup and dropoff, use aliases to differentiate between them. The Boolean form is useful here since the column names will be different.
# MAGIC
# MAGIC 3. **Select and Rename Immediately** - This practice helps avoid ambiguous duplicate column names that might cause issues in downstream code.
# MAGIC
# MAGIC 4. **Choose Join Direction Deliberately** - A left join from the fact table will hide any unreferenced dimension rows, while a right or full join from the dimension will reveal them.
# MAGIC
# MAGIC 5. **Broadcast Small Dimension Tables Explicitly** - Use `F.broadcast()` to optimize performance with smaller dimension tables, and confirm this by checking for `BroadcastHashJoin` in `.explain()`.
# MAGIC
# MAGIC This pattern of repeated lookups and explicit selections is what Notebook 07's capstone uses to build `trip_enriched` through multiple joins.
# MAGIC
# MAGIC **Next:** **`04 - Semi Joins and Anti Joins`**