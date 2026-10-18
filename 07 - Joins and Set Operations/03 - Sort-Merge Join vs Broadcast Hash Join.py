# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03 - Sort-Merge Join vs Broadcast Hash Join
# MAGIC
# MAGIC Each trip contains numeric IDs for its pickup and dropoff locations. To get the corresponding zone names, we join the `trip` DataFrame with `zone_lookup`
# MAGIC
# MAGIC Module 6 notebooks 01–04 must have run so `curated/trip` exists.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Understand how broadcast joins work and when to use them.
# MAGIC - Identify Sort-Merge Join and Broadcast Hash Join operators in the physical plan.
# MAGIC - Verify that both join strategies produce the same results.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup
# MAGIC
# MAGIC Attach **all-purpose compute**.

# COMMAND ----------

# MAGIC %md
# MAGIC - `trip` DataFrame: Contains 106 rows, with each row representing one trip. It includes the pickup and drop-off location IDs.
# MAGIC - `zone_lookup` DataFrame: Contains 22 rows, with each row representing one location. It provides the borough and zone names for each location ID.

# COMMAND ----------

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

# COMMAND ----------

zone_lookup.printSchema()

# COMMAND ----------

trip.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Sort-Merge-Join
# MAGIC
# MAGIC Spark can automatically choose a broadcast join when one side of the join is small enough, based on the configured broadcast threshold.
# MAGIC
# MAGIC So we first disable automatic broadcasting.

# COMMAND ----------

broadcast_threshold_str = spark.conf.get("spark.sql.autoBroadcastJoinThreshold"); 
broadcast_threshold = int(broadcast_threshold_str.rstrip('b'))

print("Broadcast threshold:", broadcast_threshold / (1024 * 1024), "MiB")

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  
spark.conf.set("spark.databricks.adaptive.autoBroadcastJoinThreshold", "-1")

# COMMAND ----------

# MAGIC %md
# MAGIC We join `trip` with `zone_lookup` to get the pickup location details for each trip.
# MAGIC
# MAGIC - `trip.pickup_location_id` is matched with `zone_lookup.location_id`.
# MAGIC - The matching row in `zone_lookup` provides the pickup borough and zone names.
# MAGIC
# MAGIC We use a left join, so all 106 trips are retained, even if some pickup location IDs have no match in `zone_lookup`.

# COMMAND ----------

t = trip.alias("t")
pz = zone_lookup.alias("pz")

trip_with_zones = (
    t.join(pz, F.col("t.pickup_location_id") == F.col("pz.location_id"), "left")
)

# COMMAND ----------

zone_columns = [
    F.col("t.trip_id"),
    F.col("t.pickup_location_id"),
    F.col("pz.borough_name").alias("pickup_borough"),
    F.col("pz.zone_name").alias("pickup_zone")
]

trip_zones = trip_with_zones.select(*zone_columns)

trip_zones.orderBy("trip_id").show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Broadcast Join
# MAGIC
# MAGIC Broadcasting is an execution strategy, not a join type.  
# MAGIC
# MAGIC In a Broadcast Hash Join, Spark makes the smaller dataset available to the join tasks. Each task uses it to find matching rows without shuffling the larger dataset by its join key.
# MAGIC
# MAGIC `F.broadcast()` asks Spark to broadcast `zone_lookup`, even though automatic broadcasting is still off.

# COMMAND ----------

trip_with_zones_broadcast = (
    t.join(F.broadcast(pz), F.col("t.pickup_location_id") == F.col("pz.location_id"), "left")
)

# COMMAND ----------

trip_zones_broadcast = trip_with_zones_broadcast.select(*zone_columns)

trip_zones_broadcast.orderBy("trip_id").show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Reset the broadcast settings.

# COMMAND ----------

spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
spark.conf.unset("spark.databricks.adaptive.autoBroadcastJoinThreshold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaway
# MAGIC
# MAGIC - A **Sort-Merge Join** shuffles and sorts data by the join keys, while a **Broadcast Hash Join** broadcasts the smaller DataFrame to avoid shuffling the larger side for the join.
# MAGIC - Use `F.broadcast()` to hint that Spark should broadcast a small lookup DataFrame.
# MAGIC - Compare the physical plans. Both strategies produce the same join results.
# MAGIC
# MAGIC **Next:** `04 - Semi Joins and Anti Joins`