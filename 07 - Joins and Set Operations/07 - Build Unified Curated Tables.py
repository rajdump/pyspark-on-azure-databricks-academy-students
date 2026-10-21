# Databricks notebook source
# MAGIC %md
# MAGIC # 07 - Build Unified Curated Tables
# MAGIC
# MAGIC In this notebook, we combine the datasets prepared in earlier lessons to create two Unity Catalog tables:
# MAGIC
# MAGIC - `trip_enriched` combines trip details with time, payment, and pickup/dropoff zone information.
# MAGIC - `trip_driver_assignment` combines driver details with trip information.
# MAGIC
# MAGIC Both tables are saved in `rideshare_dev.processed`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and source data
# MAGIC
# MAGIC Load the three curated sources and two landing sources.

# COMMAND ----------

from pyspark.sql import functions as F

landing_root = "/Volumes/rideshare_dev/landing/source_files"
curated_root = "/Volumes/rideshare_dev/processed/output_files/curated"

curated_trip = spark.read.format("parquet").load(f"{curated_root}/trip")  # noqa: F821
curated_payment = spark.read.format("parquet").load(f"{curated_root}/payment")  # noqa: F821
drivers_flat = spark.read.format("parquet").load(f"{curated_root}/drivers_flat")  # noqa: F821

trip_time = spark.read.format("parquet").load(  # noqa: F821
    f"{landing_root}/trip_time/trip_time.parquet"
)

zone_lookup_schema_ddl = """
location_id int,
borough_name string,
zone_name string,
service_zone string
"""

zone_lookup = (
    spark.read.format("json")  # noqa: F821
    .schema(zone_lookup_schema_ddl)
    .load(f"{landing_root}/zone_lookup/zone_lookup.json")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build `trip_enriched`
# MAGIC
# MAGIC Join trip, time, and payment data.

# COMMAND ----------

t = curated_trip.alias("t")
tt = trip_time.alias("tt")
pay = curated_payment.alias("pay")

trip_with_time = t.join(tt, "trip_id", "left")

trip_with_time_pay = trip_with_time.join(pay, "trip_id", "left")

# COMMAND ----------

# MAGIC %md
# MAGIC Join pickup and dropoff zone information.

# COMMAND ----------

# Broadcast the small zone lookup for pickup and dropoff joins.
pz = F.broadcast(zone_lookup.alias("pz"))
dz = F.broadcast(zone_lookup.alias("dz"))

trip_full = (
    trip_with_time_pay
    .join(pz, t.pickup_location_id == pz.location_id, "left")
    .join(dz, t.dropoff_location_id == dz.location_id, "left")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Select the 16 target columns.

# COMMAND ----------

trip_enriched = trip_full.select(
    F.col("t.trip_id"),
    F.col("t.service_type"),
    F.col("t.pickup_location_id"),
    F.col("t.dropoff_location_id"),
    F.col("t.trip_distance_miles"),
    F.col("t.ride_duration_mins"),
    F.col("tt.trip_date"),
    F.col("tt.hour_of_day"),
    F.col("pay.payment_method"),
    F.col("pay.base_fare_amount"),
    F.col("pay.tip_amount"),
    F.col("pay.driver_payout_amount"),
    F.col("pz.borough_name").alias("pickup_borough"),
    F.col("pz.zone_name").alias("pickup_zone"),
    F.col("dz.borough_name").alias("dropoff_borough"),
    F.col("dz.zone_name").alias("dropoff_zone"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC Write the table.

# COMMAND ----------

trip_enriched.write.mode("overwrite").saveAsTable(
    "rideshare_dev.processed.trip_enriched"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build `trip_driver_assignment`
# MAGIC
# MAGIC Join driver assignments with curated trips.

# COMMAND ----------

drv = drivers_flat.alias("drv")
tr = curated_trip.alias("tr")

driver_trip = drv.join(tr, "trip_id", "left")

# COMMAND ----------

# MAGIC %md
# MAGIC Select the 13 target columns.

# COMMAND ----------

trip_driver_assignment = driver_trip.select(
    F.col("drv.driver_id"),
    F.col("drv.driver_name"),
    F.col("drv.license_number"),
    F.col("drv.vehicle_make"),
    F.col("drv.vehicle_model"),
    F.col("drv.vehicle_year"),
    F.col("drv.vehicle_body_type"),
    F.col("drv.trip_id"),
    F.col("tr.service_type"),
    F.col("tr.trip_distance_miles"),
    F.col("tr.ride_duration_mins"),
    F.col("tr.pickup_location_id"),
    F.col("tr.dropoff_location_id"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC Write the table.

# COMMAND ----------

trip_driver_assignment.write.mode("overwrite").saveAsTable(
    "rideshare_dev.processed.trip_driver_assignment"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Output summary
# MAGIC
# MAGIC These row counts are **expected** for the course dataset. This notebook does not verify them.
# MAGIC
# MAGIC | Table | Expected rows | Business grain |
# MAGIC |---|---:|---|
# MAGIC | `trip_enriched` | 106 | One row per `trip_id` |
# MAGIC | `trip_driver_assignment` | 100 | One row per (`driver_id`, `trip_id`) |
# MAGIC
# MAGIC **Next:** Module 8 — Aggregations and Window Functions reads these tables directly with `spark.table()`.
