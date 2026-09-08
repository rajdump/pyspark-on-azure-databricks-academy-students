# Databricks notebook source
# MAGIC %md
# MAGIC # 07 - Build Unified Curated Tables
# MAGIC
# MAGIC Write-only business flow: load, build both tables per mapping docs,
# MAGIC `saveAsTable` overwrite.
# MAGIC
# MAGIC Curated trip/payment/drivers_flat; landing `trip_time`,
# MAGIC `zone_lookup`.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Apply the module patterns to write **`trip_enriched`** and
# MAGIC   **`trip_driver_assignment`**
# COMMAND ----------

# MAGIC %md
# MAGIC #####LOAD:`curated_trip, curated_payment, drivers, trip_time & zone_lookup`

# COMMAND ----------

from pyspark.sql import functions as F

landing_root = "/Volumes/rideshare_dev/landing/source_files"
curated_root = "/Volumes/rideshare_dev/processed/output_files/curated"

# Load curated sources
curated_trip = spark.read.format("parquet").load(f"{curated_root}/trip")  # noqa: F821
curated_payment = spark.read.format("parquet").load(f"{curated_root}/payment")  # noqa: F821
drivers_flat = spark.read.format("parquet").load(f"{curated_root}/drivers_flat")  # noqa: F821

# Load landing sources
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
# MAGIC ### trip_enriched implementation

# COMMAND ----------

# DBTITLE 1,trip_enriched
# MAGIC %md
# MAGIC ##### `1a:Left join trip_time and curated_payment`

# COMMAND ----------

# Aliases keep column references unambiguous across multiple joins.
t = curated_trip.alias("t")
tt = trip_time.alias("tt")
pay = curated_payment.alias("pay")

# Step 1: trip + trip_time (6 trips will have NULL date/hour)
trip_with_time = t.join(
    tt,
    F.col("t.trip_id") == F.col("tt.trip_id"),
    "left",
)

# Step 2: + curated_payment (trip 106 will have NULL payment cols)
trip_with_time_pay = trip_with_time.join(
    pay,
    F.col("t.trip_id") == F.col("pay.trip_id"),
    "left",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### `1b:broadcast zone_lookup`

# COMMAND ----------

# zone_lookup is 22 rows — broadcast avoids shuffling the fact side.
# Same pattern as NB 03 Section 4: alias + F.broadcast + Boolean condition.
pz = F.broadcast(zone_lookup.alias("pz"))
dz = F.broadcast(zone_lookup.alias("dz"))

trip_full = (
    trip_with_time_pay
    .join(pz, F.col("t.pickup_location_id") == F.col("pz.location_id"), "left")
    .join(dz, F.col("t.dropoff_location_id") == F.col("dz.location_id"), "left")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####  `1c. Select and rename — 16 target columns`

# COMMAND ----------

# Select exactly the 16 target columns per the mapping contract.
# Alias-qualified references avoid ambiguous-column errors from the joins.
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
# MAGIC #####  `1d.Write trip_enriched to Unity Catalog`

# COMMAND ----------

trip_enriched.write.mode("overwrite").saveAsTable(
    "rideshare_dev.processed.trip_enriched"
)

# COMMAND ----------

# DBTITLE 1,trip_driver_assignment
# MAGIC %md
# MAGIC ## `trip_driver_assignment`

# COMMAND ----------

# MAGIC %md
# MAGIC #####  `2a: Build — drivers_flat LEFT JOIN curated_trip`

# COMMAND ----------

drv = drivers_flat.alias("drv")
tr = curated_trip.alias("tr")

driver_trip = drv.join(
    tr,
    F.col("drv.trip_id") == F.col("tr.trip_id"),
    "left",
)

# Select the 13 target columns per the mapping contract.
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
# MAGIC #####  `2b: Write trip_driver_assignment to Unity Catalog`

# COMMAND ----------

trip_driver_assignment.write.mode("overwrite").saveAsTable(
    "rideshare_dev.processed.trip_driver_assignment"
)

# COMMAND ----------

# DBTITLE 1,AQE — what happened behind the scenes
# MAGIC %md
# MAGIC ## Note — Adaptive Query Execution (AQE)
# MAGIC
# MAGIC You did not tune anything in this notebook, yet Spark silently optimized the
# MAGIC joins at runtime. That’s **Adaptive Query Execution (AQE)** — enabled by
# MAGIC default since Spark 3.2.

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Output
# MAGIC
# MAGIC | Table | Rows | Grain |
# MAGIC |---|---|---|
# MAGIC | `rideshare_dev.processed.trip_enriched` | 106 | one per `trip_id` |
# MAGIC | `rideshare_dev.processed.trip_driver_assignment` | 100 | one per (`driver_id`, `trip_id`) |
# MAGIC
# MAGIC **Next:** **Module 8 — Aggregations and Window Functions** reads these tables
# MAGIC directly with `spark.table()`.