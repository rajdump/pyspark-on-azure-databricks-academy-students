# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Complex Types, Structs, Arrays, and explode
# MAGIC
# MAGIC In this notebook, we read nested `drivers` XML, work with struct and array
# MAGIC fields, and write a flattened `drivers_flat` dataset as Parquet.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Access fields inside a struct
# MAGIC - Count array elements with `F.size()`
# MAGIC - Flatten an array with `F.explode()` and compare it with `F.explode_outer()`
# MAGIC - Write a flattened dataset as Parquet

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Attach **all-purpose compute**.

# COMMAND ----------

from pyspark.sql import functions as F

# Reading path
landing_root = "/Volumes/rideshare_dev/landing/source_files"
drivers_xml_path = f"{landing_root}/drivers/drivers.xml"

# Writing path
curated_drivers_path = "/Volumes/rideshare_dev/processed/output_files/curated/drivers_flat"

print(f"drivers_xml_path = {drivers_xml_path}")
print(f"curated_drivers_path = {curated_drivers_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read the `drivers` XML
# MAGIC
# MAGIC Read the landing XML file using `rowTag="driver"`. Inspect the inferred schema
# MAGIC and sample records.

# COMMAND ----------

drivers = (
    spark.read.format("xml")  # noqa: F821
    .option("rowTag", "driver")
    .load(drivers_xml_path)
)

drivers.printSchema()
drivers.show(3, vertical=True, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Access fields inside the `vehicle` struct
# MAGIC
# MAGIC Use dot notation to select the vehicle fields as separate columns.

# COMMAND ----------

drivers_with_vehicle = drivers.select(
    F.col("driver_id"),
    F.col("name"),
    F.col("vehicle.make").alias("vehicle_make"),
    F.col("vehicle.model").alias("vehicle_model"),
    F.col("vehicle.year").alias("vehicle_year"),
    F.col("vehicle.body_type").alias("vehicle_body_type"),
)

drivers_with_vehicle.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Work with the assigned-trip array
# MAGIC
# MAGIC Access `trips_assigned.trip_id` and use `F.size()` to count the assigned trips
# MAGIC for each driver.

# COMMAND ----------

drivers_with_assignment_counts = drivers.select(
    F.col("driver_id"),
    F.col("trips_assigned.trip_id").alias("assigned_trip_ids"),
    F.size(F.col("trips_assigned.trip_id")).alias("assigned_trip_count"),
)

drivers_with_assignment_counts.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Flatten assigned trips with `explode`
# MAGIC
# MAGIC Use `F.explode()` to create one row for each assigned trip.

# COMMAND ----------

drivers_exploded = drivers.select(
    F.col("driver_id"),
    F.col("name"),
    F.explode(F.col("trips_assigned.trip_id")).alias("trip_id"),
)

print("F.explode: one row per assigned trip")
drivers_exploded.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compare `explode` and `explode_outer`
# MAGIC
# MAGIC The next example temporarily sets the assigned-trip array for `D001` to `NULL`
# MAGIC to compare how both functions handle it.
# MAGIC
# MAGIC The original `drivers` DataFrame remains unchanged.

# COMMAND ----------

explode_comparison = drivers.filter(F.col("driver_id") == "D001").select(
    F.col("driver_id"),
    F.lit(None).cast("array<bigint>").alias("assigned_trip_ids"),
)

print("F.explode drops D001 because its array is NULL in this demonstration:")
explode_comparison.select(
    F.col("driver_id"),
    F.explode(F.col("assigned_trip_ids")).alias("trip_id"),
).show(truncate=False)

print("F.explode_outer keeps D001 with a NULL trip_id:")
explode_comparison.select(
    F.col("driver_id"),
    F.explode_outer(F.col("assigned_trip_ids")).alias("trip_id"),
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write flattened drivers to curated storage
# MAGIC
# MAGIC Create `drivers_flat` from the original `drivers` DataFrame, with one row per
# MAGIC driver and assigned trip.
# MAGIC
# MAGIC Drivers with empty or `NULL` assigned-trip arrays are excluded because the
# MAGIC transformation uses `F.explode()`.
# MAGIC
# MAGIC Write the result as Parquet to curated storage.

# COMMAND ----------

drivers_flat = drivers.select(
    F.col("driver_id"),
    F.col("name").alias("driver_name"),
    F.col("license_number"),
    F.col("vehicle.make").alias("vehicle_make"),
    F.col("vehicle.model").alias("vehicle_model"),
    F.col("vehicle.year").alias("vehicle_year"),
    F.col("vehicle.body_type").alias("vehicle_body_type"),
    F.explode(F.col("trips_assigned.trip_id")).alias("trip_id"),
)

drivers_flat.write.format("parquet").mode("overwrite").save(curated_drivers_path)

print(f"Wrote curated drivers data to {curated_drivers_path}")
drivers_flat.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - Accessed fields inside the `vehicle` struct.
# MAGIC - Used `F.size()` to count assigned trips.
# MAGIC - Used `F.explode()` to create separate rows for assigned trips.
# MAGIC - Compared `F.explode()` and `F.explode_outer()` using a `NULL` array.
# MAGIC - Wrote the flattened drivers dataset as Parquet.
# MAGIC
# MAGIC **Next:** `03 - Cleaning and Curated Outputs`
