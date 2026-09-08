# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Complex Types, Structs, Arrays, and explode
# MAGIC
# MAGIC Flatten nested **`drivers`** XML and write curated `drivers_flat`.
# MAGIC
# MAGIC Landing **`drivers`**.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Access struct fields, work with array columns, and flatten with **`explode`**
# MAGIC   / **`explode_outer`**
# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC The `drivers` dataset is XML in the landing Volume. Each driver record has:
# MAGIC
# MAGIC - `driver_id`, `name`, and `license_number`
# MAGIC - `vehicle`, a struct with `make`, `model`, `year`, and `body_type`
# MAGIC - `trips_assigned`, a struct whose `trip_id` field is an array of assigned trips
# MAGIC
# MAGIC The final output has one row per `driver_id` and assigned `trip_id`.

# COMMAND ----------

from pyspark.sql import functions as F

landing_root = "/Volumes/rideshare_dev/landing/source_files"
drivers_xml_path = f"{landing_root}/drivers/drivers.xml"
curated_drivers_path = "/Volumes/rideshare_dev/processed/output_files/curated/drivers_flat"

print(f"drivers_xml_path = {drivers_xml_path}")
print(f"curated_drivers_path = {curated_drivers_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read landing `drivers` XML with `rowTag`
# MAGIC
# MAGIC XML needs `rowTag` to identify one record. In this file, each `<driver>` element
# MAGIC is one row. This is the same source pattern used in Module 5
# MAGIC **`05 - Reading XML`**.

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
# MAGIC ## 2. Access fields in the `vehicle` struct
# MAGIC
# MAGIC A **struct** stores related named fields in one column. `vehicle` is a struct, so
# MAGIC `F.col("vehicle.make")` reads its `make` field without creating extra rows.
# MAGIC
# MAGIC This keeps one row per driver while selecting only the vehicle details needed.

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
# MAGIC ## 3. Work with the `trips_assigned` array
# MAGIC
# MAGIC `trips_assigned` is a struct wrapper. Its `trip_id` field is the actual array.
# MAGIC An array keeps many values in one row, which is useful before we need one row per
# MAGIC assigned trip.
# MAGIC
# MAGIC `F.size` counts the number of elements in an array without flattening it.

# COMMAND ----------

drivers_with_assignment_counts = drivers.select(
    F.col("driver_id"),
    F.col("trips_assigned.trip_id").alias("assigned_trip_ids"),
    F.size(F.col("trips_assigned.trip_id")).alias("assigned_trip_count"),
)

drivers_with_assignment_counts.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Flatten assigned trips with `explode` and `explode_outer`
# MAGIC
# MAGIC `F.explode` creates one output row for each array element. A driver assigned
# MAGIC three trips becomes three rows.
# MAGIC
# MAGIC `F.explode` removes a row when its array is `NULL` or empty.
# MAGIC `F.explode_outer` keeps that driver and returns `NULL` for the exploded value.
# MAGIC Use `explode_outer` when keeping unmatched parent rows matters.

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
# MAGIC The landing file may not contain an unassigned driver. This small demonstration
# MAGIC keeps only `D001` and makes its array `NULL` to show the difference between the
# MAGIC two APIs. The curated output below still uses the original landing data.

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
# MAGIC Build the curated output from the original landing data. It preserves driver and
# MAGIC vehicle fields, then uses `F.explode` so the output grain is one row per
# MAGIC `driver_id` and `trip_id`.
# MAGIC
# MAGIC Write Parquet with `.mode("overwrite")` so a rerun replaces this notebook's
# MAGIC previous output.

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

drivers_flat.write.mode("overwrite").parquet(curated_drivers_path)

print(f"Wrote curated drivers data to {curated_drivers_path}")
drivers_flat.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC Build `drivers_exercise` from `drivers`:
# MAGIC
# MAGIC 1. Keep `driver_id` and `license_number`.
# MAGIC 2. Select `vehicle.make` and `vehicle.body_type` as `vehicle_make` and
# MAGIC    `vehicle_body_type`.
# MAGIC 3. Use `F.size` to create `assigned_trip_count`.
# MAGIC 4. Use `F.explode_outer` on `trips_assigned.trip_id` to create `trip_id`.
# MAGIC 5. Display the result. Do not write it.
# MAGIC
# MAGIC This repeats the struct-field, array, and flattening patterns with different
# MAGIC selected fields and uses `explode_outer` instead of the curated write's `explode`.

# COMMAND ----------

# Your code here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - Read XML with `format("xml")` and `rowTag="driver"` to create one driver row
# MAGIC   per `<driver>` element.
# MAGIC - Access struct fields with paths such as `vehicle.make`.
# MAGIC - Access the assigned-trip array through `trips_assigned.trip_id`; use `F.size`
# MAGIC   when the array should remain nested.
# MAGIC - Use `F.explode` for one row per assigned trip and `F.explode_outer` when drivers
# MAGIC   without assigned trips must remain in the output.
# MAGIC - Wrote the flattened drivers dataset to `curated/drivers_flat/` as Parquet.
# MAGIC
# MAGIC **Next:** Module 6 **`03 - Cleaning and Curated Outputs`**.
