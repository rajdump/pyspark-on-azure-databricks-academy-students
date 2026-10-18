# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 - Introduction to PySpark Joins
# MAGIC
# MAGIC A join combines rows from two DataFrames based on matching join key values.
# MAGIC The join type determines which matched and unmatched rows appear in the result.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Write a join with `df1.join(df2, on, how)`.
# MAGIC - Write the join condition as a string, a list, or a Boolean expression.
# MAGIC - Compare inner, left, right, and full joins when keys are unmatched.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup
# MAGIC
# MAGIC Attach **all-purpose compute**.

# COMMAND ----------

# MAGIC %md
# MAGIC Create two small DataFrames: one containing trips assigned to drivers and the other containing the service type for each trip.

# COMMAND ----------

driver_trips = spark.createDataFrame(  # noqa: F821
    [("D001", 8, "Ravi Kumar"), ("D002", 4, "Priya Sharma")],
    ["driver_id", "trip_id", "driver_name"],
)

driver_trip_details = spark.createDataFrame(  # noqa: F821
    [("D001", 8, "Premium"), ("D002", 5, "Premium")],
    ["driver_id", "trip_id", "service_type"],
)

driver_trips.show()
driver_trip_details.show()

# COMMAND ----------

# DBTITLE 1,2. Join syntax
# MAGIC %md
# MAGIC ## 2. Join syntax
# MAGIC
# MAGIC ### Single-column join

# COMMAND ----------

inner_join = driver_trips.join(
    driver_trip_details,
    on="trip_id",
    how="inner",
)
inner_join.show()

# COMMAND ----------

# DBTITLE 1,Single-column Boolean join
boolean_join = driver_trips.join(
    driver_trip_details,
    on=(driver_trips.trip_id == driver_trip_details.trip_id),
    how="inner",
)
boolean_join.show()

# COMMAND ----------

# DBTITLE 1,Multi-column join
# MAGIC %md
# MAGIC ### Multi-column join

# COMMAND ----------

inner_join = driver_trips.join(
    driver_trip_details,
    on=["driver_id", "trip_id"],
    how="inner",
)
inner_join.show()

# COMMAND ----------

# DBTITLE 1,Multi-column Boolean join
boolean_join = driver_trips.join(
    driver_trip_details,
    on=(driver_trips.driver_id == driver_trip_details.driver_id)
    & (driver_trips.trip_id == driver_trip_details.trip_id),
    how="inner",
)
boolean_join.show()

# COMMAND ----------

# DBTITLE 1,3. Join types
# MAGIC %md
# MAGIC ## 3. Join types
# MAGIC ### 3a. Inner join
# MAGIC
# MAGIC An inner join keeps only rows that match in both DataFrames. Here, only D001 with trip 8 exists in both. The result contains 1 row.

# COMMAND ----------

driver_trips.join(
    driver_trip_details, on=["driver_id", "trip_id"], how="inner"
).orderBy("driver_id", "trip_id").show()

# COMMAND ----------

# DBTITLE 1,3b. Left join
# MAGIC %md
# MAGIC ### 3b. Left join
# MAGIC
# MAGIC A left join keeps all rows from the left DataFrame and adds matching values from the right DataFrame. Here, D002 with trip 4 has no matching record in `driver_trip_details`, so `service_type` is `NULL`. The result contains 2 rows.

# COMMAND ----------

driver_trips.join(
    driver_trip_details, on=["driver_id", "trip_id"], how="left"
).orderBy("driver_id", "trip_id").show()

# COMMAND ----------

# DBTITLE 1,3c. Right join
# MAGIC %md
# MAGIC ### 3c. Right join
# MAGIC
# MAGIC A right join keeps all rows from the right DataFrame and adds matching values from the left DataFrame. Here, D002 with trip 5 has no matching record in `driver_trips`, so `driver_name` is `NULL`. The result contains 2 rows.

# COMMAND ----------

driver_trips.join(
    driver_trip_details, on=["driver_id", "trip_id"], how="right"
).orderBy("driver_id", "trip_id").show()

# COMMAND ----------

# DBTITLE 1,3d. Full join
# MAGIC %md
# MAGIC ### 3d. Full join
# MAGIC
# MAGIC A full join keeps all rows from both DataFrames and combines rows with matching join keys. When a row has no match, the columns from the other DataFrame contain `NULL`. The result contains 3 rows.

# COMMAND ----------

driver_trips.join(
    driver_trip_details, on=["driver_id", "trip_id"], how="full"
).orderBy("driver_id", "trip_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - `left_df.join(right_df, on, how)` joins two DataFrames. `on` specifies the join condition, and `how` specifies the join type.
# MAGIC - String and list forms use column names shared by both DataFrames. Spark keeps one copy of each specified join-key column in the result.
# MAGIC - Boolean expressions allow joins using columns with the same or different names. Both DataFrames' join-key columns remain in the result.
# MAGIC - Inner join keeps only matching rows. Left, right, and full joins also keep unmatched rows, with `NULL` in columns from the side without a match.
# MAGIC - A cross join pairs every row of one DataFrame with every row of the other, without a join key. It is covered in the next lesson.
# MAGIC
# MAGIC **Next:** `02 - Join Cardinality, NULL Keys, and Cross Joins`