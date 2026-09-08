# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Managed vs External Delta Tables
# MAGIC
# MAGIC Self-contained managed vs external CREATE / DROP / UNDROP / re-register.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Contrast managed and external Unity Catalog tables on storage location, `DROP`
# MAGIC   / `UNDROP` (including the managed dropped-table recovery window), and external
# MAGIC   re-registration
# MAGIC - Choose managed vs external (Databricks-managed storage and optimizations vs
# MAGIC   path control)
# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Handmade extract. Drop both lab names. Delete the external folder.
# MAGIC Do not insert yet.

# COMMAND ----------

from decimal import Decimal

from pyspark.sql.types import (
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
)

managed_table = "rideshare_dev.processed.fare_managed_lab"
external_table = "rideshare_dev.processed.fare_external_lab"

# Course external location — not a Volume path.
external_location_url = (
    spark.sql("DESCRIBE EXTERNAL LOCATION el_rideshare_dev")
    .select("url")
    .first()["url"]
    .rstrip("/")
)
# Subfolder only. Never CREATE at the external-location root.
external_table_path = (
    f"{external_location_url}/external-tables/fare_external_lab"
)

extract_schema = StructType(
    [
        StructField("trip_id", LongType(), False),
        StructField("service_type", StringType(), False),
        StructField("payment_method", StringType(), False),
        StructField("base_fare_amount", DecimalType(10, 2), False),
        StructField("tip_amount", DecimalType(10, 2), False),
    ]
)

trips_extract = spark.createDataFrame(
    [
        (1001, "STANDARD", "card", Decimal("20.00"), Decimal("3.00")),
        (1002, "SHARED", "cash", Decimal("15.00"), Decimal("0.00")),
        (1003, "PREMIUM", "card", Decimal("40.00"), Decimal("6.00")),
        (1004, "STANDARD", "wallet", Decimal("25.00"), Decimal("2.50")),
    ],
    schema=extract_schema,
)
trips_extract.createOrReplaceTempView("trips_extract")

# Reset: drop UC names, then delete leftover external files.
spark.sql(f"DROP TABLE IF EXISTS {managed_table}")
spark.sql(f"DROP TABLE IF EXISTS {external_table}")
dbutils.fs.rm(external_table_path, True)

print(f"managed_table = {managed_table}")
print(f"external_table = {external_table}")
print(f"external_table_path = {external_table_path}")
print("rows in extract =", trips_extract.count())
display(trips_extract.orderBy("trip_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Empty managed and external tables
# MAGIC For a managed table, no storage location is specified, as Unity Catalog
# MAGIC manages and selects it for you. For an external table, you must explicitly
# MAGIC provide an external storage path, such as `abfss://...`, rather than using
# MAGIC a `/Volumes/...` path.
# MAGIC
# MAGIC Both types of tables start with zero rows, and data will be inserted in
# MAGIC the next section.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- No LOCATION: Unity Catalog chooses the path. DV off on first CREATE.
# MAGIC CREATE TABLE rideshare_dev.processed.fare_managed_lab (
# MAGIC   trip_id BIGINT,
# MAGIC   service_type STRING,
# MAGIC   payment_method STRING,
# MAGIC   base_fare_amount DECIMAL(10, 2),
# MAGIC   tip_amount DECIMAL(10, 2)
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')

# COMMAND ----------

# LOCATION makes this external. Same schema as managed. DV off.
spark.sql(
    f"""
    CREATE TABLE {external_table} (
      trip_id BIGINT,
      service_type STRING,
      payment_method STRING,
      base_fare_amount DECIMAL(10, 2),
      tip_amount DECIMAL(10, 2)
    )
    USING DELTA
    LOCATION '{external_table_path}'
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')
    """
)

print(f"managed rows = {spark.table(managed_table).count()} (expect 0)")
print(f"external rows = {spark.table(external_table).count()} (expect 0)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Insert the data
# MAGIC
# MAGIC Insert the same four rows into both tables.
# MAGIC
# MAGIC The goal is simply to have data available for the upcoming `DROP`,
# MAGIC `UNDROP`, and re-registration tests — **not to teach DML**.

# COMMAND ----------

# Same four rows in both, so later DROP / UNDROP / re-register can check data.
spark.sql(f"INSERT INTO {managed_table} SELECT * FROM trips_extract")
spark.sql(f"INSERT INTO {external_table} SELECT * FROM trips_extract")

managed_df = spark.table(managed_table)
external_df = spark.table(external_table)
print(f"managed rows = {managed_df.count()} (expect 4)")
print(f"external rows = {external_df.count()} (expect 4)")
display(managed_df.orderBy("trip_id"))
display(external_df.orderBy("trip_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where do the files live?
# MAGIC
# MAGIC Use `DESCRIBE DETAIL` and compare the `format` and `location` values.
# MAGIC
# MAGIC Both tables store their data at an `abfss://` path:
# MAGIC
# MAGIC * **Managed table:** Unity Catalog chooses and manages the storage path.
# MAGIC * **External table:** you explicitly choose the storage path.

# COMMAND ----------

# Compare format and location.
display(spark.sql(f"DESCRIBE DETAIL {managed_table}"))
display(spark.sql(f"DESCRIBE DETAIL {external_table}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalog metadata
# MAGIC
# MAGIC Look at `table_type` and `storage_path`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Catalog metadata: MANAGED vs EXTERNAL, and storage_path.
# MAGIC SELECT table_name, table_type, storage_path
# MAGIC FROM rideshare_dev.information_schema.tables
# MAGIC WHERE table_schema = 'processed'
# MAGIC   AND table_name IN ('fare_managed_lab', 'fare_external_lab')
# MAGIC ORDER BY table_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## Managed vs external storage access
# MAGIC
# MAGIC `table_type` identifies the table as either `MANAGED` or `EXTERNAL`.
# MAGIC
# MAGIC Although `DESCRIBE DETAIL` shows an `abfss://` location for both tables,
# MAGIC the way you access those locations is different.
# MAGIC
# MAGIC * **External table:** the storage path was explicitly provided through a
# MAGIC   Unity Catalog external location, so users with the required permissions
# MAGIC   can access that path directly.
# MAGIC * **Managed table:** Unity Catalog owns and manages the storage location.
# MAGIC   You should access the data through the **table name**, not by directly
# MAGIC   reading or listing its underlying storage path.
# MAGIC
# MAGIC Therefore, the external-path example succeeds, while direct path-based
# MAGIC access to the managed table fails. An error stating that the path
# MAGIC **overlaps managed storage** is Unity Catalog enforcing this boundary.
# MAGIC
# MAGIC This is intentional: **external storage is user-managed; managed table
# MAGIC storage is Unity Catalog-managed.**

# COMMAND ----------

# Should succeed — you control this path.
display(spark.sql(f"LIST '{external_table_path}'"))

# COMMAND ----------

# Knowing the managed URI does not make LIST a supported file interface.
managed_uri = (
    spark.sql(f"DESCRIBE DETAIL {managed_table}")
    .select("location")
    .first()["location"]
)
print(f"managed_uri = {managed_uri}")
spark.sql(f"LIST '{managed_uri}'")  # Expected: AnalysisException

# COMMAND ----------

# MAGIC %md
# MAGIC ## DROP TABLE
# MAGIC
# MAGIC When you use the `DROP TABLE` command:
# MAGIC
# MAGIC - For a managed table, it removes the active registration from Unity
# MAGIC   Catalog and marks both the metadata and data for deletion. You can
# MAGIC   recover this deleted data using the `UNDROP TABLE` command within the
# MAGIC   default 7-day recovery window. After this period, the data is
# MAGIC   permanently deleted from cloud storage.
# MAGIC
# MAGIC - For an external table, it also removes the active registration from
# MAGIC   Unity Catalog and marks the metadata for deletion. However, the
# MAGIC   underlying data files remain intact, allowing you to re-register them
# MAGIC   later using the appropriate command.
# MAGIC
# MAGIC > **Note:** As of June 2026, the default recovery period for managed
# MAGIC > tables can be configured at the catalog or schema level.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Removes the active UC name. Do not CREATE this name again before UNDROP.
# MAGIC DROP TABLE rideshare_dev.processed.fare_managed_lab;
# MAGIC SHOW TABLES DROPPED IN rideshare_dev.processed

# COMMAND ----------

# Folder while the external table still exists.
print("External folder before DROP:")
display(spark.sql(f"LIST '{external_table_path}'"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- UC name only. Files at LOCATION stay.
# MAGIC DROP TABLE rideshare_dev.processed.fare_external_lab

# COMMAND ----------

# Same folder should still be here — DROP did not delete the files.
print("External folder after DROP:")
display(spark.sql(f"LIST '{external_table_path}'"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Public Preview. Use the most recently dropped fare_external_lab row.
# MAGIC SHOW TABLES DROPPED IN rideshare_dev.processed

# COMMAND ----------

# MAGIC %md
# MAGIC ## UNDROP
# MAGIC
# MAGIC `UNDROP TABLE` can restore both managed and external tables within the
# MAGIC 7-day recovery window.
# MAGIC
# MAGIC - **Managed:** restores the UC table metadata and data.
# MAGIC - **External:** restores the UC table metadata over files that already
# MAGIC   remain at the external path.

# COMMAND ----------

# Managed: relation + files UC kept. External: relation over files that stayed.
spark.sql(f"UNDROP TABLE {managed_table}")
spark.sql(f"UNDROP TABLE {external_table}")

managed_df = spark.table(managed_table)
external_df = spark.table(external_table)
print(f"managed rows = {managed_df.count()} (expect 4)")
print(f"external rows = {external_df.count()} (expect 4)")
display(managed_df.orderBy("trip_id"))
display(external_df.orderBy("trip_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Re-register the external folder
# MAGIC
# MAGIC Drop the external table again, then create a new UC table over the
# MAGIC existing ADLS folder.
# MAGIC
# MAGIC This is **re-registration**, not `UNDROP`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop the name only. Next cell re-registers the surviving folder.
# MAGIC DROP TABLE rideshare_dev.processed.fare_external_lab

# COMMAND ----------

# New UC name over the existing folder. Not UNDROP. No column list.
spark.sql(
    f"""
    CREATE TABLE {external_table}
    USING DELTA
    LOCATION '{external_table_path}'
    """
)

external_df = spark.table(external_table)
print(f"external rows = {external_df.count()} (expect 4)")
display(external_df.orderBy("trip_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## When to use which
# MAGIC
# MAGIC ### Use an external table when
# MAGIC
# MAGIC Choose an **external table** when you need control of the storage path.
# MAGIC
# MAGIC Typical cases:
# MAGIC
# MAGIC * data already exists at a specific ADLS path
# MAGIC * another system needs direct access to the same files
# MAGIC * the data format is not supported as a managed table
# MAGIC * `DROP TABLE` must leave the underlying files untouched
# MAGIC
# MAGIC You provide the `LOCATION`. Unity Catalog governs the table, while the
# MAGIC files remain at the storage path you control.
# MAGIC
# MAGIC ### Use a managed table when
# MAGIC
# MAGIC Choose a **managed table** for most new tables created in Databricks.
# MAGIC
# MAGIC Unity Catalog chooses the storage location and Databricks manages the
# MAGIC table storage for you.
# MAGIC
# MAGIC | Architecture area                       | Recommended default |
# MAGIC | --------------------------------------- | ------------------- |
# MAGIC | Landing / raw source files              | External Volume     |
# MAGIC | Bronze, Silver, and Gold tables         | Managed table       |
# MAGIC | Existing or shared data at a fixed path | External table      |
# MAGIC
# MAGIC Bronze, Silver, and Gold are **data layers**, not table types.
# MAGIC **Bronze does not mean external.**
# MAGIC
# MAGIC > **External table trade-off:** You keep control of the storage path, but
# MAGIC > managed-only capabilities such as **Predictive Optimization** are not
# MAGIC > available.
# MAGIC
# MAGIC Module 5 `landing` and `processed` are course storage areas, not
# MAGIC medallion layers.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure the recovery period
# MAGIC
# MAGIC For **Unity Catalog managed tables**, the dropped-table recovery period
# MAGIC can be configured at the **catalog or schema level**. It cannot be
# MAGIC configured per table.
# MAGIC
# MAGIC * **0 hours** — disables `UNDROP`
# MAGIC * **7 to 30 days** — keeps dropped managed tables recoverable
# MAGIC * **7 days** — default
# MAGIC * A **schema-level setting overrides the catalog setting**
# MAGIC
# MAGIC ```sql
# MAGIC -- Set 30-day recovery for managed tables in the catalog
# MAGIC ALTER CATALOG my_catalog RETAIN DROPPED TO 30 DAYS;
# MAGIC
# MAGIC -- Override with 7 days for managed tables in this schema
# MAGIC ALTER SCHEMA my_catalog.my_schema RETAIN DROPPED TO 7 DAYS;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC * Both managed and external tables are governed by Unity Catalog.
# MAGIC * This lab uses Delta for both tables. External tables can use other
# MAGIC   file formats; that is not this lab.
# MAGIC * Use **managed tables** by default for most new Databricks tables.
# MAGIC * Use **external tables** when you need to control or preserve a
# MAGIC   specific storage path.
# MAGIC * `DROP TABLE` removes the active UC registration. External files
# MAGIC   remain at their storage path.
# MAGIC * Re-registering an external folder creates a **new UC registration**
# MAGIC   over the existing files.
# MAGIC
# MAGIC **Next:** `04 - Delta Time Travel and Restore`