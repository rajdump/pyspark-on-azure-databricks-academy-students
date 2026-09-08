# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Reading Avro
# MAGIC
# MAGIC Read **`payment`** from landing (Avro copied in notebook **01**).
# MAGIC
# MAGIC `/Volumes/rideshare_dev/landing/source_files/payment/`.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC
# MAGIC - Read Avro with an explicit schema
# COMMAND ----------

# MAGIC %md
# MAGIC ## What is Avro?
# MAGIC
# MAGIC Avro is a **row-oriented binary format** commonly used to exchange
# MAGIC structured records between systems.
# MAGIC
# MAGIC ## Row-oriented storage
# MAGIC
# MAGIC In a row-oriented format, all values for one record are stored together.
# MAGIC
# MAGIC For example, a payment record may contain **`trip_id`**,
# MAGIC **`payment_method`**, **`base_fare_amount`**, and **`tip_amount`**.
# MAGIC This makes Avro suitable for Kafka events, application messages, and
# MAGIC ingestion pipelines that usually produce or consume complete records.
# MAGIC
# MAGIC The diagram below uses three small payments to show how Avro lays those
# MAGIC fields out on disk.
# COMMAND ----------

# MAGIC %md
# MAGIC Same three payments (logical table):
# MAGIC
# MAGIC | trip_id | tip_amount | payment_method |
# MAGIC |--------:|-----------:|:---------------|
# MAGIC | 1 | 2.50 | card |
# MAGIC | 2 | 1.00 | cash |
# MAGIC | 3 | 3.25 | card |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Avro — row-oriented (on disk)
# MAGIC
# MAGIC Storage follows each payment **across** (→), then the next payment.
# MAGIC One record stays together on disk.
# MAGIC
# MAGIC ```text
# MAGIC Logical rows (read →):
# MAGIC
# MAGIC   trip_id   tip_amount   payment_method
# MAGIC   -------   ----------   --------------
# MAGIC      1         2.50           card      ← payment 1
# MAGIC      2         1.00           cash      ← payment 2
# MAGIC      3         3.25           card      ← payment 3
# MAGIC
# MAGIC On disk (payment after payment):
# MAGIC
# MAGIC   [ 1 | 2.50 | card ]  [ 2 | 1.00 | cash ]  [ 3 | 3.25 | card ]
# MAGIC    <-- payment 1 -->    <-- payment 2 -->    <-- payment 3 -->
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## How is Parquet different?
# MAGIC
# MAGIC Parquet is **column-oriented**, meaning values from the same column are
# MAGIC stored together.
# MAGIC
# MAGIC If a dataset has ten columns but a query needs only **`tip_amount`**,
# MAGIC Parquet can read mainly the data for that column. Avro returns only the
# MAGIC selected column, but it must process the records containing the other
# MAGIC fields.
# MAGIC
# MAGIC Therefore, Parquet is better suited for analytical queries that read a
# MAGIC small number of columns from large datasets.
# MAGIC
# MAGIC The same three payments, stored the Parquet way:

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parquet — column-oriented (on disk)
# MAGIC
# MAGIC Storage follows each column **down** (↓), then the next column.
# MAGIC Values from the same field sit together on disk.
# MAGIC
# MAGIC ```text
# MAGIC Same table, regrouped by column (read ↓):
# MAGIC
# MAGIC   trip_id values:         1      2      3
# MAGIC   tip_amount values:      2.50   1.00   3.25
# MAGIC   payment_method values:  card   cash   card
# MAGIC
# MAGIC On disk (column after column):
# MAGIC
# MAGIC   [ 1 | 2 | 3 ]  [ 2.50 | 1.00 | 3.25 ]  [ card | cash | card ]
# MAGIC    <-- trip_id -->  <---- tip_amount ---->  <- payment_method ->
# MAGIC ```

# COMMAND ----------
# MAGIC %md
# MAGIC ## When to use each format
# MAGIC
# MAGIC Use **Avro** for ingestion and record-based data exchange.
# MAGIC
# MAGIC Use **Parquet** for analytical file storage and column-based queries.
# MAGIC
# MAGIC ## Dataset used in this notebook
# MAGIC
# MAGIC In this notebook, we read the **`payment`** Avro dataset copied to the
# MAGIC landing volume in Notebook 01.
# MAGIC
# MAGIC ## Schema handling
# MAGIC
# MAGIC Avro files store schema information in the file header. Parquet files
# MAGIC also store schema metadata.
# MAGIC
# MAGIC Spark can therefore read typed columns from both formats without using
# MAGIC **`inferSchema`**.
# MAGIC
# MAGIC In production pipelines, the discovered schema should still be validated
# MAGIC against the expected data contract.
# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Import PySpark helpers and set paths for the **`payment`** dataset.
# MAGIC
# MAGIC Course **`payment`** columns (from `docs/data/dataset-overview.md`):
# MAGIC **`trip_id`** (bigint), **`payment_method`** (string),
# MAGIC **`base_fare_amount`** (decimal(10,2)), **`surge_amount`** (decimal(10,2)),
# MAGIC **`tax_amount`** (decimal(10,2)), **`tip_amount`** (decimal(10,2)),
# MAGIC **`discount_amount`** (decimal(10,2)), **`driver_payout_amount`**
# MAGIC (decimal(10,2)).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
)

landing_root = "/Volumes/rideshare_dev/landing/source_files"
payment_avro_path = f"{landing_root}/payment/payment.avro"
practice_root = "/Volumes/rideshare_dev/processed/output_files/practice"
practice_output_path = f"{practice_root}/payment_avro_roundtrip/"

print(f"payment_avro_path = {payment_avro_path}")
print(f"practice_output_path = {practice_output_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Source path
# MAGIC
# MAGIC **`payment/payment.avro`** was copied into the landing volume in Notebook 01.
# MAGIC Format notebooks in this module read through **`/Volumes/...`** paths only.

# COMMAND ----------

display(dbutils.fs.ls(f"{landing_root}/payment"))

# COMMAND ----------

# MAGIC %md
# MAGIC You should see **`payment.avro`** and the full-size controlled-bad
# MAGIC **`bad_payment_data.csv`** variant in that folder. The path variable
# MAGIC **`payment_avro_path`** points specifically to **`payment.avro`** below.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read syntax
# MAGIC
# MAGIC Use the generic DataSource API — the same **`format(...).load(...)`**
# MAGIC pattern as CSV, JSON, Parquet, and XML in this module. Avro does not have
# MAGIC a compact **`.avro(path)`** reader shorthand like **`.parquet(path)`**.

# COMMAND ----------

payment_embedded = spark.read.format("avro").load(payment_avro_path)

print("format('avro').load(path):")
payment_embedded.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Prefer **`format("avro")`** for both reads and writes so pipelines stay
# MAGIC consistent across formats. The variable **`payment_embedded`** carries
# MAGIC forward until section 4, where we apply an explicit schema.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Read and inspect
# MAGIC
# MAGIC Without **`inferSchema`**, Avro already returns typed columns from file
# MAGIC metadata. Confirm schema, a sample row, and row count.

# COMMAND ----------

print("Schema from Avro metadata (no inferSchema):")
payment_embedded.printSchema()

print("\nSample row:")
payment_embedded.show(1, vertical=True)

row_count = payment_embedded.count()
print(f"\nRow count: {row_count} (expect 100 for the course payment file)")

# COMMAND ----------

# MAGIC %md
# MAGIC Expect **`trip_id`** as **`bigint`**, **`payment_method`** as **`string`**,
# MAGIC and the fare columns as **`decimal(10,2)`** (or a compatible numeric type
# MAGIC from the file). **`printSchema()`** inspects metadata on the driver.
# MAGIC **`count()`** is an action that executes the read plan.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Explicit schema
# MAGIC
# MAGIC Even though Avro carries types, production pipelines still declare the
# MAGIC contract up front. Module 2 introduced DDL strings and **`StructType`**;
# MAGIC file reads accept either via **`.schema(...)`**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4a. DDL schema string

# COMMAND ----------

payment_schema_ddl = """
trip_id bigint,
payment_method string,
base_fare_amount decimal(10,2),
surge_amount decimal(10,2),
tax_amount decimal(10,2),
tip_amount decimal(10,2),
discount_amount decimal(10,2),
driver_payout_amount decimal(10,2)
"""

payment = (
    spark.read.format("avro").schema(payment_schema_ddl).load(payment_avro_path)
)

print("Read with DDL schema:")
payment.printSchema()
payment.show(1, vertical=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4b. `StructType` schema

# COMMAND ----------

payment_schema = StructType(
    [
        StructField("trip_id", LongType(), False),
        StructField("payment_method", StringType(), False),
        StructField("base_fare_amount", DecimalType(10, 2), False),
        StructField("surge_amount", DecimalType(10, 2), False),
        StructField("tax_amount", DecimalType(10, 2), False),
        StructField("tip_amount", DecimalType(10, 2), False),
        StructField("discount_amount", DecimalType(10, 2), False),
        StructField("driver_payout_amount", DecimalType(10, 2), False),
    ]
)

payment_via_struct = (
    spark.read.format("avro").schema(payment_schema).load(payment_avro_path)
)

print("Same file read with StructType (schemas should match):")
payment_via_struct.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Use **`payment`** (DDL read) for the rest of this notebook after the
# MAGIC mismatch demos below.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4c. Schema mismatch (ingestion drift)
# MAGIC
# MAGIC Avro stores schema in the file header, but production pipelines
# MAGIC still pass an explicit **`.schema(...)`** contract. When that contract
# MAGIC disagrees with the file, Spark Avro does **not** invent safe values for
# MAGIC every case:
# MAGIC
# MAGIC | Drift | What Spark Avro typically does |
# MAGIC |-------|--------------------------------|
# MAGIC | Contract has an **extra** column | Column appears; values are **`null`** |
# MAGIC | Contract uses a **wrong type** | Read **fails** (including many "widenings") |
# MAGIC | Contract **omits** a file column | Column is **absent** from the DataFrame |
# MAGIC
# MAGIC Wrong-type failures include obvious mismatches (**`decimal` → `string`**)
# MAGIC and ones that look compatible (**`decimal` → `double`**, **`bigint` →
# MAGIC `double`**). Treat **`.schema(...)`** as a validation contract, not as a
# MAGIC quiet cast layer.
# MAGIC
# MAGIC After these demos, keep using **`payment`** from section 4a.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Landing dropped a column
# MAGIC
# MAGIC The contract still expects **`promo_code`**, but **`payment.avro`** does
# MAGIC not have it.

# COMMAND ----------

mismatch_dropped = (
    spark.read.format("avro")
    .schema(
        """
        trip_id bigint,
        payment_method string,
        base_fare_amount decimal(10,2),
        surge_amount decimal(10,2),
        tax_amount decimal(10,2),
        tip_amount decimal(10,2),
        discount_amount decimal(10,2),
        driver_payout_amount decimal(10,2),
        promo_code string
        """
    )
    .load(payment_avro_path)
)

print("Contract field missing from file → nulls:")
mismatch_dropped.select("trip_id", "promo_code").show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC **`promo_code`** is all **`null`**. The job can still succeed — easy to
# MAGIC miss until a downstream check fails.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Wrong type — read fails
# MAGIC
# MAGIC Declaring **`tip_amount`** as **`string`** when the file stores a decimal
# MAGIC is incompatible. Spark Avro stops the read rather than returning an
# MAGIC invented string. The same hard fail happens for **`decimal` → `double`**
# MAGIC and **`bigint` → `double`** on this format.

# COMMAND ----------

try:
    (
        spark.read.format("avro")
        .schema(
            """
            trip_id bigint,
            payment_method string,
            base_fare_amount decimal(10,2),
            surge_amount decimal(10,2),
            tax_amount decimal(10,2),
            tip_amount string,
            discount_amount decimal(10,2),
            driver_payout_amount decimal(10,2)
            """
        )
        .load(payment_avro_path)
        .select("tip_amount")
        .show(3)
    )
except Exception as exc:
    print(f"Wrong type stopped the Avro read: {type(exc).__name__}")
    print(str(exc)[:500])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Contract omits a file column
# MAGIC
# MAGIC The file has **`discount_amount`**, but this contract does not list it.
# MAGIC Undeclared columns do not appear in the DataFrame.

# COMMAND ----------

mismatch_omit = (
    spark.read.format("avro")
    .schema(
        """
        trip_id bigint,
        payment_method string,
        base_fare_amount decimal(10,2),
        surge_amount decimal(10,2),
        tax_amount decimal(10,2),
        tip_amount decimal(10,2),
        driver_payout_amount decimal(10,2)
        """
    )
    .load(payment_avro_path)
)

print("File column omitted from contract → dropped from DataFrame:")
mismatch_omit.printSchema()
mismatch_omit.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC **`discount_amount`** is gone from the result.
# MAGIC
# MAGIC **Takeaway:** for Avro ingestion, keep **`.schema(...)`** aligned with
# MAGIC the landed file. Extra contract fields become nulls; omitted file fields
# MAGIC disappear; wrong types fail the job. Do not expect quiet remaps such as
# MAGIC **`decimal` → `double`**.
# MAGIC
# MAGIC Continue with **`payment`** from section 4a.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Light reshape
# MAGIC
# MAGIC **`select`** a small fare extract for later joins to **`trip`** on
# MAGIC **`trip_id`**. Deeper transforms belong in Module 6.

# COMMAND ----------

payment_subset = payment.select(
    F.col("trip_id"),
    F.col("payment_method"),
    F.col("base_fare_amount"),
    F.col("tip_amount"),
    F.col("driver_payout_amount"),
)

payment_subset.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Avro round trip
# MAGIC
# MAGIC Write the subset to **`practice/payment_avro_roundtrip/`**, then read it
# MAGIC back. Spark writes **`part-*.avro`** files under that directory. Like
# MAGIC Parquet, Avro **preserves types** across the write — still re-apply an
# MAGIC explicit schema on read when you want a clear contract.
# MAGIC
# MAGIC Use **`format("avro").save(...)`** (recommended module pattern).

# COMMAND ----------

payment_subset.write.format("avro").mode("overwrite").save(practice_output_path)

print(f"Wrote Avro folder to {practice_output_path}")
display(dbutils.fs.ls(practice_output_path))

# COMMAND ----------

roundtrip_embedded = spark.read.format("avro").load(practice_output_path)

print("Re-read without explicit schema (types come from Avro metadata):")
roundtrip_embedded.printSchema()
roundtrip_embedded.show(1, vertical=True)

# COMMAND ----------

payment_subset_schema_ddl = """
trip_id bigint,
payment_method string,
base_fare_amount decimal(10,2),
tip_amount decimal(10,2),
driver_payout_amount decimal(10,2)
"""

roundtrip_typed = (
    spark.read.format("avro").schema(payment_subset_schema_ddl).load(practice_output_path)
)

print("Re-read with explicit schema (production pattern):")
roundtrip_typed.printSchema()
roundtrip_typed.show(1, vertical=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Avro kept decimal fare columns typed without **`inferSchema`**. We used
# MAGIC **`.mode("overwrite")`** so re-runs replace the prior folder. Save modes
# MAGIC in depth are in **07 - Write Patterns and Table Preview**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC Build a small practice extract without reusing **`payment_subset`**:
# MAGIC
# MAGIC 1. Read **`payment.avro`** again with the full **`payment_schema_ddl`**
# MAGIC    (or **`payment_schema`**) into a new DataFrame (do not reuse
# MAGIC    **`payment`** or **`payment_subset`**).
# MAGIC 2. **`select`** exactly these three columns: **`trip_id`**,
# MAGIC    **`surge_amount`**, **`tax_amount`**.
# MAGIC 3. Write the result to
# MAGIC    **`/Volumes/rideshare_dev/processed/output_files/practice/payment_exercise/`**
# MAGIC    with **`.mode("overwrite")`** using **`format("avro").save(...)`**.
# MAGIC 4. Re-read the written folder with an explicit schema for those three
# MAGIC    columns and print the schema. Confirm **`trip_id`** is **`bigint`** and
# MAGIC    the amount columns are **`decimal(10,2)`**.

# COMMAND ----------

# Your code here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - **When to use** — Avro for ingestion and record exchange; Parquet for
# MAGIC   analytical file storage and column-based queries
# MAGIC - **Avro syntax** — use **`format("avro").load(...)`** /
# MAGIC   **`format("avro").save(...)`** (no compact **`.avro(path)`** reader like
# MAGIC   Parquet)
# MAGIC - **Embedded schema** — types come from file metadata; no **`inferSchema`**
# MAGIC - **Explicit schema (DDL or `StructType`)** — still recommended for production
# MAGIC   contracts and validation
# MAGIC - **Schema mismatch** — with **`.schema(...)`** on Avro: extra→nulls,
# MAGIC   omit→missing column, wrong type (including many widenings)→**fails**
# MAGIC - **Light reshape** — **`select`** fare columns before a practice write
# MAGIC - **Avro round trip** — writes a directory of part files; types are
# MAGIC   preserved on re-read (unlike CSV)
# MAGIC
# MAGIC **Next:** **07 - Write Patterns and Table Preview** — save modes, a brief
# MAGIC partitioned write, Delta file write under **`practice/`**, and managed
# MAGIC **`saveAsTable`** into **`rideshare_dev.processed`**.
