# Databricks notebook source
# =============================================================================
# LedgerFlow Lakehouse — Phase 6: Auto Loader → Bronze
# =============================================================================
# Reads raw CDC JSON files from ADLS Gen2 using Databricks Auto Loader
# (cloudFiles) and writes to Delta Lake Bronze tables in Unity Catalog.
#
# Tables created:
#   ledgerflow_catalog.bronze.customers_raw
#   ledgerflow_catalog.bronze.loans_raw
#   ledgerflow_catalog.bronze.transactions_raw
#
# Auto Loader incrementally tracks new files — safe to re-run anytime.
# =============================================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 6 — Auto Loader → Bronze Delta Tables
# MAGIC
# MAGIC **Architecture:**
# MAGIC ```
# MAGIC ADLS Gen2 raw/        ->   Auto Loader (cloudFiles)   ->   Bronze Delta Tables
# MAGIC  customers/*.json          Schema inference                customers_raw
# MAGIC  loans/*.json              Incremental file tracking       loans_raw
# MAGIC  transactions/*.json       Unity Catalog lineage           transactions_raw
# MAGIC ```

# COMMAND ----------

# -- 0. Config -----------------------------------------------------------------

ADLS_ACCOUNT   = "ledgerflowadls"          # your storage account name
RAW_CONTAINER  = "raw"
CATALOG        = "ledgerflow_catalog"
BRONZE_SCHEMA  = "bronze"

# ABFS path base  (Azure Blob File System — native ADLS Gen2 protocol)
RAW_BASE = f"abfss://{RAW_CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net"

# Checkpoint locations (Auto Loader stores incremental state here)
# Stored inside raw/ container under the ledgerflow-raw external location
CHECKPOINT_BASE = f"abfss://raw@{ADLS_ACCOUNT}.dfs.core.windows.net/_checkpoints"

# Tables
TABLES = ["customers", "loans", "transactions"]

print(f"RAW_BASE       : {RAW_BASE}")
print(f"CHECKPOINT_BASE: {CHECKPOINT_BASE}")
print(f"Catalog        : {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

# -- 1. ADLS Gen2 Access — Unity Catalog External Location --------------------
#
# Access is handled automatically via the Unity Catalog External Location
# "ledgerflow-raw" backed by the "ledgerflow-adls-credential" managed identity.
# No spark.conf.set needed — Databricks resolves credentials transparently.

print("ADLS access via Unity Catalog External Location: ledgerflow-raw")

# COMMAND ----------

# -- 2. Ensure Bronze schema exists -------------------------------------------

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
print(f"Schema {CATALOG}.{BRONZE_SCHEMA} ready")

# COMMAND ----------

# -- 3. Auto Loader helper -----------------------------------------------------

def load_bronze_table(table_name: str):
    """
    Stream raw NDJSON from ADLS -> Delta Bronze using Auto Loader.
    Adds ingestion metadata columns for lineage tracking.
    trigger(availableNow=True) processes all pending files then stops —
    suitable for batch/scheduled runs; switch to trigger(processingTime=...)
    for continuous streaming.
    """
    source_path     = f"{RAW_BASE}/{table_name}"
    checkpoint_path = f"{CHECKPOINT_BASE}/{table_name}"
    target_table    = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}_raw"

    print(f"\n{'='*60}")
    print(f"  Loading : {target_table}")
    print(f"  Source  : {source_path}")
    print(f"  Ckpt    : {checkpoint_path}")
    print(f"{'='*60}")

    df = (
        spark.readStream
             .format("cloudFiles")                                   # Auto Loader
             .option("cloudFiles.format", "json")                    # NDJSON input
             .option("cloudFiles.schemaLocation",
                     checkpoint_path + "/schema")
             .option("cloudFiles.inferColumnTypes", "true")
             .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
             .load(source_path)
    )

    from pyspark.sql import functions as F

    df_enriched = df.select(
        "*",
        F.current_timestamp().alias("_ingest_ts"),
        F.col("_metadata.file_path").alias("_source_file"),
    )

    query = (
        df_enriched.writeStream
                   .format("delta")
                   .outputMode("append")
                   .option("checkpointLocation", checkpoint_path)
                   .option("mergeSchema", "true")
                   .trigger(availableNow=True)
                   .toTable(target_table)
    )

    query.awaitTermination()
    print(f"  Done: {target_table}\n")


# COMMAND ----------

# -- 4. Load all three Bronze tables ------------------------------------------

for table in TABLES:
    load_bronze_table(table)

print("\nAll Bronze tables loaded successfully!")

# COMMAND ----------

# -- 5. Verify row counts ------------------------------------------------------

print("\n-- Bronze Table Row Counts --")
for table in TABLES:
    full_name = f"{CATALOG}.{BRONZE_SCHEMA}.{table}_raw"
    count = spark.table(full_name).count()
    print(f"  {full_name:<50}  {count:>6} rows")

# COMMAND ----------

# -- 6. Preview customers_raw --------------------------------------------------

print("-- customers_raw (first 5 rows) --")
display(
    spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.customers_raw")
         .orderBy("created_at")
         .limit(5)
)

# COMMAND ----------

# -- 7. Preview loans_raw ------------------------------------------------------

print("-- loans_raw (first 5 rows) --")
display(
    spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.loans_raw")
         .orderBy("created_at")
         .limit(5)
)

# COMMAND ----------

# -- 8. Preview transactions_raw -----------------------------------------------

print("-- transactions_raw (first 5 rows) --")
display(
    spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.transactions_raw")
         .orderBy("txn_date")
         .limit(5)
)

# COMMAND ----------

# -- 9. CDC operation distribution ---------------------------------------------
# op meanings: r=snapshot read, c=insert, u=update, d=delete

print("-- CDC ops in loans_raw --")
display(
    spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.loans_raw")
         .groupBy("__op")
         .count()
         .orderBy("__op")
)

print("-- CDC ops in transactions_raw --")
display(
    spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.transactions_raw")
         .groupBy("__op")
         .count()
         .orderBy("__op")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 6 Complete
# MAGIC
# MAGIC Bronze tables are live in Unity Catalog and tracked by Auto Loader.
# MAGIC Next: Phase 7 — Delta Live Tables Silver pipeline (APPLY CHANGES INTO,
# MAGIC DLT Expectations, quarantine table for bad records).
