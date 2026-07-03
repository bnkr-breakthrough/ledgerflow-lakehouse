# LedgerFlow Lakehouse — Interview Guide

Use this guide to confidently explain the project in interviews for Senior Data Engineer roles (24+ LPA).

---

## Elevator Pitch (30 seconds)

> "I built LedgerFlow — a CDC-driven banking data lakehouse on Databricks. It captures every INSERT, UPDATE, and DELETE from a PostgreSQL banking database in real time using Debezium and Kafka, streams the events to Azure Data Lake Storage, and processes them through a Medallion architecture — Bronze Auto Loader, Silver DLT pipeline with CDC merge, and Gold business aggregations. The whole thing runs end-to-end in under 5 minutes via a Databricks Workflow, with Unity Catalog governance including row-level security and column masking."

---

## Q&A by Topic

### CDC & Debezium

**Q: Why CDC instead of batch ETL?**
> CDC captures every row-level change (insert/update/delete) the moment it happens via PostgreSQL's Write-Ahead Log. Batch ETL misses deletes and can't detect updates to unchanged columns. CDC gives us a complete audit trail and sub-minute latency — critical for a banking system where loan status changes need to propagate immediately.

**Q: What is the `ExtractNewRecordState` SMT?**
> Debezium wraps CDC events in a nested envelope with `before`, `after`, `source`, `op` fields. The `ExtractNewRecordState` Single Message Transform flattens this to a single-level JSON with the `after` state as the top-level fields, plus `__op`, `__ts_ms`, `__before` prefixed metadata. This means our bridge and Auto Loader consume flat JSON without any schema unpacking logic.

**Q: What does `__op` contain?**
> `r` = snapshot read (initial load), `c` = create/insert, `u` = update, `d` = delete. The double underscore is from the SMT — Debezium adds `__` prefix to metadata fields to avoid collision with column names.

---

### ADLS & Auto Loader

**Q: Why partition the ADLS files by year/month/day/hour?**
> Partition pruning. When Auto Loader or Spark reads from the raw zone, date-partitioned paths allow it to skip entire directories for historical data. It also makes it easy to audit what events arrived in a specific time window — useful for debugging and reprocessing.

**Q: Why `trigger(availableNow=True)` instead of continuous streaming?**
> `availableNow=True` processes all files that arrived since the last checkpoint, then stops. It's ideal for scheduled batch runs (daily Workflow) because it doesn't hold a cluster alive. For sub-minute latency you'd switch to `trigger(processingTime="1 minute")`, but that requires continuous compute cost.

**Q: Why `_metadata.file_path` instead of `input_file_name()`?**
> `input_file_name()` is blocked in Unity Catalog Serverless compute for security reasons. Auto Loader exposes file metadata through the `_metadata` struct column — `_metadata.file_path`, `_metadata.file_size`, `_metadata.file_modification_time`. Unity Catalog tracks this for automatic lineage.

---

### Delta Live Tables

**Q: What is `APPLY CHANGES INTO` and why use it?**
> It's DLT's built-in CDC merge operator. It applies upserts and deletes from a streaming source into a target Delta table, handling out-of-order events using a `sequence_by` column. Without it, you'd need to write complex merge logic manually with `MERGE INTO` and watermark management. DLT also handles the checkpoint state automatically.

**Q: How do you handle deletes in DLT?**
> `apply_as_deletes=expr("__op = 'd'")` — when DLT sees a record with `__op = 'd'`, it deletes the matching key from the target table. The deleted rows are tracked in a hidden `__apply_changes_version` column for audit.

**Q: What are DLT Expectations?**
> Declarative data quality rules that run on every record. `@dlt.expect_or_drop` drops records that fail — I use this for transactions (invalid amounts, unknown txn types). `expect_all_or_drop` on `create_streaming_table` applies rules at the table level for loans. Failed records can be routed to a quarantine table for investigation.

**Q: Why are Gold tables in the Silver schema initially?**
> DLT pipeline has a default catalog and schema. All tables created by the pipeline land in that schema — regardless of whether they're logically "gold". I explicitly promote them post-pipeline with `CREATE OR REPLACE TABLE gold.X AS SELECT * FROM silver.X`. In production you'd configure separate pipeline schemas per layer.

---

### Unity Catalog

**Q: Why use an Azure Access Connector instead of storage account keys?**
> Serverless compute blocks `spark.conf.set()` for dynamic credential injection — this is a security boundary. The enterprise pattern is Unity Catalog External Locations backed by Azure Managed Identity. The Access Connector gets Storage Blob Data Contributor on ADLS, and Databricks resolves credentials transparently via the External Location. No key rotation needed, no credential leakage in notebooks.

**Q: How does row-level security work in Unity Catalog?**
> You create a SQL function tagged as a ROW FILTER that takes the column value as input and returns BOOLEAN. When a user queries the table, Unity Catalog automatically injects the filter as a WHERE clause. Admins bypass the filter via `IS_ACCOUNT_GROUP_MEMBER('admins')`.

**Q: What is Unity Catalog lineage?**
> Unity Catalog automatically tracks which tables read/write from which sources at the column level. After two pipeline runs, the job shows "13 upstream tables, 6 downstream tables" — this is Unity Catalog recording that `transactions_raw` → `transactions` → `daily_collections`, etc. Visible in Catalog Explorer → table → Lineage tab. No manual documentation needed.

---

### Architecture & Design

**Q: Why Medallion architecture?**
> Bronze = raw, immutable landing zone — never lose data, always reprocess from source. Silver = clean, validated, CDC-merged business entities — what analysts query for accuracy. Gold = pre-aggregated business metrics — what dashboards and reports consume for performance. Each layer has a clear SLA and quality guarantee.

**Q: How would you scale this for production?**
> 1. Replace the Python bridge with Kafka Connect ADLS Sink connector — eliminates the bridge process entirely. 2. Switch `trigger(availableNow=True)` to `trigger(processingTime="5 minutes")` for near-real-time. 3. Add DLT enhanced autoscaling for the Silver pipeline. 4. Add alerting on DLT expectation failure rates. 5. Add schema registry (Confluent) for schema evolution governance.

**Q: How do you handle schema evolution?**
> Auto Loader with `schemaEvolutionMode = addNewColumns` — when Debezium adds a new column (e.g., PostgreSQL ALTER TABLE), Auto Loader detects it, updates the inferred schema, and adds the column to the Bronze Delta table. DLT Silver propagates it via `except_column_list` exclusion pattern.

**Q: What's the end-to-end latency?**
> ~4.5 minutes for a full batch run: 30s bridge flush + 60s Auto Loader + 90s DLT Silver + 60s Gold + 30s promotion. For event-driven runs (triggered on new files), could get Bronze→Gold in ~3 minutes.

---

## Key Numbers to Remember

| Metric | Value |
|--------|-------|
| Customers | 71 |
| Loans | 90 |
| Transactions | 1,500+ |
| NPA Rate | 11.12% |
| Total Loan Book | ₹25.67 Cr |
| ADLS files uploaded | 277+ |
| CDC events processed | 560+ |
| Pipeline runtime | ~4.5 min |
| E2E test result | 3/3 PASS |
| Workflow runs | 2/2 Succeeded |
