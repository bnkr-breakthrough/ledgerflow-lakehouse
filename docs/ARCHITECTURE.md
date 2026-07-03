# LedgerFlow Lakehouse — Architecture Deep Dive

## Overview

LedgerFlow is a CDC-driven data lakehouse that processes banking operational data in near-real-time using the Medallion architecture (Bronze → Silver → Gold) on Databricks.

---

## Component Details

### 1. Source Layer — PostgreSQL + Debezium

**PostgreSQL** runs in Docker and acts as the operational banking database. Write-Ahead Logging (WAL) is enabled with `wal_level = logical`, allowing Debezium to capture every row-level change.

**Debezium** connects to PostgreSQL via the `pgoutput` replication plugin. The `ExtractNewRecordState` Single Message Transform (SMT) flattens the CDC envelope so each Kafka message contains:
- Flat fields (no nested `before`/`after`)
- `__op`: operation type (`r`=read/snapshot, `c`=insert, `u`=update, `d`=delete)
- `__ts_ms`: Debezium event timestamp (used for ordering in DLT)
- `__before`: previous row state for updates

**Kafka topics** (one per table):
```
ledgerflow.public.customers
ledgerflow.public.loans
ledgerflow.public.transactions
```

### 2. Ingestion Bridge — kafka_to_adls_bridge.py

A Python consumer that reads from all 3 Kafka topics and writes batched NDJSON files to ADLS Gen2.

**Key design choices:**
- Batch size: 50 events per table OR 30-second flush interval (whichever first)
- Path pattern: `raw/{table}/year=YYYY/month=MM/day=DD/hour=HH/events_{ts}.json`
- Enriches each event with: `_bridge_ts`, `_kafka_offset`, `_topic`
- Graceful shutdown: flushes remaining buffer on Ctrl+C / SIGTERM
- Manual Kafka commit: only commits after successful ADLS upload

### 3. Bronze Layer — Auto Loader

Databricks Auto Loader (`cloudFiles` format) incrementally reads new JSON files from ADLS.

**Key config:**
```python
.option("cloudFiles.format", "json")
.option("cloudFiles.inferColumnTypes", "true")
.option("cloudFiles.schemaEvolutionMode", "addNewColumns")
.trigger(availableNow=True)  # batch mode — processes pending files then stops
```

**Unity Catalog access:** via External Location `ledgerflow-raw` backed by Azure Access Connector (Managed Identity) — no `spark.conf.set` needed on Serverless compute.

**Metadata columns added:**
- `_ingest_ts`: Auto Loader ingestion timestamp
- `_source_file`: `F.col("_metadata.file_path")` (Unity Catalog compatible — `input_file_name()` is blocked)

### 4. Silver Layer — Delta Live Tables

The DLT pipeline runs `APPLY CHANGES INTO` for CDC merge and adds data quality expectations.

**APPLY CHANGES INTO pattern:**
```python
dlt.apply_changes(
    target="customers",
    source="bronze_customers",
    keys=["customer_id"],
    sequence_by="__ts_ms",                      # handles out-of-order events
    apply_as_deletes=expr("__op = 'd'"),        # propagate deletes
    except_column_list=[...]                    # strip CDC metadata columns
)
```

**DLT Expectations (loans):**
- `valid_loan_amount`: amount > 0
- `valid_interest_rate`: 1–30%
- `valid_tenure`: 12–360 months
- `valid_loan_type`: HOME / PERSONAL / BUSINESS / AUTO
- `valid_status`: pending / active / defaulted / closed

**Quarantine table:** transactions that fail expectations are routed to `transactions_quarantine` with a `_quarantine_reason` column.

**`disbursed_at` conversion:**
Debezium serializes PostgreSQL `TIMESTAMP` as BIGINT (epoch milliseconds). Converted with:
```python
F.to_date(F.from_unixtime(col("disbursed_at") / 1000))
```

### 5. Gold Layer — Business Aggregations

Gold tables are computed inside the same DLT pipeline using `dlt.read()` (not `spark.table()`).

Since the DLT pipeline default schema is `silver`, gold tables initially land in silver and are promoted post-pipeline:
```sql
CREATE OR REPLACE TABLE ledgerflow_catalog.gold.portfolio_summary
AS SELECT * FROM ledgerflow_catalog.silver.portfolio_summary;
```

### 6. Unity Catalog Governance

**External Location:** `ledgerflow-raw` → `abfss://raw@ledgerflowadls.dfs.core.windows.net/`
Backed by Storage Credential `ledgerflow-adls-credential` (Azure Managed Identity via Access Connector).

**Row-level security:**
```sql
CREATE FUNCTION silver.city_row_filter(city STRING) RETURNS BOOLEAN
RETURN IS_ACCOUNT_GROUP_MEMBER('admins')
    OR city IN ('Chennai', 'Bangalore', 'Hyderabad', ...);

ALTER TABLE silver.customers SET ROW FILTER silver.city_row_filter ON (city);
```

**Column masking:**
```sql
CREATE FUNCTION silver.mask_email(email STRING) RETURNS STRING
RETURN CASE WHEN IS_ACCOUNT_GROUP_MEMBER('admins') THEN email
            ELSE CONCAT(LEFT(email, 1), '***@masked.com') END;

ALTER TABLE silver.customers ALTER COLUMN email SET MASK silver.mask_email;
```

**Lineage:** Unity Catalog automatically tracks Bronze → Silver → Gold lineage. Visible in Catalog Explorer → table → Lineage tab.

### 7. Databricks Workflow

3-task DAG orchestrating the full pipeline:

```
bronze_ingestion (Notebook)
    ↓  depends on
Silver (DLT Pipeline task) — LedgerFlow-Silver-Gold
    ↓  depends on
Gold_Promotion (Notebook) — 03_unity_catalog_setup.py
```

Total runtime: ~4.5 minutes. Lineage shows 13 upstream + 6 downstream tables after a full run.

---

## Data Flow Timing

```
PostgreSQL INSERT/UPDATE
    → Debezium WAL capture:     ~100ms
    → Kafka topic:              ~200ms
    → Bridge flush (30s max):   0–30s
    → ADLS file write:          ~500ms
    → Auto Loader (next run):   on-demand (Workflow trigger)
    → Bronze Delta write:       ~60s
    → DLT Silver CDC merge:     ~90s
    → Gold aggregations:        ~60s
    → Gold promotion:           ~30s
                                ─────────
Total (on-demand run):          ~4.5 minutes
```

---

## Infrastructure

All local infrastructure runs via Docker Compose:

| Service | Image | Port |
|---------|-------|------|
| PostgreSQL | `postgres:14` | 5432 |
| Zookeeper | `confluentinc/cp-zookeeper` | 2181 |
| Kafka | `confluentinc/cp-kafka` | 9092 |
| Debezium | `debezium/connect:2.4` | 8083 |
| Kafdrop | `obsidiandynamics/kafdrop` | 9000 |

**Azure resources:**
- Storage Account: `ledgerflowadls` (ADLS Gen2, HNS enabled)
- Container: `raw`
- Access Connector: `ledgerflow-connector` (Managed Identity)
- Databricks Workspace: `ledgerflow-workspace` (Premium tier)
