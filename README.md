# LedgerFlow Lakehouse

> **CDC-driven banking data lakehouse on Databricks** — Senior Data Engineer portfolio project demonstrating real-time Change Data Capture, Delta Live Tables, Unity Catalog governance, and end-to-end pipeline orchestration. Built entirely on free resources (Docker + Azure $200 credit).

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│  SOURCE LAYER                                                            │
│  PostgreSQL (Docker) ──WAL──▶ Debezium ──▶ Apache Kafka (Docker)        │
└─────────────────────────────────────┬────────────────────────────────────┘
                                      │  CDC events (JSON): op=r/c/u/d
                                      ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  INGESTION BRIDGE                                                        │
│  kafka_to_adls_bridge.py ──▶ ADLS Gen2 raw/  (NDJSON, partitioned)     │
│  Batch: 50 events or 30s flush · 277+ files · 560+ events               │
└─────────────────────────────────────┬────────────────────────────────────┘
                                      │  abfss://raw@ledgerflowadls/
                                      ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER  (Databricks Auto Loader)                                  │
│  cloudFiles → Delta Lake · trigger(availableNow=True)                   │
│  customers_raw (101) │ loans_raw (112) │ transactions_raw (1540)        │
│  Unity Catalog: ledgerflow_catalog.bronze.*                              │
└─────────────────────────────────────┬────────────────────────────────────┘
                                      │  Delta Live Tables
                                      ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER  (DLT — APPLY CHANGES INTO)                               │
│  CDC merge SCD Type 1 · DLT Expectations · quarantine table             │
│  customers (71) │ loans (90) │ transactions (1500+) │ quarantine (0)    │
│  Unity Catalog: ledgerflow_catalog.silver.*                              │
│  Governance: row filters · column masks · RBAC grants · lineage         │
└─────────────────────────────────────┬────────────────────────────────────┘
                                      │  DLT aggregations
                                      ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER  (Business aggregations)                                     │
│  portfolio_summary │ customer_risk_profile                               │
│  daily_collections │ npa_watchlist                                       │
│  Unity Catalog: ledgerflow_catalog.gold.*                                │
└─────────────────────────────────────┬────────────────────────────────────┘
                                      │
                                      ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  CONSUMPTION LAYER                                                       │
│  Databricks SQL Dashboard — LedgerFlow Banking Analytics (6 panels)     │
│  Databricks Workflow — LedgerFlow-Daily-Pipeline (3-task DAG, ~4.5 min) │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Source DB | PostgreSQL 14 (Docker) |
| CDC | Debezium 2.x + `ExtractNewRecordState` SMT |
| Message Bus | Apache Kafka (Confluent Docker image) + Kafdrop UI |
| Object Storage | Azure Data Lake Storage Gen2 (ABFS protocol) |
| Compute | Databricks Serverless + Delta Live Tables |
| Table Format | Delta Lake (Medallion: Bronze / Silver / Gold) |
| Governance | Unity Catalog (External Locations, Row Filters, Column Masks, Lineage) |
| Orchestration | Databricks Workflows (3-task DAG) |
| Dashboard | Databricks SQL + SQL Warehouse |
| Language | Python 3.11, PySpark, SQL |

---

## Project Structure

```
LedgerFlow-lakehouse/
├── infra/
│   ├── docker-compose.yml              # PostgreSQL + Kafka + Debezium + Kafdrop
│   └── debezium/
│       └── register_connector.sh       # Debezium connector registration
├── scripts/
│   ├── seed_data.py                    # Seeds 68 customers, 88 loans, 1000+ txns
│   ├── data_simulator.py               # Continuous CDC event generator (updates/deletes)
│   ├── kafka_to_adls_bridge.py         # Kafka consumer → ADLS Gen2 NDJSON writer
│   └── e2e_test.py                     # E2E test: inject CDC events + verify propagation
├── databricks/notebooks/
│   ├── 01_auto_loader_bronze.py        # Auto Loader → Bronze Delta tables
│   ├── 02_dlt_silver_gold.py           # DLT Silver + Gold pipeline
│   ├── 03_unity_catalog_setup.py       # Governance: row filters, column masks, grants
│   ├── 04_sql_dashboard_queries.sql    # SQL Dashboard 6-panel queries
│   └── 05_workflow_orchestrator.py     # Databricks Workflow orchestrator notebook
├── docs/
│   ├── ARCHITECTURE.md                 # Component deep dive: CDC, DLT, Unity Catalog
│   └── INTERVIEW_GUIDE.md              # Q&A prep for Senior DE interviews
├── tests/
│   └── test_data_quality.py            # Unit tests for DLT validation rules (pytest)
├── sql/
│   └── 01_create_schema.sql            # PostgreSQL banking schema DDL
├── requirements.txt
├── PROJECT_PLAN.md
└── README.md
```

---

## Key Engineering Decisions

**Debezium `ExtractNewRecordState` SMT**
Flattens the Kafka envelope so each message contains flat fields + `__op` (double underscore), `__ts_ms`, `__before`. Consumed directly by the bridge without schema unpacking.

**Unity Catalog External Location (not `spark.conf.set`)**
Serverless compute blocks dynamic Spark configs for ADLS keys. Solution: Azure Access Connector (Managed Identity) → Storage Credential → External Location. Zero credential leakage — enterprise-grade pattern.

**`_metadata.file_path` instead of `input_file_name()`**
Unity Catalog blocks `input_file_name()`. Auto Loader exposes `_metadata.file_path` as a built-in column — used for lineage tracking in Bronze tables.

**DLT `APPLY CHANGES INTO` for SCD Type 1**
Handles out-of-order CDC events using `sequence_by="__ts_ms"`. Deletes propagated via `apply_as_deletes=expr("__op = 'd'")`. Metadata columns excluded via `except_column_list`.

**`disbursed_at` epoch milliseconds**
Debezium serializes PostgreSQL `TIMESTAMP` as BIGINT (epoch ms) in Kafka. Converted in DLT with `F.to_date(F.from_unixtime(col("disbursed_at") / 1000))`.

**Gold promotion pattern**
DLT pipeline default schema = silver, so all tables land in silver. Gold tables promoted post-pipeline via `CREATE OR REPLACE TABLE gold.X AS SELECT * FROM silver.X`.

---

## Data Model

### Silver Layer

**customers** — 71 rows, CDC-merged, row-filtered + column-masked
```
customer_id | name | email* | city** | state | annual_income* | credit_score | created_at
* masked (non-admins: first char + ***@masked.com / NULL)
** row filter: non-admins see South Indian cities only
```

**loans** — 90 rows, validated by DLT expectations
```
loan_id | customer_id | loan_type | loan_amount | interest_rate | tenure_months | status | disbursed_at
DLT expectations: valid_loan_amount, valid_interest_rate (1-30), valid_tenure (12-360),
                  valid_loan_type (HOME/PERSONAL/BUSINESS/AUTO), valid_status
```

**transactions** — 1500+ rows, deduplicated on `txn_id`
```
txn_id | loan_id | amount | txn_type | txn_date | remarks | created_at
DLT expectations: valid_amount (>0), valid_txn_type, has_loan_id (NOT NULL)
```

### Gold Layer

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `portfolio_summary` | Loan count + exposure by type × status | loan_type, status, loan_count, total_exposure |
| `customer_risk_profile` | Credit tier segmentation with active exposure | credit_tier, customer_count, active_loans, total_active_exposure |
| `daily_collections` | EMI payment totals by date | txn_date, payment_count, total_collected, avg_emi |
| `npa_watchlist` | Defaulted loans with borrower details | loan_id, borrower_name, city, credit_score, days_since_disbursement |

---

## Unity Catalog Governance

```sql
-- Row-level security: non-admins see only South Indian city customers
CREATE FUNCTION silver.city_row_filter(city STRING) RETURNS BOOLEAN
RETURN IS_ACCOUNT_GROUP_MEMBER('admins')
    OR city IN ('Chennai', 'Bangalore', 'Hyderabad', 'Kochi', 'Coimbatore', ...);

ALTER TABLE silver.customers SET ROW FILTER silver.city_row_filter ON (city);

-- Column masking: email hidden for non-admins
CREATE FUNCTION silver.mask_email(email STRING) RETURNS STRING
RETURN CASE WHEN IS_ACCOUNT_GROUP_MEMBER('admins') THEN email
            ELSE CONCAT(LEFT(email, 1), '***@masked.com') END;

ALTER TABLE silver.customers ALTER COLUMN email SET MASK silver.mask_email;

-- RBAC grants on Gold layer
GRANT SELECT ON TABLE gold.portfolio_summary TO `analyst_group`;
```

---

## Databricks Workflow DAG

```
bronze_ingestion  ──▶  Silver (DLT pipeline)  ──▶  Gold_Promotion
  Notebook task          Pipeline task               Notebook task
  ~1 min                 ~2.5 min                    ~1 min
                                              Total: ~4.5 min
```

---

## Results

| Metric | Value |
|--------|-------|
| Customers in Silver | 71 |
| Loans in Silver | 90 |
| Transactions in Silver | 1,500+ |
| NPA Rate | 11.12% |
| Total Loan Book | ₹25.67 Cr |
| Active Exposure | ₹22+ Cr |
| Files uploaded to ADLS | 277+ |
| Total CDC events processed | 560+ |
| Pipeline run time (end-to-end) | ~4.5 minutes |
| Workflow runs succeeded | 2/2 |
| E2E test result | 3/3 PASS |
| Data quality quarantine rate | 0% |

---

## Running Locally

### Prerequisites
- Docker Desktop
- Python 3.11+
- Azure subscription (free $200 credit)
- Databricks workspace (Azure — Premium tier)

### 1. Start infrastructure
```bash
cd infra && docker-compose up -d
```

### 2. Install dependencies
```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

### 3. Seed data + register CDC
```bash
python scripts/seed_data.py
bash infra/debezium/register_connector.sh
```

### 4. Start simulator + bridge
```bash
# Terminal 1 — generate CDC events
python scripts/data_simulator.py

# Terminal 2 — stream to ADLS
python scripts/kafka_to_adls_bridge.py
```

### 5. Run Databricks pipeline
In Databricks UI: **Jobs & Pipelines → LedgerFlow-Daily-Pipeline → Run now**

### 6. Run E2E test
```bash
python scripts/e2e_test.py --inject
# trigger workflow, then:
python scripts/e2e_test.py --verify
```

---

## Environment Variables

Create a `.env` file (not committed to Git):
```
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=ledgerflow
POSTGRES_USER=ledgerflow
POSTGRES_PASSWORD=ledgerflow123
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
ADLS_ACCOUNT_NAME=ledgerflowadls
ADLS_ACCOUNT_KEY=<your-key>
ADLS_CONTAINER=raw
```

---

## Skills Demonstrated

- **CDC pipeline design** — PostgreSQL WAL → Debezium → Kafka → cloud storage
- **Databricks Auto Loader** — incremental file ingestion with schema evolution and lineage
- **Delta Live Tables** — declarative ETL with `APPLY CHANGES INTO`, expectations, quarantine tables
- **Unity Catalog** — external locations, row-level security, column masking, automatic lineage
- **Azure cloud** — ADLS Gen2, Access Connector, Managed Identity (no credential leakage)
- **Pipeline orchestration** — Databricks Workflows 3-task DAG with dependency management
- **Data quality engineering** — DLT expectations, quarantine tables, E2E test framework
- **SQL analytics** — Databricks SQL Dashboard with 6 KPI panels on a dedicated SQL Warehouse

---

## Author

**Beeram Neela Konda Reddy**
Senior Data Engineer | AWS Certified | SnowPro Core | Databricks Certified Data Engineer Associate

[GitHub](https://github.com/bnkr-breakthrough)
