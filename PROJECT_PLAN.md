# LedgerFlow Lakehouse — Complete Project Plan

**Author:** Beeram Neela Konda Reddy  
**Stack:** PostgreSQL · Debezium · Kafka · Databricks Auto Loader · Delta Lake · Delta Live Tables · Unity Catalog · Databricks SQL · Databricks Workflows  
**Domain:** Banking / Loan Processing (BFSI)  
**Cost:** $0 — built entirely on free & trial resources

---

## Free Resources Confirmation

| Component | Tool | Cost |
|---|---|---|
| Source Database | PostgreSQL (Docker) | Free |
| CDC Engine | Debezium (Docker) | Free |
| Message Broker | Apache Kafka + Zookeeper (Docker) | Free |
| Kafka UI | Kafdrop (Docker) | Free |
| Cloud Storage | Azure Data Lake Storage Gen2 | Free (within $200 Azure credit) |
| Lakehouse + DLT + Unity Catalog + SQL | Databricks on Azure | Free (within $200 Azure credit) |
| Orchestration | Databricks Workflows | Free (within $200 Azure credit) |
| Version Control | GitHub | Free |

**Azure free account:** $200 credit for 30 days (new account signup).  
**Databricks cluster cost:** ~$1.50–$3/hour on smallest node. Total project build time ~40 hrs active dev = ~$60–100 well within $200 limit.  
**Zero compromises.** Every planned feature — DLT, Unity Catalog, Auto Loader, Databricks SQL, Workflows — is included.

---

## Architecture Overview

```
PostgreSQL (Docker)
    ↓  [Write-Ahead Log]
Debezium (Docker)
    ↓  [CDC events: insert/update/delete]
Apache Kafka (Docker)
    ↓  [Topics per table]
Python Kafka Bridge
    ↓  [JSON files, partitioned by date]
Azure Data Lake Storage Gen2
    ↓  [Cloud storage]
Databricks Auto Loader
    ↓  [Micro-batch streaming ingestion]
Delta Lake — BRONZE (raw CDC events)
    ↓  [Delta Live Tables]
Delta Lake — SILVER (cleaned, CDC-merged, DQ-validated)
    ↓  [Delta Live Tables]
Delta Lake — GOLD (business aggregations)
    ↓
Unity Catalog (Lineage + RLS + Column Masking)
    ↓
Databricks SQL Dashboard + Databricks Workflows
```

---

## Phase 1 — Project Structure & Environment Setup

**Goal:** Set up the complete folder skeleton, Python environment, and all config files before writing any pipeline code.

### Tasks
1. Create folder structure in `LedgerFlow-lakehouse/`
2. Set up Python virtual environment (`venv`)
3. Create `requirements.txt` with all dependencies
4. Create `.gitignore`
5. Create base `README.md`
6. Initialize local Git repo
7. Create GitHub repo `bnkr-breakthrough/ledgerflow-lakehouse`

### Folder Structure
```
LedgerFlow-lakehouse/
├── infra/
│   ├── docker-compose.yml          # PostgreSQL + Kafka + Debezium + Kafdrop
│   └── debezium/
│       └── register-connector.json # Debezium connector config
├── sql/
│   ├── 01_create_schema.sql        # Create tables
│   └── 02_seed_data.sql            # Insert initial data
├── scripts/
│   ├── seed_data.py                # Python seed script
│   ├── data_simulator.py           # Ongoing change generator
│   └── kafka_to_adls_bridge.py     # Kafka consumer → ADLS Gen2
├── databricks/
│   ├── notebooks/
│   │   ├── 01_auto_loader_bronze.py
│   │   ├── 02_dlt_silver_gold.py
│   │   ├── 03_unity_catalog_setup.py
│   │   └── 04_sql_dashboard_queries.sql
│   └── workflows/
│       └── ledgerflow_workflow.json
├── tests/
│   ├── test_data_quality.py
│   └── test_pipeline_e2e.py
├── docs/
│   └── architecture_diagram.png
├── requirements.txt
├── .gitignore
├── PROJECT_PLAN.md
└── README.md
```

---

## Phase 2 — Azure & Databricks Setup

**Goal:** Create cloud account and Databricks workspace before any data touches the cloud.

### Tasks
1. Sign up for Azure free account at portal.azure.com (new email = $200 credit)
2. Create Resource Group: `ledgerflow-rg`
3. Create Azure Data Lake Storage Gen2 account: `ledgerflowadls`
   - Create containers: `raw/`, `bronze/`, `silver/`, `gold/`
4. Create Azure Databricks workspace: `ledgerflow-workspace` (Premium tier — required for Unity Catalog)
5. Launch Databricks workspace and create compute cluster:
   - Cluster name: `ledgerflow-cluster`
   - Node type: `Standard_DS3_v2` (smallest, cheapest)
   - Databricks Runtime: `14.3 LTS` or latest
   - Auto-terminate: 30 minutes idle (saves cost)
6. Mount ADLS Gen2 to Databricks using Service Principal or Access Key
7. Enable Unity Catalog on the workspace
8. Create Unity Catalog: `ledgerflow_catalog`
   - Schemas: `bronze`, `silver`, `gold`

---

## Phase 3 — Source Layer: PostgreSQL Setup

**Goal:** A live operational banking database running in Docker, seeded with realistic data, with logical replication enabled for Debezium.

### Tasks
1. Write `docker-compose.yml` for PostgreSQL with `wal_level=logical`
2. Write `01_create_schema.sql`:
   - `customers` table (customer_id, name, email, credit_score, city, created_at, updated_at)
   - `loans` table (loan_id, customer_id, loan_amount, loan_type, status, interest_rate, disbursed_at, created_at, updated_at)
   - `transactions` table (txn_id, loan_id, amount, txn_type, txn_date, created_at)
3. Write `02_seed_data.sql` — insert 50 customers, 80 loans, 200 transactions
4. Write `scripts/seed_data.py` — Python version of seed using `psycopg2`
5. Write `scripts/data_simulator.py`:
   - Every 10 seconds: randomly update a loan status (pending→active, active→defaulted, etc.)
   - Every 30 seconds: insert a new transaction (payment or disbursement)
   - Every 60 seconds: insert a new customer + loan application
   - Runs indefinitely to simulate live bank activity
6. Start PostgreSQL container and verify all tables populated

### Table: customers
```sql
CREATE TABLE customers (
    customer_id   VARCHAR(10) PRIMARY KEY,
    name          VARCHAR(100) NOT NULL,
    email         VARCHAR(150) NOT NULL,
    credit_score  INT,
    city          VARCHAR(50),
    created_at    TIMESTAMP DEFAULT NOW(),
    updated_at    TIMESTAMP DEFAULT NOW()
);
```

### Table: loans
```sql
CREATE TABLE loans (
    loan_id       VARCHAR(10) PRIMARY KEY,
    customer_id   VARCHAR(10) REFERENCES customers(customer_id),
    loan_amount   NUMERIC(12,2) NOT NULL,
    loan_type     VARCHAR(20),  -- HOME, PERSONAL, BUSINESS, AUTO
    status        VARCHAR(20),  -- pending, active, defaulted, closed
    interest_rate NUMERIC(5,2),
    disbursed_at  TIMESTAMP,
    created_at    TIMESTAMP DEFAULT NOW(),
    updated_at    TIMESTAMP DEFAULT NOW()
);
```

### Table: transactions
```sql
CREATE TABLE transactions (
    txn_id        VARCHAR(15) PRIMARY KEY,
    loan_id       VARCHAR(10) REFERENCES loans(loan_id),
    amount        NUMERIC(12,2) NOT NULL,
    txn_type      VARCHAR(20),  -- DISBURSEMENT, PAYMENT, PENALTY, FORECLOSURE
    txn_date      DATE NOT NULL,
    created_at    TIMESTAMP DEFAULT NOW()
);
```

---

## Phase 4 — CDC Layer: Debezium + Kafka Setup

**Goal:** Capture every PostgreSQL row change and publish it to Kafka topics in real time.

### Tasks
1. Add to `docker-compose.yml`:
   - Zookeeper
   - Kafka (broker)
   - Kafka Connect (with Debezium PostgreSQL connector plugin)
   - Kafdrop (web UI to visualize Kafka topics)
2. Write `infra/debezium/register-connector.json`:
   ```json
   {
     "name": "ledgerflow-postgres-connector",
     "config": {
       "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
       "database.hostname": "postgres",
       "database.port": "5432",
       "database.user": "ledgerflow",
       "database.password": "ledgerflow123",
       "database.dbname": "ledgerflowdb",
       "database.server.name": "ledgerflow",
       "table.include.list": "public.customers,public.loans,public.transactions",
       "plugin.name": "pgoutput",
       "topic.prefix": "ledgerflow"
     }
   }
   ```
3. Write script to register connector via Debezium REST API (`curl POST`)
4. Start all containers: `docker-compose up -d`
5. Verify in Kafdrop (http://localhost:9000) that 3 topics exist:
   - `ledgerflow.public.customers`
   - `ledgerflow.public.loans`
   - `ledgerflow.public.transactions`
6. Trigger a manual update in PostgreSQL and verify CDC event appears in Kafka topic

### Sample CDC Event (loan status update)
```json
{
  "schema": {...},
  "payload": {
    "before": {
      "loan_id": "L1003",
      "status": "active",
      "updated_at": 1706789400000
    },
    "after": {
      "loan_id": "L1003",
      "status": "defaulted",
      "updated_at": 1706789527000
    },
    "source": {
      "version": "2.4.0",
      "connector": "postgresql",
      "name": "ledgerflow",
      "ts_ms": 1706789527000,
      "db": "ledgerflowdb",
      "schema": "public",
      "table": "loans",
      "lsn": 24512874
    },
    "op": "u",
    "ts_ms": 1706789527543
  }
}
```

---

## Phase 5 — Bridge Layer: Kafka → ADLS Gen2

**Goal:** Since local Kafka cannot be directly reached by Databricks on Azure, a lightweight Python bridge reads from local Kafka and writes partitioned JSON files to ADLS Gen2 — the handoff point between on-prem and cloud.

### Tasks
1. Write `scripts/kafka_to_adls_bridge.py`:
   - Subscribes to all 3 Kafka topics using `confluent-kafka` Python client
   - Batches events every 30 seconds
   - Writes to ADLS Gen2 as JSON files with partition path:
     ```
     raw/loans/year=2024/month=02/day=10/hour=14/events_1707570600.json
     raw/customers/year=2024/month=02/day=10/hour=14/events_1707570600.json
     raw/transactions/year=2024/month=02/day=10/hour=14/events_1707570600.json
     ```
   - Uses `azure-storage-file-datalake` Python SDK
2. Store ADLS connection string in `.env` file (gitignored)
3. Run bridge script in background while data_simulator.py runs
4. Verify JSON files appearing in ADLS Gen2 via Azure Portal

---

## Phase 6 — Bronze Layer: Databricks Auto Loader

**Goal:** Ingest raw CDC JSON files from ADLS Gen2 into Delta Lake Bronze tables using Auto Loader — handles schema inference, schema evolution, and exactly-once delivery automatically.

### Tasks
1. Write `databricks/notebooks/01_auto_loader_bronze.py`:
   - Auto Loader reads from `raw/loans/`, `raw/customers/`, `raw/transactions/`
   - Infers schema from JSON automatically
   - Writes to Delta tables with added metadata columns:
     - `_ingested_at` (timestamp when Auto Loader processed it)
     - `_source_file` (which file it came from)
     - `_op` (CDC operation type: c/u/d)
   - Uses `checkpointLocation` for exactly-once guarantee
   - Target tables: `ledgerflow_catalog.bronze.loans_cdc`, `bronze.customers_cdc`, `bronze.transactions_cdc`
2. Run notebook and verify Bronze tables populated in Unity Catalog
3. Check record counts match what was inserted in PostgreSQL

### Bronze Schema (loans_cdc)
```
loan_id         STRING
before_status   STRING
after_status    STRING
before_amount   DOUBLE
after_amount    DOUBLE
op              STRING    (c=create, u=update, d=delete)
source_table    STRING
source_lsn      LONG
event_ts        TIMESTAMP
_ingested_at    TIMESTAMP
_source_file    STRING
```

---

## Phase 7 — Silver Layer: Delta Live Tables

**Goal:** Use DLT's declarative framework to apply CDC merge logic, enforce data quality, and produce clean current-state tables.

### Tasks
1. Write `databricks/notebooks/02_dlt_silver_gold.py` — Part 1: Silver
2. Silver pipeline steps:
   - **Step 1:** Read Bronze CDC tables as DLT streaming sources
   - **Step 2:** Apply `APPLY CHANGES INTO` (CDC merge) to materialize current state
   - **Step 3:** DLT Expectations for data quality on each table

### DLT Expectations (data quality rules)
```python
# Customers
@dlt.expect_or_drop("valid_credit_score", "credit_score BETWEEN 300 AND 900")
@dlt.expect_or_drop("email_not_null", "email IS NOT NULL")
@dlt.expect_or_drop("city_not_null", "city IS NOT NULL")

# Loans
@dlt.expect_or_drop("positive_loan_amount", "loan_amount > 0")
@dlt.expect_or_drop("valid_loan_type", "loan_type IN ('HOME','PERSONAL','BUSINESS','AUTO')")
@dlt.expect_or_drop("valid_status", "status IN ('pending','active','defaulted','closed')")
@dlt.expect_or_drop("valid_interest_rate", "interest_rate BETWEEN 1.0 AND 36.0")

# Transactions
@dlt.expect_or_drop("positive_txn_amount", "amount > 0")
@dlt.expect_or_drop("valid_txn_type", "txn_type IN ('DISBURSEMENT','PAYMENT','PENALTY','FORECLOSURE')")
```

3. Records failing expectations → quarantine table (`silver.loans_quarantine`)
4. Target tables: `ledgerflow_catalog.silver.customers`, `silver.loans`, `silver.transactions`
5. Verify DLT pipeline graph in Databricks UI (shows lineage visually)
6. Inject intentionally bad records (credit_score = -50) and verify they land in quarantine

---

## Phase 8 — Gold Layer: Delta Live Tables

**Goal:** Business-ready aggregated tables that power the dashboard and analyst queries.

### Tasks
1. Add Gold tables to same DLT pipeline (Part 2):

**gold_loan_portfolio_summary**
```sql
-- Total loans and amounts grouped by status and loan_type
SELECT
    loan_type,
    status,
    COUNT(*) AS total_loans,
    SUM(loan_amount) AS total_amount,
    AVG(interest_rate) AS avg_interest_rate,
    CURRENT_TIMESTAMP AS refreshed_at
FROM LIVE.silver_loans
GROUP BY loan_type, status
```

**gold_default_risk_customers**
```sql
-- Customers with active loans AND credit score below 650 (high risk)
SELECT
    c.customer_id,
    c.name,
    c.city,
    c.credit_score,
    l.loan_id,
    l.loan_amount,
    l.loan_type,
    l.status
FROM LIVE.silver_customers c
JOIN LIVE.silver_loans l ON c.customer_id = l.customer_id
WHERE c.credit_score < 650 AND l.status = 'active'
```

**gold_monthly_disbursements**
```sql
-- Monthly disbursement totals by city
SELECT
    DATE_FORMAT(t.txn_date, 'yyyy-MM') AS month,
    c.city,
    SUM(t.amount) AS total_disbursed,
    COUNT(DISTINCT t.loan_id) AS loans_disbursed
FROM LIVE.silver_transactions t
JOIN LIVE.silver_loans l ON t.loan_id = l.loan_id
JOIN LIVE.silver_customers c ON l.customer_id = c.customer_id
WHERE t.txn_type = 'DISBURSEMENT'
GROUP BY DATE_FORMAT(t.txn_date, 'yyyy-MM'), c.city
```

**gold_repayment_health**
```sql
-- On-time vs missed payments per loan
SELECT
    l.loan_id,
    l.customer_id,
    l.loan_type,
    l.status,
    COUNT(CASE WHEN t.txn_type = 'PAYMENT' THEN 1 END) AS payments_made,
    COUNT(CASE WHEN t.txn_type = 'PENALTY' THEN 1 END) AS penalties_incurred,
    SUM(CASE WHEN t.txn_type = 'PAYMENT' THEN t.amount ELSE 0 END) AS total_repaid
FROM LIVE.silver_loans l
LEFT JOIN LIVE.silver_transactions t ON l.loan_id = t.loan_id
GROUP BY l.loan_id, l.customer_id, l.loan_type, l.status
```

2. Run full DLT pipeline (Bronze → Silver → Gold) end-to-end
3. Verify all Gold tables populated with expected records

---

## Phase 9 — Unity Catalog: Governance Setup

**Goal:** Add enterprise-grade governance — lineage, row-level security, and column masking — proving this isn't just a pipeline but a governed data platform.

### Tasks
1. Write `databricks/notebooks/03_unity_catalog_setup.py`

**Catalog & Schema structure:**
```
ledgerflow_catalog
├── bronze
│   ├── loans_cdc
│   ├── customers_cdc
│   └── transactions_cdc
├── silver
│   ├── loans
│   ├── customers
│   ├── transactions
│   ├── loans_quarantine
│   └── customers_quarantine
└── gold
    ├── loan_portfolio_summary
    ├── default_risk_customers
    ├── monthly_disbursements
    └── repayment_health
```

**Row-Level Security** (analysts see only their city's data):
```sql
CREATE ROW FILTER ledgerflow_catalog.silver.customers_city_filter
ON ledgerflow_catalog.silver.customers
USING (city = current_user_city());

ALTER TABLE ledgerflow_catalog.silver.customers
SET ROW FILTER ledgerflow_catalog.silver.customers_city_filter ON ();
```

**Column Masking** (PII protection):
```sql
-- Mask email for non-admin users
CREATE MASK email_mask
AS (val STRING) -> CASE
    WHEN is_account_group_member('admins') THEN val
    ELSE CONCAT(LEFT(val, 2), '***@***.com')
END;

ALTER TABLE ledgerflow_catalog.silver.customers
ALTER COLUMN email SET MASK email_mask;

-- Mask credit_score for non-analysts
CREATE MASK credit_score_mask
AS (val INT) -> CASE
    WHEN is_account_group_member('analysts') THEN val
    ELSE -1
END;
```

**Data Lineage:** Verified in Databricks Unity Catalog UI — click any Gold table → "Lineage" tab → visual graph tracing Gold → Silver → Bronze → source file → original PostgreSQL WAL event.

2. Create two users in Databricks (simulate Mumbai analyst, Bangalore analyst)
3. Run same query as each user — verify different rows returned
4. Verify email column masked for non-admin user

---

## Phase 10 — Databricks SQL Dashboard

**Goal:** A business-facing dashboard that auto-refreshes as new CDC events flow through the pipeline.

### Tasks
1. Write `databricks/notebooks/04_sql_dashboard_queries.sql` — all queries below
2. In Databricks SQL → Create Dashboard: "LedgerFlow Loan Portfolio"
3. Add visualizations:

| Tile | Type | Query source |
|---|---|---|
| Loan Portfolio by Status | Pie chart | gold_loan_portfolio_summary |
| Total Active Loan Amount | Counter | gold_loan_portfolio_summary WHERE status='active' |
| Monthly Disbursements | Bar chart | gold_monthly_disbursements |
| High-Risk Customers | Table | gold_default_risk_customers |
| Repayment Health | Bar chart | gold_repayment_health |
| City-wise Loan Distribution | Bar chart | gold_loan_portfolio_summary + city join |
| Data Quality Pass Rate | Counter | COUNT(silver) / (COUNT(silver) + COUNT(quarantine)) |

4. Set auto-refresh: every 5 minutes
5. Take screenshots of final dashboard for README

---

## Phase 11 — Databricks Workflows: Orchestration

**Goal:** Automate the full pipeline execution, add alerting on data quality failures, and make the system self-operating.

### Tasks
1. Create Databricks Workflow: `LedgerFlow-Pipeline`
2. Workflow tasks (in order):
   - Task 1: `Auto Loader Bronze Refresh` — runs `01_auto_loader_bronze.py`
   - Task 2: `DLT Pipeline Trigger` — triggers the DLT pipeline (Bronze → Silver → Gold)
   - Task 3: `DQ Report` — notebook that queries quarantine tables, calculates pass/fail rates, logs results
3. Schedule: every 15 minutes
4. Configure alerts:
   - Email alert if DQ pass rate < 95%
   - Email alert if any pipeline task fails
5. Write `databricks/workflows/ledgerflow_workflow.json` — exportable workflow definition
6. Run workflow manually once to verify all tasks succeed

---

## Phase 12 — End-to-End Testing & Validation

**Goal:** Prove the pipeline works correctly under real conditions — including failure scenarios.

### Tests to run

**Test 1: Happy path (normal CDC flow)**
- Start `data_simulator.py`
- Wait 5 minutes
- Verify new records appear in Gold tables
- Expected: Silver counts increase, Gold aggregates update

**Test 2: Data quality enforcement**
- Manually insert bad record in PostgreSQL:
  ```sql
  INSERT INTO customers VALUES ('C999','Test','bad',9999,'X',NOW(),NOW());
  ```
- Expected: Bronze receives it, Silver rejects it to quarantine, Silver count unchanged

**Test 3: Row-level security**
- Query `silver.customers` as Mumbai analyst user
- Expected: only Mumbai rows returned

**Test 4: Column masking**
- Query `silver.customers` as non-admin user
- Expected: email shows `ar***@***.com`, credit_score shows -1

**Test 5: Schema evolution**
- Add a new column to PostgreSQL loans table
- Expected: Auto Loader detects new schema, Bronze table auto-evolves, no pipeline failure

**Test 6: Delete event (soft delete)**
- Delete a customer from PostgreSQL
- Expected: DLT CDC merge marks record as deleted in Silver, not hard-deleted

---

## Phase 13 — Documentation, Results & GitHub Push

**Goal:** Make the project recruiter-ready on GitHub with clear evidence of results.

### Tasks
1. Write final `README.md` with:
   - Project overview (1 paragraph)
   - Architecture diagram (image)
   - Tech stack table
   - Setup instructions (step-by-step, one command at a time)
   - Results section with metrics
   - Screenshots of Kafdrop, DLT pipeline graph, Unity Catalog lineage, SQL Dashboard
2. Write `RESULTS.md` with measurable outcomes:
   - CDC latency: X seconds from PostgreSQL change to Bronze landing
   - Data quality pass rate: X% across all tables
   - Records processed in demo run
   - Number of DQ rules enforced
   - Row-level security: verified (X cities isolated)
   - Column masking: verified (email + credit_score)
3. Create architecture diagram (draw.io or Excalidraw)
4. Take screenshots at each layer
5. Commit all code to GitHub:
   ```
   git init
   git add .
   git commit -m "feat: initial LedgerFlow lakehouse project"
   git remote add origin https://github.com/bnkr-breakthrough/ledgerflow-lakehouse.git
   git push -u origin main
   ```
6. Add repo to GitHub profile README and resume

---

## Build Order Summary

| Phase | What | Output |
|---|---|---|
| 1 | Project structure + env setup | Clean folder, venv, .gitignore |
| 2 | Azure + Databricks setup | Live workspace, ADLS containers, Unity Catalog |
| 3 | PostgreSQL + seed data | Live banking DB, data simulator |
| 4 | Debezium + Kafka | CDC events flowing to Kafka topics |
| 5 | Kafka → ADLS bridge | JSON files landing in cloud storage |
| 6 | Auto Loader → Bronze | Raw CDC Delta tables in Unity Catalog |
| 7 | DLT Silver | Clean current-state tables + quarantine |
| 8 | DLT Gold | Business aggregation tables |
| 9 | Unity Catalog governance | RLS, column masking, lineage |
| 10 | SQL Dashboard | Live business dashboard |
| 11 | Workflows | Automated scheduling + alerting |
| 12 | Testing | Validated end-to-end |
| 13 | Docs + GitHub | Resume-ready repo |

---

## What This Proves to a Recruiter

1. **CDC mastery** — You know how WAL works, why snapshots aren't enough, how Debezium captures row-level changes. Most DEs don't.
2. **Databricks-native thinking** — DLT (not hand-coded Spark), Auto Loader (not manual COPY INTO), Unity Catalog (not ad-hoc permissions). You use the platform the way it's designed.
3. **Governance mindset** — Row-level security and column masking mean you understand compliance, not just pipelines.
4. **Real engineering** — The Kafka→ADLS bridge is exactly what on-premises-to-cloud enterprises build. You've simulated a real migration problem.
5. **Data quality embedded in pipelines** — DLT Expectations with quarantine shows you don't treat DQ as an afterthought.
