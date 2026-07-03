# LedgerFlow Lakehouse — CDC-Driven Banking Data Platform

> **Portfolio Project** | Beeram Neela Konda Reddy | Databricks · Delta Lake · Kafka · Azure

[![Databricks](https://img.shields.io/badge/Databricks-Delta%20Live%20Tables-red?logo=databricks)](https://databricks.com)
[![Azure](https://img.shields.io/badge/Azure-ADLS%20Gen2-blue?logo=microsoftazure)](https://azure.microsoft.com)
[![Kafka](https://img.shields.io/badge/Apache-Kafka-black?logo=apachekafka)](https://kafka.apache.org)
[![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)](https://python.org)

---

## What This Project Demonstrates

A real-time banking data lakehouse that handles the core challenges of CDC-based pipelines — not just happy-path ingestion:

| Problem | How It's Handled |
|---|---|
| **Real-time change capture** | PostgreSQL WAL → Debezium → Kafka — captures every INSERT, UPDATE, DELETE |
| **Out-of-order CDC events** | DLT `APPLY CHANGES INTO` with `sequence_by="__ts_ms"` — correct merge regardless of arrival order |
| **Data quality enforcement** | DLT Expectations (`expect_or_drop`) — bad records routed to quarantine table, never silently dropped |
| **Credential-free cloud access** | Azure Managed Identity + Unity Catalog External Location — no `spark.conf.set`, no key leakage |
| **Row & column governance** | Unity Catalog row filters + column masks — non-admins see filtered cities and masked PII |

**Senior-level patterns demonstrated:** CDC merge (SCD Type 1) · medallion architecture · declarative DLT pipelines · Unity Catalog governance · end-to-end Workflow orchestration · E2E test framework

---

## Architecture

```
Data Simulator (Python)
  └── Real-world banking data: Indian names, cities, loan types
  └── Generates: inserts · updates · deletes via PostgreSQL
        │
        ▼  kafka_to_adls_bridge.py  (50 events or 30s flush)
PostgreSQL WAL → Debezium → Kafka
        │  CDC events: __op = r / c / u / d
        ▼
ADLS Gen2 Raw Zone  ──  NDJSON · partitioned by entity/year/month/day/hour
        │
        ▼  01_auto_loader_bronze.py
Databricks Auto Loader (cloudFiles)
  ├── Schema inference + evolution (addNewColumns)
  ├── trigger(availableNow=True) — batch mode
  └── _metadata.file_path for Unity Catalog lineage
        │
        ▼  Delta Live Tables — 02_dlt_silver_gold.py
Bronze → Silver (APPLY CHANGES INTO)
  ├── CDC merge: upserts + deletes on customer_id / loan_id
  ├── DLT Expectations — valid amounts, rates, types
  └── transactions_quarantine — failed records with _quarantine_reason
        │
        ▼
Silver → Gold (DLT aggregations)
  ├── portfolio_summary       — exposure by loan type × status
  ├── customer_risk_profile   — credit tier segmentation
  ├── daily_collections       — EMI payment totals by date
  └── npa_watchlist           — defaulted loans with borrower info
        │
        ▼  03_unity_catalog_setup.py
Unity Catalog Governance
  ├── Row filter: city_row_filter (South Indian cities for non-admins)
  ├── Column mask: mask_email · mask_income
  ├── RBAC: GRANT SELECT on Gold to analyst group
  └── Lineage: auto-tracked Bronze → Silver → Gold
        │
        ▼
Databricks SQL Dashboard — LedgerFlow Banking Analytics (6 panels)
Databricks Workflow    — 3-task DAG, ~4.5 min end-to-end
```

---

## Key Results

### Pipeline Run (End-to-End)

| Metric | Value |
|---|---|
| Customers in Silver | 71 |
| Loans in Silver | 90 |
| Transactions in Silver | 1,500+ |
| NPA Rate | **11.12%** |
| Total Loan Book | **₹25.67 Cr** |
| Active Exposure | ₹22+ Cr |

### Infrastructure

| Metric | Value |
|---|---|
| CDC events processed | 560+ |
| ADLS files uploaded | 277+ |
| Workflow runtime | **~4.5 minutes** |
| Workflow runs succeeded | 2 / 2 |
| E2E test result | **3/3 PASS** |
| DQ quarantine rate | 0% |

---

## Repository Structure

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

## Quick Start

```bash
# 1. Clone and install dependencies
git clone https://github.com/bnkr-breakthrough/LedgerFlow-lakehouse.git
cd LedgerFlow-lakehouse
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# 2. Start infrastructure (PostgreSQL + Kafka + Debezium)
cd infra && docker-compose up -d

# 3. Seed banking data and register CDC connector
python scripts/seed_data.py
bash infra/debezium/register_connector.sh

# 4. Start CDC simulator + ADLS bridge
python scripts/data_simulator.py          # Terminal 1
python scripts/kafka_to_adls_bridge.py   # Terminal 2

# 5. Run Databricks Workflow
# Jobs & Pipelines → LedgerFlow-Daily-Pipeline → Run now

# 6. Run E2E test
python scripts/e2e_test.py --inject
# (trigger workflow, then:)
python scripts/e2e_test.py --verify
```

Full step-by-step setup in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

---

## Azure + Databricks Services Used

| Service | Purpose | Cost |
|---|---|---|
| ADLS Gen2 | Raw zone storage (NDJSON, partitioned) | ~$0 (free credit) |
| Azure Access Connector | Managed Identity for ADLS access | Free |
| Databricks Serverless | Auto Loader + governance notebooks | Free trial |
| Delta Live Tables | Silver + Gold CDC pipeline | Free trial |
| Databricks SQL Warehouse | Dashboard queries | Free trial |
| Databricks Workflows | 3-task pipeline orchestration | Free trial |

**Total cloud cost for this project: ~$0** (Azure $200 free credit + Databricks trial)

---

## Data Model

Three banking entities with CDC-tracked changes:

```
customers ──────────────────────────────── one customer → many loans
    │  customer_id · credit_score · annual_income* · email*
    │  * PII masked for non-admins via Unity Catalog column masks
    ▼
loans ──────────────────────────────────── one loan → many transactions
    │  loan_id · loan_type · loan_amount · status · disbursed_at
    │  status: pending → active → defaulted / closed
    ▼
transactions ───────────────────────────── EMI payments, disbursements, penalties
       txn_id · loan_id · amount · txn_type · txn_date
       txn_type: DISBURSEMENT · PAYMENT · PENALTY · FORECLOSURE
```

**Simulated data:** Indian names, cities (Mumbai, Delhi, Bangalore, Hyderabad...), realistic loan amounts (₹50K–₹2Cr), interest rates 8.5–22%, credit scores 580–820.

---

## Scope Decisions

| Excluded | Reason |
|---|---|
| Kafka Connect ADLS Sink | Python bridge demonstrates the pattern explicitly; connector would be production shortcut |
| Continuous streaming | `trigger(availableNow=True)` demonstrates batch-on-demand pattern; streaming would require always-on compute |
| dbt transformations | DLT declarative pipeline covers transformation layer; dbt covered in other portfolio projects |
| Real bank data | Synthetic data with realistic distributions — protects privacy, still demonstrates all pipeline patterns |

---

## Documentation

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — Component deep dive: CDC internals, Unity Catalog access pattern, DLT merge logic, data flow timing
- [`docs/INTERVIEW_GUIDE.md`](docs/INTERVIEW_GUIDE.md) — 15+ Senior DE interview Q&A with production-scale extensions

---

## Related Projects

- [OrderLake](https://github.com/bnkr-breakthrough/OrderLake) — Serverless AWS data lake · Glue · Athena · PySpark · DQ scorecard
- [FinShield](https://github.com/bnkr-breakthrough/finshield-fraud-platform) — Real-time fraud detection · Kafka · Spark · Snowflake · dbt · Streamlit

---

*Portfolio-scale project — demonstrates senior-level data engineering patterns on real cloud infrastructure. See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for an honest assessment of what would differ at production scale.*
