# LedgerFlow Lakehouse

> **CDC-Driven Banking Data Lakehouse on Databricks**  
> Real-time loan portfolio analytics powered by Change Data Capture, Delta Live Tables, and Unity Catalog governance.

---

## Overview

LedgerFlow simulates a bank's operational PostgreSQL database and streams every row-level change — loan status updates, new customer applications, payment events — through a fully governed Databricks lakehouse in real time.

Built to demonstrate senior-level data engineering: CDC ingestion with Debezium, declarative pipelines with Delta Live Tables, and enterprise governance with Unity Catalog (data lineage, row-level security, column masking).

---

## Architecture

```
PostgreSQL (Docker)
    ↓  Write-Ahead Log (WAL)
Debezium (Docker)
    ↓  CDC events: insert / update / delete
Apache Kafka (Docker)
    ↓  Topics: customers | loans | transactions
Python Kafka→ADLS Bridge
    ↓  Partitioned JSON files
Azure Data Lake Storage Gen2
    ↓
Databricks Auto Loader
    ↓  Exactly-once micro-batch ingestion
Delta Lake — BRONZE  (raw CDC events)
    ↓  Delta Live Tables
Delta Lake — SILVER  (cleaned · CDC-merged · DQ-validated)
    ↓  Delta Live Tables
Delta Lake — GOLD    (business aggregations)
    ↓
Unity Catalog  (lineage · row-level security · column masking)
    ↓
Databricks SQL Dashboard + Databricks Workflows
```

---

## Tech Stack

| Layer | Tool |
|---|---|
| Source Database | PostgreSQL 15 |
| CDC Engine | Debezium 2.4 |
| Message Broker | Apache Kafka 3.6 |
| Cloud Storage | Azure Data Lake Storage Gen2 |
| Ingestion | Databricks Auto Loader |
| Storage Format | Delta Lake (Medallion: Bronze / Silver / Gold) |
| ETL Pipelines | Delta Live Tables (DLT) |
| Data Governance | Unity Catalog |
| Serving | Databricks SQL |
| Orchestration | Databricks Workflows |
| Language | Python · SQL |

---

## Project Structure

```
LedgerFlow-lakehouse/
├── infra/
│   ├── docker-compose.yml              # PostgreSQL + Kafka + Debezium + Kafdrop
│   └── debezium/
│       └── register-connector.json     # Debezium PostgreSQL connector config
├── sql/
│   ├── 01_create_schema.sql            # Banking schema creation
│   └── 02_seed_data.sql                # Initial seed data
├── scripts/
│   ├── seed_data.py                    # Python seed script
│   ├── data_simulator.py               # Ongoing change generator
│   └── kafka_to_adls_bridge.py         # Kafka consumer → ADLS Gen2
├── databricks/
│   ├── notebooks/
│   │   ├── 01_auto_loader_bronze.py    # Auto Loader ingestion → Bronze
│   │   ├── 02_dlt_silver_gold.py       # DLT pipeline: Silver + Gold
│   │   ├── 03_unity_catalog_setup.py   # Governance: RLS + column masking
│   │   └── 04_sql_dashboard_queries.sql
│   └── workflows/
│       └── ledgerflow_workflow.json    # Databricks Workflow definition
├── tests/
│   ├── test_data_quality.py
│   └── test_pipeline_e2e.py
├── docs/
│   └── architecture_diagram.png
├── .env.example
├── requirements.txt
└── PROJECT_PLAN.md
```

---

## Setup Instructions

> Full step-by-step setup guide coming in each phase. See `PROJECT_PLAN.md` for the complete build plan.

### Prerequisites
- Docker Desktop installed
- Python 3.11+
- Azure account (free tier — $200 credit)
- Databricks workspace on Azure (Premium tier)
- Git

---

## Results

> To be updated after project completion.

- CDC latency (PostgreSQL → Bronze): TBD
- Data quality pass rate: TBD
- DLT Expectations enforced: TBD
- Unity Catalog governance: Row-level security ✓ | Column masking ✓ | Data lineage ✓

---

## Author

**Beeram Neela Konda Reddy**  
ETL Developer → Senior Data Engineer  
[GitHub](https://github.com/bnkr-breakthrough) | AWS Certified Data Engineer | SnowPro Core | Databricks Certified Data Engineer Associate
