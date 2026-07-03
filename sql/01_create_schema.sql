-- =============================================================================
-- LedgerFlow Lakehouse — Banking Schema
-- Creates tables for customers, loans, and transactions
-- WAL logical replication is enabled at the Docker level
-- =============================================================================

-- ─── CUSTOMERS ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS customers (
    customer_id     VARCHAR(10)  PRIMARY KEY,
    name            VARCHAR(100) NOT NULL,
    email           VARCHAR(150) NOT NULL,
    credit_score    INT,
    city            VARCHAR(50),
    state           VARCHAR(50),
    annual_income   NUMERIC(12,2),
    created_at      TIMESTAMP    DEFAULT NOW(),
    updated_at      TIMESTAMP    DEFAULT NOW()
);

-- ─── LOANS ───────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS loans (
    loan_id         VARCHAR(10)  PRIMARY KEY,
    customer_id     VARCHAR(10)  REFERENCES customers(customer_id),
    loan_amount     NUMERIC(12,2) NOT NULL,
    loan_type       VARCHAR(20)  CHECK (loan_type IN ('HOME','PERSONAL','BUSINESS','AUTO')),
    status          VARCHAR(20)  CHECK (status IN ('pending','active','defaulted','closed'))
                                 DEFAULT 'pending',
    interest_rate   NUMERIC(5,2),
    tenure_months   INT,
    disbursed_at    TIMESTAMP,
    created_at      TIMESTAMP    DEFAULT NOW(),
    updated_at      TIMESTAMP    DEFAULT NOW()
);

-- ─── TRANSACTIONS ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS transactions (
    txn_id          VARCHAR(15)  PRIMARY KEY,
    loan_id         VARCHAR(10)  REFERENCES loans(loan_id),
    amount          NUMERIC(12,2) NOT NULL,
    txn_type        VARCHAR(20)  CHECK (txn_type IN ('DISBURSEMENT','PAYMENT','PENALTY','FORECLOSURE')),
    txn_date        DATE         NOT NULL,
    remarks         VARCHAR(255),
    created_at      TIMESTAMP    DEFAULT NOW()
);

-- ─── PUBLICATION for Debezium CDC ────────────────────────────────────────────
-- Tells PostgreSQL WAL to track all changes on these 3 tables
CREATE PUBLICATION ledgerflow_pub
    FOR TABLE customers, loans, transactions;
