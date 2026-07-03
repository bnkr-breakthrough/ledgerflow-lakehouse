-- =============================================================================
-- LedgerFlow Lakehouse — Phase 10: SQL Dashboard Queries
-- =============================================================================
-- Run each query in Databricks SQL Editor to build the dashboard panels.
-- Catalog: ledgerflow_catalog
-- =============================================================================

-- ─── QUERY 1: Portfolio Health Overview ──────────────────────────────────────
-- Panel type: Counter / Summary Bar
-- Shows total loan book size, active exposure, NPA count

SELECT
    COUNT(*)                                    AS total_loans,
    SUM(loan_amount)                            AS total_book_value,
    SUM(CASE WHEN status = 'active'     THEN loan_amount ELSE 0 END) AS active_exposure,
    SUM(CASE WHEN status = 'defaulted'  THEN loan_amount ELSE 0 END) AS npa_exposure,
    SUM(CASE WHEN status = 'closed'     THEN loan_amount ELSE 0 END) AS closed_value,
    ROUND(
        SUM(CASE WHEN status = 'defaulted' THEN loan_amount ELSE 0 END)
        / SUM(loan_amount) * 100, 2
    )                                           AS npa_percentage
FROM ledgerflow_catalog.silver.loans;


-- ─── QUERY 2: Loan Portfolio by Type and Status ───────────────────────────────
-- Panel type: Grouped Bar Chart (loan_type on X, status as color)

SELECT
    loan_type,
    status,
    COUNT(*)                        AS loan_count,
    ROUND(SUM(loan_amount), 2)      AS total_amount,
    ROUND(AVG(loan_amount), 2)      AS avg_amount,
    ROUND(AVG(interest_rate), 2)    AS avg_rate
FROM ledgerflow_catalog.silver.loans
GROUP BY loan_type, status
ORDER BY loan_type, status;


-- ─── QUERY 3: Customer Credit Score Distribution ──────────────────────────────
-- Panel type: Bar Chart (credit_band on X, count on Y)

SELECT
    CASE
        WHEN credit_score >= 800 THEN '800+ Excellent'
        WHEN credit_score >= 750 THEN '750-799 Very Good'
        WHEN credit_score >= 700 THEN '700-749 Good'
        WHEN credit_score >= 650 THEN '650-699 Average'
        WHEN credit_score >= 600 THEN '600-649 Below Average'
        ELSE                          'Below 600 Poor'
    END                             AS credit_band,
    COUNT(*)                        AS customer_count,
    ROUND(AVG(annual_income), 0)    AS avg_income,
    ROUND(AVG(credit_score), 0)     AS avg_score
FROM ledgerflow_catalog.silver.customers
GROUP BY 1
ORDER BY avg_score DESC;


-- ─── QUERY 4: Daily EMI Collection Trend ─────────────────────────────────────
-- Panel type: Line Chart (txn_date on X, total_collected on Y)

SELECT
    DATE_ADD('1970-01-01', CAST(txn_date AS INT))   AS collection_date,
    payment_count,
    ROUND(total_collected, 2)                        AS total_collected,
    ROUND(avg_emi, 2)                               AS avg_emi
FROM ledgerflow_catalog.gold.daily_collections
ORDER BY collection_date DESC
LIMIT 30;


-- ─── QUERY 5: NPA Watchlist ───────────────────────────────────────────────────
-- Panel type: Table

SELECT
    loan_id,
    borrower_name,
    city,
    credit_score,
    loan_type,
    ROUND(loan_amount, 2)               AS loan_amount,
    ROUND(interest_rate, 2)             AS interest_rate,
    days_since_disbursement
FROM ledgerflow_catalog.gold.npa_watchlist
ORDER BY loan_amount DESC;


-- ─── QUERY 6: Risk Profile — Active Exposure by Credit Tier ──────────────────
-- Panel type: Pie or Donut Chart

SELECT
    credit_tier,
    customer_count,
    active_loans,
    ROUND(total_active_exposure, 2)     AS total_active_exposure,
    ROUND(avg_credit_score, 0)          AS avg_credit_score,
    ROUND(avg_annual_income, 0)         AS avg_annual_income
FROM ledgerflow_catalog.gold.customer_risk_profile
ORDER BY avg_credit_score DESC;
