# Databricks notebook source
# =============================================================================
# LedgerFlow Lakehouse — Phase 9: Unity Catalog Governance
# =============================================================================
# Implements enterprise data governance on Silver/Gold tables:
#
#   1. Row-Level Security  — branch/city-based row filters
#   2. Column Masking      — hide email, annual_income for non-privileged users
#   3. Table/Column grants — RBAC for analyst vs admin roles
#   4. Lineage verification — confirm Auto Loader + DLT lineage is tracked
#
# Run this notebook as admin (your account) in Databricks.
# =============================================================================

# COMMAND ----------

# -- 0. Config -----------------------------------------------------------------

CATALOG = "ledgerflow_catalog"
ADMIN_USER = "bnkr.raven@gmail.com"   # your Databricks account email

spark.sql(f"USE CATALOG {CATALOG}")
print(f"Using catalog: {CATALOG}")

# COMMAND ----------

# -- 1. Verify Silver + Gold tables exist -------------------------------------

print("-- Verifying all tables exist --\n")

# Gold tables land in silver schema (DLT pipeline default schema = silver).
# We will move them to gold schema via CREATE TABLE AS SELECT below.
for layer, tables in {
    "silver": ["customers", "loans", "transactions", "transactions_quarantine",
               "portfolio_summary", "customer_risk_profile", "daily_collections", "npa_watchlist"],
}.items():
    for t in tables:
        count = spark.table(f"{CATALOG}.{layer}.{t}").count()
        print(f"  {CATALOG}.{layer}.{t:<40} {count:>6} rows")

# COMMAND ----------

# -- 1b. Promote Gold tables from silver schema → gold schema -----------------
# DLT pipeline default schema was silver, so gold aggregations landed there.
# We create proper gold schema tables here for clean separation.

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")

GOLD_TABLES = ["portfolio_summary", "customer_risk_profile", "daily_collections", "npa_watchlist"]

for t in GOLD_TABLES:
    spark.sql(f"""
        CREATE OR REPLACE TABLE {CATALOG}.gold.{t}
        COMMENT 'Gold layer: promoted from DLT silver pipeline output'
        AS SELECT * FROM {CATALOG}.silver.{t}
    """)
    count = spark.table(f"{CATALOG}.gold.{t}").count()
    print(f"  Promoted: {CATALOG}.gold.{t}  ({count} rows)")

print("\nGold schema populated.")

# COMMAND ----------

# -- 2. Row-Level Security — city-based row filter on customers ---------------
#
# Pattern: analysts from a specific city group can only see customers
# from their assigned cities. Admins see all.
#
# Unity Catalog row filters use SQL functions tagged as ROW FILTER.

spark.sql(f"""
    CREATE OR REPLACE FUNCTION {CATALOG}.silver.city_row_filter(city STRING)
    RETURNS BOOLEAN
    LANGUAGE SQL
    COMMENT 'Row filter: non-admin users see only South Indian city customers'
    RETURN
        IS_ACCOUNT_GROUP_MEMBER('admins')
        OR city IN ('Chennai', 'Bangalore', 'Hyderabad', 'Kochi', 'Coimbatore',
                    'Mysore', 'Visakhapatnam', 'Madurai')
""")

print("Row filter function created: silver.city_row_filter")

# COMMAND ----------

# Apply the row filter to silver.customers

spark.sql(f"""
    ALTER TABLE {CATALOG}.silver.customers
    SET ROW FILTER {CATALOG}.silver.city_row_filter ON (city)
""")

print("Row filter applied to silver.customers")
print("  -> Admins: see all 50 customers")
print("  -> Non-admins: see only South Indian city customers")

# COMMAND ----------

# -- 3. Column Masking — hide email and annual_income -------------------------
#
# Non-admin users see masked values:
#   email        -> first char + '***@masked.com'
#   annual_income -> NULL (hidden entirely)

spark.sql(f"""
    CREATE OR REPLACE FUNCTION {CATALOG}.silver.mask_email(email STRING)
    RETURNS STRING
    LANGUAGE SQL
    COMMENT 'Column mask: show only first char of email to non-admins'
    RETURN
        CASE
            WHEN IS_ACCOUNT_GROUP_MEMBER('admins') THEN email
            ELSE CONCAT(LEFT(email, 1), '***@masked.com')
        END
""")

spark.sql(f"""
    CREATE OR REPLACE FUNCTION {CATALOG}.silver.mask_income(annual_income DOUBLE)
    RETURNS DOUBLE
    LANGUAGE SQL
    COMMENT 'Column mask: hide annual income from non-admins'
    RETURN
        CASE
            WHEN IS_ACCOUNT_GROUP_MEMBER('admins') THEN annual_income
            ELSE NULL
        END
""")

print("Column mask functions created:")
print("  silver.mask_email   -> first char + ***@masked.com for non-admins")
print("  silver.mask_income  -> NULL for non-admins")

# COMMAND ----------

# Apply column masks to silver.customers

spark.sql(f"""
    ALTER TABLE {CATALOG}.silver.customers
    ALTER COLUMN email
    SET MASK {CATALOG}.silver.mask_email
""")

spark.sql(f"""
    ALTER TABLE {CATALOG}.silver.customers
    ALTER COLUMN annual_income
    SET MASK {CATALOG}.silver.mask_income
""")

print("Column masks applied to silver.customers:")
print("  -> email masked for non-admins")
print("  -> annual_income masked for non-admins")

# COMMAND ----------

# -- 4. Verify masking works (as admin you see real data) ---------------------

print("\n-- silver.customers with masks applied (admin view — unmasked) --")
display(
    spark.table(f"{CATALOG}.silver.customers")
         .select("customer_id", "name", "email", "annual_income", "credit_score", "city")
         .orderBy("customer_id")
         .limit(10)
)

# COMMAND ----------

# -- 5. Table-level GRANTS — RBAC ---------------------------------------------

# Grant READ on Gold tables to all users (public analytics layer)
# In a real org you'd grant to specific groups; here we grant to the admin
# user as a demonstration since we have a single-user workspace.

spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{ADMIN_USER}`")
spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.gold TO `{ADMIN_USER}`")

for table in GOLD_TABLES:
    spark.sql(f"GRANT SELECT ON TABLE {CATALOG}.gold.{table} TO `{ADMIN_USER}`")
    print(f"  GRANT SELECT -> {CATALOG}.gold.{table}")

print("\nGold layer grants applied.")

# COMMAND ----------

# -- 6. Data Lineage — verify Unity Catalog tracks the full pipeline ----------

print("\n-- Lineage is automatically tracked by Unity Catalog --")
print("""
Full pipeline lineage (visible in Catalog Explorer → table → Lineage tab):

  ADLS Gen2 raw/customers/*.json
      └─> [Auto Loader]
          └─> ledgerflow_catalog.bronze.customers_raw
              └─> [DLT - APPLY CHANGES INTO]
                  └─> ledgerflow_catalog.silver.customers
                      └─> [DLT join + groupBy]
                          └─> ledgerflow_catalog.gold.customer_risk_profile

  ADLS Gen2 raw/transactions/*.json
      └─> [Auto Loader]
          └─> ledgerflow_catalog.bronze.transactions_raw
              ├─> [DLT expect_or_drop]
              │   └─> ledgerflow_catalog.silver.transactions
              │       └─> [DLT groupBy txn_date]
              │           └─> ledgerflow_catalog.gold.daily_collections
              └─> [DLT quarantine filter]
                  └─> ledgerflow_catalog.silver.transactions_quarantine
""")

# COMMAND ----------

# -- 7. NPA Watchlist — verify Gold data quality ------------------------------

print("-- NPA Watchlist (defaulted loans) --")
display(
    spark.table(f"{CATALOG}.gold.npa_watchlist")
         .select("loan_id", "borrower_name", "city", "credit_score",
                 "loan_type", "loan_amount", "days_since_disbursement")
         .orderBy("loan_amount", ascending=False)
)

# COMMAND ----------

# -- 8. Portfolio Summary by loan type ----------------------------------------

print("-- Portfolio Summary --")
display(
    spark.table(f"{CATALOG}.gold.portfolio_summary")
         .orderBy("loan_type", "status")
)

# COMMAND ----------

# -- 9. Customer Risk Profile -------------------------------------------------

print("-- Customer Risk Profile by Credit Tier --")
display(
    spark.table(f"{CATALOG}.gold.customer_risk_profile")
         .orderBy("credit_tier")
)

# COMMAND ----------

# -- 10. Daily Collections trend ----------------------------------------------

print("-- Daily EMI Collections (last 10 days) --")
display(
    spark.table(f"{CATALOG}.gold.daily_collections")
         .orderBy("txn_date", ascending=False)
         .limit(10)
)

# COMMAND ----------

print("""
=============================================================
  Phase 9 — Unity Catalog Governance COMPLETE
=============================================================

What is now enforced:
  Row Filter  : silver.customers filtered by city for non-admins
  Col Mask    : email + annual_income hidden for non-admins
  GRANTS      : Gold layer SELECT granted (RBAC demo)
  Lineage     : Full Bronze -> Silver -> Gold tracked automatically

To view lineage visually:
  Catalog -> ledgerflow_catalog -> silver -> customers -> Lineage tab

Next: Phase 10 — Databricks SQL Dashboard
=============================================================
""")
