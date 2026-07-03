# Databricks notebook source
# =============================================================================
# LedgerFlow Lakehouse — Phase 7 & 8: DLT Silver + Gold Pipeline
# =============================================================================
# This notebook runs as a Delta Live Tables (DLT) pipeline — NOT as a
# regular notebook. Create a DLT pipeline in Databricks UI pointing to
# this file.
#
# Silver tables (CDC-merged, validated):
#   ledgerflow_catalog.silver.customers
#   ledgerflow_catalog.silver.loans
#   ledgerflow_catalog.silver.transactions
#   ledgerflow_catalog.silver.transactions_quarantine
#
# Gold tables (business aggregations):
#   ledgerflow_catalog.gold.portfolio_summary
#   ledgerflow_catalog.gold.customer_risk_profile
#   ledgerflow_catalog.gold.daily_collections
#   ledgerflow_catalog.gold.npa_watchlist
# =============================================================================

import dlt
from pyspark.sql import functions as F
from pyspark.sql.functions import col, expr, when, round as spark_round

CATALOG = "ledgerflow_catalog"

# =============================================================================
# SILVER LAYER — CDC merge + data quality
# =============================================================================

# -----------------------------------------------------------------------------
# 1. Streaming source views from Bronze
#    These wrap the Bronze tables as DLT streaming sources.
# -----------------------------------------------------------------------------

@dlt.view(name="bronze_customers")
def bronze_customers():
    return spark.readStream.table(f"{CATALOG}.bronze.customers_raw")

@dlt.view(name="bronze_loans")
def bronze_loans():
    return spark.readStream.table(f"{CATALOG}.bronze.loans_raw")

@dlt.view(name="bronze_transactions")
def bronze_transactions():
    return spark.readStream.table(f"{CATALOG}.bronze.transactions_raw")

# -----------------------------------------------------------------------------
# 2. APPLY CHANGES INTO — customers
#    Merges CDC events into a clean SCD Type 1 Silver table.
#    op=r/c → upsert, op=d → delete
# -----------------------------------------------------------------------------

dlt.create_streaming_table(
    name="customers",
    comment="Silver: CDC-merged customer master. One row per customer_id.",
    table_properties={"quality": "silver"},
)

dlt.apply_changes(
    target="customers",
    source="bronze_customers",
    keys=["customer_id"],
    sequence_by="__ts_ms",                          # Debezium event timestamp
    apply_as_deletes=expr("__op = 'd'"),
    except_column_list=["_ingest_ts", "_source_file",
                        "__op", "__ts_ms", "__table", "__before",
                        "_kafka_offset", "_topic", "_bridge_ts",
                        "year", "month", "day", "hour"],
)

# -----------------------------------------------------------------------------
# 3. APPLY CHANGES INTO — loans (with DLT Expectations)
# -----------------------------------------------------------------------------

dlt.create_streaming_table(
    name="loans",
    comment="Silver: CDC-merged loans. Validated amounts and rates.",
    table_properties={"quality": "silver"},
    expect_all_or_drop={
        "valid_loan_amount":   "loan_amount > 0",
        "valid_interest_rate": "interest_rate BETWEEN 1 AND 30",
        "valid_tenure":        "tenure_months BETWEEN 12 AND 360",
        "valid_loan_type":     "loan_type IN ('HOME','PERSONAL','BUSINESS','AUTO')",
        "valid_status":        "status IN ('pending','active','defaulted','closed')",
    },
)

dlt.apply_changes(
    target="loans",
    source="bronze_loans",
    keys=["loan_id"],
    sequence_by="__ts_ms",
    apply_as_deletes=expr("__op = 'd'"),
    except_column_list=["_ingest_ts", "_source_file",
                        "__op", "__ts_ms", "__table", "__before",
                        "_kafka_offset", "_topic", "_bridge_ts",
                        "year", "month", "day", "hour"],
)

# -----------------------------------------------------------------------------
# 4. transactions — valid records (Silver)
# -----------------------------------------------------------------------------

@dlt.table(
    name="transactions",
    comment="Silver: validated transactions, bad records quarantined.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_amount",   "amount > 0")
@dlt.expect_or_drop("valid_txn_type", "txn_type IN ('DISBURSEMENT','PAYMENT','PENALTY','FORECLOSURE')")
@dlt.expect_or_drop("has_loan_id",    "loan_id IS NOT NULL")
def silver_transactions():
    return (
        spark.readStream.table(f"{CATALOG}.bronze.transactions_raw")
             .where(col("__op") != "d")             # ignore deletes
             .select(
                 "txn_id", "loan_id", "amount", "txn_type",
                 "txn_date", "remarks", "created_at",
                 "_ingest_ts", "_source_file",
             )
             .dropDuplicates(["txn_id"])
    )

# -----------------------------------------------------------------------------
# 5. transactions_quarantine — records that failed expectations
# -----------------------------------------------------------------------------

@dlt.table(
    name="transactions_quarantine",
    comment="Silver: transactions that failed data quality checks.",
    table_properties={"quality": "quarantine"},
)
def transactions_quarantine():
    return (
        spark.readStream.table(f"{CATALOG}.bronze.transactions_raw")
             .where(
                 (col("amount") <= 0) |
                 (~col("txn_type").isin("DISBURSEMENT","PAYMENT","PENALTY","FORECLOSURE")) |
                 col("loan_id").isNull()
             )
             .withColumn("_quarantine_reason",
                 when(col("amount") <= 0, "invalid_amount")
                 .when(~col("txn_type").isin("DISBURSEMENT","PAYMENT","PENALTY","FORECLOSURE"), "invalid_txn_type")
                 .otherwise("missing_loan_id")
             )
             .withColumn("_quarantined_at", F.current_timestamp())
    )

# =============================================================================
# GOLD LAYER — Business aggregations
# =============================================================================

# -----------------------------------------------------------------------------
# 6. portfolio_summary — total exposure by loan type and status
# -----------------------------------------------------------------------------

@dlt.table(
    name="portfolio_summary",
    comment="Gold: loan portfolio exposure by type and status.",
    table_properties={"quality": "gold"},
)
def gold_portfolio_summary():
    return (
        dlt.read("loans")
           .groupBy("loan_type", "status")
           .agg(
               F.count("loan_id").alias("loan_count"),
               spark_round(F.sum("loan_amount"), 2).alias("total_exposure"),
               spark_round(F.avg("loan_amount"), 2).alias("avg_loan_amount"),
               spark_round(F.avg("interest_rate"), 2).alias("avg_interest_rate"),
           )
           .orderBy("loan_type", "status")
    )

# -----------------------------------------------------------------------------
# 7. customer_risk_profile — credit tiers with loan exposure
# -----------------------------------------------------------------------------

@dlt.table(
    name="customer_risk_profile",
    comment="Gold: customer risk segmentation by credit score band.",
    table_properties={"quality": "gold"},
)
def gold_customer_risk_profile():
    customers = dlt.read("customers")
    loans     = dlt.read("loans")

    customers_with_tier = customers.withColumn(
        "credit_tier",
        when(col("credit_score") >= 750, "Excellent")
        .when(col("credit_score") >= 700, "Good")
        .when(col("credit_score") >= 650, "Average")
        .otherwise("Poor")
    )

    return (
        customers_with_tier.join(
            loans.where(col("status") == "active"),
            "customer_id", "left"
        )
        .groupBy("credit_tier")
        .agg(
            F.count("customer_id").alias("customer_count"),
            F.countDistinct("loan_id").alias("active_loans"),
            spark_round(F.sum("loan_amount"), 2).alias("total_active_exposure"),
            spark_round(F.avg("credit_score"), 0).alias("avg_credit_score"),
            spark_round(F.avg("annual_income"), 0).alias("avg_annual_income"),
        )
        .orderBy("credit_tier")
    )

# -----------------------------------------------------------------------------
# 8. daily_collections — EMI payment collections by date
# -----------------------------------------------------------------------------

@dlt.table(
    name="daily_collections",
    comment="Gold: daily EMI collection totals.",
    table_properties={"quality": "gold"},
)
def gold_daily_collections():
    return (
        dlt.read("transactions")
           .where(col("txn_type") == "PAYMENT")
           .groupBy("txn_date")
           .agg(
               F.count("txn_id").alias("payment_count"),
               spark_round(F.sum("amount"), 2).alias("total_collected"),
               spark_round(F.avg("amount"), 2).alias("avg_emi"),
           )
           .orderBy(col("txn_date").desc())
    )

# -----------------------------------------------------------------------------
# 9. npa_watchlist — defaulted loans with customer details
# -----------------------------------------------------------------------------

@dlt.table(
    name="npa_watchlist",
    comment="Gold: Non-Performing Assets — defaulted loans with borrower info.",
    table_properties={"quality": "gold"},
)
def gold_npa_watchlist():
    loans     = dlt.read("loans").where(col("status") == "defaulted")
    customers = dlt.read("customers")

    return (
        loans.join(customers, "customer_id")
             .select(
                 loans["loan_id"],
                 customers["customer_id"],
                 customers["name"].alias("borrower_name"),
                 customers["city"],
                 customers["credit_score"],
                 loans["loan_type"],
                 loans["loan_amount"],
                 loans["interest_rate"],
                 loans["tenure_months"],
                 loans["disbursed_at"],
                 F.datediff(F.current_date(), F.to_date(F.from_unixtime(col("disbursed_at") / 1000))).alias("days_since_disbursement"),
             )
             .orderBy(col("loan_amount").desc())
    )
