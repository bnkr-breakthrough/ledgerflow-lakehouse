# Databricks notebook source
# =============================================================================
# LedgerFlow Lakehouse — Phase 11: Workflow Orchestrator
# =============================================================================
# This notebook is used as a TASK inside a Databricks Workflow (Job).
# It can also be run standalone for ad-hoc pipeline execution.
#
# Pipeline order:
#   Task 1 (this notebook, cell group A) — Auto Loader Bronze
#   Task 2 (DLT pipeline task)           — Silver + Gold DLT
#   Task 3 (this notebook, cell group B) — Gold promotion + verification
#
# In the Databricks Workflow UI, create 3 tasks:
#   Task 1: Notebook task  → databricks/notebooks/01_auto_loader_bronze.py
#   Task 2: Pipeline task  → LedgerFlow-Silver-Gold  (depends on Task 1)
#   Task 3: Notebook task  → databricks/notebooks/03_unity_catalog_setup.py (depends on Task 2)
#
# This file (05_workflow_orchestrator.py) is an all-in-one alternative:
# run it as a single notebook task that calls the other notebooks in sequence.
# =============================================================================

# COMMAND ----------

# -- 0. Config -----------------------------------------------------------------

import time
from datetime import datetime, timezone

CATALOG         = "ledgerflow_catalog"
WORKSPACE_PATH  = "/Workspace/Users/bnkr.raven@gmail.com/LedgerFlow"   # adjust if different
DLT_PIPELINE_ID = ""    # fill in after creating the pipeline (Jobs & Pipelines → Pipelines → copy ID)

RUN_START = datetime.now(timezone.utc)
print(f"{'='*65}")
print(f"  LedgerFlow Pipeline Orchestrator")
print(f"  Started : {RUN_START.strftime('%Y-%m-%d %H:%M:%S UTC')}")
print(f"  Catalog : {CATALOG}")
print(f"{'='*65}\n")

# COMMAND ----------

# =============================================================================
# STEP 1 — Auto Loader Bronze
# =============================================================================
# Reads new CDC JSON files from ADLS raw/ and appends to bronze Delta tables.
# Uses trigger(availableNow=True) so it processes pending files then stops.

print("STEP 1: Auto Loader → Bronze")
print("-" * 40)

step1_start = time.time()

dbutils.notebook.run(
    "/Workspace/Users/bnkr.raven@gmail.com/LedgerFlow/01_auto_loader_bronze",
    timeout_seconds=600,   # 10-minute timeout
    arguments={},
)

step1_elapsed = round(time.time() - step1_start, 1)
print(f"\n✓ Step 1 complete in {step1_elapsed}s\n")

# COMMAND ----------

# =============================================================================
# STEP 2 — Trigger DLT Pipeline (Silver + Gold)
# =============================================================================
# Uses Databricks SDK to start a pipeline update and wait for completion.
# The DLT pipeline runs APPLY CHANGES INTO (CDC merge) and gold aggregations.

print("STEP 2: Trigger DLT Silver/Gold Pipeline")
print("-" * 40)

step2_start = time.time()

if not DLT_PIPELINE_ID:
    print("  [SKIP] DLT_PIPELINE_ID not set.")
    print("  Set DLT_PIPELINE_ID at the top of this notebook, or run the")
    print("  pipeline manually from Jobs & Pipelines → LedgerFlow-Silver-Gold.")
else:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.pipelines import StartCause

    w = WorkspaceClient()

    print(f"  Starting pipeline: {DLT_PIPELINE_ID}")
    update = w.pipelines.start_update(pipeline_id=DLT_PIPELINE_ID, full_refresh=False)
    update_id = update.update_id
    print(f"  Update ID: {update_id}")

    # Poll until terminal state
    terminal_states = {"COMPLETED", "FAILED", "CANCELED"}
    while True:
        state = w.pipelines.get_update(
            pipeline_id=DLT_PIPELINE_ID, update_id=update_id
        ).update.state.value
        elapsed = round(time.time() - step2_start)
        print(f"  Pipeline state: {state}  ({elapsed}s elapsed)")
        if state in terminal_states:
            break
        time.sleep(20)

    if state != "COMPLETED":
        raise Exception(f"DLT pipeline ended with state: {state}")

    step2_elapsed = round(time.time() - step2_start, 1)
    print(f"\n✓ Step 2 complete in {step2_elapsed}s\n")

# COMMAND ----------

# =============================================================================
# STEP 3 — Promote Gold tables + Verify row counts
# =============================================================================
# Re-promotes gold tables from silver schema (DLT lands everything in silver).
# Verifies all layers have expected data.

print("STEP 3: Gold Promotion + Verification")
print("-" * 40)

step3_start = time.time()

spark.sql(f"USE CATALOG {CATALOG}")

# Promote gold tables from silver → gold schema
GOLD_TABLES = ["portfolio_summary", "customer_risk_profile", "daily_collections", "npa_watchlist"]

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")

for t in GOLD_TABLES:
    spark.sql(f"""
        CREATE OR REPLACE TABLE {CATALOG}.gold.{t}
        COMMENT 'Gold layer: promoted from DLT silver pipeline output'
        AS SELECT * FROM {CATALOG}.silver.{t}
    """)
    count = spark.table(f"{CATALOG}.gold.{t}").count()
    print(f"  ✓ {CATALOG}.gold.{t:<35} {count:>6} rows")

step3_elapsed = round(time.time() - step3_start, 1)
print(f"\n✓ Step 3 complete in {step3_elapsed}s\n")

# COMMAND ----------

# =============================================================================
# STEP 4 — Full Layer Verification
# =============================================================================

print("STEP 4: Full Pipeline Verification")
print("-" * 40)

verification = {
    "bronze": ["customers_raw", "loans_raw", "transactions_raw"],
    "silver": ["customers", "loans", "transactions", "transactions_quarantine"],
    "gold":   ["portfolio_summary", "customer_risk_profile", "daily_collections", "npa_watchlist"],
}

all_ok = True
for layer, tables in verification.items():
    print(f"\n  [{layer.upper()}]")
    for t in tables:
        try:
            count = spark.table(f"{CATALOG}.{layer}.{t}").count()
            status = "✓" if count > 0 else "⚠ EMPTY"
            if count == 0:
                all_ok = False
            print(f"    {status}  {CATALOG}.{layer}.{t:<40} {count:>6} rows")
        except Exception as e:
            print(f"    ✗  {CATALOG}.{layer}.{t:<40} ERROR: {e}")
            all_ok = False

# COMMAND ----------

# =============================================================================
# STEP 5 — Pipeline Summary
# =============================================================================

RUN_END     = datetime.now(timezone.utc)
total_secs  = round((RUN_END - RUN_START).total_seconds())

print(f"\n{'='*65}")
print(f"  LedgerFlow Pipeline Run Summary")
print(f"{'='*65}")
print(f"  Started : {RUN_START.strftime('%Y-%m-%d %H:%M:%S UTC')}")
print(f"  Ended   : {RUN_END.strftime('%Y-%m-%d %H:%M:%S UTC')}")
print(f"  Duration: {total_secs}s  ({total_secs//60}m {total_secs%60}s)")
print(f"  Status  : {'✓ ALL CHECKS PASSED' if all_ok else '⚠ SOME CHECKS FAILED — review above'}")
print(f"{'='*65}")

if not all_ok:
    raise Exception("Pipeline verification failed — one or more tables are empty or missing.")
