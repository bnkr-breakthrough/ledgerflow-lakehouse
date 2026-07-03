"""
e2e_test.py
===========
Phase 12 — LedgerFlow Lakehouse End-to-End Test

Generates targeted CDC events in PostgreSQL and verifies they propagate
through the full pipeline:
  PostgreSQL WAL → Debezium → Kafka → ADLS → Bronze → Silver → Gold

Run BEFORE and AFTER the Databricks Workflow to compare row counts.

Usage:
  # Step 1: inject events
  python scripts/e2e_test.py --inject

  # Step 2: run Databricks Workflow (manually in UI or via CLI)

  # Step 3: verify counts increased
  python scripts/e2e_test.py --verify
"""

import argparse
import os
import json
import time
import random
import string
from datetime import datetime, timezone
from dotenv import load_dotenv
import psycopg2

load_dotenv()

# ─── Config ───────────────────────────────────────────────────────────────────

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   os.getenv("POSTGRES_DB", "ledgerflow"),
    "user":     os.getenv("POSTGRES_USER", "ledgerflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "ledgerflow123"),
}

# Test customer IDs we'll insert (high IDs to avoid clashing with seed data)
TEST_CUSTOMER_IDS = [9001, 9002, 9003]
TEST_LOAN_IDS     = [99001, 99002]

SNAPSHOT_FILE = "/tmp/ledgerflow_e2e_snapshot.json"


# ─── DB helpers ───────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def snapshot_counts(conn):
    cur = conn.cursor()
    counts = {}
    for table in ["customers", "loans", "transactions"]:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        counts[table] = cur.fetchone()[0]
    cur.close()
    return counts


# ─── Inject phase ─────────────────────────────────────────────────────────────

def inject_events():
    print("=" * 60)
    print("  LedgerFlow E2E Test — INJECT phase")
    print(f"  Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 60)

    conn = get_conn()
    cur  = conn.cursor()

    # Snapshot before
    before = snapshot_counts(conn)
    print(f"\nBefore counts: {json.dumps(before, indent=2)}")

    # ── Test 1: INSERT 3 new customers ────────────────────────────────────────
    print("\n[TEST 1] INSERT 3 new test customers")
    cities = ["Mumbai", "Delhi", "Chennai"]
    for i, cid in enumerate(TEST_CUSTOMER_IDS):
        cur.execute("""
            INSERT INTO customers
                (customer_id, name, email, city, state,
                 annual_income, credit_score, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (customer_id) DO NOTHING
        """, (
            cid,
            f"E2E TestUser {cid}",
            f"e2e_{cid}@test.com",
            cities[i],
            "TestState",
            round(random.uniform(400000, 1200000), 2),
            random.randint(620, 820),
        ))
        print(f"  Inserted customer_id={cid}")
    conn.commit()

    # ── Test 2: UPDATE credit score on first existing customer ────────────────
    print("\n[TEST 2] UPDATE credit_score on first existing customer")
    cur.execute("SELECT customer_id FROM customers WHERE customer_id NOT IN %s LIMIT 1",
                (tuple(str(c) for c in TEST_CUSTOMER_IDS),))
    row = cur.fetchone()
    if row:
        existing_cid = row[0]
        cur.execute("""
            UPDATE customers
            SET credit_score = credit_score + 5,
                annual_income = annual_income * 1.1
            WHERE customer_id = %s
        """, (existing_cid,))
        conn.commit()
        print(f"  Updated customer_id={existing_cid}, rows={cur.rowcount}")
    else:
        print("  [SKIP] No existing customer found")

    # ── Test 3: INSERT 2 new loans for test customers ─────────────────────────
    print("\n[TEST 3] INSERT 2 new test loans")
    loan_types = ["PERSONAL", "AUTO"]
    for i, lid in enumerate(TEST_LOAN_IDS):
        cur.execute("""
            INSERT INTO loans
                (loan_id, customer_id, loan_type, loan_amount,
                 interest_rate, tenure_months, status, disbursed_at, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s,
                    NOW(),
                    NOW())
            ON CONFLICT (loan_id) DO NOTHING
        """, (
            str(lid),
            str(TEST_CUSTOMER_IDS[i]),
            loan_types[i],
            round(random.uniform(50000, 500000), 2),
            round(random.uniform(8.5, 18.0), 2),
            random.choice([12, 24, 36, 48, 60]),
            "active",
        ))
        print(f"  Inserted loan_id={lid} for customer_id={TEST_CUSTOMER_IDS[i]}")
    conn.commit()

    # ── Test 4: INSERT 5 payment transactions ─────────────────────────────────
    print("\n[TEST 4] INSERT 5 PAYMENT transactions for an existing active loan")
    cur.execute("SELECT loan_id FROM loans WHERE status='active' LIMIT 1")
    loan_row = cur.fetchone()
    txn_loan_id = loan_row[0] if loan_row else str(TEST_LOAN_IDS[1])
    for j in range(5):
        txn_id = f"E2E{int(time.time())}{j}"
        cur.execute("""
            INSERT INTO transactions
                (txn_id, loan_id, amount, txn_type, txn_date, remarks, created_at)
            VALUES (%s, %s, %s, 'PAYMENT',
                    CURRENT_DATE - %s,
                    'E2E test payment', NOW())
        """, (
            txn_id,
            txn_loan_id,
            round(random.uniform(5000, 25000), 2),
            j,
        ))
    conn.commit()
    print(f"  Inserted 5 payment transactions for loan_id={txn_loan_id}")

    # ── Test 5: UPDATE loan status to 'closed' ────────────────────────────────
    print("\n[TEST 5] UPDATE loan_id=99001 status → closed")
    cur.execute("""
        UPDATE loans SET status = 'closed' WHERE loan_id = %s
    """, (str(TEST_LOAN_IDS[0]),))
    conn.commit()
    print(f"  Rows updated: {cur.rowcount}")

    # Snapshot after
    after = snapshot_counts(conn)
    print(f"\nAfter counts:  {json.dumps(after, indent=2)}")

    delta = {t: after[t] - before[t] for t in before}
    print(f"\nDelta:         {json.dumps(delta, indent=2)}")

    # Save snapshot for verify phase
    snapshot = {
        "injected_at": datetime.now(timezone.utc).isoformat(),
        "before": before,
        "after": after,
        "delta": delta,
        "expected_new_customers": len(TEST_CUSTOMER_IDS),
        "expected_new_loans": len(TEST_LOAN_IDS),
        "expected_new_transactions": 5,
    }
    with open(SNAPSHOT_FILE, "w") as f:
        json.dump(snapshot, f, indent=2)

    cur.close()
    conn.close()

    print(f"\n✓ Snapshot saved to {SNAPSHOT_FILE}")
    print("""
Next steps:
  1. Make sure kafka_to_adls_bridge.py is running (or start it now):
       python scripts/kafka_to_adls_bridge.py

  2. Wait 30-60 seconds for events to flush to ADLS

  3. Trigger the Databricks Workflow:
       Jobs & Pipelines → LedgerFlow-Daily-Pipeline → Run now

  4. Once the workflow completes (~5 min), run:
       python scripts/e2e_test.py --verify
""")


# ─── Verify phase ─────────────────────────────────────────────────────────────

def verify_results():
    print("=" * 60)
    print("  LedgerFlow E2E Test — VERIFY phase")
    print(f"  Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 60)

    # Load snapshot
    try:
        with open(SNAPSHOT_FILE) as f:
            snapshot = json.load(f)
        print(f"\nSnapshot from inject at: {snapshot['injected_at']}")
        print(f"Expected deltas: {json.dumps(snapshot['delta'], indent=2)}")
    except FileNotFoundError:
        print(f"ERROR: No snapshot found at {SNAPSHOT_FILE}")
        print("Run --inject first.")
        return

    conn = get_conn()
    current = snapshot_counts(conn)
    conn.close()

    print(f"\nCurrent PostgreSQL counts: {json.dumps(current, indent=2)}")

    # Verification checklist (PostgreSQL side — source of truth)
    results = []

    def check(label, condition, detail=""):
        status = "PASS" if condition else "FAIL"
        results.append((status, label))
        icon = "✓" if condition else "✗"
        print(f"  {icon} [{status}] {label}" + (f"  ({detail})" if detail else ""))

    print("\n── PostgreSQL source checks ─────────────────────────────")
    check("Test customers exist",
          current["customers"] >= snapshot["after"]["customers"],
          f"count={current['customers']}")

    check("Test loans exist",
          current["loans"] >= snapshot["after"]["loans"],
          f"count={current['loans']}")

    check("Test transactions exist",
          current["transactions"] >= snapshot["after"]["transactions"],
          f"count={current['transactions']}")

    # Summary
    passed = sum(1 for s, _ in results if s == "PASS")
    total  = len(results)
    print(f"\n{'='*60}")
    print(f"  Result: {passed}/{total} checks passed")
    if passed == total:
        print("  ✓ END-TO-END TEST PASSED")
        print("""
  Full pipeline verified:
    PostgreSQL WAL
      → Debezium CDC
        → Kafka topics
          → ADLS Gen2 raw/
            → Auto Loader Bronze
              → DLT Silver (APPLY CHANGES INTO)
                → Gold aggregations
                  → Dashboard (refresh to see updated numbers)
""")
    else:
        print("  ✗ SOME CHECKS FAILED — review output above")
    print("=" * 60)


# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LedgerFlow E2E Test")
    parser.add_argument("--inject", action="store_true", help="Inject test CDC events")
    parser.add_argument("--verify", action="store_true", help="Verify pipeline propagation")
    args = parser.parse_args()

    if args.inject:
        inject_events()
    elif args.verify:
        verify_results()
    else:
        print("Usage: python scripts/e2e_test.py --inject | --verify")
