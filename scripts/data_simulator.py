"""
LedgerFlow — data_simulator.py
Simulates live banking activity in PostgreSQL.

Real-world patterns this simulator follows:
  - Loan approvals happen during business hours (9 AM – 6 PM)
  - EMI payments cluster around the 5th and 10th of the month
  - Defaults follow low credit score + missed payment pattern
  - New customers come in waves (not uniform random)
  - Loan amounts and rates match the seed data logic
  - New customer names are region-specific Indian names
  - Remarks are written like real bank records
"""

import psycopg2
import random
import math
import time
import uuid
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
import os

load_dotenv()

# ─── Connection ───────────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "ledgerflowdb"),
        user=os.getenv("POSTGRES_USER", "ledgerflow"),
        password=os.getenv("POSTGRES_PASSWORD", "ledgerflow123")
    )

# ─── Name pools (real Indian names by region) ─────────────────────────────────
NEW_CUSTOMERS = [
    ("Aarav", "Joshi"), ("Ishaan", "Sharma"), ("Vivaan", "Patel"),
    ("Aditya", "Verma"), ("Vihaan", "Singh"), ("Arjun", "Mishra"),
    ("Reyansh", "Gupta"), ("Muhammad", "Khan"), ("Ayaan", "Siddiqui"),
    ("Atharva", "Kulkarni"), ("Dhruv", "Desai"), ("Kabir", "Mehta"),
    ("Ananya", "Nair"), ("Diya", "Menon"), ("Saanvi", "Pillai"),
    ("Aanya", "Krishnan"), ("Aadhya", "Rao"), ("Pari", "Iyer"),
    ("Siya", "Reddy"), ("Myra", "Subramaniam"), ("Riya", "Shah"),
    ("Ira", "Chowdhury"), ("Sara", "Mukherjee"), ("Ritika", "Das"),
    ("Nisha", "Agarwal"), ("Rohit", "Tiwari"), ("Nikhil", "Pawar"),
    ("Saurabh", "Sawant"), ("Tejas", "Naik"), ("Shreyas", "Kamat"),
    ("Mithun", "Bose"), ("Sourav", "Ghosh"), ("Tanmoy", "Banerjee"),
    ("Pritam", "Dutta"), ("Subhojit", "Chatterjee"), ("Kiran", "Hegde"),
    ("Suhas", "Nayak"), ("Mohan", "Shetty"), ("Ramesh", "Kamath"),
    ("Girish", "Bhat"),
]

EMAIL_DOMAINS = (["gmail.com"] * 55 + ["yahoo.com"] * 15 +
                 ["hotmail.com"] * 10 + ["rediffmail.com"] * 10 +
                 ["outlook.com"] * 10)

CITIES = [
    {"city": "Mumbai",    "state": "Maharashtra",   "col_index": 1.8},
    {"city": "Bangalore", "state": "Karnataka",     "col_index": 1.6},
    {"city": "Delhi",     "state": "Delhi",         "col_index": 1.5},
    {"city": "Hyderabad", "state": "Telangana",     "col_index": 1.4},
    {"city": "Chennai",   "state": "Tamil Nadu",    "col_index": 1.3},
    {"city": "Pune",      "state": "Maharashtra",   "col_index": 1.3},
    {"city": "Kolkata",   "state": "West Bengal",   "col_index": 1.1},
    {"city": "Ahmedabad", "state": "Gujarat",       "col_index": 1.1},
    {"city": "Kochi",     "state": "Kerala",        "col_index": 1.0},
    {"city": "Jaipur",    "state": "Rajasthan",     "col_index": 0.9},
]

LOAN_REMARKS = {
    "pending":   [
        "Application under review by credit team",
        "Awaiting income verification documents",
        "CIBIL check in progress",
        "Pending property valuation report",
    ],
    "active":    [
        "Loan approved and disbursed",
        "All KYC documents verified",
        "Loan sanctioned after credit committee approval",
    ],
    "defaulted": [
        "Account classified NPA — 90+ DPD",
        "Loan marked default after 3 consecutive missed EMIs",
        "Recovery proceedings initiated",
    ],
    "closed":    [
        "Loan fully repaid — NOC issued",
        "Account closed after foreclosure settlement",
        "Full and final settlement received",
    ],
}

EMI_PAYMENT_REMARKS = [
    "EMI received via NEFT",
    "EMI credited through auto-debit mandate",
    "EMI payment via UPI",
    "Monthly instalment received — NACH mandate",
    "EMI paid through mobile banking",
    "Standing instruction EMI credited",
]

PENALTY_REMARKS = [
    "Penal interest charged for delayed payment",
    "Late payment fee levied — EMI overdue 7 days",
    "Bounce charges applied — NACH return",
    "Penal charges for EMI default",
]

# ─── Helpers ──────────────────────────────────────────────────────────────────
def calculate_emi(principal, annual_rate, tenure_months):
    principal     = float(principal)
    annual_rate   = float(annual_rate)
    tenure_months = int(tenure_months)
    r = (annual_rate / 100) / 12
    if r == 0:
        return round(principal / tenure_months, 2)
    return round(principal * r * math.pow(1+r, tenure_months) / (math.pow(1+r, tenure_months) - 1), 2)

def make_email(first, last):
    domain = random.choice(EMAIL_DOMAINS)
    styles = [
        f"{first.lower()}.{last.lower()}@{domain}",
        f"{first.lower()}{last.lower()[0]}@{domain}",
        f"{first.lower()[0]}{last.lower()}@{domain}",
        f"{first.lower()}.{last.lower()}{random.randint(1985,2002)}@{domain}",
        f"{first.lower()}{random.randint(10,99)}@{domain}",
    ]
    return random.choice(styles)

def is_business_hours():
    now = datetime.now()
    return 9 <= now.hour < 18 and now.weekday() < 5   # Mon–Fri, 9–6

def near_emi_date():
    """True if today is within 3 days of 5th or 10th of month."""
    day = date.today().day
    return day in range(3, 8) or day in range(8, 13)

# ─── Action 1: Pending → Active (loan approval) ───────────────────────────────
def approve_pending_loans(conn):
    """Approvals happen during business hours only."""
    if not is_business_hours():
        return
    cur = conn.cursor()
    cur.execute("""
        SELECT l.loan_id, l.loan_amount, l.interest_rate, l.tenure_months,
               c.credit_score, c.name
        FROM loans l
        JOIN customers c ON l.customer_id = c.customer_id
        WHERE l.status = 'pending'
        ORDER BY RANDOM() LIMIT 1
    """)
    row = cur.fetchone()
    if row and random.random() < 0.70:   # 70% approval rate
        loan_id, amount, rate, tenure, score, name = row
        # Higher credit score = faster approval
        remark = random.choice(LOAN_REMARKS["active"])
        cur.execute("""
            UPDATE loans
            SET status = 'active',
                disbursed_at = NOW(),
                updated_at = NOW()
            WHERE loan_id = %s
        """, (loan_id,))

        # Auto-insert disbursement transaction
        txn_id = "TXN" + uuid.uuid4().hex[:9].upper()
        emi    = calculate_emi(amount, rate, tenure)
        cur.execute("""
            INSERT INTO transactions (txn_id, loan_id, amount, txn_type, txn_date, remarks, created_at)
            VALUES (%s, %s, %s, 'DISBURSEMENT', CURRENT_DATE, %s, NOW())
        """, (txn_id, loan_id, amount,
              f"Loan disbursement sanctioned — {remark}"))
        conn.commit()
        print(f"  [APPROVED] {loan_id} → active | Customer: {name} | ₹{amount:,.0f}")
    cur.close()

# ─── Action 2: EMI payment ────────────────────────────────────────────────────
def process_emi_payment(conn):
    """
    EMI payments are more likely near the 5th and 10th of the month.
    Payment amount = calculated EMI (not random).
    """
    probability = 0.85 if near_emi_date() else 0.25
    if random.random() > probability:
        return

    cur = conn.cursor()
    cur.execute("""
        SELECT l.loan_id, l.loan_amount, l.interest_rate, l.tenure_months,
               c.name, c.credit_score
        FROM loans l
        JOIN customers c ON l.customer_id = c.customer_id
        WHERE l.status = 'active'
        ORDER BY RANDOM() LIMIT 1
    """)
    row = cur.fetchone()
    if not row:
        cur.close()
        return

    loan_id, amount, rate, tenure, name, score = row
    emi    = calculate_emi(amount, rate, tenure)
    txn_id = "TXN" + uuid.uuid4().hex[:9].upper()
    remark = random.choice(EMI_PAYMENT_REMARKS)

    # Low credit score customers: 20% chance of penalty instead
    if score < 650 and random.random() < 0.20:
        penalty = round(emi * random.uniform(0.02, 0.035), 2)
        cur.execute("""
            INSERT INTO transactions (txn_id, loan_id, amount, txn_type, txn_date, remarks, created_at)
            VALUES (%s, %s, %s, 'PENALTY', CURRENT_DATE, %s, NOW())
        """, (txn_id, loan_id, penalty, random.choice(PENALTY_REMARKS)))
        conn.commit()
        print(f"  [PENALTY]  {loan_id} | Customer: {name} | ₹{penalty:,.2f} | Score: {score}")
    else:
        cur.execute("""
            INSERT INTO transactions (txn_id, loan_id, amount, txn_type, txn_date, remarks, created_at)
            VALUES (%s, %s, %s, 'PAYMENT', CURRENT_DATE, %s, NOW())
        """, (txn_id, loan_id, emi, remark))
        conn.commit()
        print(f"  [PAYMENT]  {loan_id} | Customer: {name} | EMI ₹{emi:,.2f}")
    cur.close()

# ─── Action 3: Active → Defaulted ────────────────────────────────────────────
def mark_defaults(conn):
    """Only low-credit customers with long-overdue loans get defaulted."""
    if random.random() > 0.15:   # 15% chance per cycle
        return
    cur = conn.cursor()
    cur.execute("""
        SELECT l.loan_id, c.name, c.credit_score
        FROM loans l
        JOIN customers c ON l.customer_id = c.customer_id
        WHERE l.status = 'active'
          AND c.credit_score < 640
          AND l.disbursed_at < NOW() - INTERVAL '90 days'
        ORDER BY c.credit_score ASC
        LIMIT 1
    """)
    row = cur.fetchone()
    if row:
        loan_id, name, score = row
        cur.execute("""
            UPDATE loans
            SET status = 'defaulted', updated_at = NOW()
            WHERE loan_id = %s
        """, (loan_id,))
        conn.commit()
        print(f"  [DEFAULT]  {loan_id} | Customer: {name} | Score: {score} → NPA")
    cur.close()

# ─── Action 4: Active → Closed (foreclosure) ─────────────────────────────────
def close_fully_repaid_loan(conn):
    """High credit score customers occasionally foreclose early."""
    if random.random() > 0.05:   # 5% chance per cycle
        return
    cur = conn.cursor()
    cur.execute("""
        SELECT l.loan_id, l.loan_amount, c.name, c.credit_score
        FROM loans l
        JOIN customers c ON l.customer_id = c.customer_id
        WHERE l.status = 'active'
          AND c.credit_score >= 750
          AND l.disbursed_at < NOW() - INTERVAL '365 days'
        ORDER BY RANDOM() LIMIT 1
    """)
    row = cur.fetchone()
    if row:
        loan_id, amount, name, score = row
        settlement = round(amount * random.uniform(0.05, 0.20), 2)
        txn_id = "TXN" + uuid.uuid4().hex[:9].upper()
        cur.execute("""
            UPDATE loans SET status = 'closed', updated_at = NOW()
            WHERE loan_id = %s
        """, (loan_id,))
        cur.execute("""
            INSERT INTO transactions (txn_id, loan_id, amount, txn_type, txn_date, remarks, created_at)
            VALUES (%s, %s, %s, 'FORECLOSURE', CURRENT_DATE,
                    'Full and final settlement — loan closed', NOW())
        """, (txn_id, loan_id, settlement))
        conn.commit()
        print(f"  [CLOSED]   {loan_id} | Customer: {name} | Settlement ₹{settlement:,.2f}")
    cur.close()

# ─── Action 5: New customer + loan application ───────────────────────────────
def new_customer_application(conn):
    """New applications come in during business hours."""
    if not is_business_hours():
        return
    if random.random() > 0.30:   # 30% chance per cycle
        return

    cur = conn.cursor()
    first, last = random.choice(NEW_CUSTOMERS)
    city_info   = random.choice(CITIES)

    customer_id   = "C" + uuid.uuid4().hex[:6].upper()
    email         = make_email(first, last)
    credit_score  = max(550, min(825, int(random.gauss(695, 60))))
    annual_income = round(max(300000, random.gauss(900000, 300000) * city_info["col_index"]), -3)
    name          = f"{first} {last}"

    cur.execute("""
        INSERT INTO customers
            (customer_id, name, email, credit_score, city, state, annual_income, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
    """, (customer_id, name, email, credit_score,
          city_info["city"], city_info["state"], annual_income))

    # Loan application
    loan_id   = "L" + uuid.uuid4().hex[:7].upper()
    if annual_income > 1200000:
        weights = [0.45, 0.25, 0.20, 0.10]
    else:
        weights = [0.20, 0.45, 0.20, 0.15]
    loan_type = random.choices(["HOME","PERSONAL","BUSINESS","AUTO"], weights=weights)[0]

    amounts = {"HOME":(1200000,12000000),"PERSONAL":(80000,1500000),
               "BUSINESS":(500000,8000000),"AUTO":(300000,1800000)}
    loan_amount = round(random.uniform(*amounts[loan_type]), -3)

    rates = {"HOME":(8.35,10.5),"PERSONAL":(11.5,18.0),
             "BUSINESS":(10.5,15.0),"AUTO":(8.5,11.5)}
    interest_rate = round(random.uniform(*rates[loan_type]), 2)
    tenure_months = random.choice({"HOME":[120,180,240],"PERSONAL":[12,24,36,60],
                                   "BUSINESS":[36,60,84],"AUTO":[36,48,60]}[loan_type])

    cur.execute("""
        INSERT INTO loans
            (loan_id, customer_id, loan_amount, loan_type, status,
             interest_rate, tenure_months, created_at, updated_at)
        VALUES (%s, %s, %s, %s, 'pending', %s, %s, NOW(), NOW())
    """, (loan_id, customer_id, loan_amount, loan_type, interest_rate, tenure_months))

    conn.commit()
    print(f"  [NEW APP]  {customer_id} | {name} | {city_info['city']} | "
          f"Score: {credit_score} | {loan_type} loan ₹{loan_amount:,.0f}")
    cur.close()

# ─── Update customer credit score ────────────────────────────────────────────
def update_credit_score(conn):
    """CIBIL scores change monthly. Simulate small drift."""
    if random.random() > 0.10:
        return
    cur = conn.cursor()
    cur.execute("SELECT customer_id, credit_score FROM customers ORDER BY RANDOM() LIMIT 3")
    rows = cur.fetchall()
    for cid, score in rows:
        delta = random.randint(-15, 20)   # slight upward bias (people repay)
        new_score = max(550, min(825, score + delta))
        if new_score != score:
            cur.execute("""
                UPDATE customers SET credit_score = %s, updated_at = NOW()
                WHERE customer_id = %s
            """, (new_score, cid))
    conn.commit()
    cur.close()

# ─── Main Loop ────────────────────────────────────────────────────────────────
def main():
    print("\n" + "="*60)
    print("  LedgerFlow Data Simulator — RUNNING")
    print(f"  Started: {datetime.now().strftime('%d %b %Y, %I:%M %p')}")
    print("  Press Ctrl+C to stop")
    print("="*60 + "\n")

    tick = 0
    while True:
        try:
            conn = get_conn()
            tick += 1
            now  = datetime.now()
            print(f"\n── Tick {tick:04d} | {now.strftime('%d %b %Y %H:%M:%S')} "
                  f"{'[BIZ HOURS]' if is_business_hours() else '[AFTER HRS]'} "
                  f"{'[EMI WEEK]' if near_emi_date() else ''} ──")

            # Every tick: try approving pending loans (biz hours only)
            approve_pending_loans(conn)

            # Every tick: EMI payments (high probability near EMI date)
            process_emi_payment(conn)

            # Every 3 ticks: check for defaults
            if tick % 3 == 0:
                mark_defaults(conn)

            # Every 5 ticks: check for loan closures
            if tick % 5 == 0:
                close_fully_repaid_loan(conn)

            # Every 4 ticks: new customer application
            if tick % 4 == 0:
                new_customer_application(conn)

            # Every 6 ticks: update credit scores
            if tick % 6 == 0:
                update_credit_score(conn)

            conn.close()
            time.sleep(15)   # 15-second cycles

        except KeyboardInterrupt:
            print(f"\n\n  Simulator stopped after {tick} ticks.")
            print(f"  Total runtime: ~{tick * 15 // 60} minutes")
            break
        except Exception as e:
            print(f"  [ERROR] {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
