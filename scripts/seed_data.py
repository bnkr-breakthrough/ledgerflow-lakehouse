"""
LedgerFlow — seed_data.py
Seeds PostgreSQL with realistic Indian banking data.

Design principles:
  - Names are region-specific (South Indian, North Indian, Maharashtrian, Bengali, Gujarati)
  - Credit scores follow a bell curve (most people 650–760)
  - Income is correlated with city cost of living
  - Loan amounts are correlated with loan type + city + income
  - EMI payments are mathematically calculated (not random amounts)
  - Transaction dates follow real monthly payment cycles (5th, 10th, 15th of month)
  - Email patterns mirror real usage (gmail dominant, some yahoo/hotmail/rediff)
  - Some customers have 2 loans (realistic for Indian middle class)
"""

import psycopg2
import random
import math
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
import os

load_dotenv()

random.seed(42)   # reproducible data

# ─── DB Connection ────────────────────────────────────────────────────────────
conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=int(os.getenv("POSTGRES_PORT", 5432)),
    dbname=os.getenv("POSTGRES_DB", "ledgerflowdb"),
    user=os.getenv("POSTGRES_USER", "ledgerflow"),
    password=os.getenv("POSTGRES_PASSWORD", "ledgerflow123")
)
cur = conn.cursor()

# ─── Region-specific Indian names ────────────────────────────────────────────
SOUTH_INDIAN = [
    ("Venkatesh", "Narasimhan"), ("Karthikeyan", "Subramaniam"), ("Aravind", "Krishnamurthy"),
    ("Deepak", "Raghunathan"), ("Srinivasan", "Iyengar"), ("Pradeep", "Ramamurthy"),
    ("Lakshmi", "Venkataraman"), ("Kavitha", "Rajagopalan"), ("Meenakshi", "Sundaram"),
    ("Anitha", "Krishnaswamy"), ("Divya", "Ramachandran"), ("Saranya", "Natarajan"),
    ("Harish", "Nambiar"), ("Unnikrishnan", "Pillai"), ("Sreeja", "Menon"),
    ("Ajith", "Kumar"), ("Priya", "Nair"), ("Renjith", "Varma"),
]

NORTH_INDIAN = [
    ("Rajesh", "Sharma"), ("Amit", "Mishra"), ("Rahul", "Gupta"),
    ("Vikram", "Singh"), ("Suresh", "Yadav"), ("Naveen", "Tiwari"),
    ("Ankita", "Srivastava"), ("Pooja", "Chauhan"), ("Nisha", "Pandey"),
    ("Sunita", "Agarwal"), ("Ritu", "Verma"), ("Sanjay", "Joshi"),
    ("Mukesh", "Rastogi"), ("Deepak", "Bajpai"), ("Shikha", "Saxena"),
    ("Arun", "Tripathi"), ("Neha", "Shukla"), ("Manish", "Dubey"),
]

MAHARASHTRIAN = [
    ("Sachin", "Patil"), ("Prasad", "Kulkarni"), ("Ganesh", "Deshmukh"),
    ("Sandeep", "Joshi"), ("Vinayak", "Deshpande"), ("Nikhil", "Pawar"),
    ("Vrinda", "Gokhale"), ("Madhuri", "Sathe"), ("Snehal", "Bhosale"),
    ("Rupali", "Mane"), ("Ashwini", "Kamble"), ("Vaishali", "Shirke"),
]

BENGALI = [
    ("Subhash", "Chatterjee"), ("Debashish", "Mukherjee"), ("Partha", "Banerjee"),
    ("Soumya", "Ghosh"), ("Arnab", "Bose"), ("Sayan", "Das"),
    ("Moumita", "Roy"), ("Sutapa", "Sen"), ("Priyanka", "Chakraborty"),
    ("Debarati", "Datta"), ("Srabani", "Pal"), ("Ruma", "Biswas"),
]

GUJARATI = [
    ("Jigar", "Patel"), ("Dhruv", "Shah"), ("Maulik", "Mehta"),
    ("Chirag", "Desai"), ("Bhavik", "Modi"), ("Ravi", "Thakkar"),
    ("Hiral", "Patel"), ("Foram", "Shah"), ("Komal", "Gandhi"),
    ("Bijal", "Trivedi"), ("Drashti", "Vyas"), ("Purvi", "Jani"),
]

ALL_NAMES = SOUTH_INDIAN + NORTH_INDIAN + MAHARASHTRIAN + BENGALI + GUJARATI

# ─── Cities with cost-of-living index (higher = more expensive) ───────────────
CITIES = [
    {"city": "Mumbai",     "state": "Maharashtra",  "col_index": 1.8, "region": "west"},
    {"city": "Bangalore",  "state": "Karnataka",    "col_index": 1.6, "region": "south"},
    {"city": "Delhi",      "state": "Delhi",        "col_index": 1.5, "region": "north"},
    {"city": "Hyderabad",  "state": "Telangana",    "col_index": 1.4, "region": "south"},
    {"city": "Chennai",    "state": "Tamil Nadu",   "col_index": 1.3, "region": "south"},
    {"city": "Pune",       "state": "Maharashtra",  "col_index": 1.3, "region": "west"},
    {"city": "Kolkata",    "state": "West Bengal",  "col_index": 1.1, "region": "east"},
    {"city": "Ahmedabad",  "state": "Gujarat",      "col_index": 1.1, "region": "west"},
    {"city": "Jaipur",     "state": "Rajasthan",    "col_index": 0.9, "region": "north"},
    {"city": "Kochi",      "state": "Kerala",       "col_index": 1.0, "region": "south"},
    {"city": "Lucknow",    "state": "Uttar Pradesh","col_index": 0.8, "region": "north"},
    {"city": "Nagpur",     "state": "Maharashtra",  "col_index": 0.9, "region": "west"},
]

EMAIL_DOMAINS = ["gmail.com"] * 55 + ["yahoo.com"] * 15 + \
                ["hotmail.com"] * 10 + ["rediffmail.com"] * 10 + \
                ["outlook.com"] * 10

# ─── Bell-curve credit score (realistic population distribution) ───────────────
def realistic_credit_score():
    """Most Indians cluster 650–760. Very few below 550 or above 820."""
    score = int(random.gauss(700, 55))
    return max(550, min(825, score))

# ─── Income correlated with city cost of living ───────────────────────────────
def realistic_income(col_index):
    base = random.gauss(900000, 350000)   # mean ₹9L, std ₹3.5L
    income = base * col_index
    return round(max(300000, min(5000000, income)), -3)   # round to nearest 1000

# ─── EMI calculation ─────────────────────────────────────────────────────────
def calculate_emi(principal, annual_rate, tenure_months):
    """Standard reducing balance EMI formula."""
    r = (annual_rate / 100) / 12
    if r == 0:
        return round(principal / tenure_months, 2)
    emi = principal * r * math.pow(1 + r, tenure_months) / (math.pow(1 + r, tenure_months) - 1)
    return round(emi, 2)

# ─── Email generator ─────────────────────────────────────────────────────────
def make_email(first, last):
    domain = random.choice(EMAIL_DOMAINS)
    style  = random.randint(1, 5)
    fn, ln = first.lower(), last.lower()
    if style == 1: return f"{fn}.{ln}@{domain}"
    if style == 2: return f"{fn}{ln[0]}@{domain}"
    if style == 3: return f"{fn[0]}{ln}@{domain}"
    if style == 4: return f"{fn}.{ln}{random.randint(1980,2000)}@{domain}"
    return f"{fn}{random.randint(10,99)}@{domain}"

# ─── Loan amount correlated with type + city + income ────────────────────────
def realistic_loan_amount(loan_type, col_index, annual_income):
    if loan_type == "HOME":
        # Home loan: typically 4–6x annual income, scaled by city
        base = annual_income * random.uniform(4.0, 6.5) * col_index
        return round(min(max(base, 1200000), 15000000), -4)   # ₹12L–₹1.5Cr

    elif loan_type == "PERSONAL":
        # Personal loan: 0.5–1.5x annual income
        base = annual_income * random.uniform(0.5, 1.5)
        return round(min(max(base, 80000), 1500000), -3)

    elif loan_type == "BUSINESS":
        # Business loan: 1–3x annual income
        base = annual_income * random.uniform(1.0, 3.0)
        return round(min(max(base, 500000), 8000000), -4)

    elif loan_type == "AUTO":
        # Auto loan: fixed bands (hatchback to SUV)
        return random.choice([450000, 650000, 850000, 1200000, 1500000, 1800000])

# ─── Interest rate correlated with credit score ───────────────────────────────
def realistic_interest_rate(loan_type, credit_score):
    base_rates = {
        "HOME":     {"excellent": 8.35, "good": 8.75, "average": 9.25, "poor": 10.50},
        "PERSONAL": {"excellent": 11.5, "good": 13.0, "average": 15.5, "poor": 18.0},
        "BUSINESS": {"excellent": 10.5, "good": 11.5, "average": 13.0, "poor": 15.0},
        "AUTO":     {"excellent": 8.50, "good": 9.25, "average": 10.0, "poor": 11.5},
    }
    if credit_score >= 780:   tier = "excellent"
    elif credit_score >= 720: tier = "good"
    elif credit_score >= 650: tier = "average"
    else:                     tier = "poor"

    base = base_rates[loan_type][tier]
    # Add small variation (±0.25%)
    return round(base + random.uniform(-0.25, 0.25), 2)

# ─── Natural tenure choices ───────────────────────────────────────────────────
TENURES = {
    "HOME":     [120, 180, 240, 300],   # 10, 15, 20, 25 years
    "PERSONAL": [12, 24, 36, 48, 60],
    "BUSINESS": [36, 48, 60, 84],
    "AUTO":     [36, 48, 60, 72],
}

# ─── EMI due dates (Indian banks commonly use 5th or 10th) ───────────────────
EMI_DUE_DAYS = [5, 10, 15]

# =============================================================================
# SEED CUSTOMERS
# =============================================================================
print("\n" + "="*55)
print("  LedgerFlow — Seeding Database")
print("="*55)

print("\n[1/3] Seeding customers...")
random.shuffle(ALL_NAMES)
customers = []

for i, (first, last) in enumerate(ALL_NAMES[:50], start=1):
    city_info   = random.choice(CITIES)
    customer_id = f"C{i:04d}"
    email       = make_email(first, last)
    credit_score  = realistic_credit_score()
    annual_income = realistic_income(city_info["col_index"])
    created_at    = datetime.now() - timedelta(days=random.randint(180, 900))
    phone         = f"+91 {random.randint(7000000000,9999999999)}"

    cur.execute("""
        INSERT INTO customers
            (customer_id, name, email, credit_score, city, state, annual_income, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (customer_id) DO NOTHING
    """, (
        customer_id,
        f"{first} {last}",
        email,
        credit_score,
        city_info["city"],
        city_info["state"],
        annual_income,
        created_at,
        created_at
    ))
    customers.append({
        "id": customer_id, "name": f"{first} {last}",
        "credit_score": credit_score, "income": annual_income,
        "city": city_info["city"], "col_index": city_info["col_index"],
        "created_at": created_at
    })

conn.commit()
print(f"  ✓ {len(customers)} customers inserted")

# =============================================================================
# SEED LOANS
# =============================================================================
print("\n[2/3] Seeding loans...")
loans = []
loan_counter = 1

# Some customers get 2 loans (realistic — HOME + PERSONAL is common)
double_loan_customers = set(random.sample(range(len(customers)), 20))

for i, cust in enumerate(customers):
    # Determine how many loans this customer gets
    num_loans = 2 if i in double_loan_customers else 1

    for _ in range(num_loans):
        loan_id   = f"L{loan_counter:05d}"
        loan_counter += 1

        # Loan type weighted by income (high income → more HOME loans)
        if cust["income"] > 1200000:
            weights = [0.45, 0.25, 0.20, 0.10]
        else:
            weights = [0.20, 0.45, 0.20, 0.15]
        loan_type = random.choices(["HOME","PERSONAL","BUSINESS","AUTO"], weights=weights)[0]

        loan_amount   = realistic_loan_amount(loan_type, cust["col_index"], cust["income"])
        interest_rate = realistic_interest_rate(loan_type, cust["credit_score"])
        tenure_months = random.choice(TENURES[loan_type])

        # Status weighted by credit score
        if cust["credit_score"] >= 720:
            status_weights = [0.05, 0.75, 0.05, 0.15]
        elif cust["credit_score"] >= 650:
            status_weights = [0.10, 0.65, 0.12, 0.13]
        else:
            status_weights = [0.15, 0.50, 0.25, 0.10]

        status     = random.choices(["pending","active","defaulted","closed"], weights=status_weights)[0]
        created_at = cust["created_at"] + timedelta(days=random.randint(30, 180))
        disbursed_at = created_at + timedelta(days=random.randint(3, 12)) if status != "pending" else None
        emi_amount = calculate_emi(loan_amount, interest_rate, tenure_months)
        emi_due_day = random.choice(EMI_DUE_DAYS)

        cur.execute("""
            INSERT INTO loans
                (loan_id, customer_id, loan_amount, loan_type, status,
                 interest_rate, tenure_months, disbursed_at, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (loan_id) DO NOTHING
        """, (
            loan_id, cust["id"], loan_amount, loan_type, status,
            interest_rate, tenure_months, disbursed_at, created_at, created_at
        ))
        loans.append({
            "id": loan_id, "customer_id": cust["id"],
            "status": status, "amount": loan_amount,
            "emi": emi_amount, "emi_due_day": emi_due_day,
            "disbursed_at": disbursed_at, "created_at": created_at,
            "tenure": tenure_months, "type": loan_type
        })

conn.commit()
print(f"  ✓ {len(loans)} loans inserted")

# =============================================================================
# SEED TRANSACTIONS
# =============================================================================
print("\n[3/3] Seeding transactions...")
txn_count = 0
active_loans = [l for l in loans if l["status"] in ("active", "closed", "defaulted") and l["disbursed_at"]]

for loan in active_loans:
    disbursed_at = loan["disbursed_at"]
    emi          = loan["emi"]
    emi_due_day  = loan["emi_due_day"]

    # 1. Disbursement transaction (always the first)
    txn_id = f"TXN{txn_count+1:07d}"
    cur.execute("""
        INSERT INTO transactions (txn_id, loan_id, amount, txn_type, txn_date, remarks, created_at)
        VALUES (%s, %s, %s, 'DISBURSEMENT', %s, %s, %s)
        ON CONFLICT (txn_id) DO NOTHING
    """, (
        txn_id, loan["id"], loan["amount"],
        disbursed_at.date(),
        f"Loan disbursement - {loan['type']} loan sanctioned",
        disbursed_at
    ))
    txn_count += 1

    # 2. Monthly EMI payments on the due date
    months_elapsed = min(
        random.randint(3, 18),
        loan["tenure"]
    )
    payment_month = disbursed_at + timedelta(days=32)

    for m in range(months_elapsed):
        txn_id = f"TXN{txn_count+1:07d}"

        # Determine payment date: emi_due_day of each month
        try:
            pay_date = payment_month.replace(day=emi_due_day)
        except ValueError:
            pay_date = payment_month.replace(day=28)

        # Defaulted loans: miss some payments + penalties
        if loan["status"] == "defaulted":
            if random.random() < 0.40:   # 40% of payments missed
                # Insert penalty instead
                if random.random() < 0.6:
                    penalty_id = f"TXN{txn_count+1:07d}"
                    penalty_amt = round(emi * random.uniform(0.02, 0.04), 2)
                    cur.execute("""
                        INSERT INTO transactions (txn_id, loan_id, amount, txn_type, txn_date, remarks, created_at)
                        VALUES (%s, %s, %s, 'PENALTY', %s, %s, %s)
                        ON CONFLICT (txn_id) DO NOTHING
                    """, (
                        penalty_id, loan["id"], penalty_amt, pay_date,
                        f"Late payment penalty - month {m+1}",
                        pay_date
                    ))
                    txn_count += 1
                payment_month += timedelta(days=32)
                continue

        # Normal payment (sometimes slightly early, sometimes 1-2 days late)
        actual_pay_date = pay_date + timedelta(days=random.randint(-2, 3))
        remarks = f"EMI payment - month {m+1} of {loan['tenure']}"

        cur.execute("""
            INSERT INTO transactions (txn_id, loan_id, amount, txn_type, txn_date, remarks, created_at)
            VALUES (%s, %s, %s, 'PAYMENT', %s, %s, %s)
            ON CONFLICT (txn_id) DO NOTHING
        """, (
            txn_id, loan["id"], emi, actual_pay_date, remarks, actual_pay_date
        ))
        txn_count += 1
        payment_month += timedelta(days=32)

    # 3. Foreclosure transaction for closed loans
    if loan["status"] == "closed":
        txn_id = f"TXN{txn_count+1:07d}"
        # Remaining principal (approximate)
        remaining = round(loan["amount"] * random.uniform(0.05, 0.30), 2)
        close_date = payment_month.date()
        cur.execute("""
            INSERT INTO transactions (txn_id, loan_id, amount, txn_type, txn_date, remarks, created_at)
            VALUES (%s, %s, %s, 'FORECLOSURE', %s, %s, %s)
            ON CONFLICT (txn_id) DO NOTHING
        """, (
            txn_id, loan["id"], remaining, close_date,
            "Full and final settlement - loan closed",
            payment_month
        ))
        txn_count += 1

conn.commit()
print(f"  ✓ {txn_count} transactions inserted")

# ─── Summary ─────────────────────────────────────────────────────────────────
print("\n" + "="*55)
cur.execute("SELECT COUNT(*) FROM customers"); print(f"  customers    : {cur.fetchone()[0]:>5} rows")
cur.execute("SELECT COUNT(*) FROM loans");     print(f"  loans        : {cur.fetchone()[0]:>5} rows")
cur.execute("SELECT COUNT(*) FROM transactions"); print(f"  transactions : {cur.fetchone()[0]:>5} rows")

cur.execute("SELECT status, COUNT(*) FROM loans GROUP BY status ORDER BY status")
print("\n  Loan status breakdown:")
for row in cur.fetchall():
    print(f"    {row[0]:<12}: {row[1]} loans")

cur.execute("SELECT txn_type, COUNT(*) FROM transactions GROUP BY txn_type ORDER BY txn_type")
print("\n  Transaction type breakdown:")
for row in cur.fetchall():
    print(f"    {row[0]:<15}: {row[1]} transactions")

print("="*55)
cur.close()
conn.close()
print("\n✅ Seed complete — data looks like a real bank.\n")
