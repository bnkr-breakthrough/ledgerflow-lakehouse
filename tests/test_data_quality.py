"""
test_data_quality.py
====================
Unit tests for LedgerFlow data quality rules.
Tests the same logic used in DLT Expectations so we can validate
rules without running the full pipeline.

Run:
  pytest tests/test_data_quality.py -v
"""

import pytest


# ─── Loan validation rules (mirrors DLT expect_all_or_drop) ──────────────────

def is_valid_loan_amount(amount):
    return amount is not None and amount > 0

def is_valid_interest_rate(rate):
    return rate is not None and 1 <= rate <= 30

def is_valid_tenure(months):
    return months is not None and 12 <= months <= 360

def is_valid_loan_type(loan_type):
    return loan_type in ("HOME", "PERSONAL", "BUSINESS", "AUTO")

def is_valid_status(status):
    return status in ("pending", "active", "defaulted", "closed")


# ─── Transaction validation rules ────────────────────────────────────────────

def is_valid_amount(amount):
    return amount is not None and amount > 0

def is_valid_txn_type(txn_type):
    return txn_type in ("DISBURSEMENT", "PAYMENT", "PENALTY", "FORECLOSURE")

def has_loan_id(loan_id):
    return loan_id is not None


# ─── Tests: Loans ─────────────────────────────────────────────────────────────

class TestLoanValidation:

    def test_valid_loan_amount(self):
        assert is_valid_loan_amount(500000) is True
        assert is_valid_loan_amount(1) is True

    def test_invalid_loan_amount_zero(self):
        assert is_valid_loan_amount(0) is False

    def test_invalid_loan_amount_negative(self):
        assert is_valid_loan_amount(-10000) is False

    def test_invalid_loan_amount_none(self):
        assert is_valid_loan_amount(None) is False

    def test_valid_interest_rate_boundaries(self):
        assert is_valid_interest_rate(1) is True
        assert is_valid_interest_rate(30) is True
        assert is_valid_interest_rate(12.5) is True

    def test_invalid_interest_rate_below(self):
        assert is_valid_interest_rate(0.5) is False

    def test_invalid_interest_rate_above(self):
        assert is_valid_interest_rate(35) is False

    def test_valid_tenure_boundaries(self):
        assert is_valid_tenure(12) is True
        assert is_valid_tenure(360) is True
        assert is_valid_tenure(60) is True

    def test_invalid_tenure_below(self):
        assert is_valid_tenure(6) is False

    def test_invalid_tenure_above(self):
        assert is_valid_tenure(480) is False

    def test_valid_loan_types(self):
        for t in ("HOME", "PERSONAL", "BUSINESS", "AUTO"):
            assert is_valid_loan_type(t) is True

    def test_invalid_loan_type(self):
        assert is_valid_loan_type("MORTGAGE") is False
        assert is_valid_loan_type("") is False
        assert is_valid_loan_type(None) is False

    def test_valid_statuses(self):
        for s in ("pending", "active", "defaulted", "closed"):
            assert is_valid_status(s) is True

    def test_invalid_status(self):
        assert is_valid_status("ACTIVE") is False   # case sensitive
        assert is_valid_status("written_off") is False
        assert is_valid_status(None) is False


# ─── Tests: Transactions ──────────────────────────────────────────────────────

class TestTransactionValidation:

    def test_valid_amount(self):
        assert is_valid_amount(15000.50) is True

    def test_invalid_amount_zero(self):
        assert is_valid_amount(0) is False

    def test_invalid_amount_negative(self):
        assert is_valid_amount(-500) is False

    def test_valid_txn_types(self):
        for t in ("DISBURSEMENT", "PAYMENT", "PENALTY", "FORECLOSURE"):
            assert is_valid_txn_type(t) is True

    def test_invalid_txn_type(self):
        assert is_valid_txn_type("REFUND") is False
        assert is_valid_txn_type("payment") is False  # case sensitive

    def test_has_loan_id_valid(self):
        assert has_loan_id("L00001") is True

    def test_has_loan_id_none(self):
        assert has_loan_id(None) is False


# ─── Tests: NPA rate calculation ──────────────────────────────────────────────

class TestNPACalculation:

    def test_npa_rate(self):
        total = 1000000
        defaulted = 112000
        npa_rate = round(defaulted / total * 100, 2)
        assert npa_rate == 11.2

    def test_npa_rate_zero(self):
        npa_rate = round(0 / 1000000 * 100, 2)
        assert npa_rate == 0.0

    def test_npa_rate_precision(self):
        # Mirrors the ROUND(..., 2) in the SQL query
        npa_rate = round(112345.67 / 1000000 * 100, 2)
        assert isinstance(npa_rate, float)
        assert len(str(npa_rate).split(".")[-1]) <= 2
