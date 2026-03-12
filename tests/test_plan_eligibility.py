"""
Unit tests for is_plan_eligible() pure function.

Comprehensive coverage of all eligibility scenarios including:
- Status-based filtering
- Token comparison logic
- Edge cases and defaults
- Type coercion
"""

import unittest

from lib.core_utils.plan_eligibility import get_eligibility_reason, is_plan_eligible


class TestIsPlanEligible(unittest.TestCase):
    """Unit tests for is_plan_eligible function."""

    # ==========================================
    # Happy Path Tests
    # ==========================================

    def test_initial_run_eligible(self):
        """Test: status='approved', run_token=0, executed=-1 → eligible."""
        doc = {
            "status": "approved",
            "run_token": 0,
            "executed_run_token": -1,
        }
        self.assertTrue(is_plan_eligible(doc))

    def test_manual_rerun_eligible(self):
        """Test: status='approved', run_token=1, executed=0 → eligible."""
        doc = {
            "status": "approved",
            "run_token": 1,
            "executed_run_token": 0,
        }
        self.assertTrue(is_plan_eligible(doc))

    def test_second_rerun_eligible(self):
        """Test: status='approved', run_token=2, executed=1 → eligible."""
        doc = {
            "status": "approved",
            "run_token": 2,
            "executed_run_token": 1,
        }
        self.assertTrue(is_plan_eligible(doc))

    def test_large_token_gap_eligible(self):
        """Test: status='approved', run_token=100, executed=50 → eligible."""
        doc = {
            "status": "approved",
            "run_token": 100,
            "executed_run_token": 50,
        }
        self.assertTrue(is_plan_eligible(doc))

    # ==========================================
    # Not Eligible: Already Executed
    # ==========================================

    def test_already_executed_not_eligible(self):
        """Test: status='approved', run_token=0, executed=0 → not eligible."""
        doc = {
            "status": "approved",
            "run_token": 0,
            "executed_run_token": 0,
        }
        self.assertFalse(is_plan_eligible(doc))

    def test_executed_token_higher_not_eligible(self):
        """Test: status='approved', run_token=1, executed=2 → not eligible."""
        doc = {
            "status": "approved",
            "run_token": 1,
            "executed_run_token": 2,
        }
        self.assertFalse(is_plan_eligible(doc))

    def test_tokens_equal_not_eligible(self):
        """Test: status='approved', run_token=5, executed=5 → not eligible."""
        doc = {
            "status": "approved",
            "run_token": 5,
            "executed_run_token": 5,
        }
        self.assertFalse(is_plan_eligible(doc))

    # ==========================================
    # Not Eligible: Wrong Status
    # ==========================================

    def test_draft_status_not_eligible(self):
        """Test: status='draft' → not eligible (awaiting approval)."""
        doc = {
            "status": "draft",
            "run_token": 0,
            "executed_run_token": -1,
        }
        self.assertFalse(is_plan_eligible(doc))

    def test_rejected_status_not_eligible(self):
        """Test: status='rejected' → not eligible."""
        doc = {
            "status": "rejected",
            "run_token": 0,
            "executed_run_token": -1,
        }
        self.assertFalse(is_plan_eligible(doc))

    def test_expired_status_not_eligible(self):
        """Test: status='expired' → not eligible."""
        doc = {
            "status": "expired",
            "run_token": 0,
            "executed_run_token": -1,
        }
        self.assertFalse(is_plan_eligible(doc))

    def test_empty_status_not_eligible(self):
        """Test: status='' → not eligible."""
        doc = {
            "status": "",
            "run_token": 0,
            "executed_run_token": -1,
        }
        self.assertFalse(is_plan_eligible(doc))

    def test_case_sensitive_status_not_eligible(self):
        """Test: status='Approved' (capitalized) → not eligible."""
        doc = {
            "status": "Approved",
            "run_token": 0,
            "executed_run_token": -1,
        }
        self.assertFalse(is_plan_eligible(doc))

    def test_uppercase_status_not_eligible(self):
        """Test: status='APPROVED' → not eligible."""
        doc = {
            "status": "APPROVED",
            "run_token": 0,
            "executed_run_token": -1,
        }
        self.assertFalse(is_plan_eligible(doc))

    # ==========================================
    # Default Values (Missing Fields)
    # ==========================================

    def test_missing_run_token_defaults_to_zero(self):
        """Test: missing run_token defaults to 0 → eligible with executed=-1."""
        doc = {
            "status": "approved",
            # run_token missing → defaults to 0
            "executed_run_token": -1,
        }
        self.assertTrue(is_plan_eligible(doc))

    def test_missing_executed_token_defaults_to_minus_one(self):
        """Test: missing executed_run_token defaults to -1 → eligible."""
        doc = {
            "status": "approved",
            "run_token": 0,
            # executed_run_token missing → defaults to -1
        }
        self.assertTrue(is_plan_eligible(doc))

    def test_missing_both_tokens_eligible(self):
        """Test: both tokens missing → run_token=0 > executed=-1 → eligible."""
        doc = {
            "status": "approved",
        }
        self.assertTrue(is_plan_eligible(doc))

    def test_missing_status_defaults_to_draft(self):
        """Test: missing status defaults to 'draft' → not eligible."""
        doc = {
            "run_token": 0,
            "executed_run_token": -1,
        }
        self.assertFalse(is_plan_eligible(doc))

    def test_empty_doc_not_eligible(self):
        """Test: empty document → status defaults to 'draft' → not eligible."""
        doc = {}
        self.assertFalse(is_plan_eligible(doc))

    # ==========================================
    # Type Coercion Tests
    # ==========================================

    def test_string_tokens_coerced(self):
        """Test: string tokens are coerced to int."""
        doc = {
            "status": "approved",
            "run_token": "1",
            "executed_run_token": "0",
        }
        self.assertTrue(is_plan_eligible(doc))

    def test_float_tokens_coerced(self):
        """Test: float tokens are coerced to int."""
        doc = {
            "status": "approved",
            "run_token": 1.9,  # coerced to 1
            "executed_run_token": 0.1,  # coerced to 0
        }
        self.assertTrue(is_plan_eligible(doc))

    def test_invalid_run_token_not_eligible(self):
        """Test: invalid run_token (non-numeric) → not eligible."""
        doc = {
            "status": "approved",
            "run_token": "invalid",
            "executed_run_token": -1,
        }
        self.assertFalse(is_plan_eligible(doc))

    def test_invalid_executed_token_not_eligible(self):
        """Test: invalid executed_run_token (non-numeric) → not eligible."""
        doc = {
            "status": "approved",
            "run_token": 0,
            "executed_run_token": "invalid",
        }
        self.assertFalse(is_plan_eligible(doc))

    def test_none_run_token_not_eligible(self):
        """Test: None run_token → not eligible (can't coerce)."""
        doc = {
            "status": "approved",
            "run_token": None,
            "executed_run_token": -1,
        }
        self.assertFalse(is_plan_eligible(doc))

    def test_list_token_not_eligible(self):
        """Test: list as token → not eligible."""
        doc = {
            "status": "approved",
            "run_token": [0],
            "executed_run_token": -1,
        }
        self.assertFalse(is_plan_eligible(doc))

    # ==========================================
    # Edge Cases
    # ==========================================

    def test_negative_run_token_not_eligible(self):
        """Test: negative run_token with executed=-1 → not eligible."""
        doc = {
            "status": "approved",
            "run_token": -2,
            "executed_run_token": -1,
        }
        self.assertFalse(is_plan_eligible(doc))

    def test_both_negative_tokens_eligible(self):
        """Test: run_token=-1 > executed=-2 → eligible (weird but valid)."""
        doc = {
            "status": "approved",
            "run_token": -1,
            "executed_run_token": -2,
        }
        self.assertTrue(is_plan_eligible(doc))

    def test_very_large_tokens(self):
        """Test: very large token values work correctly."""
        doc = {
            "status": "approved",
            "run_token": 999999999,
            "executed_run_token": 999999998,
        }
        self.assertTrue(is_plan_eligible(doc))

    def test_zero_tokens_not_eligible(self):
        """Test: both tokens zero → not eligible."""
        doc = {
            "status": "approved",
            "run_token": 0,
            "executed_run_token": 0,
        }
        self.assertFalse(is_plan_eligible(doc))


class TestGetEligibilityReason(unittest.TestCase):
    """Unit tests for get_eligibility_reason function."""

    def test_eligible_reason(self):
        """Test reason for eligible plan."""
        doc = {
            "status": "approved",
            "run_token": 1,
            "executed_run_token": 0,
        }
        reason = get_eligibility_reason(doc)
        self.assertIn("Eligible", reason)
        self.assertIn("approved", reason)
        self.assertIn("run_token=1", reason)

    def test_not_eligible_status_reason(self):
        """Test reason for wrong status."""
        doc = {
            "status": "draft",
            "run_token": 0,
            "executed_run_token": -1,
        }
        reason = get_eligibility_reason(doc)
        self.assertIn("Not eligible", reason)
        self.assertIn("draft", reason)

    def test_not_eligible_tokens_reason(self):
        """Test reason for already executed."""
        doc = {
            "status": "approved",
            "run_token": 0,
            "executed_run_token": 0,
        }
        reason = get_eligibility_reason(doc)
        self.assertIn("Not eligible", reason)
        self.assertIn("already executed", reason)

    def test_invalid_token_reason(self):
        """Test reason for invalid tokens."""
        doc = {
            "status": "approved",
            "run_token": "invalid",
            "executed_run_token": -1,
        }
        reason = get_eligibility_reason(doc)
        self.assertIn("Invalid", reason)


if __name__ == "__main__":
    unittest.main()
