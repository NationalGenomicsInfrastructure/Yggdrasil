"""
Unit tests for is_plan_eligible() pure function.

Comprehensive coverage of all eligibility scenarios including:
- Status-based filtering
- Token comparison logic
- Edge cases: missing fields are never defaulted
- Tokens are never coerced: only JSON integers in range count
"""

import unittest

from yggdrasil.contract.controls import MAX_TOKEN, token_pair
from yggdrasil.contract.diagnostics import MAX_LENGTH, describe_recorded
from yggdrasil.storage.plan_eligibility import get_eligibility_reason, is_plan_eligible

# Stands for a token removed from the document.
ABSENT = object()


def _doc(run_token=1, executed_run_token=0, status="approved") -> dict:
    """An eligibility document; ABSENT removes a field."""
    fields = {
        "status": status,
        "run_token": run_token,
        "executed_run_token": executed_run_token,
    }
    return {key: value for key, value in fields.items() if value is not ABSENT}


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
    # Missing Fields Are Never Defaulted
    # ==========================================

    def test_missing_run_token_not_eligible(self):
        """Test: missing run_token is invalid, never 0 → not eligible."""
        doc = {
            "status": "approved",
            # run_token missing
            "executed_run_token": -1,
        }
        self.assertFalse(is_plan_eligible(doc))

    def test_missing_executed_token_not_eligible(self):
        """Test: missing executed_run_token is invalid, never -1 → not eligible."""
        doc = {
            "status": "approved",
            "run_token": 0,
            # executed_run_token missing
        }
        self.assertFalse(is_plan_eligible(doc))

    def test_missing_both_tokens_not_eligible(self):
        """Test: both tokens missing → not eligible."""
        doc = {
            "status": "approved",
        }
        self.assertFalse(is_plan_eligible(doc))

    def test_missing_status_not_eligible(self):
        """Test: missing status is not approved → not eligible."""
        doc = {
            "run_token": 0,
            "executed_run_token": -1,
        }
        self.assertFalse(is_plan_eligible(doc))

    def test_empty_doc_not_eligible(self):
        """Test: empty document → no status and no tokens → not eligible."""
        doc = {}
        self.assertFalse(is_plan_eligible(doc))

    # ==========================================
    # Tokens Are Never Coerced
    # ==========================================

    def test_string_tokens_not_eligible(self):
        """Test: string tokens are not JSON integers → not eligible."""
        doc = {
            "status": "approved",
            "run_token": "1",
            "executed_run_token": "0",
        }
        self.assertFalse(is_plan_eligible(doc))

    def test_float_tokens_not_eligible(self):
        """Test: float tokens are not JSON integers, even whole ones → not eligible."""
        for run_token, executed_run_token in ((1.9, 0.1), (1.0, 0.0)):
            with self.subTest(run_token=run_token):
                doc = {
                    "status": "approved",
                    "run_token": run_token,
                    "executed_run_token": executed_run_token,
                }
                self.assertFalse(is_plan_eligible(doc))

    def test_boolean_tokens_not_eligible(self):
        """Test: booleans are not JSON integers, though Python's bool is an int."""
        doc = {
            "status": "approved",
            "run_token": True,
            "executed_run_token": False,
        }
        self.assertFalse(is_plan_eligible(doc))

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
        """Test: None run_token → not eligible (null is not a token)."""
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

    def test_both_negative_tokens_not_eligible(self):
        """Test: run_token=-1, executed=-2 → both below their minimum → not eligible."""
        doc = {
            "status": "approved",
            "run_token": -1,
            "executed_run_token": -2,
        }
        self.assertFalse(is_plan_eligible(doc))

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

    def test_tokens_at_the_maximum(self):
        """Test: tokens up to 2**53 - 1 count; above it they are invalid."""
        self.assertTrue(is_plan_eligible(_doc(MAX_TOKEN, MAX_TOKEN - 1)))
        self.assertFalse(is_plan_eligible(_doc(MAX_TOKEN, MAX_TOKEN)))
        self.assertFalse(is_plan_eligible(_doc(MAX_TOKEN + 1, MAX_TOKEN)))
        self.assertFalse(is_plan_eligible(_doc(MAX_TOKEN + 1, 0)))


class TestStrictTokenRule(unittest.TestCase):
    """Eligibility reads tokens only through the contract's token rule."""

    # (run_token, executed_run_token): every way a pair can be valid or not.
    PAIRS = [
        (0, -1),
        (1, 0),
        (5, 4),
        (5, 5),
        (MAX_TOKEN, -1),
        (MAX_TOKEN, MAX_TOKEN),
        (ABSENT, -1),
        (5, ABSENT),
        (ABSENT, ABSENT),
        (None, -1),
        (5, None),
        ("5", 4),
        (5, "4"),
        (5.0, 4),
        (5, 4.0),
        (True, 0),
        (1, False),
        (-1, -1),
        (5, -2),
        (MAX_TOKEN + 1, 0),
        (5, MAX_TOKEN + 1),
        (4, 5),
        ([5], 4),
        ({"value": 5}, 4),
        (10**5000, 4),
    ]

    def label(self, value) -> str:
        """A subTest label for a token, short whatever the token holds."""
        return "absent" if value is ABSENT else describe_recorded(value)

    def test_eligibility_follows_the_token_rule_for_every_pair(self):
        for run, executed in self.PAIRS:
            with self.subTest(run_token=self.label(run), executed=self.label(executed)):
                doc = _doc(run, executed)
                pair = token_pair(doc)
                expected = pair.valid and pair.run > pair.executed
                self.assertEqual(is_plan_eligible(doc), expected)

    def test_the_reason_agrees_with_the_decision_for_every_pair(self):
        for run, executed in self.PAIRS:
            with self.subTest(run_token=self.label(run), executed=self.label(executed)):
                doc = _doc(run, executed)
                pair = token_pair(doc)
                reason = get_eligibility_reason(doc)
                if not pair.valid:
                    self.assertTrue(
                        reason.startswith("Not eligible: invalid tokens"), reason
                    )
                    for problem in pair.problems:
                        self.assertIn(problem, reason)
                elif is_plan_eligible(doc):
                    self.assertTrue(reason.startswith("Eligible:"), reason)
                else:
                    self.assertIn("already executed", reason)

    def test_invalid_tokens_are_reported_before_the_status(self):
        reason = get_eligibility_reason(_doc("5", 4, status="draft"))
        self.assertEqual(
            reason,
            "Not eligible: invalid tokens "
            "(run_token '5' is a str, not a JSON integer)",
        )


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
        """Test reason for invalid tokens names every problem."""
        doc = {
            "status": "approved",
            "run_token": "invalid",
        }
        reason = get_eligibility_reason(doc)
        self.assertEqual(
            reason,
            "Not eligible: invalid tokens "
            "(run_token 'invalid' is a str, not a JSON integer; "
            "executed_run_token is missing)",
        )

    def test_missing_status_reason(self):
        """Test reason for a document without a status."""
        reason = get_eligibility_reason(_doc(status=ABSENT))
        self.assertEqual(reason, "Not eligible: status is missing (must be 'approved')")

    def test_status_that_cannot_be_shown_whole_is_still_reported(self):
        """Test the reason shows any stored status, bounded, without raising."""
        for case, status in (
            ("huge integer", 10**5000),
            ("long string", "x" * 100_000),
            ("nested", {"value": [10**5000]}),
            ("wide list", list(range(100_000))),
        ):
            with self.subTest(case=case):
                reason = get_eligibility_reason(_doc(status=status))
                self.assertTrue(reason.startswith("Not eligible: status "), reason)
                self.assertTrue(reason.endswith("(must be 'approved')"), reason)
                self.assertLess(len(reason), MAX_LENGTH + 60)
        self.assertIn(
            "<unrepresentable int>", get_eligibility_reason(_doc(status=10**5000))
        )


if __name__ == "__main__":
    unittest.main()
