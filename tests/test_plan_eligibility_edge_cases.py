"""
Exhaustive edge case tests for plan eligibility logic.

These tests ensure 100% coverage of the is_plan_eligible() function
with boundary conditions, invalid inputs, and unusual but valid cases.
"""

import unittest
from typing import Any


class TestPlanEligibilityStatusEdgeCases(unittest.TestCase):
    """Tests for status field edge cases."""

    def test_status_approved_lowercase(self):
        """Test approved status (lowercase)."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {"status": "approved", "run_token": 1, "executed_run_token": 0}
        self.assertTrue(is_plan_eligible(doc))

    def test_status_approved_uppercase(self):
        """Test APPROVED status (uppercase) - should be case-sensitive."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {"status": "APPROVED", "run_token": 1, "executed_run_token": 0}
        # Implementation is case-sensitive; uppercase should NOT be eligible
        self.assertFalse(is_plan_eligible(doc))

    def test_status_draft(self):
        """Test draft status."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {"status": "draft", "run_token": 1, "executed_run_token": 0}
        self.assertFalse(is_plan_eligible(doc))

    def test_status_pending(self):
        """Test pending status (legacy, treated as draft)."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {"status": "pending", "run_token": 1, "executed_run_token": 0}
        self.assertFalse(is_plan_eligible(doc))

    def test_status_rejected(self):
        """Test rejected status."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {"status": "rejected", "run_token": 1, "executed_run_token": 0}
        self.assertFalse(is_plan_eligible(doc))

    def test_status_empty_string(self):
        """Test empty status string."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {"status": "", "run_token": 1, "executed_run_token": 0}
        self.assertFalse(is_plan_eligible(doc))

    def test_status_missing(self):
        """Test missing status field."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {"run_token": 1, "executed_run_token": 0}
        self.assertFalse(is_plan_eligible(doc))

    def test_status_none(self):
        """Test None status."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {"status": None, "run_token": 1, "executed_run_token": 0}
        self.assertFalse(is_plan_eligible(doc))

    def test_status_whitespace(self):
        """Test whitespace-only status."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {"status": "  ", "run_token": 1, "executed_run_token": 0}
        self.assertFalse(is_plan_eligible(doc))

    def test_status_unknown_value(self):
        """Test unknown status value."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {"status": "processing", "run_token": 1, "executed_run_token": 0}
        self.assertFalse(is_plan_eligible(doc))


class TestPlanEligibilityTokenEdgeCases(unittest.TestCase):
    """Tests for run_token and executed_run_token edge cases."""

    def test_tokens_equal_zero(self):
        """Test both tokens at zero (never run, never requested)."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {"status": "approved", "run_token": 0, "executed_run_token": 0}
        # Tokens equal = not eligible (already run or never started)
        self.assertFalse(is_plan_eligible(doc))

    def test_tokens_equal_one(self):
        """Test both tokens at 1 (run once)."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {"status": "approved", "run_token": 1, "executed_run_token": 1}
        self.assertFalse(is_plan_eligible(doc))

    def test_tokens_equal_large(self):
        """Test both tokens at large value."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {"status": "approved", "run_token": 999, "executed_run_token": 999}
        self.assertFalse(is_plan_eligible(doc))

    def test_run_token_greater_by_one(self):
        """Test run_token exactly one greater than executed_run_token."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {"status": "approved", "run_token": 1, "executed_run_token": 0}
        self.assertTrue(is_plan_eligible(doc))

    def test_run_token_greater_by_many(self):
        """Test run_token much greater than executed_run_token."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {"status": "approved", "run_token": 10, "executed_run_token": 0}
        self.assertTrue(is_plan_eligible(doc))

    def test_run_token_missing(self):
        """Test missing run_token field."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {"status": "approved", "executed_run_token": 0}
        # Missing run_token should default to 0 or be ineligible
        self.assertFalse(is_plan_eligible(doc))

    def test_executed_run_token_missing(self):
        """Test missing executed_run_token field."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {"status": "approved", "run_token": 1}
        # Missing executed_run_token should default to 0, making plan eligible
        self.assertTrue(is_plan_eligible(doc))

    def test_both_tokens_missing(self):
        """Test both tokens missing."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {"status": "approved"}
        # Both missing: run_token defaults to 0, executed_run_token defaults to -1
        # 0 > -1 = True, so eligible
        self.assertTrue(is_plan_eligible(doc))

    def test_run_token_none(self):
        """Test run_token is None."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {"status": "approved", "run_token": None, "executed_run_token": 0}
        self.assertFalse(is_plan_eligible(doc))

    def test_executed_run_token_none(self):
        """Test executed_run_token is None."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {"status": "approved", "run_token": 1, "executed_run_token": None}
        # None fails int() conversion → TypeError caught → return False
        self.assertFalse(is_plan_eligible(doc))

    def test_run_token_negative(self):
        """Test negative run_token (invalid but defensible)."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {"status": "approved", "run_token": -1, "executed_run_token": 0}
        # -1 > 0 is False, so not eligible
        self.assertFalse(is_plan_eligible(doc))

    def test_executed_run_token_negative(self):
        """Test negative executed_run_token (invalid but defensible)."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {"status": "approved", "run_token": 0, "executed_run_token": -1}
        # 0 > -1 is True, so technically eligible
        self.assertTrue(is_plan_eligible(doc))

    def test_executed_run_token_greater_than_run_token(self):
        """Test executed_run_token > run_token (should never happen)."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        # This is an invalid state but should be handled gracefully
        doc = {"status": "approved", "run_token": 1, "executed_run_token": 5}
        self.assertFalse(is_plan_eligible(doc))

    def test_tokens_as_strings(self):
        """Test tokens as string values (type coercion)."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        # CouchDB might return these as strings in some scenarios
        doc = {"status": "approved", "run_token": "1", "executed_run_token": "0"}
        # Should handle string-to-int conversion or be defensive
        # Actual behavior depends on implementation
        # This test documents expected behavior
        try:
            result = is_plan_eligible(doc)
            # If it handles strings, it should be eligible
            self.assertTrue(result)
        except (TypeError, ValueError):
            # If it doesn't handle strings, that's also acceptable
            pass

    def test_tokens_as_floats(self):
        """Test tokens as float values."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {"status": "approved", "run_token": 1.0, "executed_run_token": 0.0}
        # Float comparison should work (1.0 > 0.0)
        self.assertTrue(is_plan_eligible(doc))

    def test_tokens_very_large(self):
        """Test very large token values."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {
            "status": "approved",
            "run_token": 10**18,
            "executed_run_token": 10**18 - 1,
        }
        self.assertTrue(is_plan_eligible(doc))


class TestPlanEligibilityDocumentEdgeCases(unittest.TestCase):
    """Tests for document structure edge cases."""

    def test_empty_document(self):
        """Test empty document."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc: dict[str, Any] = {}
        self.assertFalse(is_plan_eligible(doc))

    def test_none_document(self):
        """Test None as document."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        # Should handle None gracefully
        try:
            result = is_plan_eligible(None)  # type: ignore
            self.assertFalse(result)
        except (TypeError, AttributeError):
            # Raising is also acceptable
            pass

    def test_document_with_extra_fields(self):
        """Test document with extra fields."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {
            "status": "approved",
            "run_token": 1,
            "executed_run_token": 0,
            "extra_field": "ignored",
            "another": 12345,
        }
        self.assertTrue(is_plan_eligible(doc))

    def test_document_with_nested_status(self):
        """Test document with status in wrong location."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {
            "metadata": {"status": "approved"},  # Wrong location
            "run_token": 1,
            "executed_run_token": 0,
        }
        # Status at root level is missing
        self.assertFalse(is_plan_eligible(doc))

    def test_document_with_plan_field(self):
        """Test that plan field doesn't affect eligibility."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {
            "status": "approved",
            "run_token": 1,
            "executed_run_token": 0,
            "plan": {
                "plan_id": "test",
                "steps": [],
            },
        }
        # Plan content is irrelevant for eligibility
        self.assertTrue(is_plan_eligible(doc))

    def test_document_with_auto_run_field(self):
        """Test that auto_run field doesn't directly affect eligibility."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        # auto_run=True with status=approved
        doc1 = {
            "status": "approved",
            "auto_run": True,
            "run_token": 1,
            "executed_run_token": 0,
        }
        self.assertTrue(is_plan_eligible(doc1))

        # auto_run=False with status=approved
        doc2 = {
            "status": "approved",
            "auto_run": False,
            "run_token": 1,
            "executed_run_token": 0,
        }
        self.assertTrue(is_plan_eligible(doc2))

        # auto_run=True with status=draft (still not eligible)
        doc3 = {
            "status": "draft",
            "auto_run": True,
            "run_token": 1,
            "executed_run_token": 0,
        }
        self.assertFalse(is_plan_eligible(doc3))


class TestPlanEligibilityTableDriven(unittest.TestCase):
    """Table-driven tests for comprehensive coverage."""

    def test_eligibility_matrix(self):
        """Test all combinations of status and token states."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        test_cases = [
            # (status, run_token, executed_run_token, expected, description)
            # Approved status variations
            ("approved", 1, 0, True, "approved, first run"),
            ("approved", 2, 1, True, "approved, re-run"),
            ("approved", 1, 1, False, "approved, already executed"),
            ("approved", 0, 0, False, "approved, tokens both zero"),
            # Non-approved statuses
            ("draft", 1, 0, False, "draft status"),
            ("pending", 1, 0, False, "pending status"),
            ("rejected", 1, 0, False, "rejected status"),
            ("expired", 1, 0, False, "expired status"),
            # Edge token values
            ("approved", 100, 99, True, "approved, large tokens"),
            ("approved", 100, 100, False, "approved, large tokens equal"),
            ("approved", 0, -1, True, "approved, negative executed"),
            ("approved", -1, -2, True, "approved, both negative"),
        ]

        for status, run_token, executed_token, expected, desc in test_cases:
            with self.subTest(description=desc):
                doc = {
                    "status": status,
                    "run_token": run_token,
                    "executed_run_token": executed_token,
                }
                result = is_plan_eligible(doc)
                self.assertEqual(
                    result,
                    expected,
                    f"Failed for {desc}: expected {expected}, got {result}",
                )


class TestPlanEligibilityDefensiveProgramming(unittest.TestCase):
    """Tests for defensive programming and error handling."""

    def test_status_with_leading_trailing_spaces(self):
        """Test status with whitespace that might slip through."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {"status": " approved ", "run_token": 1, "executed_run_token": 0}
        # Strict comparison: " approved " != "approved"
        self.assertFalse(is_plan_eligible(doc))

    def test_status_with_newline(self):
        """Test status with embedded newline."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {"status": "approved\n", "run_token": 1, "executed_run_token": 0}
        self.assertFalse(is_plan_eligible(doc))

    def test_unicode_status(self):
        """Test status with unicode characters."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {"status": "αpproved", "run_token": 1, "executed_run_token": 0}
        self.assertFalse(is_plan_eligible(doc))

    def test_status_as_number(self):
        """Test status as numeric value."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {"status": 1, "run_token": 1, "executed_run_token": 0}
        # 1 != "approved"
        self.assertFalse(is_plan_eligible(doc))

    def test_status_as_boolean(self):
        """Test status as boolean."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {"status": True, "run_token": 1, "executed_run_token": 0}
        self.assertFalse(is_plan_eligible(doc))

    def test_status_as_list(self):
        """Test status as list."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {"status": ["approved"], "run_token": 1, "executed_run_token": 0}
        self.assertFalse(is_plan_eligible(doc))

    def test_status_as_dict(self):
        """Test status as dict."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {"status": {"value": "approved"}, "run_token": 1, "executed_run_token": 0}
        self.assertFalse(is_plan_eligible(doc))


class TestPlanEligibilityRealWorldScenarios(unittest.TestCase):
    """Tests based on real-world scenarios."""

    def test_freshly_created_auto_run_plan(self):
        """Test plan just created with auto_run=True."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {
            "_id": "pln_tenx_P12345_v1",
            "realm": "tenx",
            "scope": {"kind": "project", "id": "P12345"},
            "status": "approved",  # auto_run plans get approved status
            "auto_run": True,
            "run_token": 1,
            "executed_run_token": 0,
            "created_at": "2026-01-16T10:00:00Z",
        }
        self.assertTrue(is_plan_eligible(doc))

    def test_freshly_created_approval_required_plan(self):
        """Test plan just created with auto_run=False."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {
            "_id": "pln_tenx_P12345_v1",
            "realm": "tenx",
            "scope": {"kind": "project", "id": "P12345"},
            "status": "draft",  # non-auto_run plans start as draft
            "auto_run": False,
            "run_token": 1,
            "executed_run_token": 0,
            "created_at": "2026-01-16T10:00:00Z",
        }
        self.assertFalse(is_plan_eligible(doc))

    def test_plan_after_genstat_approval(self):
        """Test plan after Genstat approves it."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {
            "_id": "pln_tenx_P12345_v1",
            "realm": "tenx",
            "scope": {"kind": "project", "id": "P12345"},
            "status": "approved",  # Changed by Genstat
            "auto_run": False,
            "run_token": 1,
            "executed_run_token": 0,
            "approved_at": "2026-01-16T11:00:00Z",
            "approved_by": "user@example.com",
        }
        self.assertTrue(is_plan_eligible(doc))

    def test_plan_after_successful_execution(self):
        """Test plan after successful execution."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {
            "_id": "pln_tenx_P12345_v1",
            "realm": "tenx",
            "scope": {"kind": "project", "id": "P12345"},
            "status": "approved",
            "auto_run": True,
            "run_token": 1,
            "executed_run_token": 1,  # Updated by Yggdrasil
        }
        self.assertFalse(is_plan_eligible(doc))

    def test_plan_after_rerun_request(self):
        """Test plan after Genstat requests re-run."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {
            "_id": "pln_tenx_P12345_v1",
            "realm": "tenx",
            "scope": {"kind": "project", "id": "P12345"},
            "status": "approved",
            "auto_run": True,
            "run_token": 2,  # Incremented by Genstat
            "executed_run_token": 1,  # Still at previous value
            "run_requested_at": "2026-01-16T14:00:00Z",
            "run_requested_by": "user@example.com",
        }
        self.assertTrue(is_plan_eligible(doc))

    def test_plan_with_multiple_reruns(self):
        """Test plan that has been re-run multiple times."""
        from lib.core_utils.plan_eligibility import is_plan_eligible

        doc = {
            "_id": "pln_tenx_P12345_v1",
            "realm": "tenx",
            "status": "approved",
            "run_token": 5,
            "executed_run_token": 5,  # All runs completed
        }
        self.assertFalse(is_plan_eligible(doc))


if __name__ == "__main__":
    unittest.main(verbosity=2)
