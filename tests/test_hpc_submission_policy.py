import unittest

from yggdrasil.toolkit.hpc_submission_policy import HPCSubmissionPolicy


class TestHPCSubmissionPolicy(unittest.TestCase):
    """
    Comprehensive tests for HPCSubmissionPolicy - the HPC submission decision engine.

    Tests the aggregation logic for multiple signals to determine whether HPC jobs
    should be submitted automatically or manually, including:
    - User manual submission overrides
    - Document auto-submission preferences
    - Realm auto-submission capabilities
    - Complex decision matrix scenarios
    """

    def setUp(self):
        """Set up test fixtures for each test."""
        # Default test policy with known values
        self.default_policy = HPCSubmissionPolicy()

        # Pre-configured policies for specific test scenarios
        self.auto_policy = HPCSubmissionPolicy(
            user_manual_submit=False, doc_auto_submit=True, realm_supports_auto=True
        )

        self.manual_policy = HPCSubmissionPolicy(
            user_manual_submit=True, doc_auto_submit=True, realm_supports_auto=True
        )

    # =====================================================
    # INITIALIZATION TESTS
    # =====================================================

    def test_default_initialization(self):
        """Test default initialization values."""
        policy = HPCSubmissionPolicy()

        # Verify default values
        self.assertIsNone(policy.user_manual_submit)
        self.assertTrue(policy.doc_auto_submit)
        self.assertFalse(policy.realm_supports_auto)

        # With defaults, should be manual (realm doesn't support auto)
        self.assertFalse(policy.should_auto_submit())

    def test_custom_initialization(self):
        """Test initialization with custom values."""
        policy = HPCSubmissionPolicy(
            user_manual_submit=True, doc_auto_submit=False, realm_supports_auto=True
        )

        # Verify custom values
        self.assertTrue(policy.user_manual_submit)
        self.assertFalse(policy.doc_auto_submit)
        self.assertTrue(policy.realm_supports_auto)

    def test_initialization_with_none_values(self):
        """Test initialization with None user_manual_submit."""
        policy = HPCSubmissionPolicy(
            user_manual_submit=None, doc_auto_submit=True, realm_supports_auto=True
        )

        self.assertIsNone(policy.user_manual_submit)
        self.assertTrue(policy.doc_auto_submit)
        self.assertTrue(policy.realm_supports_auto)

    # =====================================================
    # PROPERTY GETTER TESTS
    # =====================================================

    def test_user_manual_submit_getter(self):
        """Test user_manual_submit property getter."""
        # Test with True
        policy = HPCSubmissionPolicy(user_manual_submit=True)
        self.assertTrue(policy.user_manual_submit)

        # Test with False
        policy = HPCSubmissionPolicy(user_manual_submit=False)
        self.assertFalse(policy.user_manual_submit)

        # Test with None
        policy = HPCSubmissionPolicy(user_manual_submit=None)
        self.assertIsNone(policy.user_manual_submit)

    def test_doc_auto_submit_getter(self):
        """Test doc_auto_submit property getter."""
        # Test with True
        policy = HPCSubmissionPolicy(doc_auto_submit=True)
        self.assertTrue(policy.doc_auto_submit)

        # Test with False
        policy = HPCSubmissionPolicy(doc_auto_submit=False)
        self.assertFalse(policy.doc_auto_submit)

    def test_realm_supports_auto_getter(self):
        """Test realm_supports_auto property getter."""
        # Test with True
        policy = HPCSubmissionPolicy(realm_supports_auto=True)
        self.assertTrue(policy.realm_supports_auto)

        # Test with False
        policy = HPCSubmissionPolicy(realm_supports_auto=False)
        self.assertFalse(policy.realm_supports_auto)

    # =====================================================
    # PROPERTY SETTER TESTS
    # =====================================================

    def test_user_manual_submit_setter(self):
        """Test user_manual_submit property setter."""
        policy = HPCSubmissionPolicy()

        # Set to True
        policy.user_manual_submit = True
        self.assertTrue(policy.user_manual_submit)

        # Set to False
        policy.user_manual_submit = False
        self.assertFalse(policy.user_manual_submit)

        # Set to None
        policy.user_manual_submit = None
        self.assertIsNone(policy.user_manual_submit)

    def test_doc_auto_submit_setter(self):
        """Test doc_auto_submit property setter."""
        policy = HPCSubmissionPolicy()

        # Set to False
        policy.doc_auto_submit = False
        self.assertFalse(policy.doc_auto_submit)

        # Set back to True
        policy.doc_auto_submit = True
        self.assertTrue(policy.doc_auto_submit)

    def test_realm_supports_auto_setter(self):
        """Test realm_supports_auto property setter."""
        policy = HPCSubmissionPolicy()

        # Set to True
        policy.realm_supports_auto = True
        self.assertTrue(policy.realm_supports_auto)

        # Set back to False
        policy.realm_supports_auto = False
        self.assertFalse(policy.realm_supports_auto)

    # =====================================================
    # DECISION LOGIC TESTS - AUTO SUBMISSION
    # =====================================================

    def test_should_auto_submit_all_conditions_true(self):
        """Test auto submission when all conditions allow it."""
        policy = HPCSubmissionPolicy(
            user_manual_submit=False, doc_auto_submit=True, realm_supports_auto=True
        )

        self.assertTrue(policy.should_auto_submit())

    def test_should_auto_submit_user_none_other_true(self):
        """Test auto submission when user override is None and others are True."""
        policy = HPCSubmissionPolicy(
            user_manual_submit=None, doc_auto_submit=True, realm_supports_auto=True
        )

        self.assertTrue(policy.should_auto_submit())

    # =====================================================
    # DECISION LOGIC TESTS - MANUAL SUBMISSION (FORCED)
    # =====================================================

    def test_should_auto_submit_user_forces_manual(self):
        """Test that user manual override forces manual submission."""
        policy = HPCSubmissionPolicy(
            user_manual_submit=True, doc_auto_submit=True, realm_supports_auto=True
        )

        # Even with doc and realm supporting auto, user override forces manual
        self.assertFalse(policy.should_auto_submit())

    def test_should_auto_submit_doc_forces_manual(self):
        """Test that document setting forces manual submission."""
        policy = HPCSubmissionPolicy(
            user_manual_submit=False, doc_auto_submit=False, realm_supports_auto=True
        )

        # Even with user allowing auto and realm supporting it, doc forces manual
        self.assertFalse(policy.should_auto_submit())

    def test_should_auto_submit_realm_forces_manual(self):
        """Test that realm limitation forces manual submission."""
        policy = HPCSubmissionPolicy(
            user_manual_submit=False, doc_auto_submit=True, realm_supports_auto=False
        )

        # Even with user and doc allowing auto, realm limitation forces manual
        self.assertFalse(policy.should_auto_submit())

    # =====================================================
    # DECISION MATRIX COMPREHENSIVE TESTS
    # =====================================================

    def test_decision_matrix_all_combinations(self):
        """Test all possible combinations of the three boolean inputs."""
        # Test all 8 combinations of (user_manual_submit, doc_auto_submit, realm_supports_auto)
        # Only one combination should result in auto submission

        test_cases = [
            # (user_manual, doc_auto, realm_auto, expected_auto)
            (True, True, True, False),  # user forces manual
            (True, True, False, False),  # user forces manual
            (True, False, True, False),  # user forces manual
            (True, False, False, False),  # user forces manual
            (False, True, True, True),  # only case that allows auto
            (False, True, False, False),  # realm doesn't support auto
            (False, False, True, False),  # doc forces manual
            (False, False, False, False),  # multiple restrictions
        ]

        for user_manual, doc_auto, realm_auto, expected_auto in test_cases:
            with self.subTest(
                user_manual=user_manual,
                doc_auto=doc_auto,
                realm_auto=realm_auto,
                expected=expected_auto,
            ):
                policy = HPCSubmissionPolicy(
                    user_manual_submit=user_manual,
                    doc_auto_submit=doc_auto,
                    realm_supports_auto=realm_auto,
                )

                self.assertEqual(
                    policy.should_auto_submit(),
                    expected_auto,
                    f"Failed for user_manual={user_manual}, "
                    f"doc_auto={doc_auto}, realm_auto={realm_auto}",
                )

    def test_decision_matrix_with_none_user_override(self):
        """Test decision matrix when user_manual_submit is None."""
        test_cases = [
            # (doc_auto, realm_auto, expected_auto)
            (True, True, True),  # should allow auto
            (True, False, False),  # realm blocks auto
            (False, True, False),  # doc blocks auto
            (False, False, False),  # both block auto
        ]

        for doc_auto, realm_auto, expected_auto in test_cases:
            with self.subTest(
                doc_auto=doc_auto, realm_auto=realm_auto, expected=expected_auto
            ):
                policy = HPCSubmissionPolicy(
                    user_manual_submit=None,
                    doc_auto_submit=doc_auto,
                    realm_supports_auto=realm_auto,
                )

                self.assertEqual(
                    policy.should_auto_submit(),
                    expected_auto,
                    f"Failed for user_manual=None, "
                    f"doc_auto={doc_auto}, realm_auto={realm_auto}",
                )

    # =====================================================
    # EDGE CASE AND SCENARIO TESTS
    # =====================================================

    def test_runtime_property_changes(self):
        """Test that changing properties at runtime affects decisions correctly."""
        policy = HPCSubmissionPolicy(
            user_manual_submit=False, doc_auto_submit=True, realm_supports_auto=True
        )

        # Initially should allow auto
        self.assertTrue(policy.should_auto_submit())

        # Change user to force manual
        policy.user_manual_submit = True
        self.assertFalse(policy.should_auto_submit())

        # Change back to None (no override)
        policy.user_manual_submit = None
        self.assertTrue(policy.should_auto_submit())

        # Change doc to force manual
        policy.doc_auto_submit = False
        self.assertFalse(policy.should_auto_submit())

        # Change back to auto
        policy.doc_auto_submit = True
        self.assertTrue(policy.should_auto_submit())

        # Change realm to not support auto
        policy.realm_supports_auto = False
        self.assertFalse(policy.should_auto_submit())

    def test_multiple_blocking_conditions(self):
        """Test behavior when multiple conditions block auto submission."""
        policy = HPCSubmissionPolicy(
            user_manual_submit=True, doc_auto_submit=False, realm_supports_auto=False
        )

        # All three conditions block auto submission
        self.assertFalse(policy.should_auto_submit())

        # Remove one blocking condition at a time
        policy.user_manual_submit = False
        self.assertFalse(policy.should_auto_submit())  # Still blocked by doc and realm

        policy.doc_auto_submit = True
        self.assertFalse(policy.should_auto_submit())  # Still blocked by realm

        policy.realm_supports_auto = True
        self.assertTrue(policy.should_auto_submit())  # Now all conditions allow auto

    def test_user_manual_submit_precedence(self):
        """Test that user_manual_submit=True takes precedence over other settings."""
        # Test with all other conditions favoring auto
        policy = HPCSubmissionPolicy(
            user_manual_submit=True, doc_auto_submit=True, realm_supports_auto=True
        )

        self.assertFalse(policy.should_auto_submit())

        # Even if we change other properties, user override should dominate
        policy.doc_auto_submit = False
        policy.realm_supports_auto = False
        self.assertFalse(policy.should_auto_submit())

    # =====================================================
    # REAL-WORLD SCENARIO TESTS
    # =====================================================

    def test_cli_manual_override_scenario(self):
        """Test scenario where CLI user forces manual submission."""
        # Simulate: user runs `ygg run-doc doc123 --manual-submit`
        policy = HPCSubmissionPolicy(
            user_manual_submit=True,  # From CLI flag
            doc_auto_submit=True,  # Doc says auto is OK
            realm_supports_auto=True,  # Realm can handle auto
        )

        # Despite everything else being configured for auto, user override wins
        self.assertFalse(policy.should_auto_submit())

    def test_development_realm_scenario(self):
        """Test scenario with a realm under development."""
        # Simulate: realm is not fully implemented yet
        policy = HPCSubmissionPolicy(
            user_manual_submit=None,  # No user override
            doc_auto_submit=True,  # Doc wants auto
            realm_supports_auto=False,  # But realm can't handle it yet
        )

        # Should fall back to manual due to realm limitations
        self.assertFalse(policy.should_auto_submit())

    def test_conservative_document_scenario(self):
        """Test scenario where document explicitly requests manual submission."""
        # Simulate: document has "auto_submit": false
        policy = HPCSubmissionPolicy(
            user_manual_submit=None,  # No user override
            doc_auto_submit=False,  # Doc explicitly says manual
            realm_supports_auto=True,  # Realm could handle auto
        )

        # Should respect document preference
        self.assertFalse(policy.should_auto_submit())

    def test_fully_automated_scenario(self):
        """Test scenario where everything is configured for automation."""
        # Simulate: production system with mature realm
        policy = HPCSubmissionPolicy(
            user_manual_submit=None,  # No user override
            doc_auto_submit=True,  # Doc allows auto
            realm_supports_auto=True,  # Realm fully supports auto
        )

        # Should allow auto submission
        self.assertTrue(policy.should_auto_submit())

    def test_explicit_user_auto_permission_scenario(self):
        """Test scenario where user explicitly allows auto submission."""
        # Simulate: user wants to ensure auto submission is allowed
        policy = HPCSubmissionPolicy(
            user_manual_submit=False,  # User explicitly allows auto
            doc_auto_submit=True,  # Doc allows auto
            realm_supports_auto=True,  # Realm supports auto
        )

        # Should allow auto submission
        self.assertTrue(policy.should_auto_submit())

    # =====================================================
    # CONSISTENCY AND INVARIANT TESTS
    # =====================================================

    def test_decision_consistency(self):
        """Test that decisions are consistent across multiple calls."""
        policy = HPCSubmissionPolicy(
            user_manual_submit=False, doc_auto_submit=True, realm_supports_auto=True
        )

        # Should return the same result consistently
        first_result = policy.should_auto_submit()
        for _ in range(10):
            self.assertEqual(policy.should_auto_submit(), first_result)

    def test_property_independence(self):
        """Test that properties can be changed independently."""
        policy = HPCSubmissionPolicy()

        # Change each property independently and verify others unchanged
        original_doc = policy.doc_auto_submit
        original_realm = policy.realm_supports_auto

        policy.user_manual_submit = True
        self.assertEqual(policy.doc_auto_submit, original_doc)
        self.assertEqual(policy.realm_supports_auto, original_realm)

        policy.doc_auto_submit = False
        self.assertTrue(policy.user_manual_submit)
        self.assertEqual(policy.realm_supports_auto, original_realm)

        policy.realm_supports_auto = True
        self.assertTrue(policy.user_manual_submit)
        self.assertFalse(policy.doc_auto_submit)

    def test_boolean_type_enforcement(self):
        """Test that properties handle boolean types correctly."""
        policy = HPCSubmissionPolicy()

        # Test that non-None values for user_manual_submit work correctly
        for value in [True, False]:
            policy.user_manual_submit = value
            self.assertEqual(policy.user_manual_submit, value)
            self.assertIsInstance(policy.user_manual_submit, bool)

        # Test that boolean properties work correctly
        for value in [True, False]:
            policy.doc_auto_submit = value
            self.assertEqual(policy.doc_auto_submit, value)
            self.assertIsInstance(policy.doc_auto_submit, bool)

            policy.realm_supports_auto = value
            self.assertEqual(policy.realm_supports_auto, value)
            self.assertIsInstance(policy.realm_supports_auto, bool)


if __name__ == "__main__":
    unittest.main()
