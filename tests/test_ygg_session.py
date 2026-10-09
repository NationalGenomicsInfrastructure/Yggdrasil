import unittest

from yggdrasil.config.session import YggSession


class TestYggSession(unittest.TestCase):
    """
    Comprehensive tests for YggSession - session-wide state management.

    Tests initialization, state management, error handling, and immutability
    guarantees for both dev mode and manual HPC submission flags.
    """

    def setUp(self):
        """Reset YggSession state before each test."""
        # Reset class variables to their initial state
        YggSession._YggSession__dev_mode = False  # type: ignore
        YggSession._YggSession__dev_already_set = False  # type: ignore
        YggSession._YggSession__manual_submit = False  # type: ignore
        YggSession._YggSession__manual_already_set = False  # type: ignore

    def tearDown(self):
        """Clean up YggSession state after each test."""
        # Reset class variables to their initial state
        YggSession._YggSession__dev_mode = False  # type: ignore
        YggSession._YggSession__dev_already_set = False  # type: ignore
        YggSession._YggSession__manual_submit = False  # type: ignore
        YggSession._YggSession__manual_already_set = False  # type: ignore

    # =====================================================
    # DEV MODE TESTS
    # =====================================================

    def test_initial_dev_mode_state(self):
        """Test that dev mode is initially False and not set."""
        self.assertFalse(YggSession.is_dev())
        # Access private attributes for state verification
        self.assertFalse(YggSession._YggSession__dev_already_set)  # type: ignore

    def test_init_dev_mode_true(self):
        """Test setting dev mode to True."""
        # Act
        YggSession.init_dev_mode(True)

        # Assert
        self.assertTrue(YggSession.is_dev())
        self.assertTrue(YggSession._YggSession__dev_already_set)  # type: ignore

    def test_init_dev_mode_false(self):
        """Test explicitly setting dev mode to False."""
        # Act
        YggSession.init_dev_mode(False)

        # Assert
        self.assertFalse(YggSession.is_dev())
        self.assertTrue(YggSession._YggSession__dev_already_set)  # type: ignore

    def test_dev_mode_cannot_change_after_set_to_true(self):
        """Test that dev mode cannot be changed once set to True."""
        # Arrange
        YggSession.init_dev_mode(True)

        # Act & Assert
        with self.assertRaises(RuntimeError) as context:
            YggSession.init_dev_mode(False)

        self.assertIn("Dev mode was already set", str(context.exception))
        self.assertIn("Cannot change mid-run", str(context.exception))

        # Verify state remains unchanged
        self.assertTrue(YggSession.is_dev())

    def test_dev_mode_cannot_change_after_set_to_false(self):
        """Test that dev mode cannot be changed once set to False."""
        # Arrange
        YggSession.init_dev_mode(False)

        # Act & Assert
        with self.assertRaises(RuntimeError) as context:
            YggSession.init_dev_mode(True)

        self.assertIn("Dev mode was already set", str(context.exception))
        self.assertIn("Cannot change mid-run", str(context.exception))

        # Verify state remains unchanged
        self.assertFalse(YggSession.is_dev())

    def test_dev_mode_repeated_same_value_raises_error(self):
        """Test that setting the same dev mode value twice raises an error."""
        # Arrange
        YggSession.init_dev_mode(True)

        # Act & Assert - even setting the same value should raise error
        with self.assertRaises(RuntimeError):
            YggSession.init_dev_mode(True)

    # =====================================================
    # MANUAL SUBMIT TESTS
    # =====================================================

    def test_initial_manual_submit_state(self):
        """Test that manual submit is initially False and not set."""
        self.assertFalse(YggSession.is_manual_submit())
        # Access private attributes for state verification
        self.assertFalse(YggSession._YggSession__manual_already_set)  # type: ignore

    def test_init_manual_submit_true(self):
        """Test setting manual submit to True."""
        # Act
        YggSession.init_manual_submit(True)

        # Assert
        self.assertTrue(YggSession.is_manual_submit())
        self.assertTrue(YggSession._YggSession__manual_already_set)  # type: ignore

    def test_init_manual_submit_false(self):
        """Test explicitly setting manual submit to False."""
        # Act
        YggSession.init_manual_submit(False)

        # Assert
        self.assertFalse(YggSession.is_manual_submit())
        self.assertTrue(YggSession._YggSession__manual_already_set)  # type: ignore

    def test_manual_submit_cannot_change_after_set_to_true(self):
        """Test that manual submit cannot be changed once set to True."""
        # Arrange
        YggSession.init_manual_submit(True)

        # Act & Assert
        with self.assertRaises(RuntimeError) as context:
            YggSession.init_manual_submit(False)

        self.assertIn("HPC submission flag already set", str(context.exception))
        self.assertIn("cannot change mid-run", str(context.exception))

        # Verify state remains unchanged
        self.assertTrue(YggSession.is_manual_submit())

    def test_manual_submit_cannot_change_after_set_to_false(self):
        """Test that manual submit cannot be changed once set to False."""
        # Arrange
        YggSession.init_manual_submit(False)

        # Act & Assert
        with self.assertRaises(RuntimeError) as context:
            YggSession.init_manual_submit(True)

        self.assertIn("HPC submission flag already set", str(context.exception))
        self.assertIn("cannot change mid-run", str(context.exception))

        # Verify state remains unchanged
        self.assertFalse(YggSession.is_manual_submit())

    def test_manual_submit_repeated_same_value_raises_error(self):
        """Test that setting the same manual submit value twice raises an error."""
        # Arrange
        YggSession.init_manual_submit(True)

        # Act & Assert - even setting the same value should raise error
        with self.assertRaises(RuntimeError):
            YggSession.init_manual_submit(True)

    # =====================================================
    # INDEPENDENCE TESTS
    # =====================================================

    def test_dev_mode_and_manual_submit_are_independent(self):
        """Test that dev mode and manual submit flags are independent."""
        # Act - set dev mode first
        YggSession.init_dev_mode(True)
        self.assertTrue(YggSession.is_dev())
        self.assertFalse(YggSession.is_manual_submit())

        # Act - set manual submit
        YggSession.init_manual_submit(False)
        self.assertTrue(YggSession.is_dev())
        self.assertFalse(YggSession.is_manual_submit())

        # Verify both flags are properly set
        self.assertTrue(YggSession._YggSession__dev_already_set)  # type: ignore
        self.assertTrue(YggSession._YggSession__manual_already_set)  # type: ignore

    def test_manual_submit_and_dev_mode_are_independent(self):
        """Test independence in the opposite order."""
        # Act - set manual submit first
        YggSession.init_manual_submit(True)
        self.assertFalse(YggSession.is_dev())
        self.assertTrue(YggSession.is_manual_submit())

        # Act - set dev mode
        YggSession.init_dev_mode(False)
        self.assertFalse(YggSession.is_dev())
        self.assertTrue(YggSession.is_manual_submit())

        # Verify both flags are properly set
        self.assertTrue(YggSession._YggSession__dev_already_set)  # type: ignore
        self.assertTrue(YggSession._YggSession__manual_already_set)  # type: ignore

    def test_all_four_combinations_work(self):
        """Test all combinations of dev mode and manual submit settings."""
        test_cases = [(True, True), (True, False), (False, True), (False, False)]

        for dev_mode, manual_submit in test_cases:
            with self.subTest(dev_mode=dev_mode, manual_submit=manual_submit):
                # Reset state
                self.setUp()

                # Set both flags
                YggSession.init_dev_mode(dev_mode)
                YggSession.init_manual_submit(manual_submit)

                # Verify both are set correctly
                self.assertEqual(YggSession.is_dev(), dev_mode)
                self.assertEqual(YggSession.is_manual_submit(), manual_submit)

    # =====================================================
    # EDGE CASES AND TYPE HANDLING
    # =====================================================

    def test_dev_mode_with_non_boolean_truthy_values(self):
        """Test dev mode with truthy non-boolean values."""
        # These should work because Python's bool() conversion will handle them
        truthy_values = [1, "True", [1], {"key": "value"}]

        for value in truthy_values:
            with self.subTest(value=value):
                self.setUp()  # Reset state
                YggSession.init_dev_mode(value)
                self.assertTrue(YggSession.is_dev())

    def test_dev_mode_with_non_boolean_falsy_values(self):
        """Test dev mode with falsy non-boolean values."""
        falsy_values = [0, "", None, [], {}]

        for value in falsy_values:
            with self.subTest(value=value):
                self.setUp()  # Reset state
                YggSession.init_dev_mode(value)
                self.assertFalse(YggSession.is_dev())

    def test_manual_submit_with_non_boolean_truthy_values(self):
        """Test manual submit with truthy non-boolean values."""
        truthy_values = [1, "True", [1], {"key": "value"}]

        for value in truthy_values:
            with self.subTest(value=value):
                self.setUp()  # Reset state
                YggSession.init_manual_submit(value)
                self.assertTrue(YggSession.is_manual_submit())

    def test_manual_submit_with_non_boolean_falsy_values(self):
        """Test manual submit with falsy non-boolean values."""
        falsy_values = [0, "", None, [], {}]

        for value in falsy_values:
            with self.subTest(value=value):
                self.setUp()  # Reset state
                YggSession.init_manual_submit(value)
                self.assertFalse(YggSession.is_manual_submit())

    # =====================================================
    # CLASS BEHAVIOR TESTS
    # =====================================================

    def test_ygg_session_is_class_not_instance(self):
        """Test that YggSession works as a class with class methods."""
        # YggSession should be used as a class, not instantiated
        # All methods should be class methods

        # Verify methods are class methods
        self.assertTrue(callable(YggSession.init_dev_mode))
        self.assertTrue(callable(YggSession.is_dev))
        self.assertTrue(callable(YggSession.init_manual_submit))
        self.assertTrue(callable(YggSession.is_manual_submit))

    def test_multiple_access_patterns(self):
        """Test that the class can be accessed in various ways."""
        # Set initial state
        YggSession.init_dev_mode(True)
        YggSession.init_manual_submit(False)

        # Test multiple ways of accessing the same methods
        self.assertTrue(YggSession.is_dev())

        # Even if someone creates an instance (though not intended),
        # the class methods should still work
        instance = YggSession()
        self.assertTrue(instance.is_dev())
        self.assertFalse(instance.is_manual_submit())

    # =====================================================
    # CONCURRENCY AND STATE CONSISTENCY TESTS
    # =====================================================

    def test_state_consistency_under_rapid_access(self):
        """Test that state remains consistent under rapid read access."""
        # Set initial state
        YggSession.init_dev_mode(True)
        YggSession.init_manual_submit(False)

        # Rapid access should return consistent results
        for _ in range(100):
            self.assertTrue(YggSession.is_dev())
            self.assertFalse(YggSession.is_manual_submit())

    def test_immutability_guarantee(self):
        """Test that once set, flags truly cannot be changed."""
        # Set both flags
        YggSession.init_dev_mode(True)
        YggSession.init_manual_submit(False)

        # Record initial state
        initial_dev = YggSession.is_dev()
        initial_manual = YggSession.is_manual_submit()

        # Try to change both (should fail)
        with self.assertRaises(RuntimeError):
            YggSession.init_dev_mode(False)

        with self.assertRaises(RuntimeError):
            YggSession.init_manual_submit(True)

        # Verify state unchanged
        self.assertEqual(YggSession.is_dev(), initial_dev)
        self.assertEqual(YggSession.is_manual_submit(), initial_manual)

    # =====================================================
    # ERROR MESSAGE QUALITY TESTS
    # =====================================================

    def test_dev_mode_error_message_quality(self):
        """Test that dev mode error messages are informative."""
        YggSession.init_dev_mode(True)

        with self.assertRaises(RuntimeError) as context:
            YggSession.init_dev_mode(False)

        error_msg = str(context.exception)
        # Check for key information in error message
        self.assertIn("Dev mode", error_msg)
        self.assertIn("already set", error_msg)
        self.assertIn("Cannot change", error_msg)
        self.assertIn("mid-run", error_msg)

    def test_manual_submit_error_message_quality(self):
        """Test that manual submit error messages are informative."""
        YggSession.init_manual_submit(False)

        with self.assertRaises(RuntimeError) as context:
            YggSession.init_manual_submit(True)

        error_msg = str(context.exception)
        # Check for key information in error message
        self.assertIn("HPC submission flag", error_msg)
        self.assertIn("already set", error_msg)
        self.assertIn("cannot change", error_msg)
        self.assertIn("mid-run", error_msg)


if __name__ == "__main__":
    unittest.main()
