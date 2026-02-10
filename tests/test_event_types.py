import unittest

from lib.core_utils.event_types import EventType


class TestEventType(unittest.TestCase):
    """
    Comprehensive tests for EventType enum.

    Tests enum values, string behavior, serialization, and validation.
    """

    def test_enum_values(self):
        """Test that all expected enum values exist with correct string values."""
        # Test that all expected enum members exist
        self.assertTrue(hasattr(EventType, "PROJECT_CHANGE"))
        self.assertTrue(hasattr(EventType, "FLOWCELL_READY"))
        self.assertTrue(hasattr(EventType, "DELIVERY_READY"))
        self.assertTrue(hasattr(EventType, "PLAN_EXECUTION"))
        self.assertTrue(hasattr(EventType, "COUCHDB_DOC_CHANGED"))
        self.assertTrue(hasattr(EventType, "COUCHDB_DOC_DELETED"))
        self.assertTrue(hasattr(EventType, "TEST_SCENARIO_CHANGE"))

        # Test the actual string values
        self.assertEqual(EventType.PROJECT_CHANGE, "project_change")
        self.assertEqual(EventType.FLOWCELL_READY, "flowcell_ready")
        self.assertEqual(EventType.DELIVERY_READY, "delivery_ready")
        self.assertEqual(EventType.PLAN_EXECUTION, "plan_execution")
        self.assertEqual(EventType.COUCHDB_DOC_CHANGED, "couchdb_doc_changed")
        self.assertEqual(EventType.COUCHDB_DOC_DELETED, "couchdb_doc_deleted")
        self.assertEqual(EventType.TEST_SCENARIO_CHANGE, "test_scenario_change")

    def test_enum_count(self):
        """Test that we have exactly the expected number of event types."""
        all_events = list(EventType)
        # 3 generic ingress + 1 internal + 4 legacy = 8
        expected_count = 7
        self.assertEqual(len(all_events), expected_count)

        # Ensure all expected members are present
        expected_events = {
            # Generic ingress types
            EventType.COUCHDB_DOC_CHANGED,
            EventType.COUCHDB_DOC_DELETED,
            # Internal types
            EventType.PLAN_EXECUTION,
            # Legacy types (deprecated)
            EventType.PROJECT_CHANGE,
            EventType.FLOWCELL_READY,
            EventType.DELIVERY_READY,
            EventType.TEST_SCENARIO_CHANGE,
        }
        self.assertEqual(set(all_events), expected_events)

    def test_string_inheritance(self):
        """Test that EventType properly inherits from str."""
        # Test that EventType values are strings
        self.assertIsInstance(EventType.PROJECT_CHANGE, str)
        self.assertIsInstance(EventType.FLOWCELL_READY, str)
        self.assertIsInstance(EventType.DELIVERY_READY, str)
        self.assertIsInstance(EventType.PLAN_EXECUTION, str)

        # Test string operations work on the value
        self.assertTrue(EventType.PROJECT_CHANGE.value.startswith("project"))
        self.assertTrue(EventType.FLOWCELL_READY.value.endswith("ready"))
        self.assertIn("_", EventType.DELIVERY_READY.value)
        self.assertEqual(len(EventType.PLAN_EXECUTION.value), 14)

        # Test that string operations work on the enum itself (via equality comparison)
        self.assertTrue(EventType.PROJECT_CHANGE == "project_change")
        self.assertTrue(EventType.FLOWCELL_READY == "flowcell_ready")

    def test_equality_comparisons(self):
        """Test equality comparisons between enum members and strings."""
        # Test enum member equality
        self.assertEqual(EventType.PROJECT_CHANGE, EventType.PROJECT_CHANGE)
        self.assertNotEqual(EventType.PROJECT_CHANGE, EventType.FLOWCELL_READY)

        # Test string equality (since EventType inherits from str)
        self.assertEqual(EventType.PROJECT_CHANGE, "project_change")
        self.assertEqual("flowcell_ready", EventType.FLOWCELL_READY)
        self.assertNotEqual(EventType.DELIVERY_READY, "invalid_value")
        self.assertNotEqual("another_invalid", EventType.PLAN_EXECUTION)

    def test_string_representation(self):
        """Test string representations of enum members."""
        # Test str() function returns the enum name, not the value
        self.assertEqual(str(EventType.PROJECT_CHANGE), "EventType.PROJECT_CHANGE")
        self.assertEqual(str(EventType.FLOWCELL_READY), "EventType.FLOWCELL_READY")
        self.assertEqual(str(EventType.DELIVERY_READY), "EventType.DELIVERY_READY")
        self.assertEqual(str(EventType.PLAN_EXECUTION), "EventType.PLAN_EXECUTION")

        # Test repr() function
        self.assertEqual(
            repr(EventType.PROJECT_CHANGE),
            "<EventType.PROJECT_CHANGE: 'project_change'>",
        )
        self.assertIn("EventType.FLOWCELL_READY", repr(EventType.FLOWCELL_READY))

        # To get the string value, use .value attribute or direct comparison
        self.assertEqual(EventType.PROJECT_CHANGE.value, "project_change")

    def test_name_attribute(self):
        """Test that enum members have correct name attributes."""
        self.assertEqual(EventType.PROJECT_CHANGE.name, "PROJECT_CHANGE")
        self.assertEqual(EventType.FLOWCELL_READY.name, "FLOWCELL_READY")
        self.assertEqual(EventType.DELIVERY_READY.name, "DELIVERY_READY")
        self.assertEqual(EventType.PLAN_EXECUTION.name, "PLAN_EXECUTION")

    def test_value_attribute(self):
        """Test that enum members have correct value attributes."""
        self.assertEqual(EventType.PROJECT_CHANGE.value, "project_change")
        self.assertEqual(EventType.FLOWCELL_READY.value, "flowcell_ready")
        self.assertEqual(EventType.DELIVERY_READY.value, "delivery_ready")
        self.assertEqual(EventType.PLAN_EXECUTION.value, "plan_execution")

    def test_enum_creation_from_value(self):
        """Test creating enum instances from string values."""
        # Test creating from valid values
        self.assertEqual(EventType("project_change"), EventType.PROJECT_CHANGE)
        self.assertEqual(EventType("flowcell_ready"), EventType.FLOWCELL_READY)
        self.assertEqual(EventType("delivery_ready"), EventType.DELIVERY_READY)
        self.assertEqual(EventType("plan_execution"), EventType.PLAN_EXECUTION)

        # Test invalid value raises ValueError
        with self.assertRaises(ValueError):
            EventType("invalid_event_type")

        with self.assertRaises(ValueError):
            EventType("")

        with self.assertRaises(ValueError):
            EventType("PROJECT_CHANGE")  # Wrong case

    def test_enum_membership(self):
        """Test membership testing with 'in' operator."""
        # Test valid membership
        self.assertIn(EventType.PROJECT_CHANGE, EventType)
        self.assertIn(EventType.FLOWCELL_READY, EventType)
        self.assertIn(EventType.DELIVERY_READY, EventType)

        # Test membership by value
        project_change = EventType("project_change")
        self.assertIn(project_change, EventType)

    def test_iteration(self):
        """Test that EventType is iterable and returns all members."""
        event_list = list(EventType)
        expected_events = [
            # Generic ingress types
            EventType.COUCHDB_DOC_CHANGED,
            EventType.COUCHDB_DOC_DELETED,
            # Internal types
            EventType.PLAN_EXECUTION,
            # Legacy types
            EventType.PROJECT_CHANGE,
            EventType.FLOWCELL_READY,
            EventType.DELIVERY_READY,
            EventType.TEST_SCENARIO_CHANGE,
        ]

        # Check that all expected events are in the list
        for expected_event in expected_events:
            self.assertIn(expected_event, event_list)

        # Check that we have exactly the expected count
        self.assertEqual(len(event_list), len(expected_events))

    def test_hashability(self):
        """Test that EventType members are hashable and can be used as dict keys."""
        # Test that enum members can be used as dictionary keys
        event_handlers = {
            EventType.PROJECT_CHANGE: "handle_project_change",
            EventType.FLOWCELL_READY: "handle_flowcell_ready",
            EventType.DELIVERY_READY: "handle_delivery_ready",
            EventType.PLAN_EXECUTION: "handle_plan_execution",
        }

        self.assertEqual(
            event_handlers[EventType.PROJECT_CHANGE], "handle_project_change"
        )
        self.assertEqual(
            event_handlers[EventType.FLOWCELL_READY], "handle_flowcell_ready"
        )
        self.assertEqual(
            event_handlers[EventType.DELIVERY_READY], "handle_delivery_ready"
        )
        self.assertEqual(
            event_handlers[EventType.PLAN_EXECUTION], "handle_plan_execution"
        )

        # Test in sets
        event_set = {
            EventType.PROJECT_CHANGE,
            EventType.FLOWCELL_READY,
            EventType.DELIVERY_READY,
            EventType.PLAN_EXECUTION,
        }
        self.assertEqual(len(event_set), 4)
        self.assertIn(EventType.PROJECT_CHANGE, event_set)

    def test_type_checking(self):
        """Test type checking behavior."""
        # Test isinstance checks
        self.assertIsInstance(EventType.PROJECT_CHANGE, EventType)
        self.assertIsInstance(EventType.FLOWCELL_READY, EventType)
        self.assertIsInstance(EventType.DELIVERY_READY, EventType)
        self.assertIsInstance(EventType.PLAN_EXECUTION, EventType)

        # Test that string values are also instances of str
        self.assertIsInstance(EventType.PROJECT_CHANGE, str)

        # Test that regular strings are not instances of EventType
        self.assertNotIsInstance("project_change", EventType)
        self.assertNotIsInstance("random_string", EventType)

    def test_json_serialization_compatibility(self):
        """Test that EventType values can be JSON serialized."""
        import json

        # Test direct serialization of enum values
        json_str = json.dumps(EventType.PROJECT_CHANGE)
        self.assertEqual(json_str, '"project_change"')

        # Test in a dictionary
        data = {
            "event_type": EventType.FLOWCELL_READY,
            "timestamp": "2024-01-01T10:00:00",
        }
        json_str = json.dumps(data)
        parsed = json.loads(json_str)
        self.assertEqual(parsed["event_type"], "flowcell_ready")

    def test_case_sensitivity(self):
        """Test that EventType is case-sensitive."""
        # Valid values should work
        self.assertEqual(EventType("project_change"), EventType.PROJECT_CHANGE)

        # Wrong case should fail
        with self.assertRaises(ValueError):
            EventType("PROJECT_CHANGE")

        with self.assertRaises(ValueError):
            EventType("Project_Change")

        with self.assertRaises(ValueError):
            EventType("FLOWCELL_READY")

    def test_immutability(self):
        """Test that enum values cannot be modified."""
        # Enum members should be immutable
        original_value = EventType.PROJECT_CHANGE

        # Attempting to modify should raise an error or have no effect
        with self.assertRaises(AttributeError):
            EventType.PROJECT_CHANGE = "modified_value"  # type: ignore

        # Value should remain unchanged
        self.assertEqual(EventType.PROJECT_CHANGE, original_value)

    def test_comparison_operations(self):
        """Test comparison operations between enum members."""
        # Test equality
        self.assertTrue(EventType.PROJECT_CHANGE == EventType.PROJECT_CHANGE)
        self.assertFalse(EventType.PROJECT_CHANGE == EventType.FLOWCELL_READY)

        # Test inequality
        self.assertFalse(EventType.PROJECT_CHANGE != EventType.PROJECT_CHANGE)
        self.assertTrue(EventType.PROJECT_CHANGE != EventType.FLOWCELL_READY)

        # Test identity
        self.assertIs(EventType.PROJECT_CHANGE, EventType.PROJECT_CHANGE)
        self.assertIsNot(EventType.PROJECT_CHANGE, EventType.FLOWCELL_READY)

    def test_bool_evaluation(self):
        """Test boolean evaluation of enum members."""
        # All enum members should evaluate to True
        self.assertTrue(bool(EventType.PROJECT_CHANGE))
        self.assertTrue(bool(EventType.FLOWCELL_READY))
        self.assertTrue(bool(EventType.DELIVERY_READY))
        self.assertTrue(bool(EventType.PLAN_EXECUTION))

        # Should be truthy in if statements
        if EventType.PROJECT_CHANGE:
            result = True
        else:
            result = False
        self.assertTrue(result)


if __name__ == "__main__":
    unittest.main()
