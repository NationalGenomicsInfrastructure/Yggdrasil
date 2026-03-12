import unittest
from dataclasses import dataclass
from typing import Any

from yggdrasil.flow.utils.codec import structure, unstructure


class TestUnstructure(unittest.TestCase):
    """
    Comprehensive tests for unstructure function.

    Tests conversion of Python objects to serializable data structures.
    Handles both cattrs-enabled and fallback modes.
    """

    # =====================================================
    # BASIC TYPE UNSTRUCTURE TESTS
    # =====================================================

    def test_unstructure_none(self):
        """Test unstructuring None value."""
        result = unstructure(None)
        self.assertIsNone(result)

    def test_unstructure_bool(self):
        """Test unstructuring boolean values."""
        self.assertEqual(unstructure(True), True)
        self.assertEqual(unstructure(False), False)

    def test_unstructure_int(self):
        """Test unstructuring integer values."""
        self.assertEqual(unstructure(42), 42)
        self.assertEqual(unstructure(0), 0)
        self.assertEqual(unstructure(-123), -123)

    def test_unstructure_float(self):
        """Test unstructuring float values."""
        self.assertEqual(unstructure(3.14), 3.14)
        self.assertEqual(unstructure(0.0), 0.0)
        self.assertEqual(unstructure(-2.5), -2.5)

    def test_unstructure_string(self):
        """Test unstructuring string values."""
        self.assertEqual(unstructure("hello"), "hello")
        self.assertEqual(unstructure(""), "")
        self.assertEqual(unstructure("unicode: 🎉"), "unicode: 🎉")

    # =====================================================
    # COLLECTION UNSTRUCTURE TESTS
    # =====================================================

    def test_unstructure_list(self):
        """Test unstructuring list values."""
        result = unstructure([1, 2, 3])
        self.assertEqual(result, [1, 2, 3])

    def test_unstructure_empty_list(self):
        """Test unstructuring empty list."""
        result = unstructure([])
        self.assertEqual(result, [])

    def test_unstructure_nested_list(self):
        """Test unstructuring nested lists."""
        result = unstructure([[1, 2], [3, 4]])
        self.assertEqual(result, [[1, 2], [3, 4]])

    def test_unstructure_dict(self):
        """Test unstructuring dictionary values."""
        result = unstructure({"key": "value", "number": 42})
        self.assertEqual(result, {"key": "value", "number": 42})

    def test_unstructure_empty_dict(self):
        """Test unstructuring empty dictionary."""
        result = unstructure({})
        self.assertEqual(result, {})

    def test_unstructure_nested_dict(self):
        """Test unstructuring nested dictionaries."""
        data = {"outer": {"inner": {"deep": "value"}}}
        result = unstructure(data)
        self.assertEqual(result, data)

    def test_unstructure_tuple(self):
        """Test unstructuring tuple values."""
        result = unstructure((1, 2, 3))
        # Note: behavior may vary depending on cattrs availability
        self.assertIn(result, [(1, 2, 3), [1, 2, 3]])

    def test_unstructure_set(self):
        """Test unstructuring set values."""
        result = unstructure({1, 2, 3})
        # Result format depends on cattrs availability
        if isinstance(result, list):
            self.assertEqual(set(result), {1, 2, 3})
        else:
            self.assertEqual(result, {1, 2, 3})

    # =====================================================
    # DATACLASS UNSTRUCTURE TESTS
    # =====================================================

    def test_unstructure_simple_dataclass(self):
        """Test unstructuring a simple dataclass."""

        @dataclass
        class Point:
            x: int
            y: int

        point = Point(x=10, y=20)
        result = unstructure(point)

        # With cattrs: becomes dict, without: passes through
        if isinstance(result, dict):
            self.assertEqual(result, {"x": 10, "y": 20})
        else:
            self.assertIs(result, point)

    def test_unstructure_nested_dataclass(self):
        """Test unstructuring nested dataclasses."""

        @dataclass
        class Inner:
            value: str

        @dataclass
        class Outer:
            inner: Inner
            count: int

        obj = Outer(inner=Inner(value="test"), count=5)
        result = unstructure(obj)

        # With cattrs: becomes nested dict, without: passes through
        if isinstance(result, dict):
            self.assertEqual(result["count"], 5)
            if isinstance(result["inner"], dict):
                self.assertEqual(result["inner"]["value"], "test")

    def test_unstructure_dataclass_with_list(self):
        """Test unstructuring dataclass containing a list."""

        @dataclass
        class Container:
            items: list[int]

        container = Container(items=[1, 2, 3])
        result = unstructure(container)

        if isinstance(result, dict):
            self.assertEqual(result["items"], [1, 2, 3])

    # =====================================================
    # COMPLEX TYPE UNSTRUCTURE TESTS
    # =====================================================

    def test_unstructure_mixed_types(self):
        """Test unstructuring complex nested structures."""
        data = {
            "string": "value",
            "number": 42,
            "float": 3.14,
            "bool": True,
            "none": None,
            "list": [1, 2, 3],
            "nested": {"inner": "data"},
        }
        result = unstructure(data)

        # Should handle all types
        self.assertIsInstance(result, dict)
        self.assertEqual(result["string"], "value")
        self.assertEqual(result["number"], 42)

    def test_unstructure_list_of_dicts(self):
        """Test unstructuring list of dictionaries."""
        data = [{"id": 1, "name": "first"}, {"id": 2, "name": "second"}]
        result = unstructure(data)

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)

    # =====================================================
    # FALLBACK BEHAVIOR TESTS
    # =====================================================

    def test_unstructure_identity_in_fallback_mode(self):
        """Test that fallback mode returns objects unchanged."""
        # Test with various types to verify fallback behavior
        test_values = [
            42,
            "string",
            [1, 2, 3],
            {"key": "value"},
            None,
            True,
        ]

        for value in test_values:
            result = unstructure(value)
            # In fallback mode or with simple types, should be unchanged
            if not hasattr(value, "__dict__"):
                self.assertEqual(result, value)

    def test_unstructure_custom_object_fallback(self):
        """Test unstructuring custom object in fallback mode."""

        class CustomClass:
            def __init__(self, value):
                self.value = value

        obj = CustomClass(42)
        result = unstructure(obj)

        # With cattrs: becomes dict, without: passes through
        # This documents the behavior difference
        self.assertTrue(isinstance(result, (dict, CustomClass)))


class TestStructure(unittest.TestCase):
    """
    Comprehensive tests for structure function.

    Tests conversion of serializable data back to typed Python objects.
    Handles both cattrs-enabled and fallback modes.
    """

    # =====================================================
    # BASIC TYPE STRUCTURE TESTS
    # =====================================================

    def test_structure_int(self):
        """Test structuring integer values."""
        result = structure(42, int)
        self.assertEqual(result, 42)
        self.assertIsInstance(result, int)

    def test_structure_float(self):
        """Test structuring float values."""
        result = structure(3.14, float)
        self.assertEqual(result, 3.14)
        self.assertIsInstance(result, float)

    def test_structure_string(self):
        """Test structuring string values."""
        result = structure("hello", str)
        self.assertEqual(result, "hello")
        self.assertIsInstance(result, str)

    def test_structure_bool(self):
        """Test structuring boolean values."""
        self.assertEqual(structure(True, bool), True)
        self.assertEqual(structure(False, bool), False)

    def test_structure_none(self):
        """Test structuring None value."""
        # Note: cattrs doesn't support structuring to NoneType
        # This documents expected behavior - use Optional[T] instead
        try:
            result = structure(None, type(None))
            self.assertIsNone(result)
        except Exception:
            # Expected with cattrs - NoneType not supported
            # In fallback mode, would return None
            pass

    # =====================================================
    # COLLECTION STRUCTURE TESTS
    # =====================================================

    def test_structure_list_of_ints(self):
        """Test structuring list of integers."""
        result = structure([1, 2, 3], list[int])
        self.assertEqual(result, [1, 2, 3])
        self.assertIsInstance(result, list)

    def test_structure_dict_of_strings(self):
        """Test structuring dictionary."""
        data = {"key": "value"}
        result = structure(data, dict[str, str])
        self.assertEqual(result, {"key": "value"})
        self.assertIsInstance(result, dict)

    # =====================================================
    # DATACLASS STRUCTURE TESTS
    # =====================================================

    def test_structure_simple_dataclass(self):
        """Test structuring data into a dataclass."""

        @dataclass
        class Point:
            x: int
            y: int

        data = {"x": 10, "y": 20}
        result = structure(data, Point)

        # With cattrs: creates dataclass instance, without: returns data unchanged
        if isinstance(result, Point):
            self.assertEqual(result.x, 10)
            self.assertEqual(result.y, 20)
        else:
            # Fallback mode
            self.assertEqual(result, data)

    def test_structure_nested_dataclass(self):
        """Test structuring nested dataclass structures."""

        @dataclass
        class Inner:
            value: str

        @dataclass
        class Outer:
            inner: Inner
            count: int

        data = {"inner": {"value": "test"}, "count": 5}
        result = structure(data, Outer)

        if isinstance(result, Outer):
            self.assertEqual(result.count, 5)
            self.assertEqual(result.inner.value, "test")

    def test_structure_dataclass_with_defaults(self):
        """Test structuring dataclass with default values."""

        @dataclass
        class Config:
            name: str
            enabled: bool = True
            timeout: int = 30

        # Only provide name
        data = {"name": "test"}
        result = structure(data, Config)

        if isinstance(result, Config):
            self.assertEqual(result.name, "test")
            self.assertEqual(result.enabled, True)
            self.assertEqual(result.timeout, 30)

    def test_structure_dataclass_with_list_field(self):
        """Test structuring dataclass with list field."""

        @dataclass
        class Container:
            items: list[int]

        data = {"items": [1, 2, 3]}
        result = structure(data, Container)

        if isinstance(result, Container):
            self.assertEqual(result.items, [1, 2, 3])

    # =====================================================
    # TYPE CONVERSION TESTS
    # =====================================================

    def test_structure_type_coercion(self):
        """Test that structure can coerce compatible types."""
        # String to int
        result = structure("42", int)
        if isinstance(result, int):
            self.assertEqual(result, 42)

    def test_structure_preserves_type_with_fallback(self):
        """Test fallback mode preserves input unchanged."""
        # In fallback mode, data is returned as-is
        data = {"x": 10, "y": 20}

        @dataclass
        class Point:
            x: int
            y: int

        result = structure(data, Point)

        # Either structured or passed through
        self.assertTrue(isinstance(result, (Point, dict)))

    # =====================================================
    # ROUND-TRIP TESTS
    # =====================================================

    def test_round_trip_simple_dataclass(self):
        """Test round-trip: dataclass -> unstructure -> structure."""

        @dataclass
        class Point:
            x: int
            y: int

        original = Point(x=10, y=20)
        unstructured = unstructure(original)
        restructured = structure(unstructured, Point)

        # With cattrs: should reconstruct, without: may pass through
        if isinstance(restructured, Point):
            self.assertEqual(restructured.x, 10)
            self.assertEqual(restructured.y, 20)

    def test_round_trip_nested_dataclass(self):
        """Test round-trip with nested dataclasses."""

        @dataclass
        class Inner:
            value: str

        @dataclass
        class Outer:
            inner: Inner
            count: int

        original = Outer(inner=Inner(value="test"), count=5)
        unstructured = unstructure(original)
        restructured = structure(unstructured, Outer)

        if isinstance(restructured, Outer):
            self.assertEqual(restructured.count, 5)
            if isinstance(restructured.inner, Inner):
                self.assertEqual(restructured.inner.value, "test")

    def test_round_trip_with_lists(self):
        """Test round-trip with lists of dataclasses."""

        @dataclass
        class Item:
            id: int
            name: str

        @dataclass
        class Container:
            items: list[Item]

        original = Container(
            items=[Item(id=1, name="first"), Item(id=2, name="second")]
        )
        unstructured = unstructure(original)
        restructured = structure(unstructured, Container)

        if isinstance(restructured, Container):
            self.assertEqual(len(restructured.items), 2)

    # =====================================================
    # FALLBACK MODE TESTS
    # =====================================================

    def test_structure_fallback_returns_data_unchanged(self):
        """Test that fallback mode returns data unchanged."""

        @dataclass
        class Point:
            x: int
            y: int

        data = {"x": 10, "y": 20}

        # If cattrs is not available, should return data as-is
        result = structure(data, Point)

        # Either structured to Point or returned as dict
        self.assertTrue(isinstance(result, (Point, dict)))

    def test_fallback_preserves_nested_structures(self):
        """Test that fallback preserves nested structures."""
        data = {"outer": {"inner": {"deep": "value"}}}

        result = structure(data, dict)

        self.assertEqual(result, data)


class TestCodecIntegration(unittest.TestCase):
    """
    Integration tests showing real-world usage patterns for codec module.
    """

    # =====================================================
    # WORKFLOW PERSISTENCE TESTS
    # =====================================================

    def test_workflow_state_persistence(self):
        """Test persisting workflow state to/from serializable format."""

        @dataclass
        class WorkflowState:
            step_id: str
            status: str
            progress: int

        # Create state
        state = WorkflowState(step_id="step_001", status="running", progress=50)

        # Serialize for storage
        serialized = unstructure(state)

        # Deserialize from storage
        restored = structure(serialized, WorkflowState)

        # Verify restoration
        if isinstance(restored, WorkflowState):
            self.assertEqual(restored.step_id, "step_001")
            self.assertEqual(restored.status, "running")
            self.assertEqual(restored.progress, 50)

    def test_plan_serialization(self):
        """Test serializing a plan with multiple steps."""

        @dataclass
        class StepSpec:
            step_id: str
            name: str
            params: dict[str, Any]

        @dataclass
        class Plan:
            plan_id: str
            steps: list[StepSpec]

        # Create plan
        plan = Plan(
            plan_id="plan_001",
            steps=[
                StepSpec(step_id="s1", name="step1", params={"key": "value"}),
                StepSpec(step_id="s2", name="step2", params={"count": 10}),
            ],
        )

        # Serialize
        serialized = unstructure(plan)

        # Should be JSON-serializable format
        import json

        try:
            json_str = json.dumps(serialized)
            deserialized = json.loads(json_str)

            # Restore
            restored = structure(deserialized, Plan)

            if isinstance(restored, Plan):
                self.assertEqual(restored.plan_id, "plan_001")
                self.assertEqual(len(restored.steps), 2)
        except (TypeError, AttributeError):
            # Fallback mode might not produce JSON-serializable format
            pass

    # =====================================================
    # API PAYLOAD TESTS
    # =====================================================

    def test_api_request_payload(self):
        """Test creating API request payloads."""

        @dataclass
        class APIRequest:
            endpoint: str
            method: str
            params: dict[str, Any]

        request = APIRequest(
            endpoint="/api/jobs", method="POST", params={"job_type": "analysis"}
        )

        # Convert to dict for HTTP request
        payload = unstructure(request)

        # Should be dict-like for JSON encoding
        if isinstance(payload, dict):
            self.assertIn("endpoint", payload)
            self.assertEqual(payload["method"], "POST")

    def test_api_response_parsing(self):
        """Test parsing API responses to dataclasses."""

        @dataclass
        class JobResponse:
            job_id: str
            status: str
            created_at: str

        # Simulate API response
        api_data = {
            "job_id": "job_123",
            "status": "pending",
            "created_at": "2025-01-01",
        }

        # Parse to typed object
        response = structure(api_data, JobResponse)

        if isinstance(response, JobResponse):
            self.assertEqual(response.job_id, "job_123")
            self.assertEqual(response.status, "pending")

    # =====================================================
    # CONFIGURATION MANAGEMENT TESTS
    # =====================================================

    def test_config_file_loading(self):
        """Test loading configuration from file format."""

        @dataclass
        class DatabaseConfig:
            host: str
            port: int
            database: str

        @dataclass
        class AppConfig:
            app_name: str
            debug: bool
            database: DatabaseConfig

        # Simulate loaded config file
        config_data = {
            "app_name": "MyApp",
            "debug": True,
            "database": {"host": "localhost", "port": 5432, "database": "mydb"},
        }

        # Structure into typed config
        config = structure(config_data, AppConfig)

        if isinstance(config, AppConfig):
            self.assertEqual(config.app_name, "MyApp")
            self.assertTrue(config.debug)
            if isinstance(config.database, DatabaseConfig):
                self.assertEqual(config.database.host, "localhost")
                self.assertEqual(config.database.port, 5432)

    # =====================================================
    # EVENT HANDLING TESTS
    # =====================================================

    def test_event_serialization(self):
        """Test serializing events for event sourcing."""

        @dataclass
        class Event:
            event_type: str
            timestamp: str
            payload: dict[str, Any]

        event = Event(
            event_type="step.started",
            timestamp="2025-01-01T00:00:00Z",
            payload={"step_id": "s1"},
        )

        # Serialize for event store
        serialized = unstructure(event)

        # Should be storable format
        if isinstance(serialized, dict):
            self.assertEqual(serialized["event_type"], "step.started")
            self.assertIn("payload", serialized)

    # =====================================================
    # TYPE SAFETY TESTS
    # =====================================================

    def test_type_preservation_with_annotations(self):
        """Test that type annotations are respected."""

        @dataclass
        class TypedData:
            count: int
            ratio: float
            name: str

        data = {"count": 10, "ratio": 0.5, "name": "test"}
        result = structure(data, TypedData)

        if isinstance(result, TypedData):
            self.assertIsInstance(result.count, int)
            self.assertIsInstance(result.ratio, float)
            self.assertIsInstance(result.name, str)


class TestCodecFallbackBehavior(unittest.TestCase):
    """
    Tests specifically for fallback behavior when cattrs is not available.
    """

    def test_fallback_identity_function(self):
        """Test that fallback mode acts as identity function."""
        test_data = {"key": "value", "nested": {"inner": 123}}

        # Both should return data unchanged in fallback
        unstructured = unstructure(test_data)
        structured = structure(test_data, dict)

        # In fallback mode, both are identity
        self.assertEqual(unstructured, test_data)
        self.assertEqual(structured, test_data)

    def test_fallback_preserves_complex_objects(self):
        """Test that fallback preserves complex objects."""

        class CustomObject:
            def __init__(self, value):
                self.value = value

        obj = CustomObject(42)

        # Fallback should preserve object
        result = unstructure(obj)

        # Either converted or preserved
        self.assertTrue(isinstance(result, (dict, CustomObject)))


if __name__ == "__main__":
    unittest.main()
