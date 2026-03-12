"""
Comprehensive tests for yggdrasil.flow.utils.jsonify module.
Tests cover all conversion paths and edge cases.
"""

import unittest
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from pathlib import Path

from yggdrasil.flow.utils.jsonify import to_jsonable


class Color(Enum):
    RED = "red"
    GREEN = "green"
    BLUE = "blue"


@dataclass
class Person:
    name: str
    age: int


@dataclass
class NestedData:
    id: int
    person: Person
    tags: list[str]


class CustomObject:
    def __init__(self, value: str):
        self.value = value

    def to_dict(self):
        return {"custom_value": self.value}


class TestToJsonable(unittest.TestCase):
    """Test to_jsonable function with various input types."""

    def test_none(self):
        """None should pass through as None."""
        self.assertIsNone(to_jsonable(None))

    def test_bool_true(self):
        """Boolean True should pass through."""
        self.assertTrue(to_jsonable(True))

    def test_bool_false(self):
        """Boolean False should pass through."""
        self.assertFalse(to_jsonable(False))

    def test_int(self):
        """Integers should pass through unchanged."""
        self.assertEqual(to_jsonable(42), 42)
        self.assertEqual(to_jsonable(0), 0)
        self.assertEqual(to_jsonable(-100), -100)

    def test_float(self):
        """Floats should pass through unchanged."""
        self.assertEqual(to_jsonable(3.14), 3.14)
        self.assertEqual(to_jsonable(0.0), 0.0)
        self.assertEqual(to_jsonable(-2.5), -2.5)

    def test_str(self):
        """Strings should pass through unchanged."""
        self.assertEqual(to_jsonable("hello"), "hello")
        self.assertEqual(to_jsonable(""), "")
        self.assertEqual(to_jsonable("unicode: 你好"), "unicode: 你好")

    def test_date(self):
        """Date objects should be converted to ISO format."""
        d = date(2025, 11, 21)
        self.assertEqual(to_jsonable(d), "2025-11-21")

    def test_datetime(self):
        """Datetime objects should be converted to ISO format."""
        dt = datetime(2025, 11, 21, 14, 30, 45)
        self.assertEqual(to_jsonable(dt), "2025-11-21T14:30:45")

    def test_datetime_with_microseconds(self):
        """Datetime with microseconds should be converted to ISO format."""
        dt = datetime(2025, 11, 21, 14, 30, 45, 123456)
        self.assertEqual(to_jsonable(dt), "2025-11-21T14:30:45.123456")

    def test_enum(self):
        """Enum should be converted to its value."""
        self.assertEqual(to_jsonable(Color.RED), "red")
        self.assertEqual(to_jsonable(Color.GREEN), "green")

    def test_path(self):
        """Path objects should be converted to strings."""
        p = Path("/home/user/file.txt")
        self.assertEqual(to_jsonable(p), "/home/user/file.txt")

    def test_path_relative(self):
        """Relative paths should be converted to strings."""
        p = Path("relative/path/file.txt")
        self.assertEqual(to_jsonable(p), "relative/path/file.txt")

    def test_dataclass_simple(self):
        """Simple dataclass should be converted to dict."""
        person = Person(name="Alice", age=30)
        result = to_jsonable(person)
        self.assertEqual(result, {"name": "Alice", "age": 30})

    def test_dataclass_nested(self):
        """Nested dataclass should be fully converted."""
        data = NestedData(
            id=1, person=Person(name="Bob", age=25), tags=["python", "testing"]
        )
        result = to_jsonable(data)
        expected = {
            "id": 1,
            "person": {"name": "Bob", "age": 25},
            "tags": ["python", "testing"],
        }
        self.assertEqual(result, expected)

    def test_dict_empty(self):
        """Empty dict should pass through."""
        self.assertEqual(to_jsonable({}), {})

    def test_dict_simple(self):
        """Simple dict should have string keys."""
        data = {"key": "value", "number": 42}
        result = to_jsonable(data)
        self.assertEqual(result, {"key": "value", "number": 42})

    def test_dict_with_non_string_keys(self):
        """Dict with non-string keys should convert keys to strings."""
        data = {1: "one", 2: "two"}
        result = to_jsonable(data)
        self.assertEqual(result, {"1": "one", "2": "two"})

    def test_dict_with_enum_keys(self):
        """Dict with enum keys should convert keys to strings."""
        data = {Color.RED: "stop", Color.GREEN: "go"}
        result = to_jsonable(data)
        self.assertIn("Color.RED", result)
        self.assertIn("Color.GREEN", result)

    def test_dict_nested(self):
        """Nested dicts should be recursively converted."""
        data = {
            "user": {"name": "Charlie", "age": 35},
            "settings": {"theme": "dark", "notifications": True},
        }
        result = to_jsonable(data)
        expected = {
            "user": {"name": "Charlie", "age": 35},
            "settings": {"theme": "dark", "notifications": True},
        }
        self.assertEqual(result, expected)

    def test_dict_with_path_values(self):
        """Dict with Path values should convert them."""
        data = {"input": Path("/data/input.txt"), "output": Path("/data/output.txt")}
        result = to_jsonable(data)
        self.assertEqual(
            result, {"input": "/data/input.txt", "output": "/data/output.txt"}
        )

    def test_dict_with_datetime_values(self):
        """Dict with datetime values should convert them."""
        dt = datetime(2025, 1, 1, 12, 0, 0)
        data = {"timestamp": dt}
        result = to_jsonable(data)
        self.assertEqual(result, {"timestamp": "2025-01-01T12:00:00"})

    def test_set_empty(self):
        """Empty set should be converted to empty list."""
        self.assertEqual(to_jsonable(set()), [])

    def test_set_with_strings(self):
        """Set should be converted to sorted list for determinism."""
        data = {"zebra", "apple", "banana"}
        result = to_jsonable(data)
        self.assertEqual(result, ["apple", "banana", "zebra"])

    def test_set_with_numbers(self):
        """Set with numbers should be converted to sorted list."""
        data = {3, 1, 2}
        result = to_jsonable(data)
        # Numbers are sorted by string representation
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 3)

    def test_list_empty(self):
        """Empty list should pass through."""
        self.assertEqual(to_jsonable([]), [])

    def test_list_simple(self):
        """Simple list should pass through with conversion."""
        data = [1, "two", 3.0, None]
        result = to_jsonable(data)
        self.assertEqual(result, [1, "two", 3.0, None])

    def test_list_nested(self):
        """Nested lists should be recursively converted."""
        data = [[1, 2], [3, 4], [5, 6]]
        result = to_jsonable(data)
        self.assertEqual(result, [[1, 2], [3, 4], [5, 6]])

    def test_list_with_paths(self):
        """List with Path objects should convert them."""
        data = [Path("/file1.txt"), Path("/file2.txt")]
        result = to_jsonable(data)
        self.assertEqual(result, ["/file1.txt", "/file2.txt"])

    def test_list_with_dataclasses(self):
        """List with dataclasses should convert them."""
        data = [Person("Alice", 30), Person("Bob", 25)]
        result = to_jsonable(data)
        expected = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
        self.assertEqual(result, expected)

    def test_tuple(self):
        """Tuples should be converted to lists."""
        data = (1, "two", 3.0)
        result = to_jsonable(data)
        self.assertEqual(result, [1, "two", 3.0])

    def test_tuple_nested(self):
        """Nested tuples should be converted to nested lists."""
        data = ((1, 2), (3, 4))
        result = to_jsonable(data)
        self.assertEqual(result, [[1, 2], [3, 4]])

    def test_bytes_converted_to_string(self):
        """Bytes should be converted to string."""
        data = b"hello"
        result = to_jsonable(data)
        self.assertEqual(result, "b'hello'")

    def test_bytearray_converted_to_string(self):
        """Bytearray should be converted to string."""
        data = bytearray(b"hello")
        result = to_jsonable(data)
        self.assertIsInstance(result, str)

    def test_custom_object_with_to_dict(self):
        """Object with to_dict method should use it."""
        obj = CustomObject("test_value")
        result = to_jsonable(obj)
        self.assertEqual(result, {"custom_value": "test_value"})

    def test_custom_object_without_to_dict(self):
        """Object without to_dict should be converted to string."""

        class SimpleObject:
            pass

        obj = SimpleObject()
        result = to_jsonable(obj)
        self.assertIsInstance(result, str)
        self.assertIn("SimpleObject", result)

    def test_complex_nested_structure(self):
        """Complex nested structure should be fully converted."""
        data = {
            "metadata": {
                "created": datetime(2025, 11, 21, 10, 30),
                "author": Person("David", 40),
                "status": Color.GREEN,
            },
            "files": [Path("/data/file1.txt"), Path("/data/file2.txt")],
            "tags": {"python", "testing", "automation"},
            "nested_list": [[1, 2], [3, 4]],
        }
        result = to_jsonable(data)

        self.assertIsInstance(result, dict)
        self.assertEqual(result["metadata"]["created"], "2025-11-21T10:30:00")
        self.assertEqual(result["metadata"]["author"], {"name": "David", "age": 40})
        self.assertEqual(result["metadata"]["status"], "green")
        self.assertEqual(result["files"], ["/data/file1.txt", "/data/file2.txt"])
        self.assertIsInstance(result["tags"], list)
        self.assertEqual(len(result["tags"]), 3)
        self.assertEqual(result["nested_list"], [[1, 2], [3, 4]])

    def test_dataclass_class_not_instance(self):
        """Dataclass class (not instance) should be converted to string."""
        result = to_jsonable(Person)
        self.assertIsInstance(result, str)
        self.assertIn("Person", result)

    def test_int_subclass(self):
        """Int subclass should pass through as int."""

        class MyInt(int):
            pass

        result = to_jsonable(MyInt(42))
        self.assertEqual(result, 42)

    def test_str_subclass(self):
        """String subclass should pass through as string."""

        class MyStr(str):
            pass

        result = to_jsonable(MyStr("hello"))
        self.assertEqual(result, "hello")

    def test_dict_with_mixed_types(self):
        """Dict with various value types should all be converted."""
        data = {
            "string": "value",
            "number": 42,
            "float": 3.14,
            "bool": True,
            "none": None,
            "date": date(2025, 11, 21),
            "path": Path("/file.txt"),
            "list": [1, 2, 3],
            "nested": {"key": "value"},
        }
        result = to_jsonable(data)

        self.assertEqual(result["string"], "value")
        self.assertEqual(result["number"], 42)
        self.assertEqual(result["float"], 3.14)
        self.assertTrue(result["bool"])
        self.assertIsNone(result["none"])
        self.assertEqual(result["date"], "2025-11-21")
        self.assertEqual(result["path"], "/file.txt")
        self.assertEqual(result["list"], [1, 2, 3])
        self.assertEqual(result["nested"], {"key": "value"})

    def test_recursive_dataclass_conversion(self):
        """Verify dataclass conversion uses asdict and recurses."""

        @dataclass
        class Inner:
            path: Path

        @dataclass
        class Outer:
            inner: Inner

        obj = Outer(inner=Inner(path=Path("/test.txt")))
        result = to_jsonable(obj)
        self.assertEqual(result, {"inner": {"path": "/test.txt"}})

    def test_empty_string_in_dict_key(self):
        """Empty string as dict key should be preserved."""
        data = {"": "empty_key_value"}
        result = to_jsonable(data)
        self.assertEqual(result, {"": "empty_key_value"})

    def test_large_nested_list(self):
        """Large nested list should be handled correctly."""
        data = [[i, i + 1] for i in range(100)]
        result = to_jsonable(data)
        self.assertEqual(len(result), 100)
        self.assertEqual(result[0], [0, 1])
        self.assertEqual(result[99], [99, 100])


if __name__ == "__main__":
    unittest.main()
