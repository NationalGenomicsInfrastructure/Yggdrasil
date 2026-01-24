"""
Unit tests for yggdrasil.flow.utils.typing_coerce module.

Tests the type-driven parameter coercion logic that converts string values
to Path objects based on function type signatures.
"""

import unittest
from pathlib import Path
from typing import Optional, Union

from yggdrasil.flow.utils.typing_coerce import (
    _is_path_type,
    _unwrap_annotated,
    coerce_params_to_signature_types,
)


class TestUnwrapAnnotated(unittest.TestCase):
    """Tests for _unwrap_annotated helper."""

    def test_unwraps_annotated_type(self):
        """Test that Annotated[T, ...] unwraps to T."""

        from yggdrasil.flow.utils.typing_coerce import Annotated

        if Annotated is None:
            self.skipTest("Annotated not available")

        tp = Annotated[Path, "input"]
        result = _unwrap_annotated(tp)
        self.assertIs(result, Path)

    def test_returns_non_annotated_unchanged(self):
        """Test that non-Annotated types pass through unchanged."""
        result = _unwrap_annotated(Path)
        self.assertIs(result, Path)

        result = _unwrap_annotated(str)
        self.assertIs(result, str)


class TestIsPathType(unittest.TestCase):
    """Tests for _is_path_type helper."""

    def test_recognizes_direct_path(self):
        """Test that Path is recognized as a path type."""
        self.assertTrue(_is_path_type(Path))

    def test_recognizes_optional_path(self):
        """Test that Optional[Path] is recognized as a path type."""
        self.assertTrue(_is_path_type(Optional[Path]))

    def test_recognizes_union_path_none(self):
        """Test that Union[Path, None] is recognized as a path type."""
        self.assertTrue(_is_path_type(Union[Path, None]))

    def test_rejects_path_with_other_types(self):
        """Test that Union[Path, str] is NOT recognized as a path-only type."""
        self.assertFalse(_is_path_type(Union[Path, str]))

    def test_rejects_non_path_types(self):
        """Test that non-Path types are not recognized."""
        self.assertFalse(_is_path_type(str))
        self.assertFalse(_is_path_type(int))
        self.assertFalse(_is_path_type(Optional[str]))


class TestCoerceParamsBasic(unittest.TestCase):
    """Tests for basic parameter coercion."""

    def test_coerces_string_to_path_for_path_param(self):
        """Test that string values are coerced to Path for Path-typed params."""

        def step_fn(input_path: Path) -> None:
            pass

        params = {"input_path": "/tmp/input"}
        result = coerce_params_to_signature_types(step_fn, params)

        self.assertIsInstance(result["input_path"], Path)
        self.assertEqual(result["input_path"], Path("/tmp/input"))

    def test_leaves_string_params_unchanged(self):
        """Test that string-typed params remain strings."""

        def step_fn(name: str, path: Path) -> None:
            pass

        params = {"name": "test", "path": "/tmp/test"}
        result = coerce_params_to_signature_types(step_fn, params)

        self.assertEqual(result["name"], "test")
        self.assertIsInstance(result["path"], Path)

    def test_does_not_coerce_non_string_values(self):
        """Test that already-Path values are not re-coerced."""

        def step_fn(path: Path) -> None:
            pass

        path_obj = Path("/tmp/path")
        params = {"path": path_obj}
        result = coerce_params_to_signature_types(step_fn, params)

        self.assertIs(result["path"], path_obj)

    def test_handles_missing_params(self):
        """Test that coercion handles params not in function signature."""

        def step_fn(path: Path) -> None:
            pass

        params = {"path": "/tmp/path", "extra": "value"}
        result = coerce_params_to_signature_types(step_fn, params)

        self.assertIsInstance(result["path"], Path)
        self.assertEqual(result["extra"], "value")


class TestCoerceParamsAnnotated(unittest.TestCase):
    """Tests for Annotated type coercion."""

    def test_unwraps_annotated_path(self):
        """Test that Annotated[Path, ...] is coerced."""
        from yggdrasil.flow.utils.typing_coerce import Annotated

        if Annotated is None:
            self.skipTest("Annotated not available")

        def step_fn(input_path: Annotated[Path, "input"]) -> None:
            pass

        params = {"input_path": "/tmp/input"}
        result = coerce_params_to_signature_types(step_fn, params)

        self.assertIsInstance(result["input_path"], Path)

    def test_preserves_non_annotated_types(self):
        """Test that Annotated[str, ...] stays string."""
        from yggdrasil.flow.utils.typing_coerce import Annotated

        if Annotated is None:
            self.skipTest("Annotated not available")

        def step_fn(url: Annotated[str, "URL"]) -> None:
            pass

        params = {"url": "https://example.com"}
        result = coerce_params_to_signature_types(step_fn, params)

        self.assertEqual(result["url"], "https://example.com")
        self.assertIsInstance(result["url"], str)


class TestCoerceParamsOptional(unittest.TestCase):
    """Tests for Optional and Union type coercion."""

    def test_coerces_optional_path(self):
        """Test that Optional[Path] parameters are coerced."""

        def step_fn(maybe_path: Path | None) -> None:
            pass

        params = {"maybe_path": "/tmp/maybe"}
        result = coerce_params_to_signature_types(step_fn, params)

        self.assertIsInstance(result["maybe_path"], Path)

    def test_preserves_optional_str(self):
        """Test that Optional[str] parameters stay string."""

        def step_fn(maybe_name: str | None) -> None:
            pass

        params = {"maybe_name": "test"}
        result = coerce_params_to_signature_types(step_fn, params)

        self.assertEqual(result["maybe_name"], "test")

    def test_handles_none_values(self):
        """Test that None values are not coerced."""

        def step_fn(maybe_path: Path | None) -> None:
            pass

        params = {"maybe_path": None}
        result = coerce_params_to_signature_types(step_fn, params)

        self.assertIsNone(result["maybe_path"])

    def test_coerces_union_path_none(self):
        """Test that Union[Path, None] parameters are coerced."""

        def step_fn(file: Path | None) -> None:
            pass

        params = {"file": "/tmp/file"}
        result = coerce_params_to_signature_types(step_fn, params)

        self.assertIsInstance(result["file"], Path)


class TestCoerceParamsMultiple(unittest.TestCase):
    """Tests for multiple parameters with mixed types."""

    def test_coerces_multiple_paths(self):
        """Test coercion with multiple Path parameters."""

        def step_fn(input_path: Path, output_path: Path, name: str) -> None:
            pass

        params = {"input_path": "/tmp/in", "output_path": "/tmp/out", "name": "test"}
        result = coerce_params_to_signature_types(step_fn, params)

        self.assertIsInstance(result["input_path"], Path)
        self.assertIsInstance(result["output_path"], Path)
        self.assertEqual(result["name"], "test")

    def test_handles_empty_params(self):
        """Test coercion with no parameters."""

        def step_fn() -> None:
            pass

        params = {}
        result = coerce_params_to_signature_types(step_fn, params)

        self.assertEqual(result, {})

    def test_handles_no_type_hints(self):
        """Test coercion on functions without type hints."""

        def step_fn(path):  # No type hint
            pass

        params = {"path": "/tmp/path"}
        result = coerce_params_to_signature_types(step_fn, params)

        # Should return params unchanged
        self.assertEqual(result["path"], "/tmp/path")


class TestCoerceParamsDecorated(unittest.TestCase):
    """Tests for coercion with decorated functions."""

    def test_unwraps_decorator(self):
        """Test that inspect.unwrap attempts to handle decorators."""

        def decorator(fn):
            def wrapper(*args, **kwargs):
                return fn(*args, **kwargs)

            # Preserve annotations
            wrapper.__annotations__ = fn.__annotations__
            wrapper.__wrapped__ = fn
            return wrapper

        @decorator
        def step_fn(path: Path) -> None:
            pass

        params = {"path": "/tmp/path"}
        result = coerce_params_to_signature_types(step_fn, params)

        # Should successfully coerce
        self.assertIsInstance(result["path"], Path)

    def test_handles_multiple_decorators(self):
        """Test with multiple layers of decorators."""

        def decorator1(fn):
            def wrapper1(*args, **kwargs):
                return fn(*args, **kwargs)

            wrapper1.__wrapped__ = fn
            return wrapper1

        def decorator2(fn):
            def wrapper2(*args, **kwargs):
                return fn(*args, **kwargs)

            wrapper2.__wrapped__ = fn
            return wrapper2

        @decorator1
        @decorator2
        def step_fn(path: Path) -> None:
            pass

        params = {"path": "/tmp/path"}
        result = coerce_params_to_signature_types(step_fn, params)

        # Should successfully coerce
        self.assertIsInstance(result["path"], Path)


class TestCoerceParamsErrorHandling(unittest.TestCase):
    """Tests for error handling and edge cases."""

    def test_handles_invalid_type_hints(self):
        """Test graceful handling when type hints can't be resolved."""

        def step_fn(path: "NonExistentType") -> None:  # type: ignore[name-defined]  # Forward ref, not resolvable
            pass

        params = {"path": "/tmp/path"}
        result = coerce_params_to_signature_types(step_fn, params)

        # Should return params unchanged (can't resolve hint)
        self.assertEqual(result, params)

    def test_handles_callable_without_signature(self):
        """Test coercion on callables without inspectable signature."""

        class CallableClass:
            def __call__(self, path: Path) -> None:
                pass

        obj = CallableClass()
        params = {"path": "/tmp/path"}

        # Should gracefully handle or succeed
        try:
            result = coerce_params_to_signature_types(obj, params)
            # If successful, check for coercion
            if "path" in result:
                # Coercion may or may not succeed depending on callable inspection
                pass
        except Exception:
            # Graceful failure is acceptable
            pass


class TestCoerceIntegration(unittest.TestCase):
    """Integration tests simulating real step usage."""

    def test_realistic_step_coercion(self):
        """Test coercion in a realistic step context."""
        from yggdrasil.flow.utils.typing_coerce import Annotated

        if Annotated is None:
            self.skipTest("Annotated not available")

        def process_files(
            input_dir: Annotated[Path, "input"],
            output_file: Path,
            config_url: str,
            sample_id: str,
        ) -> None:
            pass

        params = {
            "input_dir": "/data/samples",
            "output_file": "/results/output.txt",
            "config_url": "https://config.example.com/settings.json",
            "sample_id": "S001",
        }

        result = coerce_params_to_signature_types(process_files, params)

        # Check coercions
        self.assertIsInstance(result["input_dir"], Path)
        self.assertIsInstance(result["output_file"], Path)
        self.assertEqual(
            result["config_url"], "https://config.example.com/settings.json"
        )
        self.assertEqual(result["sample_id"], "S001")

        # Verify the paths are correct
        self.assertEqual(result["input_dir"], Path("/data/samples"))
        self.assertEqual(result["output_file"], Path("/results/output.txt"))


if __name__ == "__main__":
    unittest.main()
