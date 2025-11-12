import unittest
from functools import wraps
from unittest.mock import MagicMock

from yggdrasil.flow.utils.callable_ref import fn_ref_from_callable, resolve_callable


class TestFnRefFromCallable(unittest.TestCase):
    """
    Comprehensive tests for fn_ref_from_callable function.

    Tests conversion of callable functions to their string reference format
    'module:function_name', including decorator unwrapping behavior.
    """

    # =====================================================
    # BASIC FUNCTION REFERENCE TESTS
    # =====================================================

    def test_simple_function_reference(self):
        """Test converting a simple function to reference string."""

        def simple_function():
            pass

        ref = fn_ref_from_callable(simple_function)

        # Should be in format 'module:func_name'
        self.assertIn(":", ref)
        self.assertTrue(ref.endswith(":simple_function"))
        self.assertIn("test_flow_utils_callable_ref", ref)

    def test_module_function_reference(self):
        """Test converting a module-level function to reference."""
        # Use a known module function
        import math

        ref = fn_ref_from_callable(math.sqrt)

        # Should reference the math module
        self.assertEqual(ref, "math:sqrt")

    def test_lambda_function_reference(self):
        """Test converting a lambda function to reference."""
        lambda_func = lambda x: x * 2

        ref = fn_ref_from_callable(lambda_func)

        # Lambdas have special name '<lambda>'
        self.assertTrue(ref.endswith(":<lambda>"))

    def test_builtin_function_reference(self):
        """Test converting a built-in function to reference."""
        ref = fn_ref_from_callable(len)

        # Built-ins are in builtins module
        self.assertEqual(ref, "builtins:len")

    # =====================================================
    # DECORATOR UNWRAPPING TESTS
    # =====================================================

    def test_decorated_function_unwraps(self):
        """Test that decorated functions are unwrapped to get original."""

        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            return wrapper

        @decorator
        def decorated_function():
            pass

        ref = fn_ref_from_callable(decorated_function)

        # Should unwrap and get the original function name
        self.assertTrue(ref.endswith(":decorated_function"))

    def test_multiple_decorators_unwraps_all(self):
        """Test that multiple layers of decorators are fully unwrapped."""

        def decorator1(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            return wrapper

        def decorator2(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            return wrapper

        @decorator1
        @decorator2
        def multi_decorated():
            pass

        ref = fn_ref_from_callable(multi_decorated)

        # Should unwrap all layers
        self.assertTrue(ref.endswith(":multi_decorated"))

    def test_decorator_without_wraps_still_unwraps(self):
        """Test unwrapping even when decorator doesn't use functools.wraps."""

        def bad_decorator(func):
            # Doesn't use @wraps, but inspect.unwrap can still handle it
            # if it sets __wrapped__
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            wrapper.__wrapped__ = func  # Manual setting
            return wrapper

        @bad_decorator
        def original_function():
            pass

        ref = fn_ref_from_callable(original_function)

        # Should still unwrap via __wrapped__ attribute
        self.assertTrue(ref.endswith(":original_function"))

    # =====================================================
    # CLASS METHOD AND STATIC METHOD TESTS
    # =====================================================

    def test_static_method_reference(self):
        """Test converting a static method to reference."""

        class TestClass:
            @staticmethod
            def static_method():
                pass

        ref = fn_ref_from_callable(TestClass.static_method)

        # Should reference the static method
        self.assertTrue(ref.endswith(":static_method"))

    def test_class_method_reference(self):
        """Test converting a class method to reference."""

        class TestClass:
            @classmethod
            def class_method(cls):
                pass

        ref = fn_ref_from_callable(TestClass.class_method)

        # Should reference the class method
        self.assertTrue(ref.endswith(":class_method"))

    def test_instance_method_reference(self):
        """Test converting an instance method to reference."""

        class TestClass:
            def instance_method(self):
                pass

        instance = TestClass()
        ref = fn_ref_from_callable(instance.instance_method)

        # Should reference the method name
        self.assertTrue(ref.endswith(":instance_method"))

    # =====================================================
    # FORMAT VALIDATION TESTS
    # =====================================================

    def test_reference_format_has_colon(self):
        """Test that reference format always contains colon separator."""

        def test_func():
            pass

        ref = fn_ref_from_callable(test_func)

        # Must contain exactly one colon
        self.assertEqual(ref.count(":"), 1)

    def test_reference_format_module_before_colon(self):
        """Test that module path comes before colon."""

        def test_func():
            pass

        ref = fn_ref_from_callable(test_func)
        module_part, func_part = ref.split(":")

        # Module part should be a valid Python module path
        self.assertTrue(all(c.isalnum() or c in "._" for c in module_part))
        # Function part should be a valid identifier
        self.assertTrue(
            func_part.replace("<", "").replace(">", "").isidentifier()
            or func_part == "<lambda>"
        )

    def test_reference_is_string(self):
        """Test that reference is always returned as a string."""

        def test_func():
            pass

        ref = fn_ref_from_callable(test_func)

        self.assertIsInstance(ref, str)

    # =====================================================
    # EDGE CASES AND SPECIAL FUNCTIONS
    # =====================================================

    def test_nested_function_reference(self):
        """Test converting a nested (closure) function to reference."""

        def outer_function():
            def inner_function():
                pass

            return inner_function

        inner = outer_function()
        ref = fn_ref_from_callable(inner)

        # Should reference the inner function
        self.assertTrue(ref.endswith(":inner_function"))

    def test_partial_function_reference(self):
        """Test converting a functools.partial to reference."""
        from functools import partial

        def original_func(a, b):
            return a + b

        partial_func = partial(original_func, 1)

        # partial objects don't have __name__, so this will raise AttributeError
        # This documents current behavior - partials need special handling
        with self.assertRaises(AttributeError):
            ref = fn_ref_from_callable(partial_func)


class TestResolveCallable(unittest.TestCase):
    """
    Comprehensive tests for resolve_callable function.

    Tests resolution of string references back to callable functions,
    including both colon-separated and dot-separated formats.
    """

    # =====================================================
    # CALLABLE PASS-THROUGH TESTS
    # =====================================================

    def test_callable_returns_itself(self):
        """Test that passing a callable returns it unchanged."""

        def test_func():
            pass

        result = resolve_callable(test_func)

        self.assertIs(result, test_func)

    def test_lambda_returns_itself(self):
        """Test that passing a lambda returns it unchanged."""
        lambda_func = lambda x: x * 2

        result = resolve_callable(lambda_func)

        self.assertIs(result, lambda_func)

    def test_builtin_returns_itself(self):
        """Test that passing a built-in returns it unchanged."""
        result = resolve_callable(len)

        self.assertIs(result, len)

    # =====================================================
    # COLON-SEPARATED FORMAT TESTS
    # =====================================================

    def test_resolve_colon_format_builtin(self):
        """Test resolving 'module:function' format for built-in."""
        result = resolve_callable("builtins:len")

        self.assertIs(result, len)
        self.assertTrue(callable(result))

    def test_resolve_colon_format_math(self):
        """Test resolving 'module:function' format for math module."""
        result = resolve_callable("math:sqrt")

        import math

        self.assertIs(result, math.sqrt)
        self.assertTrue(callable(result))

    def test_resolve_colon_format_os_path(self):
        """Test resolving 'module.submodule:function' format."""
        result = resolve_callable("os.path:exists")

        import os.path

        self.assertIs(result, os.path.exists)
        self.assertTrue(callable(result))

    def test_resolve_colon_format_deep_module(self):
        """Test resolving deeply nested module with colon format."""
        # Test with a known deep module path
        result = resolve_callable("json.decoder:JSONDecoder")

        from json.decoder import JSONDecoder

        self.assertIs(result, JSONDecoder)

    # =====================================================
    # DOT-SEPARATED FORMAT TESTS (LEGACY)
    # =====================================================

    def test_resolve_dot_format_builtin(self):
        """Test resolving 'module.function' format (legacy)."""
        result = resolve_callable("builtins.len")

        self.assertIs(result, len)
        self.assertTrue(callable(result))

    def test_resolve_dot_format_math(self):
        """Test resolving 'module.function' format for math."""
        result = resolve_callable("math.sqrt")

        import math

        self.assertIs(result, math.sqrt)

    def test_resolve_dot_format_ambiguous_prefers_last(self):
        """Test that dot format splits on last dot (module.submodule.func)."""
        # os.path.exists could be interpreted as:
        # - 'os.path' module, 'exists' function  (correct)
        # - 'os' module, 'path.exists' attribute (wrong)
        result = resolve_callable("os.path.exists")

        import os.path

        self.assertIs(result, os.path.exists)

    # =====================================================
    # FORMAT PREFERENCE TESTS
    # =====================================================

    def test_colon_format_takes_precedence(self):
        """Test that colon format is preferred when present."""
        # If string contains ':', use colon splitting
        result = resolve_callable("math:sqrt")

        import math

        self.assertIs(result, math.sqrt)

    def test_colon_format_with_dots_in_module(self):
        """Test colon format with dots in module path."""
        result = resolve_callable("os.path:join")

        import os.path

        self.assertIs(result, os.path.join)

    # =====================================================
    # ROUND-TRIP TESTS
    # =====================================================

    def test_round_trip_builtin(self):
        """Test converting function to ref and back."""
        original = len
        ref = fn_ref_from_callable(original)
        resolved = resolve_callable(ref)

        self.assertIs(resolved, original)

    def test_round_trip_math_function(self):
        """Test round-trip with math function."""
        import math

        original = math.sqrt
        ref = fn_ref_from_callable(original)
        resolved = resolve_callable(ref)

        self.assertIs(resolved, original)

    def test_round_trip_os_path_function(self):
        """Test round-trip with nested module function."""
        import os.path

        original = os.path.exists
        ref = fn_ref_from_callable(original)
        resolved = resolve_callable(ref)

        self.assertIs(resolved, original)

    # =====================================================
    # ERROR HANDLING TESTS
    # =====================================================

    def test_invalid_module_raises_error(self):
        """Test that invalid module name raises ImportError."""
        with self.assertRaises(ModuleNotFoundError):
            resolve_callable("nonexistent_module:function")

    def test_invalid_function_raises_error(self):
        """Test that invalid function name raises AttributeError."""
        with self.assertRaises(AttributeError):
            resolve_callable("math:nonexistent_function")

    def test_empty_string_raises_error(self):
        """Test that empty string raises appropriate error."""
        with self.assertRaises(ValueError):
            resolve_callable("")

    def test_only_colon_raises_error(self):
        """Test that string with only colon raises error."""
        with self.assertRaises((ModuleNotFoundError, ValueError)):
            resolve_callable(":")

    def test_multiple_colons_uses_first(self):
        """Test behavior with multiple colons (splits on first)."""
        # This documents current behavior - splits on first colon
        # "a:b:c" -> module="a", func="b:c"
        with self.assertRaises((ModuleNotFoundError, AttributeError)):
            resolve_callable("math:sqrt:extra")

    # =====================================================
    # CLASS AND METHOD RESOLUTION TESTS
    # =====================================================

    def test_resolve_class_constructor(self):
        """Test resolving a class (callable constructor)."""
        result = resolve_callable("pathlib:Path")

        from pathlib import Path

        self.assertIs(result, Path)
        self.assertTrue(callable(result))

    def test_resolve_class_from_submodule(self):
        """Test resolving a class from a submodule."""
        result = resolve_callable("unittest.mock:MagicMock")

        self.assertIs(result, MagicMock)

    # =====================================================
    # INTEGRATION TESTS
    # =====================================================

    def test_resolve_and_call_function(self):
        """Test that resolved function is actually callable."""
        func = resolve_callable("math:sqrt")

        result = func(16)

        self.assertEqual(result, 4.0)

    def test_resolve_and_instantiate_class(self):
        """Test that resolved class can be instantiated."""
        PathClass = resolve_callable("pathlib:Path")

        instance = PathClass("/tmp")

        from pathlib import Path

        self.assertIsInstance(instance, Path)

    def test_resolve_callable_with_type_checking(self):
        """Test that resolved callable has correct attributes."""
        func = resolve_callable("math:sqrt")

        # Should have function attributes
        self.assertTrue(callable(func))
        self.assertTrue(hasattr(func, "__name__"))
        self.assertEqual(func.__name__, "sqrt")

    # =====================================================
    # SPECIAL CASES
    # =====================================================

    def test_resolve_dunder_method(self):
        """Test resolving special dunder methods."""
        # Note: This is a class method, not a function
        result = resolve_callable("builtins:dict")

        self.assertIs(result, dict)
        self.assertTrue(callable(result))

    def test_resolve_private_function(self):
        """Test resolving private (underscore-prefixed) functions."""
        # Many modules have private functions we can test with
        # For example, os has _exists on some platforms
        # This is more about format handling than actual resolution
        try:
            # Try to resolve a known private function if available
            result = resolve_callable("os:_exists")
            self.assertTrue(callable(result))
        except AttributeError:
            # If not available, test format handling
            with self.assertRaises(AttributeError):
                resolve_callable("os:_nonexistent_private_function")


class TestCallableRefIntegration(unittest.TestCase):
    """
    Integration tests for callable_ref module showing real-world usage patterns.
    """

    def test_workflow_step_registration_pattern(self):
        """Test typical workflow step registration pattern."""
        # Use a known module function instead of locally defined
        import math

        workflow_step = math.floor

        # Convert to reference for storage
        ref = fn_ref_from_callable(workflow_step)
        self.assertIsInstance(ref, str)
        self.assertEqual(ref, "math:floor")

        # Later, resolve and execute
        resolved = resolve_callable(ref)
        result = resolved(3.7)

        self.assertEqual(result, 3)

    def test_plugin_system_pattern(self):
        """Test plugin system pattern with string references."""
        # Simulate a plugin registry
        plugin_registry = {
            "math_plugin": "math:sqrt",
            "path_plugin": "pathlib:Path",
            "json_plugin": "json:dumps",
        }

        # Resolve and use plugins
        sqrt_func = resolve_callable(plugin_registry["math_plugin"])
        self.assertEqual(sqrt_func(4), 2.0)

        Path = resolve_callable(plugin_registry["path_plugin"])
        path_obj = Path("/tmp")
        from pathlib import Path as PathClass

        self.assertIsInstance(path_obj, PathClass)

        dumps_func = resolve_callable(plugin_registry["json_plugin"])
        result = dumps_func({"key": "value"})
        self.assertIn("key", result)

    def test_serialization_deserialization_pattern(self):
        """Test serialization pattern for workflow persistence."""
        import math

        # Original function
        original_function = math.sqrt

        # Serialize (convert to string reference)
        serialized = fn_ref_from_callable(original_function)
        self.assertEqual(serialized, "math:sqrt")

        # Simulate storage (e.g., JSON, database)
        stored_data = {"function_ref": serialized, "params": [16]}

        # Deserialize (resolve back to function)
        restored_function = resolve_callable(stored_data["function_ref"])

        # Execute with stored params (positional args)
        result = restored_function(*stored_data["params"])
        self.assertEqual(result, 4.0)

    def test_dynamic_function_loading_pattern(self):
        """Test dynamic function loading from configuration."""
        # Simulate configuration
        config = {
            "validators": [
                "builtins:bool",
                "builtins:int",
                "builtins:str",
            ]
        }

        # Load all validators
        validators = [resolve_callable(ref) for ref in config["validators"]]

        # Verify all are callable
        self.assertTrue(all(callable(v) for v in validators))
        self.assertEqual(validators, [bool, int, str])

    def test_decorator_unwrapping_in_workflow(self):
        """Test that decorated workflow steps are properly referenced."""
        import math
        from functools import wraps

        # Test with a known function that we can resolve
        original_func = math.ceil

        def workflow_decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            return wrapper

        decorated = workflow_decorator(original_func)

        # Should unwrap and get original function reference
        ref = fn_ref_from_callable(decorated)
        self.assertEqual(ref, "math:ceil")

        # Can resolve back to the original
        resolved = resolve_callable(ref)
        self.assertEqual(resolved.__name__, "ceil")
        # Verify it works
        self.assertEqual(resolved(3.2), 4)


if __name__ == "__main__":
    unittest.main()
