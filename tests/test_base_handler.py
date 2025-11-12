import asyncio
import unittest
from abc import ABC
from typing import Any, ClassVar
from unittest.mock import AsyncMock, Mock, patch

from lib.core_utils.event_types import EventType
from lib.handlers.base_handler import BaseHandler


class TestBaseHandler(unittest.TestCase):
    """
    Comprehensive tests for BaseHandler - the abstract base class for all event handlers.

    Tests the handler interface contract, abstract method enforcement, synchronous
    and asynchronous execution patterns, and integration with the event system.
    """

    def setUp(self):
        """Set up test fixtures for each test."""

        # Create concrete test implementations for testing
        class ConcreteHandler(BaseHandler):
            event_type: ClassVar[EventType] = EventType.PROJECT_CHANGE

            def __init__(self):
                self.handle_task_called = False
                self.call_called = False
                self.last_payload = None
                self.handle_task_mock = AsyncMock()

            async def handle_task(self, payload: dict[str, Any]) -> None:
                self.handle_task_called = True
                self.last_payload = payload
                await self.handle_task_mock(payload)

            def __call__(self, payload: dict[str, Any]) -> None:
                self.call_called = True
                self.last_payload = payload
                # Schedule the async task
                asyncio.create_task(self.handle_task(payload))

        # Create incomplete handler for testing abstract enforcement
        class IncompleteHandler(BaseHandler):
            event_type: ClassVar[EventType] = EventType.FLOWCELL_READY

            # Missing handle_task implementation
            def __call__(self, payload: dict[str, Any]) -> None:
                pass

        class MissingCallHandler(BaseHandler):
            event_type: ClassVar[EventType] = EventType.PROJECT_CHANGE

            async def handle_task(self, payload: dict[str, Any]) -> None:
                pass

            # Missing __call__ implementation

        self.ConcreteHandler = ConcreteHandler
        self.IncompleteHandler = IncompleteHandler
        self.MissingCallHandler = MissingCallHandler

    # =====================================================
    # ABSTRACT BASE CLASS TESTS
    # =====================================================

    def test_is_abstract_base_class(self):
        """Test that BaseHandler is properly defined as an abstract base class."""
        # BaseHandler should be abstract
        self.assertTrue(issubclass(BaseHandler, ABC))

        # Should not be instantiable directly
        with self.assertRaises(TypeError):
            BaseHandler()  # type: ignore

    def test_abstract_method_enforcement(self):
        """Test that abstract methods are properly enforced."""
        # Incomplete handler missing handle_task should not be instantiable
        with self.assertRaises(TypeError) as context:
            self.IncompleteHandler()  # type: ignore

        error_message = str(context.exception)
        self.assertIn("handle_task", error_message)

        # Handler missing __call__ should not be instantiable
        with self.assertRaises(TypeError) as context:
            self.MissingCallHandler()  # type: ignore

        error_message = str(context.exception)
        self.assertIn("__call__", error_message)

    def test_concrete_implementation_instantiation(self):
        """Test that concrete implementations can be instantiated."""
        # Complete implementation should be instantiable
        handler = self.ConcreteHandler()

        # Should be instance of BaseHandler
        self.assertIsInstance(handler, BaseHandler)
        self.assertIsInstance(handler, self.ConcreteHandler)

    # =====================================================
    # IDENTITY HELPER METHOD TESTS
    # =====================================================

    def test_class_qualified_name(self):
        """Test class_qualified_name returns correct module.qualname format."""
        handler = self.ConcreteHandler()

        # Should return module.qualname format
        qualified_name = handler.class_qualified_name()

        # Should contain module name
        self.assertIn("test_base_handler", qualified_name)
        # Should contain class name
        self.assertIn("ConcreteHandler", qualified_name)
        # Should be in correct format
        self.assertTrue(qualified_name.endswith(".ConcreteHandler"))

    def test_class_qualified_name_with_nested_classes(self):
        """Test class_qualified_name handles nested classes correctly."""

        class OuterHandler(BaseHandler):
            event_type: ClassVar[EventType] = EventType.PROJECT_CHANGE

            class InnerHandler(BaseHandler):
                event_type: ClassVar[EventType] = EventType.FLOWCELL_READY

                async def handle_task(self, payload: dict[str, Any]) -> None:
                    pass

                def __call__(self, payload: dict[str, Any]) -> None:
                    pass

            async def handle_task(self, payload: dict[str, Any]) -> None:
                pass

            def __call__(self, payload: dict[str, Any]) -> None:
                pass

        outer = OuterHandler()
        inner = OuterHandler.InnerHandler()

        # Outer should have simple qualname
        outer_qname = outer.class_qualified_name()
        self.assertIn("OuterHandler", outer_qname)
        self.assertNotIn("InnerHandler", outer_qname)

        # Inner should have nested qualname
        inner_qname = inner.class_qualified_name()
        self.assertIn("OuterHandler.InnerHandler", inner_qname)

    def test_class_key_returns_tuple(self):
        """Test class_key returns (module, qualname) tuple."""
        handler = self.ConcreteHandler()

        key = handler.class_key()

        # Should be a tuple
        self.assertIsInstance(key, tuple)
        # Should have exactly 2 elements
        self.assertEqual(len(key), 2)
        # Both should be strings
        self.assertIsInstance(key[0], str)
        self.assertIsInstance(key[1], str)

    def test_class_key_stability(self):
        """Test class_key returns stable identity across instances."""
        handler1 = self.ConcreteHandler()
        handler2 = self.ConcreteHandler()

        key1 = handler1.class_key()
        key2 = handler2.class_key()

        # Keys should be identical for same class
        self.assertEqual(key1, key2)

    def test_class_key_uniqueness(self):
        """Test class_key distinguishes different handler classes."""
        concrete_handler = self.ConcreteHandler()
        incomplete_handler_cls = self.IncompleteHandler

        # Get keys
        concrete_key = concrete_handler.class_key()

        # Keys should be different for different classes
        # (IncompleteHandler can't be instantiated, but we can call class method)
        incomplete_key = incomplete_handler_cls.class_key()

        self.assertNotEqual(concrete_key, incomplete_key)

    def test_class_key_format(self):
        """Test class_key format matches (module, qualname)."""
        handler = self.ConcreteHandler()

        key = handler.class_key()
        module_name, qualname = key

        # Module should match
        self.assertEqual(module_name, handler.__module__)
        # Qualname should match
        self.assertEqual(qualname, handler.__class__.__qualname__)

    def test_class_key_used_for_deduplication(self):
        """Test class_key can be used for handler deduplication."""
        handler1 = self.ConcreteHandler()
        handler2 = self.ConcreteHandler()

        class AnotherHandler(BaseHandler):
            event_type: ClassVar[EventType] = EventType.PROJECT_CHANGE

            async def handle_task(self, payload: dict[str, Any]) -> None:
                pass

            def __call__(self, payload: dict[str, Any]) -> None:
                pass

        another_handler = AnotherHandler()

        # Simulate deduplication using class_key
        seen_keys = set()
        unique_handlers = []

        for handler in [handler1, handler2, another_handler]:
            key = handler.class_key()
            if key not in seen_keys:
                seen_keys.add(key)
                unique_handlers.append(handler)

        # Should have deduplicated ConcreteHandler instances
        self.assertEqual(len(unique_handlers), 2)
        # Should have kept one ConcreteHandler and one AnotherHandler
        self.assertEqual(len(seen_keys), 2)

    def test_identity_methods_are_class_methods(self):
        """Test that identity helper methods are proper class methods."""
        # Should be callable on class without instantiation
        qualified_name = self.ConcreteHandler.class_qualified_name()
        key = self.ConcreteHandler.class_key()

        self.assertIsInstance(qualified_name, str)
        self.assertIsInstance(key, tuple)

        # Should return same results when called on instance
        handler = self.ConcreteHandler()
        self.assertEqual(qualified_name, handler.class_qualified_name())
        self.assertEqual(key, handler.class_key())

    # =====================================================
    # EVENT TYPE CLASS VARIABLE TESTS
    # =====================================================

    def test_event_type_class_variable(self):
        """Test that event_type class variable is properly defined."""
        handler = self.ConcreteHandler()

        # Should have event_type as class variable
        self.assertTrue(hasattr(handler.__class__, "event_type"))
        self.assertIsInstance(handler.__class__.event_type, EventType)
        self.assertEqual(handler.__class__.event_type, EventType.PROJECT_CHANGE)

    def test_event_type_inheritance(self):
        """Test that event_type is properly inherited in subclasses."""

        class SubHandler(self.ConcreteHandler):
            event_type: ClassVar[EventType] = EventType.FLOWCELL_READY

        handler = SubHandler()
        self.assertEqual(handler.__class__.event_type, EventType.FLOWCELL_READY)

        # Parent class should still have its own event_type
        parent_handler = self.ConcreteHandler()
        self.assertEqual(parent_handler.__class__.event_type, EventType.PROJECT_CHANGE)

    # =====================================================
    # ABSTRACT METHOD SIGNATURE TESTS
    # =====================================================

    def test_handle_task_method_signature(self):
        """Test handle_task method signature and behavior."""
        handler = self.ConcreteHandler()

        # Should be a coroutine function
        self.assertTrue(asyncio.iscoroutinefunction(handler.handle_task))

        # Should accept payload parameter
        import inspect

        sig = inspect.signature(handler.handle_task)
        self.assertIn("payload", sig.parameters)

        # Parameter should be typed as dict[str, Any]
        payload_param = sig.parameters["payload"]
        self.assertEqual(str(payload_param.annotation), "dict[str, typing.Any]")

    def test_call_method_signature(self):
        """Test __call__ method signature and behavior."""
        handler = self.ConcreteHandler()

        # Should be callable
        self.assertTrue(callable(handler))

        # Should accept payload parameter
        import inspect

        sig = inspect.signature(handler.__call__)
        self.assertIn("payload", sig.parameters)

        # Should not be a coroutine function (sync interface)
        self.assertFalse(asyncio.iscoroutinefunction(handler.__call__))

    # =====================================================
    # RUN_NOW METHOD TESTS
    # =====================================================

    def test_run_now_method_exists(self):
        """Test that run_now method is provided by base class."""
        handler = self.ConcreteHandler()

        # Should have run_now method
        self.assertTrue(hasattr(handler, "run_now"))
        self.assertTrue(callable(handler.run_now))

    def test_run_now_calls_handle_task(self):
        """Test that run_now properly calls handle_task."""
        handler = self.ConcreteHandler()
        test_payload = {"test": "data", "id": "12345"}

        # Call run_now
        handler.run_now(test_payload)

        # Should have called handle_task with the payload
        self.assertTrue(handler.handle_task_called)
        self.assertEqual(handler.last_payload, test_payload)
        handler.handle_task_mock.assert_called_once_with(test_payload)

    def test_run_now_blocks_until_completion(self):
        """Test that run_now blocks until async operation completes."""

        class TimedHandler(BaseHandler):
            event_type: ClassVar[EventType] = EventType.PROJECT_CHANGE

            def __init__(self):
                self.start_time = None
                self.end_time = None

            async def handle_task(self, payload: dict[str, Any]) -> None:
                import time

                self.start_time = time.time()
                await asyncio.sleep(0.1)  # Simulate async work
                self.end_time = time.time()

            def __call__(self, payload: dict[str, Any]) -> None:
                asyncio.create_task(self.handle_task(payload))

        handler = TimedHandler()
        test_payload = {"test": "blocking"}

        import time

        before_call = time.time()
        handler.run_now(test_payload)
        after_call = time.time()

        # Should have completed the async work
        self.assertIsNotNone(handler.start_time)
        self.assertIsNotNone(handler.end_time)

        # run_now should have blocked until completion
        elapsed = after_call - before_call
        self.assertGreaterEqual(elapsed, 0.1)  # At least the sleep duration

    def test_run_now_with_exception_handling(self):
        """Test run_now behavior when handle_task raises an exception."""

        class ExceptionHandler(BaseHandler):
            event_type: ClassVar[EventType] = EventType.PROJECT_CHANGE

            async def handle_task(self, payload: dict[str, Any]) -> None:
                raise ValueError("Test exception")

            def __call__(self, payload: dict[str, Any]) -> None:
                asyncio.create_task(self.handle_task(payload))

        handler = ExceptionHandler()
        test_payload = {"test": "exception"}

        # run_now should propagate the exception
        with self.assertRaises(ValueError) as context:
            handler.run_now(test_payload)

        self.assertEqual(str(context.exception), "Test exception")

    # =====================================================
    # ASYNC EXECUTION PATTERN TESTS
    # =====================================================

    def test_call_creates_async_task(self):
        """Test that __call__ properly creates async tasks."""
        handler = self.ConcreteHandler()
        test_payload = {"test": "async", "value": 42}

        # Mock asyncio.create_task to verify it's called
        with patch("asyncio.create_task") as mock_create_task:
            handler(test_payload)

            # Should have called create_task
            mock_create_task.assert_called_once()

            # Verify the call was made with a coroutine
            call_args = mock_create_task.call_args[0]
            self.assertEqual(len(call_args), 1)  # Should have one argument
            # We can't easily test the coroutine itself without running it

    def test_async_execution_in_event_loop(self):
        """Test proper async execution within an event loop."""

        async def test_async_execution():
            handler = self.ConcreteHandler()
            test_payload = {"async_test": True, "data": [1, 2, 3]}

            # Create and await the task manually
            task = asyncio.create_task(handler.handle_task(test_payload))
            await task

            # Should have processed the payload
            self.assertTrue(handler.handle_task_called)
            self.assertEqual(handler.last_payload, test_payload)

        # Run the async test
        asyncio.run(test_async_execution())

    def test_multiple_async_tasks_concurrency(self):
        """Test that multiple async tasks can run concurrently."""

        async def test_concurrent_execution():
            handler = self.ConcreteHandler()
            payloads = [
                {"task": 1, "data": "first"},
                {"task": 2, "data": "second"},
                {"task": 3, "data": "third"},
            ]

            # Create multiple tasks
            tasks = [
                asyncio.create_task(handler.handle_task(payload))
                for payload in payloads
            ]

            # Wait for all to complete
            await asyncio.gather(*tasks)

            # All should have been called
            self.assertEqual(handler.handle_task_mock.call_count, 3)

            # Check that all payloads were processed
            call_args = [call[0][0] for call in handler.handle_task_mock.call_args_list]
            self.assertEqual(len(call_args), 3)
            for payload in payloads:
                self.assertIn(payload, call_args)

        asyncio.run(test_concurrent_execution())

    # =====================================================
    # PAYLOAD HANDLING TESTS
    # =====================================================

    def test_payload_parameter_handling(self):
        """Test that payload parameters are properly handled."""
        handler = self.ConcreteHandler()

        # Test with complex payload
        complex_payload = {
            "document": {"id": "test_doc", "type": "project"},
            "module_location": "/path/to/module",
            "metadata": {
                "timestamp": "2025-07-24T15:30:00Z",
                "source": "test",
                "nested": {"deep": {"value": 123}},
            },
            "list_data": [1, 2, 3, "string", {"nested": True}],
        }

        handler.run_now(complex_payload)

        # Should receive exact payload
        self.assertEqual(handler.last_payload, complex_payload)
        handler.handle_task_mock.assert_called_once_with(complex_payload)

    def test_empty_payload_handling(self):
        """Test handling of empty payloads."""
        handler = self.ConcreteHandler()
        empty_payload = {}

        handler.run_now(empty_payload)

        self.assertEqual(handler.last_payload, empty_payload)
        handler.handle_task_mock.assert_called_once_with(empty_payload)

    def test_payload_immutability_concern(self):
        """Test that handlers should not modify the original payload."""

        class PayloadModifyingHandler(BaseHandler):
            event_type: ClassVar[EventType] = EventType.PROJECT_CHANGE

            async def handle_task(self, payload: dict[str, Any]) -> None:
                # Simulate handler modifying payload (bad practice)
                payload["modified"] = True
                payload["original_keys"] = list(payload.keys())

            def __call__(self, payload: dict[str, Any]) -> None:
                asyncio.create_task(self.handle_task(payload))

        handler = PayloadModifyingHandler()
        original_payload = {"test": "data", "immutable": True}
        payload_copy = original_payload.copy()

        handler.run_now(original_payload)

        # Original payload should be modified (this test documents current behavior)
        # In a real implementation, handlers should work with copies
        self.assertNotEqual(original_payload, payload_copy)
        self.assertTrue(original_payload.get("modified", False))

    # =====================================================
    # INTEGRATION AND REAL-WORLD SCENARIO TESTS
    # =====================================================

    def test_handler_registration_pattern(self):
        """Test typical handler registration pattern."""
        handler = self.ConcreteHandler()

        # Simulate registration in YggdrasilCore
        mock_core = Mock()
        mock_core.handlers = {}

        # Register handler
        mock_core.handlers[handler.event_type] = handler

        # Verify registration
        self.assertIn(handler.event_type, mock_core.handlers)
        self.assertIs(mock_core.handlers[handler.event_type], handler)

    def test_event_dispatch_simulation(self):
        """Test simulated event dispatch from YggdrasilCore."""
        handler = self.ConcreteHandler()

        # Simulate YggdrasilCore.handle_event calling the handler
        mock_event = Mock()
        mock_event.event_type = EventType.PROJECT_CHANGE
        mock_event.payload = {"document": {"id": "test_doc"}}

        # Mock asyncio.create_task since we're not in an event loop
        with patch("asyncio.create_task") as mock_create_task:
            # Simulate core finding and calling handler
            if mock_event.event_type == handler.event_type:
                handler(mock_event.payload)

            # Should have called create_task
            mock_create_task.assert_called_once()

            # Should have been called with the payload
            self.assertTrue(handler.call_called)
            self.assertEqual(handler.last_payload, mock_event.payload)

    def test_cli_one_off_execution_pattern(self):
        """Test one-off CLI execution pattern."""
        handler = self.ConcreteHandler()

        # Simulate CLI calling run_once -> handler.run_now
        cli_payload = {
            "document": {"id": "cli_doc_123"},
            "module_location": "/cli/module/path",
            "source": "CLI",
        }

        # Simulate YggdrasilCore.run_once behavior
        handler.run_now(cli_payload)

        # Should complete synchronously
        self.assertTrue(handler.handle_task_called)
        self.assertEqual(handler.last_payload, cli_payload)

    def test_error_propagation_patterns(self):
        """Test error propagation in different execution patterns."""

        class ErrorHandler(BaseHandler):
            event_type: ClassVar[EventType] = EventType.PROJECT_CHANGE

            def __init__(self, error_type=None):
                self.error_type = error_type

            async def handle_task(self, payload: dict[str, Any]) -> None:
                if self.error_type:
                    raise self.error_type("Handler error")

            def __call__(self, payload: dict[str, Any]) -> None:
                # In real implementation, this would create_task
                # but for testing, we'll just call directly
                asyncio.create_task(self.handle_task(payload))

        # Test synchronous error propagation
        sync_handler = ErrorHandler(ValueError)
        with self.assertRaises(ValueError):
            sync_handler.run_now({"test": "sync_error"})

        # Test async error handling (would be caught by asyncio)
        async def test_async_error():
            async_handler = ErrorHandler(RuntimeError)
            with self.assertRaises(RuntimeError):
                await async_handler.handle_task({"test": "async_error"})

        asyncio.run(test_async_error())

        # Test async call error handling with mocked create_task
        task_handler = ErrorHandler(RuntimeError)
        with patch("asyncio.create_task") as mock_create_task:
            task_handler({"test": "task_error"})
            mock_create_task.assert_called_once()

    # =====================================================
    # TYPE ANNOTATION AND INTERFACE TESTS
    # =====================================================

    def test_type_annotations_compliance(self):
        """Test that implementations comply with type annotations."""
        handler = self.ConcreteHandler()

        # Test payload type compliance
        import inspect

        handle_task_sig = inspect.signature(handler.handle_task)
        call_sig = inspect.signature(handler.__call__)
        run_now_sig = inspect.signature(handler.run_now)

        # All should have payload parameter
        self.assertIn("payload", handle_task_sig.parameters)
        self.assertIn("payload", call_sig.parameters)
        self.assertIn("payload", run_now_sig.parameters)

        # Return types should be correct
        self.assertEqual(handle_task_sig.return_annotation, None)
        self.assertEqual(call_sig.return_annotation, None)
        self.assertEqual(run_now_sig.return_annotation, None)

    def test_interface_contract_compliance(self):
        """Test that concrete implementations satisfy the interface contract."""
        handler = self.ConcreteHandler()

        # Must have event_type class variable
        self.assertTrue(hasattr(handler.__class__, "event_type"))

        # Must implement required methods
        self.assertTrue(hasattr(handler, "handle_task"))
        self.assertTrue(hasattr(handler, "__call__"))
        self.assertTrue(hasattr(handler, "run_now"))

        # Methods must be callable
        self.assertTrue(callable(handler.handle_task))
        self.assertTrue(callable(handler.__call__))
        self.assertTrue(callable(handler.run_now))

        # handle_task must be async
        self.assertTrue(asyncio.iscoroutinefunction(handler.handle_task))

        # __call__ and run_now must be sync
        self.assertFalse(asyncio.iscoroutinefunction(handler.__call__))
        self.assertFalse(asyncio.iscoroutinefunction(handler.run_now))


if __name__ == "__main__":
    unittest.main()
