import asyncio
import unittest
from abc import ABC
from typing import Any, ClassVar
from unittest.mock import AsyncMock, Mock, patch

from lib.core_utils.event_types import EventType
from lib.handlers.base_handler import BaseHandler
from yggdrasil.flow.model import Plan
from yggdrasil.flow.planner.api import PlanDraft


def make_dummy_plan_draft(plan_id: str = "test_plan") -> PlanDraft:
    """Helper to create a minimal PlanDraft for tests."""
    plan = Plan(
        plan_id=plan_id,
        realm="test",
        scope={"kind": "project", "id": "P12345"},
        steps=[],
    )
    return PlanDraft(plan=plan, auto_run=True)


class TestBaseHandler(unittest.TestCase):
    """
    Comprehensive tests for BaseHandler - the abstract base class for all event handlers.

    Tests the handler interface contract, abstract method enforcement, synchronous
    and asynchronous execution patterns, and integration with the event system.

    NOTE: BaseHandler uses `generate_plan_drafts` (returns list[PlanDraft]).
    """

    @classmethod
    def setUpClass(cls):
        """Set up class-level resources."""
        # Store original event loop policy
        cls.original_event_loop_policy = asyncio.get_event_loop_policy()

    @classmethod
    def tearDownClass(cls):
        """Clean up class-level resources and reset event loop state."""
        # Reset to default event loop policy for subsequent tests
        asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())

    def setUp(self):
        """Set up test fixtures for each test."""

        # Create concrete test implementations for testing
        class ConcreteHandler(BaseHandler):
            event_type: ClassVar[EventType] = EventType.PROJECT_CHANGE

            def __init__(self):
                self.generate_plan_drafts_called = False
                self.call_called = False
                self.last_payload = None
                self.generate_plan_drafts_mock = AsyncMock(
                    return_value=[make_dummy_plan_draft()]
                )

            def derive_scope(self, doc: dict[str, Any]) -> dict[str, Any]:
                # Simple test implementation
                return {"kind": "project", "id": doc.get("project_id", "test_project")}

            async def generate_plan_drafts(
                self, payload: dict[str, Any]
            ) -> list[PlanDraft]:
                self.generate_plan_drafts_called = True
                self.last_payload = payload
                return await self.generate_plan_drafts_mock(payload)

        # Create incomplete handler for testing abstract enforcement
        class IncompleteHandler(BaseHandler):
            event_type: ClassVar[EventType] = EventType.FLOWCELL_READY

            def derive_scope(self, doc: dict[str, Any]) -> dict[str, Any]:
                return {"kind": "flowcell", "id": doc.get("flowcell_id", "test_fc")}

            # Missing generate_plan_drafts implementation

        self.ConcreteHandler = ConcreteHandler
        self.IncompleteHandler = IncompleteHandler

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
        # Incomplete handler missing generate_plan_drafts should not be instantiable
        with self.assertRaises(TypeError) as context:
            self.IncompleteHandler()  # type: ignore

        error_message = str(context.exception)
        self.assertIn("generate_plan_drafts", error_message)

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

                def derive_scope(self, doc: dict[str, Any]) -> dict[str, Any]:
                    return {
                        "kind": "flowcell",
                        "id": doc.get("flowcell_id", "inner_fc"),
                    }

                async def generate_plan_drafts(
                    self, payload: dict[str, Any]
                ) -> list[PlanDraft]:
                    return [make_dummy_plan_draft()]

                def __call__(self, payload: dict[str, Any]) -> None:
                    pass

            def derive_scope(self, doc: dict[str, Any]) -> dict[str, Any]:
                return {"kind": "project", "id": doc.get("project_id", "outer_project")}

            async def generate_plan_drafts(
                self, payload: dict[str, Any]
            ) -> list[PlanDraft]:
                return [make_dummy_plan_draft()]

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

            def derive_scope(self, doc: dict[str, Any]) -> dict[str, Any]:
                return {"kind": "project", "id": doc.get("project_id", "dedup_project")}

            async def generate_plan_drafts(
                self, payload: dict[str, Any]
            ) -> list[PlanDraft]:
                return [make_dummy_plan_draft()]

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

    def test_generate_plan_drafts_method_signature(self):
        """Test generate_plan_drafts method signature and behavior."""
        handler = self.ConcreteHandler()

        # Should be a coroutine function
        self.assertTrue(asyncio.iscoroutinefunction(handler.generate_plan_drafts))

        # Should accept payload parameter
        import inspect

        sig = inspect.signature(handler.generate_plan_drafts)
        self.assertIn("payload", sig.parameters)

        # Parameter should be typed as dict[str, Any]
        payload_param = sig.parameters["payload"]
        self.assertEqual(str(payload_param.annotation), "dict[str, typing.Any]")

    # =====================================================
    # RUN_NOW METHOD TESTS
    # =====================================================

    def test_run_now_method_exists(self):
        """Test that run_now method is provided by base class."""
        handler = self.ConcreteHandler()

        # Should have run_now method
        self.assertTrue(hasattr(handler, "run_now"))
        self.assertTrue(callable(handler.run_now))

    def test_run_now_calls_generate_plan_drafts(self):
        """Test that run_now properly calls generate_plan_drafts."""
        handler = self.ConcreteHandler()
        test_payload = {"test": "data", "id": "12345"}

        # Call run_now
        result = handler.run_now(test_payload)

        # Should have called generate_plan_drafts with the payload
        self.assertTrue(handler.generate_plan_drafts_called)
        self.assertEqual(handler.last_payload, test_payload)
        handler.generate_plan_drafts_mock.assert_called_once_with(test_payload)
        # Should return a list
        self.assertIsInstance(result, list)

    def test_run_now_blocks_until_completion(self):
        """Test that run_now blocks until async operation completes."""

        class TimedHandler(BaseHandler):
            event_type: ClassVar[EventType] = EventType.PROJECT_CHANGE

            def __init__(self):
                self.start_time = None
                self.end_time = None

            def derive_scope(self, doc: dict[str, Any]) -> dict[str, Any]:
                return {"kind": "project", "id": doc.get("project_id", "timed_project")}

            async def generate_plan_drafts(
                self, payload: dict[str, Any]
            ) -> list[PlanDraft]:
                import time

                self.start_time = time.time()
                await asyncio.sleep(0.1)  # Simulate async work
                self.end_time = time.time()
                return [make_dummy_plan_draft()]

            def __call__(self, payload: dict[str, Any]) -> None:
                asyncio.create_task(self.generate_plan_drafts(payload))

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
        """Test run_now behavior when generate_plan_drafts raises an exception."""

        class ExceptionHandler(BaseHandler):
            event_type: ClassVar[EventType] = EventType.PROJECT_CHANGE

            def derive_scope(self, doc: dict[str, Any]) -> dict[str, Any]:
                return {
                    "kind": "project",
                    "id": doc.get("project_id", "exception_project"),
                }

            async def generate_plan_drafts(
                self, payload: dict[str, Any]
            ) -> list[PlanDraft]:
                raise ValueError("Test exception")

            def __call__(self, payload: dict[str, Any]) -> None:
                asyncio.create_task(self.generate_plan_drafts(payload))

        handler = ExceptionHandler()
        test_payload = {"test": "exception"}

        # run_now should propagate the exception
        with self.assertRaises(ValueError) as context:
            handler.run_now(test_payload)

        self.assertEqual(str(context.exception), "Test exception")

    def test_run_now_raises_error_in_async_context(self):
        """Test that run_now raises RuntimeError when called from async context."""

        async def test_async_context():
            handler = self.ConcreteHandler()
            test_payload = {"test": "async_context"}

            # Calling run_now from within an async context should raise
            with self.assertRaises(RuntimeError) as context:
                handler.run_now(test_payload)

            error_message = str(context.exception)
            self.assertIn(
                "cannot be called from within an async context", error_message
            )
            self.assertIn("generate_plan_drafts", error_message)

        # Run the async test
        asyncio.run(test_async_context())

    # =====================================================
    # ASYNC EXECUTION PATTERN TESTS
    # =====================================================

    def test_async_execution_in_event_loop(self):
        """Test proper async execution within an event loop."""

        async def test_async_execution():
            handler = self.ConcreteHandler()
            test_payload = {"async_test": True, "data": [1, 2, 3]}

            # Create and await the task manually
            task = asyncio.create_task(handler.generate_plan_drafts(test_payload))
            await task

            # Should have processed the payload
            self.assertTrue(handler.generate_plan_drafts_called)
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
                asyncio.create_task(handler.generate_plan_drafts(payload))
                for payload in payloads
            ]

            # Wait for all to complete
            await asyncio.gather(*tasks)

            # All should have been called
            self.assertEqual(handler.generate_plan_drafts_mock.call_count, 3)

            # Check that all payloads were processed
            call_args = [
                call[0][0] for call in handler.generate_plan_drafts_mock.call_args_list
            ]
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
        handler.generate_plan_drafts_mock.assert_called_once_with(complex_payload)

    def test_empty_payload_handling(self):
        """Test handling of empty payloads."""
        handler = self.ConcreteHandler()
        empty_payload = {}

        handler.run_now(empty_payload)

        self.assertEqual(handler.last_payload, empty_payload)
        handler.generate_plan_drafts_mock.assert_called_once_with(empty_payload)

    def test_payload_immutability_concern(self):
        """Test that handlers should not modify the original payload."""

        class PayloadModifyingHandler(BaseHandler):
            event_type: ClassVar[EventType] = EventType.PROJECT_CHANGE

            def derive_scope(self, doc: dict[str, Any]) -> dict[str, Any]:
                return {
                    "kind": "project",
                    "id": doc.get("project_id", "modify_project"),
                }

            async def generate_plan_drafts(
                self, payload: dict[str, Any]
            ) -> list[PlanDraft]:
                # Simulate handler modifying payload (bad practice)
                payload["modified"] = True
                payload["original_keys"] = list(payload.keys())
                return [make_dummy_plan_draft()]

            def __call__(self, payload: dict[str, Any]) -> None:
                asyncio.create_task(self.generate_plan_drafts(payload))

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
        self.assertTrue(handler.generate_plan_drafts_called)
        self.assertEqual(handler.last_payload, cli_payload)

    def test_error_propagation_patterns(self):
        """Test error propagation in different execution patterns."""

        class ErrorHandler(BaseHandler):
            event_type: ClassVar[EventType] = EventType.PROJECT_CHANGE

            def __init__(self, error_type=None):
                self.error_type = error_type

            def derive_scope(self, doc: dict[str, Any]) -> dict[str, Any]:
                return {"kind": "project", "id": doc.get("project_id", "error_project")}

            async def generate_plan_drafts(
                self, payload: dict[str, Any]
            ) -> list[PlanDraft]:
                if self.error_type:
                    raise self.error_type("Handler error")
                return [make_dummy_plan_draft()]

            def __call__(self, payload: dict[str, Any]) -> None:
                # In real implementation, this would create_task
                # but for testing, we'll just call directly
                asyncio.create_task(self.generate_plan_drafts(payload))

        # Test synchronous error propagation
        sync_handler = ErrorHandler(ValueError)
        with self.assertRaises(ValueError):
            sync_handler.run_now({"test": "sync_error"})

        # Test async error handling (would be caught by asyncio)
        async def test_async_error():
            async_handler = ErrorHandler(RuntimeError)
            with self.assertRaises(RuntimeError):
                await async_handler.generate_plan_drafts({"test": "async_error"})

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

        generate_plan_drafts_sig = inspect.signature(handler.generate_plan_drafts)
        run_now_sig = inspect.signature(handler.run_now)

        # Required methods should have payload parameter
        self.assertIn("payload", generate_plan_drafts_sig.parameters)
        self.assertIn("payload", run_now_sig.parameters)

        # Return types should be correct
        # generate_plan_drafts returns list[PlanDraft]
        self.assertEqual(generate_plan_drafts_sig.return_annotation, list[PlanDraft])
        # run_now also returns list[PlanDraft] (blocking version of generate_plan_drafts)
        # Use get_type_hints() to resolve string annotations (PEP 563 / from __future__ import annotations)
        import typing

        run_now_hints = typing.get_type_hints(handler.__class__.run_now)
        self.assertEqual(run_now_hints.get("return"), list[PlanDraft])

    def test_interface_contract_compliance(self):
        """Test that concrete implementations satisfy the interface contract."""
        handler = self.ConcreteHandler()

        # Must have event_type class variable
        self.assertTrue(hasattr(handler.__class__, "event_type"))

        # Must implement required methods
        self.assertTrue(hasattr(handler, "generate_plan_drafts"))
        self.assertTrue(hasattr(handler, "run_now"))
        self.assertTrue(hasattr(handler, "derive_scope"))

        # Methods must be callable
        self.assertTrue(callable(handler.generate_plan_drafts))
        self.assertTrue(callable(handler.run_now))
        self.assertTrue(callable(handler.derive_scope))

        # generate_plan_drafts must be async
        self.assertTrue(asyncio.iscoroutinefunction(handler.generate_plan_drafts))

        # run_now and derive_scope must be sync
        self.assertFalse(asyncio.iscoroutinefunction(handler.run_now))
        self.assertFalse(asyncio.iscoroutinefunction(handler.derive_scope))


if __name__ == "__main__":
    unittest.main()
