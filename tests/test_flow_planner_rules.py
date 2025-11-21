import unittest
from pathlib import Path
from unittest.mock import Mock

from yggdrasil.flow.planner import rules
from yggdrasil.flow.planner.builder import PlanBuilder


class TestStepRule(unittest.TestCase):
    """Comprehensive tests for StepRule dataclass."""

    def test_fields(self):
        """Test StepRule with all fields set."""

        def dummy_when(facts):
            return True

        def dummy_build(builder, facts):
            pass

        rule = rules.StepRule(name="foo", when=dummy_when, build=dummy_build, order=42)
        self.assertEqual(rule.name, "foo")
        self.assertEqual(rule.order, 42)
        self.assertIs(rule.when, dummy_when)
        self.assertIs(rule.build, dummy_build)

    def test_default_order(self):
        """Test StepRule with default order value."""

        def dummy_when(facts):
            return True

        def dummy_build(builder, facts):
            pass

        rule = rules.StepRule(name="test", when=dummy_when, build=dummy_build)
        self.assertEqual(rule.order, 100)

    def test_name_required(self):
        """Test that name is required."""

        def dummy_when(facts):
            return True

        def dummy_build(builder, facts):
            pass

        with self.assertRaises(TypeError):
            rules.StepRule(when=dummy_when, build=dummy_build)  # type: ignore

    def test_when_required(self):
        """Test that when guard is required."""

        def dummy_build(builder, facts):
            pass

        with self.assertRaises(TypeError):
            rules.StepRule(name="test", build=dummy_build)  # type: ignore

    def test_build_required(self):
        """Test that build function is required."""

        def dummy_when(facts):
            return True

        with self.assertRaises(TypeError):
            rules.StepRule(name="test", when=dummy_when)  # type: ignore

    def test_when_callable_true(self):
        """Test when guard returns True."""

        def always_true(facts):
            return True

        def dummy_build(builder, facts):
            pass

        rule = rules.StepRule(name="test", when=always_true, build=dummy_build)
        self.assertTrue(rule.when({"any": "facts"}))

    def test_when_callable_false(self):
        """Test when guard returns False."""

        def always_false(facts):
            return False

        def dummy_build(builder, facts):
            pass

        rule = rules.StepRule(name="test", when=always_false, build=dummy_build)
        self.assertFalse(rule.when({"any": "facts"}))

    def test_when_conditional(self):
        """Test when guard with conditional logic."""

        def check_status(facts):
            return facts.get("status") == "active"

        def dummy_build(builder, facts):
            pass

        rule = rules.StepRule(name="test", when=check_status, build=dummy_build)
        self.assertTrue(rule.when({"status": "active"}))
        self.assertFalse(rule.when({"status": "inactive"}))

    def test_build_invocation(self):
        """Test that build function can be invoked."""
        called = {}

        def dummy_when(facts):
            return True

        def track_build(builder, facts):
            called["builder"] = builder
            called["facts"] = facts

        rule = rules.StepRule(name="test", when=dummy_when, build=track_build)

        mock_builder = Mock()
        test_facts = {"key": "value"}
        rule.build(mock_builder, test_facts)

        self.assertIs(called["builder"], mock_builder)
        self.assertEqual(called["facts"], test_facts)

    def test_order_zero(self):
        """Test StepRule with order 0."""

        def dummy_when(facts):
            return True

        def dummy_build(builder, facts):
            pass

        rule = rules.StepRule(name="test", when=dummy_when, build=dummy_build, order=0)
        self.assertEqual(rule.order, 0)

    def test_order_negative(self):
        """Test StepRule with negative order."""

        def dummy_when(facts):
            return True

        def dummy_build(builder, facts):
            pass

        rule = rules.StepRule(
            name="test", when=dummy_when, build=dummy_build, order=-10
        )
        self.assertEqual(rule.order, -10)

    def test_order_high_value(self):
        """Test StepRule with high order value."""

        def dummy_when(facts):
            return True

        def dummy_build(builder, facts):
            pass

        rule = rules.StepRule(
            name="test", when=dummy_when, build=dummy_build, order=1000
        )
        self.assertEqual(rule.order, 1000)

    def test_name_with_special_chars(self):
        """Test StepRule with name containing special characters."""

        def dummy_when(facts):
            return True

        def dummy_build(builder, facts):
            pass

        rule = rules.StepRule(
            name="test-rule_123.v2", when=dummy_when, build=dummy_build
        )
        self.assertEqual(rule.name, "test-rule_123.v2")

    def test_equality(self):
        """Test StepRule equality."""

        def guard(facts):
            return True

        def build(builder, facts):
            pass

        rule1 = rules.StepRule(name="test", when=guard, build=build, order=10)
        rule2 = rules.StepRule(name="test", when=guard, build=build, order=10)

        self.assertEqual(rule1, rule2)

    def test_inequality_different_name(self):
        """Test StepRule inequality with different names."""

        def guard(facts):
            return True

        def build(builder, facts):
            pass

        rule1 = rules.StepRule(name="test1", when=guard, build=build)
        rule2 = rules.StepRule(name="test2", when=guard, build=build)

        self.assertNotEqual(rule1, rule2)

    def test_inequality_different_order(self):
        """Test StepRule inequality with different orders."""

        def guard(facts):
            return True

        def build(builder, facts):
            pass

        rule1 = rules.StepRule(name="test", when=guard, build=build, order=1)
        rule2 = rules.StepRule(name="test", when=guard, build=build, order=2)

        self.assertNotEqual(rule1, rule2)


class TestStepRuleRegistry(unittest.TestCase):
    """Comprehensive tests for StepRuleRegistry."""

    def setUp(self):
        import tempfile

        self.registry = rules.StepRuleRegistry()
        self.tempdir = tempfile.TemporaryDirectory()
        self.builder = PlanBuilder(
            plan_id="test_plan",
            realm="test_realm",
            scope={"project": "foo"},
            base=Path(self.tempdir.name),
        )
        self.facts = {"a": 1, "b": 2}

    def tearDown(self):
        self.tempdir.cleanup()

    def test_rule_decorator(self):
        """Test @registry.rule decorator."""
        called = {}

        @self.registry.rule("test", when=lambda f: True, order=10)
        def build_fn(builder, facts):
            called["ok"] = True

        self.assertEqual(len(self.registry._items), 1)
        rule = self.registry._items[0]
        self.assertEqual(rule.name, "test")
        self.assertEqual(rule.order, 10)
        rule.build(self.builder, self.facts)
        self.assertTrue(called["ok"])

    def test_rule_decorator_returns_function(self):
        """Test that @registry.rule returns the decorated function."""

        def original_fn(builder, facts):
            pass  # Changed from return "original" to pass

        decorated_fn = self.registry.rule("test", when=lambda f: True)(original_fn)

        self.assertIs(decorated_fn, original_fn)

    def test_rule_decorator_multiple_rules(self):
        """Test registering multiple rules."""

        @self.registry.rule("rule1", when=lambda f: True)
        def build1(builder, facts):
            pass

        @self.registry.rule("rule2", when=lambda f: True)
        def build2(builder, facts):
            pass

        @self.registry.rule("rule3", when=lambda f: True)
        def build3(builder, facts):
            pass

        self.assertEqual(len(self.registry._items), 3)
        self.assertEqual(self.registry._items[0].name, "rule1")
        self.assertEqual(self.registry._items[1].name, "rule2")
        self.assertEqual(self.registry._items[2].name, "rule3")

    def test_when_decorator_equals(self):
        """Test @registry.when decorator with equals condition."""
        called = {}

        @self.registry.when(key="a", equals=1, name="eq_rule")
        def build_fn(builder, facts):
            called["ok"] = True

        self.assertEqual(len(self.registry._items), 1)
        rule = self.registry._items[0]
        self.assertEqual(rule.name, "eq_rule")
        self.assertTrue(rule.when({"a": 1}))
        self.assertFalse(rule.when({"a": 2}))
        rule.build(self.builder, {"a": 1})
        self.assertTrue(called["ok"])

    def test_when_decorator_in(self):
        """Test @registry.when decorator with in_ condition."""
        called = {}

        @self.registry.when(key="a", in_=[1, 2, 3], name="in_rule")
        def build_fn(builder, facts):
            called["ok"] = True

        rule = self.registry._items[-1]
        self.assertTrue(rule.when({"a": 2}))
        self.assertFalse(rule.when({"a": 5}))

    def test_when_decorator_present(self):
        """Test @registry.when decorator with present condition."""
        called = {}

        @self.registry.when(key="a", present=True, name="present_rule")
        def build_fn(builder, facts):
            called["ok"] = True

        rule = self.registry._items[-1]
        self.assertTrue(rule.when({"a": 1}))
        self.assertFalse(rule.when({"b": 2}))

    def test_when_decorator_predicate(self):
        """Test @registry.when decorator with custom predicate."""
        called = {}

        @self.registry.when(predicate=lambda f: f.get("a") == 42, name="pred_rule")
        def build_fn(builder, facts):
            called["ok"] = True

        rule = self.registry._items[-1]
        self.assertTrue(rule.when({"a": 42}))
        self.assertFalse(rule.when({"a": 0}))

    def test_when_decorator_no_name_uses_function_name(self):
        """Test @registry.when uses function name if name not provided."""

        @self.registry.when(key="test", equals="value")
        def my_custom_rule(builder, facts):
            pass

        rule = self.registry._items[-1]
        self.assertEqual(rule.name, "my_custom_rule")

    def test_when_decorator_default_order(self):
        """Test @registry.when uses default order 100."""

        @self.registry.when(key="test", equals="value")
        def test_rule(builder, facts):
            pass

        rule = self.registry._items[-1]
        self.assertEqual(rule.order, 100)

    def test_when_decorator_custom_order(self):
        """Test @registry.when with custom order."""

        @self.registry.when(key="test", equals="value", order=50)
        def test_rule(builder, facts):
            pass

        rule = self.registry._items[-1]
        self.assertEqual(rule.order, 50)

    def test_when_decorator_combined_conditions(self):
        """Test @registry.when with multiple conditions combined."""

        @self.registry.when(
            key="status",
            equals="active",
            predicate=lambda f: f.get("priority") == "high",
        )
        def test_rule(builder, facts):
            pass

        rule = self.registry._items[-1]

        # Both conditions must be true
        self.assertTrue(rule.when({"status": "active", "priority": "high"}))
        self.assertFalse(rule.when({"status": "active", "priority": "low"}))
        self.assertFalse(rule.when({"status": "inactive", "priority": "high"}))

    def test_when_decorator_present_false(self):
        """Test @registry.when with present=False (key should not exist)."""

        @self.registry.when(key="optional_key", present=False)
        def test_rule(builder, facts):
            pass

        rule = self.registry._items[-1]

        # Note: present=False doesn't check for absence, it's just not checked
        # The current implementation only checks if present=True
        # If present is False or None, no check is performed

    def test_when_decorator_in_with_empty_list(self):
        """Test @registry.when with empty in_ list."""

        @self.registry.when(key="status", in_=[])
        def test_rule(builder, facts):
            pass

        rule = self.registry._items[-1]

        # Should never match since in_ is empty
        self.assertFalse(rule.when({"status": "any"}))

    def test_when_decorator_equals_none(self):
        """Test @registry.when with equals=None."""

        @self.registry.when(key="optional", equals=None)
        def test_rule(builder, facts):
            pass

        rule = self.registry._items[-1]

        self.assertTrue(rule.when({"optional": None}))
        self.assertTrue(rule.when({}))  # Missing key means get returns None
        # Note: equals=None matches both missing keys and explicit None values

    def test_when_decorator_key_with_nested_access(self):
        """Test @registry.when with key that doesn't handle nested access."""

        # Note: Current implementation uses facts.get(key), so no nested access
        @self.registry.when(key="nested.key", equals="value")
        def test_rule(builder, facts):
            pass

        rule = self.registry._items[-1]

        # Looks for literal key "nested.key", not nested["key"]
        self.assertTrue(rule.when({"nested.key": "value"}))
        self.assertFalse(rule.when({"nested": {"key": "value"}}))

    def test_active_for_and_apply(self):
        """Test active_for and apply methods."""
        calls = []

        @self.registry.when(key="a", equals=1, name="r1", order=2)
        def build1(builder, facts):
            calls.append("r1")

        @self.registry.when(key="b", equals=2, name="r2", order=1)
        def build2(builder, facts):
            calls.append("r2")

        active = self.registry.active_for({"a": 1, "b": 2})
        self.assertEqual([r.name for r in active], ["r2", "r1"])
        self.registry.apply(builder=self.builder, facts={"a": 1, "b": 2})
        self.assertEqual(calls, ["r2", "r1"])

    def test_active_for_no_matches(self):
        """Test active_for when no rules match."""

        @self.registry.when(key="x", equals=100)
        def test_rule(builder, facts):
            pass

        active = self.registry.active_for({"y": 200})
        self.assertEqual(len(active), 0)

    def test_active_for_all_match(self):
        """Test active_for when all rules match."""

        @self.registry.when(key="status", equals="active", name="rule1")
        def rule1(builder, facts):
            pass

        @self.registry.when(key="priority", equals="high", name="rule2")
        def rule2(builder, facts):
            pass

        @self.registry.when(predicate=lambda f: True, name="rule3")
        def rule3(builder, facts):
            pass

        active = self.registry.active_for({"status": "active", "priority": "high"})
        self.assertEqual(len(active), 3)

    def test_active_for_sorted_by_order(self):
        """Test active_for returns rules sorted by order."""

        @self.registry.when(predicate=lambda f: True, name="rule_order_50", order=50)
        def rule1(builder, facts):
            pass

        @self.registry.when(predicate=lambda f: True, name="rule_order_10", order=10)
        def rule2(builder, facts):
            pass

        @self.registry.when(predicate=lambda f: True, name="rule_order_100", order=100)
        def rule3(builder, facts):
            pass

        active = self.registry.active_for({})
        self.assertEqual(active[0].name, "rule_order_10")
        self.assertEqual(active[1].name, "rule_order_50")
        self.assertEqual(active[2].name, "rule_order_100")

    def test_apply_empty_registry(self):
        """Test apply with empty registry does nothing."""
        empty_registry = rules.StepRuleRegistry()
        # Should not raise any errors
        empty_registry.apply(builder=self.builder, facts=self.facts)

    def test_apply_no_matching_rules(self):
        """Test apply when no rules match."""
        calls = []

        @self.registry.when(key="nonexistent", equals="value")
        def test_rule(builder, facts):
            calls.append("called")

        self.registry.apply(builder=self.builder, facts={"other": "data"})
        self.assertEqual(len(calls), 0)

    def test_apply_passes_builder_and_facts(self):
        """Test apply passes correct builder and facts to build functions."""
        captured = {}

        @self.registry.when(predicate=lambda f: True)
        def capture_args(builder, facts):
            captured["builder"] = builder
            captured["facts"] = facts

        test_facts = {"test_key": "test_value"}
        self.registry.apply(builder=self.builder, facts=test_facts)

        self.assertIs(captured["builder"], self.builder)
        self.assertEqual(captured["facts"], test_facts)

    def test_apply_executes_in_order(self):
        """Test apply executes build functions in order."""
        execution_order = []

        @self.registry.when(predicate=lambda f: True, order=3)
        def rule3(builder, facts):
            execution_order.append(3)

        @self.registry.when(predicate=lambda f: True, order=1)
        def rule1(builder, facts):
            execution_order.append(1)

        @self.registry.when(predicate=lambda f: True, order=2)
        def rule2(builder, facts):
            execution_order.append(2)

        self.registry.apply(builder=self.builder, facts={})
        self.assertEqual(execution_order, [1, 2, 3])

    def test_registry_items_accessible(self):
        """Test that _items list is accessible."""

        @self.registry.when(predicate=lambda f: True)
        def test_rule(builder, facts):
            pass

        self.assertIsInstance(self.registry._items, list)
        self.assertEqual(len(self.registry._items), 1)

    def test_registry_items_mutable(self):
        """Test that _items list can be modified."""
        initial_len = len(self.registry._items)

        def dummy_when(facts):
            return True

        def dummy_build(builder, facts):
            pass

        # Directly append to _items
        self.registry._items.append(
            rules.StepRule(name="manual", when=dummy_when, build=dummy_build)
        )

        self.assertEqual(len(self.registry._items), initial_len + 1)

    def test_multiple_registries_independent(self):
        """Test that multiple registries are independent."""
        registry1 = rules.StepRuleRegistry()
        registry2 = rules.StepRuleRegistry()

        @registry1.when(predicate=lambda f: True)
        def rule1(builder, facts):
            pass

        @registry2.when(predicate=lambda f: True)
        def rule2(builder, facts):
            pass

        self.assertEqual(len(registry1._items), 1)
        self.assertEqual(len(registry2._items), 1)
        self.assertNotEqual(registry1._items[0], registry2._items[0])


class TestPredicates(unittest.TestCase):
    """Comprehensive tests for predicate helper functions."""

    def test_when_eq(self):
        """Test when_eq predicate helper."""
        guard = rules.when_eq("foo", 42)
        self.assertTrue(guard({"foo": 42}))
        self.assertFalse(guard({"foo": 0}))

    def test_when_eq_with_string(self):
        """Test when_eq with string values."""
        guard = rules.when_eq("status", "active")
        self.assertTrue(guard({"status": "active"}))
        self.assertFalse(guard({"status": "inactive"}))

    def test_when_eq_with_none(self):
        """Test when_eq with None value."""
        guard = rules.when_eq("optional", None)
        self.assertTrue(guard({"optional": None}))
        self.assertTrue(guard({}))  # Missing key returns None from .get()
        self.assertFalse(guard({"optional": "value"}))

    def test_when_eq_missing_key(self):
        """Test when_eq when key is missing."""
        guard = rules.when_eq("missing_key", "value")
        self.assertFalse(guard({}))
        self.assertFalse(guard({"other_key": "value"}))

    def test_when_in(self):
        """Test when_in predicate helper."""
        guard = rules.when_in("bar", [1, 2, 3])
        self.assertTrue(guard({"bar": 2}))
        self.assertFalse(guard({"bar": 5}))

    def test_when_in_with_strings(self):
        """Test when_in with string values."""
        guard = rules.when_in("status", ["active", "pending", "approved"])
        self.assertTrue(guard({"status": "active"}))
        self.assertTrue(guard({"status": "pending"}))
        self.assertFalse(guard({"status": "rejected"}))

    def test_when_in_empty_list(self):
        """Test when_in with empty values list."""
        guard = rules.when_in("key", [])
        self.assertFalse(guard({"key": "any_value"}))

    def test_when_in_missing_key(self):
        """Test when_in when key is missing."""
        guard = rules.when_in("missing_key", [1, 2, 3])
        self.assertFalse(guard({}))

    def test_when_in_with_none(self):
        """Test when_in with None in values."""
        guard = rules.when_in("optional", [None, "value1", "value2"])
        self.assertTrue(guard({"optional": None}))
        self.assertTrue(guard({}))  # Missing key returns None
        self.assertTrue(guard({"optional": "value1"}))
        self.assertFalse(guard({"optional": "other"}))

    def test_when_in_converts_to_set(self):
        """Test when_in converts values to set (performance)."""
        # This tests the implementation detail that it uses set()
        guard = rules.when_in("key", [1, 2, 3, 2, 1])  # Duplicates
        # Should still work correctly
        self.assertTrue(guard({"key": 2}))
        self.assertFalse(guard({"key": 5}))

    def test_when_in_with_tuple(self):
        """Test when_in with tuple values."""
        guard = rules.when_in("key", (1, 2, 3))
        self.assertTrue(guard({"key": 2}))
        self.assertFalse(guard({"key": 5}))

    def test_when_eq_type_sensitive(self):
        """Test when_eq is type sensitive for strings vs ints."""
        guard = rules.when_eq("num", 1)
        self.assertTrue(guard({"num": 1}))
        self.assertFalse(guard({"num": "1"}))  # String "1" != int 1
        self.assertTrue(guard({"num": 1.0}))  # Float 1.0 == int 1 in Python

    def test_when_in_type_sensitive(self):
        """Test when_in is type sensitive."""
        guard = rules.when_in("value", [1, 2, 3])
        self.assertTrue(guard({"value": 1}))
        self.assertFalse(guard({"value": "1"}))

    def test_predicates_composable(self):
        """Test that predicates can be composed."""
        eq_guard = rules.when_eq("status", "active")
        in_guard = rules.when_in("priority", ["high", "critical"])

        # Compose with lambda
        combined_guard = lambda facts: eq_guard(facts) and in_guard(facts)

        self.assertTrue(combined_guard({"status": "active", "priority": "high"}))
        self.assertFalse(combined_guard({"status": "inactive", "priority": "high"}))
        self.assertFalse(combined_guard({"status": "active", "priority": "low"}))

    def test_when_eq_with_complex_value(self):
        """Test when_eq with complex value types."""
        guard = rules.when_eq("config", {"enabled": True, "timeout": 30})
        self.assertTrue(guard({"config": {"enabled": True, "timeout": 30}}))
        self.assertFalse(guard({"config": {"enabled": False, "timeout": 30}}))

    def test_when_in_with_tuple_values(self):
        """Test when_in works with tuple values (hashable)."""
        # when_in requires hashable values since it creates a set
        guard = rules.when_in("coord", [(1, 2), (3, 4), (5, 6)])
        self.assertTrue(guard({"coord": (1, 2)}))
        self.assertTrue(guard({"coord": (3, 4)}))
        self.assertFalse(guard({"coord": (7, 8)}))


if __name__ == "__main__":
    unittest.main()
