import unittest
from pathlib import Path

from yggdrasil.flow.planner import rules
from yggdrasil.flow.planner.builder import PlanBuilder


class TestStepRule(unittest.TestCase):
    def test_fields(self):
        def dummy_when(facts):
            return True

        def dummy_build(builder, facts):
            pass

        rule = rules.StepRule(name="foo", when=dummy_when, build=dummy_build, order=42)
        self.assertEqual(rule.name, "foo")
        self.assertEqual(rule.order, 42)
        self.assertIs(rule.when, dummy_when)
        self.assertIs(rule.build, dummy_build)


class TestStepRuleRegistry(unittest.TestCase):
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

    def test_when_decorator_equals(self):
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
        called = {}

        @self.registry.when(key="a", in_=[1, 2, 3], name="in_rule")
        def build_fn(builder, facts):
            called["ok"] = True

        rule = self.registry._items[-1]
        self.assertTrue(rule.when({"a": 2}))
        self.assertFalse(rule.when({"a": 5}))

    def test_when_decorator_present(self):
        called = {}

        @self.registry.when(key="a", present=True, name="present_rule")
        def build_fn(builder, facts):
            called["ok"] = True

        rule = self.registry._items[-1]
        self.assertTrue(rule.when({"a": 1}))
        self.assertFalse(rule.when({"b": 2}))

    def test_when_decorator_predicate(self):
        called = {}

        @self.registry.when(predicate=lambda f: f.get("a") == 42, name="pred_rule")
        def build_fn(builder, facts):
            called["ok"] = True

        rule = self.registry._items[-1]
        self.assertTrue(rule.when({"a": 42}))
        self.assertFalse(rule.when({"a": 0}))

    def test_active_for_and_apply(self):
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


class TestPredicates(unittest.TestCase):
    def test_when_eq(self):
        guard = rules.when_eq("foo", 42)
        self.assertTrue(guard({"foo": 42}))
        self.assertFalse(guard({"foo": 0}))

    def test_when_in(self):
        guard = rules.when_in("bar", [1, 2, 3])
        self.assertTrue(guard({"bar": 2}))
        self.assertFalse(guard({"bar": 5}))


if __name__ == "__main__":
    unittest.main()
