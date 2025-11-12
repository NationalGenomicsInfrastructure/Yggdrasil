import unittest

from yggdrasil.flow.planner import facts_base


class TestDistilledFacts(unittest.TestCase):
    def test_dataclass_fields(self):
        df = facts_base.DistilledFacts(
            realm="foo", scope={"a": 1}, version=2, data={"x": 42}
        )
        self.assertEqual(df.realm, "foo")
        self.assertEqual(df.scope, {"a": 1})
        self.assertEqual(df.version, 2)
        self.assertEqual(df.data, {"x": 42})

    def test_default_version_and_data(self):
        df = facts_base.DistilledFacts(realm="bar", scope={})
        self.assertEqual(df.version, 1)
        self.assertEqual(df.data, {})


class DummyDistiller(facts_base.FactDistiller):
    def distil_facts(self, *, doc, realm, scope):
        return facts_base.DistilledFacts(realm=realm, scope=scope, data=doc)


class TestFactDistiller(unittest.TestCase):
    def test_abstract(self):
        with self.assertRaises(TypeError):
            facts_base.FactDistiller()  # type: ignore

    def test_distil_facts(self):
        d = DummyDistiller()
        doc = {"foo": "bar"}
        realm = "baz"
        scope = {"x": 1}
        result = d.distil_facts(doc=doc, realm=realm, scope=scope)
        self.assertIsInstance(result, facts_base.DistilledFacts)
        self.assertEqual(result.realm, realm)
        self.assertEqual(result.scope, scope)
        self.assertEqual(result.data, doc)


if __name__ == "__main__":
    unittest.main()
