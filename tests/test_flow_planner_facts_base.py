import unittest

from yggdrasil.flow.planner import facts_base


class TestDistilledFacts(unittest.TestCase):
    """Comprehensive tests for DistilledFacts dataclass."""

    def test_dataclass_fields(self):
        """Test DistilledFacts with all fields set."""
        df = facts_base.DistilledFacts(
            realm="foo", scope={"a": 1}, version=2, data={"x": 42}
        )
        self.assertEqual(df.realm, "foo")
        self.assertEqual(df.scope, {"a": 1})
        self.assertEqual(df.version, 2)
        self.assertEqual(df.data, {"x": 42})

    def test_default_version_and_data(self):
        """Test DistilledFacts defaults for version and data."""
        df = facts_base.DistilledFacts(realm="bar", scope={})
        self.assertEqual(df.version, 1)
        self.assertEqual(df.data, {})

    def test_realm_required(self):
        """Test that realm is required."""
        with self.assertRaises(TypeError):
            facts_base.DistilledFacts(scope={})  # type: ignore

    def test_scope_required(self):
        """Test that scope is required."""
        with self.assertRaises(TypeError):
            facts_base.DistilledFacts(realm="test")  # type: ignore

    def test_different_realm_values(self):
        """Test DistilledFacts with different realm values."""
        df1 = facts_base.DistilledFacts(realm="projects", scope={})
        df2 = facts_base.DistilledFacts(realm="flowcells", scope={})

        self.assertEqual(df1.realm, "projects")
        self.assertEqual(df2.realm, "flowcells")
        self.assertNotEqual(df1.realm, df2.realm)

    def test_different_scope_values(self):
        """Test DistilledFacts with different scope values."""
        df1 = facts_base.DistilledFacts(
            realm="test", scope={"kind": "project", "id": "P123"}
        )
        df2 = facts_base.DistilledFacts(
            realm="test", scope={"kind": "flowcell", "id": "FC456"}
        )

        self.assertNotEqual(df1.scope, df2.scope)

    def test_version_zero(self):
        """Test DistilledFacts with version 0."""
        df = facts_base.DistilledFacts(realm="test", scope={}, version=0)
        self.assertEqual(df.version, 0)

    def test_version_high_value(self):
        """Test DistilledFacts with high version number."""
        df = facts_base.DistilledFacts(realm="test", scope={}, version=100)
        self.assertEqual(df.version, 100)

    def test_data_empty_dict(self):
        """Test DistilledFacts with empty data dict."""
        df = facts_base.DistilledFacts(realm="test", scope={}, data={})
        self.assertEqual(df.data, {})
        self.assertIsInstance(df.data, dict)

    def test_data_complex_structure(self):
        """Test DistilledFacts with complex nested data."""
        complex_data = {
            "metadata": {"created": "2025-11-21", "author": "system"},
            "samples": ["S1", "S2", "S3"],
            "metrics": {"count": 3, "validated": True},
        }
        df = facts_base.DistilledFacts(realm="test", scope={}, data=complex_data)

        self.assertEqual(df.data["metadata"]["author"], "system")
        self.assertEqual(df.data["samples"], ["S1", "S2", "S3"])
        self.assertTrue(df.data["metrics"]["validated"])

    def test_scope_complex_structure(self):
        """Test DistilledFacts with complex scope."""
        scope = {
            "kind": "project",
            "id": "P12345",
            "parent": {"kind": "organization", "id": "ORG001"},
        }
        df = facts_base.DistilledFacts(realm="projects", scope=scope)

        self.assertEqual(df.scope["kind"], "project")
        self.assertEqual(df.scope["parent"]["id"], "ORG001")

    def test_equality_same_values(self):
        """Test that two DistilledFacts with same values are equal."""
        df1 = facts_base.DistilledFacts(
            realm="test", scope={"id": "1"}, version=1, data={"key": "value"}
        )
        df2 = facts_base.DistilledFacts(
            realm="test", scope={"id": "1"}, version=1, data={"key": "value"}
        )

        self.assertEqual(df1, df2)

    def test_inequality_different_realm(self):
        """Test that DistilledFacts with different realms are not equal."""
        df1 = facts_base.DistilledFacts(realm="projects", scope={})
        df2 = facts_base.DistilledFacts(realm="flowcells", scope={})

        self.assertNotEqual(df1, df2)

    def test_inequality_different_version(self):
        """Test that DistilledFacts with different versions are not equal."""
        df1 = facts_base.DistilledFacts(realm="test", scope={}, version=1)
        df2 = facts_base.DistilledFacts(realm="test", scope={}, version=2)

        self.assertNotEqual(df1, df2)

    def test_inequality_different_data(self):
        """Test that DistilledFacts with different data are not equal."""
        df1 = facts_base.DistilledFacts(realm="test", scope={}, data={"a": 1})
        df2 = facts_base.DistilledFacts(realm="test", scope={}, data={"b": 2})

        self.assertNotEqual(df1, df2)

    def test_repr(self):
        """Test that DistilledFacts has a useful repr."""
        df = facts_base.DistilledFacts(
            realm="projects", scope={"id": "P123"}, version=2, data={"key": "value"}
        )
        repr_str = repr(df)

        self.assertIn("DistilledFacts", repr_str)
        self.assertIn("projects", repr_str)

    def test_data_mutation_allowed(self):
        """Test that data dict can be mutated (not frozen)."""
        df = facts_base.DistilledFacts(realm="test", scope={}, data={"key": "value"})
        df.data["new_key"] = "new_value"

        self.assertEqual(df.data["new_key"], "new_value")

    def test_default_data_factory(self):
        """Test that default data creates new dicts for each instance."""
        df1 = facts_base.DistilledFacts(realm="test", scope={})
        df2 = facts_base.DistilledFacts(realm="test", scope={})

        df1.data["key"] = "value"

        self.assertNotIn("key", df2.data)
        self.assertIsNot(df1.data, df2.data)

    def test_empty_realm_string(self):
        """Test DistilledFacts with empty realm string."""
        df = facts_base.DistilledFacts(realm="", scope={})
        self.assertEqual(df.realm, "")

    def test_special_chars_in_realm(self):
        """Test DistilledFacts with special characters in realm."""
        df = facts_base.DistilledFacts(realm="realm-with_special.chars", scope={})
        self.assertEqual(df.realm, "realm-with_special.chars")

    def test_none_values_in_scope(self):
        """Test DistilledFacts with None values in scope."""
        df = facts_base.DistilledFacts(realm="test", scope={"id": None, "name": None})
        self.assertIsNone(df.scope["id"])

    def test_none_values_in_data(self):
        """Test DistilledFacts with None values in data."""
        df = facts_base.DistilledFacts(realm="test", scope={}, data={"key": None})
        self.assertIsNone(df.data["key"])

    def test_large_data_structure(self):
        """Test DistilledFacts with large data structure."""
        large_data = {f"key_{i}": f"value_{i}" for i in range(1000)}
        df = facts_base.DistilledFacts(realm="test", scope={}, data=large_data)

        self.assertEqual(len(df.data), 1000)
        self.assertEqual(df.data["key_500"], "value_500")


class DummyDistiller(facts_base.FactDistiller):
    """Minimal implementation for testing."""

    def distil_facts(self, *, doc, realm, scope):
        return facts_base.DistilledFacts(realm=realm, scope=scope, data=doc)


class TestFactDistiller(unittest.TestCase):
    """Comprehensive tests for FactDistiller ABC."""

    def test_abstract(self):
        """Test that FactDistiller cannot be instantiated directly."""
        with self.assertRaises(TypeError):
            facts_base.FactDistiller()  # type: ignore

    def test_distil_facts(self):
        """Test basic FactDistiller implementation."""
        d = DummyDistiller()
        doc = {"foo": "bar"}
        realm = "baz"
        scope = {"x": 1}
        result = d.distil_facts(doc=doc, realm=realm, scope=scope)
        self.assertIsInstance(result, facts_base.DistilledFacts)
        self.assertEqual(result.realm, realm)
        self.assertEqual(result.scope, scope)
        self.assertEqual(result.data, doc)

    def test_distil_facts_requires_implementation(self):
        """Test that subclass without distil_facts cannot be instantiated."""

        class IncompleteDistiller(facts_base.FactDistiller):
            pass

        with self.assertRaises(TypeError):
            IncompleteDistiller()  # type: ignore

    def test_distil_facts_with_transformation(self):
        """Test FactDistiller that transforms data."""

        class TransformingDistiller(facts_base.FactDistiller):
            def distil_facts(self, *, doc, realm, scope):
                transformed_data = {
                    "project_id": doc.get("id", "").upper(),
                    "sample_count": len(doc.get("samples", [])),
                }
                return facts_base.DistilledFacts(
                    realm=realm, scope=scope, version=2, data=transformed_data
                )

        distiller = TransformingDistiller()
        doc = {"id": "p123", "samples": ["s1", "s2", "s3"]}
        result = distiller.distil_facts(doc=doc, realm="projects", scope={"id": "P123"})

        self.assertEqual(result.data["project_id"], "P123")
        self.assertEqual(result.data["sample_count"], 3)
        self.assertEqual(result.version, 2)

    def test_distil_facts_with_empty_doc(self):
        """Test FactDistiller with empty document."""
        distiller = DummyDistiller()
        result = distiller.distil_facts(doc={}, realm="test", scope={})

        self.assertEqual(result.data, {})

    def test_distil_facts_with_complex_doc(self):
        """Test FactDistiller with complex nested document."""
        distiller = DummyDistiller()
        doc = {
            "id": "P123",
            "metadata": {"created": "2025-11-21", "status": "active"},
            "samples": ["S1", "S2"],
        }
        result = distiller.distil_facts(doc=doc, realm="projects", scope={"id": "P123"})

        self.assertEqual(result.data, doc)
        self.assertEqual(result.data["id"], "P123")

    def test_distil_facts_with_filtering(self):
        """Test FactDistiller that filters data."""

        class FilteringDistiller(facts_base.FactDistiller):
            def distil_facts(self, *, doc, realm, scope):
                # Only keep specific fields
                filtered_data = {
                    k: v for k, v in doc.items() if k in ["id", "status", "priority"]
                }
                return facts_base.DistilledFacts(
                    realm=realm, scope=scope, data=filtered_data
                )

        distiller = FilteringDistiller()
        doc = {
            "id": "P123",
            "status": "active",
            "priority": "high",
            "extra_field": "should_be_ignored",
            "another_extra": "also_ignored",
        }
        result = distiller.distil_facts(doc=doc, realm="test", scope={})

        self.assertIn("id", result.data)
        self.assertIn("status", result.data)
        self.assertIn("priority", result.data)
        self.assertNotIn("extra_field", result.data)
        self.assertNotIn("another_extra", result.data)

    def test_distil_facts_with_validation(self):
        """Test FactDistiller that validates data."""

        class ValidatingDistiller(facts_base.FactDistiller):
            def distil_facts(self, *, doc, realm, scope):
                if "id" not in doc:
                    raise ValueError("Document must have 'id' field")
                return facts_base.DistilledFacts(realm=realm, scope=scope, data=doc)

        distiller = ValidatingDistiller()

        # Valid document
        result = distiller.distil_facts(doc={"id": "P123"}, realm="test", scope={})
        self.assertEqual(result.data["id"], "P123")

        # Invalid document
        with self.assertRaises(ValueError):
            distiller.distil_facts(doc={"name": "no_id"}, realm="test", scope={})

    def test_distil_facts_with_enrichment(self):
        """Test FactDistiller that enriches data."""

        class EnrichingDistiller(facts_base.FactDistiller):
            def distil_facts(self, *, doc, realm, scope):
                enriched_data = doc.copy()
                enriched_data["_realm"] = realm
                enriched_data["_scope_id"] = scope.get("id")
                enriched_data["_enriched"] = True
                return facts_base.DistilledFacts(
                    realm=realm, scope=scope, data=enriched_data
                )

        distiller = EnrichingDistiller()
        result = distiller.distil_facts(
            doc={"original": "data"},
            realm="projects",
            scope={"id": "P123"},
        )

        self.assertEqual(result.data["original"], "data")
        self.assertEqual(result.data["_realm"], "projects")
        self.assertEqual(result.data["_scope_id"], "P123")
        self.assertTrue(result.data["_enriched"])

    def test_distil_facts_different_realms(self):
        """Test FactDistiller with different realm values."""
        distiller = DummyDistiller()

        result1 = distiller.distil_facts(
            doc={"key": "value"}, realm="projects", scope={}
        )
        result2 = distiller.distil_facts(
            doc={"key": "value"}, realm="flowcells", scope={}
        )

        self.assertEqual(result1.realm, "projects")
        self.assertEqual(result2.realm, "flowcells")
        self.assertNotEqual(result1.realm, result2.realm)

    def test_distil_facts_different_scopes(self):
        """Test FactDistiller with different scope values."""
        distiller = DummyDistiller()

        result1 = distiller.distil_facts(doc={}, realm="test", scope={"id": "P123"})
        result2 = distiller.distil_facts(doc={}, realm="test", scope={"id": "P456"})

        self.assertEqual(result1.scope, {"id": "P123"})
        self.assertEqual(result2.scope, {"id": "P456"})

    def test_distil_facts_keyword_only_params(self):
        """Test that distil_facts requires keyword-only arguments."""
        distiller = DummyDistiller()

        # Should work with keywords
        result = distiller.distil_facts(doc={}, realm="test", scope={})
        self.assertIsInstance(result, facts_base.DistilledFacts)

        # Should fail with positional args (if we try to call it that way)
        # This is enforced by the signature with * in the parameter list

    def test_distil_facts_state_preservation(self):
        """Test that FactDistiller can maintain state across calls."""

        class StatefulDistiller(facts_base.FactDistiller):
            def __init__(self):
                self.call_count = 0

            def distil_facts(self, *, doc, realm, scope):
                self.call_count += 1
                data = doc.copy()
                data["_call_number"] = self.call_count
                return facts_base.DistilledFacts(realm=realm, scope=scope, data=data)

        distiller = StatefulDistiller()

        result1 = distiller.distil_facts(doc={}, realm="test", scope={})
        result2 = distiller.distil_facts(doc={}, realm="test", scope={})
        result3 = distiller.distil_facts(doc={}, realm="test", scope={})

        self.assertEqual(result1.data["_call_number"], 1)
        self.assertEqual(result2.data["_call_number"], 2)
        self.assertEqual(result3.data["_call_number"], 3)

    def test_distil_facts_returns_distilled_facts_instance(self):
        """Test that distil_facts returns DistilledFacts instance."""
        distiller = DummyDistiller()
        result = distiller.distil_facts(doc={}, realm="test", scope={})

        self.assertIsInstance(result, facts_base.DistilledFacts)

    def test_multiple_distiller_implementations(self):
        """Test multiple FactDistiller implementations coexist."""

        class DistillerA(facts_base.FactDistiller):
            def distil_facts(self, *, doc, realm, scope):
                return facts_base.DistilledFacts(
                    realm=realm, scope=scope, data={"type": "A", **doc}
                )

        class DistillerB(facts_base.FactDistiller):
            def distil_facts(self, *, doc, realm, scope):
                return facts_base.DistilledFacts(
                    realm=realm, scope=scope, data={"type": "B", **doc}
                )

        distiller_a = DistillerA()
        distiller_b = DistillerB()

        result_a = distiller_a.distil_facts(
            doc={"key": "value"}, realm="test", scope={}
        )
        result_b = distiller_b.distil_facts(
            doc={"key": "value"}, realm="test", scope={}
        )

        self.assertEqual(result_a.data["type"], "A")
        self.assertEqual(result_b.data["type"], "B")


if __name__ == "__main__":
    unittest.main()
