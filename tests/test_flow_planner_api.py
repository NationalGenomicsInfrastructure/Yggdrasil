import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock

from yggdrasil.flow.model import Plan, StepSpec
from yggdrasil.flow.planner.api import (
    FactsProvider,
    PlanDraft,
    Planner,
    PlanningContext,
)


class TestFactsProvider(unittest.TestCase):
    """
    Comprehensive tests for FactsProvider ABC.

    Tests the abstract base class for normalizing documents into facts.
    """

    # =====================================================
    # ABC COMPLIANCE TESTS
    # =====================================================

    def test_facts_provider_is_abstract(self):
        """Test that FactsProvider cannot be instantiated directly."""
        with self.assertRaises(TypeError):
            FactsProvider()  # type: ignore

    def test_facts_provider_requires_distil_facts(self):
        """Test that subclass must implement distil_facts."""

        class IncompleteProvider(FactsProvider):
            pass

        with self.assertRaises(TypeError):
            IncompleteProvider()  # type: ignore

    def test_facts_provider_concrete_implementation(self):
        """Test that concrete implementation can be created."""

        class ConcreteProvider(FactsProvider):
            def distil_facts(self, doc):
                return {"key": "value"}

        provider = ConcreteProvider()
        self.assertIsInstance(provider, FactsProvider)

    def test_distil_facts_has_correct_signature(self):
        """Test that distil_facts has expected signature."""

        class TestProvider(FactsProvider):
            def distil_facts(self, doc):
                return doc

        provider = TestProvider()
        result = provider.distil_facts({"test": "data"})
        self.assertEqual(result, {"test": "data"})

    # =====================================================
    # IMPLEMENTATION TESTS
    # =====================================================

    def test_custom_facts_provider_simple(self):
        """Test custom FactsProvider with simple logic."""

        class SimpleProvider(FactsProvider):
            def distil_facts(self, doc):
                return {"project_id": doc.get("id"), "status": doc.get("status")}

        provider = SimpleProvider()
        doc = {"id": "P123", "status": "active", "extra": "ignored"}
        facts = provider.distil_facts(doc)

        self.assertEqual(facts["project_id"], "P123")
        self.assertEqual(facts["status"], "active")
        self.assertNotIn("extra", facts)

    def test_custom_facts_provider_with_transformation(self):
        """Test FactsProvider with data transformation."""

        class TransformingProvider(FactsProvider):
            def distil_facts(self, doc):
                return {
                    "project_id": doc.get("id", "").upper(),
                    "sample_count": len(doc.get("samples", [])),
                    "has_metadata": "metadata" in doc,
                }

        provider = TransformingProvider()
        doc = {
            "id": "p123",
            "samples": ["s1", "s2", "s3"],
            "metadata": {"key": "value"},
        }
        facts = provider.distil_facts(doc)

        self.assertEqual(facts["project_id"], "P123")
        self.assertEqual(facts["sample_count"], 3)
        self.assertTrue(facts["has_metadata"])

    def test_custom_facts_provider_handles_empty_doc(self):
        """Test FactsProvider with empty document."""

        class SafeProvider(FactsProvider):
            def distil_facts(self, doc):
                return {
                    "project_id": doc.get("id", "unknown"),
                    "status": doc.get("status", "pending"),
                }

        provider = SafeProvider()
        facts = provider.distil_facts({})

        self.assertEqual(facts["project_id"], "unknown")
        self.assertEqual(facts["status"], "pending")

    def test_multiple_facts_providers(self):
        """Test multiple FactsProvider implementations."""

        class ProjectProvider(FactsProvider):
            def distil_facts(self, doc):
                return {"type": "project", "id": doc.get("project_id")}

        class SampleProvider(FactsProvider):
            def distil_facts(self, doc):
                return {"type": "sample", "id": doc.get("sample_id")}

        proj_provider = ProjectProvider()
        sample_provider = SampleProvider()

        proj_facts = proj_provider.distil_facts({"project_id": "P123"})
        sample_facts = sample_provider.distil_facts({"sample_id": "S456"})

        self.assertEqual(proj_facts["type"], "project")
        self.assertEqual(sample_facts["type"], "sample")


class TestPlanningContext(unittest.TestCase):
    """
    Comprehensive tests for PlanningContext dataclass.

    Tests the context object passed to planners containing scope,
    source documents, and configuration.
    """

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = TemporaryDirectory()
        self.scope_dir = Path(self.temp_dir.name) / "scope"
        self.scope_dir.mkdir()

    def tearDown(self):
        """Clean up temporary resources."""
        self.temp_dir.cleanup()

    # =====================================================
    # INITIALIZATION TESTS
    # =====================================================

    def test_planning_context_initialization_minimal(self):
        """Test PlanningContext with minimal required fields."""
        ctx = PlanningContext(
            realm="test",
            scope={"kind": "project", "id": "P123"},
            scope_dir=self.scope_dir,
            emitter=None,
            source_doc={"id": "doc123"},
            reason="test trigger",
        )

        self.assertEqual(ctx.realm, "test")
        self.assertEqual(ctx.scope["kind"], "project")
        self.assertEqual(ctx.scope_dir, self.scope_dir)
        self.assertIsNone(ctx.emitter)
        self.assertEqual(ctx.source_doc["id"], "doc123")
        self.assertEqual(ctx.reason, "test trigger")
        self.assertIsNone(ctx.realm_config)

    def test_planning_context_with_all_fields(self):
        """Test PlanningContext with all fields including optional."""
        mock_emitter = Mock()
        realm_config = {"setting1": "value1", "setting2": 42}

        ctx = PlanningContext(
            realm="production",
            scope={"kind": "flowcell", "id": "FC001"},
            scope_dir=self.scope_dir,
            emitter=mock_emitter,
            source_doc={"type": "flowcell", "status": "complete"},
            reason="flowcell completed",
            realm_config=realm_config,
        )

        self.assertEqual(ctx.realm, "production")
        self.assertEqual(ctx.scope["id"], "FC001")
        self.assertIs(ctx.emitter, mock_emitter)
        self.assertEqual(ctx.realm_config["setting1"], "value1")  # type: ignore

    def test_planning_context_is_dataclass(self):
        """Test that PlanningContext behaves as a dataclass."""
        ctx = PlanningContext(
            realm="test",
            scope={},
            scope_dir=self.scope_dir,
            emitter=None,
            source_doc={},
            reason="test",
        )

        # Should have dataclass functionality
        self.assertTrue(hasattr(ctx, "__dataclass_fields__"))

    # =====================================================
    # FIELD TYPE TESTS
    # =====================================================

    def test_planning_context_realm_types(self):
        """Test realm field with various string values."""
        realms = ["production", "staging", "development", "test", "local"]

        for realm in realms:
            ctx = PlanningContext(
                realm=realm,
                scope={},
                scope_dir=self.scope_dir,
                emitter=None,
                source_doc={},
                reason="test",
            )
            self.assertEqual(ctx.realm, realm)

    def test_planning_context_scope_types(self):
        """Test scope field with various dict structures."""
        scopes = [
            {"kind": "project", "id": "P123"},
            {"kind": "flowcell", "id": "FC001", "lane": 1},
            {"kind": "sample", "id": "S456", "project": "P123"},
            {},
        ]

        for scope in scopes:
            ctx = PlanningContext(
                realm="test",
                scope=scope,
                scope_dir=self.scope_dir,
                emitter=None,
                source_doc={},
                reason="test",
            )
            self.assertEqual(ctx.scope, scope)

    def test_planning_context_scope_dir_types(self):
        """Test scope_dir with Path objects."""
        paths = [
            Path("/tmp/test1"),
            Path("/var/data/scope"),
            self.scope_dir,
        ]

        for path in paths:
            ctx = PlanningContext(
                realm="test",
                scope={},
                scope_dir=path,
                emitter=None,
                source_doc={},
                reason="test",
            )
            self.assertEqual(ctx.scope_dir, path)

    def test_planning_context_emitter_types(self):
        """Test emitter field with various types."""
        emitters = [None, Mock(), "string_emitter", 42]

        for emitter in emitters:
            ctx = PlanningContext(
                realm="test",
                scope={},
                scope_dir=self.scope_dir,
                emitter=emitter,
                source_doc={},
                reason="test",
            )
            self.assertEqual(ctx.emitter, emitter)

    def test_planning_context_source_doc_types(self):
        """Test source_doc with various dict structures."""
        docs = [
            {"id": "doc123", "type": "project"},
            {"complex": {"nested": {"data": [1, 2, 3]}}},
            {},
        ]

        for doc in docs:
            ctx = PlanningContext(
                realm="test",
                scope={},
                scope_dir=self.scope_dir,
                emitter=None,
                source_doc=doc,
                reason="test",
            )
            self.assertEqual(ctx.source_doc, doc)

    def test_planning_context_reason_types(self):
        """Test reason field with various strings."""
        reasons = [
            "project updated",
            "flowcell completed",
            "manual trigger",
            "",
        ]

        for reason in reasons:
            ctx = PlanningContext(
                realm="test",
                scope={},
                scope_dir=self.scope_dir,
                emitter=None,
                source_doc={},
                reason=reason,
            )
            self.assertEqual(ctx.reason, reason)

    # =====================================================
    # USE CASE TESTS
    # =====================================================

    def test_planning_context_for_project_workflow(self):
        """Test PlanningContext for project-based workflow."""
        ctx = PlanningContext(
            realm="production",
            scope={"kind": "project", "id": "P2025001", "name": "Cancer Study"},
            scope_dir=Path("/data/projects/P2025001"),
            emitter=Mock(),
            source_doc={
                "id": "P2025001",
                "status": "samples_received",
                "sample_count": 24,
            },
            reason="project status changed to samples_received",
            realm_config={"analysis_pipeline": "v2.1", "notify": True},
        )

        self.assertEqual(ctx.scope["kind"], "project")
        self.assertEqual(ctx.scope["id"], "P2025001")
        self.assertEqual(ctx.source_doc["sample_count"], 24)
        self.assertTrue(ctx.realm_config["notify"])  # type: ignore

    def test_planning_context_for_flowcell_workflow(self):
        """Test PlanningContext for flowcell-based workflow."""
        ctx = PlanningContext(
            realm="production",
            scope={"kind": "flowcell", "id": "FC_2025_001", "instrument": "NovaSeq"},
            scope_dir=Path("/data/flowcells/FC_2025_001"),
            emitter=Mock(),
            source_doc={
                "id": "FC_2025_001",
                "status": "sequencing_complete",
                "run_date": "2025-01-12",
            },
            reason="sequencing completed",
        )

        self.assertEqual(ctx.scope["kind"], "flowcell")
        self.assertEqual(ctx.source_doc["status"], "sequencing_complete")


class TestPlanDraft(unittest.TestCase):
    """
    Comprehensive tests for PlanDraft dataclass.

    Tests the draft plan object returned by planners including
    approval requirements and metadata.
    """

    # =====================================================
    # INITIALIZATION TESTS
    # =====================================================

    def test_plan_draft_initialization_minimal(self):
        """Test PlanDraft with minimal fields."""
        plan = Plan(plan_id="p1", realm="test", scope={}, steps=[])
        draft = PlanDraft(plan=plan)

        self.assertEqual(draft.plan, plan)
        self.assertTrue(draft.auto_run)
        self.assertEqual(draft.approvals_required, [])
        self.assertEqual(draft.notes, "")
        self.assertEqual(draft.preview, {})

    def test_plan_draft_initialization_with_all_fields(self):
        """Test PlanDraft with all fields."""
        plan = Plan(plan_id="p1", realm="test", scope={}, steps=[])
        draft = PlanDraft(
            plan=plan,
            auto_run=False,
            approvals_required=["manager", "scientist"],
            notes="Requires manual review",
            preview={"estimated_duration": "2h", "cost": 100},
        )

        self.assertEqual(draft.plan, plan)
        self.assertFalse(draft.auto_run)
        self.assertEqual(len(draft.approvals_required), 2)
        self.assertIn("manager", draft.approvals_required)
        self.assertEqual(draft.notes, "Requires manual review")
        self.assertEqual(draft.preview["cost"], 100)

    def test_plan_draft_is_dataclass(self):
        """Test that PlanDraft behaves as a dataclass."""
        plan = Plan(plan_id="p1", realm="test", scope={}, steps=[])
        draft = PlanDraft(plan=plan)

        self.assertTrue(hasattr(draft, "__dataclass_fields__"))

    # =====================================================
    # FIELD MUTATION TESTS
    # =====================================================

    def test_plan_draft_auto_run_modification(self):
        """Test modifying auto_run field."""
        plan = Plan(plan_id="p1", realm="test", scope={}, steps=[])
        draft = PlanDraft(plan=plan, auto_run=True)

        self.assertTrue(draft.auto_run)

        draft.auto_run = False
        self.assertFalse(draft.auto_run)

    def test_plan_draft_approvals_modification(self):
        """Test modifying approvals_required list."""
        plan = Plan(plan_id="p1", realm="test", scope={}, steps=[])
        draft = PlanDraft(plan=plan)

        self.assertEqual(draft.approvals_required, [])

        draft.approvals_required.append("approver1")
        draft.approvals_required.append("approver2")

        self.assertEqual(len(draft.approvals_required), 2)

    def test_plan_draft_notes_modification(self):
        """Test modifying notes field."""
        plan = Plan(plan_id="p1", realm="test", scope={}, steps=[])
        draft = PlanDraft(plan=plan, notes="Initial note")

        self.assertEqual(draft.notes, "Initial note")

        draft.notes = "Updated note"
        self.assertEqual(draft.notes, "Updated note")

    def test_plan_draft_preview_modification(self):
        """Test modifying preview dict."""
        plan = Plan(plan_id="p1", realm="test", scope={}, steps=[])
        draft = PlanDraft(plan=plan)

        self.assertEqual(draft.preview, {})

        draft.preview["key1"] = "value1"
        draft.preview["key2"] = 42

        self.assertEqual(draft.preview["key1"], "value1")
        self.assertEqual(draft.preview["key2"], 42)

    # =====================================================
    # USE CASE TESTS
    # =====================================================

    def test_plan_draft_auto_run_scenario(self):
        """Test PlanDraft for auto-run scenario."""
        plan = Plan(
            plan_id="auto_plan_001",
            realm="production",
            scope={"kind": "project", "id": "P123"},
            steps=[
                StepSpec(
                    step_id="s1",
                    name="preprocessing",
                    fn_ref="module:preprocess",
                    params={},
                )
            ],
        )
        draft = PlanDraft(
            plan=plan,
            auto_run=True,
            notes="Standard preprocessing pipeline",
            preview={"steps_count": 1, "estimated_time": "30m"},
        )

        self.assertTrue(draft.auto_run)
        self.assertEqual(draft.approvals_required, [])
        self.assertEqual(draft.preview["steps_count"], 1)

    def test_plan_draft_approval_required_scenario(self):
        """Test PlanDraft requiring approval."""
        plan = Plan(
            plan_id="manual_plan_001",
            realm="production",
            scope={"kind": "project", "id": "P456"},
            steps=[],
        )
        draft = PlanDraft(
            plan=plan,
            auto_run=False,
            approvals_required=["lab_manager", "pi", "quality_control"],
            notes="High-cost analysis requiring multiple approvals",
            preview={
                "estimated_cost": 5000,
                "resource_intensive": True,
                "reason": "Custom analysis pipeline",
            },
        )

        self.assertFalse(draft.auto_run)
        self.assertEqual(len(draft.approvals_required), 3)
        self.assertIn("pi", draft.approvals_required)
        self.assertTrue(draft.preview["resource_intensive"])

    def test_plan_draft_with_complex_preview(self):
        """Test PlanDraft with complex preview data."""
        plan = Plan(plan_id="p1", realm="test", scope={}, steps=[])
        draft = PlanDraft(
            plan=plan,
            preview={
                "summary": {"total_steps": 5, "estimated_duration": "2h"},
                "resources": {"cpu": 16, "memory": "64GB", "disk": "500GB"},
                "steps_preview": [
                    {"step_id": "s1", "name": "prep", "duration": "10m"},
                    {"step_id": "s2", "name": "analysis", "duration": "1.5h"},
                ],
            },
        )

        self.assertEqual(draft.preview["summary"]["total_steps"], 5)
        self.assertEqual(draft.preview["resources"]["cpu"], 16)
        self.assertEqual(len(draft.preview["steps_preview"]), 2)

    def test_plan_draft_empty_approvals_list(self):
        """Test that empty approvals list is created by default."""
        plan = Plan(plan_id="p1", realm="test", scope={}, steps=[])
        draft1 = PlanDraft(plan=plan)
        draft2 = PlanDraft(plan=plan)

        # Each instance should have its own list
        draft1.approvals_required.append("approver1")

        self.assertEqual(len(draft1.approvals_required), 1)
        self.assertEqual(len(draft2.approvals_required), 0)


class TestPlannerProtocol(unittest.TestCase):
    """
    Comprehensive tests for Planner Protocol.

    Tests the protocol definition for planner implementations.
    """

    # =====================================================
    # PROTOCOL COMPLIANCE TESTS
    # =====================================================

    def test_planner_protocol_has_generate_method(self):
        """Test that Planner protocol requires generate method."""
        self.assertTrue(hasattr(Planner, "generate"))

    def test_custom_planner_can_implement_protocol(self):
        """Test that custom classes can implement Planner protocol."""

        class CustomPlanner:
            def generate(self, ctx):
                plan = Plan(
                    plan_id="custom_plan",
                    realm=ctx.realm,
                    scope=ctx.scope,
                    steps=[],
                )
                return PlanDraft(plan=plan)

        planner = CustomPlanner()
        self.assertTrue(hasattr(planner, "generate"))
        self.assertTrue(callable(planner.generate))

    def test_planner_implementation_with_simple_logic(self):
        """Test simple Planner implementation."""
        temp_dir = TemporaryDirectory()
        try:

            class SimplePlanner:
                def generate(self, ctx: PlanningContext) -> PlanDraft:
                    plan = Plan(
                        plan_id=f"plan_{ctx.scope.get('id', 'unknown')}",
                        realm=ctx.realm,
                        scope=ctx.scope,
                        steps=[],
                    )
                    return PlanDraft(plan=plan, auto_run=True)

            planner = SimplePlanner()
            ctx = PlanningContext(
                realm="test",
                scope={"id": "P123"},
                scope_dir=Path(temp_dir.name),
                emitter=None,
                source_doc={},
                reason="test",
            )

            draft = planner.generate(ctx)

            self.assertIsInstance(draft, PlanDraft)
            self.assertEqual(draft.plan.plan_id, "plan_P123")
            self.assertTrue(draft.auto_run)
        finally:
            temp_dir.cleanup()

    def test_planner_implementation_with_conditional_logic(self):
        """Test Planner with conditional auto_run logic."""
        temp_dir = TemporaryDirectory()
        try:

            class ConditionalPlanner:
                def generate(self, ctx: PlanningContext) -> PlanDraft:
                    plan = Plan(
                        plan_id="conditional_plan",
                        realm=ctx.realm,
                        scope=ctx.scope,
                        steps=[],
                    )

                    # Require approval for production realm
                    auto_run = ctx.realm != "production"
                    approvals = ["manager"] if ctx.realm == "production" else []

                    return PlanDraft(
                        plan=plan, auto_run=auto_run, approvals_required=approvals
                    )

            planner = ConditionalPlanner()

            # Test with production realm
            ctx_prod = PlanningContext(
                realm="production",
                scope={},
                scope_dir=Path(temp_dir.name),
                emitter=None,
                source_doc={},
                reason="test",
            )
            draft_prod = planner.generate(ctx_prod)

            self.assertFalse(draft_prod.auto_run)
            self.assertIn("manager", draft_prod.approvals_required)

            # Test with test realm
            ctx_test = PlanningContext(
                realm="test",
                scope={},
                scope_dir=Path(temp_dir.name),
                emitter=None,
                source_doc={},
                reason="test",
            )
            draft_test = planner.generate(ctx_test)

            self.assertTrue(draft_test.auto_run)
            self.assertEqual(draft_test.approvals_required, [])
        finally:
            temp_dir.cleanup()

    def test_planner_implementation_uses_context_data(self):
        """Test Planner that uses context data to build plan."""
        temp_dir = TemporaryDirectory()
        try:

            class DataDrivenPlanner:
                def generate(self, ctx: PlanningContext) -> PlanDraft:
                    sample_count = ctx.source_doc.get("sample_count", 0)

                    steps = []
                    if sample_count > 0:
                        steps.append(
                            StepSpec(
                                step_id="s1",
                                name="process_samples",
                                fn_ref="module:process",
                                params={"count": sample_count},
                            )
                        )

                    plan = Plan(
                        plan_id="data_driven_plan",
                        realm=ctx.realm,
                        scope=ctx.scope,
                        steps=steps,
                    )

                    return PlanDraft(
                        plan=plan,
                        preview={"sample_count": sample_count},
                    )

            planner = DataDrivenPlanner()
            ctx = PlanningContext(
                realm="test",
                scope={},
                scope_dir=Path(temp_dir.name),
                emitter=None,
                source_doc={"sample_count": 5},
                reason="test",
            )

            draft = planner.generate(ctx)

            self.assertEqual(len(draft.plan.steps), 1)
            self.assertEqual(draft.plan.steps[0].params["count"], 5)
            self.assertEqual(draft.preview["sample_count"], 5)
        finally:
            temp_dir.cleanup()


class TestAPIIntegration(unittest.TestCase):
    """
    Integration tests for planner API components.
    """

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = TemporaryDirectory()

    def tearDown(self):
        """Clean up temporary resources."""
        self.temp_dir.cleanup()

    def test_complete_planning_workflow(self):
        """Test complete workflow from facts to plan draft."""

        # 1. FactsProvider normalizes document
        class ProjectFactsProvider(FactsProvider):
            def distil_facts(self, doc):
                return {
                    "project_id": doc.get("id"),
                    "status": doc.get("status"),
                    "sample_count": len(doc.get("samples", [])),
                }

        # 2. Create planning context
        source_doc = {
            "id": "P123",
            "status": "ready",
            "samples": ["S1", "S2", "S3"],
        }

        facts_provider = ProjectFactsProvider()
        facts = facts_provider.distil_facts(source_doc)

        ctx = PlanningContext(
            realm="production",
            scope={"kind": "project", "id": facts["project_id"]},
            scope_dir=Path(self.temp_dir.name) / facts["project_id"],
            emitter=Mock(),
            source_doc=source_doc,
            reason="project ready for processing",
        )

        # 3. Planner generates draft
        class ProjectPlanner:
            def generate(self, ctx: PlanningContext) -> PlanDraft:
                steps = [
                    StepSpec(
                        step_id="s1",
                        name="analyze",
                        fn_ref="module:analyze",
                        params={"project_id": ctx.scope["id"]},
                    )
                ]

                plan = Plan(
                    plan_id=f"plan_{ctx.scope['id']}",
                    realm=ctx.realm,
                    scope=ctx.scope,
                    steps=steps,
                )

                return PlanDraft(
                    plan=plan,
                    auto_run=True,
                    notes=f"Processing project {ctx.scope['id']}",
                )

        planner = ProjectPlanner()
        draft = planner.generate(ctx)

        # Verify complete workflow
        self.assertEqual(facts["sample_count"], 3)
        self.assertEqual(draft.plan.plan_id, "plan_P123")
        self.assertEqual(len(draft.plan.steps), 1)
        self.assertTrue(draft.auto_run)

    def test_multi_realm_planning(self):
        """Test planning across multiple realms."""

        class RealmAwarePlanner:
            def generate(self, ctx: PlanningContext) -> PlanDraft:
                plan = Plan(
                    plan_id=f"{ctx.realm}_plan",
                    realm=ctx.realm,
                    scope=ctx.scope,
                    steps=[],
                )

                # Different behavior per realm
                if ctx.realm == "production":
                    return PlanDraft(
                        plan=plan,
                        auto_run=False,
                        approvals_required=["supervisor"],
                        notes="Production requires approval",
                    )
                else:
                    return PlanDraft(
                        plan=plan,
                        auto_run=True,
                        notes="Test/dev can auto-run",
                    )

        planner = RealmAwarePlanner()

        for realm in ["production", "staging", "development"]:
            ctx = PlanningContext(
                realm=realm,
                scope={},
                scope_dir=Path(self.temp_dir.name) / realm,
                emitter=None,
                source_doc={},
                reason="test",
            )

            draft = planner.generate(ctx)

            if realm == "production":
                self.assertFalse(draft.auto_run)
                self.assertIn("supervisor", draft.approvals_required)
            else:
                self.assertTrue(draft.auto_run)


if __name__ == "__main__":
    unittest.main()
