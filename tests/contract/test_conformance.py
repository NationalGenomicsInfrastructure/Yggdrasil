"""What Yggdrasil's writers produce conforms to the published contract.

A builder's complete output conforms: plan documents, attempt reports and
snapshots, including those that describe malformed plans, cancelled attempts,
orchestration failures, publication failures and legacy histories. An update
writer keeps what it does not own exactly as read and does not certify it,
so the finalizer's own fields conform while a foreign field it kept is still
reported. A malformed plan is the realm's error, and its document does not
conform.
"""

from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Annotated, Any

from tests.contract.support import (
    ABSENT,
    SPOOL_PLAN_ID,
    ConformingBuilders,
    SpoolTestCase,
    assert_conforms,
    changed,
    problems,
    stored_snapshot,
)
from tests.execution_support import REALM, CouchBackend, SQLiteBackend
from tests.plan_store_support import (
    DRAINED_FAILURE,
    PREFLIGHT_REJECTED,
    SCOPE,
    SUCCEEDED,
    continuation_plan,
    finalization_for,
    make_plan,
)
from yggdrasil.contract import ATTEMPT_REPORT, PLAN_DOCUMENT, PLAN_STATUS
from yggdrasil.contract.schema import (
    MISSING_FIELD,
    NOT_IN_VOCABULARY,
    SCHEMA_RULE,
    UNEXPECTED_FIELD,
    UNVERSIONED,
    WRONG_TYPE,
    schema_for,
)
from yggdrasil.flow.artifacts import SimpleArtifactRef
from yggdrasil.flow.errors import OrchestrationError, PermanentStepError
from yggdrasil.flow.events.attempt_records import (
    ATTEMPT_REPORT_EVENT,
    ATTEMPT_STARTED_EVENT,
    record_filename,
)
from yggdrasil.flow.events.emitter import EventEmitter, FileSpoolEmitter
from yggdrasil.flow.model import Plan
from yggdrasil.flow.planner.builder import PlanBuilder
from yggdrasil.flow.step import In, Out, StepContext, step
from yggdrasil.ops.snapshot import ATTEMPT_RUNNING, PROJECTION_LEGACY
from yggdrasil.storage.plan_documents import build_plan_document
from yggdrasil.storage.plan_updates import build_finalized_document

# What a realm may put in its payloads: anything JSON can hold.
NESTED: dict[str, Any] = {
    "lanes": [1, 2, {"lane": 3, "flags": [True, None, 1.5]}],
    "empty": {},
    "deep": {"a": {"b": {"c": ["d"]}}},
}


# Artifacts and steps of a plan that PlanBuilder wires from annotations.
READS = SimpleArtifactRef(key_name="reads", folder="demux")
QC_REPORT = SimpleArtifactRef(key_name="report", folder="qc")


@step
def demux(ctx: StepContext, out: Annotated[Path, Out(READS)]) -> None:
    """Write the reads; never run here."""


@step
def qc(
    ctx: StepContext,
    data: Annotated[Path, In(READS)],
    out: Annotated[Path, Out(QC_REPORT)],
) -> None:
    """Check the reads; never run here."""


def built_document(**kwargs: Any) -> dict[str, Any]:
    """The builder's document of the default two-step plan."""
    return build_plan_document(make_plan(), "test_realm", dict(SCOPE), **kwargs)


def shown(path: str) -> str:
    """Return a dotted path the way a problem names it: ``plan.steps[0].deps``."""
    return path.replace(".0.", "[0].").replace(".1.", "[1].")


def finalized() -> dict[str, Any]:
    """A built continue_independent plan's document, after its first request.

    In the attempt that finished the request, s1 failed and blocked s2, so the
    recorded report carries a failure record and blockers.
    """
    document = build_plan_document(continuation_plan(), "test_realm", dict(SCOPE))
    request = finalization_for(document, ending=DRAINED_FAILURE)
    return build_finalized_document(document, request, now="2026-10-22T10:00:05Z")


class TestPlanDocumentBuilds(ConformingBuilders):
    """Every plan document the builder makes conforms."""

    def test_the_builders_documents_conform(self):
        cases: dict[str, dict[str, Any]] = {
            "draft": built_document(),
            "auto-run": built_document(auto_run=True),
            "continue_independent": build_plan_document(
                continuation_plan(), "test_realm", dict(SCOPE)
            ),
            "run-once": built_document(
                execution_authority="run_once", execution_owner="run_once:abc"
            ),
            "with every optional input": built_document(
                preview={"summary": NESTED},
                notes="regenerated after a sample sheet fix",
                source_doc_id="P1",
                source_doc_rev="3-abc",
            ),
            "without steps": build_plan_document(
                Plan(plan_id="pln_empty", realm="test_realm", scope=dict(SCOPE)),
                "test_realm",
                dict(SCOPE),
            ),
        }
        for name, document in cases.items():
            with self.subTest(name):
                assert_conforms(self, PLAN_DOCUMENT, document)

    def test_a_plan_builders_plan_conforms(self):
        # PlanBuilder injects paths into params and declares inputs and outputs.
        with TemporaryDirectory() as base:
            builder = PlanBuilder(
                plan_id="pln_built",
                realm="test_realm",
                scope=dict(SCOPE),
                base=Path(base),
            )
            builder.add_step_fn(demux, step_id="demux", params={"lanes": [1, 2]})
            builder.add_step_fn(qc, step_id="qc")
            document = build_plan_document(builder.to_plan(), "test_realm", dict(SCOPE))

        steps = document["plan"]["steps"]
        self.assertEqual(steps[1]["deps"], ["demux"])
        self.assertTrue(steps[1]["inputs"] and steps[1]["outputs"])
        self.assertIsInstance(steps[1]["params"]["data"], str)
        assert_conforms(self, PLAN_DOCUMENT, document)

    def test_realm_payloads_are_open(self):
        document = built_document(preview=NESTED)
        document["plan"]["steps"][0]["params"] = NESTED
        document["plan"]["steps"][0]["scope"] = NESTED
        document["scope"] = {**SCOPE, "extra": NESTED}
        assert_conforms(self, PLAN_DOCUMENT, document)


class TestClosedAndOpenParts(ConformingBuilders):
    """Contract fields and x_ extensions only; closed objects; known values."""

    def test_actor_fields_and_approvals_required_conform(self):
        document = {
            **built_document(),
            "approved_at": "2026-10-22T10:00:00Z",
            "approved_by": "alice",
            "run_requested_at": "2026-10-22T11:00:00Z",
            "run_requested_by": "bob",
            "approvals_required": ["lab", "bioinformatics"],
        }
        assert_conforms(self, PLAN_DOCUMENT, document)

    def test_an_x_extension_conforms(self):
        for value in ("OPS-1", 7, None, NESTED):
            with self.subTest(value=value):
                document = {**built_document(), "x_ops_ticket": value}
                assert_conforms(self, PLAN_DOCUMENT, document)

    def test_an_unprefixed_field_is_the_one_problem(self):
        document = {**built_document(), "ticket": "OPS-1"}
        self.assertEqual(
            problems(PLAN_DOCUMENT, document), [("ticket", UNEXPECTED_FIELD)]
        )

    def test_closed_objects_refuse_unknown_keys(self):
        record = "last_finalized_execution"
        diagnostic = {"message": "m", "error_type": None, "details": {}, "ticket": 1}
        for path, value, expected in (
            ("plan.ticket", 1, "plan.ticket"),
            ("plan.steps.0.ticket", 1, "plan.steps[0].ticket"),
            (f"{record}.ticket", 1, f"{record}.ticket"),
            (f"{record}.report.ticket", 1, f"{record}.report.ticket"),
            (
                f"{record}.report.failures.s1.ticket",
                1,
                f"{record}.report.failures.s1.ticket",
            ),
            (f"{record}.report.counts.skipped", 0, f"{record}.report.counts.skipped"),
            (
                f"{record}.report.diagnostic",
                diagnostic,
                f"{record}.report.diagnostic.ticket",
            ),
        ):
            with self.subTest(path=path):
                self.assertEqual(
                    problems(PLAN_DOCUMENT, changed(finalized(), path, value)),
                    [(expected, UNEXPECTED_FIELD)],
                )

    def test_each_vocabulary_refuses_an_unknown_value(self):
        base = finalized()
        for path, value in (
            ("status", "pending"),
            ("execution_authority", "owner"),
            ("plan.failure_policy", "Fail_Fast"),
            ("last_finalized_execution.failure_policy", "stop"),
            ("last_finalized_execution.outcome", "partial"),
            ("last_finalized_execution.termination_reason", "stopped"),
            ("last_finalized_execution.report.outcome", "partial"),
            ("last_finalized_execution.report.termination_reason", "stopped"),
            ("last_finalized_execution.report.step_outcomes.s1", "skipped"),
        ):
            with self.subTest(path=path):
                self.assertEqual(
                    problems(PLAN_DOCUMENT, changed(base, path, value)),
                    [(path, NOT_IN_VOCABULARY)],
                )

    def test_each_required_field_is_reported_missing(self):
        base = finalized()
        paths = [
            *schema_for(PLAN_DOCUMENT, 1)["required"],
            *(f"plan.{name}" for name in base["plan"]),
            *(f"plan.steps.0.{name}" for name in base["plan"]["steps"][0]),
            *(
                f"last_finalized_execution.{name}"
                for name in base["last_finalized_execution"]
            ),
        ]
        self.assertEqual(len(paths), 14 + 5 + 8 + 9)
        for path in paths:
            with self.subTest(path=path):
                # Without its marker a document is unversioned: no schema applies.
                code = UNVERSIONED if path == "schema_version" else MISSING_FIELD
                self.assertEqual(
                    problems(PLAN_DOCUMENT, changed(base, path, ABSENT)),
                    [(shown(path), code)],
                )

    def test_a_null_control_field_does_not_conform(self):
        for path, code in (
            ("run_token", WRONG_TYPE),
            ("executed_run_token", WRONG_TYPE),
            ("plan_generation", WRONG_TYPE),
            ("created_at", WRONG_TYPE),
            ("status", NOT_IN_VOCABULARY),
            ("execution_authority", NOT_IN_VOCABULARY),
            ("plan.failure_policy", NOT_IN_VOCABULARY),
            ("plan.steps.0.deps", WRONG_TYPE),
        ):
            with self.subTest(path=path):
                self.assertEqual(
                    problems(PLAN_DOCUMENT, changed(built_document(), path, None)),
                    [(shown(path), code)],
                )
        assert_conforms(self, PLAN_DOCUMENT, built_document(execution_owner=None))

    def test_the_finalizers_two_fields_come_together(self):
        base = finalized()
        self.assertEqual(
            problems(PLAN_DOCUMENT, changed(base, "last_finalized_execution", ABSENT)),
            [("last_finalized_execution", MISSING_FIELD)],
        )
        self.assertEqual(
            problems(PLAN_DOCUMENT, changed(base, "last_executed_at", ABSENT)),
            [("last_executed_at", MISSING_FIELD)],
        )


class TestMalformedPlans(ConformingBuilders):
    """A malformed plan goes into its document as given, which then fails."""

    def built(self, plan: Plan, **kwargs: Any) -> dict[str, Any]:
        """The builder's document of a malformed plan, which is not checked."""
        with self.nonconforming_builds():
            return build_plan_document(plan, "test_realm", dict(SCOPE), **kwargs)

    def test_a_non_string_step_id_fails_at_the_step(self):
        for value in (3, None, ""):
            with self.subTest(value=value):
                plan = make_plan()
                plan.steps[0].step_id = value  # type: ignore[assignment]
                document = self.built(plan)
                self.assertEqual(document["plan"]["steps"][0]["step_id"], value)
                code = WRONG_TYPE if value != "" else SCHEMA_RULE
                self.assertEqual(
                    problems(PLAN_DOCUMENT, document), [("plan.steps[0].step_id", code)]
                )

    def test_other_malformed_realm_input_fails_where_it_is(self):
        for step_field, value, kwargs, expected in (
            ("deps", ["s1", 1], {}, "plan.steps[1].deps[1]"),
            ("outputs", {"report": 5}, {}, "plan.steps[1].outputs.report"),
            ("inputs", {"reads": None}, {}, "plan.steps[1].inputs.reads"),
            ("params", ["a"], {}, "plan.steps[1].params"),
            (None, None, {"preview": ["not", "an", "object"]}, "preview"),
            (None, None, {"notes": {"text": "a note"}}, "notes"),
        ):
            with self.subTest(expected=expected):
                plan = make_plan()
                if step_field is not None:
                    setattr(plan.steps[1], step_field, value)
                document = self.built(plan, **kwargs)
                self.assertEqual(
                    problems(PLAN_DOCUMENT, document), [(expected, WRONG_TYPE)]
                )

    def test_a_scope_without_text_kind_and_id_fails(self):
        plan = make_plan()
        plan.scope = {"kind": "project", "id": 7}
        document = self.built(plan)
        self.assertEqual(
            problems(PLAN_DOCUMENT, document), [("plan.scope.id", WRONG_TYPE)]
        )


class StoredDocumentScenarios:
    """What both plan stores write conforms; the finalizer keeps what it does not own.

    Mixed into one ConformingBuilders case per backend, which names the
    backend in ``backend_type``.
    """

    backend_type: Any
    backend: Any
    plans: Any

    def setUp(self):
        super().setUp()
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        self.backend = self.backend_type(self, Path(temp_dir.name))
        self.plans = self.backend.plans

    def stored(self, plan_id: str = "pln_test_P1_v1") -> dict[str, Any]:
        """The plan document as stored, with its revision."""
        document = self.plans.fetch_plan(plan_id)
        assert document is not None
        return document

    def test_new_and_regenerated_plans_conform(self):
        self.plans.save_plan(make_plan(), "test_realm", dict(SCOPE))
        new = self.stored()
        self.plans.save_plan(make_plan(message="again"), "test_realm", dict(SCOPE))
        regenerated = self.stored()
        for name, document in (("new", new), ("regenerated", regenerated)):
            with self.subTest(name):
                self.assertTrue(document["_rev"])
                assert_conforms(self, PLAN_DOCUMENT, document)

    def test_a_plan_regenerated_over_an_unversioned_one_conforms(self):
        legacy = built_document()
        del legacy["schema_version"]
        legacy["created_at"] = "2025-01-02T03:04:05+00:00"
        legacy["approved_by"] = "alice"
        self.backend.raw_put(legacy)

        self.plans.save_plan(make_plan(message="again"), "test_realm", dict(SCOPE))

        document = self.stored()
        self.assertEqual(document["created_at"], "2025-01-02T03:04:05Z")
        self.assertNotIn("approved_by", document)
        assert_conforms(self, PLAN_DOCUMENT, document)

    def test_finalizing_a_conforming_document_conforms(self):
        for plan, ending in (
            (make_plan("pln_succeeded"), SUCCEEDED),
            (continuation_plan("pln_drained"), DRAINED_FAILURE),
            (continuation_plan("pln_rejected"), PREFLIGHT_REJECTED),
        ):
            with self.subTest(ending=ending):
                self.plans.save_plan(plan, "test_realm", dict(SCOPE), auto_run=True)
                request = finalization_for(self.stored(plan.plan_id), ending=ending)

                result = self.plans.finalize_execution(request)

                self.assertTrue(result.recorded, result.message)
                document = self.stored(plan.plan_id)
                self.assertIn("last_finalized_execution", document)
                assert_conforms(self, PLAN_DOCUMENT, document)

    def test_finalizing_keeps_a_foreign_field_without_certifying_it(self):
        self.plans.save_plan(make_plan(), "test_realm", dict(SCOPE), auto_run=True)
        document = self.stored()
        document["ticket"] = {"id": "OPS-1"}
        self.backend.conditional_put(document)
        before = self.stored()

        result = self.plans.finalize_execution(finalization_for(before))

        self.assertTrue(result.recorded, result.message)
        after = self.stored()
        self.assertEqual(after["ticket"], {"id": "OPS-1"})
        self.assertIs(type(after["schema_version"]), int)
        self.assertEqual(after["schema_version"], before["schema_version"])
        self.assertEqual(problems(PLAN_DOCUMENT, after), [("ticket", UNEXPECTED_FIELD)])


class TestSQLiteStoredDocuments(StoredDocumentScenarios, ConformingBuilders):
    """The scenarios on the SQLite plan store, on a temporary database file."""

    backend_type = SQLiteBackend


class TestCouchStoredDocuments(StoredDocumentScenarios, ConformingBuilders):
    """The scenarios on the CouchDB plan store, on an in-memory client."""

    backend_type = CouchBackend


class FailingEmitter:
    """Publishes through another emitter, failing on chosen event types."""

    def __init__(self, inner: EventEmitter, *failing: str) -> None:
        """Publish through inner, failing on the event types in failing."""
        self.inner = inner
        self.failing = set(failing)

    def emit(self, event: dict[str, Any]) -> None:
        """Publish event, or raise OSError if its type is one to fail on."""
        if event.get("type") in self.failing:
            raise OSError(f"the spool refused {event.get('type')}")
        self.inner.emit(event)


class TestReports(SpoolTestCase):
    """Reports of every ending conform, including of plans that were malformed."""

    def assert_report_conforms(self, context: Any, *, published: bool = True) -> None:
        """Assert the attempt's report conforms, as held and as published."""
        assert_conforms(self, ATTEMPT_REPORT, context.report.to_dict())
        if published:
            assert_conforms(self, ATTEMPT_REPORT, self.published_report(context))

    def test_a_rejected_plan_with_steps_that_are_not_text(self):
        plan = self.plan("a", "b")
        plan.steps[0].step_id = 3  # type: ignore[assignment]
        plan.steps[1].step_id = None  # type: ignore[assignment]

        context = self.finished(plan, lambda ctx: None)

        self.assertEqual(context.report.termination_reason.value, "preflight_rejected")
        self.assert_report_conforms(context)
        with self.nonconforming_builds():
            document = build_plan_document(plan, REALM, dict(SCOPE))
        self.assertEqual(
            problems(PLAN_DOCUMENT, document),
            [
                ("plan.steps[0].step_id", WRONG_TYPE),
                ("plan.steps[1].step_id", WRONG_TYPE),
            ],
        )

    def test_a_rejected_plan_with_an_unknown_policy(self):
        plan = self.plan()
        plan.failure_policy = "Fail_Fast"

        context = self.finished(plan, lambda ctx: None)

        self.assertEqual(context.report.failure_policy, "Fail_Fast")
        self.assert_report_conforms(context)

    def test_drained_failed_and_failed_fast_attempts(self):
        def fail_a(ctx: StepContext) -> None:
            if ctx.step_id == "a":
                raise PermanentStepError("boom", code="E1", advice="retry")

        for policy in ("continue_independent", "fail_fast"):
            with self.subTest(policy=policy):
                plan = self.plan("a", "b", policy=policy)
                plan.steps[1].deps = ["a"]

                context = self.finished(plan, fail_a)

                self.assertIsNotNone(context.report.failures.get("a"))
                self.assert_report_conforms(context)

    def test_a_cancelled_attempt(self):
        plan = self.plan("a", "b")
        plan.steps[1].deps = ["a"]
        context = self.context(plan)

        self.finished(plan, lambda ctx: context.request_cancellation(), context=context)

        self.assertEqual(context.report.termination_reason.value, "cancelled")
        self.assert_report_conforms(context)

    def test_an_orchestration_error(self):
        def broken(ctx: StepContext) -> None:
            raise OrchestrationError("the work root is unwritable")

        context = self.finished(self.plan(), broken)

        report = context.report.to_dict()
        self.assertEqual(report["termination_reason"], "orchestration_error")
        self.assertEqual(report["diagnostic"]["details"], {"running_step_id": "a"})
        self.assert_report_conforms(context)

    def test_a_report_that_could_not_be_published(self):
        self.engine.emitter = FailingEmitter(self.engine.emitter, ATTEMPT_REPORT_EVENT)

        context = self.finished(self.plan(), lambda ctx: None)

        report = context.report.to_dict()
        self.assertEqual(report["termination_reason"], "orchestration_error")
        self.assertEqual(
            report["publication_failure"]["details"],
            {"superseded_termination_reason": "completed"},
        )
        self.assert_report_conforms(context, published=False)

    def test_a_report_whose_publication_was_skipped(self):
        self.engine.emitter = FailingEmitter(self.engine.emitter, "step.started")

        context = self.finished(self.plan(), lambda ctx: None)

        report = context.report.to_dict()
        self.assertEqual(
            report["publication_failure"]["details"]["publication_skipped"], True
        )
        self.assert_report_conforms(context, published=False)


class TestSnapshots(SpoolTestCase):
    """Snapshots of every kind of attempt, and of legacy histories, conform."""

    def assert_snapshot_conforms(self) -> dict[str, Any]:
        """Assert the plan's snapshot conforms as stored; return it."""
        snapshot = stored_snapshot(self.snapshot())
        assert snapshot is not None
        assert_conforms(self, PLAN_STATUS, snapshot)
        return snapshot

    def test_a_legacy_projection(self):
        @step
        def legacy(ctx: StepContext) -> None:
            ctx.progress(50)

        with TemporaryDirectory() as work:
            for step_id in ("a", "b"):
                legacy(
                    StepContext(
                        realm=REALM,
                        scope=dict(SCOPE),
                        plan_id=SPOOL_PLAN_ID,
                        step_id=step_id,
                        step_name=step_id,
                        workdir=Path(work),
                        scope_dir=Path(work),
                        emitter=FileSpoolEmitter(self.spool),
                        run_id="run_20250101T000000000000Z_aaaaaa",
                    )
                )

        snapshot = self.assert_snapshot_conforms()
        self.assertEqual(snapshot["projection"], PROJECTION_LEGACY)
        self.assertEqual(snapshot["steps"]["a"]["state"], "step.succeeded")

    def test_a_running_attempt(self):
        snapshot = self.running(self.plan("a", "b"), lambda ctx: ctx.progress(10))

        stored = stored_snapshot(snapshot)
        assert_conforms(self, PLAN_STATUS, stored)
        self.assertEqual(snapshot["attempt"]["state"], ATTEMPT_RUNNING)

    def test_an_attempt_whose_start_record_is_missing(self):
        context = self.finished(self.plan(), lambda ctx: None)
        (self.attempt_dir(context) / record_filename(ATTEMPT_STARTED_EVENT)).unlink()

        snapshot = self.assert_snapshot_conforms()
        self.assertIsNone(snapshot["attempt"]["execution_authority"])

    def test_ended_attempts(self):
        def fail_a(ctx: StepContext) -> None:
            if ctx.step_id == "a":
                raise PermanentStepError("boom")

        def broken(ctx: StepContext) -> None:
            raise OrchestrationError("the work root is unwritable")

        for reason, body, deps in (
            ("completed", lambda ctx: None, []),
            ("completed", fail_a, ["a"]),
            ("orchestration_error", broken, []),
        ):
            with self.subTest(reason=reason, deps=deps):
                plan = self.plan("a", "b")
                plan.steps[1].deps = deps
                self.finished(plan, body)
                snapshot = self.assert_snapshot_conforms()
                self.assertEqual(snapshot["attempt"]["termination_reason"], reason)

    def test_a_cancelled_attempt(self):
        plan = self.plan("a", "b")
        context = self.context(plan)

        self.finished(plan, lambda ctx: context.request_cancellation(), context=context)

        snapshot = self.assert_snapshot_conforms()
        self.assertEqual(snapshot["attempt"]["termination_reason"], "cancelled")
        self.assertEqual(snapshot["steps"]["b"]["state"], "unreached")


class TestSnapshotParts(SpoolTestCase):
    """The snapshot's closed parts refuse what the consumer never writes."""

    def setUp(self) -> None:
        super().setUp()

        def fail(ctx: StepContext) -> None:
            raise PermanentStepError("boom")

        plan = self.plan("a", "b")
        plan.steps[1].deps = ["a"]
        self.finished(plan, fail)
        snapshot = stored_snapshot(self.snapshot())
        assert snapshot is not None
        self.base = snapshot

    def test_closed_parts(self):
        for path, value, expected in (
            ("ticket", 1, [("ticket", UNEXPECTED_FIELD)]),
            ("attempt.ticket", 1, [("attempt.ticket", UNEXPECTED_FIELD)]),
            ("steps.a.ticket", 1, [("steps.a.ticket", UNEXPECTED_FIELD)]),
            ("projection", "other", [("projection", NOT_IN_VOCABULARY)]),
            ("projection", PROJECTION_LEGACY, [("attempt", WRONG_TYPE)]),
            ("attempt", None, [("attempt", WRONG_TYPE)]),
            ("attempt.state", "done", [("attempt.state", NOT_IN_VOCABULARY)]),
            (
                "attempt.state",
                ATTEMPT_RUNNING,
                [
                    ("attempt.counts", WRONG_TYPE),
                    ("attempt.ended_at", WRONG_TYPE),
                    ("attempt.is_drained", WRONG_TYPE),
                    ("attempt.outcome", WRONG_TYPE),
                    ("attempt.termination_reason", WRONG_TYPE),
                ],
            ),
            ("steps.a.state", "failed", [("steps.a.state", NOT_IN_VOCABULARY)]),
            ("steps.a.outcome", "skipped", [("steps.a.outcome", NOT_IN_VOCABULARY)]),
            ("steps.a.error", {"error": "boom"}, [("steps.a.error", SCHEMA_RULE)]),
            ("type", "plan_document", [("type", NOT_IN_VOCABULARY)]),
            ("scope", {"kind": "project"}, [("scope.id", MISSING_FIELD)]),
        ):
            with self.subTest(path=path, value=value):
                self.assertEqual(
                    problems(PLAN_STATUS, changed(self.base, path, value)), expected
                )

    def test_open_parts(self):
        for path in (
            "steps.a.metrics",
            "steps.a.artifacts",
            "steps.a.job",
            "steps.a.progress",
            "steps.a.fingerprint",
            "steps.a.ts",
            "attempt.failure_policy",
        ):
            with self.subTest(path=path):
                assert_conforms(self, PLAN_STATUS, changed(self.base, path, NESTED))
        assert_conforms(
            self,
            PLAN_STATUS,
            changed(self.base, "scope", {"kind": 1, "id": [2], "x": 3}),
        )


if __name__ == "__main__":
    unittest.main()
