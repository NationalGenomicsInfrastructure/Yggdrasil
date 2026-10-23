"""The conformance checks that other tests rely on.

A ConformingBuilders test fails when a builder it calls produces a document
that does not conform, wherever the builder is called from, and only then.
Each case runs a one-test ConformingBuilders case and inspects its result.
"""

from __future__ import annotations

import unittest
from collections.abc import Callable
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import Mock

import yggdrasil.ops.consumer
import yggdrasil.storage.couchdb.plan_store
import yggdrasil.storage.sqlite
from tests.contract.support import ConformingBuilders
from tests.plan_store_support import SCOPE, make_plan
from yggdrasil.core.engine import Engine
from yggdrasil.flow.events.attempt_records import (
    ATTEMPT_REPORT_EVENT,
    ATTEMPT_STARTED_EVENT,
)
from yggdrasil.flow.model import Plan
from yggdrasil.ops import snapshot
from yggdrasil.ops.snapshot import (
    AttemptEvents,
    SpooledEvent,
    StepRun,
    project_attempt,
    project_legacy,
)
from yggdrasil.storage import plan_documents
from yggdrasil.storage.plan_documents import build_plan_document

EXECUTION_ID = "20261022T100000000000Z_0000"


def malformed_plan() -> Plan:
    """A plan whose first step ID is not text."""
    plan = make_plan()
    plan.steps[0].step_id = 3  # type: ignore[assignment]
    return plan


def snapshot_of(projection: str, scope: dict[str, Any]) -> Callable[[], Any]:
    """Build a snapshot through the ops consumer's name for the builder."""

    def build() -> Any:
        return yggdrasil.ops.consumer.snapshot_document(
            realm="test_realm",
            plan_id="plan_1",
            scope=scope,
            projection=projection,
            attempt=None,
            steps={},
            updated_at="2026-10-22T10:00:00Z",
        )

    return build


class TestConformingBuilders(unittest.TestCase):
    """What makes a ConformingBuilders test fail, and what does not."""

    def run_case(
        self, body: Callable[[ConformingBuilders], Any], *, check: bool = True
    ) -> unittest.TestResult:
        """Run body as the one test of a ConformingBuilders case.

        Args:
            body: The test's body, given the running case.
            check: The case's check_builder_outputs.

        Returns:
            unittest.TestResult: The outcome.
        """

        class Case(ConformingBuilders):
            check_builder_outputs = check

            def test(self) -> None:
                body(self)

        result = unittest.TestResult()
        Case("test").run(result)
        self.assertEqual(result.errors, [])
        return result

    def assert_fails_naming(self, result: unittest.TestResult, *texts: str) -> None:
        """Assert the case failed once, with every text in its failure."""
        self.assertEqual(len(result.failures), 1, result.failures)
        ((_, report),) = result.failures
        for text in texts:
            self.assertIn(text, report)

    def test_a_nonconforming_plan_document_fails_the_test(self):
        for name, build in (
            ("the SQLite store", lambda: yggdrasil.storage.sqlite.build_plan_document),
            (
                "the CouchDB store",
                lambda: yggdrasil.storage.couchdb.plan_store.build_plan_document,
            ),
            ("the test module", lambda: build_plan_document),
        ):
            with self.subTest(name):
                result = self.run_case(
                    lambda case: build()(malformed_plan(), "test_realm", dict(SCOPE))
                )
                self.assert_fails_naming(
                    result, "build_plan_document built", "plan.steps[0].step_id"
                )

    def test_the_builder_still_returns_what_it_built(self):
        def body(case: ConformingBuilders) -> None:
            document = build_plan_document(malformed_plan(), "test_realm", dict(SCOPE))
            case.assertEqual(document["plan"]["steps"][0]["step_id"], 3)

        self.assert_fails_naming(self.run_case(body), "plan.steps[0].step_id")

    def test_a_conforming_document_passes(self):
        result = self.run_case(
            lambda case: build_plan_document(make_plan(), "test_realm", dict(SCOPE))
        )
        self.assertTrue(result.wasSuccessful())

    def test_a_stored_snapshot_is_checked(self):
        result = self.run_case(lambda case: snapshot_of("other", dict(SCOPE))())
        self.assert_fails_naming(result, "snapshot_document built", "projection")

    def test_a_snapshot_that_is_never_stored_is_not_checked(self):
        for scope in ({}, {"kind": "project"}):
            with self.subTest(scope=scope):
                result = self.run_case(lambda case: snapshot_of("other", scope)())
                self.assertTrue(result.wasSuccessful())

    def test_a_projection_is_checked_as_a_stored_snapshot(self):
        partial = AttemptEvents(
            execution_id=EXECUTION_ID,
            report={
                "type": ATTEMPT_REPORT_EVENT,
                "execution_id": EXECUTION_ID,
                "report": {
                    "step_ids": ["a"],
                    "step_outcomes": {"a": "failed"},
                    "failures": {"a": {"step_id": "a", "error": "boom"}},
                },
            },
        )
        result = self.run_case(lambda case: project_attempt(partial))
        self.assert_fails_naming(result, "project_attempt built", "steps.a.error")

        legacy = {
            "a": StepRun("run_1", [SpooledEvent("1.json", {"type": "step.started"})])
        }
        self.assertTrue(
            self.run_case(lambda case: project_legacy(legacy)).wasSuccessful()
        )

    def test_a_published_report_is_checked(self):
        def body(case: ConformingBuilders) -> None:
            emitter = Mock()
            with TemporaryDirectory() as root:
                engine = Engine(work_root=Path(root), emitter=emitter)
                engine._emit_engine_event(
                    "a report", {"type": ATTEMPT_REPORT_EVENT, "report": {}}
                )
                engine._emit_engine_event(
                    "a start record", {"type": ATTEMPT_STARTED_EVENT}
                )
            case.assertEqual(emitter.emit.call_count, 2)

        self.assert_fails_naming(
            self.run_case(body), "the engine built", "execution_id"
        )

    def test_a_block_can_build_nonconforming_documents(self):
        def body(case: ConformingBuilders) -> None:
            with case.nonconforming_builds():
                build_plan_document(malformed_plan(), "test_realm", dict(SCOPE))

        self.assertTrue(self.run_case(body).wasSuccessful())

        def after_the_block(case: ConformingBuilders) -> None:
            body(case)
            build_plan_document(malformed_plan(), "test_realm", dict(SCOPE))

        self.assert_fails_naming(
            self.run_case(after_the_block), "plan.steps[0].step_id"
        )

    def test_a_class_can_opt_out(self):
        def body(case: ConformingBuilders) -> None:
            case.assertIs(build_plan_document, plan_documents.build_plan_document)
            build_plan_document(malformed_plan(), "test_realm", dict(SCOPE))

        self.assertTrue(self.run_case(body, check=False).wasSuccessful())

    def test_every_builder_is_restored_after_the_test(self):
        self.run_case(lambda case: None)

        self.assertIs(
            yggdrasil.storage.sqlite.build_plan_document,
            plan_documents.build_plan_document,
        )
        self.assertIs(
            yggdrasil.storage.couchdb.plan_store.build_plan_document,
            plan_documents.build_plan_document,
        )
        self.assertIs(
            yggdrasil.ops.consumer.snapshot_document, snapshot.snapshot_document
        )
        self.assertIs(build_plan_document, plan_documents.build_plan_document)
        self.assertIs(project_attempt, snapshot.project_attempt)
        self.assertEqual(
            Engine._emit_engine_event.__qualname__, "Engine._emit_engine_event"
        )


if __name__ == "__main__":
    unittest.main()
