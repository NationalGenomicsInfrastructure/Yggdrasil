"""What Yggdrasil's producers store when a realm hands them unexpected values.

The published schemas describe what producers write, and leave a field open
only where a value from a realm reaches the stored document unchanged. Each
test here sends an unexpected value (another type, an empty string, a nested
structure) through the real producers: the engine and the ``@step`` wrapper
publishing to a file spool, and the ops consumer building the snapshot from
it. It then pins what is recorded. The schemas follow these results.

Values a step publishes are observations: the snapshot records them as the
step published them. A plan is not an observation: what a realm puts in a
plan goes into the plan document as given, and a malformed plan's document
fails validation.
"""

from __future__ import annotations

import unittest
from typing import Any

from tests.contract.support import SpoolTestCase
from tests.execution_support import SCOPE
from yggdrasil.contract.timestamps import is_canonical
from yggdrasil.flow.errors import PermanentStepError
from yggdrasil.flow.model import StepResult
from yggdrasil.flow.step import StepContext

# Unexpected values a realm may publish where a producer expects something else.
UNEXPECTED: tuple[Any, ...] = (7, [1, "x"], {"nested": {"deep": [None]}}, "")


class TestSnapshotStepValues(SpoolTestCase):
    """A step entry records what the step's events carry, unchanged."""

    def test_identity_values_are_kept_unless_empty(self):
        # ts, fingerprint and job come from the latest event that carries a
        # non-empty value: an empty string leaves the earlier one in place.
        for value in UNEXPECTED:
            with self.subTest(value=value):
                snapshot = self.running(
                    self.plan(),
                    lambda ctx: ctx.emit(
                        "step.echo", ts=value, fingerprint=value, job=value
                    ),
                )
                entry = snapshot["steps"]["a"]
                self.assertEqual(entry["state"], "step.echo")
                if value == "":
                    self.assertTrue(is_canonical(entry["ts"]), entry["ts"])
                    self.assertIsInstance(entry["fingerprint"], str)
                    self.assertNotEqual(entry["fingerprint"], "")
                    self.assertIsNone(entry["job"])
                else:
                    self.assertEqual(
                        (entry["ts"], entry["fingerprint"], entry["job"]),
                        (value, value, value),
                    )

    def test_result_values_are_kept_as_published(self):
        # progress, artifacts and metrics come from the effective event alone.
        for value in UNEXPECTED:
            with self.subTest(value=value):
                snapshot = self.running(
                    self.plan(),
                    lambda ctx: ctx.emit(
                        "step.echo", progress=value, artifacts=value, metrics=value
                    ),
                )
                entry = snapshot["steps"]["a"]
                self.assertEqual(
                    (entry["progress"], entry["artifacts"], entry["metrics"]),
                    (value, value, value),
                )

    def test_a_steps_metrics_result_is_kept_as_returned(self):
        for value in UNEXPECTED:
            with self.subTest(value=value):
                self.finished(self.plan(), lambda ctx: StepResult(metrics=value))
                entry = self.snapshot()["steps"]["a"]
                self.assertEqual(entry["state"], "step.succeeded")
                self.assertEqual(entry["metrics"], value)
                self.assertEqual(entry["artifacts"], [])

    def test_the_step_name_is_always_text(self):
        for value, shown in ((7, "7"), ([1, "x"], "[1, 'x']"), ("", "a")):
            with self.subTest(value=value):
                snapshot = self.running(
                    self.plan(), lambda ctx: ctx.emit("step.echo", step_name=value)
                )
                self.assertEqual(snapshot["steps"]["a"]["step_name"], shown)

    def test_the_run_id_is_always_the_engines(self):
        for value in (*UNEXPECTED, None):
            with self.subTest(value=value):
                snapshot = self.running(
                    self.plan(), lambda ctx: ctx.emit("step.echo", run_id=value)
                )
                run_id = snapshot["steps"]["a"]["run_id"]
                self.assertIsInstance(run_id, str)
                self.assertTrue(run_id.startswith("run_"), run_id)

    def test_only_step_events_set_a_state(self):
        # A payload can override the type, but only a step.* type is a state.
        for value in UNEXPECTED:
            with self.subTest(value=value):
                snapshot = self.running(
                    self.plan(), lambda ctx: ctx.emit("step.echo", type=value)
                )
                self.assertEqual(snapshot["steps"]["a"]["state"], "step.started")


class TestSnapshotStepErrors(SpoolTestCase):
    """A failed step's error has one of two fixed key sets; values are kept."""

    def test_a_live_failure_keeps_the_events_values(self):
        for value in UNEXPECTED:
            with self.subTest(value=value):
                snapshot = self.running(
                    self.plan(),
                    lambda ctx: ctx.emit(
                        "step.failed", error=value, kind=value, code=value, advice=value
                    ),
                )
                entry = snapshot["steps"]["a"]
                self.assertEqual(entry["state"], "step.failed")
                self.assertEqual(
                    entry["error"],
                    {"error": value, "kind": value, "code": value, "advice": value},
                )

    def test_a_live_failure_without_details_has_every_key(self):
        snapshot = self.running(self.plan(), lambda ctx: ctx.emit("step.failed"))
        self.assertEqual(
            snapshot["steps"]["a"]["error"],
            {"error": None, "kind": None, "code": None, "advice": None},
        )

    def test_a_reported_failure_keeps_the_realms_code_and_advice(self):
        # StepError's code and advice reach the report, the published report
        # and the snapshot as the realm raised them.
        for value in UNEXPECTED:
            with self.subTest(value=value):

                def fail(ctx: StepContext) -> None:
                    raise PermanentStepError("boom", code=value, advice=value)  # type: ignore[arg-type]

                context = self.finished(self.plan(), fail)
                record = {
                    "step_id": "a",
                    "error": "boom",
                    "kind": "permanent",
                    "code": value,
                    "advice": value,
                    "error_type": "PermanentStepError",
                }
                self.assertEqual(context.report.to_dict()["failures"]["a"], record)
                self.assertEqual(
                    self.published_report(context)["failures"]["a"], record
                )
                self.assertEqual(self.snapshot()["steps"]["a"]["error"], record)

    def test_any_other_exception_is_reported_as_text(self):
        def fail(ctx: StepContext) -> None:
            raise KeyError(("not", "text"))

        context = self.finished(self.plan(), fail)

        self.assertEqual(
            self.snapshot()["steps"]["a"]["error"],
            {
                "step_id": "a",
                "error": "('not', 'text')",
                "kind": "permanent",
                "code": None,
                "advice": None,
                "error_type": "KeyError",
            },
        )
        self.assertEqual(
            self.published_report(context)["failures"]["a"]["error"],
            "('not', 'text')",
        )


class TestSnapshotScope(SpoolTestCase):
    """The snapshot's scope is the plan's, or a step event's when it has none."""

    def test_the_plans_scope_wins_over_a_step_events(self):
        snapshot = self.running(
            self.plan(),
            lambda ctx: ctx.emit("step.echo", scope={"kind": "other", "id": 1}),
        )
        self.assertEqual(snapshot["scope"], SCOPE)

    def test_a_step_event_supplies_the_scope_of_a_plan_without_one(self):
        scope = {"kind": "project", "id": 7, "extra": ["x", {"y": None}]}
        snapshot = self.running(
            self.plan(scope={}),
            lambda ctx: ctx.emit("step.echo", scope=scope),
        )
        self.assertEqual(snapshot["scope"], scope)


class TestReportValues(SpoolTestCase):
    """The report records the plan it was given, malformed or not."""

    def test_step_ids_are_kept_as_the_plan_gave_them(self):
        plan = self.plan("a", "b")
        plan.steps[0].step_id = 3  # type: ignore[assignment]
        plan.steps[1].step_id = None  # type: ignore[assignment]

        context = self.finished(plan, lambda ctx: None)

        report = self.published_report(context)
        self.assertEqual(report["termination_reason"], "preflight_rejected")
        self.assertEqual(report["step_ids"], [3, None])
        self.assertEqual(report["unreached_step_ids"], [3, None])
        self.assertEqual(context.report.to_dict()["step_ids"], [3, None])
        # The snapshot lists only steps it can file: none here.
        self.assertEqual(self.snapshot()["steps"], {})

    def test_the_failure_policy_is_kept_as_recorded(self):
        for value in (7, "Fail_Fast", ""):
            with self.subTest(value=value):
                plan = self.plan()
                plan.failure_policy = value  # type: ignore[assignment]

                context = self.finished(plan, lambda ctx: None)

                report = self.published_report(context)
                self.assertEqual(report["termination_reason"], "preflight_rejected")
                self.assertEqual(report["failure_policy"], value)
                self.assertEqual(self.snapshot()["attempt"]["failure_policy"], value)

    def test_an_attempt_without_a_captured_request_records_none(self):
        context = self.finished(self.plan(), lambda ctx: None)

        report = self.published_report(context)
        self.assertEqual((report["plan_generation"], report["run_token"]), (None, None))
        attempt = self.snapshot()["attempt"]
        self.assertEqual(
            (attempt["plan_generation"], attempt["run_token"]), (None, None)
        )


if __name__ == "__main__":
    unittest.main()
