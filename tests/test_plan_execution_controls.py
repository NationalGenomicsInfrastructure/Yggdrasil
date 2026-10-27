"""The executor's control checks, through the coordinator, on both backends.

Control state is never defaulted. A plan whose execution tokens are not a
valid pair is not eligible; one whose failure policy is missing or unknown, or
whose plan or steps lack an execution field, does not describe an executable
request. Neither runs, and nothing is written: no execution ID is allocated or
reserved, no attempt directory or report appears, and the stored document is
unchanged, revision included. Other plans in the same store keep running.

A finished request whose result cannot be recorded because of the plan's
tokens, as read or as the write would leave them, stays pending until the
document changes: a regeneration drops it whatever the tokens say, a changed
authority only once they are valid, and a newer run request retries it.
"""

from __future__ import annotations

import logging
import unittest
from collections.abc import Callable
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import patch

from tests.execution_support import (
    PLAN_ID,
    CapturingEngine,
    CouchBackend,
    ExecutionTestCase,
    SQLiteBackend,
    chain_plan,
    directory_tree,
    run_bounded,
)
from yggdrasil.contract.controls import PLAN_DEFINITION_FIELDS, STEP_DEFINITION_FIELDS
from yggdrasil.daemon.plan_execution import (
    DAEMON_CLAIM,
    ExecutionResult,
    ExecutionStatus,
    PlanExecutionCoordinator,
)
from yggdrasil.flow.events.emitter import FileSpoolEmitter
from yggdrasil.storage.plan_eligibility import is_plan_eligible
from yggdrasil.storage.plan_updates import FinalizationStatus

COORDINATOR_LOGGER = "yggdrasil.daemon.plan_execution.PlanExecutionCoordinator"

# Stands for a field removed from the stored document.
ABSENT = object()


def _put(container: dict[str, Any], key: str, value: Any) -> None:
    """Set key in container to value, or remove it for ABSENT."""
    if value is ABSENT:
        del container[key]
    else:
        container[key] = value


class ControlTestCase(ExecutionTestCase):
    """A coordinator whose engine publishes to a real file spool.

    An execution ID that was reserved, an attempt that started or a report
    that was published leaves a file behind, so comparing the sandbox before
    and after shows that nothing of the kind happened. Subclasses choose the
    backend with ``backend_type``.

    Attributes:
        sandbox: Holds the engine's work root and its event spool.
    """

    def setUp(self) -> None:
        super().setUp()
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        self.sandbox = Path(temp_dir.name)
        self.engine = CapturingEngine(
            work_root=self.sandbox / "work",
            emitter=FileSpoolEmitter(self.sandbox / "spool"),
            journal=self.journal,
        )
        self.coordinator = PlanExecutionCoordinator(
            engine=self.engine, plan_store=self.store, sleep=self.record_delay
        )

    # ----- the store, as stored -----

    def raw(self, plan_id: str = PLAN_ID) -> dict[str, Any] | None:
        """The plan document exactly as the backend holds it, ``_rev`` included."""
        if isinstance(self.backend, SQLiteBackend):
            return self.backend.internal.get_document("plans", plan_id)
        return self.backend.server.raw(plan_id)

    def plan_seq(self) -> int | None:
        """The SQLite store's plan-change sequence; None on CouchDB."""
        if isinstance(self.backend, SQLiteBackend):
            return self.backend.internal.current_plan_seq()
        return None

    def edited(self, plan_id: str, edit: Callable[[dict[str, Any]], None]) -> None:
        """Save a valid plan as plan_id, then store it changed by edit."""
        self.save(chain_plan(plan_id=plan_id))
        doc = self.stored(plan_id)
        edit(doc)
        self.backend.conditional_put(doc)

    # ----- assertions -----

    def assert_refused(
        self, plan_id: str, status: ExecutionStatus, *mentions: str
    ) -> ExecutionResult:
        """Execute plan_id; assert it was refused with status and nothing changed.

        Args:
            plan_id: The plan to execute.
            status: The refusal expected.
            *mentions: Text the refusal's message must contain, each once.

        Returns:
            ExecutionResult: The refusal.
        """
        before, seq = self.raw(plan_id), self.plan_seq()
        files = directory_tree(self.sandbox)
        attempts = len(self.engine.contexts)
        finalizations = self.store.calls["finalize"]

        with (
            patch.object(
                self.engine,
                "reserve_execution_id",
                wraps=self.engine.reserve_execution_id,
            ) as reserve,
            self.assertLogs(COORDINATOR_LOGGER, level=logging.INFO) as logs,
        ):
            result = self.execute(plan_id)

        self.assertEqual(result.status, status, result.message)
        for text in mentions:
            self.assertEqual(result.message.count(text), 1, result.message)
        self.assertTrue(any(result.message in line for line in logs.output))
        reserve.assert_not_called()
        self.assertEqual(len(self.engine.contexts), attempts)
        self.assertEqual(self.steps.calls, [])
        self.assertEqual(self.store.calls["finalize"], finalizations)
        self.assertEqual(directory_tree(self.sandbox), files)
        self.assertEqual(self.raw(plan_id), before)
        self.assertEqual(self.plan_seq(), seq)
        self.assertFalse(self.coordinator.is_in_flight(plan_id))
        self.assertIsNone(self.coordinator.pending_finalization(plan_id))
        return result

    def assert_a_valid_plan_still_runs(self) -> None:
        """Run a valid plan in the same store, and assert it is recorded."""
        valid = chain_plan(plan_id="pln_exec_valid")
        self.save(valid)
        result = self.execute(valid.plan_id)
        self.assertEqual(result.status, ExecutionStatus.FINALIZED, result.message)
        self.assert_recorded(result)

    # ----- results that cannot be recorded -----

    def finish_after(
        self, edit: Callable[[], None], plan_id: str = PLAN_ID
    ) -> ExecutionResult:
        """Execute the plan, applying edit while its attempt is held at a gate.

        Args:
            edit: Changes the stored plan; runs after the attempt started and
                before it finishes.
            plan_id: The plan to execute.

        Returns:
            ExecutionResult: How the request was resolved.
        """
        gate = self.steps.block("a")

        async def scenario() -> ExecutionResult:
            running = self.coordinator.submit(plan_id, DAEMON_CLAIM)
            await gate.reached()
            edit()
            gate.release()
            return await running

        try:
            return run_bounded(scenario())
        finally:
            del self.steps.behaviors["a"]

    def assert_pending(self, result: ExecutionResult, *, run_token: int) -> None:
        """Assert result is a finished request kept pending at run_token."""
        self.assertEqual(
            result.status, ExecutionStatus.FINALIZATION_PENDING, result.message
        )
        pending = self.coordinator.pending_finalization(PLAN_ID)
        assert pending is not None, "no pending finalization"
        self.assertEqual(pending.run_token, run_token)
        self.assertFalse(self.coordinator.is_in_flight(PLAN_ID))


class ControlScenarios:
    """Invalid control state, against one plan-store backend.

    Mixed into one ControlTestCase per backend.
    """

    # ----- tokens -----

    def test_a_plan_without_a_valid_token_pair_is_not_executed(self):
        cases: dict[str, tuple[dict[str, Any], str]] = {
            "no run_token": ({"run_token": ABSENT}, "run_token is missing"),
            "a string run_token": (
                {"run_token": "5"},
                "run_token '5' is a str, not a JSON integer",
            ),
            "a float run_token": (
                {"run_token": 5.0},
                "run_token 5.0 is a float, not a JSON integer",
            ),
            "a boolean run_token": (
                {"run_token": True},
                "run_token True is a bool, not a JSON integer",
            ),
            "executed above requested": (
                {"run_token": 4, "executed_run_token": 5},
                "executed_run_token 5 is above run_token 4",
            ),
        }
        for index, (case, (fields, problem)) in enumerate(cases.items()):
            plan_id = f"{PLAN_ID}_tokens_{index}"
            with self.subTest(case=case):

                def edit(doc: dict[str, Any], fields: dict[str, Any] = fields) -> None:
                    for key, value in fields.items():
                        _put(doc, key, value)

                self.edited(plan_id, edit)

                result = self.assert_refused(
                    plan_id, ExecutionStatus.NOT_ELIGIBLE, problem
                )
                self.assertIn("invalid tokens", result.message)
        self.assert_a_valid_plan_still_runs()

    # ----- failure policy -----

    def test_a_plan_without_a_known_failure_policy_is_not_executed(self):
        cases = {
            "no failure_policy": (ABSENT, "plan.failure_policy is missing"),
            "a null failure_policy": (None, "plan.failure_policy is null"),
            "an unknown failure_policy": (
                "carry_on_regardless",
                "plan.failure_policy 'carry_on_regardless' is not one of "
                "['continue_independent', 'fail_fast']",
            ),
            "a misspelled failure_policy": ("Fail_Fast", "'Fail_Fast' is not one of"),
        }
        for index, (case, (value, problem)) in enumerate(cases.items()):
            plan_id = f"{PLAN_ID}_policy_{index}"
            with self.subTest(case=case):

                def edit(doc: dict[str, Any], value: Any = value) -> None:
                    _put(doc["plan"], "failure_policy", value)

                self.edited(plan_id, edit)

                # Refused at admission, not by preflight: no report is
                # published, and the request stays eligible.
                self.assert_refused(plan_id, ExecutionStatus.INVALID_DOCUMENT, problem)
                self.assertTrue(is_plan_eligible(self.stored(plan_id)))
        self.assert_a_valid_plan_still_runs()

    # ----- execution fields -----

    def test_a_plan_missing_an_execution_field_is_not_executed(self):
        cases: list[tuple[str, Callable[[dict[str, Any]], dict[str, Any]]]] = [
            *(
                (f"plan.{key}", lambda doc: doc["plan"])
                for key in PLAN_DEFINITION_FIELDS
            ),
            *(
                (f"plan.steps[1].{key}", lambda doc: doc["plan"]["steps"][1])
                for key in STEP_DEFINITION_FIELDS
            ),
        ]
        index = 0
        for path, container in cases:
            key = path.rsplit(".", 1)[1]
            for value, state in ((ABSENT, "missing"), (None, "null")):
                index += 1
                plan_id = f"{PLAN_ID}_field_{index}"
                with self.subTest(path=path, state=state):

                    def edit(
                        doc: dict[str, Any],
                        container: Callable[[dict[str, Any]], dict[str, Any]] = (
                            container
                        ),
                        key: str = key,
                        value: Any = value,
                    ) -> None:
                        _put(container(doc), key, value)

                    self.edited(plan_id, edit)

                    self.assert_refused(
                        plan_id, ExecutionStatus.INVALID_DOCUMENT, f"{path} is {state}"
                    )
        self.assert_a_valid_plan_still_runs()

    def test_an_approved_plan_without_steps_is_not_finalized_as_succeeded(self):
        self.edited(PLAN_ID, lambda doc: doc["plan"].pop("steps"))

        self.assert_refused(
            PLAN_ID, ExecutionStatus.INVALID_DOCUMENT, "plan.steps is missing"
        )

        self.assert_unconsumed()
        self.assertTrue(is_plan_eligible(self.stored()))

    def test_every_problem_is_named_once_in_one_refusal(self):
        def edit(doc: dict[str, Any]) -> None:
            del doc["plan"]["failure_policy"]
            del doc["plan"]["steps"][0]["deps"]
            doc["plan"]["steps"][1]["outputs"] = None

        self.edited(PLAN_ID, edit)

        self.assert_refused(
            PLAN_ID,
            ExecutionStatus.INVALID_DOCUMENT,
            "plan.failure_policy is missing",
            "plan.steps[0].deps is missing",
            "plan.steps[1].outputs is null",
        )

    # ----- results that cannot be recorded -----

    def test_a_result_that_would_leave_executed_above_run_stays_pending(self):
        self.save()
        self.update(run_token=5)

        result = self.finish_after(lambda: self.update(run_token=4))

        # Refused at once, not retried: the document has to change first.
        self.assert_pending(result, run_token=5)
        self.assertEqual(
            result.finalization.status, FinalizationStatus.INVALID_CONTROL_STATE
        )
        self.assertIn("run_token 4 / executed_run_token 5", result.message)
        self.assertEqual(self.store.calls["finalize"], 1)
        self.assertEqual(self.delays, [])
        lowered = self.raw()
        self.assertEqual((lowered["run_token"], lowered["executed_run_token"]), (4, -1))
        self.assertNotIn("last_finalized_execution", lowered)

        # Another request with the tokens unchanged: still pending, judged by
        # reading the plan alone; nothing runs and nothing is written.
        again = self.execute()

        self.assert_pending(again, run_token=5)
        self.assertIn("run_token 4 / executed_run_token 5", again.message)
        self.assertEqual(len(self.engine.contexts), 1)
        self.assertEqual(self.store.calls["finalize"], 1)
        self.assertEqual(self.raw(), lowered)

        # A newer request records the pending result first, then runs.
        self.update(run_token=6)
        del self.journal[:]

        newer = self.execute()

        self.assertEqual(
            self.journal, ["read", "finalize:5", "read", "attempt:6", "finalize:6"]
        )
        recorded, served = self.store.finalizations[-2:]
        self.assertEqual(recorded.status, FinalizationStatus.COMMITTED)
        self.assertIn(result.report.execution_id, recorded.message)
        self.assertEqual(served.status, FinalizationStatus.COMMITTED)
        self.assertIsNone(self.coordinator.pending_finalization(PLAN_ID))
        self.assertEqual(newer.status, ExecutionStatus.FINALIZED, newer.message)
        self.assert_recorded(newer, run_token=6)

    def test_a_retry_refused_over_the_tokens_keeps_the_result_pending(self):
        self.save()
        self.update(run_token=5)
        self.assert_pending(
            self.finish_after(lambda: self.update(run_token=4)), run_token=5
        )
        self.update(run_token=6)

        def lower_before_finalizing(request):
            # Another writer lowers the run token again just before the retry.
            self.update(run_token=4)
            return None

        self.store.finalize_actions.append((None, lower_before_finalizing))

        result = self.execute()

        self.assert_pending(result, run_token=5)
        self.assertIn("recording it was refused", result.message)
        self.assertIn("run_token 4 / executed_run_token 5", result.message)
        self.assertEqual(self.store.calls["finalize"], 2)
        self.assertEqual(len(self.engine.contexts), 1)
        lowered = self.raw()
        self.assertEqual((lowered["run_token"], lowered["executed_run_token"]), (4, -1))

    def test_a_result_over_unreadable_tokens_stays_pending(self):
        self.save()

        result = self.finish_after(lambda: self.update(executed_run_token="soon"))

        self.assert_pending(result, run_token=0)
        self.assertIn("executed_run_token 'soon' is a str", result.message)
        broken = self.raw()

        again = self.execute()

        self.assert_pending(again, run_token=0)
        self.assertEqual(self.raw(), broken)
        self.assertEqual(self.store.calls["finalize"], 1)
        self.assertEqual(len(self.engine.contexts), 1)

    def test_a_regeneration_drops_the_pending_result_whatever_the_tokens_say(self):
        self.save()
        self.assert_pending(
            self.finish_after(lambda: self.update(executed_run_token="soon")),
            run_token=0,
        )
        self.save()  # regenerated: a new generation
        self.update(executed_run_token="soon")
        regenerated = self.raw()

        with self.assertLogs(COORDINATOR_LOGGER, level=logging.WARNING) as logs:
            result = self.execute()

        self.assertIsNone(self.coordinator.pending_finalization(PLAN_ID))
        self.assertTrue(
            any("settled without writing" in line for line in logs.output),
            logs.output,
        )
        # The regenerated plan's own tokens are still invalid, so it waits.
        self.assertEqual(result.status, ExecutionStatus.NOT_ELIGIBLE)
        self.assertEqual(self.raw(), regenerated)
        self.assertEqual(len(self.engine.contexts), 1)

    def test_a_changed_authority_drops_the_pending_result_once_tokens_are_valid(self):
        self.save()
        self.assert_pending(
            self.finish_after(lambda: self.update(executed_run_token="soon")),
            run_token=0,
        )
        self.update(execution_authority="run_once", execution_owner="run_once:other")

        still = self.execute()

        self.assert_pending(still, run_token=0)

        self.update(executed_run_token=-1)
        repaired = self.raw()

        result = self.execute()

        self.assertIsNone(self.coordinator.pending_finalization(PLAN_ID))
        self.assertEqual(result.status, ExecutionStatus.NOT_AUTHORIZED)
        self.assertEqual(self.raw(), repaired)
        self.assertEqual(self.store.calls["finalize"], 1)


class TestControlsSQLite(ControlScenarios, ControlTestCase):
    """The control checks against the SQLite plan store."""


class TestControlsCouch(ControlScenarios, ControlTestCase):
    """The control checks against the CouchDB plan store."""

    backend_type = CouchBackend


if __name__ == "__main__":
    unittest.main()
