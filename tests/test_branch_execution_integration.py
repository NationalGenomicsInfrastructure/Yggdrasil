"""Independent branch execution end to end, through the real callers.

The test realm's branching recipes stand in for the companion demux realm's
plan: shared validation, then a metadata update and one branch per lane, each
branch a chain. In ``branch_failure`` the metadata update is independent of
the branches; in ``branch_failure_metadata_required`` it is a declared
prerequisite of each. Both fail the metadata update and lane 2's processing.

A scenario document goes the whole way. The realm's own WatchSpec turns it
into an event through WatcherManager's routing, the real ``TestRealmHandler``
plans it, and ``YggdrasilCore`` persists the plan. The plan is then executed
through the daemon's plan-event handler or through run-once, by the engine,
coordinator, event spool and plan store the core wires itself, running the
test realm's real steps. Nothing on the execution path is scripted. Snapshots
come from one ops-consumer cycle, as the daemon's ops service runs them and
run-once runs one on exit, and are read back from the backend's snapshot
store.

Every scenario runs on both internal-storage backends: SQLite as a dev daemon
builds it, and CouchDB through the production ``PlanDBManager`` and
``OpsWriter`` on an in-memory client that enforces CouchDB's revision rules.
The CouchDB change feed is the one part not modelled. No scenario needs it:
run-once admits the plans it creates through its recovery pass, and the
daemon receives its plan events as PlanWatcher emits them.
"""

from __future__ import annotations

import json
import logging
import unittest
from pathlib import Path
from typing import Any

from tests.integration_support import (
    CORE_LOGGER,
    PLAN_ID,
    REALM,
    SCOPE,
    CoreTestCase,
    CouchStorage,
    SQLiteStorage,
    scenario,
)
from yggdrasil.core.execution_ids import execution_order_key
from yggdrasil.daemon.plan_execution import ExecutionResult, ExecutionStatus
from yggdrasil.flow.outcomes import (
    AttemptReport,
    ExecutionOutcome,
    StepOutcome,
    TerminationReason,
)
from yggdrasil.storage.plan_eligibility import is_plan_eligible

SUCCEEDED = StepOutcome.SUCCEEDED
REUSED = StepOutcome.REUSED
FAILED = StepOutcome.FAILED
BLOCKED = StepOutcome.BLOCKED

LANE_STEPS = [
    f"lane_{lane}__{step}"
    for lane in (1, 2)
    for step in ("prepare", "process", "upload")
]

# What each recipe's attempt establishes under continue_independent.
BRANCH_FAILURE_OUTCOMES = {
    "validate_shared": SUCCEEDED,
    "update_metadata": FAILED,
    "lane_1__prepare": SUCCEEDED,
    "lane_1__process": SUCCEEDED,
    "lane_1__upload": SUCCEEDED,
    "lane_2__prepare": SUCCEEDED,
    "lane_2__process": FAILED,
    "lane_2__upload": BLOCKED,
}
METADATA_REQUIRED_OUTCOMES = {
    "validate_shared": SUCCEEDED,
    "update_metadata": FAILED,
    **dict.fromkeys(LANE_STEPS, BLOCKED),
}

# The state a snapshot shows for each outcome: the event type recording it.
STATE_OF = {
    SUCCEEDED: "step.succeeded",
    REUSED: "step.skipped",
    FAILED: "step.failed",
    BLOCKED: "step.blocked",
}


class BranchExecutionTestCase(CoreTestCase):
    """A core test case with the spool readers and assertions of the branches.

    Subclasses choose the internal-storage backend with ``storage_type``.
    """

    def attempt_dir(self, execution_id: str) -> Path:
        """One attempt's directory in the spool."""
        return self.spool / REALM / PLAN_ID / "attempts" / execution_id

    def spooled(self, execution_id: str | None = None) -> list[tuple[Path, dict]]:
        """Return spooled events, each with its path in the spool.

        Args:
            execution_id: Only this attempt's events, from its own directory;
                all of the plan's when None.

        Returns:
            list[tuple[Path, dict]]: Each event, with its path relative to the
            attempt's directory (or to the plan's, when all are returned), in
            path order.
        """
        root = (
            self.spool / REALM / PLAN_ID
            if execution_id is None
            else self.attempt_dir(execution_id)
        )
        events = [
            (path.relative_to(root), json.loads(path.read_text()))
            for path in sorted(root.rglob("*.json"))
        ]
        return [
            (path, event)
            for path, event in events
            if execution_id is None or event.get("execution_id") == execution_id
        ]

    def events_of_type(self, event_type: str, report: AttemptReport) -> list[Path]:
        """Return where one attempt's events of one type are spooled."""
        return [
            path
            for path, event in self.spooled(report.execution_id)
            if event["type"] == event_type
        ]

    def steps_with(self, event_type: str, report: AttemptReport) -> list[str]:
        """Return the steps whose streams hold one attempt's events of a type."""
        return sorted(
            str(path.parent.relative_to("steps"))
            for path in self.events_of_type(event_type, report)
        )

    def work_file(self, step_id: str, name: str) -> Path:
        """Return a file in a step's work directory."""
        return self.core.work_root / PLAN_ID / step_id / name

    # ----- assertions -----

    def assert_drained_failure(
        self, result: ExecutionResult, outcomes: dict[str, StepOutcome]
    ) -> AttemptReport:
        """Assert a continuation attempt drained, failed, and was recorded.

        Args:
            result: How the request was resolved.
            outcomes: The outcome expected of every step.

        Returns:
            AttemptReport: The attempt's report.
        """
        self.assertEqual(result.status, ExecutionStatus.FINALIZED)
        report = result.report
        assert report is not None
        self.assertEqual(report.termination_reason, TerminationReason.COMPLETED)
        self.assertTrue(report.is_drained)
        self.assertEqual(report.outcome, ExecutionOutcome.FAILED)
        self.assertEqual(report.step_outcomes, outcomes)
        self.assertEqual(report.unreached_step_ids, [])
        return report

    def assert_recorded(self, report: AttemptReport, *, run_token: int) -> None:
        """Assert the plan document records report as its finished request.

        The request is consumed although the attempt failed, the approval
        status is left as it was, and the plan is no longer eligible.

        Args:
            report: The attempt's report.
            run_token: The run token the attempt served.
        """
        doc = self.stored()
        self.assertEqual(doc["status"], "approved")
        self.assertEqual(doc["executed_run_token"], run_token)
        record = doc["last_finalized_execution"]
        self.assertEqual(record["execution_id"], report.execution_id)
        self.assertEqual(record["run_token"], run_token)
        self.assertEqual(record["outcome"], "failed")
        self.assertEqual(record["report"], report.to_dict())
        self.assertFalse(is_plan_eligible(doc))

    def assert_correlated(self, *reports: AttemptReport) -> None:
        """Assert every spooled event belongs to one of these attempts.

        Each event must carry the execution ID of one of them, and that
        attempt's plan generation and run token.

        Args:
            reports: The reports of every attempt the plan should have had.
        """
        identities = {
            report.execution_id: (report.plan_generation, report.run_token)
            for report in reports
        }
        events = self.spooled()
        self.assertTrue(events)
        for path, event in events:
            with self.subTest(event=str(path)):
                self.assertIn(event.get("execution_id"), identities)
                self.assertEqual(
                    (event["plan_generation"], event["run_token"]),
                    identities[event["execution_id"]],
                )

    def assert_one_record_each(self, report: AttemptReport) -> None:
        """Assert the attempt published its start and its report exactly once."""
        started = self.events_of_type("plan.attempt_started", report)
        published = self.events_of_type("plan.attempt_report", report)
        self.assertEqual(started, [Path("plan_attempt_started.json")])
        self.assertEqual(published, [Path("plan_attempt_report.json")])
        ((_, event),) = [
            item
            for item in self.spooled(report.execution_id)
            if item[1]["type"] == "plan.attempt_report"
        ]
        self.assertEqual(event["report"], report.to_dict())

    def assert_snapshot_shows(
        self,
        snapshot: dict[str, Any],
        report: dict[str, Any],
        *,
        authority: str = "daemon",
    ) -> None:
        """Assert a snapshot shows one attempt, and every step as it ended.

        Args:
            snapshot: The stored snapshot.
            report: The attempt's report, serialized, as the plan store
                records it.
            authority: The execution authority the attempt ran under.
        """
        self.assertEqual(snapshot["projection"], "attempt")
        self.assertEqual(snapshot["scope"], SCOPE)
        attempt = snapshot["attempt"]
        for field in (
            "execution_id",
            "plan_generation",
            "run_token",
            "failure_policy",
            "termination_reason",
            "outcome",
            "counts",
        ):
            with self.subTest(field=field):
                self.assertEqual(attempt[field], report[field])
        self.assertEqual(attempt["execution_authority"], authority)
        self.assertEqual(attempt["state"], "finished")

        steps = snapshot["steps"]
        self.assertEqual(sorted(steps), sorted(report["step_ids"]))
        for step_id, outcome in report["step_outcomes"].items():
            with self.subTest(step=step_id):
                self.assertEqual(
                    steps[step_id]["state"], STATE_OF[StepOutcome(outcome)]
                )
                self.assertEqual(steps[step_id]["outcome"], outcome)
        for step_id in report["unreached_step_ids"]:
            with self.subTest(step=step_id):
                self.assertEqual(steps[step_id]["state"], "unreached")
                self.assertIsNone(steps[step_id]["outcome"])
                self.assertIsNone(steps[step_id]["run_id"])

    def assert_runs_belong_to(
        self, snapshot: dict[str, Any], execution_id: str
    ) -> None:
        """Assert every run a snapshot shows is a run of one attempt.

        Args:
            snapshot: The stored snapshot.
            execution_id: The attempt.
        """
        for step_id, step in snapshot["steps"].items():
            if step["run_id"] is None:
                continue
            with self.subTest(step=step_id):
                step_dir = self.attempt_dir(execution_id) / "steps" / step_id
                events = [json.loads(p.read_text()) for p in step_dir.glob("*.json")]
                self.assertTrue(events)
                self.assertEqual(
                    {event["execution_id"] for event in events}, {execution_id}
                )
                self.assertIn(step["run_id"], {event.get("run_id") for event in events})


class DaemonScenarios:
    """The daemon's path, from a changed scenario document to its snapshot.

    Mixed into one test case per internal-storage backend.
    """

    def test_failed_branch_blocks_its_dependents_while_the_other_completes(self):
        doc = self.plan(scenario("branch_failure"))
        self.assertEqual(doc["status"], "approved")
        self.assertEqual(doc["plan"]["failure_policy"], "continue_independent")

        with self.assertLogs(CORE_LOGGER, level=logging.DEBUG) as logs:
            result = self.execute_as_daemon()

        report = self.assert_drained_failure(result, BRANCH_FAILURE_OUTCOMES)
        # A drained, truthfully recorded failure is a warning, not an error.
        (summary,) = [r for r in logs.records if r.getMessage() == result.message]
        self.assertEqual(summary.levelno, logging.WARNING)
        self.assertEqual(report.plan_generation, doc["plan_generation"])
        self.assertEqual(report.run_token, 0)
        self.assertEqual(
            report.direct_blockers, {"lane_2__upload": ["lane_2__process"]}
        )
        self.assertEqual(
            report.failed_ancestors, {"lane_2__upload": ["lane_2__process"]}
        )
        for step_id, message in (
            ("update_metadata", "Planned metadata update failure"),
            ("lane_2__process", "Planned failure processing lane 2"),
        ):
            failure = report.failures[step_id]
            self.assertEqual(failure.error_type, "RuntimeError")
            self.assertIn(message, failure.error)
        # Lane 1's real steps ran, although the metadata update had failed.
        self.assertEqual(
            self.work_file("lane_1__prepare", "lane_1_config.txt").read_text(),
            "Configuration for lane 1",
        )
        self.assert_recorded(report, run_token=0)

        # Events: one attempt, told apart from any other by what it stamped.
        self.assert_correlated(report)
        self.assert_one_record_each(report)
        self.assertEqual(
            self.events_of_type("step.blocked", report),
            [Path("steps", "lane_2__upload", "0001_step_blocked.json")],
        )
        self.assertEqual(
            self.steps_with("step.failed", report),
            ["lane_2__process", "update_metadata"],
        )
        self.assertEqual(
            self.steps_with("step.succeeded", report),
            sorted(
                step_id
                for step_id, outcome in BRANCH_FAILURE_OUTCOMES.items()
                if outcome is SUCCEEDED
            ),
        )
        # A blocked step never ran: its stream holds its blocked event alone,
        # which names no run.
        blocked_dir = self.attempt_dir(report.execution_id) / "steps" / "lane_2__upload"
        self.assertEqual(
            [p.name for p in blocked_dir.iterdir()], ["0001_step_blocked.json"]
        )
        self.assertNotIn(
            "run_id", json.loads((blocked_dir / "0001_step_blocked.json").read_text())
        )

        snapshot = self.consume_spool()
        self.assert_snapshot_shows(snapshot, report.to_dict())
        blocked = snapshot["steps"]["lane_2__upload"]
        self.assertEqual(blocked["direct_blockers"], ["lane_2__process"])
        self.assertEqual(blocked["failed_ancestors"], ["lane_2__process"])
        self.assertIsNone(blocked["run_id"])
        self.assertEqual(
            snapshot["steps"]["update_metadata"]["error"]["error_type"], "RuntimeError"
        )
        self.assertEqual(
            snapshot["attempt"]["counts"],
            {"succeeded": 5, "reused": 0, "failed": 2, "blocked": 1, "unreached": 0},
        )

        # The finished request is not run again by a later event for it.
        again = self.execute_as_daemon()
        self.assertEqual(again.status, ExecutionStatus.NOT_ELIGIBLE)
        self.assert_correlated(report)

    def test_required_metadata_failure_blocks_every_branch(self):
        doc = self.plan(scenario("branch_failure_metadata_required"))
        self.assertEqual(doc["plan"]["failure_policy"], "continue_independent")

        result = self.execute_as_daemon()

        report = self.assert_drained_failure(result, METADATA_REQUIRED_OUTCOMES)
        self.assertEqual(
            report.direct_blockers,
            {
                "lane_1__prepare": ["update_metadata"],
                "lane_1__process": ["lane_1__prepare"],
                "lane_1__upload": ["lane_1__process"],
                "lane_2__prepare": ["update_metadata"],
                "lane_2__process": ["lane_2__prepare"],
                "lane_2__upload": ["lane_2__process"],
            },
        )
        self.assertEqual(
            report.failed_ancestors, dict.fromkeys(LANE_STEPS, ["update_metadata"])
        )
        # Lane 2's failing step is never invoked: it is blocked first.
        self.assertNotIn("lane_2__process", report.failures)
        self.assertFalse(
            self.work_file("lane_1__prepare", "lane_1_config.txt").exists()
        )
        self.assert_recorded(report, run_token=0)

        self.assert_correlated(report)
        self.assert_one_record_each(report)
        self.assertEqual(
            self.steps_with("step.blocked", report),
            sorted(LANE_STEPS),
        )
        self.assertEqual(
            self.steps_with("step.started", report),
            ["update_metadata", "validate_shared"],
        )

        snapshot = self.consume_spool()
        self.assert_snapshot_shows(snapshot, report.to_dict())
        for step_id in ("lane_1__prepare", "lane_2__prepare"):
            self.assertEqual(
                snapshot["steps"][step_id]["direct_blockers"], ["update_metadata"]
            )
        self.assertEqual(
            snapshot["steps"]["lane_2__upload"]["failed_ancestors"],
            ["update_metadata"],
        )

    def test_rerun_reuses_finished_work_and_retries_what_failed(self):
        self.plan(scenario("branch_failure"))
        first = self.assert_drained_failure(
            self.execute_as_daemon(), BRANCH_FAILURE_OUTCOMES
        )
        # Lane 1's declared output goes missing between the attempts.
        self.work_file("lane_1__prepare", "lane_1_config.txt").unlink()

        self.request_rerun()
        result = self.execute_as_daemon()

        report = self.assert_drained_failure(
            result,
            {
                "validate_shared": REUSED,
                "update_metadata": FAILED,
                # Its declared output is missing, so it runs again...
                "lane_1__prepare": SUCCEEDED,
                # ...while its dependents' results still stand.
                "lane_1__process": REUSED,
                "lane_1__upload": REUSED,
                "lane_2__prepare": REUSED,
                "lane_2__process": FAILED,
                "lane_2__upload": BLOCKED,
            },
        )
        self.assertEqual(report.run_token, 1)
        self.assertEqual(report.plan_generation, first.plan_generation)
        self.assertGreater(
            execution_order_key(report.execution_id),
            execution_order_key(first.execution_id),
        )
        self.assertTrue(self.work_file("lane_1__prepare", "lane_1_config.txt").exists())
        self.assert_recorded(report, run_token=1)
        self.assert_correlated(first, report)
        self.assert_one_record_each(report)
        self.assertEqual(
            [
                event["reason"]
                for _, event in self.spooled(report.execution_id)
                if event["type"] == "step.skipped"
            ],
            ["cache_hit"] * 4,
        )

        # The snapshot shows the rerun alone, never the first attempt's steps.
        snapshot = self.consume_spool()
        self.assert_snapshot_shows(snapshot, report.to_dict())
        self.assert_runs_belong_to(snapshot, report.execution_id)

    def test_fail_fast_policy_stops_at_the_first_failure_and_stays_eligible(self):
        # The same plan under the legacy policy, named by the scenario.
        doc = self.plan(scenario("branch_failure", failure_policy="fail_fast"))
        self.assertEqual(doc["plan"]["failure_policy"], "fail_fast")

        with self.assertLogs(CORE_LOGGER, level=logging.ERROR):
            result = self.execute_as_daemon()

        self.assertEqual(result.status, ExecutionStatus.UNFINISHED)
        report = result.report
        assert report is not None
        self.assertEqual(report.termination_reason, TerminationReason.FAILED_FAST)
        self.assertEqual(
            report.step_outcomes,
            {"validate_shared": SUCCEEDED, "update_metadata": FAILED},
        )
        # Work the failure cut short is unreached, not blocked.
        self.assertEqual(report.unreached_step_ids, LANE_STEPS)
        self.assertEqual(self.events_of_type("step.blocked", report), [])
        stored = self.stored()
        self.assertEqual(stored["executed_run_token"], -1)
        self.assertNotIn("last_finalized_execution", stored)
        self.assertTrue(is_plan_eligible(stored))

        self.assert_correlated(report)
        self.assert_one_record_each(report)
        snapshot = self.consume_spool()
        self.assert_snapshot_shows(snapshot, report.to_dict())


class RunOnceScenarios:
    """``yggdrasil run-doc`` from the scenario document to its snapshot.

    Mixed into one test case per internal-storage backend.
    """

    def test_failed_branch_finishes_the_healthy_one_and_exits_nonzero(self):
        exit_code = self.run_once(scenario("branch_failure"))

        self.assertEqual(exit_code, 1)
        doc = self.stored()
        self.assertEqual(doc["execution_authority"], "run_once")
        self.assertTrue(doc["execution_owner"].startswith("run_once:"))
        self.assertEqual(doc["executed_run_token"], 0)
        record = doc["last_finalized_execution"]
        self.assertEqual(record["outcome"], "failed")
        self.assertEqual(
            record["report"]["step_outcomes"],
            {
                step_id: outcome.value
                for step_id, outcome in BRANCH_FAILURE_OUTCOMES.items()
            },
        )
        self.assertTrue(self.work_file("lane_1__prepare", "lane_1_config.txt").exists())

        # Run-once writes the snapshot itself on its way out.
        snapshot = self.stored_snapshot()
        self.assert_snapshot_shows(snapshot, record["report"], authority="run_once")
        self.assertEqual(snapshot["attempt"]["execution_owner"], doc["execution_owner"])
        self.assert_runs_belong_to(snapshot, record["execution_id"])


class TestDaemonBranchesSQLite(DaemonScenarios, BranchExecutionTestCase):
    storage_type = SQLiteStorage


class TestDaemonBranchesCouchDB(DaemonScenarios, BranchExecutionTestCase):
    storage_type = CouchStorage


class TestRunOnceBranchesSQLite(RunOnceScenarios, BranchExecutionTestCase):
    storage_type = SQLiteStorage


class TestRunOnceBranchesCouchDB(RunOnceScenarios, BranchExecutionTestCase):
    storage_type = CouchStorage


if __name__ == "__main__":
    unittest.main()
