"""Snapshots of real attempts, built from the spool the engine wrote.

Every test runs attempts through a real Engine and FileSpoolEmitter, then
builds the plan's snapshot the way the ops consumer does. Each one pins a way
the old per-step selection went wrong, or a way an ordering key could: a
snapshot must show one attempt, the latest observed one, with every step it
planned, and must say how that attempt ended. An attempt directory counts for
ordering new attempts as soon as it exists, but only a start or report record
makes an attempt observed.
"""

import json
import shutil
import threading
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import Mock, patch

import lib.ops.consumer as consumer_module
from lib.ops.consumer import FileSpoolConsumer, build_plan_snapshot
from lib.ops.snapshot import (
    ATTEMPT_FINISHED,
    ATTEMPT_RUNNING,
    PROJECTION_ATTEMPT,
    PROJECTION_LEGACY,
    STATE_INTERRUPTED,
    STATE_PENDING,
    STATE_UNREACHED,
)
from tests.execution_support import (
    REALM,
    SCOPE,
    WAIT,
    Clock,
    ScriptedSteps,
    make_plan,
    spec,
)
from yggdrasil.core.engine import Engine
from yggdrasil.core.execution_ids import (
    ExecutionIdAllocator,
    execution_timestamp,
    format_execution_id,
)
from yggdrasil.flow.attempt import AttemptContext
from yggdrasil.flow.errors import PermanentStepError
from yggdrasil.flow.events.attempt_records import (
    ATTEMPT_REPORT_EVENT,
    ATTEMPT_STARTED_EVENT,
    SpoolAttemptHistory,
    attempt_dir,
    record_filename,
)
from yggdrasil.flow.events.emitter import FileSpoolEmitter
from yggdrasil.flow.model import CONTINUE_INDEPENDENT_POLICY, FAIL_FAST_POLICY, Plan
from yggdrasil.flow.outcomes import AttemptReport, TerminationReason

CONTINUE = CONTINUE_INDEPENDENT_POLICY
FAIL_FAST = FAIL_FAST_POLICY
PLAN_ID = "pln_snapshots"
T0 = datetime(2026, 9, 21, 12, 5, tzinfo=UTC)
# Later than any real clock these tests run under.
FUTURE = datetime(2099, 1, 1, tzinfo=UTC)


def plan_of(*specs, policy: str = CONTINUE) -> Plan:
    """A plan of scripted steps under this module's plan ID."""
    return make_plan(*specs, policy=policy, plan_id=PLAN_ID)


class SnapshotTestCase(unittest.TestCase):
    """An engine publishing to a temporary spool, and the snapshot it yields."""

    def setUp(self) -> None:
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        root = Path(temp_dir.name)
        self.spool = root / "spool"
        self.plan_dir = self.spool / REALM / PLAN_ID
        self.attempts = self.plan_dir / "attempts"
        self.steps = ScriptedSteps()
        resolver = patch(
            "yggdrasil.core.engine.resolve_callable", return_value=self.steps.fn
        )
        resolver.start()
        self.addCleanup(resolver.stop)
        self.engine = Engine(
            work_root=root / "work", emitter=FileSpoolEmitter(self.spool)
        )

    def context(
        self,
        plan: Plan,
        *,
        plan_generation: str | None = None,
        run_token: int | None = None,
    ) -> AttemptContext:
        """Open an attempt's context, with its ID reserved as callers do."""
        execution_id, reserved = self.engine.reserve_execution_id(plan)
        return AttemptContext.for_plan(
            plan,
            execution_id=execution_id,
            plan_generation=plan_generation,
            run_token=run_token,
            execution_id_reserved=reserved,
        )

    def attempt(
        self,
        plan: Plan,
        *,
        plan_generation: str | None = None,
        run_token: int | None = None,
    ) -> AttemptContext:
        """Run one attempt with a captured identity; return its context."""
        context = self.context(
            plan, plan_generation=plan_generation, run_token=run_token
        )
        try:
            self.engine._run_attempt(plan, context=context)
        except Exception:
            pass  # how it ended is in the report
        return context

    def attempt_dir(self, execution_id: str) -> Path:
        """One attempt's directory."""
        return attempt_dir(self.spool, REALM, PLAN_ID, execution_id)

    def step_dir(self, execution_id: str, step_id: str) -> Path:
        """One step's event directory within one attempt."""
        return self.attempt_dir(execution_id) / "steps" / step_id

    def snapshot(self) -> dict[str, Any]:
        """The plan's snapshot, as the consumer builds it."""
        return build_plan_snapshot(self.plan_dir, REALM, PLAN_ID)

    def states(self, snapshot: dict[str, Any]) -> dict[str, tuple[Any, Any]]:
        """Each step's (state, outcome) in a snapshot."""
        return {
            step_id: (entry["state"], entry["outcome"])
            for step_id, entry in snapshot["steps"].items()
        }

    def assert_shows(self, snapshot: dict[str, Any], report: AttemptReport) -> None:
        """Assert the snapshot shows the finished attempt behind report."""
        self.assertEqual(snapshot["projection"], PROJECTION_ATTEMPT)
        attempt = snapshot["attempt"]
        self.assertEqual(attempt["execution_id"], report.execution_id)
        self.assertEqual(attempt["state"], ATTEMPT_FINISHED)
        assert report.termination_reason is not None and report.outcome is not None
        self.assertEqual(attempt["termination_reason"], report.termination_reason.value)
        self.assertEqual(attempt["outcome"], report.outcome.value)
        self.assertEqual(attempt["counts"], report.counts)


class TestOneAttemptPerSnapshot(SnapshotTestCase):
    """A snapshot never mixes attempts, and never shows a stale one."""

    def assert_later_failure_imports_nothing(
        self, policy: str, downstream: tuple[Any, Any]
    ) -> None:
        """Run a successful attempt, then a failing one; check the snapshot."""
        # Everything succeeds first; then a, with new params, fails.
        first = self.attempt(
            plan_of(spec("a"), spec("b", "a"), spec("c", "b"), policy=policy)
        )
        self.steps.fail("a", PermanentStepError("lane broke"))
        second = self.attempt(
            plan_of(spec("a", version=2), spec("b", "a"), spec("c", "b"), policy=policy)
        )

        snapshot = self.snapshot()

        self.assert_shows(snapshot, second.report)
        self.assertEqual(
            self.states(snapshot),
            {"a": ("step.failed", "failed"), "b": downstream, "c": downstream},
        )
        # b still has the first attempt's run in the spool; it is not shown.
        self.assertTrue(any(self.step_dir(first.execution_id, "b").iterdir()))
        self.assertIsNone(snapshot["steps"]["b"]["run_id"])

    def test_later_failed_continuation_does_not_import_earlier_successes(self):
        self.assert_later_failure_imports_nothing(CONTINUE, ("step.blocked", "blocked"))

    def test_later_fail_fast_failure_does_not_import_earlier_successes(self):
        self.assert_later_failure_imports_nothing(FAIL_FAST, (STATE_UNREACHED, None))

    def test_newer_running_attempt_is_shown_over_an_older_success(self):
        self.attempt(plan_of(spec("a"), spec("b", "a")))
        gate = self.steps.block("a")
        newer = plan_of(spec("a", version=2), spec("b", "a"))
        worker = threading.Thread(target=self.attempt, args=(newer,))
        worker.start()
        self.addCleanup(worker.join, WAIT)
        self.addCleanup(gate.release)
        self.assertTrue(gate.entered.wait(WAIT))

        running = self.snapshot()

        self.assertEqual(running["attempt"]["state"], ATTEMPT_RUNNING)
        self.assertEqual(
            self.states(running),
            {"a": ("step.started", None), "b": (STATE_PENDING, None)},
        )
        gate.release()
        worker.join(WAIT)
        # b's reuse is the newer attempt's own outcome; the older one ran b.
        self.assertEqual(
            self.states(self.snapshot()),
            {"a": ("step.succeeded", "succeeded"), "b": ("step.skipped", "reused")},
        )

    def test_replaying_an_old_attempt_changes_nothing(self):
        first = self.attempt(plan_of(spec("a"), spec("b")))
        self.steps.fail("b", PermanentStepError("broken"))
        second = self.attempt(plan_of(spec("a"), spec("b", version=2)))
        expected = self.snapshot()

        # Re-deliver every file the first attempt wrote, in place; then copy
        # its report and its events into the newer attempt's directories.
        first_dir = self.attempt_dir(first.execution_id)
        for path in first_dir.rglob("*.json"):
            path.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
        shutil.copy(
            first_dir / record_filename(ATTEMPT_REPORT_EVENT),
            self.attempt_dir(second.execution_id) / "zz_replayed_report.json",
        )
        for step_id in ("a", "b"):
            for path in self.step_dir(first.execution_id, step_id).glob("*.json"):
                shutil.copy(
                    path,
                    self.step_dir(second.execution_id, step_id) / f"zz_{path.name}",
                )

        replayed = self.snapshot()
        self.assert_shows(replayed, second.report)
        self.assertEqual(replayed["steps"], expected["steps"])

    def test_late_and_repeated_events_do_not_erase_a_failure(self):
        self.steps.fail("a", PermanentStepError("broken"))
        context = self.attempt(plan_of(spec("a")))
        step_dir = self.step_dir(context.execution_id, "a")
        failed_file = next(step_dir.glob("*_step_failed.json"))
        failed = json.loads(failed_file.read_text(encoding="utf-8"))

        # Progress arriving after the failure, and the failure delivered twice.
        late = {
            **failed,
            "type": "step.progress",
            "seq": 99,
            "eid": "late",
            "progress": 80,
        }
        (step_dir / "0099_step_progress.json").write_text(
            json.dumps(late), encoding="utf-8"
        )
        shutil.copy(failed_file, step_dir / "9999_step_failed_copy.json")

        entry = self.snapshot()["steps"]["a"]
        self.assertEqual((entry["state"], entry["outcome"]), ("step.failed", "failed"))
        self.assertEqual(entry["error"], context.report.failures["a"].to_dict())

    def test_every_attempt_keeps_its_own_records(self):
        contexts = [self.attempt(plan_of(spec("a"))) for _ in range(3)]

        self.assertEqual(
            sorted(
                str(p.relative_to(self.attempts)) for p in self.attempts.glob("*/*")
            ),
            sorted(
                f"{c.execution_id}/{name}"
                for c in contexts
                for name in (
                    record_filename(ATTEMPT_STARTED_EVENT),
                    record_filename(ATTEMPT_REPORT_EVENT),
                    "steps",
                )
            ),
        )
        self.assert_shows(self.snapshot(), contexts[-1].report)


class TestAttemptEndings(SnapshotTestCase):
    """However an attempt ends, its snapshot says so and stops looking live."""

    def test_fail_fast_failure(self):
        self.steps.fail("a", PermanentStepError("broken"))
        context = self.attempt(
            plan_of(spec("a"), spec("b", "a"), spec("c"), policy=FAIL_FAST)
        )

        snapshot = self.snapshot()

        self.assert_shows(snapshot, context.report)
        self.assertEqual(
            snapshot["attempt"]["termination_reason"],
            TerminationReason.FAILED_FAST.value,
        )
        # Neither the dependent nor the unrelated step is relabelled blocked.
        self.assertEqual(
            self.states(snapshot),
            {
                "a": ("step.failed", "failed"),
                "b": (STATE_UNREACHED, None),
                "c": (STATE_UNREACHED, None),
            },
        )
        self.assertEqual(snapshot["steps"]["a"]["error"]["error"], "broken")

    def test_cooperative_cancellation(self):
        plan = plan_of(spec("a"), spec("b", "a"), spec("c"), spec("d"))
        context = self.context(plan)
        self.steps.fail("a", PermanentStepError("broken"))
        self.steps.behaviors["c"] = lambda ctx: context.request_cancellation()
        with self.assertRaises(Exception):
            self.engine._run_attempt(plan, context=context)

        snapshot = self.snapshot()

        self.assert_shows(snapshot, context.report)
        self.assertEqual(
            snapshot["attempt"]["termination_reason"], TerminationReason.CANCELLED.value
        )
        self.assertEqual(
            self.states(snapshot),
            {
                "a": ("step.failed", "failed"),
                "b": ("step.blocked", "blocked"),
                "c": ("step.succeeded", "succeeded"),
                "d": (STATE_UNREACHED, None),
            },
        )
        self.assertEqual(snapshot["steps"]["b"]["direct_blockers"], ["a"])
        self.assertEqual(snapshot["steps"]["d"]["direct_blockers"], [])

    def test_preflight_rejection(self):
        context = self.attempt(plan_of(spec("a", "b"), spec("b", "a")))

        snapshot = self.snapshot()

        self.assert_shows(snapshot, context.report)
        self.assertEqual(
            snapshot["attempt"]["termination_reason"],
            TerminationReason.PREFLIGHT_REJECTED.value,
        )
        self.assertIn("cycle", snapshot["attempt"]["diagnostic"]["message"])
        self.assertEqual(snapshot["scope"], SCOPE)
        self.assertEqual(
            self.states(snapshot),
            {"a": (STATE_UNREACHED, None), "b": (STATE_UNREACHED, None)},
        )

    def test_step_failing_before_its_wrapper_ran_is_shown_failed(self):
        # Hashing a declared input is realm-controlled work done before the
        # @step wrapper runs, so the failure publishes no step event at all.
        data = self.spool.parent / "input.txt"
        data.write_text("reads", encoding="utf-8")
        unhashable = spec("a")
        unhashable.inputs = {"data": str(data)}
        with patch(
            "yggdrasil.core.engine.sha256_file",
            side_effect=PermissionError("input unreadable"),
        ):
            context = self.attempt(plan_of(unhashable, spec("b")))

        snapshot = self.snapshot()

        self.assert_shows(snapshot, context.report)
        self.assertFalse(
            self.step_dir(context.execution_id, "a").exists(), "a published an event"
        )
        self.assertEqual(
            self.states(snapshot),
            {"a": ("step.failed", "failed"), "b": ("step.succeeded", "succeeded")},
        )
        self.assertEqual(
            snapshot["steps"]["a"]["error"], context.report.failures["a"].to_dict()
        )
        self.assertIsNone(snapshot["steps"]["a"]["run_id"])

    def test_success_event_the_engine_never_confirmed_is_not_shown_as_success(self):
        # a's wrapper publishes step.succeeded; writing its cache marker then
        # fails, which aborts the attempt with no outcome recorded for a.
        with patch(
            "yggdrasil.core.engine._replace_marker",
            side_effect=OSError("disk full"),
        ):
            context = self.attempt(plan_of(spec("a"), spec("b")))

        snapshot = self.snapshot()

        self.assert_shows(snapshot, context.report)
        self.assertEqual(
            snapshot["attempt"]["termination_reason"],
            TerminationReason.ORCHESTRATION_ERROR.value,
        )
        step_dir = self.step_dir(context.execution_id, "a")
        (succeeded,) = step_dir.glob("*_step_succeeded.json")
        self.assertEqual(
            self.states(snapshot),
            {"a": (STATE_INTERRUPTED, None), "b": (STATE_UNREACHED, None)},
        )
        self.assertEqual(
            snapshot["steps"]["a"]["run_id"],
            json.loads(succeeded.read_text(encoding="utf-8"))["run_id"],
        )

    def test_consumer_writes_the_snapshot_of_an_attempt_that_ran_no_step(self):
        self.attempt(plan_of(spec("a", "b"), spec("b", "a")))
        writer = Mock()

        FileSpoolConsumer(spool_root=self.spool, writer=writer).consume()

        writer.write.assert_called_once()
        plan_dir, snapshot = writer.write.call_args.args
        self.assertEqual(plan_dir, self.plan_dir)
        self.assertEqual(
            snapshot["attempt"]["termination_reason"],
            TerminationReason.PREFLIGHT_REJECTED.value,
        )


class TestAttemptOrder(SnapshotTestCase):
    """Attempts are ordered by execution ID, never by generation or token."""

    def test_regeneration_whose_generation_sorts_lower_is_still_shown(self):
        self.attempt(plan_of(spec("a")), plan_generation="f" * 32, run_token=4)
        newer = self.attempt(
            plan_of(spec("a", regenerated=True)), plan_generation="0" * 32, run_token=0
        )

        snapshot = self.snapshot()

        self.assert_shows(snapshot, newer.report)
        self.assertEqual(snapshot["attempt"]["plan_generation"], "0" * 32)
        self.assertEqual(snapshot["attempt"]["run_token"], 0)

    def test_attempts_of_one_request_are_ordered_and_kept_apart(self):
        # An interrupted attempt and its retry share generation and token.
        plan = plan_of(spec("a"), spec("b"))
        interrupted = self.context(plan, plan_generation="gen", run_token=0)
        self.steps.behaviors["a"] = lambda ctx: interrupted.request_cancellation()
        with self.assertRaises(Exception):
            self.engine._run_attempt(plan, context=interrupted)
        self.steps.behaviors.clear()
        self.steps.fail("b", PermanentStepError("broken"))
        retry = self.attempt(plan, plan_generation="gen", run_token=0)

        snapshot = self.snapshot()

        self.assert_shows(snapshot, retry.report)
        self.assertEqual(
            self.states(snapshot),
            {"a": ("step.skipped", "reused"), "b": ("step.failed", "failed")},
        )
        # a's run is the retry's cache hit, not the interrupted attempt's run.
        (skipped,) = self.step_dir(retry.execution_id, "a").glob("*_step_skipped.json")
        retry_run = json.loads(skipped.read_text(encoding="utf-8"))["run_id"]
        (started,) = self.step_dir(interrupted.execution_id, "a").glob(
            "*_step_started.json"
        )
        interrupted_run = json.loads(started.read_text(encoding="utf-8"))["run_id"]
        self.assertNotEqual(retry_run, interrupted_run)
        self.assertEqual(snapshot["steps"]["a"]["run_id"], retry_run)

    def test_attempt_with_a_caller_built_id_never_hides_an_allocated_one(self):
        # "exec_sched_plan" sorts above any allocated ID by name alone.
        plan = plan_of(spec("a"))
        custom = AttemptContext.for_plan(plan, execution_id="exec_sched_plan")
        self.engine._run_attempt(plan, context=custom)

        allocated = self.attempt(plan)

        self.assertEqual(
            self.snapshot()["attempt"]["execution_id"], allocated.report.execution_id
        )

    def test_id_resembling_an_allocated_one_never_hides_a_newer_attempt(self):
        november = datetime(2026, 11, 1, tzinfo=UTC)
        for index, malformed in enumerate(
            (
                # Read on its own, the unpadded date is 1 November, yet the ID
                # sorts above every ID allocated on that day.
                "2026111T000000000000Z_ffff",
                "20991231T235959999999Z",  # no suffix
                "20991231T235959999999Z_FFFF",  # upper-case suffix
                "exec_20991231T235959999999Z_ffff",  # the old prefix
            )
        ):
            with self.subTest(malformed=malformed):
                plan = make_plan(
                    spec("a"), policy=CONTINUE, plan_id=f"pln_malformed_{index}"
                )
                self.engine._run_attempt(
                    plan, context=AttemptContext.for_plan(plan, execution_id=malformed)
                )
                self.engine = Engine(
                    work_root=self.spool.parent / "work",
                    emitter=FileSpoolEmitter(self.spool),
                    execution_ids=ExecutionIdAllocator(
                        SpoolAttemptHistory(self.spool), clock=Clock(november)
                    ),
                )

                allocated = self.attempt(plan)

                self.assertEqual(
                    execution_timestamp(allocated.report.execution_id), november
                )
                plan_dir = self.spool / REALM / plan.plan_id
                shown = build_plan_snapshot(plan_dir, REALM, plan.plan_id)
                self.assertEqual(
                    shown["attempt"]["execution_id"], allocated.report.execution_id
                )


class TestReadingStaysProportional(SnapshotTestCase):
    """A snapshot reads what its attempt left behind, not the plan's history.

    Reads are counted where the consumer opens every event file, so each test
    states exactly which files a snapshot may open.
    """

    def count_reads(self) -> list[Path]:
        """Record every event file the consumer opens from now on."""
        reads: list[Path] = []
        original = consumer_module._safe_load

        def counting(path: Path) -> dict[str, Any]:
            reads.append(path)
            return original(path)

        patcher = patch.object(consumer_module, "_safe_load", counting)
        patcher.start()
        self.addCleanup(patcher.stop)
        return reads

    def history(self, *step_ids: str, attempts: int = 3) -> list[str]:
        """Run attempts that each execute every step afresh."""
        return [
            self.attempt(plan_of(*(spec(s, version=v) for s in step_ids))).execution_id
            for v in range(attempts)
        ]

    def test_only_the_shown_attempts_files_are_opened(self):
        older = self.history("a", "b")
        self.steps.fail("a", PermanentStepError("broken"))
        shown = self.attempt(plan_of(spec("a", version=9), spec("b", "a")))
        # Newer reservations nothing was recorded in, as a cancellation leaves.
        unused = [
            format_execution_id(FUTURE + timedelta(days=d), "0000") for d in (0, 1)
        ]
        for execution_id in unused:
            self.attempt_dir(execution_id).mkdir()
        reads = self.count_reads()

        snapshot = self.snapshot()

        self.assertEqual(snapshot["attempt"]["execution_id"], shown.execution_id)
        # The unused reservations are asked for their two records, which they
        # do not have; nothing else is opened outside the shown attempt.
        outside = [
            p
            for p in reads
            if not p.is_relative_to(self.attempt_dir(shown.execution_id))
        ]
        self.assertEqual(
            sorted(outside),
            sorted(
                self.attempt_dir(execution_id) / record_filename(event_type)
                for execution_id in unused
                for event_type in (ATTEMPT_STARTED_EVENT, ATTEMPT_REPORT_EVENT)
            ),
        )
        for execution_id in older:
            self.assertFalse(
                [p for p in reads if p.is_relative_to(self.attempt_dir(execution_id))]
            )

    def test_rejected_attempt_opens_no_step_file(self):
        self.history("a", "b")
        rejected = self.attempt(plan_of(spec("a", "b"), spec("b", "a")))
        reads = self.count_reads()

        snapshot = self.snapshot()

        self.assertEqual(
            snapshot["attempt"]["termination_reason"],
            TerminationReason.PREFLIGHT_REJECTED.value,
        )
        self.assertEqual(
            sorted(reads),
            sorted(
                self.attempt_dir(rejected.execution_id) / record_filename(event_type)
                for event_type in (ATTEMPT_STARTED_EVENT, ATTEMPT_REPORT_EVENT)
            ),
        )

    def test_steps_outside_the_attempt_are_never_read(self):
        self.history("a", "retired")
        shown = self.attempt(plan_of(spec("a", version=9)))
        # Even a stray directory for another step inside the shown attempt.
        stray = self.step_dir(shown.execution_id, "retired")
        stray.mkdir(parents=True)
        (stray / "0001_step_started.json").write_text("{}")
        reads = self.count_reads()

        snapshot = self.snapshot()

        self.assertEqual(set(snapshot["steps"]), {"a"})
        self.assertFalse([p for p in reads if "retired" in p.parts])


class TestObservedAttempts(SnapshotTestCase):
    """Directories order new attempts; only records make an attempt observed."""

    def test_unrecorded_directories_order_new_attempts_but_are_never_shown(self):
        shown = self.attempt(plan_of(spec("a")))
        # Newer than it: an empty reservation, and one whose only record is
        # unreadable.
        empty = format_execution_id(FUTURE, "0000")
        corrupt = format_execution_id(FUTURE + timedelta(days=1), "0000")
        self.attempt_dir(empty).mkdir()
        self.attempt_dir(corrupt).mkdir()
        (self.attempt_dir(corrupt) / record_filename(ATTEMPT_STARTED_EVENT)).write_text(
            "{not json"
        )

        # Observation: neither is shown, and nothing looks running.
        before = self.snapshot()
        self.assert_shows(before, shown.report)
        self.assertEqual(before["attempt"]["state"], ATTEMPT_FINISHED)
        # Allocation: both still count, so the next attempt orders above them.
        after = self.attempt(plan_of(spec("a")))
        self.assertGreater(after.execution_id, corrupt)
        self.assertEqual(
            execution_timestamp(after.execution_id),
            FUTURE + timedelta(days=1, microseconds=1),
        )
        self.assert_shows(self.snapshot(), after.report)

    def test_report_without_its_start_record_is_observed(self):
        context = self.attempt(plan_of(spec("a"), spec("b")))
        (self.attempt_dir(context.execution_id) / "plan_attempt_started.json").unlink()

        snapshot = self.snapshot()

        self.assert_shows(snapshot, context.report)
        self.assertEqual(
            self.states(snapshot),
            {
                "a": ("step.succeeded", "succeeded"),
                "b": ("step.succeeded", "succeeded"),
            },
        )

    def test_start_record_alone_shows_a_running_attempt(self):
        context = self.attempt(plan_of(spec("a")))
        (self.attempt_dir(context.execution_id) / "plan_attempt_report.json").unlink()

        snapshot = self.snapshot()

        self.assertEqual(snapshot["attempt"]["execution_id"], context.execution_id)
        self.assertEqual(snapshot["attempt"]["state"], ATTEMPT_RUNNING)

    def test_record_naming_another_attempt_does_not_make_one_observed(self):
        shown = self.attempt(plan_of(spec("a")))
        misplaced = format_execution_id(FUTURE, "0000")
        self.attempt_dir(misplaced).mkdir()
        for name in ("plan_attempt_started.json", "plan_attempt_report.json"):
            shutil.copy(
                self.attempt_dir(shown.execution_id) / name,
                self.attempt_dir(misplaced) / name,
            )

        self.assert_shows(self.snapshot(), shown.report)

    def test_leftover_old_layout_neither_shows_nor_blocks(self):
        # A spool written before attempt directories: root-level records of a
        # far-future attempt, and its correlated step run.
        old_id = "exec_20990101T000000000000Z_" + "f" * 32
        self.plan_dir.mkdir(parents=True)
        (self.plan_dir / f"{old_id}_plan_attempt_started.json").write_text(
            json.dumps(
                {
                    "type": ATTEMPT_STARTED_EVENT,
                    "execution_id": old_id,
                    "scope": SCOPE,
                    "steps": [{"step_id": "old", "step_name": "old", "deps": []}],
                }
            )
        )
        old_run = self.plan_dir / "old" / "run_20990101T000000000000Z_aaaaaa"
        old_run.mkdir(parents=True)
        (old_run / "0001_step_failed.json").write_text(
            json.dumps({"type": "step.failed", "seq": 1, "execution_id": old_id})
        )
        self.assertIsNone(self.snapshot()["attempt"])

        context = self.attempt(plan_of(spec("a")))

        # Allocated by the clock, not above the old far-future record.
        self.assertLess(context.execution_id, "2099")
        snapshot = self.snapshot()
        self.assert_shows(snapshot, context.report)
        self.assertEqual(set(snapshot["steps"]), {"a"})
        self.assertEqual(snapshot["scope"], SCOPE)


class TestLegacyHistories(SnapshotTestCase):
    """Uncorrelated events are a labelled projection, never part of an attempt."""

    def write_legacy_run(self, step_id: str, run_id: str, *types: str) -> None:
        """Write a pre-correlation run of one step."""
        run_dir = self.plan_dir / step_id / run_id
        run_dir.mkdir(parents=True)
        for seq, type_ in enumerate(types, start=1):
            (run_dir / f"{seq:04d}_{type_.replace('.', '_')}.json").write_text(
                json.dumps(
                    {
                        "type": type_,
                        "seq": seq,
                        "scope": SCOPE,
                        "step_id": step_id,
                        "step_name": step_id,
                    }
                ),
                encoding="utf-8",
            )

    def test_history_without_attempt_records_is_a_legacy_projection(self):
        self.write_legacy_run("a", "run_20250101T000000000000Z_aaaaaa", "step.started")
        self.write_legacy_run(
            "a", "run_20250102T000000000000Z_bbbbbb", "step.started", "step.succeeded"
        )

        snapshot = self.snapshot()

        self.assertEqual(snapshot["projection"], PROJECTION_LEGACY)
        self.assertIsNone(snapshot["attempt"])
        self.assertEqual(snapshot["scope"], SCOPE)
        self.assertEqual(
            snapshot["steps"]["a"]["run_id"], "run_20250102T000000000000Z_bbbbbb"
        )
        self.assertEqual(snapshot["steps"]["a"]["state"], "step.succeeded")

    def test_legacy_projection_never_treats_attempts_as_a_step(self):
        self.write_legacy_run("a", "run_20250101T000000000000Z_aaaaaa", "step.started")
        # Only an unused reservation: no attempt is observed.
        self.attempt_dir(format_execution_id(T0, "0000")).mkdir(parents=True)

        snapshot = self.snapshot()

        self.assertEqual(snapshot["projection"], PROJECTION_LEGACY)
        self.assertEqual(set(snapshot["steps"]), {"a"})

    def test_correlated_attempt_is_shown_without_any_legacy_step(self):
        # Legacy runs that even sort after the attempt's own.
        self.write_legacy_run(
            "a", "run_29990101T000000000000Z_legacy", "step.started", "step.succeeded"
        )
        self.write_legacy_run("x", "run_29990101T000000000000Z_legacy", "step.failed")
        self.steps.fail("a", PermanentStepError("broken"))
        context = self.attempt(plan_of(spec("a"), spec("b")))

        snapshot = self.snapshot()

        self.assert_shows(snapshot, context.report)
        self.assertEqual(set(snapshot["steps"]), {"a", "b"})
        self.assertEqual(
            self.states(snapshot),
            {"a": ("step.failed", "failed"), "b": ("step.succeeded", "succeeded")},
        )
        self.assertNotEqual(
            snapshot["steps"]["a"]["run_id"], "run_29990101T000000000000Z_legacy"
        )


if __name__ == "__main__":
    unittest.main()
