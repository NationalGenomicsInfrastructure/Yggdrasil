"""Tests for what an execution attempt publishes.

Every event an attempt publishes carries the attempt's correlation, including
the events the engine emits itself rather than through a StepContext. Each
attempt leaves a start record before anything else happens, publishes one
``step.blocked`` per step a failure blocks at the moment it is blocked, and
ends with one report. The execution IDs that order attempts come from the
engine's allocator, which reads back only the spool the engine writes to.

Steps run through real ``@step`` functions. Spool-backed tests read the files a
FileSpoolEmitter wrote; the rest record events in memory. Cross-thread tests
coordinate with Gate handshakes, never sleeps.
"""

import json
import os
import shutil
import threading
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import Mock, patch

from lib.ops.consumer import build_plan_snapshot
from tests.execution_support import (
    REALM,
    SCOPE,
    WAIT,
    Clock,
    Gate,
    RecordingEmitter,
    ScriptedSteps,
    make_plan,
    spec,
)
from yggdrasil.core.engine import Engine
from yggdrasil.core.execution_ids import (
    ExecutionIdAllocator,
    ReservationExhaustedError,
    execution_timestamp,
    format_execution_id,
)
from yggdrasil.flow.attempt import AttemptContext
from yggdrasil.flow.errors import (
    AttemptCancelledError,
    EventPublicationError,
    OrchestrationError,
    PermanentStepError,
    PreflightValidationError,
    TransientStepError,
)
from yggdrasil.flow.events.attempt_records import (
    ATTEMPT_REPORT_EVENT,
    ATTEMPT_STARTED_EVENT,
    STEP_BLOCKED_EVENT,
    SpoolAttemptDirectories,
    SpoolAttemptHistory,
    attempt_dir,
    attempt_spool_path,
    attempts_dir,
    record_filename,
    step_events_dir,
)
from yggdrasil.flow.events.emitter import EventEmitter, FileSpoolEmitter
from yggdrasil.flow.model import CONTINUE_INDEPENDENT_POLICY, FAIL_FAST_POLICY, Plan
from yggdrasil.flow.outcomes import AttemptReport, StepOutcome, TerminationReason
from yggdrasil.flow.step import StepContext

CONTINUE = CONTINUE_INDEPENDENT_POLICY
FAIL_FAST = FAIL_FAST_POLICY
PLAN_ID = "pln_events"
CORRELATION_FIELDS = ("execution_id", "plan_generation", "run_token")
T0 = datetime(2026, 9, 21, 12, 5, tzinfo=UTC)
FAR_FUTURE_ID = "20990101T000000000000Z_ffff"


def exception_chain(exc: BaseException) -> list[BaseException]:
    """Every exception reachable from exc through __cause__ and __context__."""
    seen: list[BaseException] = []
    pending = [exc]
    while pending:
        current = pending.pop()
        if any(current is s for s in seen):
            continue
        seen.append(current)
        pending.extend(
            e for e in (current.__cause__, current.__context__) if e is not None
        )
    return seen


def plan_of(*specs, policy: str = CONTINUE, plan_id: str = PLAN_ID) -> Plan:
    """A plan of scripted steps under this module's plan ID."""
    return make_plan(*specs, policy=policy, plan_id=plan_id)


class EngineEventsTestCase(unittest.TestCase):
    """An engine recording events in memory, and a spool for spool-backed ones."""

    def setUp(self) -> None:
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        self.root = Path(temp_dir.name)
        self.work_root = self.root / "work"
        self.spool = self.root / "spool"
        self.steps = ScriptedSteps()
        resolver = patch(
            "yggdrasil.core.engine.resolve_callable", return_value=self.steps.fn
        )
        resolver.start()
        self.addCleanup(resolver.stop)
        self.emitter = RecordingEmitter()
        self.engine = Engine(work_root=self.work_root, emitter=self.emitter)

    def spool_engine(self, **kwargs: Any) -> Engine:
        """An engine publishing to this test's spool."""
        return Engine(
            work_root=self.work_root, emitter=FileSpoolEmitter(self.spool), **kwargs
        )

    def run_attempt(
        self, plan: Plan, context: AttemptContext, engine: Engine | None = None
    ) -> BaseException | None:
        """Run one attempt through _run_attempt; return what it raised."""
        try:
            (engine or self.engine)._run_attempt(plan, context=context)
        except BaseException as exc:
            return exc
        return None

    def types(self) -> list[str]:
        """Event types recorded so far, in emission order."""
        return [str(event.get("type")) for event in self.emitter.events]

    def of_type(self, event_type: str) -> list[dict]:
        """Recorded events of one type."""
        return [e for e in self.emitter.events if e.get("type") == event_type]

    def plan_dir(self, plan: Plan) -> Path:
        """The plan's spool directory."""
        return self.spool / plan.realm / plan.plan_id

    def attempt_dir(self, plan: Plan, execution_id: str) -> Path:
        """One attempt's directory in this test's spool."""
        return attempt_dir(self.spool, plan.realm, plan.plan_id, execution_id)

    def spooled(self, path: Path) -> dict:
        """One spooled event."""
        return json.loads(path.read_text(encoding="utf-8"))

    def snapshot(self, plan: Plan) -> dict:
        """The plan's snapshot, built from the spool."""
        return build_plan_snapshot(self.plan_dir(plan), plan.realm, plan.plan_id)


class TestEventCorrelation(EngineEventsTestCase):
    """Every event of one attempt carries that attempt's identity, and only it."""

    def test_attempts_are_told_apart_by_every_event_type(self):
        # "stable" executes in the first attempt and is reused in the second;
        # "flaky" fails transiently in both. Between them the attempts publish
        # every kind of event, including both the engine emits directly.
        self.steps.fail("flaky", TransientStepError("cluster busy"))
        plan = plan_of(spec("stable"), spec("flaky"), spec("after", "flaky"))

        first = self.engine.run(plan)
        first_events = list(self.emitter.events)
        self.emitter.events.clear()
        second = self.engine.run(plan)
        second_events = list(self.emitter.events)

        assert first is not None and second is not None
        self.assertNotEqual(first.execution_id, second.execution_id)
        for report, events in ((first, first_events), (second, second_events)):
            with self.subTest(execution_id=report.execution_id):
                self.assertEqual(
                    {event["execution_id"] for event in events},
                    {report.execution_id},
                )
        types = {str(event["type"]) for event in second_events}
        self.assertLessEqual(
            {
                ATTEMPT_STARTED_EVENT,
                "step.skipped",
                "step.started",
                "step.failed",
                "step.retry_unimplemented",
                STEP_BLOCKED_EVENT,
                ATTEMPT_REPORT_EVENT,
            },
            types,
        )

    def test_captured_generation_and_token_are_on_every_event(self):
        self.steps.fail("b", PermanentStepError("broken"))
        plan = plan_of(spec("a"), spec("b"), spec("c", "b"))
        context = AttemptContext.for_plan(
            plan, execution_id="exec_captured", plan_generation="gen-1", run_token=3
        )

        self.assertIsNone(self.run_attempt(plan, context))

        self.assertGreater(len(self.emitter.events), 5)
        for event in self.emitter.events:
            with self.subTest(type=event["type"]):
                self.assertEqual(
                    {field: event[field] for field in CORRELATION_FIELDS},
                    {
                        "execution_id": "exec_captured",
                        "plan_generation": "gen-1",
                        "run_token": 3,
                    },
                )

    def test_direct_run_events_say_that_nothing_was_captured(self):
        report = self.engine.run(plan_of(spec("a")))

        assert report is not None
        for event in self.emitter.events:
            with self.subTest(type=event["type"]):
                self.assertEqual(event["execution_id"], report.execution_id)
                self.assertIsNone(event["plan_generation"])
                self.assertIsNone(event["run_token"])

    def test_a_payload_field_cannot_move_an_event_into_another_attempt(self):
        self.steps.behaviors["a"] = lambda ctx: ctx.emit(
            "step.echo", execution_id="forged", run_token=99
        )

        report = self.engine.run(plan_of(spec("a")))

        assert report is not None
        (echo,) = self.of_type("step.echo")
        self.assertEqual(echo["execution_id"], report.execution_id)
        self.assertIsNone(echo["run_token"])

    def test_step_context_and_its_data_access_carry_the_attempt(self):
        seen: list[tuple[Any, Any]] = []

        def inspect(ctx: StepContext) -> None:
            assert ctx.data is not None
            trace = ctx.data._trace_context
            assert trace is not None
            seen.append((ctx.correlation, trace.correlation))

        self.steps.behaviors["a"] = inspect
        plan = plan_of(spec("a"))
        context = AttemptContext.for_plan(
            plan, execution_id="exec_ctx", plan_generation="gen-2", run_token=1
        )

        self.assertIsNone(self.run_attempt(plan, context))

        self.assertEqual(seen, [(context.correlation, context.correlation)])

    def test_cache_skip_says_it_was_a_cache_hit(self):
        plan = plan_of(spec("a"))
        self.engine.run(plan)

        self.engine.run(plan)

        (skipped,) = self.of_type("step.skipped")
        self.assertEqual(skipped["reason"], "cache_hit")


class TestAttemptStartRecord(EngineEventsTestCase):
    """Every attempt is recorded before it does anything else."""

    def test_start_record_comes_first_and_describes_the_attempt(self):
        plan = plan_of(spec("a"), spec("b", "a"), policy=FAIL_FAST)
        context = AttemptContext.for_plan(
            plan,
            execution_id="exec_start",
            plan_generation="gen-3",
            run_token=2,
            execution_authority="run_once",
            execution_owner="owner-1",
        )

        self.assertIsNone(self.run_attempt(plan, context))

        self.assertEqual(self.types()[0], ATTEMPT_STARTED_EVENT)
        (started,) = self.of_type(ATTEMPT_STARTED_EVENT)
        self.assertEqual(
            {
                key: started[key]
                for key in (
                    "realm",
                    "scope",
                    "plan_id",
                    "execution_id",
                    "plan_generation",
                    "run_token",
                    "execution_authority",
                    "execution_owner",
                    "failure_policy",
                    "started_at",
                    "steps",
                )
            },
            {
                "realm": REALM,
                "scope": SCOPE,
                "plan_id": PLAN_ID,
                "execution_id": "exec_start",
                "plan_generation": "gen-3",
                "run_token": 2,
                "execution_authority": "run_once",
                "execution_owner": "owner-1",
                "failure_policy": FAIL_FAST,
                "started_at": context.report.started_at,
                "steps": [
                    {"step_id": "a", "step_name": "a", "deps": []},
                    {"step_id": "b", "step_name": "b", "deps": ["a"]},
                ],
            },
        )
        self.assertEqual(
            started["_spool_path"],
            attempt_spool_path(
                REALM, PLAN_ID, "exec_start", "plan_attempt_started.json"
            ),
        )

    def test_rejected_attempt_leaves_a_readable_start_record(self):
        engine = self.spool_engine()
        plan = plan_of(spec("a", "b"), spec("b", "a"))

        with self.assertRaises(PreflightValidationError):
            engine.run(plan)

        (attempt,) = (self.plan_dir(plan) / "attempts").iterdir()
        records = sorted(attempt.iterdir())
        self.assertEqual(
            [p.name for p in records],
            ["plan_attempt_report.json", "plan_attempt_started.json"],
        )
        by_type = {self.spooled(p)["type"]: self.spooled(p) for p in records}
        started = by_type[ATTEMPT_STARTED_EVENT]
        self.assertEqual([entry["step_id"] for entry in started["steps"]], ["a", "b"])
        self.assertEqual(
            by_type[ATTEMPT_REPORT_EVENT]["report"]["termination_reason"],
            TerminationReason.PREFLIGHT_REJECTED.value,
        )
        self.assertEqual(
            by_type[ATTEMPT_REPORT_EVENT]["execution_id"], started["execution_id"]
        )
        self.assertEqual(attempt.name, started["execution_id"])
        # No step ran: no step events at all, and no work directory.
        self.assertFalse((self.work_root / PLAN_ID).exists())
        self.assertEqual(self.steps.calls, [])

    def test_attempt_without_steps_is_recorded_too(self):
        report = self.engine.run(plan_of())

        assert report is not None
        self.assertEqual(self.types(), [ATTEMPT_STARTED_EVENT, ATTEMPT_REPORT_EVENT])
        self.assertEqual(self.of_type(ATTEMPT_STARTED_EVENT)[0]["steps"], [])
        self.assertIs(report.termination_reason, TerminationReason.COMPLETED)

    def test_failed_start_publication_runs_and_writes_nothing(self):
        self.emitter.fail_on = {ATTEMPT_STARTED_EVENT}
        plan = plan_of(spec("a"))
        context = AttemptContext.for_plan(plan, execution_id="exec_unrecorded")

        exc = self.run_attempt(plan, context)

        self.assertIsInstance(exc, EventPublicationError)
        self.assertEqual(self.steps.calls, [])
        self.assertFalse((self.work_root / PLAN_ID).exists())
        report = context.report
        self.assertIs(report.termination_reason, TerminationReason.ORCHESTRATION_ERROR)
        self.assertEqual(report.unreached_step_ids, ["a"])
        # The report is not pushed through the emitter that just failed.
        self.assertEqual(self.types(), [ATTEMPT_STARTED_EVENT])
        assert report.publication_failure is not None
        self.assertEqual(
            report.publication_failure.details["publication_skipped"], True
        )

    def test_execution_id_that_cannot_name_records_is_refused(self):
        plan = plan_of(spec("a"))
        for execution_id in ("", "../elsewhere", "a/b"):
            with self.subTest(execution_id=execution_id):
                context = AttemptContext.for_plan(plan, execution_id=execution_id)

                exc = self.run_attempt(plan, context)

                self.assertIsInstance(exc, OrchestrationError)
                self.assertIn("cannot name spool records", str(exc))
                self.assertFalse(context.report.is_finished)
        self.assertEqual(self.emitter.events, [])
        self.assertEqual(self.steps.calls, [])


class TestBlockedStepEvents(EngineEventsTestCase):
    """A blocked step is published once, as soon as it is blocked."""

    def test_blocked_step_is_published_before_the_next_step_starts(self):
        self.steps.fail("a", PermanentStepError("broken"))

        self.engine.run(plan_of(spec("a"), spec("b", "a"), spec("c")))

        lifecycle = [
            (e["type"], e.get("step_id"))
            for e in self.emitter.events
            if e["type"] not in (ATTEMPT_STARTED_EVENT, ATTEMPT_REPORT_EVENT)
        ]
        self.assertEqual(
            lifecycle,
            [
                ("step.started", "a"),
                ("step.failed", "a"),
                (STEP_BLOCKED_EVENT, "b"),
                ("step.started", "c"),
                ("step.succeeded", "c"),
            ],
        )

    def test_blocked_chain_names_the_failure_at_its_root(self):
        self.steps.fail("a", PermanentStepError("broken"))

        self.engine.run(plan_of(spec("a"), spec("b", "a"), spec("c", "b")))

        blocked = {e["step_id"]: e for e in self.of_type(STEP_BLOCKED_EVENT)}
        self.assertEqual(set(blocked), {"b", "c"})
        self.assertEqual(blocked["b"]["direct_blockers"], ["a"])
        self.assertEqual(blocked["b"]["failed_ancestors"], ["a"])
        self.assertEqual(blocked["c"]["direct_blockers"], ["b"])
        self.assertEqual(blocked["c"]["failed_ancestors"], ["a"])
        self.assertEqual(blocked["c"]["step_name"], "c")
        self.assertNotIn("run_id", blocked["c"]["_spool_path"])

    def test_join_is_shown_blocked_at_once_and_its_diagnostics_settle_later(self):
        # A and B are independent prerequisites of join J. A fails first; B is
        # held running, then fails too.
        engine = self.spool_engine()
        self.steps.fail("A", PermanentStepError("lane A broke"))
        gate = Gate()

        def hold_then_fail(ctx: StepContext) -> None:
            gate.pass_through()
            raise PermanentStepError("lane B broke")

        self.steps.behaviors["B"] = hold_then_fail
        plan = plan_of(spec("A"), spec("B"), spec("J", "A", "B"))
        outcome: dict[str, Any] = {}

        def run() -> None:
            try:
                outcome["report"] = engine.run(plan)
            except BaseException as exc:  # surfaced by the assertions below
                outcome["error"] = exc

        worker = threading.Thread(target=run)
        worker.start()
        self.addCleanup(worker.join, WAIT)
        self.addCleanup(gate.release)
        self.assertTrue(gate.entered.wait(WAIT))

        # B is still running, and J already appears blocked by A.
        running = self.snapshot(plan)
        self.assertEqual(running["attempt"]["state"], "running")
        self.assertEqual(running["steps"]["B"]["state"], "step.started")
        self.assertEqual(running["steps"]["J"]["state"], STEP_BLOCKED_EVENT)
        self.assertEqual(running["steps"]["J"]["outcome"], "blocked")
        self.assertEqual(running["steps"]["J"]["direct_blockers"], ["A"])
        self.assertEqual(running["steps"]["J"]["failed_ancestors"], ["A"])

        gate.release()
        worker.join(WAIT)
        self.assertFalse(worker.is_alive())
        self.assertNotIn("error", outcome)
        report: AttemptReport = outcome["report"]

        # One step.blocked for J, opening J's event stream.
        attempt = self.attempt_dir(plan, report.execution_id)
        join_dir = attempt / "steps" / "J"
        blocked_name = "0001_step_blocked.json"
        self.assertEqual([p.name for p in join_dir.iterdir()], [blocked_name])
        blocked = self.spooled(join_dir / blocked_name)
        self.assertEqual(blocked["direct_blockers"], ["A"])
        self.assertEqual(blocked["seq"], 1)
        self.assertNotIn("run_id", blocked)
        self.assertFalse((self.work_root / PLAN_ID / "J").exists())

        # The report and the snapshot name both failures.
        self.assertEqual(report.direct_blockers["J"], ["A", "B"])
        self.assertEqual(report.failed_ancestors["J"], ["A", "B"])
        finished = self.snapshot(plan)
        self.assertEqual(finished["attempt"]["state"], "finished")
        self.assertEqual(finished["steps"]["J"]["direct_blockers"], ["A", "B"])
        self.assertEqual(finished["steps"]["J"]["failed_ancestors"], ["A", "B"])

        # Delivering J's earlier event again, late, narrows neither list.
        shutil.copy(join_dir / blocked_name, join_dir / "zz_redelivered.json")
        (join_dir / blocked_name).write_text(json.dumps(blocked), encoding="utf-8")
        replayed = self.snapshot(plan)
        self.assertEqual(replayed["steps"]["J"]["direct_blockers"], ["A", "B"])
        self.assertEqual(replayed["steps"]["J"]["failed_ancestors"], ["A", "B"])

        # Exactly one report, and it is what run() returned.
        reports = [
            self.spooled(p)
            for p in self.plan_dir(plan).rglob("*.json")
            if self.spooled(p)["type"] == ATTEMPT_REPORT_EVENT
        ]
        self.assertEqual(len(reports), 1)
        self.assertEqual(reports[0]["report"], report.to_dict())

    def test_cancellation_after_blocking_keeps_the_blockers(self):
        plan = plan_of(spec("a"), spec("b", "a"), spec("c"), spec("d"))
        context = AttemptContext.for_plan(plan, execution_id="exec_cancelled")
        self.steps.fail("a", PermanentStepError("broken"))
        self.steps.behaviors["c"] = lambda ctx: context.request_cancellation()

        exc = self.run_attempt(plan, context)

        self.assertIsInstance(exc, AttemptCancelledError)
        report = context.report
        self.assertIs(report.termination_reason, TerminationReason.CANCELLED)
        self.assertIs(report.step_outcomes["b"], StepOutcome.BLOCKED)
        self.assertEqual(report.direct_blockers["b"], ["a"])
        self.assertEqual(report.failed_ancestors["b"], ["a"])
        # d had no failed prerequisite: it has no outcome, not a blocked one.
        self.assertEqual(report.unreached_step_ids, ["d"])
        self.assertEqual(
            [e["step_id"] for e in self.of_type(STEP_BLOCKED_EVENT)], ["b"]
        )

    def test_failure_to_publish_a_block_aborts_the_attempt(self):
        self.steps.fail("a", PermanentStepError("lane broke"))
        self.emitter.fail_on = {STEP_BLOCKED_EVENT}
        plan = plan_of(spec("a"), spec("b", "a"), spec("c"))
        context = AttemptContext.for_plan(plan, execution_id="exec_unobserved")

        exc = self.run_attempt(plan, context)

        self.assertIsInstance(exc, EventPublicationError)
        self.assertEqual(self.steps.calls, ["a"], "work continued unobservably")
        report = context.report
        self.assertIs(report.termination_reason, TerminationReason.ORCHESTRATION_ERROR)
        self.assertIs(report.step_outcomes["a"], StepOutcome.FAILED)
        self.assertEqual(report.unreached_step_ids, ["c"])
        # The step failure that led to the block stays reachable.
        assert exc is not None
        self.assertTrue(
            any(isinstance(e, PermanentStepError) for e in exception_chain(exc)),
            exception_chain(exc),
        )
        self.assertNotIn(ATTEMPT_REPORT_EVENT, self.types())

    def test_fail_fast_blocks_nothing(self):
        self.steps.fail("a", PermanentStepError("broken"))

        with self.assertRaises(PermanentStepError):
            self.engine.run(plan_of(spec("a"), spec("b", "a"), policy=FAIL_FAST))

        self.assertEqual(self.of_type(STEP_BLOCKED_EVENT), [])


class TestAttemptReportRecord(EngineEventsTestCase):
    """One report per attempt, filed under the attempt's own name."""

    def test_report_is_filed_under_the_attempt_with_its_correlation(self):
        engine = self.spool_engine()
        self.steps.fail("a", PermanentStepError("broken"))

        report = engine.run(plan_of(spec("a"), spec("b")))

        assert report is not None
        path = self.attempt_dir(plan_of(), report.execution_id) / record_filename(
            ATTEMPT_REPORT_EVENT
        )
        published = self.spooled(path)
        self.assertEqual(published["report"], report.to_dict())
        self.assertEqual(
            {field: published[field] for field in CORRELATION_FIELDS},
            report.correlation.event_fields(),
        )


class TestExecutionIdSource(EngineEventsTestCase):
    """The engine orders attempts against the spool it writes, and no other."""

    def far_future_record_in(self, spool: Path) -> None:
        """Leave an attempt at the plan far in the future, in a spool."""
        (attempts_dir(spool, REALM, PLAN_ID) / FAR_FUTURE_ID).mkdir(parents=True)

    def test_default_allocator_and_reserver_use_the_emitters_spool(self):
        self.far_future_record_in(self.spool)
        engine = self.spool_engine()

        report = engine.run(plan_of(spec("a")))

        assert report is not None
        history = engine.execution_ids.history
        assert isinstance(history, SpoolAttemptHistory)
        self.assertEqual(history.root, self.spool)
        reserver = engine.attempt_reserver
        assert isinstance(reserver, SpoolAttemptDirectories)
        self.assertEqual(reserver.root, self.spool)
        self.assertGreater(report.execution_id, FAR_FUTURE_ID)

    def test_restarted_engine_with_its_clock_behind_still_orders_after(self):
        def allocator(clock: Clock) -> ExecutionIdAllocator:
            return ExecutionIdAllocator(SpoolAttemptHistory(self.spool), clock=clock)

        plan = plan_of(spec("a"))
        before = self.spool_engine(execution_ids=allocator(Clock(T0)))
        first = before.run(plan)

        after = self.spool_engine(
            execution_ids=allocator(Clock(T0 - timedelta(hours=1)))
        )
        second = after.run(plan)

        assert first is not None and second is not None
        self.assertGreater(second.execution_id, first.execution_id)
        self.assertEqual(
            self.snapshot(plan)["attempt"]["execution_id"], second.execution_id
        )

    def test_mocked_emitter_reaches_no_spool(self):
        # An unrelated spool the environment points at, holding a record that
        # would push this plan's IDs into the future if it were read.
        default_spool = self.root / "default_spool"
        self.far_future_record_in(default_spool)
        emitter = Mock(spec=EventEmitter)

        with (
            patch.dict(os.environ, {"YGG_EVENT_SPOOL": str(default_spool)}),
            patch(
                "yggdrasil.flow.events.emitter.resolve_event_spool",
                side_effect=AssertionError("resolved a default spool"),
            ),
            patch.object(
                SpoolAttemptHistory,
                "recorded_execution_ids",
                side_effect=AssertionError("read a spool"),
            ),
            patch.object(
                SpoolAttemptDirectories,
                "reserve",
                side_effect=AssertionError("reserved in a spool"),
            ),
        ):
            engine = Engine(work_root=self.work_root, emitter=emitter)
            report = engine.run(plan_of(spec("a")))

        assert report is not None
        self.assertIsNone(engine.execution_ids.history)
        self.assertIsNone(engine.attempt_reserver)
        # Nothing was reserved in the spool the environment points at.
        self.assertEqual(
            [p.name for p in attempts_dir(default_spool, REALM, PLAN_ID).iterdir()],
            [FAR_FUTURE_ID],
        )
        self.assertLess(report.execution_id, FAR_FUTURE_ID)
        self.assertEqual(
            {call.args[0]["execution_id"] for call in emitter.emit.call_args_list},
            {report.execution_id},
        )

    def test_unreadable_history_starts_no_attempt(self):
        class Unreadable:
            def recorded_execution_ids(self, realm: str, plan_id: str) -> list[str]:
                raise PermissionError("spool unreadable")

        engine = Engine(
            work_root=self.work_root,
            emitter=self.emitter,
            execution_ids=ExecutionIdAllocator(Unreadable()),
        )

        with self.assertRaises(OrchestrationError) as cm:
            engine.run(plan_of(spec("a")))

        self.assertIn("Allocating an execution ID", str(cm.exception))
        self.assertIsInstance(cm.exception.__cause__, PermissionError)
        self.assertEqual(self.emitter.events, [])
        self.assertEqual(self.steps.calls, [])

    def test_attempts_sharing_an_engine_share_its_order(self):
        engine = Engine(
            work_root=self.work_root,
            emitter=self.emitter,
            execution_ids=ExecutionIdAllocator(clock=Clock(T0)),
        )

        first = engine.run(plan_of(spec("a"), plan_id="plan_one"))
        second = engine.run(plan_of(spec("a"), plan_id="plan_two"))

        assert first is not None and second is not None
        self.assertEqual(execution_timestamp(first.execution_id), T0)
        self.assertEqual(
            execution_timestamp(second.execution_id), T0 + timedelta(microseconds=1)
        )


class HistoryWithoutReserve:
    """A history that can be read but cannot reserve anything."""

    def recorded_execution_ids(self, realm: str, plan_id: str) -> list[str]:
        return []


class ScriptedReserver:
    """A reserver answering from a script, recording what it was asked."""

    def __init__(self, *answers: bool | Exception) -> None:
        self.answers = list(answers)
        self.asked: list[str] = []

    def reserve(self, realm: str, plan_id: str, execution_id: str) -> bool:
        self.asked.append(execution_id)
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer


class TestReservation(EngineEventsTestCase):
    """Every attempt's ID is reserved exactly once before it publishes anything."""

    def test_injected_allocator_still_reserves_in_the_emitters_spool(self):
        # The allocator sees no history, so it proposes an ID that is taken;
        # the engine's own reserver, on the emitter's spool, refuses it.
        taken = format_execution_id(T0, "c68e")
        for name, history in (("none", None), ("read-only", HistoryWithoutReserve())):
            with self.subTest(history=name):
                plan_id = f"{PLAN_ID}_{name}"
                existing = attempt_dir(self.spool, REALM, plan_id, taken)
                existing.mkdir(parents=True)
                (existing / "plan_attempt_report.json").write_text("earlier")
                engine = self.spool_engine(
                    execution_ids=ExecutionIdAllocator(
                        history, clock=Clock(T0), new_suffix=lambda: "c68e"
                    )
                )

                report = engine.run(plan_of(spec("a"), plan_id=plan_id))

                assert report is not None
                assert isinstance(engine.attempt_reserver, SpoolAttemptDirectories)
                self.assertEqual(engine.attempt_reserver.root, self.spool)
                self.assertEqual(
                    report.execution_id,
                    format_execution_id(T0 + timedelta(microseconds=1), "c68e"),
                )
                self.assertEqual(
                    (existing / "plan_attempt_report.json").read_text(), "earlier"
                )
                self.assertEqual(
                    [p.name for p in existing.iterdir()], ["plan_attempt_report.json"]
                )

    def test_run_reserves_once_and_says_so(self):
        reserver = ScriptedReserver(True)
        engine = Engine(
            work_root=self.work_root, emitter=self.emitter, attempt_reserver=reserver
        )

        with patch.object(engine, "_run_attempt", wraps=engine._run_attempt) as run:
            report = engine.run(plan_of(spec("a")))

        assert report is not None
        self.assertEqual(reserver.asked, [report.execution_id])
        self.assertTrue(run.call_args.kwargs["context"].execution_id_reserved)

    def test_exhausted_reservation_starts_nothing(self):
        reserver = ScriptedReserver(False, False, False)
        engine = Engine(
            work_root=self.work_root, emitter=self.emitter, attempt_reserver=reserver
        )

        with self.assertRaises(OrchestrationError) as caught:
            engine.run(plan_of(spec("a")))

        self.assertIn("Allocating an execution ID", str(caught.exception))
        self.assertIsInstance(caught.exception.__cause__, ReservationExhaustedError)
        self.assertEqual(len(reserver.asked), 3)
        self.assertEqual(self.emitter.events, [])
        self.assertEqual(self.steps.calls, [])
        self.assertFalse((self.work_root / PLAN_ID).exists())

    def test_caller_built_id_is_reserved_when_its_attempt_starts(self):
        engine = self.spool_engine()
        plan = plan_of(spec("a"))
        context = AttemptContext.for_plan(plan, execution_id="exec_fixed")
        self.assertFalse(context.execution_id_reserved)

        self.assertIsNone(self.run_attempt(plan, context, engine))

        self.assertTrue(context.execution_id_reserved)
        self.assertEqual(
            sorted(p.name for p in self.attempt_dir(plan, "exec_fixed").iterdir()),
            ["plan_attempt_report.json", "plan_attempt_started.json", "steps"],
        )

    def test_taken_caller_built_id_is_refused_not_renamed_or_overwritten(self):
        engine = self.spool_engine()
        plan = plan_of(spec("a"))
        existing = self.attempt_dir(plan, "exec_fixed")
        existing.mkdir(parents=True)
        (existing / "plan_attempt_report.json").write_text("the earlier attempt's")
        context = AttemptContext.for_plan(plan, execution_id="exec_fixed")

        exc = self.run_attempt(plan, context, engine)

        self.assertIsInstance(exc, OrchestrationError)
        self.assertIn("already taken", str(exc))
        self.assertFalse(context.execution_id_reserved)
        self.assertFalse(context.report.is_finished)
        self.assertEqual(self.steps.calls, [])
        self.assertEqual(
            [p.name for p in (self.plan_dir(plan) / "attempts").iterdir()],
            ["exec_fixed"],
        )
        self.assertEqual(
            (existing / "plan_attempt_report.json").read_text(),
            "the earlier attempt's",
        )
        self.assertEqual(
            [p.name for p in existing.iterdir()], ["plan_attempt_report.json"]
        )

    def test_failing_to_reserve_a_caller_built_id_runs_nothing(self):
        engine = Engine(
            work_root=self.work_root,
            emitter=self.emitter,
            attempt_reserver=ScriptedReserver(PermissionError("spool read-only")),
        )
        plan = plan_of(spec("a"))
        context = AttemptContext.for_plan(plan, execution_id="exec_fixed")

        exc = self.run_attempt(plan, context, engine)

        self.assertIsInstance(exc, OrchestrationError)
        assert exc is not None
        self.assertIsInstance(exc.__cause__, PermissionError)
        self.assertFalse(context.execution_id_reserved)
        self.assertFalse(context.report.is_finished)
        self.assertEqual(self.emitter.events, [])
        self.assertEqual(self.steps.calls, [])

    def test_already_reserved_context_is_not_reserved_again(self):
        reserver = ScriptedReserver()  # any call would fail: nothing scripted
        engine = Engine(
            work_root=self.work_root, emitter=self.emitter, attempt_reserver=reserver
        )
        plan = plan_of(spec("a"))
        context = AttemptContext.for_plan(
            plan, execution_id="exec_fixed", execution_id_reserved=True
        )

        self.assertIsNone(self.run_attempt(plan, context, engine))

        self.assertEqual(reserver.asked, [])

    def test_without_a_reserver_nothing_is_claimed(self):
        plan = plan_of(spec("a"))
        context = AttemptContext.for_plan(plan, execution_id="exec_fixed")

        self.assertIsNone(self.run_attempt(plan, context))

        self.assertIsNone(self.engine.attempt_reserver)
        self.assertFalse(context.execution_id_reserved)


class TestAttemptFileLayout(EngineEventsTestCase):
    """What each kind of attempt leaves in the spool, file by file."""

    def tree(self, plan: Plan, execution_id: str) -> list[str]:
        """Every file in an attempt's directory, relative to it."""
        attempt = self.attempt_dir(plan, execution_id)
        return sorted(
            str(p.relative_to(attempt)) for p in attempt.rglob("*") if p.is_file()
        )

    def stream(self, plan: Plan, execution_id: str, step_id: str) -> list[dict]:
        """One step's events in an attempt, in file-name order."""
        directory = step_events_dir(self.attempt_dir(plan, execution_id), step_id)
        return [
            self.spooled(p) for p in sorted(directory.glob("*.json")) if p.is_file()
        ]

    def assert_numbered(self, events: list[dict], names: list[str]) -> None:
        """Assert a step's events are numbered 1..n, as their names say."""
        self.assertEqual([e["seq"] for e in events], list(range(1, len(events) + 1)))
        self.assertEqual(
            [int(n.split("_", 1)[0]) for n in names], [e["seq"] for e in events]
        )

    def test_executed_reused_and_blocked_steps(self):
        engine = self.spool_engine()
        first = engine.run(plan_of(spec("a")))
        self.steps.fail("b", PermanentStepError("broken"))
        self.steps.behaviors["d"] = lambda ctx: ctx.progress(50)
        plan = plan_of(spec("a"), spec("b"), spec("c", "b"), spec("d"))

        report = engine.run(plan)

        assert first is not None and report is not None
        self.assertEqual(
            self.tree(plan, report.execution_id),
            [
                "plan_attempt_report.json",
                "plan_attempt_started.json",
                "steps/a/0001_step_skipped.json",
                "steps/b/0001_step_started.json",
                "steps/b/0002_step_failed.json",
                "steps/c/0001_step_blocked.json",
                "steps/d/0001_step_started.json",
                "steps/d/0002_step_progress.json",
                "steps/d/0003_step_succeeded.json",
            ],
        )
        # The first attempt keeps its own directory, untouched.
        self.assertEqual(
            self.tree(plan, first.execution_id),
            [
                "plan_attempt_report.json",
                "plan_attempt_started.json",
                "steps/a/0001_step_started.json",
                "steps/a/0002_step_succeeded.json",
            ],
        )
        streams = {s: self.stream(plan, report.execution_id, s) for s in "abcd"}
        for step_id, events in streams.items():
            with self.subTest(step=step_id):
                names = sorted(
                    p.name
                    for p in (
                        self.attempt_dir(plan, report.execution_id) / "steps" / step_id
                    ).glob("*.json")
                )
                self.assert_numbered(events, names)
                self.assertEqual(
                    {e["execution_id"] for e in events}, {report.execution_id}
                )
        # Every evaluation carries its own run ID; a blocked step has none.
        (skipped,) = streams["a"]
        self.assertTrue(skipped["run_id"].startswith("run_"))
        self.assertEqual(len({e["run_id"] for e in streams["b"]}), 1)
        self.assertEqual(len({e["run_id"] for e in streams["d"]}), 1)
        self.assertNotEqual(streams["b"][0]["run_id"], streams["d"][0]["run_id"])
        self.assertNotIn("run_id", streams["c"][0])
        # Work directories and markers stay where they always were.
        self.assertTrue(
            (self.work_root / PLAN_ID / "a" / "success.fingerprint").exists()
        )
        self.assertTrue(
            (self.work_root / PLAN_ID / "d" / "success.fingerprint").exists()
        )

    def test_retry_diagnostic_continues_the_failed_steps_stream(self):
        engine = self.spool_engine()
        self.steps.fail("a", TransientStepError("cluster busy"))
        plan = plan_of(spec("a"))

        report = engine.run(plan)

        assert report is not None
        events = self.stream(plan, report.execution_id, "a")
        self.assertEqual(
            [e["type"] for e in events],
            ["step.started", "step.failed", "step.retry_unimplemented"],
        )
        self.assertEqual([e["seq"] for e in events], [1, 2, 3])
        self.assertEqual(len({e["run_id"] for e in events}), 1)
        self.assertIn(
            "steps/a/0003_step_retry_unimplemented.json",
            self.tree(plan, report.execution_id),
        )

    def test_data_access_traces_join_the_steps_stream(self):
        from yggdrasil.flow.data_access.couchdb_data import CouchDBExecutionClient

        def write_and_trace(ctx: StepContext) -> None:
            ctx.progress(10)
            assert ctx.data is not None
            client = CouchDBExecutionClient(
                Mock(),
                Mock(),
                permissions=frozenset({"read", "write"}),
                realm_id=REALM,
                connection_name="conn",
                resource="db",
                trace_context=ctx.data._trace_context,
            )
            client._emit("data_access.write.succeeded", doc_id="doc_1")

        engine = self.spool_engine()
        self.steps.behaviors["a"] = write_and_trace
        plan = plan_of(spec("a"))

        report = engine.run(plan)

        assert report is not None
        self.assertEqual(
            [p for p in self.tree(plan, report.execution_id) if p.startswith("steps/")],
            [
                "steps/a/0001_step_started.json",
                "steps/a/0002_step_progress.json",
                "steps/a/0003_data_access_write_succeeded.json",
                "steps/a/0004_step_succeeded.json",
            ],
        )
        events = self.stream(plan, report.execution_id, "a")
        trace = events[2]
        self.assertEqual(trace["seq"], 3)
        self.assertEqual(trace["execution_id"], report.execution_id)
        self.assertEqual(trace["run_id"], events[0]["run_id"])

    def test_attempt_without_steps_leaves_only_its_records(self):
        engine = self.spool_engine()
        plan = plan_of()

        report = engine.run(plan)

        assert report is not None
        self.assertEqual(
            self.tree(plan, report.execution_id),
            ["plan_attempt_report.json", "plan_attempt_started.json"],
        )

    def test_cancelled_attempt_leaves_only_what_it_reached(self):
        engine = self.spool_engine()
        plan = plan_of(spec("a"), spec("b"), spec("c"))
        execution_id, reserved = engine.reserve_execution_id(plan)
        context = AttemptContext.for_plan(
            plan, execution_id=execution_id, execution_id_reserved=reserved
        )
        self.steps.behaviors["b"] = lambda ctx: context.request_cancellation()

        self.assertIsInstance(
            self.run_attempt(plan, context, engine), AttemptCancelledError
        )

        self.assertEqual(
            self.tree(plan, execution_id),
            [
                "plan_attempt_report.json",
                "plan_attempt_started.json",
                "steps/a/0001_step_started.json",
                "steps/a/0002_step_succeeded.json",
                "steps/b/0001_step_started.json",
                "steps/b/0002_step_succeeded.json",
            ],
        )

    def test_numbers_past_four_digits_are_ordered_by_number(self):
        def many_events(ctx: StepContext) -> None:
            ctx._seq = 9998  # as if 9998 events had been published already
            ctx.progress(99)

        engine = self.spool_engine()
        self.steps.behaviors["a"] = many_events
        plan = plan_of(spec("a"))

        report = engine.run(plan)

        assert report is not None
        self.assertIn(
            "steps/a/9999_step_progress.json", self.tree(plan, report.execution_id)
        )
        self.assertIn(
            "steps/a/10000_step_succeeded.json", self.tree(plan, report.execution_id)
        )
        # By name, the progress event sorts last; by number, the success does.
        entry = self.snapshot(plan)["steps"]["a"]
        self.assertEqual((entry["state"], entry["progress"]), ("step.succeeded", 100))

    def test_step_ids_outside_the_builder_pattern_keep_their_streams_apart(self):
        # Manually built steps may use IDs PlanBuilder would not produce,
        # including one nested under another's.
        engine = self.spool_engine()
        self.steps.fail("lane_1", PermanentStepError("lane 1 broke"))
        plan = plan_of(spec("lane_1"), spec("lane_1/process", "lane_1"), spec("1.lane"))

        report = engine.run(plan)

        assert report is not None
        self.assertEqual(
            self.tree(plan, report.execution_id),
            [
                "plan_attempt_report.json",
                "plan_attempt_started.json",
                "steps/1.lane/0001_step_started.json",
                "steps/1.lane/0002_step_succeeded.json",
                "steps/lane_1/0001_step_started.json",
                "steps/lane_1/0002_step_failed.json",
                "steps/lane_1/process/0001_step_blocked.json",
            ],
        )
        snapshot = self.snapshot(plan)
        self.assertEqual(
            list(snapshot["steps"]), ["lane_1", "lane_1/process", "1.lane"]
        )
        states = {
            step_id: (entry["state"], entry["outcome"])
            for step_id, entry in snapshot["steps"].items()
        }
        self.assertEqual(
            states,
            {
                "lane_1": ("step.failed", "failed"),
                "lane_1/process": (STEP_BLOCKED_EVENT, "blocked"),
                "1.lane": ("step.succeeded", "succeeded"),
            },
        )
        self.assertIsNone(snapshot["steps"]["lane_1/process"]["run_id"])
        self.assertEqual(
            snapshot["steps"]["lane_1"]["run_id"],
            self.stream(plan, report.execution_id, "lane_1")[0]["run_id"],
        )

    def test_step_ids_that_would_collide_as_paths_keep_separate_streams(self):
        # A step ID can name another step's event file, or its temporary file,
        # in any letter case: a case-insensitive filesystem reads the upper-case
        # name as the lower-case file. Whichever step publishes first, both run
        # and keep streams of their own. (IDs a path would normalize onto
        # another step's are rejected by preflight; see
        # tests/test_core_engine_preflight.py.)
        engine = self.spool_engine()
        groups = [
            ["lane", "lane/0001_step_started.json", "lane/0001_step_started.json.tmp"],
            ["lane", "lane/0001_step_started.JSON", "lane/0001_step_started.Json.Tmp"],
        ]
        for index, group in enumerate(groups):
            for order in (group, group[::-1]):
                with self.subTest(order=order):
                    plan = plan_of(
                        *(spec(s) for s in order),
                        plan_id=f"{PLAN_ID}_{index}_{order == group}",
                    )

                    report = engine.run(plan)

                    assert report is not None
                    self.assertEqual(report.counts["succeeded"], len(order))
                    snapshot = self.snapshot(plan)
                    self.assertEqual(list(snapshot["steps"]), order)
                    run_ids = set()
                    for step_id in order:
                        events = self.stream(plan, report.execution_id, step_id)
                        self.assertEqual(
                            [e["type"] for e in events],
                            ["step.started", "step.succeeded"],
                        )
                        self.assertEqual({e["step_id"] for e in events}, {step_id})
                        entry = snapshot["steps"][step_id]
                        self.assertEqual(entry["state"], "step.succeeded")
                        self.assertEqual(entry["run_id"], events[0]["run_id"])
                        run_ids.add(entry["run_id"])
                    self.assertEqual(len(run_ids), len(order))


if __name__ == "__main__":
    unittest.main()
