"""Shared fixtures for plan-execution coordination tests.

Attempts run through a real Engine and real ``@step`` functions whose behavior
is scripted per step, against a real plan store: SQLite on a temporary file,
or ``PlanDBManager`` on an in-memory client that enforces CouchDB's revision
rules. The engine and the store are wrapped only to observe calls and to pause
or fail them at chosen points. The logic under test is never replaced.

Cross-thread coordination uses ``threading.Event`` handshakes (:class:`Gate`),
never sleeps. ``WAIT`` is only an upper bound that keeps a broken test from
hanging, and :class:`Watchdog` fails a test whose event loop stops running.
"""

from __future__ import annotations

import asyncio
import logging
import signal
import threading
import unittest
from collections import Counter
from collections.abc import Callable, Coroutine, Iterator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, TypeVar
from unittest.mock import patch

from tests.plan_store_support import (
    FakeCouchServer,
    patch_api_exception,
    plan_db_manager_on,
)
from yggdrasil.core.engine import Engine
from yggdrasil.daemon.plan_execution import (
    DAEMON_CLAIM,
    ExecutionClaim,
    ExecutionResult,
    ExecutionStatus,
    PlanExecutionCoordinator,
)
from yggdrasil.flow.attempt import AttemptContext
from yggdrasil.flow.events.emitter import EventEmitter, FileSpoolEmitter
from yggdrasil.flow.model import FAIL_FAST_POLICY, Plan, StepResult, StepSpec
from yggdrasil.flow.outcomes import AttemptReport
from yggdrasil.flow.step import StepContext, step
from yggdrasil.storage.errors import PlanStoreError
from yggdrasil.storage.plan_updates import (
    ExecutionFinalization,
    FinalizationResult,
    FinalizationStatus,
)
from yggdrasil.storage.protocols import PlanStore
from yggdrasil.storage.sqlite import SQLiteInternalStore, SQLitePlanStore

# Upper bound for cross-thread handshakes. Never slept on.
WAIT = 5.0

# Upper bound for one scenario on an event loop, so a regression that leaves a
# result unresolved fails the test instead of hanging the suite.
SCENARIO_LIMIT = 30.0

# How much longer the watchdog waits than a scenario's own limit: it is only
# for a loop too stuck for that limit to fire.
WATCHDOG_GRACE = 5.0

# Every step resolves to the same scripted callable; the reference is nominal.
STEP_REF = "tests.execution:scripted"

PLAN_ID = "pln_exec_P1_v1"
REALM = "test_realm"
SCOPE = {"kind": "project", "id": "P1"}

COORDINATOR_LOGGER = "yggdrasil.daemon.plan_execution.PlanExecutionCoordinator"

# One transient backend failure, as the stores raise it.
RETRYABLE = PlanStoreError("database is locked", retryable=True)

T = TypeVar("T")


class Clock:
    """A clock for execution-ID allocators that stays where a test puts it.

    Attributes:
        now: The time it returns; timezone-aware.
    """

    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


class EventLoopStalled(Exception):
    """Raised by a :class:`Watchdog` into a main thread that stopped progressing."""


class Watchdog:
    """Fails a test whose main thread stops making progress, from outside it.

    An asyncio timeout fires only when the event loop regains control, which a
    coroutine spinning without yielding never gives back. A real-time interval
    timer does not depend on the loop: SIGALRM interrupts the main thread
    whatever it is doing and raises :class:`EventLoopStalled` there. Because
    code under test may swallow that exception, the watchdog also records that
    it fired, and callers assert on :attr:`expired`.

    A no-op off the main thread and where interval timers do not exist.

    Attributes:
        limit: Seconds before the watchdog fires.
        expired: Whether it fired.
    """

    def __init__(self, limit: float) -> None:
        self.limit = limit
        self.expired = False
        self._armed = False
        self._previous: Any = None

    def __enter__(self) -> Watchdog:
        self._armed = (
            hasattr(signal, "setitimer")
            and threading.current_thread() is threading.main_thread()
        )
        if self._armed:
            self._previous = signal.signal(signal.SIGALRM, self._expire)
            signal.setitimer(signal.ITIMER_REAL, self.limit)
        return self

    def __exit__(self, *exc_info: object) -> None:
        if self._armed:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, self._previous)

    def _expire(self, signum: int, frame: object) -> None:
        self.expired = True
        raise EventLoopStalled(f"no progress within {self.limit}s")


def run_bounded(
    scenario: Coroutine[Any, Any, T], *, limit: float = SCENARIO_LIMIT
) -> T:
    """Run a scenario on a fresh event loop, failing instead of hanging.

    ``asyncio.wait_for`` fails a scenario whose awaited result never arrives;
    a :class:`Watchdog`, a little later, fails one whose event loop stopped
    running altogether.

    Args:
        scenario: The coroutine to run.
        limit: Seconds the scenario may take.

    Returns:
        T: What the scenario returned.

    Raises:
        TimeoutError: If the scenario did not finish within limit.
        AssertionError: If the event loop stopped making progress.
    """
    with Watchdog(limit + WATCHDOG_GRACE) as watchdog:
        result = asyncio.run(asyncio.wait_for(scenario, limit))
    if watchdog.expired:
        raise AssertionError("the event loop stopped making progress")
    return result


class Gate:
    """A pause point that one thread reaches and another releases.

    Attributes:
        entered: Set when a thread reaches the gate.
        released: Set when the gate lets it through.
    """

    def __init__(self) -> None:
        self.entered = threading.Event()
        self.released = threading.Event()

    def pass_through(self) -> None:
        """Announce arrival, then wait to be released.

        Raises:
            TimeoutError: If nobody releases the gate within WAIT.
        """
        self.entered.set()
        if not self.released.wait(WAIT):
            raise TimeoutError("the gate was never released")

    def release(self) -> None:
        """Let the waiting thread through."""
        self.released.set()

    async def reached(self) -> None:
        """Wait, off the event loop, until a thread reaches the gate.

        Raises:
            AssertionError: If nothing reaches it within WAIT.
        """
        if not await asyncio.to_thread(self.entered.wait, WAIT):
            raise AssertionError("nothing reached the gate")


class RecordingEmitter(EventEmitter):
    """Emitter that records every event and can fail on chosen event types."""

    def __init__(self) -> None:
        self.events: list[dict] = []
        self.fail_on: set[str] = set()
        self._lock = threading.Lock()

    def emit(self, event: dict) -> None:
        with self._lock:
            self.events.append(event)
        if event.get("type") in self.fail_on:
            raise OSError(f"event spool unavailable for {event.get('type')}")


class ScriptedSteps:
    """One ``@step`` callable whose behavior is scripted per step ID.

    Attributes:
        calls: Step IDs in the order their bodies started.
        params: The params each step was last invoked with, by step ID.
        behaviors: What a step does when invoked, by step ID.
        running: Step bodies running right now.
        max_running: The most step bodies ever running at once.
    """

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.params: dict[str, dict[str, Any]] = {}
        self.behaviors: dict[str, Callable[[StepContext], None]] = {}
        self.running = 0
        self.max_running = 0
        lock = threading.Lock()
        calls, params, behaviors = self.calls, self.params, self.behaviors

        @step
        def scripted(ctx: StepContext, **kwargs) -> StepResult:
            with lock:
                calls.append(ctx.step_id)
                params[ctx.step_id] = dict(kwargs)
                self.running += 1
                self.max_running = max(self.max_running, self.running)
            try:
                behavior = behaviors.get(ctx.step_id)
                if behavior is not None:
                    behavior(ctx)
            finally:
                with lock:
                    self.running -= 1
            return StepResult()

        self.fn = scripted

    def fail(self, step_id: str, exc: BaseException) -> None:
        """Make a step raise exc when invoked."""

        def raise_(ctx: StepContext) -> None:
            raise exc

        self.behaviors[step_id] = raise_

    def block(self, step_id: str) -> Gate:
        """Make a step wait at a gate when invoked, and return the gate."""
        gate = Gate()
        self.behaviors[step_id] = lambda ctx: gate.pass_through()
        return gate


class CapturingEngine(Engine):
    """Engine that keeps the context of every attempt it runs.

    Attributes:
        contexts: Every AttemptContext passed to ``_run_attempt``, in order.
        journal: Shared log of observable calls, in the order they happened.
    """

    def __init__(self, *args: Any, journal: list[str], **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.contexts: list[AttemptContext] = []
        self.journal = journal

    def _run_attempt(self, plan: Plan, *, context: AttemptContext) -> AttemptReport:
        self.contexts.append(context)
        self.journal.append(f"attempt:{context.run_token}")
        return super()._run_attempt(plan, context=context)


class ObservedStore:
    """A real plan store whose admission reads and finalizations are observable.

    Every other call passes straight through. Scripted actions apply to
    successive calls, one each: an action may pause (see :class:`Gate`), raise,
    or return a result in place of the store's. When it returns None, the call
    goes on to the real store. A finalization action scripted for one plan is
    used only by that plan's calls.

    Attributes:
        inner: The real plan store.
        journal: Shared log of observable calls, in the order they happened.
        calls: Number of calls per observed method ("read", "finalize").
        read_actions: Actions for the next ``read_plan_document`` calls.
        finalize_actions: Actions for the next ``finalize_execution`` calls,
            each with the plan it is for (None for any plan).
        finalizations: Every result a finalization call returned.
    """

    def __init__(self, inner: PlanStore, journal: list[str]) -> None:
        self.inner = inner
        self.journal = journal
        self.calls: Counter[str] = Counter()
        self.read_actions: list[Callable[[str], None]] = []
        self.finalize_actions: list[
            tuple[
                str | None,
                Callable[[ExecutionFinalization], FinalizationResult | None],
            ]
        ] = []
        self.finalizations: list[FinalizationResult] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)

    def read_plan_document(self, doc_id: str) -> dict[str, Any] | None:
        self.calls["read"] += 1
        self.journal.append("read")
        if self.read_actions:
            self.read_actions.pop(0)(doc_id)
        return self.inner.read_plan_document(doc_id)

    def finalize_execution(self, request: ExecutionFinalization) -> FinalizationResult:
        self.calls["finalize"] += 1
        self.journal.append(f"finalize:{request.run_token}")
        result: FinalizationResult | None = None
        for index, (plan_id, action) in enumerate(self.finalize_actions):
            if plan_id in (None, request.plan_id):
                del self.finalize_actions[index]
                result = action(request)
                break
        if result is None:
            result = self.inner.finalize_execution(request)
        self.finalizations.append(result)
        return result

    # ----- scripted actions -----

    def fail_finalize(self, *errors: Exception, plan_id: str | None = None) -> None:
        """Make the next finalization calls raise these errors, in order."""
        for error in errors:
            self.finalize_actions.append((plan_id, _raiser(error)))

    def commit_then_fail_finalize(self, error: Exception) -> None:
        """Let the next finalization write land, then raise error anyway."""

        def action(request: ExecutionFinalization) -> FinalizationResult | None:
            self.inner.finalize_execution(request)
            raise error

        self.finalize_actions.append((None, action))

    def conflict_finalize(self) -> None:
        """Make the next finalization call report a lost race, writing nothing."""
        self.finalize_actions.append(
            (
                None,
                lambda request: FinalizationResult(
                    status=FinalizationStatus.CONFLICT, message="scripted conflict"
                ),
            )
        )

    def pause_finalize(self, error: Exception | None = None) -> Gate:
        """Pause the next finalization call at a gate; then raise error, if any."""
        gate = Gate()

        def action(request: ExecutionFinalization) -> FinalizationResult | None:
            gate.pass_through()
            if error is not None:
                raise error
            return None

        self.finalize_actions.append((None, action))
        return gate

    def pause_read(self) -> Gate:
        """Pause the next admission read at a gate."""
        gate = Gate()
        self.read_actions.append(lambda doc_id: gate.pass_through())
        return gate


def _raiser(error: Exception) -> Callable[[ExecutionFinalization], None]:
    """An action that raises error."""

    def action(request: ExecutionFinalization) -> None:
        raise error

    return action


def directory_tree(root: Path) -> dict[str, bytes | None]:
    """Everything under root: each file's bytes and each directory, by path.

    Comparing two of these shows that nothing under root was created,
    changed or removed in between.

    Args:
        root: The directory to read; it need not exist.

    Returns:
        dict[str, bytes | None]: File contents, or None for a directory,
        keyed by path relative to root.
    """
    tree: dict[str, bytes | None] = {}
    for path in sorted(root.rglob("*")):
        relative = str(path.relative_to(root))
        tree[relative] = None if path.is_dir() else path.read_bytes()
    return tree


def spec(step_id: str, *deps: str, **params: Any) -> StepSpec:
    """A scripted step with the given dependencies and params."""
    return StepSpec(
        step_id=step_id,
        name=step_id,
        fn_ref=STEP_REF,
        params=dict(params),
        deps=list(deps),
    )


def make_plan(
    *specs: StepSpec, policy: str = FAIL_FAST_POLICY, plan_id: str = PLAN_ID
) -> Plan:
    """A plan of scripted steps."""
    return Plan(
        plan_id=plan_id,
        realm=REALM,
        scope=dict(SCOPE),
        steps=list(specs),
        failure_policy=policy,
    )


def chain_plan(policy: str = FAIL_FAST_POLICY, plan_id: str = PLAN_ID) -> Plan:
    """Step b depends on step a."""
    return make_plan(spec("a"), spec("b", "a"), policy=policy, plan_id=plan_id)


def lanes_plan(policy: str, plan_id: str = PLAN_ID) -> Plan:
    """Two independent lanes, each a demultiplexing step and its upload."""
    return make_plan(
        spec("lane1_demux"),
        spec("lane1_upload", "lane1_demux"),
        spec("lane2_demux"),
        spec("lane2_upload", "lane2_demux"),
        policy=policy,
        plan_id=plan_id,
    )


class SQLiteBackend:
    """SQLite plan store on a temporary database file."""

    name = "sqlite"

    def __init__(self, test: unittest.TestCase, directory: Path) -> None:
        self.internal = SQLiteInternalStore(directory / "ygg.sqlite3")
        self.plans = SQLitePlanStore(self.internal)

    def raw_put(self, doc: dict[str, Any]) -> None:
        """Store doc as-is, bypassing the plan store (e.g. a legacy document)."""
        self.internal.put_document("plans", doc["_id"], doc, bump_plan_seq=True)

    def conditional_put(self, doc: dict[str, Any]) -> None:
        """Replace doc at the ``_rev`` it carries, as an approval actor does."""
        self.internal.put_document(
            "plans", doc["_id"], doc, bump_plan_seq=True, expected_rev=doc["_rev"]
        )


class CouchBackend:
    """PlanDBManager on an in-memory client enforcing CouchDB revision rules."""

    name = "couchdb"

    def __init__(self, test: unittest.TestCase, directory: Path) -> None:
        patch_api_exception(test)
        self.server = FakeCouchServer()
        self.plans = plan_db_manager_on(self.server)

    def raw_put(self, doc: dict[str, Any]) -> None:
        """Store doc as-is, bypassing the plan store (e.g. a legacy document)."""
        body = dict(doc)
        body.pop("_rev", None)
        self.server.put_document(db="yggdrasil_plans", doc_id=doc["_id"], document=body)

    def conditional_put(self, doc: dict[str, Any]) -> None:
        """Replace doc at the ``_rev`` it carries, as an approval actor does."""
        self.server.put_document(db="yggdrasil_plans", doc_id=doc["_id"], document=doc)


class ExecutionTestCase(unittest.TestCase):
    """A coordinator over a real engine and a real plan store.

    Subclasses choose the backend with ``backend_type``.

    Attributes:
        journal: Admission reads, attempts and finalizations, in order.
        steps: The scripted step callable every step resolves to.
        engine: The engine, keeping every attempt's context.
        store: The observed plan store the coordinator uses.
        delays: Every finalization backoff the coordinator waited out.
        coordinator: The coordinator under test.
    """

    backend_type: type[SQLiteBackend] | type[CouchBackend] = SQLiteBackend

    def setUp(self) -> None:
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        directory = Path(temp_dir.name)
        self.backend = self.backend_type(self, directory)
        self.journal: list[str] = []
        self.store = ObservedStore(self.backend.plans, self.journal)
        self.steps = ScriptedSteps()
        resolver = patch(
            "yggdrasil.core.engine.resolve_callable", return_value=self.steps.fn
        )
        resolver.start()
        self.addCleanup(resolver.stop)
        self.emitter = RecordingEmitter()
        self.engine = CapturingEngine(
            work_root=directory / "work", emitter=self.emitter, journal=self.journal
        )
        self.delays: list[float] = []
        self.coordinator = PlanExecutionCoordinator(
            engine=self.engine,
            plan_store=self.store,  # type: ignore[arg-type]
            sleep=self.record_delay,
        )

    async def record_delay(self, delay: float) -> None:
        """Backoff stand-in: records the delay and yields without waiting."""
        self.delays.append(delay)
        await asyncio.sleep(0)

    # ----- plans in the store -----

    def save(
        self,
        plan: Plan | None = None,
        *,
        auto_run: bool = True,
        authority: str = "daemon",
        owner: str | None = None,
    ) -> dict[str, Any]:
        """Save plan (approved by default) and return the stored document."""
        plan = plan or chain_plan()
        self.backend.plans.save_plan(
            plan,
            REALM,
            dict(SCOPE),
            auto_run=auto_run,
            execution_authority=authority,
            execution_owner=owner,
        )
        return self.stored(plan.plan_id)

    def stored(self, plan_id: str = PLAN_ID) -> dict[str, Any]:
        """The plan document as currently stored."""
        doc = self.backend.plans.fetch_plan(plan_id)
        assert doc is not None, f"plan {plan_id} is not stored"
        return doc

    def update(self, plan_id: str = PLAN_ID, **fields: Any) -> None:
        """Change fields conditionally, the way an approval actor does."""
        doc = self.stored(plan_id)
        doc.update(fields)
        self.backend.conditional_put(doc)

    def request_rerun(self, plan_id: str = PLAN_ID) -> None:
        """Request a new run of the plan by raising its run token."""
        self.update(plan_id, run_token=self.stored(plan_id)["run_token"] + 1)

    # ----- running the coordinator -----

    def execute(
        self, plan_id: str = PLAN_ID, claim: ExecutionClaim = DAEMON_CLAIM
    ) -> ExecutionResult:
        """Execute a plan on a fresh event loop and return the result."""
        return run_bounded(self.coordinator.execute(plan_id, claim))

    # ----- assertions -----

    def assert_unconsumed(self, plan_id: str = PLAN_ID) -> None:
        """Assert the plan's request was not recorded as executed."""
        doc = self.stored(plan_id)
        self.assertEqual(doc["executed_run_token"], -1)
        self.assertNotIn("last_finalized_execution", doc)

    def assert_recorded(self, result: ExecutionResult, *, run_token: int = 0) -> None:
        """Assert result's attempt is the plan's recorded execution."""
        report = result.report
        assert report is not None and report.outcome is not None
        doc = self.stored(result.plan_doc_id)
        self.assertEqual(doc["executed_run_token"], run_token)
        record = doc["last_finalized_execution"]
        self.assertEqual(record["execution_id"], report.execution_id)
        self.assertEqual(record["outcome"], report.outcome.value)
        self.assertEqual(record["report"], report.to_dict())


class ControlTestCase(ExecutionTestCase):
    """A coordinator whose engine publishes to a real file spool.

    For refusals: an execution ID that was reserved, an attempt that started
    or a report that was published leaves a file behind, so comparing the
    sandbox before and after shows that nothing of the kind happened, and the
    stored document is compared whole, revision included. Subclasses choose
    the backend with ``backend_type``.

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

    @contextmanager
    def admission_reads(
        self, plan_id: str, edit: Callable[[dict[str, Any]], None]
    ) -> Iterator[None]:
        """Make admission read plan_id changed by edit, for the block.

        The stored document stays as it is: only the snapshot that admission
        decides from carries the change.

        Args:
            plan_id: The plan whose admission reads to change.
            edit: Changes the document as read.

        Yields:
            None: For the block in which admission reads the changed document.
        """
        read = self.backend.plans.read_plan_document

        def read_edited(doc_id: str) -> dict[str, Any] | None:
            doc = read(doc_id)
            if doc is not None and doc_id == plan_id:
                edit(doc)
            return doc

        with patch.object(
            self.backend.plans, "read_plan_document", side_effect=read_edited
        ):
            yield

    # ----- assertions -----

    @contextmanager
    def nothing_changes(self, plan_id: str) -> Iterator[None]:
        """Assert that the block reserves, runs, publishes and writes nothing.

        No execution ID is reserved, no attempt or step starts, no
        finalization is called, nothing appears in the sandbox, and plan_id's
        stored document, revision included, and the SQLite plan-change
        sequence stay as they were.

        Args:
            plan_id: The plan whose stored document must not change.

        Yields:
            None: For the block that must change nothing.
        """
        before, seq = self.raw(plan_id), self.plan_seq()
        files = directory_tree(self.sandbox)
        attempts = len(self.engine.contexts)
        steps = list(self.steps.calls)
        finalizations = self.store.calls["finalize"]

        with patch.object(
            self.engine, "reserve_execution_id", wraps=self.engine.reserve_execution_id
        ) as reserve:
            yield

        reserve.assert_not_called()
        self.assertEqual(len(self.engine.contexts), attempts)
        self.assertEqual(self.steps.calls, steps)
        self.assertEqual(self.store.calls["finalize"], finalizations)
        self.assertEqual(directory_tree(self.sandbox), files)
        self.assertEqual(self.raw(plan_id), before)
        self.assertEqual(self.plan_seq(), seq)

    def assert_refused(
        self,
        plan_id: str,
        status: ExecutionStatus,
        *mentions: str,
        claim: ExecutionClaim = DAEMON_CLAIM,
    ) -> ExecutionResult:
        """Execute plan_id; assert it was refused with status and nothing changed.

        Args:
            plan_id: The plan to execute.
            status: The refusal expected.
            *mentions: Text the refusal's message must contain, each once.
            claim: Which plans the caller may execute.

        Returns:
            ExecutionResult: The refusal.
        """
        with (
            self.nothing_changes(plan_id),
            self.assertLogs(COORDINATOR_LOGGER, level=logging.INFO) as logs,
        ):
            result = self.execute(plan_id, claim)

        self.assertEqual(result.status, status, result.message)
        for text in mentions:
            self.assertEqual(result.message.count(text), 1, result.message)
        self.assertTrue(any(result.message in line for line in logs.output))
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
