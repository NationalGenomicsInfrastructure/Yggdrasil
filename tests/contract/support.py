"""Checking that what Yggdrasil's builders produce conforms to the contract.

:func:`assert_conforms` checks one document. :class:`ConformingBuilders` is a
TestCase base that checks every document a builder produces while a test
runs, at the point it is produced:

- plan documents from ``build_plan_document``, where both plan stores and the
  test module itself call it, before anything changes them;
- snapshots from ``snapshot_document``, where the ops consumer and the test
  module call it, in the form a sink stores them (with ``_id``);
- the projections a test module makes with ``project_attempt`` or
  ``project_legacy``, wrapped into a stored snapshot;
- the report of every attempt the engine publishes.

A nonconforming document does not stop the code under test, which may catch
exceptions on purpose: the engine treats one raised while it publishes as a
publication failure. Each is recorded, and the test fails when it ends,
naming the document's problems and where it was built. A test that builds a
malformed document on purpose wraps that call in
:meth:`ConformingBuilders.nonconforming_builds`; a whole class opts out with
``check_builder_outputs = False``.

:class:`SpoolTestCase` runs real attempts through the engine into a
temporary spool, with a step body each test chooses, and builds snapshots
from that spool the way the ops consumer does.
"""

from __future__ import annotations

import copy
import json
import sys
import threading
import traceback
import unittest
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, ClassVar
from unittest.mock import patch

import yggdrasil.ops.consumer
import yggdrasil.storage.couchdb.plan_store
import yggdrasil.storage.sqlite
from tests.execution_support import REALM, WAIT, Gate, make_plan, spec
from yggdrasil.contract import ATTEMPT_REPORT, PLAN_DOCUMENT, PLAN_STATUS
from yggdrasil.contract.schema import ContractError, validate
from yggdrasil.core.engine import Engine
from yggdrasil.flow.attempt import AttemptContext
from yggdrasil.flow.events.attempt_records import (
    ATTEMPT_REPORT_EVENT,
    attempt_dir,
    record_filename,
)
from yggdrasil.flow.events.emitter import FileSpoolEmitter
from yggdrasil.flow.model import CONTINUE_INDEPENDENT_POLICY, Plan, StepResult
from yggdrasil.flow.step import StepContext, step
from yggdrasil.ops.consumer import build_plan_snapshot
from yggdrasil.ops.snapshot import (
    PROJECTION_ATTEMPT,
    PROJECTION_LEGACY,
    snapshot_document,
)
from yggdrasil.storage.partitions import partition_key

# Stands for a field removed from a document; see changed().
ABSENT = object()

# The envelope of a snapshot made from a projection alone.
PROJECTION_REALM = "test_realm"
PROJECTION_PLAN_ID = "pln_projection"
PROJECTION_SCOPE = {"kind": "project", "id": "P1"}
PROJECTION_UPDATED_AT = "2026-10-22T10:00:00Z"

# Frames of the stack shown for a nonconforming document.
_STACK_FRAMES = 8

# The plan whose attempts a SpoolTestCase runs.
SPOOL_PLAN_ID = "pln_contract"

# What a step does when it runs, given its context.
StepBody = Callable[[StepContext], StepResult | None]

# A builder to check: what holds it, its name there, and what wraps it in a check.
_Site = tuple[Any, str, Callable[[Callable[..., Any]], Callable[..., Any]]]


def assert_conforms(test: unittest.TestCase, kind: str, document: object) -> None:
    """Fail test unless document conforms to its kind's contract schema.

    Args:
        test: The running test case.
        kind: ``plan_document``, ``plan_status`` or ``attempt_report``.
        document: The document, as stored.
    """
    try:
        validate(kind, document)
    except ContractError as error:
        test.fail(str(error))


def problems(kind: str, document: object) -> list[tuple[str, str]]:
    """Return each problem validate reports, as (path, code); none if it conforms.

    Args:
        kind: ``plan_document``, ``plan_status`` or ``attempt_report``.
        document: The document, as stored.

    Returns:
        list[tuple[str, str]]: The problems' paths and codes, in reported order.
    """
    try:
        validate(kind, document)
    except ContractError as error:
        return [(problem.path, problem.code) for problem in error.problems]
    return []


def changed(document: dict[str, Any], path: str, value: Any) -> dict[str, Any]:
    """Return a copy of document with one field set, or removed for ABSENT.

    Args:
        document: The document; not changed.
        path: The field, as dot-separated keys; a numeric part indexes a
            list, as in ``plan.steps.0.deps``.
        value: The new value, or ABSENT.

    Returns:
        dict[str, Any]: The changed copy.
    """
    result = copy.deepcopy(document)
    *parents, last = path.split(".")
    container: Any = result
    for part in parents:
        container = container[int(part) if isinstance(container, list) else part]
    key = int(last) if isinstance(container, list) else last
    if value is ABSENT:
        del container[key]
    else:
        container[key] = value
    return result


def stored_snapshot(snapshot: dict[str, Any]) -> dict[str, Any] | None:
    """Return a snapshot the way a sink stores it, with its ``_id``.

    Both sinks derive the ID from the scope, the realm and the plan ID. The
    consumer does not store a snapshot without a scope, and a sink cannot
    store one whose scope is not an object with a kind and an ID.

    Args:
        snapshot: The snapshot ``snapshot_document`` built.

    Returns:
        dict[str, Any] | None: The stored form; None for a snapshot that is
        never stored.
    """
    scope = snapshot.get("scope")
    if not scope or not isinstance(scope, dict):
        return None
    try:
        partition = partition_key(scope)
    except ValueError:
        return None
    doc_id = f"{partition}:plan_status:{snapshot['realm']}:{snapshot['plan_id']}"
    return {**snapshot, "_id": doc_id}


def projection_snapshot(
    attempt: dict[str, Any] | None, steps: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    """Wrap a projection into a stored snapshot, under a fixed envelope.

    Args:
        attempt: The attempt summary; None for a legacy projection.
        steps: The step entries by step ID.

    Returns:
        dict[str, Any]: The snapshot, with its ``_id``.
    """
    snapshot = snapshot_document(
        realm=PROJECTION_REALM,
        plan_id=PROJECTION_PLAN_ID,
        scope=dict(PROJECTION_SCOPE),
        projection=PROJECTION_ATTEMPT if attempt is not None else PROJECTION_LEGACY,
        attempt=attempt,
        steps=steps,
        updated_at=PROJECTION_UPDATED_AT,
    )
    stored = stored_snapshot(snapshot)
    assert stored is not None  # the fixed scope is always stored
    return stored


class ConformingBuilders(unittest.TestCase):
    """A test case in which every document a builder produces must conform.

    Attributes:
        check_builder_outputs: False on a class whose tests build malformed
            documents throughout.
    """

    check_builder_outputs: ClassVar[bool] = True

    def run(self, result: unittest.TestResult | None = None) -> Any:
        """Run the test with every builder's output checked as it is produced.

        Args:
            result: Where unittest records the outcome.

        Returns:
            The result, as unittest.TestCase.run returns it.
        """
        self._nonconforming: list[str] = []
        self._unchecked = 0
        if not self.check_builder_outputs:
            return super().run(result)
        # The first cleanup registered runs last, after every other one.
        self.addCleanup(self._assert_builds_conformed)
        with ExitStack() as patches:
            for owner, name, check in self._builder_sites():
                original = getattr(owner, name)
                patches.enter_context(patch.object(owner, name, check(original)))
            return super().run(result)

    @contextmanager
    def nonconforming_builds(self) -> Iterator[None]:
        """Leave unchecked what builders produce inside the block.

        Yields:
            None: For the block, which builds a malformed document on purpose.
        """
        self._unchecked += 1
        try:
            yield
        finally:
            self._unchecked -= 1

    def _builder_sites(self) -> list[_Site]:
        """Return each builder to check, where it is called from."""
        sites: list[_Site] = [
            (yggdrasil.storage.sqlite, "build_plan_document", self._plan_documents),
            (
                yggdrasil.storage.couchdb.plan_store,
                "build_plan_document",
                self._plan_documents,
            ),
            (yggdrasil.ops.consumer, "snapshot_document", self._snapshots),
            (Engine, "_emit_engine_event", self._published_reports),
        ]
        module = sys.modules[type(self).__module__]
        for name, check in (
            ("build_plan_document", self._plan_documents),
            ("snapshot_document", self._snapshots),
            ("project_attempt", self._attempt_projections),
            ("project_legacy", self._legacy_projections),
        ):
            if hasattr(module, name):
                sites.append((module, name, check))
        return sites

    def _check(self, builder: str, kind: str, document: object) -> None:
        """Record document's problems, unless checks are suspended."""
        if self._unchecked:
            return
        try:
            validate(kind, document)
        except ContractError as error:
            stack = "".join(traceback.format_stack(limit=_STACK_FRAMES)[:-2])
            self._nonconforming.append(f"{builder} built:\n{error}\nat:\n{stack}")

    def _assert_builds_conformed(self) -> None:
        """Fail the test if a builder produced a nonconforming document."""
        if self._nonconforming:
            self.fail(
                f"{len(self._nonconforming)} nonconforming document(s):\n\n"
                + "\n\n".join(self._nonconforming)
            )

    def _plan_documents(self, build: Callable[..., Any]) -> Callable[..., Any]:
        """Check what a plan-document builder returns."""

        def checked(*args: Any, **kwargs: Any) -> Any:
            document = build(*args, **kwargs)
            self._check("build_plan_document", PLAN_DOCUMENT, document)
            return document

        return checked

    def _snapshots(self, build: Callable[..., Any]) -> Callable[..., Any]:
        """Check the stored form of what a snapshot builder returns."""

        def checked(*args: Any, **kwargs: Any) -> Any:
            snapshot = build(*args, **kwargs)
            stored = stored_snapshot(snapshot)
            if stored is not None:
                self._check("snapshot_document", PLAN_STATUS, stored)
            return snapshot

        return checked

    def _attempt_projections(self, project: Callable[..., Any]) -> Callable[..., Any]:
        """Check the snapshot that an attempt's projection makes."""

        def checked(*args: Any, **kwargs: Any) -> Any:
            summary, steps = project(*args, **kwargs)
            self._check(
                "project_attempt", PLAN_STATUS, projection_snapshot(summary, steps)
            )
            return summary, steps

        return checked

    def _legacy_projections(self, project: Callable[..., Any]) -> Callable[..., Any]:
        """Check the snapshot that a legacy projection makes."""

        def checked(*args: Any, **kwargs: Any) -> Any:
            steps = project(*args, **kwargs)
            self._check("project_legacy", PLAN_STATUS, projection_snapshot(None, steps))
            return steps

        return checked

    def _published_reports(self, emit: Callable[..., Any]) -> Callable[..., Any]:
        """Check the report in every report event the engine publishes."""

        def checked(engine: Engine, description: str, event: dict[str, Any]) -> Any:
            if event.get("type") == ATTEMPT_REPORT_EVENT:
                self._check("the engine", ATTEMPT_REPORT, event.get("report"))
            return emit(engine, description, event)

        return checked


class SpoolTestCase(ConformingBuilders):
    """An engine publishing real attempts to a temporary spool.

    Every step resolves to one ``@step`` function, which runs :attr:`body`.
    What the builders produce along the way is checked, as in any
    ConformingBuilders test.

    Attributes:
        spool: The spool root.
        engine: The engine, publishing to the spool.
        body: What a step does when it runs; set by :meth:`finished` and
            :meth:`running`.
    """

    def setUp(self) -> None:
        """Give the test an empty spool and an engine publishing to it."""
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        root = Path(temp_dir.name)
        self.spool = root / "spool"
        self.body: StepBody = lambda ctx: None
        self._plans = 0

        @step
        def realm(ctx: StepContext, **kwargs: Any) -> StepResult | None:
            return self.body(ctx)

        resolver = patch("yggdrasil.core.engine.resolve_callable", return_value=realm)
        resolver.start()
        self.addCleanup(resolver.stop)
        self.engine = Engine(
            work_root=root / "work", emitter=FileSpoolEmitter(self.spool)
        )

    def plan(
        self,
        *step_ids: str,
        scope: dict[str, Any] | None = None,
        policy: str = CONTINUE_INDEPENDENT_POLICY,
    ) -> Plan:
        """Return a plan of independent steps that no earlier success is reused for.

        Args:
            *step_ids: The plan's steps; one step, ``a``, when none are given.
            scope: The plan's scope; the default scope when None.
            policy: The plan's failure policy.

        Returns:
            Plan: The plan, under SPOOL_PLAN_ID.
        """
        self._plans += 1
        plan = make_plan(
            *(spec(step_id, attempt=self._plans) for step_id in step_ids or ("a",)),
            policy=policy,
            plan_id=SPOOL_PLAN_ID,
        )
        if scope is not None:
            plan.scope = scope
        return plan

    def context(self, plan: Plan) -> AttemptContext:
        """Open an attempt's context, with its ID reserved as callers do."""
        execution_id, reserved = self.engine.reserve_execution_id(plan)
        return AttemptContext.for_plan(
            plan, execution_id=execution_id, execution_id_reserved=reserved
        )

    def finished(
        self, plan: Plan, body: StepBody, *, context: AttemptContext | None = None
    ) -> AttemptContext:
        """Run one attempt to its end, with body as every step's work.

        Args:
            plan: The plan.
            body: What each step does.
            context: The attempt's context; a fresh one when None.

        Returns:
            AttemptContext: The context; its report says how the attempt ended.
        """
        self.body = body
        context = context or self.context(plan)
        try:
            self.engine._run_attempt(plan, context=context)
        except Exception:
            pass  # how it ended is in the report
        return context

    def running(self, plan: Plan, body: StepBody) -> dict[str, Any]:
        """Return the snapshot taken while a step waits, after body ran.

        Args:
            plan: A plan whose first step runs body.
            body: What the step does before it waits.

        Returns:
            dict[str, Any]: The snapshot of the running attempt.
        """
        gate = Gate()

        def waiting(ctx: StepContext) -> None:
            body(ctx)
            gate.pass_through()

        self.body = waiting
        worker = threading.Thread(
            target=self.engine._run_attempt,
            args=(plan,),
            kwargs={"context": self.context(plan)},
        )
        worker.start()
        self.addCleanup(worker.join, WAIT)
        self.addCleanup(gate.release)
        self.assertTrue(gate.entered.wait(WAIT))
        snapshot = self.snapshot()
        gate.release()
        worker.join(WAIT)
        return snapshot

    def snapshot(self) -> dict[str, Any]:
        """Return the plan's snapshot, as the ops consumer builds it."""
        return build_plan_snapshot(
            self.spool / REALM / SPOOL_PLAN_ID, REALM, SPOOL_PLAN_ID
        )

    def attempt_dir(self, context: AttemptContext) -> Path:
        """Return the spool directory of an attempt."""
        return attempt_dir(self.spool, REALM, SPOOL_PLAN_ID, context.execution_id)

    def published_report(self, context: AttemptContext) -> dict[str, Any]:
        """Return the report an attempt published to the spool."""
        record = self.attempt_dir(context) / record_filename(ATTEMPT_REPORT_EVENT)
        return json.loads(record.read_text("utf-8"))["report"]
