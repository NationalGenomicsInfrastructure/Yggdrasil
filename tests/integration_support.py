"""Shared fixtures for end-to-end tests through the daemon's real callers.

A test scenario document goes the whole way. The test realm's own WatchSpec
turns it into an event through WatcherManager's routing, the real
``TestRealmHandler`` plans it, and ``YggdrasilCore`` persists the plan. The
plan is then executed through the daemon's plan-event handler or through
run-once, by the engine, coordinator, event spool and plan store the core
wires itself. Snapshots come from one ops-consumer cycle, as the daemon's ops
service runs them and run-once runs one on exit, and are read back from the
backend's snapshot store.

Both internal-storage backends are available: SQLite as a dev daemon builds
it, and CouchDB through the production ``PlanDBManager`` and ``OpsWriter`` on
an in-memory client that enforces CouchDB's revision rules. The CouchDB change
feed is the one part not modelled: run-once admits the plans it creates
through its recovery pass, and the daemon receives its plan events as
PlanWatcher emits them.
"""

from __future__ import annotations

import asyncio
import os
import unittest
from collections.abc import AsyncIterator
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import Mock, patch

import yggdrasil.realms.test_realm as test_realm
from tests.execution_support import SCENARIO_LIMIT, Watchdog, run_bounded
from tests.plan_store_support import (
    FakeCouchServer,
    ops_writer_on,
    patch_api_exception,
    plan_db_manager_on,
)
from yggdrasil.daemon.core import YggdrasilCore
from yggdrasil.daemon.plan_execution import ExecutionResult
from yggdrasil.daemon.singleton import SingletonMeta
from yggdrasil.ops.consumer import FileSpoolConsumer
from yggdrasil.storage import build_internal_storage
from yggdrasil.storage.partitions import partition_key
from yggdrasil.storage.protocols import InternalStorageBundle
from yggdrasil.storage.sqlite import SQLiteInternalStore
from yggdrasil.watchers.backends.base import RawWatchEvent
from yggdrasil.watchers.backends.checkpoint_store import InMemoryCheckpointStore
from yggdrasil.watchers.events import EventType, YggdrasilEvent
from yggdrasil.watchers.manager import WatcherManager
from yggdrasil.watchers.watchspec import BoundWatchSpec

REALM = "test_realm"
SCENARIO_ID = "test_scenario:lanes"
PLAN_ID = f"{REALM}:{SCENARIO_ID}"
SCOPE = {"kind": "test_scenario", "id": SCENARIO_ID}
SNAPSHOT_ID = f"{partition_key(SCOPE)}:plan_status:{REALM}:{PLAN_ID}"
CORE_LOGGER = "yggdrasil.daemon.core.YggdrasilCore"


def scenario(recipe: str, **fields: Any) -> dict[str, Any]:
    """A test scenario document, as stored in the scenario database."""
    return {
        "_id": SCENARIO_ID,
        "_rev": "1-scenario",
        "type": "ygg_test_scenario",
        "recipe": recipe,
        **fields,
    }


def plan_event(doc: dict[str, Any]) -> YggdrasilEvent:
    """The event PlanWatcher emits for a plan document it saw."""
    return YggdrasilEvent(
        EventType.PLAN_EXECUTION,
        {"plan_doc_id": doc["_id"], "plan_doc": doc},
        "PlanWatcher",
    )


class IdleChangeSource:
    """A plan change feed on which nothing ever changes."""

    async def stream_changes_continuously(
        self, *, since: str | int, poll_interval_sec: float
    ) -> AsyncIterator[RawWatchEvent]:
        """Wait, without yielding, until the watcher is cancelled.

        Args:
            since: Ignored.
            poll_interval_sec: Ignored.

        Yields:
            RawWatchEvent: Never.
        """
        await asyncio.Event().wait()
        return
        yield


class SQLiteStorage:
    """Internal storage as a dev daemon builds it, on one SQLite file.

    Attributes:
        bundle: The storage the core is given.
    """

    def __init__(self, test: unittest.TestCase, directory: Path) -> None:
        path = directory / "ygg.sqlite3"
        self.bundle = build_internal_storage(
            {"internal_storage": {"backend": "sqlite", "sqlite": {"path": str(path)}}},
            dev_mode=True,
        )
        # A handle of its own on the same file, as an operator's tool opens it.
        self._database = SQLiteInternalStore(path)

    def conditional_put(self, doc: dict[str, Any]) -> None:
        """Replace a plan at the ``_rev`` it carries, as an approval actor does."""
        self._database.put_document(
            "plans", doc["_id"], doc, bump_plan_seq=True, expected_rev=doc["_rev"]
        )

    def snapshot(self, doc_id: str) -> dict[str, Any] | None:
        """Return a stored ops snapshot."""
        return self._database.get_document("operations_snapshots", doc_id)

    def plan_seq(self) -> int | None:
        """Return the head of the plan-change sequence."""
        return self._database.current_plan_seq()


class CouchStorage:
    """The production CouchDB storage classes, on in-memory clients.

    Attributes:
        bundle: The storage the core is given.
    """

    def __init__(self, test: unittest.TestCase, directory: Path) -> None:
        patch_api_exception(test)
        self._plans = FakeCouchServer()
        self._operations = FakeCouchServer()
        self.bundle = InternalStorageBundle(
            backend="couchdb",
            plans=plan_db_manager_on(self._plans),
            plan_changes=IdleChangeSource(),
            checkpoints=InMemoryCheckpointStore(),
            ops_snapshots=ops_writer_on(self._operations),
        )

    def conditional_put(self, doc: dict[str, Any]) -> None:
        """Replace a plan at the ``_rev`` it carries, as an approval actor does."""
        self._plans.put_document(db="yggdrasil_plans", doc_id=doc["_id"], document=doc)

    def snapshot(self, doc_id: str) -> dict[str, Any] | None:
        """Return a stored ops snapshot."""
        return self._operations.raw(doc_id)

    def plan_seq(self) -> int | None:
        """Return None: CouchDB keeps no plan-change sequence of its own."""
        return None


class CoreTestCase(unittest.TestCase):
    """A YggdrasilCore as the daemon builds it, with the test realm registered.

    Subclasses choose the internal-storage backend with ``storage_type``.
    """

    storage_type: type[SQLiteStorage] | type[CouchStorage] = SQLiteStorage

    def setUp(self) -> None:
        SingletonMeta._instances.clear()
        self.addCleanup(SingletonMeta._instances.clear)
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        directory = Path(temp_dir.name)
        self.spool = directory / "spool"
        environment = patch.dict(os.environ, {"YGG_EVENT_SPOOL": str(self.spool)})
        environment.start()
        self.addCleanup(environment.stop)

        self.storage = self.storage_type(self, directory)
        self.core = YggdrasilCore(
            {"work_root": str(directory / "work")}, storage=self.storage.bundle
        )
        with patch.object(test_realm, "is_test_realm_enabled", return_value=True):
            descriptor = test_realm.get_realm_descriptor()
        assert descriptor is not None
        self.descriptor = descriptor
        self.core._register_realm_handlers(descriptor)

    # ----- planning -----

    def plan(self, doc: dict[str, Any]) -> dict[str, Any]:
        """Deliver a changed scenario document as the daemon's watchers do.

        The realm's WatchSpec filters it and builds its event, WatcherManager
        routes it to the core, and the core has the realm's handler plan it
        and persists the plan.

        Args:
            doc: The scenario document.

        Returns:
            dict[str, Any]: The persisted plan document.
        """
        watchspecs = self.descriptor.watchspecs
        (spec,) = watchspecs() if callable(watchspecs) else watchspecs
        manager = WatcherManager(
            {}, on_event=self.core.handle_event, checkpoint_store=Mock()
        )
        change = RawWatchEvent(id=doc["_id"], doc=doc, seq="1-change", deleted=False)

        async def deliver() -> None:
            planned = asyncio.get_running_loop().create_future()
            generate = self.core._generate_and_persist_plan

            async def observed(handler: Any, payload: dict[str, Any]) -> None:
                try:
                    await generate(handler, payload)
                finally:
                    planned.set_result(None)

            with patch.object(self.core, "_generate_and_persist_plan", observed):
                manager._fan_out(
                    change,
                    [BoundWatchSpec(spec=spec, realm_id=REALM)],
                    "couchdb:yggdrasil_testdocs",
                )
                await planned

        run_bounded(deliver())
        return self.stored()

    def stored(self) -> dict[str, Any]:
        """The plan document as currently stored."""
        doc = self.core.storage.plans.fetch_plan(PLAN_ID)
        assert doc is not None, f"plan {PLAN_ID} is not stored"
        return doc

    def request_rerun(self) -> None:
        """Raise the plan's run token, as an operator requesting a rerun does."""
        doc = self.stored()
        doc["run_token"] += 1
        self.storage.conditional_put(doc)

    # ----- execution -----

    def execute_as_daemon(self) -> ExecutionResult:
        """Hand the daemon the plan's PlanWatcher event; wait for its result.

        Returns:
            ExecutionResult: How the coordinator resolved the request.
        """
        event = plan_event(self.stored())
        coordinator = self.core.plan_executions

        async def deliver() -> ExecutionResult:
            submitted: list[asyncio.Future[ExecutionResult]] = []
            submit = coordinator.submit

            def recording_submit(plan_doc_id: str, claim: Any) -> Any:
                future = submit(plan_doc_id, claim)
                submitted.append(future)
                return future

            with patch.object(coordinator, "submit", recording_submit):
                self.core._handle_plan_execution_event(event)
            (future,) = submitted
            return await future

        return run_bounded(deliver())

    def run_once(self, doc: dict[str, Any], *, force_overwrite: bool = False) -> int:
        """Run ``yggdrasil run-doc`` for a scenario document; return the exit code.

        The realm's handler is subscribed to the project events run-doc plans
        from, and the document is served in place of a project document.

        Args:
            doc: The scenario document.
            force_overwrite: Plan over an existing plan document, as
                ``--force`` does.

        Returns:
            int: The exit code.
        """
        (handler,) = self.core.subscriptions[EventType.COUCHDB_DOC_CHANGED]
        self.core.register_handler(EventType.PROJECT_CHANGE, handler)
        self.core.pdm = Mock()
        self.core.pdm.fetch_document_by_id.return_value = doc
        with Watchdog(SCENARIO_LIMIT) as watchdog:
            exit_code = self.core.run_once_with_watcher(
                doc["_id"], force_overwrite=force_overwrite, timeout_seconds=30
            )
        self.assertFalse(watchdog.expired)
        return exit_code

    # ----- what was published -----

    def consume_spool(self) -> dict[str, Any]:
        """Run one ops consumer cycle; return the plan's snapshot as stored."""
        FileSpoolConsumer(
            self.core.event_spool, self.core.storage.ops_snapshots
        ).consume()
        return self.stored_snapshot()

    def stored_snapshot(self) -> dict[str, Any]:
        """The plan's ops snapshot as currently stored."""
        snapshot = self.storage.snapshot(SNAPSHOT_ID)
        assert snapshot is not None, f"no snapshot {SNAPSHOT_ID} is stored"
        return snapshot
