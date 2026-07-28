"""Protocols and the composition-root bundle for internal storage.

These protocols capture exactly the capabilities Core already uses today.
The CouchDB implementations (``PlanDBManager``, ``OpsWriter``,
``CouchDBCheckpointStore``) satisfy them structurally; the SQLite backend
implements them against a single local database file.

The bundle intentionally has no coordination-document ("state") store: the
only live internal consumer of the ``yggdrasil`` coordination database in the
modern daemon path is checkpoint persistence, which is covered by
``CheckpointStore``. The legacy ``YggdrasilDocument`` operations are used only
by unwired legacy code (see ``docs/TECH_DEBT_LEDGER.md``) and are excluded
until a live consumer exists.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from lib.watchers.backends.base import CheckpointStore, RawWatchEvent
from yggdrasil.flow.model import Plan


@runtime_checkable
class PlanStore(Protocol):
    """Storage for plan documents (intent + approval state).

    Mirrors the existing ``PlanDBManager`` contract exactly. Plan
    eligibility, regeneration, run tokens, execution authority, ownership,
    and document shapes are identical across backends.
    """

    def save_plan(
        self,
        plan: Plan,
        realm: str,
        scope: dict[str, Any],
        *,
        auto_run: bool = False,
        execution_authority: str = "daemon",
        execution_owner: str | None = None,
        preview: dict[str, Any] | None = None,
        source_doc_id: str | None = None,
        source_doc_rev: str | None = None,
        notes: str | None = None,
    ) -> str:
        """Persist a plan document; returns the document ID."""
        ...

    def fetch_plan(self, doc_id: str) -> dict[str, Any] | None:
        """Fetch a plan document by ID, or None if not found."""
        ...

    def fetch_plan_as_model(self, doc_id: str) -> Plan | None:
        """Fetch a plan document and deserialize its Plan model."""
        ...

    def update_executed_token(
        self,
        doc_id: str,
        run_token: int,
        *,
        max_retries: int = 3,
    ) -> bool:
        """Record a successful execution of ``run_token`` for the plan."""
        ...

    def query_approved_pending(self) -> list[dict[str, Any]]:
        """Return all plans eligible for a full recovery scan."""
        ...

    def delete_plan(self, doc_id: str) -> bool:
        """Delete a plan document (testing/cleanup)."""
        ...

    def plan_exists(self, doc_id: str) -> bool:
        """Return True if the plan document exists."""
        ...

    def get_plan_summary(self, doc_id: str) -> dict[str, Any] | None:
        """Return a minimal display summary of the plan, or None."""
        ...


class PlanChangeSource(Protocol):
    """Stream of plan-document changes, backend-agnostic.

    Yields :class:`RawWatchEvent` objects where ``id`` is the plan ID,
    ``doc`` is the current plan document (None for deletions), ``seq`` is an
    opaque backend cursor, and ``deleted`` marks tombstones. Consumers never
    inspect CouchDB change-response fields or SQLite rows directly.

    Multiple mutations between polls may coalesce to the latest state: this
    source provides current plan eligibility, not an audit feed.
    """

    def stream_changes_continuously(
        self,
        *,
        since: str | int,
        poll_interval_sec: float,
    ) -> AsyncIterator[RawWatchEvent]:
        """Stream plan changes after ``since`` indefinitely.

        Args:
            since: Opaque cursor from a previous event's ``seq``, or the
                string ``"now"`` to start at the current head.
            poll_interval_sec: Seconds to sleep between polls when idle.
        """
        ...


class OpsSnapshotSink(Protocol):
    """Sink for latest-state plan_status snapshots built from the event spool.

    Matches the writer interface consumed by ``FileSpoolConsumer``.
    """

    def write(self, plan_dir: Path, snapshot: dict[str, Any]) -> None:
        """Upsert the latest snapshot for one plan."""
        ...


@dataclass(frozen=True)
class InternalStorageBundle:
    """All internal-storage capabilities, resolved once at composition root.

    Attributes:
        backend: Backend identifier ("couchdb" or "sqlite"), for logging and
            tests only — consumers must not branch on it.
        plans: Plan document store.
        plan_changes: Plan change stream for PlanWatcher.
        checkpoints: Watcher checkpoint persistence.
        ops_snapshots: Operations snapshot sink for the spool consumer.
    """

    backend: str
    plans: PlanStore
    plan_changes: PlanChangeSource
    checkpoints: CheckpointStore
    ops_snapshots: OpsSnapshotSink
