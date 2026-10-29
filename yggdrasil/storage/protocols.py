"""Protocols and the composition-root bundle for internal storage.

These protocols capture exactly the capabilities Core already uses today.
The CouchDB implementations (``PlanDBManager``, ``OpsWriter``,
``CouchDBCheckpointStore``) satisfy them structurally; the SQLite backend
implements them against a single local database file.

The bundle intentionally has no coordination-document ("state") store: the
only live internal consumer of the ``yggdrasil`` coordination database in the
modern daemon path is checkpoint persistence, which is covered by
``CheckpointStore``. A store protocol for coordination documents is added
here once such documents gain a live consumer.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from yggdrasil.flow.model import Plan
from yggdrasil.storage.plan_updates import ExecutionFinalization, FinalizationResult
from yggdrasil.watchers.backends.base import CheckpointStore, RawWatchEvent


@runtime_checkable
class PlanStore(Protocol):
    """Storage for plan documents (intent + approval state).

    Mirrors the ``PlanDBManager`` contract exactly. Plan eligibility,
    regeneration, run tokens, plan generations, execution authority,
    ownership, document shapes, and conflict behavior are identical across
    backends.

    Every write that replaces a plan document's content is conditioned on
    the state it was derived from, so a concurrent writer is never silently
    overwritten, and a lost race always surfaces as the backend-neutral
    :class:`~yggdrasil.storage.errors.RevisionConflictError`.

    Backend *failures* are normalized only by the strict read and the
    conditional update that execution relies on, :meth:`read_plan_document`
    and :meth:`finalize_execution`: they raise
    :class:`~yggdrasil.storage.errors.PlanStoreError`, which also says whether
    another attempt could help. The older methods still let their backend's
    own exceptions through — a CouchDB ``ApiException``, a ``sqlite3.Error`` —
    so a caller that must behave identically on both backends either uses
    these two methods or catches broadly.
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
        """Persist a new or regenerated plan document; returns the document ID.

        Every call assigns a fresh ``plan_generation`` and resets execution
        tokens. It may replace an empty slot, an unversioned document or a
        document of a supported family, and refuses an invalid marker, an
        unsupported family or a ``created_at`` the replacement cannot keep
        (``check_replaceable`` in ``yggdrasil/storage/plan_documents.py``).
        The write applies only to the document state that was read, so a lost
        race raises instead of replaying this call's planning intent over a
        newer document.

        Raises:
            ValueError: If execution_authority is invalid or plan.plan_id is
                missing.
            UnsupportedDocumentError: If planning may not replace the document
                in the slot. Nothing was written.
            RevisionConflictError: If another writer created or changed the
                plan document first. Nothing was written.
        """
        ...

    def fetch_plan(self, doc_id: str) -> dict[str, Any] | None:
        """Fetch a plan document by ID, or None if not found."""
        ...

    def fetch_plan_as_model(self, doc_id: str) -> Plan | None:
        """Fetch a plan document and deserialize its Plan model."""
        ...

    def read_plan_document(self, doc_id: str) -> dict[str, Any] | None:
        """Return the plan document as stored, for an execution decision.

        A plain read: it writes nothing. A body counts as the document only
        when its storage envelope is intact, ``_id`` equal to doc_id and a
        non-empty string ``_rev`` (``require_storage_envelope`` in
        ``yggdrasil/storage/plan_updates.py``). Anything else is a storage
        failure, never a missing or unversioned document. The returned
        document is one consistent snapshot to capture an execution request
        from; its content is not checked.

        Returns:
            The document with its ``_rev``, or None only when the backend
            confirms the plan does not exist.

        Raises:
            PlanStoreError: If the storage backend fails, or answers with
                something other than the plan's stored document.
        """
        ...

    def finalize_execution(self, request: ExecutionFinalization) -> FinalizationResult:
        """Record a finished execution request's outcome and executed token.

        Both are written together in one conditional write, and only if the
        plan is still of the captured family (one this release supports) and
        the captured generation, under the captured authority and owner, with
        a valid token pair that the write keeps valid, and has not recorded
        this request or a newer one. Every read the write may go over, the
        conflict reread included, is strict, like
        :meth:`read_plan_document`. Each call makes one attempt and resolves
        to one status: COMMITTED, ALREADY_COMMITTED (this execution's
        completion was already recorded), CONFLICT (still valid, retry),
        SUPERSEDED (never retry), or INVALID_CONTROL_STATE (not until the
        plan's tokens change).

        Raises:
            PlanStoreError: If the storage backend fails, or a read answers
                with something other than the plan's stored document. A
                failed write may have landed; calling again reports that as
                ALREADY_COMMITTED. Its ``retryable`` flag says whether another
                attempt could help.
        """
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
