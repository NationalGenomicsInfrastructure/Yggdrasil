"""SQLite internal-storage backend (dev mode and tests only).

One SQLite file holds all internal state as JSON documents in namespaced
rows — never pickle or opaque BLOBs. The document shapes are identical to
the CouchDB backend (shared builders in ``lib/storage/plan_documents.py``),
so dev-mode behavior stays representative of production.

Threading model:
    Consumers are concurrent asyncio tasks whose sync storage calls run on
    ``asyncio.to_thread`` worker threads. Python ``sqlite3`` connections are
    thread-bound, so this store opens a **fresh connection per operation**
    (cheap with WAL) with a bounded busy timeout. Never cache a connection
    across calls.

Durability model:
    WAL journal, short transactions. The database is *disposable dev state*:
    if the file is deleted, fresh state is created at the next startup.
    Reliable host-local storage is required — network filesystems are
    unsupported for SQLite WAL.

Change feed:
    Every plan mutation increments a global plan-change counter
    (``store_metadata['plan_change_seq']``) and stamps the new value on the
    row in the same transaction. The change source polls rows with
    ``change_seq > cursor``; multiple mutations between polls coalesce to
    the latest state (current eligibility, not an audit feed). Deletions
    are tombstones.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from lib.core_utils.errors import InternalStorageConfigurationError
from lib.core_utils.logging_utils import custom_logger
from lib.core_utils.plan_eligibility import is_plan_eligible
from lib.couchdb.partitions import partition_key
from lib.storage.config import SQLiteInternalStorageConfig
from lib.storage.plan_documents import (
    build_plan_document,
    json_safe,
    plan_model_from_document,
    plan_summary_from_document,
    utc_now_iso,
    validate_execution_authority,
)
from lib.storage.protocols import InternalStorageBundle
from lib.watchers.backends.base import Checkpoint, CheckpointStore, RawWatchEvent
from yggdrasil.flow.model import Plan

# "YGGD" — marks the file as an Yggdrasil internal-storage database.
_APPLICATION_ID = 0x59474744
_SCHEMA_VERSION = 1
_PLAN_SEQ_KEY = "plan_change_seq"

_NS_PLANS = "plans"
_NS_CHECKPOINTS = "checkpoints"
_NS_OPS = "operations_snapshots"

# Individual statements so the fresh-init can run them inside one explicit
# transaction (executescript would auto-commit and break atomicity).
_SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS store_metadata (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS documents (
        namespace TEXT NOT NULL,
        document_id TEXT NOT NULL,
        revision INTEGER NOT NULL,
        change_seq INTEGER,
        deleted INTEGER NOT NULL DEFAULT 0,
        body_json TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (namespace, document_id)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_documents_change_seq
        ON documents (namespace, change_seq)
    """,
)


class SQLiteStorageError(InternalStorageConfigurationError):
    """A SQLite storage file failed lifecycle validation.

    Subclasses :class:`InternalStorageConfigurationError` so the CLI's
    startup handling prints one concise operator message. Never triggers
    automatic deletion — resolving requires operator action.
    """


class SQLiteInternalStore:
    """Low-level namespaced JSON document store on one SQLite file.

    Attributes:
        path: Absolute path of the database file.
    """

    def __init__(self, path: Path, *, logger: logging.Logger | None = None) -> None:
        """Validate/create the database file and initialize the schema.

        Args:
            path: Absolute database file path.

        Raises:
            SQLiteStorageError: If an existing file is symlinked, owned by
                another user, malformed, unrelated to Yggdrasil, or has an
                unsupported schema version.
        """
        self._logger = logger or custom_logger(f"{__name__}.{type(self).__name__}")
        self.path = Path(path)
        self._initialize()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _initialize(self) -> None:
        """Prepare directory and file, then create or validate the schema."""
        self._prepare_directory()
        fresh = self._validate_file()

        conn = self._connect()
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            if fresh:
                # Atomic fresh-init: schema, seed metadata, and the identity
                # PRAGMAs (application_id / user_version — both transactional)
                # all commit together or not at all. This never leaves a marked
                # file with missing tables. A crash mid-init leaves either a
                # 0-byte file (treated as fresh on reopen) or a partially
                # written file that reopen validation rejects as unrelated
                # (application_id still unset) — i.e. it fails closed, requiring
                # the operator to delete the disposable dev file and restart.
                conn.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_STATEMENTS:
                        conn.execute(statement)
                    conn.execute(
                        "INSERT OR IGNORE INTO store_metadata (key, value) "
                        "VALUES (?, ?)",
                        (_PLAN_SEQ_KEY, "0"),
                    )
                    conn.execute(
                        "INSERT OR IGNORE INTO store_metadata (key, value) "
                        "VALUES (?, ?)",
                        ("created_at", utc_now_iso()),
                    )
                    conn.execute(f"PRAGMA application_id = {_APPLICATION_ID}")
                    conn.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
                    conn.execute("COMMIT")
                except BaseException:
                    conn.execute("ROLLBACK")
                    raise
                self._logger.info(
                    "Created new disposable dev internal-storage database at %s",
                    self.path,
                )
            else:
                self._validate_schema(conn)
        finally:
            conn.close()

        os.chmod(self.path, 0o600)

    def _prepare_directory(self) -> None:
        """Create missing parent directories with 0700 permissions."""
        parent = self.path.parent
        missing: list[Path] = []
        probe = parent
        while not probe.exists():
            missing.append(probe)
            probe = probe.parent
        parent.mkdir(parents=True, exist_ok=True)
        for created in missing:
            os.chmod(created, 0o700)

    def _validate_file(self) -> bool:
        """Validate an existing database file, or mark a fresh one needed.

        Returns:
            True if the file must be freshly initialized.

        Raises:
            SQLiteStorageError: On symlink, foreign owner, malformed, or
                unrelated files.
        """
        try:
            st = os.lstat(self.path)
        except FileNotFoundError:
            # Create with restrictive permissions before SQLite touches it.
            fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
            os.close(fd)
            return True

        if os.path.islink(self.path):
            raise SQLiteStorageError(
                f"Refusing symlinked internal-storage database: {self.path}"
            )
        if not os.path.isfile(self.path):
            raise SQLiteStorageError(
                f"Internal-storage path is not a regular file: {self.path}"
            )
        if st.st_uid != os.getuid():
            raise SQLiteStorageError(
                f"Internal-storage database {self.path} is owned by uid "
                f"{st.st_uid}, not the current user (uid {os.getuid()})"
            )
        if st.st_size == 0:
            return True

        # Existing non-empty file: must be a SQLite DB carrying our marker.
        try:
            conn = self._connect()
            try:
                app_id = conn.execute("PRAGMA application_id").fetchone()[0]
            finally:
                conn.close()
        except sqlite3.DatabaseError as e:
            raise SQLiteStorageError(
                f"Existing file {self.path} is not a valid SQLite database "
                f"({e}). Move it aside, or delete it if it is disposable "
                "dev state, then restart."
            ) from e

        if app_id != _APPLICATION_ID:
            raise SQLiteStorageError(
                f"Existing SQLite file {self.path} is not an Yggdrasil "
                "internal-storage database (application_id mismatch). "
                "Refusing to reuse an unrelated file."
            )
        return False

    def _validate_schema(self, conn: sqlite3.Connection) -> None:
        """Check schema version; migrate supported older versions.

        Raises:
            SQLiteStorageError: If the schema is newer than this build, or
                older than any supported migration path.
        """
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version == _SCHEMA_VERSION:
            return
        if version > _SCHEMA_VERSION:
            raise SQLiteStorageError(
                f"Internal-storage database {self.path} has schema version "
                f"{version}, newer than this build supports "
                f"({_SCHEMA_VERSION}). Upgrade Yggdrasil."
            )
        self._migrate(conn, version)

    def _migrate(self, conn: sqlite3.Connection, from_version: int) -> None:
        """Migrate a supported older schema in one transaction.

        No older supported schemas exist yet (v1 is the first). Future
        migrations must run inside a transaction so failures roll back;
        the database is never deleted automatically.

        Raises:
            SQLiteStorageError: Always, until a migration path exists.
        """
        raise SQLiteStorageError(
            f"Internal-storage database {self.path} has unsupported schema "
            f"version {from_version}. This dev database is disposable: "
            "delete the file and restart to create fresh state."
        )

    # ------------------------------------------------------------------
    # Connections (one per operation — see module docstring)
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        """Open a fresh connection with WAL-friendly settings."""
        conn = sqlite3.connect(str(self.path), timeout=5.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 5000")
        return conn

    # ------------------------------------------------------------------
    # Document operations
    # ------------------------------------------------------------------

    def get_document(self, namespace: str, doc_id: str) -> dict[str, Any] | None:
        """Fetch a live document; tombstones and misses return None.

        The stored body is returned with ``_rev`` injected (opaque
        revision), mirroring CouchDB document shapes.
        """
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT revision, deleted, body_json FROM documents "
                "WHERE namespace = ? AND document_id = ?",
                (namespace, doc_id),
            ).fetchone()
        finally:
            conn.close()

        if row is None or row["deleted"]:
            return None
        body: dict[str, Any] = json.loads(row["body_json"])
        body["_rev"] = str(row["revision"])
        return body

    def put_document(
        self,
        namespace: str,
        doc_id: str,
        body: dict[str, Any],
        *,
        bump_plan_seq: bool = False,
    ) -> None:
        """Upsert a document; optionally advance the plan-change counter.

        The revision increments on every write. ``_rev`` is stripped from
        the stored body (it is derived state, exposed on read).
        """
        payload = dict(body)
        payload.pop("_rev", None)
        payload.setdefault("_id", doc_id)
        body_json = json.dumps(json_safe(payload), sort_keys=True)
        now = utc_now_iso()

        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT revision, change_seq FROM documents "
                "WHERE namespace = ? AND document_id = ?",
                (namespace, doc_id),
            ).fetchone()
            revision = (row["revision"] + 1) if row else 1
            change_seq = row["change_seq"] if row else None
            if bump_plan_seq:
                change_seq = self._next_plan_seq(conn)
            conn.execute(
                "INSERT INTO documents "
                "(namespace, document_id, revision, change_seq, deleted, "
                " body_json, updated_at) "
                "VALUES (?, ?, ?, ?, 0, ?, ?) "
                "ON CONFLICT (namespace, document_id) DO UPDATE SET "
                "revision = excluded.revision, "
                "change_seq = excluded.change_seq, "
                "deleted = 0, "
                "body_json = excluded.body_json, "
                "updated_at = excluded.updated_at",
                (namespace, doc_id, revision, change_seq, body_json, now),
            )
            conn.execute("COMMIT")
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.OperationalError:
                pass
            raise
        finally:
            conn.close()

    def delete_document(
        self,
        namespace: str,
        doc_id: str,
        *,
        bump_plan_seq: bool = False,
    ) -> bool:
        """Tombstone a document. Returns False if it does not exist."""
        now = utc_now_iso()
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT revision, deleted FROM documents "
                "WHERE namespace = ? AND document_id = ?",
                (namespace, doc_id),
            ).fetchone()
            if row is None or row["deleted"]:
                conn.execute("COMMIT")
                return False
            change_seq = self._next_plan_seq(conn) if bump_plan_seq else None
            conn.execute(
                "UPDATE documents SET revision = ?, change_seq = "
                "COALESCE(?, change_seq), deleted = 1, updated_at = ? "
                "WHERE namespace = ? AND document_id = ?",
                (row["revision"] + 1, change_seq, now, namespace, doc_id),
            )
            conn.execute("COMMIT")
            return True
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.OperationalError:
                pass
            raise
        finally:
            conn.close()

    def touch_document(
        self,
        namespace: str,
        doc_id: str,
        *,
        bump_plan_seq: bool = True,
    ) -> bool:
        """Advance document metadata without rewriting its JSON body.

        The revision, storage timestamp, and optionally the global plan-change
        counter advance in one ``BEGIN IMMEDIATE`` transaction. The stored
        ``body_json`` remains byte-for-byte unchanged.

        Returns:
            False if the document is missing or tombstoned; otherwise True.
        """
        now = utc_now_iso()
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT revision, deleted FROM documents "
                "WHERE namespace = ? AND document_id = ?",
                (namespace, doc_id),
            ).fetchone()
            if row is None or row["deleted"]:
                conn.execute("COMMIT")
                return False

            change_seq = self._next_plan_seq(conn) if bump_plan_seq else None
            conn.execute(
                "UPDATE documents SET revision = ?, change_seq = "
                "COALESCE(?, change_seq), updated_at = ? "
                "WHERE namespace = ? AND document_id = ? AND deleted = 0",
                (row["revision"] + 1, change_seq, now, namespace, doc_id),
            )
            conn.execute("COMMIT")
            return True
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.OperationalError:
                pass
            raise
        finally:
            conn.close()

    def list_documents(self, namespace: str) -> list[dict[str, Any]]:
        """Return all live documents in a namespace (with ``_rev``)."""
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT revision, body_json FROM documents "
                "WHERE namespace = ? AND deleted = 0",
                (namespace,),
            ).fetchall()
        finally:
            conn.close()

        docs: list[dict[str, Any]] = []
        for row in rows:
            body: dict[str, Any] = json.loads(row["body_json"])
            body["_rev"] = str(row["revision"])
            docs.append(body)
        return docs

    # ------------------------------------------------------------------
    # Plan change feed
    # ------------------------------------------------------------------

    def _next_plan_seq(self, conn: sqlite3.Connection) -> int:
        """Advance the global plan-change counter inside the caller's txn."""
        current = int(
            conn.execute(
                "SELECT value FROM store_metadata WHERE key = ?",
                (_PLAN_SEQ_KEY,),
            ).fetchone()["value"]
        )
        nxt = current + 1
        conn.execute(
            "UPDATE store_metadata SET value = ? WHERE key = ?",
            (str(nxt), _PLAN_SEQ_KEY),
        )
        return nxt

    def current_plan_seq(self) -> int:
        """Return the current head of the plan-change counter."""
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT value FROM store_metadata WHERE key = ?",
                (_PLAN_SEQ_KEY,),
            ).fetchone()
        finally:
            conn.close()
        return int(row["value"]) if row else 0

    def plan_changes_after(
        self, cursor: int, limit: int = 500
    ) -> list[tuple[int, str, dict[str, Any] | None, bool]]:
        """Return plan rows changed after ``cursor``, ordered by sequence.

        Returns:
            Tuples of (change_seq, document_id, body-or-None, deleted).
            Tombstones carry ``None`` bodies.
        """
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT change_seq, document_id, revision, deleted, body_json "
                "FROM documents WHERE namespace = ? AND change_seq > ? "
                "ORDER BY change_seq ASC LIMIT ?",
                (_NS_PLANS, cursor, limit),
            ).fetchall()
        finally:
            conn.close()

        changes: list[tuple[int, str, dict[str, Any] | None, bool]] = []
        for row in rows:
            deleted = bool(row["deleted"])
            body: dict[str, Any] | None = None
            if not deleted:
                live_body: dict[str, Any] = json.loads(row["body_json"])
                live_body["_rev"] = str(row["revision"])
                body = live_body
            changes.append((row["change_seq"], row["document_id"], body, deleted))
        return changes


class SQLitePlanStore:
    """PlanStore over :class:`SQLiteInternalStore` (namespace ``plans``).

    Document shapes, eligibility, regeneration, and token semantics are
    shared with the CouchDB backend via ``lib/storage/plan_documents.py``.
    """

    def __init__(
        self,
        store: SQLiteInternalStore,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        """Bind to the shared low-level store."""
        self._store = store
        self._logger = logger or custom_logger(f"{__name__}.{type(self).__name__}")

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
        """Persist a plan document (see PlanStore protocol)."""
        validate_execution_authority(execution_authority)
        doc_id = plan.plan_id
        if not doc_id:
            raise ValueError("plan.plan_id is required for persistence")

        existing = self._store.get_document(_NS_PLANS, doc_id)
        plan_doc = build_plan_document(
            plan,
            realm,
            scope,
            auto_run=auto_run,
            execution_authority=execution_authority,
            execution_owner=execution_owner,
            preview=preview,
            source_doc_id=source_doc_id,
            source_doc_rev=source_doc_rev,
            notes=notes,
            existing=existing,
        )
        self._store.put_document(_NS_PLANS, doc_id, plan_doc, bump_plan_seq=True)
        self._logger.info(
            "Saved plan '%s' (realm=%s, status=%s)",
            doc_id,
            realm,
            plan_doc["status"],
        )
        return doc_id

    def fetch_plan(self, doc_id: str) -> dict[str, Any] | None:
        """Fetch a plan document by ID, or None."""
        return self._store.get_document(_NS_PLANS, doc_id)

    def fetch_plan_as_model(self, doc_id: str) -> Plan | None:
        """Fetch and deserialize the Plan model, or None."""
        doc = self._store.get_document(_NS_PLANS, doc_id)
        if not doc:
            return None
        return plan_model_from_document(doc, self._logger)

    def update_executed_token(
        self,
        doc_id: str,
        run_token: int,
        *,
        max_retries: int = 3,
    ) -> bool:
        """Record a successful execution of ``run_token``.

        ``max_retries`` is accepted for protocol parity; local transactions
        cannot hit CouchDB-style revision conflicts.
        """
        doc = self._store.get_document(_NS_PLANS, doc_id)
        if not doc:
            self._logger.error("Cannot update token: plan '%s' not found", doc_id)
            return False
        doc["executed_run_token"] = run_token
        doc["last_executed_at"] = utc_now_iso()
        doc["updated_at"] = utc_now_iso()
        self._store.put_document(_NS_PLANS, doc_id, doc, bump_plan_seq=True)
        self._logger.debug(
            "Updated executed_run_token=%d for plan '%s'", run_token, doc_id
        )
        return True

    def query_approved_pending(self) -> list[dict[str, Any]]:
        """Return all plans eligible for execution (recovery)."""
        docs = self._store.list_documents(_NS_PLANS)
        eligible = [doc for doc in docs if is_plan_eligible(doc)]
        self._logger.info(
            "Found %d eligible plans (of %d total) for recovery",
            len(eligible),
            len(docs),
        )
        return eligible

    def delete_plan(self, doc_id: str) -> bool:
        """Tombstone a plan document (testing/cleanup)."""
        deleted = self._store.delete_document(_NS_PLANS, doc_id, bump_plan_seq=True)
        if deleted:
            self._logger.info("Deleted plan '%s'", doc_id)
        else:
            self._logger.warning("Cannot delete: plan '%s' not found", doc_id)
        return deleted

    def plan_exists(self, doc_id: str) -> bool:
        """Return True if the plan document exists."""
        return self._store.get_document(_NS_PLANS, doc_id) is not None

    def get_plan_summary(self, doc_id: str) -> dict[str, Any] | None:
        """Return the minimal display summary, or None."""
        doc = self._store.get_document(_NS_PLANS, doc_id)
        if not doc:
            return None
        return plan_summary_from_document(doc)


class SQLitePlanChangeSource:
    """PlanChangeSource polling the SQLite plan-change counter."""

    def __init__(
        self,
        store: SQLiteInternalStore,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        """Bind to the shared low-level store."""
        self._store = store
        self._logger = logger or custom_logger(f"{__name__}.{type(self).__name__}")

    async def stream_changes_continuously(
        self,
        *,
        since: str | int,
        poll_interval_sec: float,
    ) -> AsyncIterator[RawWatchEvent]:
        """Stream plan changes after ``since`` indefinitely.

        Blocking SQLite reads run on worker threads via ``asyncio.to_thread``.
        """
        cursor = await asyncio.to_thread(self._resolve_since, since)
        while True:
            rows = await asyncio.to_thread(self._store.plan_changes_after, cursor)
            for seq, doc_id, body, deleted in rows:
                yield RawWatchEvent(id=doc_id, doc=body, seq=seq, deleted=deleted)
                cursor = seq
            if not rows:
                await asyncio.sleep(poll_interval_sec)

    def _resolve_since(self, since: str | int) -> int:
        """Resolve an opaque cursor to an integer sequence.

        ``"now"`` resolves to the current head. An unparseable cursor (e.g.
        a checkpoint written by a different backend) also starts at the
        current head with a warning. Callers requiring lossless recovery
        must coordinate the recovery scan and cursor handoff described in
        Tech Debt #17.
        """
        if since == "now":
            return self._store.current_plan_seq()
        try:
            return int(since)
        except (TypeError, ValueError):
            head = self._store.current_plan_seq()
            self._logger.warning(
                "Unparseable plan-change cursor %r for SQLite backend; "
                "starting from current head %d",
                since,
                head,
            )
            return head


class SQLiteCheckpointStore(CheckpointStore):
    """CheckpointStore over namespace ``checkpoints``."""

    def __init__(
        self,
        store: SQLiteInternalStore,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        """Bind to the shared low-level store."""
        self._store = store
        self._logger = logger or custom_logger(f"{__name__}.{type(self).__name__}")

    def load(self, backend_key: str) -> Checkpoint | None:
        """Load the checkpoint for ``backend_key``, or None."""
        doc = self._store.get_document(_NS_CHECKPOINTS, backend_key)
        if doc is None:
            return None
        return Checkpoint(
            backend_key=doc.get("backend_key", backend_key),
            value=doc.get("value"),
            updated_at=doc.get("updated_at"),
        )

    def save(self, checkpoint: Checkpoint) -> None:
        """Persist (overwrite) the checkpoint."""
        self._store.put_document(
            _NS_CHECKPOINTS,
            checkpoint.backend_key,
            {
                "backend_key": checkpoint.backend_key,
                "value": checkpoint.value,
                "updated_at": checkpoint.updated_at,
            },
        )


class SQLiteOpsSnapshotSink:
    """OpsSnapshotSink over namespace ``operations_snapshots``.

    Document IDs mirror the CouchDB sink
    (``<partition>:plan_status:<realm>:<plan_id>``) so dev snapshots stay
    representative.
    """

    def __init__(
        self,
        store: SQLiteInternalStore,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        """Bind to the shared low-level store."""
        self._store = store
        self._logger = logger or custom_logger(f"{__name__}.{type(self).__name__}")

    def write(self, plan_dir: Path, snapshot: dict[str, Any]) -> None:
        """Upsert the latest plan_status snapshot (plan_dir unused)."""
        part = partition_key(snapshot.get("scope") or {})
        doc_id = f"{part}:plan_status:{snapshot['realm']}:{snapshot['plan_id']}"
        payload = dict(snapshot)
        payload["_id"] = doc_id
        self._store.put_document(_NS_OPS, doc_id, payload)


def build_sqlite_bundle(cfg: SQLiteInternalStorageConfig) -> InternalStorageBundle:
    """Build the SQLite internal-storage bundle (dev mode/tests only).

    Args:
        cfg: Resolved SQLite configuration (dev-mode gating already applied
            by the config resolver).

    Returns:
        InternalStorageBundle backed by one SQLite database file.
    """
    store = SQLiteInternalStore(cfg.path)
    return InternalStorageBundle(
        backend="sqlite",
        plans=SQLitePlanStore(store),
        plan_changes=SQLitePlanChangeSource(store),
        checkpoints=SQLiteCheckpointStore(store),
        ops_snapshots=SQLiteOpsSnapshotSink(store),
    )
