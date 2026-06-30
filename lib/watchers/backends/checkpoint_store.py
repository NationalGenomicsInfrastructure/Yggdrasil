"""
Checkpoint storage implementations.

This module provides checkpoint persistence for watcher backends.
The default implementation stores checkpoints in the yggdrasil
internal CouchDB database.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, cast

from lib.core_utils.logging_utils import custom_logger
from lib.watchers.backends.base import Checkpoint, CheckpointStore

if TYPE_CHECKING:
    from lib.couchdb.yggdrasil_db_manager import YggdrasilDBManager

# logger = custom_logger(__name__)


class CouchDBCheckpointStore(CheckpointStore):
    """
    Stores checkpoints in the yggdrasil internal database.

    Document schema:
        {
            "_id": "watcher_checkpoint:{backend_key}",
            "type": "watcher_checkpoint",
            "backend_key": str,
            "value": str | int | None,
            "updated_at": str (ISO timestamp)
        }

    Thread safety: Intended for concurrent backend usage via deterministic
    document IDs and CouchDB revision-based updates.

    Example:
        store = CouchDBCheckpointStore()
        cp = store.load("couchdb:projects_db")
        if cp:
            print(f"Resume from: {cp.value}")
    """

    DOC_TYPE = "watcher_checkpoint"
    DOC_ID_PREFIX = "watcher_checkpoint:"
    DEFAULT_SAVE_CONFLICT_RETRIES = 3
    CONCURRENT_WRITER_WARNING = (
        "Concurrent checkpoint writer detected; another Yggdrasil daemon may be "
        "active against the same CouchDB environment. This deployment state is "
        "unsupported."
    )

    def __init__(
        self,
        db_manager: YggdrasilDBManager | None = None,
        logger: logging.Logger | None = None,
    ):
        """
        Initialize the checkpoint store.

        Args:
            db_manager: Optional YggdrasilDBManager instance.
                        If None, creates a new instance.
        """
        self._dbm: YggdrasilDBManager | None = db_manager
        self._logger = logger or custom_logger(f"{__name__}.{type(self).__name__}")

    @property
    def dbm(self) -> YggdrasilDBManager:
        """Lazy initialization of DB manager."""
        if self._dbm is None:
            from lib.couchdb.yggdrasil_db_manager import YggdrasilDBManager

            self._dbm = YggdrasilDBManager()
        return self._dbm

    def _make_doc_id(self, backend_key: str) -> str:
        """Generate document ID for a backend key."""
        return f"{self.DOC_ID_PREFIX}{backend_key}"

    def load(self, backend_key: str) -> Checkpoint | None:
        """
        Load checkpoint for the given backend key.

        Args:
            backend_key: Unique identifier for the backend instance.
                         Format: "{backend}:{connection}"

        Returns:
            Checkpoint if found, None otherwise.
        """
        doc_id = self._make_doc_id(backend_key)

        try:
            doc = self.dbm.fetch_document_by_id(doc_id)
            if doc is None:
                self._logger.debug("No checkpoint found for '%s'", backend_key)
                return None

            checkpoint = Checkpoint(
                backend_key=doc.get("backend_key", backend_key),
                value=doc.get("value"),
                updated_at=doc.get("updated_at"),
            )
            self._logger.debug(
                "Loaded checkpoint for '%s': value='%s', updated=%s",
                backend_key,
                checkpoint.value,
                checkpoint.updated_at,
            )
            return checkpoint

        except Exception as e:
            self._logger.error(
                "Error loading checkpoint for %s: %s",
                backend_key,
                e,
                exc_info=True,
            )
            return None

    @staticmethod
    def _is_conflict_error(exc: Exception) -> bool:
        """
        Return True when an exception represents a CouchDB update conflict.

        Args:
            exc: Exception raised while writing a checkpoint document.

        Returns:
            True if the exception carries a CouchDB/HTTP conflict status, False
            otherwise.

        The IBM SDK exposes the HTTP status as ``status_code`` on current
        versions, while older call paths and some test doubles expose it as
        ``code``. Checking the status attributes keeps conflict handling tied
        to the CouchDB response instead of a particular exception class.
        """
        for attr_name in ("status_code", "code"):
            status = getattr(exc, attr_name, None)
            if status == 409:
                return True
        return False

    def save(self, checkpoint: Checkpoint) -> None:
        """
        Persist checkpoint.

        Args:
            checkpoint: The checkpoint to save. Will overwrite any existing
                        checkpoint with the same backend_key.

        Non-409 database errors are logged and re-raised. 409 conflicts are
        retried with a fresh _rev. If conflicts persist, the checkpoint is left
        unsaved and the caller can continue with its in-memory position.
        """
        doc_id = self._make_doc_id(checkpoint.backend_key)
        total_attempts = self.DEFAULT_SAVE_CONFLICT_RETRIES + 1

        for retry_count in range(total_attempts):
            attempt = retry_count + 1
            doc: dict[str, Any] = {
                "_id": doc_id,
                "type": self.DOC_TYPE,
                "backend_key": checkpoint.backend_key,
                "value": checkpoint.value,
                "updated_at": checkpoint.updated_at,
            }

            try:
                # Check if document exists to get _rev for update
                existing = self.dbm.fetch_document_by_id(doc_id)
                if existing and existing.get("value") == checkpoint.value:
                    self._logger.debug(
                        "Checkpoint unchanged for '%s'; skipping save",
                        checkpoint.backend_key,
                    )
                    return
                if existing and "_rev" in existing:
                    doc["_rev"] = existing["_rev"]

                # Use put_document for upsert semantics
                # Cast to Any to satisfy Pylance (SDK expects Document | BinaryIO)
                self.dbm.server.put_document(
                    db=self.dbm.db_name,
                    doc_id=doc_id,
                    document=cast(Any, doc),
                ).get_result()

                self._logger.debug(
                    "Saved checkpoint for '%s': value='%s'",
                    checkpoint.backend_key,
                    checkpoint.value,
                )
                return

            except Exception as e:
                if self._is_conflict_error(e):
                    self._logger.warning(
                        "%s backend_key='%s', attempt=%d/%d",
                        self.CONCURRENT_WRITER_WARNING,
                        checkpoint.backend_key,
                        attempt,
                        total_attempts,
                    )
                    continue

                self._logger.error(
                    "Error saving checkpoint for '%s': %s",
                    checkpoint.backend_key,
                    e,
                    exc_info=True,
                )
                raise

        self._logger.error(
            "Failed to save checkpoint for '%s' after %d conflict retries; "
            "continuing without persisting checkpoint value='%s'",
            checkpoint.backend_key,
            self.DEFAULT_SAVE_CONFLICT_RETRIES,
            checkpoint.value,
        )


class InMemoryCheckpointStore(CheckpointStore):
    """
    In-memory checkpoint store for testing.

    Checkpoints are stored in a dict and lost on process exit.
    Thread-safe for basic use cases (dict operations are atomic in CPython).
    """

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._checkpoints: dict[str, Checkpoint] = {}
        self._logger = logger or custom_logger(f"{__name__}.{type(self).__name__}")

    def load(self, backend_key: str) -> Checkpoint | None:
        """Load checkpoint from memory."""
        cp = self._checkpoints.get(backend_key)
        if cp:
            self._logger.debug(
                "Loaded checkpoint for '%s': '%s'", backend_key, cp.value
            )
        return cp

    def save(self, checkpoint: Checkpoint) -> None:
        """Save checkpoint to memory."""
        self._checkpoints[checkpoint.backend_key] = checkpoint
        self._logger.debug(
            "Saved checkpoint for '%s': value='%s'",
            checkpoint.backend_key,
            checkpoint.value,
        )

    def clear(self) -> None:
        """Clear all checkpoints (for testing)."""
        self._checkpoints.clear()
