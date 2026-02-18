"""
Checkpoint storage implementations.

This module provides checkpoint persistence for watcher backends.
The default implementation stores checkpoints in the yggdrasil
internal CouchDB database.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, cast

from lib.watchers.backends.base import Checkpoint, CheckpointStore

if TYPE_CHECKING:
    from lib.couchdb.yggdrasil_db_manager import YggdrasilDBManager

logger = logging.getLogger(__name__.split(".")[-1])


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

    def __init__(self, db_manager: YggdrasilDBManager | None = None):
        """
        Initialize the checkpoint store.

        Args:
            db_manager: Optional YggdrasilDBManager instance.
                        If None, creates a new instance.
        """
        self._dbm: YggdrasilDBManager | None = db_manager
        self._logger = logger or logging.getLogger(
            f"{__name__}.{self.__class__.__name__}"
        )

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

    def save(self, checkpoint: Checkpoint) -> None:
        """
        Persist checkpoint.

        Args:
            checkpoint: The checkpoint to save. Will overwrite any existing
                        checkpoint with the same backend_key.

        Raises:
            Exception: If database write fails (logged but re-raised).
        """
        doc_id = self._make_doc_id(checkpoint.backend_key)

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

        except Exception as e:
            self._logger.error(
                "Error saving checkpoint for '%s': %s",
                checkpoint.backend_key,
                e,
                exc_info=True,
            )
            raise


class InMemoryCheckpointStore(CheckpointStore):
    """
    In-memory checkpoint store for testing.

    Checkpoints are stored in a dict and lost on process exit.
    Thread-safe for basic use cases (dict operations are atomic in CPython).
    """

    def __init__(self) -> None:
        self._checkpoints: dict[str, Checkpoint] = {}
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

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
