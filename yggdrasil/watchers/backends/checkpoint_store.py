"""
In-memory checkpoint storage for watcher backends.

The CouchDB-backed store lives in ``yggdrasil.storage.couchdb.checkpoint_store``.
"""

from __future__ import annotations

import logging

from yggdrasil.logging_utils import custom_logger
from yggdrasil.watchers.backends.base import Checkpoint, CheckpointStore


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
