"""
Watcher backend implementations.

This package provides generic watcher backends for various external systems.
Backends emit RawWatchEvent objects and manage their own checkpoints.

Public API:
- RawWatchEvent: Backend-agnostic event dataclass
- Checkpoint: Resume-from-last-position marker
- CheckpointStore: Abstract interface for checkpoint persistence
- WatcherBackend: Abstract base class for all backends
- CouchDBBackend: CouchDB _changes feed backend
- CouchDBCheckpointStore: Default checkpoint storage in yggdrasil DB
"""

from lib.watchers.backends.base import (
    Checkpoint,
    CheckpointStore,
    RawWatchEvent,
    WatcherBackend,
)

__all__ = [
    "RawWatchEvent",
    "Checkpoint",
    "CheckpointStore",
    "WatcherBackend",
]
