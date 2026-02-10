"""
Yggdrasil watcher infrastructure.

This package provides:
- Generic watcher backends for external systems (CouchDB, filesystem, etc.)
- WatcherManager for backend lifecycle and deduplication
- Checkpoint storage for resume semantics

Public API:
    # Core abstractions
    from lib.watchers import (
        RawWatchEvent,
        Checkpoint,
        CheckpointStore,
        WatcherBackend,
    )

    # Backends
    from lib.watchers import CouchDBBackend

    # Checkpoint stores
    from lib.watchers import CouchDBCheckpointStore, InMemoryCheckpointStore

    # Manager
    from lib.watchers import WatcherManager, WatcherBackendGroup

Legacy watchers (deprecated after Phase 3):
    - AbstractWatcher
    - CouchDBWatcher
    - SeqDataWatcher
    - PlanWatcher (excluded from refactor; remains core infrastructure)
"""

from lib.watchers.backends.base import (
    Checkpoint,
    CheckpointStore,
    RawWatchEvent,
    WatcherBackend,
)
from lib.watchers.backends.checkpoint_store import (
    CouchDBCheckpointStore,
    InMemoryCheckpointStore,
)
from lib.watchers.backends.couchdb import CouchDBBackend
from lib.watchers.manager import WatcherBackendGroup, WatcherManager

__all__ = [
    # Core abstractions
    "RawWatchEvent",
    "Checkpoint",
    "CheckpointStore",
    "WatcherBackend",
    # Backends
    "CouchDBBackend",
    # Checkpoint stores
    "CouchDBCheckpointStore",
    "InMemoryCheckpointStore",
    # Manager
    "WatcherManager",
    "WatcherBackendGroup",
]
