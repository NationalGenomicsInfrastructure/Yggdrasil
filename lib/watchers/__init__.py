"""
Yggdrasil watcher infrastructure.

This package provides:
- Generic watcher backends for external systems (CouchDB, filesystem, etc.)
- WatcherManager for backend lifecycle, deduplication, and fan-out
- WatchSpec / BoundWatchSpec for realm-defined watcher intent
- Filter evaluation for JSON Logic predicates
- Checkpoint storage for resume semantics

Public API:
    # Core abstractions
    from lib.watchers import (
        RawWatchEvent,
        Checkpoint,
        CheckpointStore,
        WatcherBackend,
    )

    # WatchSpec (realm-defined watcher intent)
    from lib.watchers import WatchSpec, BoundWatchSpec

    # Filter evaluation
    from lib.watchers import FilterResult, evaluate_filter

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
from lib.watchers.filter_eval import FilterResult, evaluate_filter
from lib.watchers.manager import WatcherBackendGroup, WatcherManager
from lib.watchers.watchspec import BoundWatchSpec, WatchSpec

__all__ = [
    # Core abstractions
    "RawWatchEvent",
    "Checkpoint",
    "CheckpointStore",
    "WatcherBackend",
    # WatchSpec
    "WatchSpec",
    "BoundWatchSpec",
    # Filter evaluation
    "FilterResult",
    "evaluate_filter",
    # Backends
    "CouchDBBackend",
    # Checkpoint stores
    "CouchDBCheckpointStore",
    "InMemoryCheckpointStore",
    # Manager
    "WatcherManager",
    "WatcherBackendGroup",
]
