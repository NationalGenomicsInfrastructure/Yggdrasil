"""
Yggdrasil watcher infrastructure.

This package provides:
- Generic watcher backends for external systems (CouchDB, filesystem, etc.)
- WatcherManager for backend lifecycle, deduplication, and fan-out
- WatchSpec / BoundWatchSpec for realm-defined watcher intent
- Filter evaluation for JSON Logic predicates
- Checkpoint storage for resume semantics

Public API:
    # Trigger event types
    from yggdrasil.watchers import EventType

    # Core abstractions
    from yggdrasil.watchers import (
        RawWatchEvent,
        Checkpoint,
        CheckpointStore,
        WatcherBackend,
    )

    # WatchSpec (realm-defined watcher intent)
    from yggdrasil.watchers import WatchSpec, BoundWatchSpec

    # Filter evaluation
    from yggdrasil.watchers import FilterResult, evaluate_filter

    # Backends
    from yggdrasil.watchers import CouchDBBackend

    # Checkpoint stores
    from yggdrasil.watchers import CouchDBCheckpointStore, InMemoryCheckpointStore

    # Manager
    from yggdrasil.watchers import WatcherManager, WatcherBackendGroup

Legacy watchers (deprecated after Phase 3):
    - AbstractWatcher
    - SeqDataWatcher
    - PlanWatcher (excluded from refactor; remains core infrastructure)
"""

from yggdrasil.watchers.backends.base import (
    Checkpoint,
    CheckpointStore,
    RawWatchEvent,
    WatcherBackend,
)
from yggdrasil.watchers.backends.checkpoint_store import (
    CouchDBCheckpointStore,
    InMemoryCheckpointStore,
)
from yggdrasil.watchers.backends.couchdb import CouchDBBackend
from yggdrasil.watchers.config_validation import (
    WatcherConfigurationError,
    WatcherConfigValidationIssue,
    validate_watcher_config_wiring,
)
from yggdrasil.watchers.events import EventType
from yggdrasil.watchers.filter_eval import FilterResult, evaluate_filter
from yggdrasil.watchers.manager import WatcherBackendGroup, WatcherManager
from yggdrasil.watchers.watchspec import BoundWatchSpec, WatchSpec

__all__ = [
    # Trigger event types
    "EventType",
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
    # Config validation
    "WatcherConfigValidationIssue",
    "WatcherConfigurationError",
    "validate_watcher_config_wiring",
]
