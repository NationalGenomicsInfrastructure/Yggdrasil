"""
Yggdrasil watcher infrastructure.

This package provides:
- Trigger event types and the event container (``EventType``, ``YggdrasilEvent``)
- WatchSpec / BoundWatchSpec for realm-defined watcher intent
- Generic watcher backends for external systems (CouchDB, filesystem, etc.)
- WatcherManager for backend lifecycle, deduplication, and fan-out
- Filter evaluation for JSON Logic predicates
- Checkpoint storage for resume semantics

Public API:
    # Realm-facing types (imported eagerly; standard library only)
    from yggdrasil.watchers import EventType, YggdrasilEvent, WatchSpec, BoundWatchSpec

    # Everything below is imported on first access, so importing the package
    # stays free of logging setup and the CouchDB client.
    from yggdrasil.watchers import (
        RawWatchEvent,
        Checkpoint,
        CheckpointStore,
        WatcherBackend,
    )
    from yggdrasil.watchers import FilterResult, evaluate_filter
    from yggdrasil.watchers import CouchDBBackend
    from yggdrasil.watchers import InMemoryCheckpointStore
    # (CouchDBCheckpointStore lives in yggdrasil.storage.couchdb.checkpoint_store)
    from yggdrasil.watchers import WatcherManager, WatcherBackendGroup

Legacy watchers (deprecated after Phase 3):
    - AbstractWatcher
    - SeqDataWatcher
    - PlanWatcher (excluded from refactor; remains core infrastructure)
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

from yggdrasil.watchers.events import EventType, YggdrasilEvent
from yggdrasil.watchers.watchspec import BoundWatchSpec, WatchSpec

if TYPE_CHECKING:
    from yggdrasil.watchers.backends.base import (
        Checkpoint,
        CheckpointStore,
        RawWatchEvent,
        WatcherBackend,
    )
    from yggdrasil.watchers.backends.checkpoint_store import InMemoryCheckpointStore
    from yggdrasil.watchers.backends.couchdb import CouchDBBackend
    from yggdrasil.watchers.config_validation import (
        WatcherConfigurationError,
        WatcherConfigValidationIssue,
        validate_watcher_config_wiring,
    )
    from yggdrasil.watchers.filter_eval import FilterResult, evaluate_filter
    from yggdrasil.watchers.manager import WatcherBackendGroup, WatcherManager

_LAZY_EXPORTS: dict[str, str] = {
    "RawWatchEvent": "yggdrasil.watchers.backends.base",
    "Checkpoint": "yggdrasil.watchers.backends.base",
    "CheckpointStore": "yggdrasil.watchers.backends.base",
    "WatcherBackend": "yggdrasil.watchers.backends.base",
    "InMemoryCheckpointStore": "yggdrasil.watchers.backends.checkpoint_store",
    "CouchDBBackend": "yggdrasil.watchers.backends.couchdb",
    "WatcherConfigValidationIssue": "yggdrasil.watchers.config_validation",
    "WatcherConfigurationError": "yggdrasil.watchers.config_validation",
    "validate_watcher_config_wiring": "yggdrasil.watchers.config_validation",
    "FilterResult": "yggdrasil.watchers.filter_eval",
    "evaluate_filter": "yggdrasil.watchers.filter_eval",
    "WatcherManager": "yggdrasil.watchers.manager",
    "WatcherBackendGroup": "yggdrasil.watchers.manager",
}


def __getattr__(name: str) -> Any:
    """Import a lazily exported name on first access and cache it on the package.

    Args:
        name: Attribute requested from the package.

    Returns:
        The exported object.

    Raises:
        AttributeError: If ``name`` is not an export of this package.
    """
    module_path = _LAZY_EXPORTS.get(name)
    if module_path is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module_path), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """List the package's attributes including the lazy exports."""
    return sorted(set(globals()) | set(_LAZY_EXPORTS))


__all__ = [
    # Trigger event types
    "EventType",
    "YggdrasilEvent",
    # WatchSpec
    "WatchSpec",
    "BoundWatchSpec",
    # Core abstractions
    "RawWatchEvent",
    "Checkpoint",
    "CheckpointStore",
    "WatcherBackend",
    # Filter evaluation
    "FilterResult",
    "evaluate_filter",
    # Backends
    "CouchDBBackend",
    # Checkpoint stores
    "InMemoryCheckpointStore",
    # Manager
    "WatcherManager",
    "WatcherBackendGroup",
    # Config validation
    "WatcherConfigValidationIssue",
    "WatcherConfigurationError",
    "validate_watcher_config_wiring",
]
