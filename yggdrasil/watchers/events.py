import datetime
from enum import Enum
from typing import Any


class EventType(str, Enum):
    """
    Core event types for Yggdrasil.

    Generic Ingress Types (from watcher backends):
        These are backend-sourced events. Realms use filter_expr and
        target_handlers in WatchSpec to route to specific handlers.

    Internal Types:
        Used by core infrastructure (e.g., PlanWatcher).

    Legacy Types (deprecated):
        Domain-specific types from pre-refactor. Will be removed
        after migration to generic ingress types.
    """

    # --- Generic Ingress Types (NEW) ---
    # CouchDB backend events
    COUCHDB_DOC_CHANGED = "couchdb_doc_changed"
    COUCHDB_DOC_DELETED = "couchdb_doc_deleted"

    # Filesystem backend events (future)
    # FS_FILE_CREATED = "fs_file_created"
    # FS_FILE_MODIFIED = "fs_file_modified"
    # FS_FILE_DELETED = "fs_file_deleted"

    # --- Internal Types ---
    PLAN_EXECUTION = "plan_execution"  # PlanWatcher → Engine

    # --- Legacy Types (deprecated, remove after migration) ---
    PROJECT_CHANGE = "project_change"
    FLOWCELL_READY = "flowcell_ready"
    DELIVERY_READY = "delivery_ready"
    TEST_SCENARIO_CHANGE = "test_scenario_change"


class YggdrasilEvent:
    """
    A lightweight container for events that watchers produce and YggdrasilCore consumes.

    Attributes:
        event_type (EventType): The event type (e.g. EventType.PROJECT_CHANGE).
        payload (Any): Arbitrary data relevant to the event (e.g. info about a changed file).
        source (str): Identifier of the event source (e.g. "filesystem", "couchdb").
        timestamp (datetime.datetime): When the event was created.
    """

    __slots__ = ("event_type", "payload", "source", "timestamp")

    def __init__(self, event_type: EventType | str, payload: Any, source: str):
        if isinstance(event_type, EventType):
            normalized = event_type
        elif isinstance(event_type, str):
            try:
                normalized = EventType(event_type)
            except ValueError:
                try:
                    normalized = EventType[event_type]
                except KeyError as exc:
                    raise ValueError(f"Unknown event_type: {event_type!r}") from exc
        else:
            raise TypeError(
                "event_type must be an EventType or str convertible to EventType"
            )

        self.event_type: EventType = normalized
        self.payload = payload
        self.source = source
        self.timestamp = datetime.datetime.now()

    def __repr__(self) -> str:
        return (
            f"YggdrasilEvent("
            f"event_type={self.event_type!r}, "
            f"payload={self.payload!r}, "
            f"source={self.source!r}, "
            f"timestamp={self.timestamp.isoformat()})"
        )
