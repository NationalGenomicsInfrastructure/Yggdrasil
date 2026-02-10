"""
WatchSpec & BoundWatchSpec — realm-defined watcher intent.

A *WatchSpec* declares **what** a realm wants to watch and **how** raw
backend events should be transformed into domain-level
:class:`YggdrasilEvent` objects.  It is purely declarative; no backend
logic lives here.

A *BoundWatchSpec* pairs a WatchSpec with the ``realm_id`` that provided
it, enabling the fan-out layer to tag emitted events with realm routing
information.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from lib.watchers.backends.base import RawWatchEvent

from lib.core_utils.event_types import EventType


@dataclass(frozen=True)
class WatchSpec:
    """
    Realm-defined watcher intent.

    ``connection`` fully identifies the resource via the connection config.
    The connection config contains both endpoint (url, auth) and resource
    info (db, path).  This ensures WatchSpec stays declarative while the
    connection config is the source of truth.

    Deleted Event Handling:
        CouchDBBackend emits ``COUCHDB_DOC_CHANGED`` for **all** changes,
        including deletes.  ``RawWatchEvent.deleted`` indicates whether
        the document was deleted.  Realms should check this in their
        ``filter_expr`` or ``build_payload()`` if they need to
        distinguish deletes from updates.

    Attributes:
        backend:
            Backend type identifier (e.g. ``"couchdb"``, ``"fs"``).
        connection:
            Logical connection name (references config; fully
            identifies the watched resource).
        event_type:
            Core :class:`EventType` to emit when filter matches.
        build_scope:
            Callable to extract scope ``dict`` from a
            :class:`RawWatchEvent`.
        build_payload:
            Callable to build payload ``dict`` from a
            :class:`RawWatchEvent`.
        target_handlers:
            Optional list of *handler_id* values to route to.
            If ``None``, routes to **all** handlers subscribing to
            *event_type* within the same realm.
        filter_expr:
            Optional JSON Logic predicate evaluated on the
            :class:`RawWatchEvent` (as a ``dict``).
    """

    backend: str
    connection: str
    event_type: EventType
    build_scope: Callable[[RawWatchEvent], dict[str, Any]]
    build_payload: Callable[[RawWatchEvent], dict[str, Any]]
    target_handlers: list[str] | None = None
    filter_expr: dict[str, Any] | None = None


@dataclass
class BoundWatchSpec:
    """
    WatchSpec bound to its owning realm.

    ``realm_id`` comes from the :class:`RealmDescriptor` that provided
    this WatchSpec.
    """

    spec: WatchSpec
    realm_id: str

    @property
    def backend_group_key(self) -> tuple[str, str]:
        """Key for backend deduplication: ``(backend, connection)``."""
        return (self.spec.backend, self.spec.connection)
