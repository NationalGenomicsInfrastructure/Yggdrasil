"""
RealmDescriptor — unified realm definition.

A RealmDescriptor bundles a realm's identity, handler classes, and watcher
specifications into a single discoverable unit.  External realms expose a
provider function via the ``ygg.realm`` entry-point group::

    [project.entry-points."ygg.realm"]
    my_realm = "my_realm:get_realm_descriptor"

Core discovers all such providers at startup, validates uniqueness and
binding constraints, then wires handlers + watchers into the system.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lib.watchers.watchspec import WatchSpec
    from yggdrasil.flow.base_handler import BaseHandler


@dataclass(frozen=True)
class RealmDescriptor:
    """
    Unified realm definition providing identity, handlers, and watch specifications.

    Discovered via ``ygg.realm`` entry point group.  Each entry point must
    reference a provider function::

        def get_realm_descriptor() -> RealmDescriptor | None

    Attributes:
        realm_id:
            Unique identifier for this realm.  **Required.**  Validated
            unique at startup.  This is the single source of truth for
            realm identity.
        handler_classes:
            List of handler **classes** (not instances).  Core
            instantiates each with no args: ``handler = handler_cls()``.
            Every class must have ``handler_id: ClassVar[str]`` and
            ``event_type: ClassVar[EventType]``.
        watchspecs:
            Either a static list of :class:`WatchSpec` or a callable
            returning one.  The callable form enables dev-mode gating
            (e.g. ``lambda: [] if disabled else [...]``).

    Validation (performed by core at startup):
        - *realm_id* must be non-empty and unique across all realms.
        - Each handler class must have *handler_id* (fatal if missing).
        - *(realm_id, handler_id)* must be unique (fatal on collision).
        - *WatchSpec.target_handlers* must reference valid *handler_id*
          values in this realm.
        - If *target_handlers* is ``None``, at least one handler must
          subscribe to the spec's *event_type*.
    """

    realm_id: str
    handler_classes: list[type[BaseHandler]] = field(default_factory=list)
    watchspecs: list[WatchSpec] | Callable[[], list[WatchSpec]] = field(
        default_factory=list
    )

    def get_watchspecs(self) -> list[WatchSpec]:
        """Resolve watchspecs, invoking callable if needed."""
        if callable(self.watchspecs):
            return self.watchspecs()
        return self.watchspecs
