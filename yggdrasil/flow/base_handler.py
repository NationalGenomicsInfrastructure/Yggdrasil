from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

from lib.core_utils.event_types import EventType
from yggdrasil.flow.planner.api import PlanDraft, PlanningContext

if TYPE_CHECKING:
    from yggdrasil.flow.data_access import DataAccess


class BaseHandler(ABC):
    """
    All handlers must implement:
      - handler_id: stable identifier within the realm (REQUIRED)
      - event_type: which EventType this handler subscribes to
      - generate_plan_drafts: async method that returns a list[PlanDraft]
      - derive_scope: extract scope from document

    Handler provisioning (v1):
      - Handlers are provided as CLASSES via RealmDescriptor.handler_classes
      - Core instantiates each with no args: handler = handler_cls()
      - Factories/DI are deferred to future versions

    YggdrasilCore will:
      - Call generate_plan_drafts() to get a list[PlanDraft]
      - Persist each plan to database
      - Check for approval requests
      - Pass plans to Engine for execution
    """

    # Realm authors MUST set these class variables
    event_type: ClassVar[EventType]
    handler_id: ClassVar[str]  # Stable identifier within realm

    # Set by core during registration (do not set manually)
    realm_id: str | None = None

    # ---------- identity helpers ----------
    @classmethod
    def class_qualified_name(cls) -> str:
        """Return fully qualified class name: '<module>.<qualname>'."""
        return f"{cls.__module__}.{cls.__qualname__}"

    @classmethod
    def class_key(cls) -> tuple[str, str]:
        """Stable identity: (module, qualname)."""
        return (cls.__module__, cls.__qualname__)

    # ---------- data access ----------

    def _require_realm_id(self) -> str:
        """Return realm_id or raise if it has not been set by Core.

        Raises:
            RuntimeError: If ``realm_id`` is None (handler not yet registered).
        """
        if not self.realm_id:
            raise RuntimeError(
                f"{type(self).__name__}.realm_id is not set. "
                "Core must set it during registration (or set it manually in tests)."
            )
        return self.realm_id

    @property
    def _data_access(self) -> DataAccess:
        """Lazy DataAccess instance, created once per handler on first access."""
        try:
            return self.__data_access  # type: ignore[attr-defined]
        except AttributeError:
            from yggdrasil.flow.data_access import DataAccess

            self.__data_access: DataAccess = DataAccess(self._require_realm_id())
            return self.__data_access

    def build_planning_context(
        self,
        *,
        scope: dict[str, Any],
        scope_dir: Path,
        emitter: Any,
        source_doc: dict[str, Any],
        reason: str,
        realm_config: Mapping[str, Any] | None = None,
    ) -> PlanningContext:
        """Construct a PlanningContext with data access always injected.

        This is the only supported way for realm authors to build a
        :class:`PlanningContext`.  It derives ``realm`` from ``self.realm_id``
        and always injects ``data=self._data_access`` so that
        ``PlanningContext.__post_init__`` never raises.

        Args:
            scope: Scope dict, e.g. ``{"kind": "flowcell", "id": "ABCDE"}``.
            scope_dir: Absolute working directory for this scope.
            emitter: EventEmitter instance (or None).
            source_doc: Triggering CouchDB document snapshot.
            reason: Human-readable trigger description.
            realm_config: Optional realm-specific configuration.

        Returns:
            A fully populated :class:`PlanningContext`.
        """
        return PlanningContext(
            realm=self._require_realm_id(),
            scope=scope,
            scope_dir=scope_dir,
            emitter=emitter,
            source_doc=source_doc,
            reason=reason,
            realm_config=realm_config,
            data=self._data_access,
        )

    @abstractmethod
    def derive_scope(self, doc: dict[str, Any]) -> dict[str, Any]:
        """
        Return {'kind': <string>, 'id': <string>} for this document.
        Examples: {'kind':'project','id': P12345} or {'kind':'flowcell','id': FCID}.
        """
        ...

    @abstractmethod
    async def generate_plan_drafts(self, payload: dict[str, Any]) -> list[PlanDraft]:
        """
        Generate PlanDrafts from the trigger payload.

        Returns:
            list[PlanDraft]: One or more drafts. Single-plan handlers return a
            one-element list. Fan-out handlers (e.g. demux) return one draft per
            independent plan (lane, samplesheet entry, etc.).

        This replaces the old handle_task pattern. Handlers now only generate plans;
        YggdrasilCore handles persistence, approval routing, and engine execution.
        """
        ...

    def run_now(self, payload: dict[str, Any]) -> list[PlanDraft]:
        """
        Blocking, one-off entrypoint for CLI mode.

        Runs generate_plan_drafts() to completion and returns the list of drafts.
        Must be called from a synchronous context (no running event loop).

        Raises:
            RuntimeError: If called from within an async context.
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            # No running loop - safe to use asyncio.run()
            return asyncio.run(self.generate_plan_drafts(payload))

        # If we get here, there IS a running loop - raise explicit error
        raise RuntimeError(
            "run_now() cannot be called from within an async context. "
            "Use 'await handler.generate_plan_drafts(payload)' instead."
        )
