import asyncio
from abc import ABC, abstractmethod
from typing import Any, ClassVar

from lib.core_utils.event_types import EventType
from yggdrasil.flow.planner.api import PlanDraft


class BaseHandler(ABC):
    """
    All handlers must implement:
      - handler_id: stable identifier within the realm (REQUIRED)
      - event_type: which EventType this handler subscribes to
      - generate_plan_draft: async method that returns a PlanDraft
      - derive_scope: extract scope from document
      - __call__: for async dispatch (under the running event loop)

    Handler provisioning (v1):
      - Handlers are provided as CLASSES via RealmDescriptor.handler_classes
      - Core instantiates each with no args: handler = handler_cls()
      - Factories/DI are deferred to future versions

    YggdrasilCore will:
      - Call generate_plan_draft() to get a PlanDraft
      - Persist the plan to database
      - Check for approval requests
      - Pass plan to Engine for execution
    """

    # Realm authors MUST set these class variables
    event_type: ClassVar[EventType]
    handler_id: ClassVar[str]  # Stable identifier within realm

    # Set by core during registration (do not set manually)
    realm_id: str | None = None

    # ---------- identity helpers ----------
    @classmethod
    def class_qualified_name(cls) -> str:
        """e.g. '<realm>.yggdrasil_realm.project_handler.<RealmProjectHandler>'"""
        return f"{cls.__module__}.{cls.__qualname__}"

    @classmethod
    def class_key(cls) -> tuple[str, str]:
        """Stable identity: (module, qualname)."""
        return (cls.__module__, cls.__qualname__)

    @abstractmethod
    def derive_scope(self, doc: dict[str, Any]) -> dict[str, Any]:
        """
        Return {'kind': <string>, 'id': <string>} for this document.
        Examples: {'kind':'project','id': P12345} or {'kind':'flowcell','id': FCID}.
        """
        ...

    @abstractmethod
    async def generate_plan_draft(self, payload: dict[str, Any]) -> PlanDraft:
        """
        Generate a PlanDraft from the trigger payload.

        Returns:
            PlanDraft: Contains plan + auto_run flag + approvals_required + notes.

        This replaces the old handle_task pattern. Handlers now only generate plans;
        YggdrasilCore handles persistence, approval routing, and engine execution.
        """
        ...

    @abstractmethod
    def __call__(self, payload: dict[str, Any]) -> None:
        """
        Schedule generate_plan_draft under asyncio.create_task().
        YggdrasilCore will await the result and handle plan persistence/execution.
        """
        ...

    def run_now(self, payload: dict[str, Any]) -> PlanDraft:
        """
        Blocking, one-off entrypoint for CLI mode.
        Simply runs generate_plan_draft() to completion and returns the draft.
        """
        return asyncio.run(self.generate_plan_draft(payload))
