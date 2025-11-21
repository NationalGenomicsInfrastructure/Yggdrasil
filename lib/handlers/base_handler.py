import asyncio
from abc import ABC, abstractmethod
from typing import Any, ClassVar

from yggdrasil.core_utils.event_types import EventType  # type: ignore


class BaseHandler(ABC):
    """
    All handlers must implement:
      - __call__: for async dispatch (under the running event loop)
      - handle_async: the actual async work
      - run_now: a sync wrapper for one-off CLI use
    """

    # realm authors must set this
    event_type: ClassVar[EventType]

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
    async def handle_task(self, payload: dict[str, Any]) -> None:
        """
        Coroutine that does the real work.
        Subclasses implement this (e.g. resolving realm, running it).
        """
        ...

    @abstractmethod
    def __call__(self, payload: dict[str, Any]) -> None:
        """
        Schedule handle_async under asyncio.create_task().
        """
        ...

    def run_now(self, payload: dict[str, Any]) -> None:
        """
        Blocking, one-off entrypoint for CLI mode.
        Simply runs handle_async() to completion.
        """
        asyncio.run(self.handle_task(payload))
