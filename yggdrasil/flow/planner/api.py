from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from yggdrasil.flow.model import Plan

if TYPE_CHECKING:
    from yggdrasil.flow.data_access import DataAccess


class FactsProvider(ABC):
    @abstractmethod
    def distil_facts(self, doc: dict[str, Any]) -> dict[str, Any]:
        """Normalize the source document into a compact facts dict for planning."""
        ...


@dataclass
class PlanningContext:
    realm: str
    scope: dict[str, Any]  # {"kind":"project","id":"P12345"}, etc.
    scope_dir: Path  # ABS path: working root for THIS scope (project/flowcell/...)
    emitter: Any  # EventEmitter (or None; planner can ignore)
    source_doc: dict[str, Any]  # triggering document snapshot
    reason: str  # e.g. "projects/P12345 updated"
    data: DataAccess  # realm-scoped read-only data access; always set by build_planning_context
    realm_config: Mapping[str, Any] | None = None  # optional realm-specific config


@dataclass
class PlanDraft:
    plan: Plan
    auto_run: bool = True  # False => hold for human approval
    approvals_required: list[str] = field(default_factory=list)
    notes: str = ""  # human summary for Genstat
    preview: dict[str, Any] = field(default_factory=dict)  # structured UI data


class Planner(Protocol):
    def generate(self, ctx: PlanningContext) -> PlanDraft: ...
