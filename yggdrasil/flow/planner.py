# yggdrasil/flow/planner.py
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from .model import Plan


@dataclass
class PlanningContext:
    realm: str
    scope: dict[str, Any]  # {"kind":"project","id":"P12345"}, etc.
    work_root: Path  # where planner may stage intake outputs
    emitter: Any  # EventEmitter (or None; planner can ignore)
    source_doc: dict[str, Any]  # triggering document snapshot
    reason: str  # e.g. "projects/P12345 updated"


@dataclass
class PlanDraft:
    plan: Plan
    auto_run: bool = True  # False => hold for human approval
    approvals_required: list[str] = field(default_factory=list)
    notes: str = ""  # human summary for Genstat
    preview: dict[str, Any] = field(default_factory=dict)  # structured UI data


class Planner(Protocol):
    def generate(self, ctx: PlanningContext) -> PlanDraft: ...
