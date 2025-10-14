from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class StepSpec:
    step_id: str  # e.g. "cellranger_multi__P12345_1001__9b7f"
    name: str  # e.g. "cellranger_multi"
    fn_ref: str  # dotted path to the @step function
    params: dict[str, Any]  # kwargs passed to the function
    deps: list[str] = field(default_factory=list)
    scope: dict[str, Any] = field(default_factory=dict)
    inputs: dict[str, str] = field(default_factory=dict)


@dataclass
class Plan:
    plan_id: str
    realm: str
    scope: dict[str, Any]
    steps: list[StepSpec] = field(default_factory=list)


@dataclass
class Artifact:
    role: str  # semantic label, e.g. "library_csv", "cr_outs", "submit_script"
    path: str  # absolute or workdir-relative path
    digest: str | None = None  # sha256 for files or "dirhash:<hex>" for dirs


@dataclass
class StepResult:
    artifacts: list[Artifact] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)
    extra: dict[str, Any] = field(default_factory=dict)
