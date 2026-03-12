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

    def to_dict(self) -> dict[str, Any]:
        """
        Serialize Plan to a dictionary for database storage.

        Returns:
            dict: Dictionary representation with all fields, including serialized steps.

        Example:
            >>> plan = Plan(plan_id="p1", realm="tenx", scope={"id": "P123"}, steps=[...])
            >>> plan_dict = plan.to_dict()
            >>> # plan_dict can be stored in CouchDB
        """
        return {
            "plan_id": self.plan_id,
            "realm": self.realm,
            "scope": self.scope,
            "steps": [
                {
                    "step_id": s.step_id,
                    "name": s.name,
                    "fn_ref": s.fn_ref,
                    "params": s.params,
                    "deps": s.deps,
                    "scope": s.scope,
                    "inputs": s.inputs,
                }
                for s in self.steps
            ],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Plan:
        """
        Deserialize Plan from a dictionary (typically from database).

        Args:
            data: Dictionary with plan fields (expected from to_dict() or DB storage)

        Returns:
            Plan: Reconstructed Plan instance

        Raises:
            KeyError: If required fields (plan_id, realm) are missing
            TypeError: If steps list contains invalid entries (should not occur with
                      proper validation)

        Note:
            Missing optional fields (deps, scope, inputs) are populated with defaults
            (empty lists/dicts) rather than raising errors. This defensive approach
            handles partial or legacy documents gracefully.

        Example:
            >>> plan_dict = {
            ...     "plan_id": "p1",
            ...     "realm": "tenx",
            ...     "scope": {"id": "P123"},
            ...     "steps": [
            ...         {
            ...             "step_id": "s1",
            ...             "name": "preprocess",
            ...             "fn_ref": "module:fn",
            ...             "params": {},
            ...             "deps": [],
            ...             "scope": {},
            ...             "inputs": {},
            ...         }
            ...     ],
            ... }
            >>> plan = Plan.from_dict(plan_dict)
            >>> assert plan.plan_id == "p1"
        """
        steps = [
            StepSpec(
                step_id=s["step_id"],
                name=s["name"],
                fn_ref=s["fn_ref"],
                params=s["params"],
                deps=s.get("deps", []),
                scope=s.get("scope", {}),
                inputs=s.get("inputs", {}),
            )
            for s in data.get("steps", [])
        ]
        return cls(
            plan_id=data["plan_id"],
            realm=data["realm"],
            scope=data.get("scope", {}),
            steps=steps,
        )


@dataclass
class Artifact:
    key: str  # semantic label, e.g. "library_csv", "cr_outs", "submit_script"
    path: str  # absolute or workdir-relative path
    digest: str | None = None  # sha256 for files or "dirhash:<hex>" for dirs


@dataclass
class StepResult:
    artifacts: list[Artifact] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)
    extra: dict[str, Any] = field(default_factory=dict)
