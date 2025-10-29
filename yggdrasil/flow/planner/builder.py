from __future__ import annotations

import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from yggdrasil.flow.model import Plan, StepSpec
from yggdrasil.flow.utils.callable_ref import fn_ref_from_callable

_STEP_ID_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*$")


def _validate_step_id(step_id: str) -> None:
    if not step_id or not _STEP_ID_RE.match(step_id):
        raise ValueError(
            f"Bad step_id '{step_id}': must match ^[A-Za-z][A-Za-z0-9_-]*$"
        )


def _role_key(role: str | Enum) -> str:
    return role.value if isinstance(role, Enum) else str(role)


@dataclass
class PlanBuilder:
    """
    Small helper that:
      - creates stable paths under a <base> dir by semantic 'role'
      - wires dependencies by declaring which roles a step requires/provides
      - accumulates StepSpecs and emits a Plan
    """

    plan_id: str
    realm: str
    scope: dict[str, Any]
    base: Path

    steps: list[StepSpec] = field(default_factory=list)
    _role_provider: dict[str, str] = field(default_factory=dict)  # role -> step_id
    _role_path: dict[str, str] = field(
        default_factory=dict
    )  # role -> absolute path (str)

    # ----- path helpers -----
    def dir_for(self, role: str) -> Path:
        p = self.base / role
        p.mkdir(parents=True, exist_ok=True)
        return p

    def file_for(self, role: str, filename: str) -> Path:
        d = self.dir_for(role)
        return d / filename

    # A more implicit version of the above two helpers
    def path(self, role: str, filename: str | None = None) -> Path:
        return self.file_for(role, filename) if filename else self.dir_for(role)

    def _default_step_id(self, fn: Callable, version: str = "v1") -> str:
        proj = self.scope.get("id", "na")
        return f"{fn.__name__}__{proj}__{version}"

    # ----- role wiring -----
    def provide(self, role: str, path: str, by_step_id: str) -> None:
        """Register that a given step provides a given role at a given path."""
        role = _role_key(role)
        self._role_provider[role] = by_step_id
        self._role_path[role] = path

    def require(self, role: str) -> str:
        """Return the current path for a role (if already provided)."""
        role = _role_key(role)
        path = self._role_path.get(role)
        if not path:
            raise KeyError(f"Role '{role}' not yet provided by any prior step.")
        return path

    # ----- adding steps -----
    def add_step_fn(
        self,
        fn: Callable,
        *,
        step_id: str | None = None,
        params: dict[str, Any],
        inputs: dict[str, str] | None = None,
        provides: dict[str, str] | None = None,
        requires_roles: Iterable[str] = (),
        version: str = "v1",
    ) -> StepSpec:
        sid = step_id or self._default_step_id(fn, version)
        _validate_step_id(sid)

        return self._add_step(
            step_id=sid,
            name=fn.__name__,
            fn_ref=fn_ref_from_callable(fn),
            params=params,
            inputs=inputs,
            provides=provides,
            requires_roles=requires_roles,
        )

    def _add_step(
        self,
        *,
        step_id: str,
        name: str,
        fn_ref: str,
        params: dict[str, Any],
        inputs: dict[str, str] | None = None,
        provides: dict[str, str] | None = None,
        requires_roles: Iterable[str] = (),
    ) -> StepSpec:
        inputs = {_role_key(k): v for k, v in (inputs or {}).items()}
        provides = {_role_key(k): v for k, v in (provides or {}).items()}

        # Infer deps from role keys in addition to requires_roles
        all_required: set[str] = {_role_key(r) for r in requires_roles} | set(
            inputs.keys()
        )
        deps: list[str] = []

        for role in sorted(all_required):
            prov = self._role_provider.get(role)
            if prov:
                if prov != step_id and prov not in deps:
                    deps.append(prov)
            else:
                raise KeyError(
                    f"Step '{name}' requires role '{_role_key(role)}' which has no provider."
                )

        spec = StepSpec(
            step_id=step_id,
            name=name,
            fn_ref=fn_ref,
            params=params,
            deps=deps,
            scope=self.scope,
            inputs=inputs,
        )
        self.steps.append(spec)

        # Register provided roles AFTER we’ve created this step
        for role, path in provides.items():
            self.provide(role, path, by_step_id=step_id)

        return spec

    # ----- finalize -----
    def to_plan(self) -> Plan:
        return Plan(
            plan_id=self.plan_id, realm=self.realm, scope=self.scope, steps=self.steps
        )
