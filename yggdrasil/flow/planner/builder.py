from __future__ import annotations

import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Literal

from yggdrasil.flow.artifacts import ensure_artifact_ref
from yggdrasil.flow.model import Plan, StepSpec
from yggdrasil.flow.utils.callable_ref import fn_ref_from_callable

_STEP_ID_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*$")


def _validate_step_id(step_id: str) -> None:
    if not step_id or not _STEP_ID_RE.match(step_id):
        raise ValueError(
            f"Bad step_id '{step_id}': must match ^[A-Za-z][A-Za-z0-9_-]*$"
        )


def _artifact_key(key: str | Enum) -> str:
    return key.value if isinstance(key, Enum) else str(key)


def _maybe_inject_params(
    fn,
    params: dict[str, Any],
    base: Path,
    mode: Literal["auto", "none", "inputs", "outputs"] = "auto",
) -> dict[str, Any]:

    p = dict(params)  # don’t mutate caller
    if mode in ("auto", "inputs"):
        for pname, aref in getattr(fn, "__step_inputs__", {}).items():
            p.setdefault(pname, ensure_artifact_ref(aref).resolve_path(base))
    if mode in ("auto", "outputs"):
        for pname, aref in getattr(fn, "__step_outputs__", {}).items():
            p.setdefault(pname, ensure_artifact_ref(aref).resolve_path(base))
    return p


@dataclass
class PlanBuilder:
    """
    Small helper that:
      - creates stable paths under a <base> dir by semantic artifact key
      - wires dependencies by declaring which artifact keys a step requires/provides
      - accumulates StepSpecs and emits a Plan
    """

    plan_id: str
    realm: str
    scope: dict[str, Any]
    base: Path

    steps: list[StepSpec] = field(default_factory=list)
    _artifact_provider: dict[str, str] = field(default_factory=dict)  # key -> step_id
    _artifact_path: dict[str, str] = field(default_factory=dict)  # key -> abs path

    # ----- path helpers -----
    def artifact_path(self, ref: object) -> Path:
        aref = ensure_artifact_ref(ref)
        return aref.resolve_path(self.base)  # ensures workspace exists

    # Get the *workspace directory* for a given artifact ref
    def artifact_workspace(self, ref: object) -> Path:
        aref = ensure_artifact_ref(ref)
        p = aref.resolve_path(self.base)
        # If the resolved path looks like a file (has a suffix), use its parent.
        # Otherwise treat it as a directory.
        ws = p.parent if p.suffix else p
        ws.mkdir(parents=True, exist_ok=True)
        return ws

    def _default_step_id(self, fn: Callable, version: str = "v1") -> str:
        proj = self.scope.get("id", "na")
        return f"{fn.__name__}__{proj}__{version}"

    def _map_from_refs(self, refs: list[object] | None) -> dict[str, str]:
        m: dict[str, str] = {}
        if not refs:
            return m
        for ref in refs:
            aref = ensure_artifact_ref(ref)
            m[aref.key()] = str(aref.resolve_path(self.base))
        return m

    # ----- Artifact registry: key → (path, producer). Drives deps + path lookups -----
    def record_artifact(self, key: str | Enum, path: str, by_step_id: str) -> None:
        """Remember that step `by_step_id` produced artifact `key` at `path`."""
        k = _artifact_key(key)
        self._artifact_provider[k] = by_step_id
        self._artifact_path[k] = path

    def path_for(self, key: str | Enum) -> str:
        """Return the known path for artifact `key` (must have been produced already)."""
        k = _artifact_key(key)
        try:
            return self._artifact_path[k]
        except KeyError as e:
            raise KeyError(
                f"Artifact '{k}' has no known path (no prior step produced it)."
            ) from e

    # ----- adding steps -----
    def add_step_fn(
        self,
        fn: Callable,
        *,
        step_id: str | None = None,
        params: dict[str, Any] | None = None,
        requires_artifacts: Iterable[str | Enum] = (),  # new wiring
        inject_io: Literal["auto", "none", "inputs", "outputs"] = "auto",
        version: str = "v1",
    ) -> StepSpec:
        sid = step_id or self._default_step_id(fn, version)

        # Ensure a fresh dict (and don’t mutate the caller’s)
        params = {} if params is None else dict(params)

        # _validate_step_id(sid) --> done in _add_step below

        ann_in = list(getattr(fn, "__step_inputs__", {}).values())
        ann_out = list(getattr(fn, "__step_outputs__", {}).values())

        in_map = self._map_from_refs(ann_in)  # {key -> abs path}
        out_map = self._map_from_refs(ann_out)  # {key -> abs path}

        # Optional convenience: pre-fill kwargs with resolved paths
        call_params = _maybe_inject_params(fn, params, self.base, mode=inject_io)

        return self._add_step(
            step_id=sid,
            name=fn.__name__,
            fn_ref=fn_ref_from_callable(fn),
            params=call_params,
            inputs=in_map,
            outputs=out_map,
            requires_artifacts=requires_artifacts,
        )

    def _add_step(
        self,
        *,
        step_id: str,
        name: str,
        fn_ref: str,
        params: dict[str, Any] | None = None,
        inputs: dict[str, str] | None = None,
        outputs: dict[str, str] | None = None,
        requires_artifacts: Iterable[str | Enum] = (),
    ) -> StepSpec:
        _validate_step_id(step_id)

        params = {} if params is None else params
        inputs = {_artifact_key(k): v for k, v in (inputs or {}).items()}
        outputs = {_artifact_key(k): v for k, v in (outputs or {}).items()}

        # Infer deps from artifact keys in addition to requires_artifacts
        required_keys: set[str] = {_artifact_key(r) for r in requires_artifacts} | set(
            inputs.keys()
        )
        deps: list[str] = []

        for key in sorted(required_keys):
            prov = self._artifact_provider.get(key)
            if prov:
                if prov != step_id and prov not in deps:
                    deps.append(prov)
            else:
                raise KeyError(
                    f"Step '{name}' requires artifact '{key}' which has no provider."
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

        # Register provided artifact keys AFTER we’ve created this step
        for key, path in outputs.items():
            self.record_artifact(key, path, by_step_id=step_id)

        return spec

    # ----- finalize -----
    def to_plan(self) -> Plan:
        return Plan(
            plan_id=self.plan_id, realm=self.realm, scope=self.scope, steps=self.steps
        )
