from __future__ import annotations

import hashlib
import json
import logging
import os
import uuid
from pathlib import Path
from typing import Any

from lib.core_utils.logging_utils import custom_logger
from yggdrasil.flow.errors import PermanentStepError, TransientStepError
from yggdrasil.flow.events.emitter import EventEmitter, FileSpoolEmitter
from yggdrasil.flow.model import Plan, StepResult, StepSpec
from yggdrasil.flow.step import StepContext
from yggdrasil.flow.utils.callable_ref import resolve_callable
from yggdrasil.flow.utils.hash import dirhash_stats, sha256_file
from yggdrasil.flow.utils.typing_coerce import coerce_params_to_signature_types
from yggdrasil.flow.utils.ygg_time import utcnow_compact, utcnow_iso

logger = custom_logger(__name__)


# ------------ Utilities ------------


def _json_sha256(data: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(data, sort_keys=True, default=str).encode()
    ).hexdigest()


def _short(h: str, n: int = 4) -> str:
    return h[:n]


def _new_run_id() -> str:
    return f"run_{utcnow_compact()}_{uuid.uuid4().hex[:6]}"


def _looks_like_path(v: Any) -> bool:
    return isinstance(v, str) and ("/" in v or "\\" in v)


def _lint_missing_inputs(spec: StepSpec, fn: Any) -> None:
    """Warn if a step has path-like params but no declared inputs."""
    has_declared = bool(spec.inputs) or bool(getattr(fn, "_input_keys", ()))
    if has_declared:
        return
    suspicious = [k for k, v in spec.params.items() if _looks_like_path(v)]
    if suspicious:
        logger.warning(
            '%s has path-like params "%s" but no declared inputs ',
            spec.step_id,
            suspicious,
        )


def _default_fingerprint(spec: StepSpec, fn: Any) -> str:
    """
    Fingerprint = sha256(JSON of params + digests of declared inputs).
    Inputs are either:
      - spec.inputs (planner-provided), or
      - fn._input_keys taken from params (decorator-declared), or
      - none (params-only fallback).
    """
    enriched: dict[str, Any] = {"params": spec.params}

    # (a) planner-provided inputs
    declared: dict[str, str] = dict(spec.inputs)

    # (b) else, use step-declared input_keys to pull paths from params
    if not declared:
        input_keys = getattr(fn, "_input_keys", ())

        for key in input_keys:
            value = spec.params.get(key)
            # Accept Path or str (defensive: accept Path values if provided)
            if isinstance(value, str | Path):
                declared[key] = str(value)

    # Hash only declared inputs
    for key, path_str in declared.items():
        p = Path(path_str)
        if p.is_dir():
            enriched[f"input:{key}:dirhash"] = dirhash_stats(p)
        elif p.is_file():
            enriched[f"input:{key}:sha256"] = f"sha256:{sha256_file(p)}"
        else:
            enriched[f"input:{key}:missing"] = True

    return f"sha256:{_json_sha256(enriched)}"


# ------------ Engine ------------


class Engine:
    """
    Minimal sequential executor with:
    - plan dir + plan.json
    - per-step workdir
    - fingerprint + cache skip (file-based)
    - event spool emission via StepContext (handled by @step decorator)
    """

    def __init__(
        self,
        work_root: str | Path | None = None,
        emitter: EventEmitter | None = None,
        logger: logging.Logger | None = None,
    ):
        self._logger = logger or custom_logger(f"{__name__}.{type(self).__name__}")
        self.work_root = Path(
            work_root or os.environ.get("YGG_WORK_ROOT") or "/tmp/ygg_work"
        )
        self.emitter = emitter or FileSpoolEmitter()

    def _scope_dir(self, plan_dir: Path) -> Path:
        return plan_dir.parent

    def _plan_dir(self, plan: Plan) -> Path:
        return self.work_root / plan.plan_id

    def _step_dir(self, plan_dir: Path, spec: StepSpec) -> Path:
        return plan_dir / spec.step_id

    def _write_plan_file(self, plan: Plan, plan_dir: Path) -> None:
        plan_dir.mkdir(parents=True, exist_ok=True)
        (plan_dir / "plan.json").write_text(
            json.dumps(
                {
                    "plan_id": plan.plan_id,
                    "realm": plan.realm,
                    "scope": plan.scope,
                    "steps": [spec.__dict__ for spec in plan.steps],
                },
                indent=2,
                sort_keys=True,
                default=str,
            )
        )

    def _topo_validate(self, plan: Plan) -> None:
        seen: set[str] = set()
        by_id: dict[str, StepSpec] = {s.step_id: s for s in plan.steps}
        for spec in plan.steps:
            missing = [d for d in spec.deps if d not in seen and d not in by_id]
            if missing:
                raise ValueError(f"Unknown deps in {spec.step_id}: {missing}")
            # only enforce order weakly: deps must appear somewhere in plan
            seen.add(spec.step_id)

    def run(self, plan: Plan) -> None:
        plan_dir = self._plan_dir(plan)
        self._write_plan_file(plan, plan_dir)
        self._topo_validate(plan)

        for spec in plan.steps:
            step_dir = self._step_dir(plan_dir, spec)
            step_dir.mkdir(parents=True, exist_ok=True)

            fn = resolve_callable(spec.fn_ref)
            if not hasattr(fn, "_step_name"):
                raise ValueError(
                    f"Undecorated step function detected for step '{spec.step_id}' "
                    f"(fn_ref='{spec.fn_ref}'). "
                    f"Decorate it with '@step' from 'yggdrasil.flow.step'."
                )
            _lint_missing_inputs(spec, fn)

            run_id = _new_run_id()

            # compute fingerprint and check cache // pass fn so we can read _input_keys
            fingerprint = _default_fingerprint(spec, fn)
            fp_file = step_dir / "success.fingerprint"

            if fp_file.exists() and fp_file.read_text().strip() == fingerprint:
                # emit a 'skipped' event
                self.emitter.emit(
                    {
                        "type": "step.skipped",
                        "realm": plan.realm,
                        "scope": plan.scope,
                        "plan_id": plan.plan_id,
                        "step_id": spec.step_id,
                        "step_name": spec.name,
                        "fingerprint": fingerprint,
                        "seq": 1,
                        "eid": str(uuid.uuid4()),
                        "ts": utcnow_iso(),
                        "_spool_path": {
                            "realm": plan.realm,
                            "plan_id": plan.plan_id,
                            "step_id": spec.step_id,
                            "run_id": run_id,
                            "filename": "0001_step_skipped.json",
                        },
                    }
                )
                continue

            # build context and call the step function
            from yggdrasil.flow.data_access import DataAccess

            ctx = StepContext(
                realm=plan.realm,
                scope=spec.scope or plan.scope,
                plan_id=plan.plan_id,
                step_id=spec.step_id,
                step_name=spec.name,
                workdir=step_dir,
                scope_dir=self._scope_dir(plan_dir),
                emitter=self.emitter,
                run_mode=os.environ.get("YGG_RUN_MODE", "auto"),
                fingerprint=fingerprint,
                run_id=run_id,
                data=DataAccess(plan.realm),
            )

            try:
                # Coerce string params to Path where function signature expects it
                coerced_params = coerce_params_to_signature_types(fn, spec.params)
                # returns StepResult (decorator wraps emissions)
                result = fn(ctx, **coerced_params)
            except TransientStepError as e:
                # The @step wrapper has already emitted "step.failed".
                # Add a precise diagnostic so operators know retry isn't wired yet.
                # TODO: Implement retry.
                self.emitter.emit(
                    {
                        "type": "step.retry_unimplemented",
                        "realm": plan.realm,
                        "scope": plan.scope,
                        "plan_id": plan.plan_id,
                        "step_id": spec.step_id,
                        "error": str(e),
                        "kind": "transient",
                        "_spool_path": {
                            "realm": plan.realm,
                            "plan_id": plan.plan_id,
                            "step_id": spec.step_id,
                            "run_id": run_id,
                            "filename": "retry_unimplemented.json",
                        },
                    }
                )
                # Treat transient as permanent until retries are implemented.
                raise PermanentStepError(
                    f"Retry not implemented for transient failure: {e}"
                ) from e

            if result is not None and not isinstance(result, StepResult):
                self._logger.warning(
                    "Step %s returned %r (expected StepResult or None)",
                    spec.step_id,
                    type(result),
                )

            # mark success in cache after function returns without exception
            fp_file.write_text(fingerprint)
