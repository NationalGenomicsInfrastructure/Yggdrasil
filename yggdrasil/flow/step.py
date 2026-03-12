from __future__ import annotations

import functools
import inspect
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, get_args, get_origin, get_type_hints

if TYPE_CHECKING:
    from yggdrasil.flow.data_access import DataAccess

from yggdrasil.flow.artifacts import ArtifactRefProtocol, ensure_artifact_ref
from yggdrasil.flow.errors import PermanentStepError, TransientStepError
from yggdrasil.flow.events.emitter import EventEmitter, FileSpoolEmitter
from yggdrasil.flow.model import Artifact, StepResult
from yggdrasil.flow.utils.hash import dirhash_stats, sha256_file

CTX_PARAM = "ctx"


@dataclass(frozen=True)
class In:
    artifact: Any


@dataclass(frozen=True)
class Out:
    artifact: Any


def _extract_io(fn):
    """
    Introspect a @step function and return:
      inputs:  {param_name: artifact_id}
      outputs: {param_name: artifact_id}
      knobs:   set[param_name]   # required, non-IO, non-ctx params
    """
    # Resolve annotations including Annotated[...] extras
    hints = get_type_hints(fn, include_extras=True)

    # Identify the ctx parameter by name or annotation
    sig = inspect.signature(fn)
    params = sig.parameters
    # ctx_param_names = {
    #     name for name, p in params.items()
    #     if name == "ctx" or (p.annotation.__name__ if hasattr(p.annotation, "__name__") else None) == "StepContext"
    # }

    inputs: dict[str, Any] = {}
    outputs: dict[str, Any] = {}

    for name, annot in hints.items():
        if get_origin(annot) is Annotated:
            _, *meta = get_args(annot)
            for m in meta:
                if isinstance(m, In):
                    inputs[name] = m.artifact
                elif isinstance(m, Out):
                    outputs[name] = m.artifact

    # Required knobs = non-ctx, non-IO, and no default
    knobs: set[str] = set()
    for name, p in params.items():
        if name == CTX_PARAM:
            continue
        if name in inputs or name in outputs:
            continue
        if p.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        ):
            if p.default is inspect._empty:
                knobs.add(name)

    return inputs, outputs, knobs


# ---------- Step context & decorator ----------


@dataclass
class StepContext:
    realm: str
    scope: dict[
        str, Any
    ]  # {"kind":"flowcell","id":"A22FN2TLT3"} or {"kind":"project","id":"P12345"}, etc.
    plan_id: str  # the concrete plan id executing
    step_id: str  # e.g. "cellranger_multi__P12345_1001__9b7f"
    step_name: str  # e.g. "cellranger_multi"
    workdir: Path
    scope_dir: Path
    emitter: EventEmitter = field(default_factory=FileSpoolEmitter)
    run_mode: str = "auto"  # "auto" or "render_only"
    fingerprint: str | None = None  # set by engine; steps can read it
    run_id: str | None = None  #
    data: DataAccess | None = (
        None  # realm-scoped read-only data access (injected by Engine)
    )
    _seq: int = 0  # private counter, starts at 0
    _artifacts: list[Artifact] = field(default_factory=list)

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    @property
    def artifacts(self) -> list[Artifact]:
        return list(self._artifacts)

    def emit(self, type_: str, **payload: Any) -> None:
        seq = self._next_seq()
        event = {
            "type": type_,
            "seq": seq,
            "realm": self.realm,
            "scope": self.scope,
            "plan_id": self.plan_id,
            "step_id": self.step_id,
            "step_name": self.step_name,
            "fingerprint": self.fingerprint,
            **payload,
        }
        # Route into nested dirs for readability
        # NOTE: We let the `emitter` decide final path; pass hints in the event
        event["_spool_path"] = {
            "realm": self.realm,
            "plan_id": self.plan_id,
            "step_id": self.step_id,
            "run_id": self.run_id,
            "filename": f"{seq:04d}_{type_.replace('.', '_')}.json",
        }
        self.emitter.emit(event)

    # NOTE: REPLACES add_artifact_ref
    def record_artifact(
        self,
        ref: object,
        *,
        path: Path | None = None,
        digest: str | None = None,
    ) -> Artifact:
        aref: ArtifactRefProtocol = ensure_artifact_ref(ref)
        p = path or aref.resolve_path(self.scope_dir)

        if digest is None:
            if p.is_dir():
                digest = dirhash_stats(p)
            elif p.is_file():
                digest = f"sha256:{sha256_file(p)}"

        art = Artifact(key=aref.key(), path=str(p), digest=digest)
        self._artifacts.append(art)  # <- add to context
        # Emit immediately so UIs can reflect it mid-step if desired
        self.emit("step.artifact", artifact=art.__dict__)
        return art

    # Back-compat alias (keep temporarily, or remove)
    add_artifact_ref = record_artifact
    # def add_artifact_ref(self, ref: object, *, digest: str | None = None) -> Artifact:
    #     aref: ArtifactRefProtocol = ensure_artifact_ref(ref)
    #     path = aref.resolve_path(self.scope_dir)

    #     if digest is None:
    #         if path.is_dir():
    #             digest = dirhash_stats(path)
    #         elif path.is_file():
    #             digest = f"sha256:{sha256_file(path)}"

    #     art = Artifact(key=aref.key(), path=str(path), digest=digest)
    #     # Emit immediately so UIs can reflect it mid-step if desired
    #     self.emit("step.artifact", artifact=art.__dict__)  # includes "key"
    #     return art

    def progress(self, pct: float, message: str | None = None) -> None:
        """Optional mid-step progress signal (0..100)."""
        self.emit("step.progress", progress=max(0, min(100, pct)), message=message)


# def step(name: str | None = None, *, input_keys: tuple[str, ...] = ()):
def step(_fn=None, *, name: str | None = None):
    """
    Decorator to make a function an Yggdrasil 'step'.

    The engine handles:
      - fingerprinting & cache checks BEFORE calling the wrapped fn
      - building StepContext and workdir
    The wrapped fn should accept (ctx: StepContext, **params) and return StepResult or None.
    """

    def deco(fn: Callable[..., Any]):
        sname = name or fn.__name__
        ins, outs, sargs = _extract_io(fn)

        @functools.wraps(fn)
        def wrapper(ctx: StepContext, **kwargs) -> StepResult:
            ctx.emit("step.started", params=kwargs)
            try:
                result = fn(ctx, **kwargs)
                if result is None:
                    result = StepResult()

                # if user skipped returning artifacts, use what the context recorded
                if not result.artifacts and ctx._artifacts:
                    result.artifacts = list(ctx._artifacts)

                # Emit success with a compact manifest
                manifest = [a.__dict__ for a in result.artifacts]
                ctx.emit(
                    "step.succeeded",
                    artifacts=manifest,
                    metrics=result.metrics,
                    extra=result.extra,
                )
                return result
            except PermanentStepError as e:
                ctx.emit(
                    "step.failed",
                    kind="permanent",
                    error=str(e),
                    code=e.code,
                    advice=e.advice,
                )
                raise  # engine decides whether to stop dependents

            except TransientStepError as e:
                ctx.emit(
                    "step.failed",
                    kind="transient",
                    error=str(e),
                    code=e.code,
                    advice=e.advice,
                )
                raise

            except Exception as e:
                # Unknown -> treat as permanent by default (TODO: Not sure whether to treat as 'transient' once?)
                ctx.emit("step.failed", kind="permanent", error=str(e))
                raise

        # Step metadata (for builder/engine)
        setattr(wrapper, "_step_name", sname)
        setattr(wrapper, "__step_inputs__", ins)  # {param_name: Artifact}
        setattr(wrapper, "__step_outputs__", outs)  # {param_name: Artifact}
        setattr(wrapper, "__step_args__", sargs)  # set[str]
        setattr(
            wrapper, "_input_keys", tuple(ins.keys())
        )  # back-compat for fingerprinting

        return wrapper

    # Allow @step and @step()
    if callable(_fn):
        return deco(_fn)

    return deco
