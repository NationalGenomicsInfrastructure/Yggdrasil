from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from yggdrasil.flow.events.emitter import EventEmitter, FileSpoolEmitter
from yggdrasil.flow.model import Artifact, StepResult
from yggdrasil.flow.utils.hash import dirhash_stats, sha256_file

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
    emitter: EventEmitter = field(default_factory=FileSpoolEmitter)
    run_mode: str = "auto"  # "auto" or "render_only"
    fingerprint: str | None = None  # set by engine; steps can read it
    run_id: str | None = None  #
    _seq: int = 0  # private counter, starts at 0

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

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

    def add_artifact(self, role: str, path: str, digest: str | None = None) -> Artifact:
        p = Path(path)
        if digest is None:
            if p.is_dir():
                digest = dirhash_stats(p)
            elif p.is_file():
                digest = f"sha256:{sha256_file(p)}"
        art = Artifact(role=role, path=str(p), digest=digest)
        # Emit immediately so UIs can reflect it mid-step if desired
        self.emit("step.artifact", artifact=art.__dict__)
        return art

    def progress(self, pct: float, message: str | None = None) -> None:
        """Optional mid-step progress signal (0..100)."""
        self.emit("step.progress", progress=max(0, min(100, pct)), message=message)


def step(name: str | None = None, *, input_keys: tuple[str, ...] = ()):
    """
    Decorator to make a function an Yggdrasil 'step'.

    input_keys: tuple of parameter names that are *content inputs*
                (e.g., 'bcl_dir', 'samplesheet', 'library_csv', 'ref_path').
                The engine will hash these paths' contents to build the
                fingerprint, unless the planner provides StepSpec.inputs.

    The engine handles:
      - fingerprinting & cache checks BEFORE calling the wrapped fn
      - building StepContext and workdir
    The wrapped fn should accept (ctx: StepContext, **params) and return StepResult or None.
    """

    def deco(fn: Callable[..., Any]):
        sname = name or fn.__name__

        def wrapper(ctx: StepContext, **kwargs) -> StepResult:
            ctx.emit("step.started", params=kwargs)
            try:
                res = fn(ctx, **kwargs)
                if res is None:
                    res = StepResult()
                # Emit success with a compact manifest
                manifest = [a.__dict__ for a in res.artifacts]
                ctx.emit(
                    "step.succeeded",
                    artifacts=manifest,
                    metrics=res.metrics,
                    extra=res.extra,
                )
                return res
            except Exception as e:
                ctx.emit("step.failed", error=str(e))
                raise

        # attach metadata to the returned callable
        # mark for reflection (engine may inspect this)
        cast(Any, wrapper)._step_name = sname
        cast(Any, wrapper)._input_keys = tuple(input_keys)

        # NOTE: No need. Just attaching to the original fn for introspection/debug
        setattr(fn, "_step_name", sname)
        setattr(fn, "_input_keys", tuple(input_keys))

        return wrapper

    return deco
