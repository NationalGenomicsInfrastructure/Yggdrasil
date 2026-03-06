# Flow API Overview

`yggdrasil.flow` provides the `@step` decorator, planner protocol, and event emitters used by all realm handlers.

---

## Core dataclasses (`yggdrasil.flow.model`)

### `Plan`

The concrete, frozen workflow to execute. Created by a handler's planner and persisted in `yggdrasil_plans`.

| Field | Type | Description |
|-------|------|-------------|
| `plan_id` | `str` | Unique identifier (e.g. `"my_realm:object-123"`) |
| `realm` | `str` | Realm that owns this plan |
| `scope` | `dict` | Scope dict (`{"kind": ..., "id": ...}`) |
| `steps` | `list[StepSpec]` | Ordered list of step specs |

### `StepSpec`

Defines one step inside a plan.

| Field | Type | Description |
|-------|------|-------------|
| `step_id` | `str` | Unique within the plan |
| `name` | `str` | Human-readable label |
| `fn_ref` | `str` | Dotted import path to the `@step` function |
| `params` | `dict` | Static parameters passed to the step |
| `deps` | `list[str]` | `step_id`s that must succeed before this step runs |
| `inputs` | `dict` | Artifact paths tracked for fingerprinting (optional) |
| `scope` | `dict \| None` | Override scope for this step (optional) |

### `StepContext`

Passed to every `@step` function at execution time.

| Field | Type | Description |
|-------|------|-------------|
| `realm` | `str` | Realm ID |
| `scope` | `dict` | Scope dict for this step |
| `plan_id` | `str` | Owning plan ID |
| `step_id` | `str` | This step's ID |
| `step_name` | `str` | Human-readable step name |
| `workdir` | `Path` | Per-run working directory (unique per fingerprint run) |
| `scope_dir` | `Path` | Shared scope directory for artifacts across all plan steps |
| `emitter` | `BaseEmitter` | Event emitter |
| `run_mode` | `str` | `"auto"` or `"manual"` |
| `fingerprint` | `str` | SHA-256 fingerprint for this run |
| `run_id` | `str` | Unique run ID (UUID fragment) |
| `data` | `DataAccess` | Read-only access to configured data sources |

### `PlanningContext`

Passed to `generate_plan_draft()`. Contains everything a handler needs to build a plan.

| Field | Type | Description |
|-------|------|-------------|
| `realm` | `str` | Realm ID |
| `scope` | `dict` | Scope dict for the triggering event |
| `scope_dir` | `Path` | Workspace directory for this scope |
| `emitter` | `BaseEmitter` | Event emitter to use for the plan |
| `source_doc` | `dict` | The raw document that triggered the event |
| `reason` | `str` | Human-readable trigger reason |
| `realm_config` | `dict \| None` | Optional realm-specific config slice |
| `data` | `DataAccess` | Read-only access for plan-time data fetches |

### `PlanDraft`

Output of a handler's plan generation.

| Field | Type | Description |
|-------|------|-------------|
| `plan` | `Plan` | The generated plan |
| `auto_run` | `bool` | If `True`, plan is approved immediately; if `False`, status is `"draft"` |
| `approvals_required` | `list[str]` | Labels of required approvers (informational) |
| `notes` | `str` | Human-readable description of the plan |
| `preview` | `dict \| None` | Metadata for display before execution |

### `Artifact`

A named output of a step.

| Field | Type | Description |
|-------|------|-------------|
| `key` | `str` | Semantic label (e.g. `"output_dir"`, `"report_file"`) |
| `path` | `str \| Path` | Location on disk |
| `digest` | `str` | `sha256:<hex>` for files, `dirhash:<hex>` for directories |

---

## `@step` decorator (`yggdrasil.flow.step`)

Wraps a plain function to standardize step lifecycle:

1. Creates `ctx.workdir`
2. Emits `step.started`
3. Calls the function with `ctx` as first argument
4. Emits `step.succeeded` (with artifacts + metrics) or `step.failed`

```python
from yggdrasil.flow.step import step
from yggdrasil.flow.model import StepContext, StepResult

@step
def my_step(ctx: StepContext, message: str) -> StepResult:
    print(f"[{ctx.scope['id']}] {message}")
    return StepResult(metrics={"message_len": len(message)})
```

**Typed inputs and outputs** via `Annotated` annotations:

```python
from typing import Annotated
from pathlib import Path
from yggdrasil.flow.artifacts import In, Out

@step
def run_processor(
    ctx: StepContext,
    input_dir: Annotated[Path, In("input_dir")],
    output_file: Annotated[Path, Out("output_file")],
    config: str,
) -> StepResult:
    ...
```

`In(key)` declarations tell the Engine which paths to include in the fingerprint computation. `Out(key)` declarations register expected artifacts.

---

## Planner protocol (`yggdrasil.flow.planner`)

Handlers subclass `BaseHandler` and implement `generate_plan_draft()`. Anything that implements that method is conceptually a planner, but `BaseHandler` is the supported extension point.

`BaseHandler` provides a default `build_planning_context()` method that constructs a `PlanningContext` from a scope dict and event payload. Override it only if you need non-standard context setup.

---

## Engine (`yggdrasil.core.engine`)

`Engine.run(plan)` runs a `Plan` sequentially:

1. Creates `<work_root>/<plan_id>/` and writes `plan.json`
2. For each `StepSpec` (respecting `deps` ordering):
   - Creates `<plan_dir>/<step_id>/` workdir
   - Computes fingerprint from params + input file digests
   - Checks `success.fingerprint` — **skips** if unchanged (cache hit)
   - Dynamically imports `fn_ref` and calls it
   - On success: writes `success.fingerprint`, emits `step.succeeded`
   - On failure: emits `step.failed`, aborts remaining steps

The engine's workspace and event spool are configured at daemon startup by `YggdrasilCore`. See [Configuration](../getting_started/configuration.md).

---

## Fingerprint computation

Default fingerprint = `sha256(JSON(params) + digests_of_inputs)`.

Input digests are sourced from (in priority order):
1. `StepSpec.inputs` dict (planner-provided)
2. `fn._input_keys` (declared via `In(...)` annotations on the function)
3. No inputs (params-only fingerprint)

For each input path:
- **File** → `sha256:<hex>`
- **Directory** → `dirhash:<hex>` (hash of sorted paths + sizes + mtimes)
- **Missing** → recorded as `"missing"`

If params and inputs are unchanged from a previous run, the Engine skips the step.

---

## Event system

### Event spool layout

```
$YGG_EVENT_SPOOL/
  <realm>/
    <plan_id>/
      0001_plan_started.json
      <step_id>/
        <run_id>/
          0001_step_started.json
          0002_step_progress.json
          0003_step_artifact.json
          0004_step_succeeded.json
```

Plan-level events omit the `<step_id>` directory.

### Common event types

| Type | When emitted |
|------|-------------|
| `plan.started` | Engine begins executing a plan |
| `step.started` | Step function entered |
| `step.progress` | Optional mid-step progress update |
| `step.artifact` | One artifact registered |
| `step.succeeded` | Step finished successfully |
| `step.failed` | Step raised an exception |
| `step.skipped` | Engine skipped due to cache hit |

Each event JSON record contains: `type`, `seq`, `ts`, `eid`, `realm`, `scope`, `plan_id`, `step_id`, `step_name`, `fingerprint`.

### Emitters

| Class | Description | Use case |
|-------|-------------|----------|
| `FileSpoolEmitter` | Writes one JSON file per event to the configured spool directory | Default; zero infrastructure, crash-tolerant |
| `TeeEmitter` | Fans out to multiple emitters in parallel | Combine FileSpool with another sink |
| `CouchEmitter` | Writes events as CouchDB documents | When you need queryable event history |

The concrete emitter is configured by the operator at daemon startup (via `YGG_EVENT_SPOOL` or, in future, `main.json`). Realm step functions interact with the emitter only via `ctx.emitter`, which is typed as `EventEmitter`. Realm code should never import or instantiate concrete emitter classes directly.

---

## Artifact protocol

Artifacts implement `ArtifactRefProtocol`:

- `key()` → semantic label string
- `resolve_path(scope_dir: Path)` → absolute path

The built-in `SimpleArtifactRef` covers the common case:

```python
from yggdrasil.flow.artifacts import SimpleArtifactRef

ref = SimpleArtifactRef(key="pipeline_output", relative_path="results/output")
abs_path = ref.resolve_path(scope_dir)
```

Register an artifact from within a step:

```python
ctx.record_artifact(ref, path=outs_dir, digest=compute_dirhash(outs_dir))
```

---

## See also

- [Architecture Overview](../architecture/overview.md) — how the Engine fits into the broader event flow
- [Realm Authoring Cookbook](../realm_authoring/cookbook.md) — step writing patterns, recipe factories, progress emission
- [Glossary](../reference/glossary.md) — full terminology reference
