# Realm Authoring Cookbook

Common patterns for realm authors. For the full reference, see [guide.md](guide.md).

---

## Pattern 1: Dev-mode gating (realm invisible in production)

Return `None` from `get_realm_descriptor()` when the realm should not be active:

```python
# my_realm/__init__.py
from lib.core_utils.ygg_session import YggSession
from yggdrasil.core.realm.descriptor import RealmDescriptor


def get_realm_descriptor() -> RealmDescriptor | None:
    if not YggSession.is_dev():
        return None  # Not discovered at all in production
    from my_realm.handler import MyDevHandler
    return RealmDescriptor(
        realm_id="my_realm",
        handler_classes=[MyDevHandler],
        watchspecs=_get_watchspecs,
    )
```

**When to use:** Dev-only or debug realms that must be completely invisible in production.

**Alternative:** Return the descriptor but make `watchspecs` return `[]` when disabled — the handler stays registered for CLI use, but no automatic events fire.

---

## Pattern 2: Multiple handlers in one realm

A single realm can export multiple handlers for different event types:

```python
def get_realm_descriptor() -> RealmDescriptor:
    return RealmDescriptor(
        realm_id="my_realm",
        handler_classes=[MyProjectHandler, MyDeliveryHandler],
        watchspecs=_get_watchspecs,
    )
```

```python
class MyProjectHandler(BaseHandler):
    event_type: ClassVar[EventType] = EventType.COUCHDB_DOC_CHANGED
    handler_id: ClassVar[str] = "project_handler"
    ...

class MyDeliveryHandler(BaseHandler):
    event_type: ClassVar[EventType] = EventType.COUCHDB_DOC_CHANGED
    handler_id: ClassVar[str] = "delivery_handler"
    ...
```

Route each WatchSpec to a specific handler via `target_handlers`:

```python
WatchSpec(
    backend="couchdb",
    connection="projects_db",
    event_type=EventType.COUCHDB_DOC_CHANGED,
    filter_expr={"==": [{"var": "doc.type"}, "project"]},
    build_scope=_project_scope,
    build_payload=_project_payload,
    target_handlers=["project_handler"],  # Only this handler receives it
),
WatchSpec(
    backend="couchdb",
    connection="projects_db",
    event_type=EventType.COUCHDB_DOC_CHANGED,
    filter_expr={"==": [{"var": "doc.type"}, "delivery"]},
    build_scope=_delivery_scope,
    build_payload=_delivery_payload,
    target_handlers=["delivery_handler"],
),
```

---

## Pattern 3: Schema-driven routing in a single handler

When one handler needs to dispatch to different plan shapes based on document content:

```python
async def generate_plan_draft(self, payload: dict[str, Any]) -> PlanDraft:
    doc = payload["doc"]
    ctx: PlanningContext = payload["planning_ctx"]

    analysis_type = doc.get("analysis_type", "default")

    if analysis_type == "mode_a":
        steps = build_mode_a_steps(doc, ctx)
    elif analysis_type == "mode_b":
        steps = build_mode_b_steps(doc, ctx)
    else:
        raise ValueError(f"Unknown analysis_type: {analysis_type!r}")

    plan = Plan(
        plan_id=f"my_realm:{ctx.scope['id']}",
        realm=self.realm_id or "my_realm",
        scope=ctx.scope,
        steps=steps,
    )
    return PlanDraft(plan=plan, auto_run=True, approvals_required=[], notes="")
```

---

## Pattern 4: Approval workflow

Set `auto_run=False` to require manual approval before execution:

```python
return PlanDraft(
    plan=plan,
    auto_run=False,             # Plan saved as status="draft"
    approvals_required=["team_lead"],
    notes="Requires review before execution",
    preview={
        "item_count": len(items),
        "estimated_gb": total_gb,
    },
)
```

The plan is stored in `yggdrasil_plans` with `status="draft"`. It executes only after an operator sets `status="approved"` and increments `run_token`. (Currently there is no UI available to perform this task).

---

## Pattern 5: Writing steps with StepContext

Steps receive a `StepContext` providing workdir, emitter, scope, and realm. Decorate with `@step`:

```python
from yggdrasil.flow.step import step
from yggdrasil.flow.model import StepContext, StepResult


@step
def run_pipeline(ctx: StepContext, config_file: str, threads: int = 4) -> StepResult:
    """Run an external pipeline tool."""
    cmd = [
        "my_tool", "run",
        "--id", ctx.scope["id"],
        "--config", config_file,
        "--threads", str(threads),
    ]

    # ctx.workdir is a unique per-run directory
    result = subprocess.run(cmd, cwd=ctx.workdir, capture_output=True)

    if result.returncode != 0:
        raise RuntimeError(f"my_tool failed: {result.stderr.decode()}")

    # Register output directory as artifact
    outs_dir = ctx.workdir / ctx.scope["id"] / "output"
    ctx.record_artifact("pipeline_output", path=outs_dir)

    return StepResult(metrics={"returncode": result.returncode})
```

**`StepContext` fields:**

| Field | Type | Description |
|-------|------|-------------|
| `realm` | `str` | Realm ID |
| `scope` | `dict` | Scope dict (`{"kind": ..., "id": ...}`) |
| `plan_id` | `str` | Current plan ID |
| `step_id` | `str` | Current step ID |
| `step_name` | `str` | Human-readable step name |
| `workdir` | `Path` | Per-run working directory |
| `scope_dir` | `Path` | Shared scope directory across all steps in this plan |
| `emitter` | `BaseEmitter` | Event emitter for progress/artifact events |
| `run_mode` | `str` | `"auto"` or `"manual"` |
| `fingerprint` | `str` | SHA-256 fingerprint for this run |
| `run_id` | `str` | Unique run ID |

---

## Pattern 6: Emitting progress from a long step

Use `ctx.emitter` to emit progress events so operators know a long step is alive:
(It is generally recommended to keep steps as short as possible - i.e. perform a well defined single task)

```python
@step
def step_sleep(ctx: StepContext, duration_sec: float) -> StepResult:
    import time
    from yggdrasil.flow.events.emitter import ProgressEvent

    steps = 4
    for i in range(1, steps + 1):
        time.sleep(duration_sec / steps)
        pct = int(i / steps * 100)
        ctx.emitter.emit(ProgressEvent(
            realm=ctx.realm,
            scope=ctx.scope,
            plan_id=ctx.plan_id,
            step_id=ctx.step_id,
            run_id=ctx.run_id,
            message=f"Sleep {pct}% complete",
            percent=pct,
        ))

    return StepResult(metrics={"slept_sec": duration_sec})
```

---

## Pattern 7: Recipe factory — common step patterns

Use a `recipes.py` module to keep handler logic thin. A recipe is a plain function returning a list of `StepSpec`:

```python
# my_realm/recipes.py
from yggdrasil.flow.model import StepSpec

_PREFIX = "my_realm.steps"


def standard_pipeline(item_id: str, config: str) -> list[StepSpec]:
    return [
        StepSpec(
            step_id="process",
            name="Process item",
            fn_ref=f"{_PREFIX}.run_processor",
            params={"item_id": item_id, "config": config},
        ),
        StepSpec(
            step_id="report",
            name="Generate report",
            fn_ref=f"{_PREFIX}.run_reporter",
            params={"item_id": item_id},
            deps=["process"],
        ),
    ]
```

In the handler:

```python
from my_realm.recipes import standard_pipeline

steps = standard_pipeline(item_id=doc["_id"], config=doc["config"])
```

---

## Pattern 8: Plan-time data fetch

To embed data fetched from CouchDB directly into step params (so the plan record shows what was fetched):

```python
async def generate_plan_draft(self, payload: dict[str, Any]) -> PlanDraft:
    ctx: PlanningContext = payload["planning_ctx"]

    # Fetch at planning time — result baked into the plan
    client = ctx.data.couchdb("config_db")
    ref_doc = await client.get("config:pipeline_defaults")
    config_path = ref_doc["default_config"] if ref_doc else "/fallback/defaults.yaml"

    steps = [
        StepSpec(
            step_id="process",
            fn_ref="my_realm.steps.run_processor",
            params={"config": config_path},   # baked in
        ),
    ]
    ...
```

**When to use:** When the step itself doesn't need live data access, but the plan record should document what configuration was resolved at plan-generation time.

**Alternative:** Fetch at *execution time* inside the step using the blocking API (steps are sync): `client = ctx.data.couchdb(connection)` then `doc = client.get_blocking(doc_id)` (or `find_blocking`, etc.). This makes the fetch visible via step events/metrics, not baked into plan params.

---

## Pattern 9: Declaring step inputs for fingerprinting

To make the Engine re-run a step when an input file changes (not just params), declare inputs:

```python
# In the plan
StepSpec(
    step_id="transform",
    fn_ref="my_realm.steps.run_transform",
    params={"item_id": "item-001"},
    inputs={"input_file": "/path/to/prepared.dat"},  # tracked for fingerprint
    deps=["prepare"],
)
```

Or declare via type annotation on the step function:

```python
from typing import Annotated
from yggdrasil.flow.artifacts import In, Out

@step
def run_transform(
    ctx: StepContext,
    input_file: Annotated[Path, In("input_file")],
    item_id: str,
) -> StepResult:
    ...
```

The Engine computes `sha256(params + sha256(input_file))` as the fingerprint. If the file changes, the cached fingerprint mismatches and the step re-runs.

---

## See also

- [Realm Authoring Guide](guide.md) — full reference for `RealmDescriptor`, `WatchSpec`, validation rules
- [Flow API Overview](../flow_api/overview.md) — `@step`, `Engine`, emitters, `PlanDraft` fields
- [Architecture Overview](../architecture/overview.md) — how realms plug into the core
- [Test Realm](../reference/test_realm.md) — running test scenarios to validate your pipeline
