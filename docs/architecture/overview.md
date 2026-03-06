# Architecture Overview

Yggdrasil is an event-driven orchestration framework. It watches external systems (CouchDB databases, file-system paths, etc.) for changes, routes change events to **realm handlers**, and executes the resulting workflow plans via the **Engine**.

---

## Core components

```
┌─────────────────────────────────────────────────────────┐
│                     YggdrasilCore                       │
│                                                         │
│  ┌──────────────┐   YggdrasilEvent   ┌───────────────┐  │
│  │ WatcherManager│ ────────────────► │ Handler router│  │
│  │  (backends)  │                   │(subscriptions)│  │
│  └──────────────┘                   └───────┬───────┘  │
│                                             │ PlanDraft │
│                                     ┌───────▼───────┐  │
│                                     │  Plan store   │  │
│                                     │ (yggdrasil_   │  │
│                                     │  plans DB)    │  │
│                                     └───────┬───────┘  │
│                                             │ approved  │
│                                     ┌───────▼───────┐  │
│                                     │ PlanWatcher / │  │
│                                     │    Engine     │  │
│                                     └───────────────┘  │
└─────────────────────────────────────────────────────────┘
```

### WatcherManager

`lib/watchers/manager.py`

- Consumes `WatchSpec` objects declared by realm descriptors
- Resolves logical connection names (e.g. `"projects_db"`) to concrete endpoints via the `external_systems` config block
- Deduplicates backends: one `CouchDBBackend` instance per (backend-type, connection), regardless of how many WatchSpecs share it
- Evaluates `filter_expr` (JSON Logic) on each raw event to decide whether to forward it
- Calls `build_scope()` and `build_payload()` to construct a `YggdrasilEvent` and route it to core

### YggdrasilCore

`lib/core_utils/yggdrasil_core.py`

- Central orchestrator (singleton)
- Manages realm discovery via the `ygg.realm` entry-point group
- Maintains a subscriptions dict: `dict[EventType, list[BaseHandler]]`
- Dispatches `YggdrasilEvent` objects to registered handlers
- Stores `PlanDraft` outputs in `yggdrasil_plans` CouchDB database
- Runs the main async event loop

### BaseHandler

`yggdrasil/flow/base_handler.py`

Each realm provides one or more handler classes. A handler:

1. Declares the `EventType` it handles (class attribute)
2. Implements `derive_scope(doc)` — extracts a scope dict `{"kind": ..., "id": ...}`
3. Implements `generate_plan_draft(payload)` — returns a `PlanDraft` containing the execution plan

Handlers **generate plans; they do not execute them**. Execution is decoupled: the core schedules it asynchronously, and the Engine runs plans sequentially.

### Engine

`yggdrasil/core/engine.py`

Executes a `Plan` (list of `StepSpec`):

1. Creates `<work_root>/<plan_id>/` and writes `plan.json`
2. For each step: creates a workdir, computes a fingerprint, checks the cache, calls the `@step`-decorated function
3. Emits structured events to the configured event spool at each step lifecycle point

### PlanWatcher

Monitors the `yggdrasil_plans` database. When a plan document transitions to `status="approved"` and `run_token > executed_run_token`, the PlanWatcher picks it up and dispatches it to the Engine.

---

## Realm plugin system

External packages register as realms via the `ygg.realm` entry-point group in their `pyproject.toml`:

```toml
[project.entry-points."ygg.realm"]
my_realm = "my_package.realm:get_realm_descriptor"
```

The function must return a `RealmDescriptor` (or `None` to opt out, e.g. for dev-mode gating):

```python
from yggdrasil.core.realm import RealmDescriptor

def get_realm_descriptor() -> RealmDescriptor | None:
    return RealmDescriptor(
        realm_id="my_realm",
        handler_classes=[MyHandler],
        watchspecs=get_watchspecs,   # callable, deferred loading
    )
```

At startup, `YggdrasilCore` calls each discovered `get_realm_descriptor()`, collects `WatchSpec` objects, and subscribes handler classes to their declared `EventType`.

---

## Event flow: watcher → execution

```
1. CouchDB _changes feed detects a document update
2. CouchDBBackend emits RawWatchEvent
3. WatcherManager evaluates filter_expr (JSON Logic)
4. WatcherManager calls build_scope() + build_payload()  →  YggdrasilEvent
5. YggdrasilCore routes event to subscribed handlers
6. Handler.generate_plan_draft(payload)  →  PlanDraft
7. Core persists PlanDraft as plan document in yggdrasil_plans DB
8. PlanWatcher detects plan with status="approved"
9. Engine.run(plan)  →  steps run in workdirs, events emitted
```

---

## Two event systems

Yggdrasil has two distinct event layers — do not confuse them:

| Layer | Purpose | Classes |
|-------|---------|---------|
| **Trigger events** | Route watcher notifications to handlers | `YggdrasilEvent`, `EventType` enum, `YggdrasilCore` subscriptions |
| **Step events** | Record step lifecycle during execution | `step.started`, `step.succeeded`, `step.failed`, emitted to the configured event spool |

Trigger events control *which handler runs*. Step events record *what happened during execution*.

---

## Key directories

| Path | Contents |
|------|---------|
| `yggdrasil/` | Public API: `flow/`, `cli.py`, `core/` |
| `yggdrasil/flow/` | `@step` decorator, planner protocol, event emitter interface |
| `yggdrasil/core/` | Engine, `RealmDescriptor`, core registry |
| `lib/core_utils/` | `YggdrasilCore`, `YggSession`, config loader, logging |
| `lib/watchers/` | `WatcherManager`, `WatchSpec`, backends (CouchDB, filesystem) |
| `lib/couchdb/` | Low-level CouchDB connection, document managers |
| `lib/realms/` | Internal realm implementations and `test_realm` (dev-only) |
| `tests/` | Full test suite (1700+ tests) |

---

## See also

- [Flow API](../flow_api/overview.md) — step decorator, planner protocol, engine details
- [Realm Authoring Guide](../realm_authoring/guide.md) — writing your own realm
- [Realm Authoring Cookbook](../realm_authoring/cookbook.md) — common patterns
- [Glossary](../reference/glossary.md) — terminology reference
