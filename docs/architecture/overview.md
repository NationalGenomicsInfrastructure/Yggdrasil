# Architecture Overview

Yggdrasil is an event-driven orchestration framework. It watches external systems (CouchDB databases, file-system paths, etc.) for changes, routes change events to **realm handlers**, and executes the resulting workflow plans via the **Engine**.

---

## Core components

```
┌─────────────────────────────────────────────────────────┐
│                     YggdrasilCore                       │
│                                                         │
│  ┌───────────────┐   YggdrasilEvent  ┌───────────────┐  │
│  │ WatcherManager│ ────────────────► │ Handler router│  │
│  │  (backends)   │                   │(subscriptions)│  │
│  └───────────────┘                   └──────┬────────┘  │
│                                             │ PlanDraft │
│                                     ┌───────▼────────┐  │
│                                     │  Plan store    │  │
│                                     │ (yggdrasil_    │  │
│                                     │  plans DB)     │  │
│                                     └───────┬────────┘  │
│                                             │ approved  │
│                                     ┌───────▼────────┐  │
│                                     │ PlanWatcher /  │  │
│                                     │    Engine      │  │
│                                     └────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

### WatcherManager

`lib/watchers/manager.py`

- Consumes `WatchSpec` objects declared by realm descriptors
- Resolves logical connection names (e.g. `"projects_db"`) to concrete endpoints via the `external_systems` config block
- Deduplicates backends: one `CouchDBBackend` instance per (backend-type, connection), regardless of how many WatchSpecs share it
- Evaluates `filter_expr` (JSON Logic) on each raw event to decide whether to forward it
- Calls `build_scope()` and `build_payload()` to construct a `YggdrasilEvent` and route it to core

### CouchDB backend behavior

`lib/watchers/backends/couchdb.py`

The CouchDB backend polls the `_changes` feed using a raw HTTP GET request (not the IBM CloudantV1 SDK).

**Feed mode** — the backend starts in `feed=normal`. Once a batch is fully caught up (`pending == 0`), it switches to `feed=longpoll` to reduce polling overhead. It reverts to `feed=normal` whenever `pending > 0`.

**Internal document filtering** — `_design/*` and `_local/*` documents are silently skipped. No event is emitted; the checkpoint still advances past them.

**Checkpoint timing** — checkpoints advance per-row, immediately after each row is processed or intentionally skipped. On an abrupt restart, at most one event (the last emitted before shutdown) may be replayed. Downstream handlers are expected to be idempotent.

**404 on a non-deleted row** — treated as a recoverable skip: no event is emitted, a `WARNING` is logged, and the checkpoint advances. The row is not retried.

**Retry configuration** — transient document fetch failures (5xx, 429, network errors) are retried. The defaults can be overridden in `main.json`:

```json
"watchers": {
  "max_observation_retries": 3,
  "observation_retry_delay_s": 1.0
}
```

If this block is absent, defaults are `max_observation_retries=3` and `observation_retry_delay_s=1.0`. `_changes` poll failures are retried indefinitely (no cap) to keep the daemon alive when CouchDB is temporarily unavailable.

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
3. Implements `generate_plan_drafts(payload)` — returns a `list[PlanDraft]` containing the execution plan(s)

Handlers **generate plans; they do not execute them**. Execution is decoupled: the core schedules it asynchronously, and the Engine runs plans sequentially.

### Engine

`yggdrasil/core/engine.py`

Executes a `Plan` (list of `StepSpec`):

1. Creates `<work_root>/<plan_id>/` and writes `plan.json`
2. For each step: creates a workdir, computes a fingerprint, checks the cache, calls the `@step`-decorated function
3. Emits structured events to the configured event spool at each step lifecycle point

### PlanWatcher

`lib/watchers/plan_watcher.py`

Monitors the `yggdrasil_plans` database. When a plan document transitions to `status="approved"` and `run_token > executed_run_token`, the PlanWatcher picks it up and dispatches it to the Engine.

PlanWatcher uses `ChangesFetcher` for continuous polling with `include_docs=True`, which triggers a separate `fetch_document_by_id()` call per change row so eligibility checks have the full document body available.

### ChangesFetcher

`lib/couchdb/changes_fetcher.py`

Continuous `_changes` feed poller used by PlanWatcher. Key behaviours:

- Polls via raw HTTP GET (`fetch_changes_raw`) — **not** the IBM CloudantV1 SDK — matching the proven `CouchDBBackend` transport
- Switches between `feed=normal` (catching up, `pending > 0`) and `feed=longpoll` (caught up, `pending == 0`) to reduce overhead once current
- Fetches full document bodies separately via `fetch_document_by_id()` when `include_docs=True`
- Filters `_design/*` and `_local/*` documents before yielding
- Handles transient network and server errors with exponential backoff; never permanently aborts on connection resets

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
6. Handler.generate_plan_drafts(payload)  →  list[PlanDraft]
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
| `lib/couchdb/` | CouchDB connection handler, document managers, `ChangesFetcher` (continuous poller), typed models (`couchdb_models.py`) |
| `lib/realms/` | Internal realm implementations and `test_realm` (dev-only) |
| `tests/` | Full test suite (1700+ tests) |

---

## See also

- [Flow API](../flow_api/overview.md) — step decorator, planner protocol, engine details
- [Realm Authoring Guide](../realm_authoring/guide.md) — writing your own realm
- [Realm Authoring Cookbook](../realm_authoring/cookbook.md) — common patterns
- [Glossary](../reference/glossary.md) — terminology reference
