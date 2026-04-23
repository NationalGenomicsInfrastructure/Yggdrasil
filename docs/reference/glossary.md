# Yggdrasil Glossary

---

## Core data model

### Scope
A small dict identifying what a run pertains to: `{"kind": ..., "id": ...}`. Every plan and step is bound to a scope.
```json
{"kind": "project", "id": "proj-123"}
```

### Plan
A frozen, ordered list of `StepSpec`s to execute, identified by `plan_id`. Created by a handler's `generate_plan_drafts()` and persisted in `yggdrasil_plans`.

### StepSpec
One step instance inside a `Plan`. Declares the function reference (`fn_ref`), static parameters (`params`), step dependencies (`deps`), and optional input paths used for fingerprinting.

### PlanDraft
One element of the list returned by a handler's `generate_plan_drafts()`. Wraps a `Plan` with an `auto_run` flag (`True` = execute immediately; `False` = hold for approval), a list of required approvers (future), and human-readable notes.

### PlanningContext
Passed to `generate_plan_drafts()`. Carries: `realm`, `scope`, `scope_dir`, `emitter`, `source_doc` (the triggering document), `reason`, optional `realm_config`, and a `DataAccess` instance.

### StepContext (`ctx`)
Passed to every `@step` function. Provides: `realm`, `scope`, `plan_id`, `step_id`, `step_name`, `workdir`, `scope_dir`, `emitter`, `fingerprint`, `run_id`, and `data` (DataAccess).

### Artifact
A named output of a step: `key` (semantic label), `path` (location on disk), `digest` (`sha256:<hex>` for files, `dirhash:<hex>` for directories).

### Fingerprint
A deterministic SHA-256 digest of a step's params and declared inputs, used for caching. If it matches the `success.fingerprint` from a previous run, the step is skipped.

### StepResult
Returned by a `@step` function. Contains `artifacts`, `metrics` (scalar key-value map), and optional `extra` data.

---

## Realm system

### Realm
A Python package that extends Yggdrasil with handlers and WatchSpecs. Registered via the `ygg.realm` entry-point group.

### RealmDescriptor
Declares a realm: a unique `realm_id`, a list of handler classes, and WatchSpecs (static list or callable). Returned by the realm's `get_realm_descriptor()` function.

### BaseHandler
Abstract base class for all realm handlers. Subclasses declare `event_type` and `handler_id` class attributes and implement `derive_scope(doc)` and `generate_plan_drafts(payload)`.

### WatchSpec
A frozen, declarative watcher intent: watch a named `connection` (from config) for backend events, apply an optional `filter_expr` (JSON Logic predicate), and — on match — emit a `YggdrasilEvent` of a given `EventType` to subscribed handlers.

### RawWatchEvent
The raw event object produced by a backend (e.g. `CouchDBBackend`). Contains the changed document (`doc`), a `deleted` flag, and backend metadata. Passed to `build_scope()` and `build_payload()` in a `WatchSpec`.

---

## Event routing

### EventType
Enum (`lib.core_utils.event_types`) controlling which handlers receive a given event. Active values:
- `COUCHDB_DOC_CHANGED` — a document was created or updated in a watched CouchDB database
- `COUCHDB_DOC_DELETED` — a document was deleted
- `PLAN_EXECUTION` — internal; used by `PlanWatcher` to trigger Engine runs

### YggdrasilEvent
An enriched event routed from `WatcherManager` to handlers. Carries `event_type`, `scope`, `payload`, and metadata.

### WatcherManager
Manages backend watcher instances. Resolves `WatchSpec.connection` to a concrete endpoint via the `external_systems` config block, evaluates `filter_expr` on each raw event, constructs a `YggdrasilEvent`, and routes it to `YggdrasilCore`.

### YggdrasilCore
Central orchestrator (singleton). Discovers realms via `ygg.realm` entry points, manages handler subscriptions, dispatches `YggdrasilEvent` objects, persists `PlanDraft` outputs to `yggdrasil_plans`, and runs the main async event loop.

### PlanWatcher
Watches `yggdrasil_plans`. When a plan transitions to `status="approved"` with an unexecuted run token, dispatches it to the Engine.

---

## Execution

### Engine (`yggdrasil.core.engine`)
Sequential plan executor. For each `StepSpec`: creates a workdir, computes a fingerprint, skips on cache hit, dynamically imports and calls the `@step`-decorated function, emits step lifecycle events.

### `@step` decorator
Wraps a plain Python function to standardise the step lifecycle: creates `ctx.workdir`, emits `step.started`, calls the function, emits `step.succeeded` or `step.failed`.

---

## Step events

Structured JSON records emitted to the configured event spool during plan execution. Distinct from trigger events — these record *what happened during execution*.

**Types:**
- `plan.started` — Engine began a plan
- `step.started` — step function entered
- `step.progress` — optional mid-step update
- `step.artifact` — one artifact registered
- `step.succeeded` — step completed
- `step.failed` — step raised an exception
- `step.skipped` — skipped due to fingerprint cache hit

Each record contains: `type`, `seq`, `ts`, `eid`, `realm`, `scope`, `plan_id`, `step_id`, `step_name`, `fingerprint`.

**Spool layout:**
```
<spool_root>/
  <realm>/<plan_id>/
    0001_plan_started.json
    <step_id>/<run_id>/
      0001_step_started.json
      ...
      000N_step_succeeded.json
```

### EventEmitter
Protocol: a single `emit(event: dict)` method. Three concrete implementations (in `yggdrasil.flow.events`):
- `FileSpoolEmitter` — writes one JSON file per event to the spool directory (default)
- `TeeEmitter` — fans out to multiple emitters
- `CouchEmitter` — writes events as CouchDB documents

Realm code interacts with the emitter only via `ctx.emitter` (typed as `EventEmitter`). Concrete emitter classes should never be imported by realm code.

---

## Data access

### DataAccess
Realm-scoped, read-only gateway to external system connections. Enforces the `realm_allowlist` and `max_limit` policies configured under `external_systems.connections`. Accessed via `ctx.data` in both step functions and the planning context. Only connections explicitly configured with a `data_access` policy are accessible.
