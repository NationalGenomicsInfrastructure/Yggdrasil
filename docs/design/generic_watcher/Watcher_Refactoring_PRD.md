# PRD: Generic Watcher Architecture Refactor for Yggdrasil

## Version
v1.1

Status: Draft
Target branch: watcher-refactor
Out of scope: engine concurrency, ops schema changes, file transfer pipelines

⸻
1.	Context

This PRD describes a refactor of Yggdrasil’s watcher architecture to make it:
	•	Generic
	•	Realm-extensible
	•	Configuration-driven
	•	Free of domain-specific assumptions

The design assumes the plan-approval architecture is already in place:

external systems → watchers → handlers (realms) → PlanDraft → DB → PlanWatcher → Engine

Watchers are plan producers, never executors.

⸻
2.	Problem Statement

The current watcher system has structural limitations:
	•	Domain-specific watchers live in core (e.g. projects, flowcells)
	•	Adding a new realm requires modifying Yggdrasil itself
	•	Multiple realms watching the same external resource risk duplicated watchers
	•	External users cannot add new watchers without forking the repo
	•	Configuration and intent are mixed: what is watched, how it is watched, and why it is watched are intertwined

This prevents Yggdrasil from acting as a generic automation platform.

⸻
3.	Design Principles
	1.	Yggdrasil is a platform, not a pipeline
	2.	Realms are plugins, not submodules
	3.	Watchers observe systems — realms interpret events
	4.	One watcher per resource, many subscribers
	5.	Events are cheap; watchers are expensive

These principles guide all architectural decisions in this refactor.

⸻
4.	Goals

4.1 Functional Goals
	•	Allow external realms to declare what they want to watch
	•	Ensure only one watcher exists per external resource
	•	Preserve the existing event → plan → engine execution flow
	•	Keep PlanWatcher intact (it is core infrastructure, not a realm concern)

4.2 Architectural Goals
	•	Watcher backends in core are generic and backend-specific only
	•	Domain semantics live exclusively in external realms
	•	All connection details live in configuration, not code
	•	Lay foundations that allow future backends (Kafka, S3, APIs) without refactor

4.3 Non-Goals
	•	Refactoring plan execution or Engine concurrency
	•	Refactoring plan approval logic
	•	Introducing result sinks or data export
	•	Changing ops schema or consumers
	•	Advanced backpressure management
	•	Circuit breakers or dead-letter queues
	•	Metrics collection beyond basic logging

⸻
5.	High-Level Architecture

External systems (CouchDB, FS, Postgres, …)
	↓
Generic watcher backends (core)
	↓
WatcherManager (fan-out + deduplication)
	↓
Realm WatchSpecs (filter + mapping)
	↓
YggdrasilCore.handle_event()
	↓
Realm planners → PlanDraft
	↓
PlanWatcher → Engine

⸻
6.	Watcher Backends (Core)

Yggdrasil provides generic watcher backends. These are responsible only for:
	•	Connecting to an external system
	•	Emitting low-level change events

They must not:
	•	Know about projects, samples, flowcells, etc.
	•	Contain realm or domain logic

6.1 Initial Backends
	•	CouchDB watcher backend
	•	Filesystem watcher backend (deferred; Phase ≥2, minimal to no infrastructure initially)
	•	Postgres watcher backend (deferred; placeholder until needed)

All backends implement a common interface.

Conceptual interface:
```
class WatcherBackend:
    async def start(self) -> None
    async def stop(self) -> None
    async def events(self) -> AsyncIterator[RawWatchEvent]
```

Backends MUST implement checkpointing via a shared interface, e.g.:
	•	load_checkpoint() -> Checkpoint | None
	•	save_checkpoint(cp: Checkpoint) -> None
	•	Checkpoint key is the dedupe key: (backend, connection, resource, op/event_name...)
	•	Checkpoints are persisted in the yggdrasil internal DB (like you do now with WatcherCheckpointStore) unless explicitly overridden.


⸻
7.	RawWatchEvent

Watcher backends emit structured raw events:
	•	CouchDB: {doc, id, seq, deleted}
	•	Filesystem: {path, event_type}
	•	Postgres: {row, operation}

Example:
```
RawWatchEvent = {
    "id": str,
    "doc": dict | None,
    "seq": str | int | None,
    "deleted": bool,
    "meta": dict,
}
```

Notes:
	•	deleted is included to support explicit delete semantics and backend parity
	•	Backend-specific fields must be placed under meta
	•	No domain assumptions are made at this level

⸻
8.	WatchSpec (Realm-Defined)

Realms declare watcher intent via WatchSpecs.
```
@dataclass(frozen=True)
class WatchSpec:
    backend: str                 # "couchdb", "fs", ...
    connection: str              # config key
    resource: str                # backend-specific
    event_type: EventType        # generic ingress EventType
    target_handlers: list[str] | None = None
    filter_expr: dict | None     # optional predicate  (evaluated on RawWatchEvent)
    build_scope: Callable[[RawWatchEvent], dict]
    build_payload: Callable[[RawWatchEvent], dict]
```

Responsibilities:
    • Express what to watch (backend/connection/resource)
    • Filter raw events (filter_expr)
    • Convert raw events into Yggdrasil scope + payload (build_scope/build_payload)
    • Optionally target specific handler(s) within the owning realm (target_handlers)

Discovery:
    • WatchSpecs are discovered via a dedicated entry point group `ygg.watchspec`
      pointing to a provider function `get_watch_specs() -> list[WatchSpec]`.
    • Handlers are discovered separately via `ygg.handler`.
    • YggdrasilCore binds each WatchSpec to its owning realm at discovery time
      (realm_id is derived from the ygg.watchspec provider entry point / distribution identity).
      WatchSpecs do not carry realm_id themselves.

Filter semantics:
    • filter_expr is evaluated against the full RawWatchEvent object.
      Example field access:
        - CouchDB document fields: doc.type, doc.status, ...
        - Delete flag: deleted
        - Backend-specific details: meta.*
Example filter_expr (JSON Logic):
```
{
	"and": [
		{ "==": [ { "var": "doc.type" }, "project" ] },
		{ "==": [ { "var": "deleted" }, false ] }
	]
}
```

Routing semantics:
    • If target_handlers is provided, events are routed only to the specified handler(s) within the owning realm.
    • If omitted or None, events are routed to all handlers in the owning realm that subscribe to the given event_type.

Handler identity (STRICT):
    • Every handler MUST declare a stable `handler_id: ClassVar[str]`.
    • (realm_id, handler_id) MUST be unique at startup.
    • Missing handler_id or collisions are fatal startup/configuration errors.
	• Enforcement is performed by YggdrasilCore during handler registration.

⸻
9.	Event Flow (Detailed)
	1.	Startup
		•	Yggdrasil loads realms
		•	Collects all WatchSpecs
		•	Groups specs and instantiates watcher backends
	2.	External change occurs
		•	Watcher backend emits RawWatchEvent
	3.	Fan-out
		•	Each matching WatchSpec:
		•	Applies filter_expr
		•	Builds scope and payload
	4.	Core submission
	```
	yggdrasil_core.handle_event(
	    YggdrasilEvent(
	        event_type=spec.event_type,
	        payload={
				**payload,
				"realm_id": bound_realm_id,
				"target_handlers": spec.target_handlers,
			},
	        source=backend_name,
	    )
	)
	```
	`realm_id` is used by core routing to deliver the event only to handlers belonging to the intended realm.
	`bound_realm_id` is assigned by WatcherManager based on the `ygg.watchspec` provider entry point that supplied spec.
	
	5.	Existing flow continues
		•	Realm planners generate PlanDraft
		•	PlanWatcher executes approved plans
		•	Engine emits ops events

⸻
10.	WatcherManager (Core)

WatcherManager orchestrates watcher lifecycle and fan-out.

Responsibilities:
	•	Collect WatchSpecs from all realms
	•	Group by (backend, connection, resource) - (event_type is NOT part of the grouping key; it is handled at the WatchSpec level)
	•	Instantiate one backend watcher per group
	•	Fan out raw events to all matching WatchSpecs (Fan-out is implemented by iterating only over WatchSpecs bound to the emitting backend group)
	•	Validate WatchSpec → handler bindings at startup:
		•	If a WatchSpec specifies target_handlers:
			•	All referenced handler_id values MUST exist in the owning realm’s registered handlers.
			•	Missing handler references are fatal configuration errors.

Deduplication guarantee:
	•	Only one watcher per external resource
	•	Multiple realms can subscribe independently

10.1 — Error Handling (v1)
	•	Backends must handle transient failures internally (retry with simple backoff).
	•	Backend failures must not crash YggdrasilCore.
	•	If a WatchSpec’s filter_expr, build_scope, or build_payload raises:
		•	Log includes: realm_id, handler_id(s), backend key, and a small excerpt of RawWatchEvent.
		•	Other WatchSpecs continue unaffected.
	•	No retries, dead-lettering, or circuit breakers in v1.

Purpose: fail fast, isolate faults, keep the system alive.

10.2 — Backpressure (v1)
	•	v1 assumes event rates are manageable.
	•	WatcherManager processes events synchronously per backend stream.
	•	No internal queues or throttling mechanisms are introduced in this phase.
	•	Backpressure and buffering strategies are explicitly deferred.

⸻
11.	Configuration Layer

All connection details live in configuration, not code.

Example:
```
{
  "endpoints": {
    "couch_primary": {
      "backend": "couchdb",
      "url": "https://couch.example.org",
      "auth": {
        "user_env": "YGG_COUCH_USER",
        "pass_env": "YGG_COUCH_PASS"
      }
    },
    "lims_pg": {
      "backend": "postgres",
      "dsn_env": "YGG_LIMS_DSN"
    }
  },
  "connections": {
    "projects_db": { "endpoint": "couch_primary", "resource": { "db": "projects" } },
    "yggdrasil_db": { "endpoint": "couch_primary", "resource": { "db": "yggdrasil" } },
    "plans_db": { "endpoint": "couch_primary", "resource": { "db": "yggdrasil_plans" } }
  }
}
```

WatchSpecs refer to connections by logical name only.
Secrets are injected via environment variables (or “env var references”), not stored in config.

11.1 — Graceful Shutdown
	•	On SIGINT/SIGTERM:
		•	WatcherManager stops accepting new backend events.
		•	Backends are instructed to stop cleanly.
		•	In-flight event handling is allowed to complete.
	•	No drain timeout enforcement in v1.
	•	Process exits once active watcher loops terminate.

11.2 — Observability (v1)
	•	Structured logging is required at:
		•	backend start/stop
		•	backend error
		•	WatchSpec match (debug-level)
	•	Logs must include:
		•	realm_id
		•	backend key (backend, connection, resource)
	•	No metrics, health endpoints, or dashboards in v1.
	•	Observability beyond logs is deferred.

⸻
12.	Explicit Exception: PlanWatcher

PlanWatcher is excluded from this refactor.

Reasons:
	•	It watches Yggdrasil’s internal database
	•	It enforces execution correctness and run-token semantics
	•	It is global, not realm-specific

PlanWatcher remains:
	•	Core infrastructure
	•	Manually wired
	•	Outside the WatchSpec system

This exception is deliberate.

⸻
13.	Migration Strategy

Phase 1 — Infrastructure
	•	Introduce WatcherBackend abstraction
	•	Implement CouchDB backend using existing logic
	•	Add WatcherManager

Phase 2 — Realm Support
	•	Add get_watch_specs() discovery
	•	Wire WatchSpecs into core

Phase 3 — Incremental Migration
	•	Migrate test realm watcher to WatchSpec provider via ygg.watchspec (reference implementation); remove _setup_test_realm_watcher() wiring.
	•	Migrate any remaining domain watchers if needed
	•	Remove hardcoded domain watchers from core (without breaking PlanWatcher)

No breaking changes to:
	•	handlers
	•	planners
	•	engine
	•	plan approval

⸻
14.	Success Criteria
	•	External realms add watchers without modifying Yggdrasil
	•	Only one watcher exists per external resource
	•	Plan approval and execution behavior is unchanged
	•	Test realm functions fully via WatchSpec
	•	No domain logic exists in watcher backends

⸻
15.	Deferred Topics
	•	Result sinks and data export
	•	Large file delivery
	•	Engine concurrency
	•	Concrete streaming backends (Kafka, MQTT)
	•	Filesystem backend “marker readiness” semantics (e.g., RTAComplete-style aggregation)
	•	Postgres backend implementation details (LISTEN/NOTIFY vs polling)