PRD: Generic Watcher Architecture Refactor for Yggdrasil

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
	•	Filesystem watcher backend
	•	Postgres watcher backend

All backends implement a common interface.

Conceptual interface:
```
class WatcherBackend:
    async def start(self) -> None
    async def stop(self) -> None
    async def events(self) -> AsyncIterator[RawWatchEvent]
```

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
    event_kind: str              # "doc_changed", "file_created", ...
    filter_expr: dict | None     # optional predicate
    build_scope: Callable[[RawWatchEvent], dict]
    build_payload: Callable[[RawWatchEvent], dict]
```

Responsibilities:
	•	Express what to watch
	•	Filter raw events
	•	Convert raw events into Yggdrasil scope and payload

WatchSpecs are exposed by realms via an entry point:
```
def get_watch_specs() -> list[WatchSpec]:
    ...
```

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
	        event_type=spec.event_kind,
	        payload=payload,
	        source=backend_name,
	    )
	)
	```
	5.	Existing flow continues
		•	Realm planners generate PlanDraft
		•	PlanWatcher executes approved plans
		•	Engine emits ops events

⸻
10.	WatcherManager (Core)

WatcherManager orchestrates watcher lifecycle and fan-out.

Responsibilities:
	•	Collect WatchSpecs from all realms
	•	Group by (backend, connection, resource, event_kind)
	•	Instantiate one backend watcher per group
	•	Fan out raw events to all matching WatchSpecs

Deduplication guarantee:
	•	Only one watcher per external resource
	•	Multiple realms can subscribe independently

⸻
11.	Configuration Layer

All connection details live in configuration, not code.

Example:
```
[connections.couchdb.projects]
backend = "couchdb"
url = "https://couch.example.org"
db = "projects"

[connections.fs.raw_runs]
backend = "fs"
path = "/data/illumina"
```

WatchSpecs refer to connections by logical name only.

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
	•	Migrate test realm watcher
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