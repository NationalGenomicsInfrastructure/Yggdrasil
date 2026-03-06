# PRD: Read-Only Data Access for Realms (CouchDB v1)

**Version**: v0.2
**Status**: Implementation-ready
**Target branch**: data-access
**Owner**: Yggdrasil core

## Changelog

- v0.2: Updated based on detailed planning session. Key changes: corrected injection point (BaseHandler lazy property + `build_planning_context`), split `require_one` into two methods, dropped `default_timeout_s` from v1 scope, added config-loading-once constraint, documented `generate_plan_draft` signature stays unchanged.

## 1. Problem / Motivation
Some realms need to read additional data sources beyond the triggering watched document:
- Join across two CouchDB databases (e.g., samplesheet_info + flowcell DB) keyed by a shared field (flowcell_id).
- Validate readiness at plan execution time using extra documents (e.g., "transferred_to_hpc" event present, run folder exists).
- Support future features like file-transfer orchestration that depends on metadata stored outside the watched DB.

We want a safe, read-only API for realms to fetch documents without letting realms:
- Construct raw CouchDB handlers directly (credential/endpoint handling stays in core).
- Write to arbitrary databases.
- Access databases they are not explicitly allowed to read.

## 2. Goals
G1. Provide a read-only CouchDB client to realms via context injection (`ctx.data`).
G2. Enforce explicit allowlisting: which realm can read which connection(s).
G3. Reuse existing `external_systems.endpoints` + `connections` configuration (no duplication).
G4. Make the implementation easy to extend later (e.g., add Postgres read client) without changing realm code patterns.
G5. Keep the watcher subsystem realm-agnostic (no realm-specific internal event types).
G6. Deterministic caching to reduce repeated client creation.

## 3. Non-Goals
NG1. Write access to external databases (no inserts/updates/deletes).
NG2. A generic "join store" / pending-doc persistence mechanism.
NG3. A new compound-watcher feature at this time (this PR is only for read access).
NG4. Supporting Postgres watchers (not in scope).
NG5. Adding new watcher defaults beyond existing watcher behavior (not in scope).

## 4. User Stories
US1 (Demux join): As a demux realm, when a samplesheet_info doc changes, I want to look up the related flowcell doc by flowcell_id and decide whether I can generate a demux plan.
US2 (Late-arriving info): As a demux realm, when a flowcell doc changes, I want to look up the samplesheet_info doc and decide whether demux is ready.
US3 (Execution checks): As a step, I want to fetch the flowcell doc again at execution time to validate the run folder exists and contains expected sentinels (`RTAComplete.txt`, etc.).

## 5. Design Overview

### 5.1 Context Injection

Add an optional `data` field to:
- `PlanningContext`
- `StepContext`

**Injection into PlanningContext**: `BaseHandler` owns a lazy `_data_access` property that constructs `DataAccess(self.realm_id)` once on first access. Realm authors MUST construct `PlanningContext` via `BaseHandler.build_planning_context(...)`, which always injects `data=self._data_access`. Direct `PlanningContext(...)` construction without `data` raises a `RuntimeError` at instantiation time.

**Important**: `generate_plan_draft`'s abstract signature is NOT changed. No core restructuring required.

**Injection into StepContext**: `Engine.run()` constructs `DataAccess(plan.realm)` and passes it when building `StepContext`.

### 5.2 DataAccess API (v1)
- `DataAccess.couchdb(connection_name: str) -> CouchDBReadClient`

`CouchDBReadClient` supports:
- `get(doc_id: str) -> dict | None`
- `find_one(selector: dict) -> dict | None  # always uses limit=1 internally`
- `find(selector: dict, *, limit: int | None = None) -> list[dict]`
- `fetch_by_field(field: str, value: Any, *, limit: int | None = None) -> list[dict]`
- `require(doc_id: str) -> dict` (raises `DataAccessNotFoundError` if missing)
- `require_one(selector: dict) -> dict` (raises `DataAccessNotFoundError` if no match)

All methods are `async`. They wrap the synchronous cloudant SDK using `asyncio.to_thread()` (default executor; bounded executor deferred to a future PR).

`find_one` always executes with `limit=1`. `find(..., limit=N)` is for multi-result queries and is clamped to `max_limit`.

No write methods. No raw server/client exposure.

### 5.3 Config + Allowlisting
We extend existing connection definitions under:
`main.json -> external_systems -> connections -> <connection_name>`

Each connection may include optional data access policy:
```
data_access:
    realm_allowlist: ["demux", "other_realm"]   # REQUIRED if data_access is present
    max_limit: 200                              # optional override
```

Rules:
- A realm may only read a connection if:
  - connection has `data_access` configured AND
  - `realm_id` is listed in `data_access.realm_allowlist`
- No wildcard support in v1 (no "*"). Explicit only.
- If a realm requests an unallowed connection: raise `DataAccessDeniedError` (fatal for that plan generation/step unless realm catches).

**Note**: `default_timeout_s` is intentionally omitted from v1. It will be added in a future PR when actually wired to Cloudant's request options. Dead config is worse than absent config.

Optional global defaults (main.json):
```
external_systems -> data_access_defaults -> couchdb:
  max_limit: 200
```

Connection-level `data_access` overrides global defaults.

Example config:
```json
{
  "external_systems": {
    "endpoints": {
      "couchdb": {
        "backend": "couchdb",
        "url": "http://host:5984",
        "auth": { "user_env": "YGG_COUCH_USER", "pass_env": "YGG_COUCH_PASS" }
      }
    },
    "data_access_defaults": {
      "couchdb": { "max_limit": 200 }
    },
    "connections": {
      "samplesheet_info_db": {
        "endpoint": "couchdb",
        "resource": { "db": "samplesheet_info" },
        "data_access": {
          "realm_allowlist": ["demux"]
        }
      },
      "flowcell_db": {
        "endpoint": "couchdb",
        "resource": { "db": "flowcells" },
        "data_access": {
          "realm_allowlist": ["demux"],
          "max_limit": 50
        }
      }
    }
  }
}
```
Effective settings = `data_access_defaults.couchdb` merged with per-connection `data_access` overrides; authorization is per-connection via `realm_allowlist`.

### 5.4 Resolver
Create a shared resolver for external_systems config:
- `lib/core_utils/external_systems_resolver.py`

Responsibilities:
- `load_external_systems_config(cfg=None) -> dict`: loads once from `ConfigLoader().load_config("main.json")` (or accepts injected cfg dict in tests). Returns a plain dict copy (not MappingProxyType).
- `resolve_endpoint(endpoint_name, cfg) -> ResolvedEndpoint`
- `resolve_connection(connection_name, cfg) -> ResolvedConnection`

Both `resolve_*` functions take `cfg: dict` as a required positional argument — callers load config first and pass it. This ensures config is loaded exactly once.

`WatcherManager` is updated to use this resolver and removes/replaces `_resolve_connection_config`. WatcherManager continues to merge its watch-specific settings (`defaults` + `connection.watch`) on top of what the resolver returns; those settings are WatcherManager-only concern.

### 5.5 Caching Strategy (Definitive)
**Config**: `DataAccess.__init__` calls `load_external_systems_config()` once and stores the result. All subsequent `couchdb()` calls use the stored dict — no repeated disk reads.

**Clients**: `DataAccess` keeps a dict `_couchdb_clients[connection_name] = CouchDBReadClient`. Under the hood, `CouchDBReadClient` wraps a `CouchDBHandler` constructed on first `couchdb()` call for that connection name.

Rationale: `connection_name` is the stable identity in our configuration model and aligns with allowlisting/policy.

### 5.6 Context Construction API (Realm Authoring Contract)

Realm authors MUST use `BaseHandler.build_planning_context(...)` to construct `PlanningContext`. Direct `PlanningContext(...)` instantiation is guarded by `__post_init__` and raises a `RuntimeError` with a clear message pointing to the correct API.

```python
# WRONG — raises RuntimeError:
ctx = PlanningContext(realm=self.realm_id, scope=..., ...)

# CORRECT:
ctx = self.build_planning_context(scope=..., ...)
```

`build_planning_context` is a concrete method on `BaseHandler` that:
- Derives `realm` from `self.realm_id` automatically.
- Always injects `data=self._data_access`.
- Accepts all other `PlanningContext` fields as keyword-only arguments.

## 6. Implementation Plan

See `docs/design/realm_data_access/implementation_plans/` for the phased implementation plan.

**Phase 0**: Update PRD (this document).
**Phase 1**: External Systems Resolver + WatcherManager refactor.
**Phase 2**: DataAccess package + context injection + enforcement + tests.
**Phase 3**: Test completeness + acceptance check.

## 7. Acceptance Criteria
AC1. Realms can read allowed connections via `ctx.data.couchdb("name")` and get documents.
AC2. Realms cannot read unallowed connections (explicit `DataAccessDeniedError`).
AC3. No write methods are exposed; no raw CouchDBHandler/server leakage.
AC4. WatcherManager and DataAccess use the same resolver logic (single source of truth).
AC5. Connection caching is by `connection_name`.
AC6. No wildcard support in allowlists in v1 (explicit only).
AC7. Direct `PlanningContext(...)` construction without `data` raises `RuntimeError` at instantiation.

## 8. Risks / Mitigations
R1. Misconfiguration causes runtime failures.
- Mitigation: validate config on startup (optional future), and provide clear exceptions.

R2. View/index needs for field-based lookup (flowcell_id not equal _id).
- Mitigation: start with selector queries; add dedicated view usage later if performance requires.

R3. Overreach: realms start depending heavily on external reads.
- Mitigation: keep API small, read-only, allowlist required, and prefer fetching in steps when possible.

R4. Thread pool contention under high load.
- Mitigation: `asyncio.to_thread()` with default executor is acceptable for v1. Add bounded `ThreadPoolExecutor` in a future PR if contention observed.
