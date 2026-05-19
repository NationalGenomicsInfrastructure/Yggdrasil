# PRD: Realm-Scoped DataAccess with Controlled Writes

**Version**: v0.3  
**Status**: Draft  
**Target branch**: data-access  
**Owner**: Yggdrasil core

## Changelog

- v0.3: Evolves the read-only DataAccess design into a controlled read/write access layer. Adds realm- and phase-scoped permissions, execution-only writes, write traceability, and a backend-adapter boundary. CouchDB remains the first supported backend, but the design avoids hard-coding DataAccess around CouchDB-specific concepts.
- v0.2: Read-only DataAccess for realm-controlled reads from CouchDB.

## 1. Problem / Motivation

The `dmx` realm needs to create or update a pre-demux `x_flowcells` document with a TACA-compatible shape. The document is derived from runtime inputs such as:

- `RunInfo.xml`
- `RunParameters.xml`
- `demux_sample_info`

This document should be written during plan execution by a realm step, not during planning and not by bypassing Yggdrasil internals.

The current DataAccess design supports controlled reads, but not a proper Yggdrasil-owned write path. Without an official write API, realm code may be tempted to reach into raw CouchDB client internals. That breaks policy enforcement, traceability, and backend abstraction.

Yggdrasil needs a controlled write path that:

- Keeps realm code away from raw backend clients.
- Enforces per-realm permissions.
- Distinguishes planning from execution.
- Keeps planning side-effect free.
- Emits traceable write events.
- Remains backend-agnostic at the core policy level.

## 2. Goals

G1. Keep a single `DataAccess` abstraction for both planning and execution.  
G2. Add controlled write support for execution steps.  
G3. Enforce permissions by realm, phase, connection, and operation.  
G4. Prevent writes during planning.  
G5. Keep the generic policy model backend-agnostic, using capabilities such as `read` and `write`.  
G6. Keep backend-specific options isolated from generic policy.  
G7. Ensure all writes go through Yggdrasil-owned access clients, never raw backend clients.  
G8. Emit traceable events for authorized writes and failed write attempts.  
G9. Preserve a clean realm-author API: easy to use correctly, hard to misuse.  
G10. Structure DataAccess so CouchDB is the first backend client, not a permanent architectural assumption.

## 3. Non-Goals

NG1. No raw CouchDB/Cloudant client exposure to realms.  
NG2. No planning-time writes.  
NG3. No delete support in the first write iteration.  
NG4. No generic SQL/PostgreSQL implementation in this PR.  
NG5. No wildcard permissions in this PR.  
NG6. No backward-compatibility requirement for the old `realm_allowlist` shape unless trivial.  
NG7. No full generic CRUD abstraction across all possible backends yet.

## 4. User Stories

US1 (dmx pre-demux document): As the `dmx` realm, during execution I want to create or update a TACA-compatible `x_flowcells` document through `ctx.data`, so the write is authorized and traceable.

US2 (planning remains side-effect free): As a Yggdrasil operator, I want planning to be read-only so plan generation can safely inspect external state without mutating systems.

US3 (realm-specific permissions): As a Yggdrasil maintainer, I want two realms using the same connection to have different permissions, so one realm can write while another can only read.

US4 (future backend support): As a Yggdrasil core developer, I want DataAccess to support future backend clients such as PostgreSQL without reshaping realm-facing APIs.

## 5. Design Overview

### 5.1 DataAccess remains the single abstraction

Realm code continues to access external systems through `ctx.data`.

The preferred realm-author API should be resource/connection-oriented, not backend-oriented:

```python
client = ctx.data.resource("x_flowcells_db")
```

or, if a better name is chosen during implementation:

```python
client = ctx.data.connection("x_flowcells_db")
```

The connection name is enough for DataAccess to resolve:

```text
DataAccess
 ├── resolves connection name
 ├── resolves endpoint/backend
 ├── resolves concrete resource
 ├── resolves realm/phase permissions
 ├── constructs backend-specific client internally
 └── returns official Yggdrasil-owned resource client
```

The realm author should generally not need to know whether the backing system is CouchDB, PostgreSQL, a filesystem, or something else, as long as the operation they need is part of the generic resource-client surface.

Backend-specific helpers may still be useful for advanced or backend-specific operations:

```python
ctx.data.couchdb("x_flowcells_db")
```

However, backend-specific helpers should be optional, not the primary path for ordinary realm code. They should validate that the requested connection actually resolves to the expected backend and fail clearly if it does not.

The initial implementation may keep `ctx.data.couchdb(...)` as the first concrete method while introducing `ctx.data.resource(...)` / `ctx.data.connection(...)` as the preferred future-facing API. The important design rule is that CouchDB must not leak into the generic core policy model. Backend-specific behavior belongs in backend-specific adapter/client code.

### 5.2 Phase-aware DataAccess

`DataAccess` is bound to both a realm and a phase:

```python
DataAccess(realm_id="dmx_realm", phase="planning")
DataAccess(realm_id="dmx_realm", phase="execution")
```

Valid phases:

```python
DataAccessPhase = Literal["planning", "execution"]
```

Planning contexts receive `phase="planning"`. Step contexts receive `phase="execution"`.

This allows DataAccess to answer authorization questions such as:

```text
Can realm dmx_realm write to x_flowcells_db during execution?
```

### 5.3 Permission model

Generic permissions are intentionally small:

```python
DataAccessPermission = Literal["read", "write"]
```

Permissions are explicit. `write` does not imply `read` for realm-authored operations.

A realm that needs to inspect a remote document and then write a derived document must be granted both:

```json
"permissions": ["read", "write"]
```

However, adapter-internal mechanics required to complete an authorized write, such as fetching `_rev` for a CouchDB upsert, are considered part of the write operation and do not grant the realm general read access.

No combined permission such as `read-write` is introduced. This avoids aliases and keeps validation simple.

### 5.4 Config shape

The backend is already defined by the endpoint:

```json
"endpoints": {
  "couchdb": {
    "backend": "couchdb",
    "url": "host.server.com:7321",
    "auth": {
      "user_env": "YGG_COUCH_USER",
      "pass_env": "YGG_COUCH_PASS"
    }
  }
}
```

Connections refer to endpoints:

```json
"x_flowcells_db": {
  "endpoint": "couchdb",
  "resource": { "db": "x_flowcells" }
}
```

Therefore backend-specific DataAccess options should not repeat the backend name. They live under either global backend defaults or the connection’s `data_access.options` block.

Recommended config:

```json
{
  "external_systems": {
    "endpoints": {
      "couchdb": {
        "backend": "couchdb",
        "url": "host.server.com:7321",
        "auth": {
          "user_env": "YGG_COUCH_USER",
          "pass_env": "YGG_COUCH_PASS"
        }
      }
    },
    "defaults": {
      "couchdb": {
        "start_seq": "0",
        "max_limit": 200
      }
    },
    "connections": {
      "x_flowcells_db": {
        "endpoint": "couchdb",
        "resource": {
          "db": "x_flowcells"
        },
        "data_access": {
          "realms": {
            "dmx_realm": {
              "planning": {
                "permissions": []
              },
              "execution": {
                "permissions": ["read", "write"]
              }
            }
          },
          "options": {
            "max_limit": 50
          }
        }
      }
    }
  }
}
```

Rules:

- `endpoint.backend` selects the adapter.
- `resource` identifies the backend resource.
- `external_systems.defaults.<backend>` defines backend-level defaults for that backend/endpoint type. Watcher and DataAccess code may each override the relevant options at their own connection-level configuration sites.
- `connection.data_access.options` optionally overrides backend defaults for that connection.
- `data_access.realms` defines realm/phase permissions.
- Missing realm policy means no access.
- Missing phase policy means no permissions for that phase.
- Planning writes are denied even if misconfigured.

### 5.5 Backend-specific options

`max_limit` is CouchDB-specific or at least query-backend-specific. It should not be part of a forced universal schema.

Default backend options should live under the existing system-level defaults namespace:

```json
"external_systems": {
  "defaults": {
    "couchdb": {
      "start_seq": "0",
      "max_limit": 200
    }
  }
}
```

This keeps defaults grouped by backend/endpoint type. Watcher code and DataAccess code can then each consume the options they understand and apply more specific overrides from the selected connection they are operating on.

Connection-specific overrides may live at:

```json
"data_access": {
  "options": {
    "max_limit": 50
  }
}
```

Effective DataAccess options are `external_systems.defaults.<backend>` merged with connection-level `data_access.options`, with the adapter consuming only the options relevant to DataAccess. The CouchDB adapter interprets `max_limit` as the maximum query result limit for methods such as `find`. Watcher-specific options such as `start_seq` may exist in the same backend defaults but are ignored by the DataAccess adapter. Future backend adapters may interpret different options or reject unknown options during validation.

### 5.6 Realm author API

Realm authors should not choose between blocking and non-blocking write methods.

Planning currently runs in an async handler context. Execution steps are currently synchronous functions. Existing read access therefore exposes async planning methods and blocking execution methods.

This PR should move toward a cleaner author-facing API where the common method names are phase-aware:

```python
# planning handler
client = ctx.data.resource("config_db")
doc = await client.get("config:pipeline_defaults")
```

```python
# execution step
client = ctx.data.resource("config_db")
doc = client.get("config:pipeline_defaults")
```

The same method name may return an awaitable in planning and a concrete value in synchronous execution. This is acceptable because the surrounding authoring context is already different: planning handlers are async, while current steps are sync.

DataAccess should not try to make this work without `await` in async planning code. Automatically awaiting “behind the scenes” inside a normal method is not a good Python API: an async operation cannot be transparently awaited from a synchronous-looking call without either blocking the event loop or returning a coroutine/task-like object. Realm authors should use `await` in async planning code.

Writes are execution-only in this PR, so the realm-facing write API should be a single synchronous method:

```python
result = ctx.data.resource("x_flowcells_db").put(
    doc_id=doc_id,
    doc=doc,
    mode="upsert",
)
```

No `put_blocking` is exposed in the first implementation.

The implementation may keep existing `get_blocking` methods temporarily while migrating docs and realm code toward phase-aware `get`. Long term, the preferred API is:

```text
planning async context:    await client.get(...)
execution sync context:    client.get(...)
execution write context:   client.put(...)
```

If Yggdrasil later supports async steps, DataAccess can adapt by returning an async execution client for async steps, while preserving the same operation names where possible.

### 5.7 Resource client and CouchDB client structure

The public resource client returned by DataAccess should expose a small operation-oriented surface:

```python
client = ctx.data.resource("x_flowcells_db")
client.get(...)
client.find(...)
client.put(...)
```

For a CouchDB-backed connection, this resource client may be implemented by a renamed `CouchDBDataClient`.

Internally it may compose smaller read/write helpers:

```text
CouchDBDataClient
 ├── _reader: CouchDBReadClient
 ├── _writer: CouchDBWriteClient | None
 └── _authorizer: DataAccessAuthorizer
```

The DataAccess layer should be structured so the returned client can later be backed by another adapter, for example `PostgresDataClient`, without changing generic permission resolution.

Backend-specific helper methods, if retained, should return the same underlying backend client:

```python
ctx.data.couchdb("x_flowcells_db")
```

but ordinary realm code should prefer the backend-neutral resource/connection method.

Authorization can happen either when constructing the client or when invoking individual operations. The operation must still fail with a clear `DataAccessDeniedError` if the realm lacks permission.

### 5.8 CouchDB write API

Initial write support should include one operation:

```python
put(doc_id: str, doc: dict[str, Any], *, mode: Literal["create", "update", "upsert"] = "upsert") -> DataAccessWriteResult
```

Modes:

- `create`: create only; fail if the document exists.
- `update`: update only; fail if the document does not exist.
- `upsert`: create if missing, update if present.

For CouchDB, `upsert` may require fetching the existing document revision internally. This is part of the authorized write operation and does not expose general read capability to the realm.

Suggested result model:

```python
@dataclass(frozen=True)
class DataAccessWriteResult:
    backend: str
    connection_name: str
    resource: str
    operation: str
    doc_id: str
    status: Literal["created", "updated"]
    old_rev: str | None
    new_rev: str | None
```

A status string is preferred over `created: bool` because it is clearer in logs and leaves room for future statuses without adding more booleans. Timestamps should not be encoded as a substitute for status. Write timing belongs to the event layer or trace metadata, not to the semantic write result itself.

### 5.9 Trace context and write events

DataAccess should receive core-owned trace metadata. Realm authors should not construct this manually in normal usage.

Suggested model:

```python
@dataclass(frozen=True)
class DataAccessTraceContext:
    realm: str
    phase: Literal["planning", "execution"]
    plan_id: str | None = None
    run_id: str | None = None
    step_id: str | None = None
    step_name: str | None = None
    scope: dict[str, Any] | None = None
    emitter: BaseEmitter | None = None
```

For planning, `phase="planning"` and most execution fields are absent.

For execution, `Engine.run()` or the StepContext construction path should provide plan/run/step identity and the emitter.

Write attempts should emit structured events such as:

- `data_access.write.succeeded`
- `data_access.write.failed`
- optionally `data_access.write.denied`

Event payload should include:

```json
{
  "realm": "dmx_realm",
  "phase": "execution",
  "plan_id": "...",
  "run_id": "...",
  "step_id": "...",
  "connection": "x_flowcells_db",
  "backend": "couchdb",
  "operation": "upsert",
  "doc_id": "...",
  "created": true,
  "old_rev": null,
  "new_rev": "1-..."
}
```

## 6. dmx Target Usage

The `dmx` realm step should look approximately like:

```python
@step
def create_x_flowcell_doc(ctx: StepContext, flowcell_id: str, scenario: dict) -> StepResult:
    doc_id = build_x_flowcell_doc_id(flowcell_id)
    doc = build_taca_x_flowcell_doc(scenario)

    result = ctx.data.resource("x_flowcells_db").put(
        doc_id=doc_id,
        doc=doc,
        mode="upsert",
    )

    return StepResult(metrics={
        "x_flowcell_doc_id": result.doc_id,
        "x_flowcell_created": result.created,
        "x_flowcell_rev": result.new_rev,
    })
```

The realm owns the domain transformation into TACA-compatible document shape. Yggdrasil owns authorization, persistence, and traceability.

## 7. Implementation Plan

Use the PRD as design intent, but create your own implementation plan (separated in as many phases as you want) after inspecting the current Yggdrasil repo. Keep changes scoped to Yggdrasil core. Do not include any realm implementation work unless explicitly requested separately. Include documentation updates and tests as first-class phases.
Below there are example phases you may use, or get inspired to create your own.

### Example phases:

### Phase 1: Update policy model

- Replace `realm_allowlist` with per-realm/per-phase permissions.
- Add config parsing for:
  - `data_access.realms.<realm_id>.<phase>.permissions`
  - `data_access.options`
- Validate permissions against known values: `read`, `write`.
- Missing realm or phase policy means no permissions.

### Phase 2: Make DataAccess phase-aware

- Add `phase` to `DataAccess.__init__`.
- Add `DataAccessTraceContext`.
- PlanningContext receives `DataAccess(..., phase="planning")`.
- StepContext receives `DataAccess(..., phase="execution", trace_context=...)`.

### Phase 3: Add backend-client boundary

- Add a preferred backend-neutral access method, named either `DataAccess.resource(connection_name)` or `DataAccess.connection(connection_name)`.
- Keep `DataAccess.couchdb(connection_name)` only as an optional backend-specific helper if useful.
- Internally resolve endpoint backend before constructing the client.
- Structure the implementation so future backend clients can be added without reshaping authorization logic.
- Avoid embedding CouchDB-only concepts in generic DataAccess policy classes.

### Phase 4: Add resource client and extend CouchDB implementation

- Introduce a generic resource-client boundary returned by `ctx.data.resource(...)` / `ctx.data.connection(...)`.
- Rename or supersede `CouchDBReadClient` with `CouchDBDataClient` for CouchDB-backed resources.
- Preserve existing read behavior.
- Move toward phase-aware method names: `await get(...)` in planning, `get(...)` in sync execution.
- Add a single realm-facing write method: `put(...)`.
- Do not expose `put_blocking` in this PR.

### Phase 5: Implement CouchDB write behavior

- Implement `create`, `update`, and `upsert` modes.
- Enforce execution-only writes.
- Enforce realm/phase `write` permission.
- Return `DataAccessWriteResult`.
- Ensure raw CouchDB handler/client remains private.

### Phase 6: Add write trace events

- Add structured event emission for successful writes.
- Add structured event emission for failed/denied writes where appropriate.
- Include realm, phase, plan, run, step, connection, backend, operation, document ID, and revision information.

### Phase 7: Update the documentation

- This PRD affects at least the realm-author API and config shape, so study the changes and update the documentation where needed

### Phase 8: Tests

Add tests for:

- Planning read allowed when `read` is granted.
- Planning write denied even if accidentally configured.
- Execution write denied without `write` permission.
- Execution write allowed with `write` permission.
- `write` does not grant realm-facing `read` permission.
- `read` + `write` permits inspect-then-write workflows.
- Missing realm policy denies access.
- Missing phase policy denies access.
- CouchDB `max_limit` still applies to query reads.
- `put(mode="create")` fails if document exists.
- `put(mode="update")` fails if document is missing.
- `put(mode="upsert")` creates or updates as expected.
- Write events include trace context.
- Realm code cannot access raw backend client internals through public API.

## 8. Acceptance Criteria

AC1. Realm steps can write authorized CouchDB documents through `ctx.data`.  
AC2. Realm code cannot access raw CouchDB clients through DataAccess public API.  
AC3. Planning-time writes are denied.  
AC4. Execution-time writes require explicit `write` permission.  
AC5. `write` does not imply realm-facing `read`.  
AC6. Backend-specific options such as `max_limit` are not part of the universal policy schema.  
AC7. DataAccess remains structured around exchangeable backend clients.  
AC8. The `dmx` realm can create/update `x_flowcells` documents from an execution step.  
AC9. DataAccess emits traceable write events with plan/run/step context.  
AC10. Existing read use cases continue to work.

## 9. Risks / Mitigations

R1. Realm authors may be confused by async reads in planning and blocking reads in execution.  
- Mitigation: Move toward phase-aware method names: `await client.get(...)` in async planning code and `client.get(...)` in sync execution steps. Keep `get_blocking` only as transitional compatibility if needed. For writes, expose only one execution-time method.

R2. Backend-specific options may become messy if too many are placed in `data_access.options`.  
- Mitigation: Prefer backend defaults under `external_systems.defaults.<backend>`, with connection-level `data_access.options` only for DataAccess-specific overrides. Each adapter should consume only the options relevant to its operation and ignore or validate unrelated backend defaults according to its policy.

R3. `upsert` may hide read-like behavior internally.  
- Mitigation: Document that adapter-internal reads required for authorized writes do not grant realm-facing read access.

R4. Future backends may not map cleanly to `read`/`write`.  
- Mitigation: Start small but keep the permission model extensible. Add new generic capabilities only when a real backend/use case requires them.

R5. Write event emission could fail after the backend write succeeds.  
- Mitigation: Treat backend write success as authoritative for the returned `DataAccessWriteResult`. Event emission failures should be caught and logged according to existing emitter failure policy. This PR should not require a full new reliability mechanism for secondary event emission, but it must not report the backend write as failed if only trace emission failed.

