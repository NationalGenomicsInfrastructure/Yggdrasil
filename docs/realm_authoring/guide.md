# Realm Authoring Guide: RealmDescriptor

## Overview

Realms are packages that extend Yggdrasil with handlers and WatchSpecs.
Use the unified `ygg.realm` entry point with `RealmDescriptor` for registration.

This guide covers:
- Creating a RealmDescriptor
- Defining handlers with required attributes
- Configuring WatchSpecs for event routing
- Dev-mode gating patterns
- Validation rules and common pitfalls

## Quick Start

### 1. Define a Handler

The handler is the core of any realm. It subscribes to an event type, extracts a scope from the incoming document, and generates a plan:

```python
# my_realm/handlers.py
from typing import Any, ClassVar

from lib.core_utils.event_types import EventType
from yggdrasil.flow.base_handler import BaseHandler
from yggdrasil.flow.model import Plan
from yggdrasil.flow.planner import PlanDraft, PlanningContext


class MyProjectHandler(BaseHandler):
    event_type: ClassVar[EventType] = EventType.COUCHDB_DOC_CHANGED
    handler_id: ClassVar[str] = "project_handler"  # Unique within realm

    def derive_scope(self, doc: dict[str, Any]) -> dict[str, Any]:
        return {"kind": "project", "id": doc.get("_id", "unknown")}

    async def generate_plan_drafts(self, payload: dict[str, Any]) -> list[PlanDraft]:
        doc = payload.get("doc", {})
        ctx: PlanningContext = payload["planning_ctx"]

        steps = [...]

        plan = Plan(
            plan_id=f"my_realm:{ctx.scope['id']}",
            realm=self.realm_id or "my_realm",
            scope=ctx.scope,
            steps=steps,
        )
        return [PlanDraft(
            plan=plan,
            auto_run=True,  # Or False for approval workflow
            approvals_required=[],
            notes="My plan notes",
        )]
```

### 2. Register the Realm

Wire the handler into a `RealmDescriptor` and expose it via a `ygg.realm` entry point:

```python
# my_realm/__init__.py
from yggdrasil.core.realm import RealmDescriptor

from my_realm.handlers import MyProjectHandler


def get_realm_descriptor() -> RealmDescriptor:
    return RealmDescriptor(
        realm_id="my_realm",
        handler_classes=[MyProjectHandler],
        watchspecs=[],  # No watcher yet — realm is registered but not triggerable
    )
```

```toml
# pyproject.toml
[project.entry-points."ygg.realm"]
my_realm = "my_realm:get_realm_descriptor"
```

> At this point the realm is registered and Yggdrasil knows about it, but no events will reach it yet — there are no watchers configured to trigger it. Continue to Step 3 to wire up event-driven triggering.

> **Note:** The entry point name (left side) is just a discovery key. Only `RealmDescriptor.realm_id` is used for identity.

### 3. Add a WatchSpec (event-driven triggering)

To have a CouchDB change automatically trigger your handler, add a `WatchSpec` to the descriptor:

```python
# my_realm/__init__.py
from typing import Any

from lib.core_utils.event_types import EventType
from lib.watchers.watchspec import WatchSpec
from yggdrasil.core.realm import RealmDescriptor

from my_realm.handlers import MyProjectHandler, MyDeliveryHandler


def _build_scope(raw_event: Any) -> dict[str, str]:
    doc = getattr(raw_event, "doc", None) or {}
    return {"kind": "project", "id": doc.get("_id", "unknown")}


def _build_payload(raw_event: Any) -> dict[str, Any]:
    doc = getattr(raw_event, "doc", None) or {}
    return {
        "doc": doc,
        "reason": f"doc_change:{doc.get('_id', 'unknown')}",
    }


def _get_watchspecs() -> list[WatchSpec]:
    return [
        WatchSpec(
            backend="couchdb",
            connection="my_connection",  # Logical name from config
            event_type=EventType.COUCHDB_DOC_CHANGED,
            filter_expr={"==": [{"var": "doc.type"}, "my_doc_type"]},
            build_scope=_build_scope,
            build_payload=_build_payload,
            target_handlers=["project_handler"],  # Optional routing
        ),
    ]


def get_realm_descriptor() -> RealmDescriptor:
    return RealmDescriptor(
        realm_id="my_realm",
        handler_classes=[MyProjectHandler, MyDeliveryHandler],
        watchspecs=_get_watchspecs,  # Callable enables dev-mode gating
    )
```

> Passing `watchspecs` a callable (rather than a list) enables dev-mode gating. See [Dev-Mode Gating](#dev-mode-gating).

## Required Handler Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `event_type` | `ClassVar[EventType]` | Which events this handler subscribes to |
| `handler_id` | `ClassVar[str]` | Stable identifier within the realm |

### Handler Methods

| Method | Required | Description |
|--------|----------|-------------|
| `derive_scope(doc)` | Yes | Extract `{"kind": ..., "id": ...}` from document |
| `generate_plan_drafts(payload)` | Yes | Async method returning `list[PlanDraft]` |
| `run_now(payload)` | Inherited | Blocking entrypoint for CLI mode |

### Instance Attributes Set by Core

| Attribute | Type | Description |
|-----------|------|-------------|
| `realm_id` | `str \| None` | Set during registration (from RealmDescriptor) |

> **Important:** `realm_id` is an **instance variable**, not a ClassVar.
> Do not declare it as a class attribute; it is set by YggdrasilCore.

## WatchSpec Fields

| Field | Required | Type | Description |
|-------|----------|------|-------------|
| `backend` | Yes | `str` | Backend type: `"couchdb"`, `"fs"` (future) |
| `connection` | Yes | `str` | Logical connection name (from config) |
| `event_type` | Yes | `EventType` | EventType to emit when filter matches |
| `build_scope` | Yes | `Callable` | `RawWatchEvent → {"kind": ..., "id": ...}` |
| `build_payload` | Yes | `Callable` | `RawWatchEvent → payload dict` |
| `filter_expr` | No | `dict \| None` | JSON Logic predicate (None = match all) |
| `target_handlers` | No | `list[str] \| None` | List of handler_ids (None = all subscribers) |

## Filter Expressions

Use [JSON Logic](https://jsonlogic.com/) syntax. The filter context is the `RawWatchEvent` dict:

| Variable | Description |
|----------|-------------|
| `doc.*` | Document fields |
| `deleted` | `True` if deletion event |
| `meta.*` | Backend-specific metadata |

### Examples

**Match specific document type:**
```python
filter_expr = {"==": [{"var": "doc.type"}, "my_doc_type"]}
```

**Match non-deleted documents with status:**
```python
filter_expr = {
    "and": [
        {"==": [{"var": "doc.status"}, "active"]},
        {"==": [{"var": "deleted"}, False]},
    ]
}
```

**Match documents where a field exists:**
```python
filter_expr = {"!!": [{"var": "doc.project_id"}]}
```

## Dev-Mode Gating

Make `watchspecs` a callable that returns `[]` when disabled:

```python
from lib.core_utils.ygg_session import YggSession


def _get_watchspecs() -> list[WatchSpec]:
    """Callable for dev-mode gating."""
    if not YggSession.is_dev():
        return []  # No WatchSpecs = no watcher created
    return [WatchSpec(...)]


RealmDescriptor(
    ...,
    watchspecs=_get_watchspecs,  # Callable, invoked at discovery
)
```

This pattern ensures:
- **Handler is always registered** (for CLI/manual triggers)
- **Watcher only active in dev mode** (no events received in prod)

### Alternative: Return None from Descriptor

For realms that should be completely invisible when disabled, return `None`
from `get_realm_descriptor()`:

```python
def get_realm_descriptor() -> RealmDescriptor | None:
    """Return descriptor only when dev mode is enabled."""
    if not YggSession.is_dev():
        return None  # Realm not discovered at all
    return RealmDescriptor(...)
```

This is cleaner when you want **no handlers and no watchers** in production.

## Validation Rules (Fatal Errors)

YggdrasilCore validates realm configuration at startup. These violations cause
the daemon to fail immediately:

1. **`realm_id` must be unique** across all realms
2. **Every handler class must have `handler_id`** class attribute
3. **`(realm_id, handler_id)` must be unique** globally
4. **If `target_handlers` is set**, all IDs must exist in that realm
5. **If `target_handlers=None`**, at least one handler must subscribe to the `event_type`

### Example Validation Error

```
RuntimeError: WatchSpec from realm 'my_realm' references unknown
handler_id 'missing_handler'. Registered handlers: ['project_handler']
```

## Handler-Only Realms

Realms with handlers but no WatchSpecs are valid — useful when events are injected programmatically or from other handlers/steps:

```python
RealmDescriptor(
    realm_id="cli_tools",
    handler_classes=[ManualProcessHandler],
    watchspecs=[],  # No watching needed
)
```

Events can be triggered via:
- Direct `handle_event()` calls
- Other handlers/steps emitting events internally

## Event Flow Summary

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Startup (setup_realms)                       │
├─────────────────────────────────────────────────────────────────────┤
│  1. discover_realms() → finds ygg.realm entry points                │
│  2. Call get_realm_descriptor() for each realm                      │
│  3. Validate realm_id uniqueness                                    │
│  4. Instantiate handlers from handler_classes                       │
│  5. Call watchspecs() if callable, collect WatchSpecs               │
│  6. Validate WatchSpec → handler bindings                           │
│  7. Wire WatchSpecs into WatcherManager                             │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        Runtime (daemon mode)                        │
├─────────────────────────────────────────────────────────────────────┤
│  1. WatcherManager starts backend watchers                          │
│  2. Backend detects change → RawWatchEvent                          │
│  3. filter_expr evaluated → match/skip                              │
│  4. build_scope() + build_payload() → YggdrasilEvent                │
│  5. YggdrasilCore.handle_event() routes to subscribed handlers      │
│  6. Handler.generate_plan_drafts() → list[PlanDraft]               │
│  7. Plan persisted to yggdrasil_plans database                      │
│  8. PlanWatcher detects eligible plan → Engine executes             │
└─────────────────────────────────────────────────────────────────────┘
```

## Migration from Legacy Patterns

### From `ygg.handler` Entry Point

**Before (deprecated):**
```toml
[project.entry-points."ygg.handler"]
my_handler = "my_realm.handler:MyHandler"
```

**After:**
```toml
[project.entry-points."ygg.realm"]
my_realm = "my_realm:get_realm_descriptor"
```

### From CouchDBWatcher

**Before (deprecated):**
```python
from lib.watchers.couchdb_watcher import CouchDBWatcher

watcher = CouchDBWatcher(
    on_event=core.handle_event,
    changes_fetcher=db.fetch_changes,
    event_type=EventType.PROJECT_CHANGE,
)
```

**After:**
```python
# In your realm's get_realm_descriptor()
WatchSpec(
    backend="couchdb",
    connection="projects_db",
    event_type=EventType.COUCHDB_DOC_CHANGED,
    filter_expr={"==": [{"var": "doc.type"}, "project"]},
    build_scope=...,
    build_payload=...,
)
```

### From ScenarioDocWatcher (Test Realm)

The test realm watcher is now configured via WatchSpec in the realm's
`get_realm_descriptor()`. No custom watcher class needed.

## Common Pitfalls

### 1. Missing `handler_id`

```python
# ❌ Wrong - will fail validation
class MyHandler(BaseHandler):
    event_type = EventType.COUCHDB_DOC_CHANGED
    # Missing handler_id!
```

```python
# ✅ Correct
class MyHandler(BaseHandler):
    event_type: ClassVar[EventType] = EventType.COUCHDB_DOC_CHANGED
    handler_id: ClassVar[str] = "my_handler"
```

### 2. Declaring `realm_id` as ClassVar

```python
# ❌ Wrong - conflicts with instance variable set by core
class MyHandler(BaseHandler):
    realm_id: ClassVar[str] = "my_realm"  # Don't do this!
```

```python
# ✅ Correct - let core set it
class MyHandler(BaseHandler):
    # realm_id is set by YggdrasilCore during registration
    pass
```

### 3. WatchSpec Without Receivers

```python
# ❌ Wrong - WatchSpec emits COUCHDB_DOC_CHANGED but no handler subscribes
class MyHandler(BaseHandler):
    event_type: ClassVar[EventType] = EventType.OTHER  # different
    handler_id: ClassVar[str] = "my_handler"
    ...

RealmDescriptor(
    realm_id="orphan",
    handler_classes=[MyHandler],  # event_type mismatch — subscribes to a different EventType
    watchspecs=[
        WatchSpec(event_type=EventType.COUCHDB_DOC_CHANGED, ...)    # emits different type
    ],
)
```
Here, `WatchSpec.event_type` != `handler.event_type`. Handler subscribes to a different EventType (EventType.OTHER), so no handler in this realm will receive the WatchSpec’s emitted events (EventType.COUCHDB_DOC_CHANGED). This leads to fatal validation error.

### 4. Passing Handler Instances

```python
# ❌ Wrong - should be classes, not instances
RealmDescriptor(
    handler_classes=[MyHandler()],  # Instance!
)
```

```python
# ✅ Correct - pass classes
RealmDescriptor(
    handler_classes=[MyHandler],  # Class, not instance
)
```

## See Also

- [Realm Authoring Cookbook](cookbook.md) — common patterns for handlers, steps, and recipes
- [Flow API Overview](../flow_api/overview.md) — `@step`, Engine, emitters, `PlanDraft` fields
- [Architecture Overview](../architecture/overview.md) — how realms plug into the core
