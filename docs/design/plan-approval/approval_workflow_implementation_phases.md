# Yggdrasil Plan Approval & Execution Workflow
## Multi-Phase Implementation Plan

**Document Version:** 1.0  
**Status:** Ready for Implementation  
**Last Updated:** January 15, 2026

---

## 1. Executive Summary

This document outlines a **phased approach** to implementing the Plan Approval & Execution Workflow per the PRD (v1.0).

**Key Approach:**
- **4 sequential phases** with clear verification gates between each
- **Linear dependencies** (Phase N requires Phase N-1 complete)
- **High modularity** (components reusable in pluggable watcher architecture later)
- **Quality-first** (unit tests in Phases 1-2, integration tests in Phase 3, E2E in Phase 4)

**Estimated Total Effort:**
- ~2,000 LOC (implementation + tests)
- 4 independent delivery units, each ~500-600 LOC

---

## 2. Dependency Graph

```
Phase 1: Core Infrastructure
├─ ChangesFetcher (generic _changes stream)
├─ WatcherCheckpointStore (DB-backed checkpoint persistence)
└─ Plan serialization (to_dict/from_dict)
   ↓
Phase 2: Plan Persistence & Eligibility
├─ PlanDBManager (uses ChangesFetcher)
└─ is_plan_eligible() (pure function)
   ↓
Phase 3: PlanWatcher & Execution
├─ PlanWatcher (integrates checkpoint + fetcher + eligibility)
├─ execute_approved_plan() (uses eligibility logic + engine)
└─ Startup recovery (queries pending plans)
   ↓
Phase 4: Tests & Documentation
└─ End-to-end workflows + Genstat contract docs
```

**Key principle:** Each phase is *independently testable* but requires Phase N-1 artifacts.

---

## 3. PHASE 1: Core Infrastructure

### 3.1 Overview

Foundation layer providing three reusable components:
1. **ChangesFetcher**: Generic async CouchDB `_changes` stream (decoupled from ProjectDBManager)
2. **WatcherCheckpointStore**: DB-backed checkpoint persistence (replaces file-based `last_processed_seq`)
3. **Plan Serialization**: `Plan.to_dict()` / `from_dict()` for database storage

These components are designed to be **checkpoint-agnostic** and **fetcher-agnostic**—usable by any watcher.

**Estimated LOC:** ~500 (280 implementation + 220 tests)

---

### 3.2 Files to Create/Modify

| File | Action | Purpose |
|------|--------|---------|
| `lib/couchdb/changes_fetcher.py` | **CREATE** | Generic async _changes fetcher (no checkpoint logic) |
| `lib/couchdb/watcher_checkpoint_store.py` | **CREATE** | DB-backed checkpoint store (singleton per watcher name) |
| `yggdrasil/flow/model.py` | **MODIFY** | Add `Plan.to_dict()` / `from_dict()` methods |
| `tests/test_changes_fetcher.py` | **CREATE** | Unit tests for ChangesFetcher |
| `tests/test_watcher_checkpoint_store.py` | **CREATE** | Unit tests for WatcherCheckpointStore |
| `tests/test_plan_serialization.py` | **CREATE** | Roundtrip tests for Plan serialization |

---

### 3.3 Key Design Decisions

**ChangesFetcher Design:**
- **Decision:** Extract generic fetcher from `ProjectDBManager.get_changes()` into standalone module
- **Why:** ProjectDBManager conflates two concerns: generic streaming + project-specific filtering
- **Trade-off:** Fetcher remains *filtering-agnostic*; consumers apply their own filters (caller responsibility)
- **Benefit:** Reusable by PlanWatcher, future watchers without duplication

**WatcherCheckpointStore Design:**
- **Decision:** Persist checkpoints to `yggdrasil` DB (not file-based)
- **Why:** Auditable, shareable across instances, integrates with existing infrastructure
- **Schema:** Documents with `_id = "watcher_checkpoint:<WatcherName>"`
- **Safety:** Uses `_rev` for optimistic locking (upsert-safe, no lost updates)
- **Fallback:** On startup, if checkpoint missing, watchers can query all pending items

**Plan Serialization Design:**
- **Decision:** Simple dataclass-based to_dict/from_dict (no custom serializer framework)
- **Why:** Straightforward, testable, matches existing `Plan` structure
- **Include:** StepSpec serialization (deps list → JSON array, etc.)
- **Skip:** Engine-level caching fingerprints (not part of intent)

---

### 3.4 Detailed Specifications

#### 3.4.1 ChangesFetcher

**Purpose:** Async generator yielding raw document changes from CouchDB `_changes` feed.

**Pseudocode Signature:**

```python
class ChangesFetcher:
    """
    Generic CouchDB _changes feed streamer.
    Decoupled from checkpoint logic (caller manages checkpoints).
    """
    
    def __init__(
        self,
        db_handler: CouchDBHandler,
        include_docs: bool = True,
        retry_delay_sec: float = 2.0,
        max_retries: int = 3,
    ):
        """Init fetcher with DB connection and retry policy."""
    
    async def fetch_changes(
        self,
        since: str | None = None,
    ) -> AsyncGenerator[dict[str, Any], None]:
        """
        Yield (doc, seq) tuples from _changes feed starting at 'since'.
        Handles connection retries and JSON parsing.
        
        Raises ApiException on persistent failures.
        Yields: {"doc": <full_doc>, "seq": <seq_id>, "id": <doc_id>}
        """
    
    async def stream_changes_continuously(
        self,
        since: str | None = None,
        poll_interval_sec: float = 5.0,
    ) -> AsyncGenerator[dict[str, Any], None]:
        """
        Convenience method: yield changes in a continuous loop.
        On fetch error, sleep poll_interval and retry.
        """
```

**Key Behaviors:**
- Retries on transient errors (3 max by default, configurable)
- Yields raw `_changes` entries (no filtering, no checkpoint updates)
- Stream is *continuous* (caller stops iteration when appropriate)
- Supports `include_docs=True` to avoid extra fetches

---

#### 3.4.2 WatcherCheckpointStore

**Purpose:** Singleton checkpoint store per watcher name. Persists to CouchDB with optimistic locking.

**Pseudocode Signature:**

```python
class WatcherCheckpointStore:
    """
    Singleton per watcher name. Manages checkpoint persistence in yggdrasil DB.
    Uses _rev-based optimistic locking for race-safety.
    """
    
    def __init__(
        self,
        watcher_name: str,
        db_handler: CouchDBHandler,  # connection to 'yggdrasil' DB
    ):
        """Init store for named watcher."""
    
    def get_checkpoint(self) -> str | None:
        """
        Fetch last saved checkpoint seq. Returns None if never saved.
        Falls back to default ("0") if doc missing.
        """
    
    def save_checkpoint(self, seq: str) -> bool:
        """
        Save checkpoint with _rev-safe upsert. 
        Returns True on success, False on conflict (caller retries).
        """
    
    def checkpoint_doc_id(self) -> str:
        """Return canonical doc ID: 'watcher_checkpoint:<WatcherName>'"""
```

**DB Document Schema:**

```json
{
  "_id": "watcher_checkpoint:PlanWatcher",
  "_rev": "2-abc123...",
  
  "type": "watcher_checkpoint",
  "watcher_name": "PlanWatcher",
  "last_seq": "1234-g1AAAABteJzLYWBgYM9gM",
  "updated_at": "2025-01-14T10:30:00Z",
  "updated_by": "yggdrasil-core"
}
```

**Key Behaviors:**
- Doc ID includes watcher name (scoped per watcher)
- `_rev` prevents lost updates in concurrent scenarios
- Upsert logic: fetch current _rev, include in PUT request
- If conflict (409), caller retries (backoff recommended)
- Missing checkpoint treated as "0" (start from beginning)

---

#### 3.4.3 Plan Serialization

**Existing Model** (from `yggdrasil/flow/model.py`):

```python
@dataclass
class StepSpec:
    step_id: str
    name: str
    fn_ref: str
    params: dict[str, Any]
    deps: list[str] = field(default_factory=list)
    scope: dict[str, Any] = field(default_factory=dict)
    inputs: dict[str, str] = field(default_factory=dict)

@dataclass
class Plan:
    plan_id: str
    realm: str
    scope: dict[str, Any]
    steps: list[StepSpec] = field(default_factory=list)
```

**New Methods:**

```python
# Add to Plan class:

def to_dict(self) -> dict[str, Any]:
    """Serialize Plan to dict for database storage."""
    return {
        "plan_id": self.plan_id,
        "realm": self.realm,
        "scope": self.scope,
        "steps": [
            {
                "step_id": s.step_id,
                "name": s.name,
                "fn_ref": s.fn_ref,
                "params": s.params,
                "deps": s.deps,
                "scope": s.scope,
                "inputs": s.inputs,
            }
            for s in self.steps
        ],
    }

@classmethod
def from_dict(cls, data: dict[str, Any]) -> "Plan":
    """Deserialize Plan from database dict."""
    steps = [
        StepSpec(
            step_id=s["step_id"],
            name=s["name"],
            fn_ref=s["fn_ref"],
            params=s["params"],
            deps=s.get("deps", []),
            scope=s.get("scope", {}),
            inputs=s.get("inputs", {}),
        )
        for s in data.get("steps", [])
    ]
    return cls(
        plan_id=data["plan_id"],
        realm=data["realm"],
        scope=data.get("scope", {}),
        steps=steps,
    )
```

**Edge Cases Handled:**
- Missing optional fields → use defaults (empty lists/dicts)
- Nested step serialization → recursive dict conversion
- Type safety → no type coercion in deserialization (caller validates if needed)

---

### 3.5 Testing Strategy

**Unit Tests (no DB integration):**

1. **ChangesFetcher:**
   - Mock CouchDB API responses
   - Test retry logic (simulate transient failures)
   - Test JSON parsing of `_changes` stream
   - Test continuous streaming with poll interval
   - Test error handling (persistent failures)

2. **WatcherCheckpointStore:**
   - Mock CouchDB upsert with _rev conflict
   - Test conflict retry (caller responsibility)
   - Test missing checkpoint → returns None
   - Test doc ID construction
   - Test _rev preservation on upsert

3. **Plan Serialization:**
   - Roundtrip test: Plan → to_dict → from_dict → equals original
   - Test with nested steps
   - Test missing optional fields → defaults applied
   - Test edge case: empty steps list
   - Test type preservation (dicts, lists, strings)

---

### 3.6 Success Criteria (Phase 1 Done When...)

- [ ] `ChangesFetcher` implemented and tests pass (100% coverage)
- [ ] `WatcherCheckpointStore` implemented and tests pass (100% coverage)
- [ ] `Plan.to_dict()` / `from_dict()` implemented and roundtrip tests pass
- [ ] No integration with live CouchDB needed (all mocked)
- [ ] Components have docstrings and examples
- [ ] Code adheres to project style (ruff, black, mypy)

---

### 3.7 Integration with Phase 2

**What Phase 2 Needs from Phase 1:**
- `ChangesFetcher` class imported in `PlanDBManager`
- `WatcherCheckpointStore` singleton for checkpoint management
- `Plan.to_dict()` / `from_dict()` for plan persistence

**No Breaking Changes Expected:** Phase 1 components are backward-compatible.

---

### 3.8 Known Limitations

- **No compression:** Large step lists (100+) → large JSON; mitigate with pagination if needed
- **No validation:** Deserialization assumes valid input; caller validates if untrusted source
- **Retry policy static:** Hardcoded max_retries=3; consider config-driven in future

---

---

## 4. PHASE 2: Plan Persistence & Eligibility

### 4.1 Overview

Introduces plan database client and eligibility logic:
1. **PlanDBManager**: CRUD operations on `yggdrasil_plans` DB using ChangesFetcher
2. **is_plan_eligible()**: Pure function determining if a plan should execute

**Estimated LOC:** ~400 (220 implementation + 180 tests)

---

### 4.2 Files to Create/Modify

| File | Action | Purpose |
|------|--------|---------|
| `lib/couchdb/plan_db_manager.py` | **CREATE** | DB client for `yggdrasil_plans` |
| `lib/core_utils/plan_eligibility.py` | **CREATE** | Pure function + edge case logic |
| `tests/test_plan_db_manager.py` | **CREATE** | Unit tests (mocked DB) |
| `tests/test_plan_eligibility.py` | **CREATE** | Comprehensive eligibility tests |

---

### 4.3 Key Design Decisions

**PlanDBManager Design:**
- **Decision:** Extend `CouchDBHandler` (matches `ProjectDBManager` pattern)
- **Why:** Consistent with existing codebase; reuses DB connection logic
- **Integration:** Uses `ChangesFetcher` from Phase 1 for `fetch_changes()` method
- **Query Methods:** Simple doc fetch + view-based query for pending plans (future CouchDB design doc)

**is_plan_eligible() Design:**
- **Decision:** Pure function (no side effects, testable in isolation)
- **Why:** Plan eligibility is deterministic logic; pure function = easier to test + reason about
- **Location:** Standalone module `lib/core_utils/plan_eligibility.py` (not in Manager class)
- **Rule:** `status='approved' AND run_token > executed_run_token`
- **Defaults:** Treat missing `executed_run_token` as -1

---

### 4.4 Detailed Specifications

#### 4.4.1 PlanDBManager

**Pseudocode Signature:**

```python
class PlanDBManager(CouchDBHandler):
    """Manages yggdrasil_plans database."""
    
    def __init__(self):
        """Init with 'yggdrasil_plans' database."""
        super().__init__("yggdrasil_plans")
        self.changes_fetcher = ChangesFetcher(self, include_docs=True)
    
    async def fetch_changes(
        self,
        checkpoint_store: WatcherCheckpointStore,
    ) -> AsyncGenerator[dict[str, Any], None]:
        """
        Yield plan doc changes using ChangesFetcher + checkpoint management.
        Internally updates checkpoint after each successful yield.
        """
    
    def save_plan(self, plan_doc: dict[str, Any]) -> str:
        """
        Upsert plan document. Returns doc_id on success.
        Handles _rev for update conflicts.
        """
    
    def fetch_plan(self, plan_id: str) -> dict[str, Any] | None:
        """Fetch single plan by ID."""
    
    def query_approved_pending(self) -> list[dict[str, Any]]:
        """
        Query all plans with status='approved' and not yet executed.
        Used for startup recovery (fallback if checkpoint missing).
        
        Returns: List of plan dicts (status, run_token, executed_run_token, etc.)
        """
```

**Plan Document Schema** (stored in `yggdrasil_plans` DB):

```json
{
  "_id": "pln_tenx_P36805_v1",
  "_rev": "1-abc123...",
  
  "realm": "tenx",
  "scope": {"kind": "project", "id": "P36805"},
  
  "status": "approved",
  "plan": { <Plan.to_dict() output> },
  "preview": { <PlanDraft.preview> },
  
  "run_token": 0,
  "executed_run_token": -1,
  
  "created_at": "2025-01-14T10:00:00Z",
  "updated_at": "2025-01-14T10:00:00Z",
  "last_executed_at": null,
  
  "run_requested_at": null,
  "run_requested_by": null,
  
  "source_doc_id": "projects:P36805",
  "source_doc_rev": "3-xyz789..."
}
```

---

#### 4.4.2 is_plan_eligible()

**Pseudocode Signature:**

```python
def is_plan_eligible(plan_doc: dict[str, Any]) -> bool:
    """
    Determine if a plan is eligible for execution.
    
    Rule: status == 'approved' AND run_token > executed_run_token
    
    Args:
        plan_doc: Plan document dict from yggdrasil_plans DB
    
    Returns:
        True if eligible, False otherwise
    
    Raises:
        None (defensive: missing fields treated as defaults)
    """
```

**Implementation Logic:**

```python
def is_plan_eligible(plan_doc: dict[str, Any]) -> bool:
    status = plan_doc.get("status", "draft")
    if status != "approved":
        return False
    
    run_token = plan_doc.get("run_token", 0)
    executed_run_token = plan_doc.get("executed_run_token", -1)
    
    return run_token > executed_run_token
```

**Example Scenarios:**

| Status | run_token | executed_run_token | Eligible? | Reason |
|--------|-----------|-------------------|-----------|--------|
| "approved" | 0 | -1 | ✓ Yes | Initial run (default case) |
| "approved" | 1 | 0 | ✓ Yes | Manual re-run (token incremented) |
| "approved" | 2 | 2 | ✗ No | Already executed (tokens equal) |
| "draft" | 0 | -1 | ✗ No | Awaiting approval |
| "rejected" | 0 | -1 | ✗ No | Rejected (out of scope but defensible) |
| "approved" | missing | -1 | ✓ Yes | Default run_token=0 |

---

### 4.5 Testing Strategy

**Unit Tests (no real DB):**

1. **PlanDBManager:**
   - Mock `CouchDBHandler` put/get operations
   - Test save_plan with _rev conflict handling
   - Test fetch_plan (found/not found)
   - Test fetch_changes integration with ChangesFetcher
   - Test checkpoint updates during fetch

2. **is_plan_eligible():**
   - Test all scenarios from table above (happy path + edge cases)
   - Test missing fields → defaults applied
   - Test with None values
   - Test token comparison logic (0 > -1, 1 > 0, etc.)
   - Test status case sensitivity (status="Approved" → not eligible)

**Edge Cases to Verify:**
- `run_token` = 0, `executed_run_token` = -1 (initial run)
- `run_token` = 0, `executed_run_token` = 0 (already ran once)
- `run_token` negative (invalid but defensible)
- `executed_run_token` > `run_token` (should never happen, but test for it)

---

### 4.6 Success Criteria (Phase 2 Done When...)

- [ ] `PlanDBManager` implemented with save, fetch, query_approved_pending methods
- [ ] `is_plan_eligible()` implemented and passes all edge case tests (100% coverage)
- [ ] fetch_changes integrates ChangesFetcher correctly
- [ ] Mocked DB tests pass without integration
- [ ] Eligibility logic is deterministic and pure (no I/O, no state)
- [ ] Code adheres to project style

---

### 4.7 Integration with Phase 3

**What Phase 3 Needs from Phase 2:**
- `PlanDBManager` class and methods
- `is_plan_eligible()` pure function
- Plan document schema (for watcher to consume)

**No Breaking Changes Expected.**

---

### 4.8 Known Limitations

- **No view queries yet:** `query_approved_pending()` uses document iteration (O(n)); CouchDB design doc for indexed query is future work
- **No schema validation:** Assumes incoming docs are valid; validate if needed for Genstat integration
- **Single token namespace:** run_token is global (not per-realm or per-step); if needed, add namespace prefix in future

---

---

## 5. PHASE 3: PlanWatcher & Execution

### 5.1 Overview

Brings plan approval workflow to life:
1. **PlanWatcher**: CouchDB watcher monitoring `yggdrasil_plans` for approved plans
2. **execute_approved_plan()**: Run plan via Engine, update executed_run_token
3. **Startup recovery**: On boot, resume from checkpoint; fallback to query pending plans

**Estimated LOC:** ~600 (300 implementation + 300 tests)

---

### 5.2 Files to Create/Modify

| File | Action | Purpose |
|------|--------|---------|
| `lib/watchers/plan_watcher.py` | **CREATE** | PlanWatcher extending AbstractWatcher |
| `lib/core_utils/yggdrasil_core.py` | **MODIFY** | Add execute_approved_plan, recovery logic, wiring |
| `tests/test_plan_watcher.py` | **CREATE** | Integration tests (mocked DB + checkpoint) |
| `tests/test_approval_workflow_integration.py` | **CREATE** | End-to-end scenario tests |

---

### 5.3 Key Design Decisions

**PlanWatcher Design:**
- **Decision:** Extend `AbstractWatcher` (matches existing watcher pattern)
- **Why:** Consistent with SeqDataWatcher, CouchDBWatcher; integrates with YggdrasilCore lifecycle
- **Checkpoint Management:** Uses `WatcherCheckpointStore` for _changes feed resume
- **Event Emission:** Emits custom event for execution eligibility (or calls Core directly)

**execute_approved_plan() Design:**
- **Decision:** Synchronous execution (not async) to avoid re-entrancy issues
- **Why:** Engine.run() is blocking; async wrapper would complicate state management
- **Token Update:** Update `executed_run_token` *after* successful execution (atomicity via _rev)
- **Error Handling:** On failure, token NOT updated → plan remains eligible for manual re-run
- **Logging:** Comprehensive logging at each step (plan fetch, eligibility check, execution, token update)

**Startup Recovery Design:**
- **Decision:** Checkpoint-first, query-fallback approach
- **Why:** Normal case (checkpoint present) avoids full DB scan; fallback handles missing checkpoint gracefully
- **Recovery Logic:**
  1. Try resume from checkpoint (normal case)
  2. If checkpoint missing, query_approved_pending() and execute eligible plans
  3. Update checkpoint after recovery complete

---

### 5.4 Detailed Specifications

#### 5.4.1 PlanWatcher

**Pseudocode Signature:**

```python
class PlanWatcher(AbstractWatcher):
    """
    Watches yggdrasil_plans _changes feed for approved plans.
    Filters by eligibility rule and emits execution events.
    """
    
    def __init__(
        self,
        on_event: Callable[[YggdrasilEvent], None],
        plan_db_manager: PlanDBManager,
        checkpoint_store: WatcherCheckpointStore,
        logger: logging.Logger | None = None,
    ):
        """Init watcher with DB manager and checkpoint store."""
        super().__init__(
            on_event=on_event,
            event_type=EventType.PLAN_APPROVED,  # Custom event type
            name="PlanWatcher",
            logger=logger,
        )
        self.plan_db = plan_db_manager
        self.checkpoint = checkpoint_store
    
    async def start(self):
        """
        Poll plan DB _changes feed.
        For each change: check eligibility, emit execution event if eligible.
        Update checkpoint after each processed change.
        """
    
    async def stop(self):
        """Graceful shutdown; save final checkpoint."""
```

**Key Behaviors:**
- Continuous polling of `_changes` feed (respects poll interval)
- Filters: only process docs with `type='yggdrasil_plan'` and `status='approved'`
- Check eligibility: `is_plan_eligible(plan_doc)`
- Emit event for each eligible plan (payload = plan_doc)
- Update checkpoint after emit (even if execution fails—failures handled downstream)
- Graceful error handling (log, continue polling)

---

#### 5.4.2 execute_approved_plan()

**Pseudocode Signature:**

```python
def execute_approved_plan(self, plan_doc_id: str) -> None:
    """
    Execute an approved plan and update execution state.
    
    Flow:
    1. Fetch plan document
    2. Check eligibility (defensive)
    3. Deserialize Plan from plan_doc["plan"]
    4. Execute via Engine (blocking)
    5. Update executed_run_token (atomic with _rev)
    
    Args:
        plan_doc_id: Plan document ID in yggdrasil_plans DB
    
    Raises:
        Logs errors; does NOT re-raise (failure → plan eligible for manual re-run)
    """
```

**Implementation Pseudocode:**

```python
def execute_approved_plan(self, plan_doc_id: str) -> None:
    try:
        # 1. Fetch plan doc
        plan_doc = self.pdm.fetch_plan(plan_doc_id)
        if not plan_doc:
            self._logger.error("Plan '%s' not found", plan_doc_id)
            return
        
        # 2. Check eligibility (defensive)
        if not is_plan_eligible(plan_doc):
            self._logger.debug("Plan '%s' not eligible; skipping", plan_doc_id)
            return
        
        # 3. Deserialize Plan
        plan = Plan.from_dict(plan_doc["plan"])
        
        # 4. Execute
        self._logger.info("Executing plan '%s'", plan.plan_id)
        self.engine.run(plan)  # Blocking call
        self._logger.info("✓ Plan '%s' execution completed", plan.plan_id)
        
        # 5. Update executed_run_token
        self._update_executed_run_token(plan_doc_id, plan_doc)
        
    except Exception as exc:
        self._logger.exception("Failed to execute plan '%s': %s", plan_doc_id, exc)
        # Do NOT re-raise; plan remains eligible for manual re-run
```

**Token Update Logic:**

```python
def _update_executed_run_token(self, plan_doc_id: str, plan_doc: dict) -> None:
    """Update executed_run_token to current run_token (race-safe via _rev)."""
    try:
        run_token = plan_doc.get("run_token", 0)
        plan_doc["executed_run_token"] = run_token
        plan_doc["last_executed_at"] = utcnow_iso()
        
        # Preserve _rev for conflict-safe upsert
        self.pdm.save_plan(plan_doc)
        self._logger.info(
            "Updated plan '%s': executed_run_token=%d",
            plan_doc_id,
            run_token,
        )
    except Exception as exc:
        self._logger.error(
            "Failed to update executed_run_token for plan '%s': %s",
            plan_doc_id,
            exc,
        )
        # If update fails, plan remains eligible (will retry on next watcher poll)
```

---

#### 5.4.3 Startup Recovery

**Pseudocode:**

```python
def _recover_approved_plans(self) -> None:
    """
    On startup, recover any approved plans that weren't executed before shutdown.
    
    Strategy:
    1. Try resume from checkpoint (normal case)
    2. If checkpoint missing, fallback to query
    """
    self._logger.info("Checking for approved plans pending execution...")
    
    # Check checkpoint
    checkpoint_store = WatcherCheckpointStore("PlanWatcher", self.ydm)
    last_seq = checkpoint_store.get_checkpoint()
    
    if last_seq:
        # Normal case: resume from checkpoint (watcher will process since 'last_seq')
        self._logger.info("Resuming from checkpoint seq='%s'", last_seq)
        return
    
    # Fallback: query all approved pending plans
    self._logger.warning("No checkpoint found; querying all pending plans...")
    pending = self.pdm.query_approved_pending()
    
    for plan_doc in pending:
        if is_plan_eligible(plan_doc):
            self._logger.info("Recovering plan '%s'", plan_doc["_id"])
            try:
                self.execute_approved_plan(plan_doc["_id"])
            except Exception as exc:
                self._logger.exception("Recovery failed for plan '%s': %s", 
                                       plan_doc["_id"], exc)
    
    # Initialize checkpoint (so next startup resumes normally)
    checkpoint_store.save_checkpoint("0")
    self._logger.info("Recovery complete.")
```

---

### 5.5 Testing Strategy

**Integration Tests (mocked DB + checkpoint):**

1. **PlanWatcher:**
   - Mock `_changes` feed with eligible/ineligible plans
   - Verify watcher processes eligible plans and emits events
   - Verify checkpoint is updated after each change
   - Verify watcher stops gracefully
   - Verify retry logic on transient fetch errors

2. **execute_approved_plan():**
   - Mock plan fetch, eligibility check, engine execution
   - Verify token is updated on success
   - Verify token NOT updated on execution failure
   - Verify error logging (doesn't re-raise)
   - Mock missing plan document → graceful error

3. **Startup Recovery:**
   - Mock checkpoint present → resume from seq
   - Mock checkpoint missing → query fallback
   - Verify eligible plans executed during recovery
   - Verify checkpoint saved after recovery

4. **End-to-End Scenario:**
   - Full approval flow: generate plan → persist with status='approved' → watcher detects → executes → token updated
   - Manual re-run: increment run_token → watcher detects as eligible again → executes
   - Rejected plan: status='rejected' → watcher filters out → no execution

---

### 5.6 Success Criteria (Phase 3 Done When...)

- [ ] `PlanWatcher` implemented and tests pass
- [ ] `execute_approved_plan()` implemented and token updates correctly
- [ ] Startup recovery works (checkpoint + fallback query)
- [ ] YggdrasilCore wired: PlanWatcher registered, execute_approved_plan callable
- [ ] Error handling verified (plan eligible after failure)
- [ ] Mocked integration tests pass (no live DB needed)
- [ ] Code adheres to project style

---

### 5.7 Integration with Phase 4

**What Phase 4 Needs from Phase 3:**
- Full PlanWatcher + execution flow implemented and tested
- Startup recovery functional
- Error paths well-defined

**Breaking Changes Expected:** None (backward compatible).

---

### 5.8 Known Limitations

- **Sequential execution:** Only one plan executes at a time (watcher is single-threaded); parallelization is future work
- **No async engine:** `engine.run()` is blocking; async execution is future enhancement
- **No approval UI:** This phase provides backend only; Genstat UI integration is Phase 4 (docs only)

---

---

## 6. PHASE 4: Tests & Documentation

### 6.1 Overview

Comprehensive end-to-end testing and Genstat contract documentation:
1. **E2E Scenario Tests**: Full approval workflows with realistic data
2. **Genstat Contract Doc**: How Genstat approves/re-runs plans

**Estimated LOC:** ~500 (100 E2E test code + 400 docs + supplementary tests)

---

### 6.2 Files to Create/Modify

| File | Action | Purpose |
|------|--------|---------|
| `tests/test_approval_workflow_e2e.py` | **CREATE** | End-to-end scenario tests |
| `docs/GENSTAT_PLAN_CONTRACT.md` | **CREATE** | Genstat integration guide |
| `tests/test_plan_eligibility_edge_cases.py` | **CREATE** | Exhaustive edge case coverage |
| README or flow docs | **MODIFY** | Add approval workflow overview |

---

### 6.3 Key Design Decisions

**Documentation Approach:**
- **Decision:** Genstat contract as markdown (no code validation)
- **Why:** PRD specifies "markdown only"; Genstat is external system
- **Content:** Field-level requirements, approval/rerun operations, examples
- **Audience:** Genstat developers integrating with Yggdrasil

**E2E Testing Approach:**
- **Decision:** Mocked DB + full component integration (no live CouchDB)
- **Why:** Verifies component interactions without infrastructure dependency
- **Scenarios:** Auto-run, approval+execution, manual re-run, recovery

---

### 6.4 Detailed Specifications

#### 6.4.1 Genstat Plan Contract

**Document Structure:**

```markdown
# Genstat Plan Contract

## Overview
Genstat is an external approval UI. It:
1. Fetches plan docs from yggdrasil_plans DB (status='draft')
2. Allows human approval/rejection
3. Updates plan docs (approval + optional re-run)

## Plan Document Format
[Schema description from Phase 2]

## Approval Operation
**Endpoint:** CouchDB PUT to yggdrasil_plans/{plan_id}

**Payload (approval):**
```json
{
  "status": "approved",
  "approved_at": "ISO-8601 timestamp",
  "approved_by": "user@domain"
}
```

**Important:** Include `_rev` from current document to avoid conflicts.

## Re-Run Operation
**Payload (re-run, increments token):**
```json
{
  "run_token": 1,
  "run_requested_at": "ISO-8601 timestamp",
  "run_requested_by": "user@domain"
}
```

## Status Values
- "draft" → pending approval (default for auto_run=false)
- "approved" → ready for execution (default for auto_run=true, or set by Genstat)
- (rejected, expired = future)

## Fields Genstat May Update
- status (draft → approved)
- run_token (manual re-run)
- approved_at, approved_by
- run_requested_at, run_requested_by
- run_attempts (optional, informational)

## Fields Genstat Must NOT Modify
- plan (immutable once persisted)
- created_at
- realm, scope

## Error Handling
- Conflict (409): `_rev` mismatch → re-fetch and retry
- Not Found (404): Plan deleted → display error to user
- Validation errors: Include error details in response
```

---

#### 6.4.2 E2E Test Scenarios

**Scenario 1: Auto-Run Plan**
```
1. Handler generates PlanDraft(auto_run=True)
2. Core persists with status='approved'
3. PlanWatcher detects plan in next poll
4. Watcher calls execute_approved_plan()
5. Engine runs plan
6. Watcher updates executed_run_token
7. Plan marked as executed
Expected: Plan executed immediately, no human interaction
```

**Scenario 2: Approval Required**
```
1. Handler generates PlanDraft(auto_run=False)
2. Core persists with status='draft'
3. PlanWatcher detects plan; filters (status != 'approved')
4. Plan remains in draft
5. Genstat fetches plan (human sees it)
6. Genstat approves: updates status='approved'
7. PlanWatcher detects status change
8. Watcher executes plan
9. Token updated
Expected: Plan waits for approval, then executes
```

**Scenario 3: Manual Re-Run**
```
1. Plan executed (executed_run_token=0, run_token=0)
2. Genstat re-run action: increments run_token=1
3. PlanWatcher detects run_token > executed_run_token
4. Watcher executes plan again
5. Token updated: executed_run_token=1
Expected: Plan executes again without regeneration
```

**Scenario 4: Startup Recovery**
```
1. Yggdrasil running; plan approved and executed
2. Yggdrasil crashes before checkpoint saved
3. On restart: checkpoint missing
4. Recovery logic queries all approved pending
5. Plan found (executed_run_token=0, run_token=0)
6. Plan ineligible (tokens equal) → not re-executed
7. Different plan found (run_token=1, executed_run_token=0)
8. Plan eligible → executed
9. Token updated, checkpoint initialized
Expected: Only eligible plans re-executed; no duplicates
```

---

### 6.5 Testing Strategy

**E2E Tests (mocked components):**
- Full flow from event → plan generation → persistence → execution → token update
- Verify all components work together
- Use fixtures for common setup (mock DB, mock engine, mock logger)
- Test failure paths (e.g., engine raises exception → plan remains eligible)

**Edge Case Tests:**
- Duplicate plan IDs (overwrite)
- Concurrent approvals (watcher resilient to _rev conflicts)
- Plans with no steps (valid edge case)
- Missing fields in fetched docs (defensive defaults)

---

### 6.6 Success Criteria (Phase 4 Done When...)

- [ ] E2E tests cover all 4 approval scenarios
- [ ] Genstat contract doc is clear and actionable
- [ ] Edge case tests pass (100% coverage of Phase 2-3 logic)
- [ ] All tests pass (unit + integration + E2E)
- [ ] Documentation is readable by external teams
- [ ] Code coverage >90% across all phases

---

### 6.7 Known Limitations

- **No Genstat code:** This phase documents contract only; Genstat implementation is external
- **No performance tests:** E2E tests are functional; load testing is future work
- **No production deployment guide:** Ops playbook (backups, monitoring) is future work

---

---

## 7. Risk Mitigation & Quality Gates

### 7.1 Risks by Phase

| Phase | Risk | Mitigation |
|-------|------|-----------|
| 1 | ChangesFetcher retry logic insufficient | Add backoff + max attempts; test failure modes |
| 1 | Checkpoint store conflicts on high concurrency | Use _rev; add conflict retry in Phase 3 |
| 2 | Eligibility logic gap (off-by-one in tokens) | Exhaustive unit tests with table-driven scenarios |
| 3 | Race condition: plan modified during execution | Token update uses _rev; conflicts logged, not re-raised |
| 3 | Startup recovery infinite loop | Query fallback only once; checkpoint saved after recovery |
| 4 | E2E tests don't catch real issues | Use realistic fixture data from actual realm planners |

### 7.2 Quality Gates (Must Pass Before Next Phase)

| Phase | Gate |
|-------|------|
| 1→2 | All Phase 1 tests pass; code review approved |
| 2→3 | All Phase 1-2 tests pass; eligibility logic 100% coverage |
| 3→4 | All Phase 1-3 tests pass; integration tests verify component interaction |
| 4→Release | E2E tests pass; Genstat contract doc reviewed by external team |

---

## 8. Overall Acceptance Criteria

**Approval Workflow Complete When:**

- ✓ All 4 phases implemented and tested
- ✓ ChangesFetcher reusable by future watchers
- ✓ WatcherCheckpointStore DB-backed and conflict-safe
- ✓ Plan serialization roundtrips correctly
- ✓ PlanDBManager CRUD operations functional
- ✓ is_plan_eligible() deterministic + 100% coverage
- ✓ PlanWatcher detects and executes eligible plans
- ✓ Startup recovery tested (checkpoint + fallback)
- ✓ Genstat contract documented
- ✓ E2E workflows tested (auto-run, approval, re-run, recovery)
- ✓ Code adheres to project style (ruff, black, mypy)
- ✓ Test coverage >90%
- ✓ No breaking changes to existing watchers or handlers

---

## 9. Next Steps

1. **Stakeholder Review:** Confirm this plan aligns with system goals
2. **Proceed to Phase 1:** Implement ChangesFetcher + WatcherCheckpointStore + Plan serialization
3. **Gate Review:** Phase 1 complete → Phase 2 begins
4. **Iterate:** Repeat gate review for Phases 2, 3, 4

---

## Appendix: Terminology

| Term | Definition |
|------|-----------|
| **Plan Document** | JSON doc in `yggdrasil_plans` DB; stores intent + approval state |
| **run_token** | Incrementing counter; manual re-runs increment this |
| **executed_run_token** | Last token that was executed; prevents re-execution |
| **Eligibility** | status='approved' AND run_token > executed_run_token |
| **Checkpoint** | Last seq processed by watcher; stored in yggdrasil DB |
| **Genstat** | External approval UI (out of scope for implementation) |
| **Recovery** | Startup logic to execute pending approved plans |

---

**Document Version:** 1.0  
**Last Updated:** January 15, 2026  
**Status:** Ready for Implementation Review
