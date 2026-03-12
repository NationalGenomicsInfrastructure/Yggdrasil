# Genstat Plan Contract

## Document Version

| Version |    Date    |     Author     |      Notes      |
|---------|------------|----------------|-----------------|
|   1.1   | 2026-01-28 | Yggdrasil Team | Token values fix |
|   1.0   | 2026-01-16 | Yggdrasil Team | Initial release |

---

## Overview

Genstat is an external approval UI that integrates with Yggdrasil's Plan Approval & Execution Workflow. This document defines the contract between Yggdrasil and Genstat for:

1. **Reading Plans**: Fetching plan documents for display
2. **Approving Plans**: Updating plan status to trigger execution
3. **Re-Running Plans**: Requesting re-execution of previously run plans

> **Note:** Plan rejection is intentionally **not supported**. If a plan should not execute, leave it in `draft` status. Fixing issues in source documents will trigger plan regeneration, which supersedes the old plan.

---

## Architecture Overview

```
┌─────────────┐        ┌──────────────────────┐         ┌─────────────┐
│   Genstat   │◄──────►│  yggdrasil_plans DB  │◄──────► │  Yggdrasil  │
│  (Approval  │ CouchDB│  (Plan Documents)    │ _changes│ PlanWatcher │
│     UI)     │   API  │                      │  feed   │   + Engine  │
└─────────────┘        └──────────────────────┘         └─────────────┘
```

**Flow:**
1. Yggdrasil handlers generate `PlanDraft` objects
2. `YggdrasilCore` persists plans to `yggdrasil_plans` database
3. Genstat queries plans with `status='draft'` (or all pending plans)
4. Human reviews and approves (no reject option) via Genstat UI
5. Genstat updates plan document (status, timestamps)
6. Yggdrasil `PlanWatcher` detects change via `_changes` feed
7. If eligible, `PlanWatcher` triggers execution via `Engine`

---

## Database: `yggdrasil_plans`

All plan documents are stored in the `yggdrasil_plans` CouchDB database.

### Connection Details

| Property | Description |
|----------|-------------|
| Database Name | `yggdrasil_plans` |
| Protocol | HTTP/HTTPS |
| Authentication | Basic Auth or IAM (IBM Cloudant) |

**Environment Variables (Yggdrasil side):**
```bash
COUCHDB_URL=http://localhost:5984
COUCHDB_USER=<user>
COUCHDB_PASSWORD=<password>
```

---

## Plan Document Schema

### Required Fields

| Field | Type | Description | Mutability |
|-------|------|-------------|------------|
| `_id` | string | Unique plan identifier (e.g., `pln_tenx_P12345_v1`) | Immutable |
| `_rev` | string | CouchDB revision (for conflict detection) | System-managed |
| `realm` | string | Handler/realm identifier (e.g., `tenx`, `ss3`) | Immutable |
| `scope` | object | Scope identifier `{kind: string, id: string}` | Immutable |
| `plan` | object | The execution plan (steps, params) | Immutable |
| `status` | string | Current plan status (see Status Values) | **Mutable by Genstat** |
| `auto_run` | boolean | If `true`, plan executes immediately | Read-only |
| `execution_authority` | string | Execution ownership: `"daemon"` or `"run_once"` | **Mutable by Genstat** (see Adoption) |
| `execution_owner` | string or null | Unique CLI session token (null for daemon plans) | **Mutable by Genstat** (see Adoption) |
| `run_token` | integer | Monotonic counter for re-run detection | **Mutable by Genstat** |
| `executed_run_token` | integer | Last executed run_token | Yggdrasil-only |
| `created_at` | string | ISO-8601 timestamp of plan creation | Immutable |

### Optional Fields

| Field | Type | Description | Mutability |
|-------|------|-------------|------------|
| `updated_at` | string | ISO-8601 timestamp of last update | System-managed |
| `approvals_required` | array | List of required approvers (informational) | Read-only |
| `approved_at` | string | ISO-8601 timestamp of approval | **Mutable by Genstat** |
| `approved_by` | string | Email/ID of approver | **Mutable by Genstat** |
| `run_requested_at` | string | ISO-8601 timestamp of re-run request | **Mutable by Genstat** |
| `run_requested_by` | string | Email/ID of re-run requester | **Mutable by Genstat** |
| `notes` | string | Human-readable notes from handler | Read-only |
| `preview` | boolean | If `true`, plan includes preview data | Read-only |

### Example Document

```json
{
  "_id": "pln_tenx_P12345_v1",
  "_rev": "3-abc123def456",
  "realm": "tenx",
  "scope": {
    "kind": "project",
    "id": "P12345"
  },
  "plan": {
    "plan_id": "pln_tenx_P12345_v1",
    "realm": "tenx",
    "scope": {"kind": "project", "id": "P12345"},
    "steps": [
      {
        "step_id": "cellranger_001",
        "name": "cellranger_count",
        "fn_ref": "tenx.steps:run_cellranger",
        "params": {"sample_id": "S001", "chemistry": "auto"},
        "inputs": [],
        "outputs": []
      },
      {
        "step_id": "qc_001",
        "name": "quality_check",
        "fn_ref": "tenx.steps:run_qc",
        "params": {},
        "inputs": [],
        "outputs": []
      }
    ]
  },
  "status": "draft",
  "auto_run": false,
  "execution_authority": "daemon",
  "execution_owner": null,
  "run_token": 0,
  "executed_run_token": -1,
  "created_at": "2026-01-16T10:30:00Z",
  "updated_at": "2026-01-16T10:30:00Z",
  "approvals_required": ["team_lead"],
  "notes": "Standard 10X GEX processing for P12345"
}
```

---

## Status Values

| Status | Description | Transitions |
|--------|-------------|-------------|
| `draft` | Pending approval; not eligible for execution | → `approved` |
| `approved` | Ready for execution (or already executed) | → (terminal) |
| `pending` | Legacy; treat as `draft` | → `approved` |

**Notes:**
- Plans with `auto_run=true` are created with `status='approved'` directly
- Plans with `auto_run=false` are created with `status='draft'` (or `pending`)
- **Rejection is not supported**: If a plan should not execute, leave it as `draft`. Fixing the underlying data will trigger plan regeneration.
## Execution Authority & Ownership

Plans track their execution context to enable isolation between CLI (`yggdrasil run-doc`) and daemon-driven workflows.

### Fields

**`execution_authority`** (string, required)
- `"daemon"` (default): Plan created/managed by the Yggdrasil daemon service
  - Daemon's PlanWatcher monitors and executes these plans
  - Standard approval → execution lifecycle
- `"run_once"`: Plan created by `yggdrasil run-doc --run-once` CLI invocation
  - A specific CLI session owns and will execute this plan
  - Daemon's PlanWatcher **ignores** these plans
  - The owning CLI session monitors via scoped watcher

**`execution_owner`** (string or null, required)
- For `daemon` authority: Always `null`
- For `run_once` authority: Unique token like `"run_once:550e8400-e29b-41d4-a716-446655440000"`
  - Generated per CLI invocation to isolate concurrent runs
  - CLI's scoped watcher filters by this exact owner

### Plan Adoption (run_once → daemon)

Genstat can "adopt" a `run_once` plan, transferring execution responsibility to the daemon. This is useful when:
- CLI user disconnects (Ctrl+C) but wants daemon to finish execution (prevents stray plans)
- Plan needs to be managed centrally after initial creation

**Adoption Operation:**

```http
PUT /yggdrasil_plans/{plan_id} HTTP/1.1
Content-Type: application/json

{
  "_id": "pln_tenx_P12345_v1",
  "_rev": "5-abc123",
  ... (all existing fields) ...
  "execution_authority": "daemon",
  "execution_owner": null,
  "updated_at": "2026-01-16T15:00:00Z"
}
```

**After adoption:**
- Plan becomes visible to daemon's PlanWatcher
- Original CLI session (if connected) detects ownership change and exits gracefully
- Standard daemon approval/execution workflow applies
- Plan's `status` remains unchanged (e.g., still `draft` if not approved)

### UI Recommendations

**Plan List View:**
- Show visual indicator for `run_once` plans (e.g., yellow "CLI" badge)
- Consider adding filter for `execution_authority`

**Plan Detail View for `run_once` plans:**
- Display warning: "This plan is owned by a CLI session. Approving will trigger execution in that session (if still connected)."
- Show "Transfer to Daemon" button prominently
- Display `execution_owner` token for debugging

---


---

## Eligibility Rule

A plan is **eligible for execution** when:

```python
is_eligible = (
    status == "approved" 
    and run_token > executed_run_token
)
```

**Initial values (new plan):**
- `run_token: 0`
- `executed_run_token: -1`
- Result: `0 > -1` is `True` → eligible on first approval

**After execution:**
- `run_token: 0`
- `executed_run_token: 0` (updated by Yggdrasil)
- Result: `0 > 0` is `False` → not eligible (prevents re-execution)

**Implications:**
- `draft` plans are never executed (need approval first)
- Only `approved` plans can be executed
- Plans where `run_token == executed_run_token` have already been executed

---

## Operations

### 1. Querying Plans

**Fetch all pending plans (for approval list):**

```http
GET /yggdrasil_plans/_find HTTP/1.1
Content-Type: application/json

{
  "selector": {
    "status": {"$in": ["draft", "pending"]}
  },
  "fields": ["_id", "_rev", "realm", "scope", "status", "created_at", "notes"],
  "limit": 100
}
```

**Fetch single plan (for detail view):**

```http
GET /yggdrasil_plans/{plan_id} HTTP/1.1
```

**Fetch plans by scope:**

```http
GET /yggdrasil_plans/_find HTTP/1.1
Content-Type: application/json

{
  "selector": {
    "scope.kind": "project",
    "scope.id": "P12345"
  }
}
```

---

### 2. Approval Operation

**Purpose:** Approve a draft plan for execution.

**Endpoint:** `PUT /yggdrasil_plans/{plan_id}`

**Request:**

```http
PUT /yggdrasil_plans/pln_tenx_P12345_v1 HTTP/1.1
Content-Type: application/json

{
  "_id": "pln_tenx_P12345_v1",
  "_rev": "3-abc123def456",
  "realm": "tenx",
  "scope": {"kind": "project", "id": "P12345"},
  "plan": { ... },
  "status": "approved",
  "auto_run": false,
  "run_token": 1,
  "executed_run_token": 0,
  "created_at": "2026-01-16T10:30:00Z",
  "updated_at": "2026-01-16T11:00:00Z",
  "approved_at": "2026-01-16T11:00:00Z",
  "approved_by": "user@example.com",
  "notes": "Standard 10X GEX processing for P12345"
}
```

**Critical Requirements:**

1. **Include `_rev`:** The current document revision must be included. CouchDB will reject the update with HTTP 409 if the revision doesn't match.

2. **Preserve immutable fields:** The following fields must NOT be modified:
   - `_id`
   - `realm`
   - `scope`
   - `plan`
   - `created_at`
   - `auto_run`

3. **Set approval metadata:**
   - `status`: Change to `"approved"`
   - `approved_at`: Current ISO-8601 timestamp
   - `approved_by`: Approver's email or user ID
   - `updated_at`: Current ISO-8601 timestamp

**Response (Success):**

```http
HTTP/1.1 201 Created
Content-Type: application/json

{
  "ok": true,
  "id": "pln_tenx_P12345_v1",
  "rev": "4-def456ghi789"
}
```

**Response (Conflict):**

```http
HTTP/1.1 409 Conflict
Content-Type: application/json

{
  "error": "conflict",
  "reason": "Document update conflict."
}
```

**Handling Conflicts:**
1. Re-fetch the document to get current `_rev`
2. Verify the plan hasn't been modified (compare `plan` hash if needed)
3. Retry the approval with updated `_rev`

---

### 3. Re-Run Operation

**Purpose:** Request re-execution of a previously executed plan.

**When to Use:**
- Plan has been executed (`run_token == executed_run_token`)
- User wants to re-run without regenerating the plan
- Typically after fixing external issues (data, resources)

**Endpoint:** `PUT /yggdrasil_plans/{plan_id}`

**Request:**

```http
PUT /yggdrasil_plans/pln_tenx_P12345_v1 HTTP/1.1
Content-Type: application/json

{
  "_id": "pln_tenx_P12345_v1",
  "_rev": "5-ghi789jkl012",
  ... (all existing fields) ...
  "status": "approved",
  "run_token": 1,
  "executed_run_token": 0,
  "run_requested_at": "2026-01-16T14:00:00Z",
  "run_requested_by": "user@example.com",
  "updated_at": "2026-01-16T14:00:00Z"
}
```

**Critical Requirements:**

1. **Increment `run_token`:** Set to `executed_run_token + 1`
   - Example: If `executed_run_token: 0`, set `run_token: 1`
2. **Keep `executed_run_token` unchanged:** Yggdrasil updates this after execution
3. **Ensure `status='approved'`:** Plan must be approved to execute

**Flow After Re-Run Request:**
1. Genstat increments `run_token`
2. PlanWatcher detects `run_token > executed_run_token`
3. Plan becomes eligible again
4. Engine executes the plan
5. Yggdrasil updates `executed_run_token` to match `run_token`

---

## Error Handling

### HTTP 409 Conflict

**Cause:** The `_rev` in your request doesn't match the current document revision.

**Resolution:**
```python
# Pseudocode
try:
    response = update_plan(plan_id, updated_doc)
except ConflictError:
    # Re-fetch current document
    current_doc = fetch_plan(plan_id)
    # Merge your changes
    current_doc['status'] = 'approved'
    current_doc['approved_at'] = now()
    current_doc['approved_by'] = current_user
    # Retry with new _rev
    response = update_plan(plan_id, current_doc)
```

### HTTP 404 Not Found

**Cause:** The plan document doesn't exist (deleted or wrong ID).

**Resolution:** Display error to user; the plan may have been deleted (rare).

### HTTP 400 Bad Request

**Cause:** Invalid JSON or missing required fields.

**Resolution:** Validate document structure before sending.

---

## Field Reference

### Fields Genstat MAY Update

| Field | Update Condition | Notes |
|-------|------------------|-------|
| `status` | On approval | `draft` → `approved` |
| `run_token` | On re-run request | Increment by 1 |
| `execution_authority` | On adoption | `run_once` → `daemon` |
| `execution_owner` | On adoption | Set to `null` when adopting |
| `approved_at` | On approval | ISO-8601 timestamp |
| `approved_by` | On approval | User identifier |
| `run_requested_at` | On re-run | ISO-8601 timestamp |
| `run_requested_by` | On re-run | User identifier |
| `updated_at` | On any update | ISO-8601 timestamp |

### Fields Genstat MUST NOT Modify

| Field | Reason |
|-------|--------|
| `_id` | Document identity |
| `realm` | Plan ownership |
| `scope` | Plan target |
| `plan` | Execution definition (immutable) |
| `created_at` | Audit trail |
| `auto_run` | Handler decision |
| `executed_run_token` | Yggdrasil-managed execution state |

---

## Best Practices

### 1. Always Include Full Document

CouchDB requires the full document for PUT operations. Always:
1. Fetch the current document
2. Modify only allowed fields
3. Send the complete document with updated `_rev`

### 2. Use Optimistic Locking

The `_rev` field provides optimistic locking:
- Always fetch before update
- Include `_rev` in updates
- Handle 409 conflicts gracefully

### 3. Validate Before Display

Plans may have:
- Empty `steps` array (valid edge case)
- Missing optional fields
- Unusual `realm` values (external handlers)

### 4. Timestamp Format

Always use ISO-8601 format with timezone:
```
2026-01-16T14:30:00Z         # UTC
2026-01-16T14:30:00+01:00    # With offset
```

### 5. User Identification

For `approved_by` and `run_requested_by`:
- Use email address (preferred): `user@example.com`
- Or user ID: `uid_12345`
- Be consistent across all operations

---

## Example Workflows

### Workflow 1: Standard Approval

```
1. User opens Genstat approval queue (or each plan linked to the related project)
2. Genstat: GET /yggdrasil_plans/_find (status=draft) (if design require assosiate plan to project)
3. User selects project/plan, e.g. for P12345
4. Genstat: GET /yggdrasil_plans/pln_tenx_P12345_v1
5. User reviews plan steps
6. User clicks "Approve", if approves
7. Genstat: PUT /yggdrasil_plans/pln_tenx_P12345_v1
   - status: "approved"
   - approved_at: <now>
   - approved_by: <user>
8. Yggdrasil PlanWatcher detects change
9. Plan executes automatically
```


### Workflow 2: Plan re-creation (plan not okay due to internal issue)

```
1 - 5. Same as Workflow 1
6. User identifies issue with plan, due to wrong upstream information
7. User corrects upstream information
8. The corresponding Yggdrasil watcher detects upstream information change
9. Plan gets regenerated
10. If plan approval is required, see Workflow 1.
```

### Workflow 3: User mistakenly approves "corrupted" plan

```
1 - 5. Same as Workflow 1
6. User clicks "Approve", without realizing plan issue
7 - 8. Same as Workflow 1
9. Plan executes, but fails due to internal (upstream) data incoherence
10. Detailed errors emitted by the engine (found in `yggdrasil_ops` DB)
11. Users get notified about the failure (perhaps a system that filters `yggdrasil_ops` DB for errors)
12. User corrects upstream information
13. The corresponding Yggdrasil watcher detects upstream information change
14. Plan gets regenerated
15. If plan approval is required, see Workflow 1.
```

### Workflow 4: Re-Run After Failure (assumes plan okay, external issue)

```
1. Plan P12345 executed (run_token=0, executed_run_token=0)
2. Execution failed due to external issue (e.g. plan okay, data not in place)
3. User fixes the issue
4. User opens Genstat, finds project/plan P12345
5. Genstat: GET /yggdrasil_plans/pln_tenx_P12345_v1
6. User clicks "Re-Run"
7. Genstat: PUT /yggdrasil_plans/pln_tenx_P12345_v1
   - run_token: 1 (incremented from 0)
   - run_requested_at: <now>
   - run_requested_by: <user>
8. Yggdrasil PlanWatcher detects run_token > executed_run_token (1 > 0)
9. Plan executes again
10. On success: executed_run_token updated to 1
```

---

## Appendix: Plan Step Schema

Each step in `plan.steps` has:

```json
{
  "step_id": "unique_step_identifier",
  "name": "human_readable_name",
  "fn_ref": "module.path:function_name",
  "params": {
    "key": "value"
  },
  "inputs": [
    {"artifact_id": "previous_step:output", "required": true}
  ],
  "outputs": [
    {"artifact_id": "this_step:result", "type": "file"}
  ],
  "deps": ["step_id_1", "step_id_2"]
}
```

**Display Recommendations:**
- Show `name` as human-readable step name
- Show `params` as configuration details
- Show `deps` to indicate execution order
- Hide `fn_ref` unless debugging

---

## Changelog

| Version | Date | Changes |
|---------|------|---------|
| 1.1 | 2026-01-28 | **Critical fix**: Corrected token initial values (`run_token: 0, executed_run_token: -1`) to match implementation. Added explicit examples showing eligibility calculation. |
| 1.0 | 2026-01-16 | Initial release |
