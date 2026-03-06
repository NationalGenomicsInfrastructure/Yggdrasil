# Yggdrasil – Plan Approval & Execution Workflow (PRD)

## Version
v1.0 (post-design lock-in)

## Status
Approved for implementation

---

## 1. Overview

This document defines the **Plan Approval & Execution Workflow** for Yggdrasil.

The workflow introduces a clean separation between:
- **Plan generation (intent)**
- **Plan execution (reality)**

It supports:
- automatic execution (auto-run),
- human approval via Genstat,
- explicit manual re-runs,
- restart safety,
- and avoids accidental re-execution of historical plans.

---

## 2. Databases

### 2.1 `yggdrasil_plans` (NEW)

Dedicated database for plan intent and approval state.

Contains **one document per plan slot**, overwritten on regeneration.

### 2.2 `yggdrasil`

Core coordination database.
- Stores watcher checkpoints (`last_seq`)
- Stores digested project-level state

### 2.3 `yggdrasil_ops`

Operational ledger.
- Stores engine/step execution events
- Remains the source of truth for execution details

---

## 3. Plan Document Model (`yggdrasil_plans`)

### 3.1 Required Fields

```json
{
  "_id": "pln_tenx_P36805_v1",

  "realm": "tenx",
  "scope": "proj-P36805",

  "status": "draft | approved",

  "plan": { ... },
  "preview": { ... },

  "run_token": 0,
  "executed_run_token": -1,

  "created_at": "ISO-8601",
  "updated_at": "ISO-8601"
}
```

### 3.2 Optional / Recommended Fields

```json
{
  "run_requested_at": "ISO-8601",
  "run_requested_by": "user@domain",

  "last_executed_at": "ISO-8601",

  "source_doc_id": "projects:P36805",
  "source_doc_rev": "3-abc123"
}
```

### Notes
- No `type` field is required since this DB is plan-dedicated.
- Plan documents represent **intent**, not execution history.

---

## 4. Plan Lifecycle

### 4.1 Plan Generation

When a realm generates a plan:

1. Compute `auto_run` via policy (veto/AND semantics).
2. Persist plan document (overwrite by `_id`):
   - `status = "approved"` if `auto_run=True`
   - `status = "draft"` otherwise
3. Reset execution state:
   - `run_token = 0`
   - `executed_run_token = -1`

This ensures regenerated plans behave as fresh intent.

---

### 4.2 Auto-Run Execution

- Auto-run plans are persisted as `status="approved"`.
- Execution is triggered via the same watcher mechanism as approved plans.
- No special execution path exists for auto-run plans.

---

### 4.3 Manual Approval (Genstat)

Genstat:
- Displays plan preview.
- Updates plan document with:
  - `status="approved"`
  - `approved_at`
  - `approved_by`
- Must include `_rev` to prevent approving stale plans.

---

### 4.4 Manual Re-run

Genstat provides a **Re-run** action:
- Increments `run_token` by `+1`
- Sets:
  - `run_requested_at`
  - `run_requested_by`
- Requires correct `_rev`

No plan regeneration is required.

---

## 5. Execution Eligibility Rules

A plan is eligible for execution iff:

```
status == "approved"
AND
run_token > executed_run_token
```

- Missing `executed_run_token` is treated as `-1`.
- This prevents:
  - re-execution on restart
  - duplicate execution of the same run
- Manual re-runs work naturally via token increment.

---

## 6. PlanWatcher Behavior

### 6.1 Trigger

- Watches `_changes` feed on `yggdrasil_plans`
- Uses `include_docs=true`

### 6.2 Execution Logic

For each plan document change:

1. Read plan document
2. Evaluate eligibility rule
3. If eligible:
   - Execute plan via Engine
4. On successful completion:
   - Update plan document:
     - `executed_run_token = run_token`
     - `last_executed_at = now`

### 6.3 Loop Safety

- Updating `executed_run_token` causes another change event.
- Eligibility rule fails (`run_token == executed_run_token`)
- Watcher skips → no infinite loop.

---

## 7. Startup Recovery

### 7.1 Normal Case

- PlanWatcher persists `_changes` checkpoint:
  - `_id = watcher_checkpoint:PlanWatcher`
  - stored in `yggdrasil` DB
- On restart:
  - Resume `_changes` feed from `last_seq`
  - Process only missed events

### 7.2 Fallback (Checkpoint Missing)

- Query plan documents in `yggdrasil_plans`
- Execute those where:
  - `status="approved"`
  - `run_token > executed_run_token`

---

## 8. Failure Handling

- Engine failure:
  - Do NOT update `executed_run_token`
  - Plan remains eligible
  - Failure is surfaced via Slack notification
- Failed plans may later be:
  - manually re-run
  - automatically resurfaced (future feature)

---

## 9. Security & Trust Model

- Genstat and Yggdrasil share CouchDB access
- Correctness relies on:
  - `_rev` enforcement
  - UI discipline (approval/rerun only edits allowed fields)
- Optional future hardening:
  - CouchDB `validate_doc_update`
  - API-based approval endpoints

---

## 10. Out of Scope (for now)

- Multi-level approvals
- Plan rejection semantics
- Approval timeouts
- Automatic retry policies
- Dry-run execution (documented separately)

---

## 11. Success Criteria

- Auto-run plans execute immediately and exactly once per run_token
- Approved plans execute exactly once per run_token
- Manual re-run works reliably
- Restart does not re-run old plans
- No stray plans remain executable
- Genstat approval is race-safe

---
