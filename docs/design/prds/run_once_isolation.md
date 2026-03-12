# Yggdrasil – Run-Once Execution Isolation (PRD)

## Version
v1.2

## Status
Approved for implementation

---

## 1. Overview

This document specifies how **run-once executions** (CLI-driven, ephemeral runs)
are isolated from **daemon-driven executions** in Yggdrasil, while still flowing
through the unified `yggdrasil_plans` control plane.

It also defines the default **blocking behavior** of run-once when approval
is required, ensuring that the CLI reflects real system state instead of
silently exiting.

The goals are to ensure that:

- Plans created via `run-once` are **never picked up by running Yggdrasil daemons**
- Run-once executions remain safe, deterministic, and debuggable
- Approval semantics are consistent between CLI and daemon workflows
- Plan overwrite behavior is explicit and user-controlled
- No plan is executed silently or ambiguously

---

## 2. Problem Statement

With the introduction of the `yggdrasil_plans` database and a unified PlanWatcher,
all executable plans flow through the same control plane.

Without additional safeguards:

- A plan created via `run-once` could be executed by a daemon
- Multiple executions could occur unintentionally
- CLI behavior could diverge from production behavior
- Plan overwrites could silently erase important intent

This PRD defines explicit rules to prevent these failure modes.

---

## 3. Design Principles

1. Execution ownership must be explicit
2. Run-once is a debugging and exception-handling tool
3. Blocking is acceptable in run-once when it reflects real system state
4. Plan overwrite must never be silent
5. One plan slot corresponds to one active intent

---

## 4. Plan Document Extensions

Two new fields are introduced on plan documents in `yggdrasil_plans`:

```json
{
  "execution_authority": "daemon | run_once",
  "execution_owner": "string"
}
```

### 4.1 `execution_authority`

Defines which execution mode is allowed to process the plan.

Allowed values:
- `daemon` — normal production execution via Yggdrasil daemon
- `run_once` — ephemeral execution via CLI run-once mode

### 4.2 `execution_owner`

Identifies **who is allowed to execute the plan**.

- For daemon-managed plans:
  - value may be omitted (or set to a static identifier (e.g. `"daemon"`))
- For run-once plans:
  - **must be generated uniquely at CLI startup**
  - recommended format: `run_once:<uuid>`
  - ensures isolation between concurrent run-once invocations

---

## 5. CLI Execution Modes

### 5.1 `--plan-only`

Creates and persists a plan without executing it.

Behavior:
- Forces `auto_run = False`
- Sets `execution_authority = "daemon"`
- Uses deterministic plan ID
- CLI exits immediately after persistence

Intended for:
- inspecting plan intent
- debugging realm logic
- allowing Genstat-based approval before execution

---

### 5.2 `--run-once`

Creates and executes a plan via a scoped PlanWatcher.

Default behavior:
- Sets `execution_authority = "run_once"`
- Generates a unique `execution_owner` at startup
- Sets `auto_run = True` (subject to realm veto logic)
- Starts a scoped PlanWatcher bound to its `execution_owner`

#### Approval-required behavior (default)

If realm policy or veto logic results in:

```text
auto_run == False
```

Then run-once:

- Persists the plan with `status = "draft"`
- Prints a clear message:
  ```
  Plan requires approval. Waiting for approval...
  (Press Ctrl+C to exit)
  ```
- Blocks until:
  - the plan is approved and executed, or
  - the user terminates the process

This behavior ensures the CLI reflects the true execution state.

---

## 6. Execution Rules

### 6.1 Daemon PlanWatcher

The daemon PlanWatcher **must skip** any plan where:

```text
execution_authority == "run_once"
```

Daemon execution is permitted only when:
- `execution_authority == "daemon"`
- `status == "approved"`
- `run_token > executed_run_token`

---

### 6.2 Run-Once Scoped PlanWatcher

A run-once scoped PlanWatcher:

- Only considers plans where:
  ```text
  execution_authority == "run_once"
  execution_owner == <its generated token>
  ```
- Blocks while waiting for approval if required
- Executes the plan once eligibility conditions are met
- Exits after success or failure

If the plan’s `execution_authority` or `execution_owner` changes such that it no longer matches the run-once scope, the run-once watcher must stop waiting and exit with a clear message.

---

## 7. Overwrite Semantics

Plan IDs are deterministic.

When creating a plan:

- CLI **must check** whether a plan document with the same `_id` already exists
- If a plan exists:
  - CLI **must warn the user**
  - CLI must display a short summary (status, origin, last update)
  - Overwrite is only permitted with explicit `--force`
- Without `--force`, plan creation must abort

This prevents silent loss of intent and accidental replacement of active plans.
Applies to all CLI plan creation modes (--plan-only and --run-once) and any other CLI path that persists a plan doc.

---

## 8. Failure Handling

- If a run-once plan fails:
  - `executed_run_token` is not advanced
  - daemon continues to ignore the plan
  - CLI exits with non-zero status
- Failed plans remain visible for inspection and manual intervention

---

## 9. Schema Enforcement

- If `execution_authority` is missing or invalid:
  - plan is skipped by all PlanWatchers
  - error is logged loudly
- This prevents accidental execution of malformed or partially-written plans

---

## 10. Interaction with Genstat

Genstat may update plan documents to:

- approve plans (`status = "approved"`)
- request reruns (`run_token` increment)

### Optional Capability (Documented Contract)

Genstat **may** convert a run-once plan into a daemon-managed plan by:

```json
{
  "execution_authority": "daemon",
  "execution_owner": null
}
```

This allows a plan generated via run-once to later be adopted into the
normal daemon workflow.

Changing a run-once plan to daemon-managed transfers execution responsibility to the daemon; run-once will stop monitoring that plan. This could be a warning in Genstat.

This behavior requires **documentation only**, meaning only update the `GENSTAT_PLAN_CONTRACT.md`; no Genstat code is mandated by this PRD.

---

## 11. Backward Compatibility

No backward compatibility guarantees are required.

Plans missing execution metadata are treated as invalid and skipped.

---

## 12. Out of Scope

- Execution prioritization
- Cross-origin automatic handoff
- Automatic cleanup of run-once plans
- Interactive plan editing in CLI

---

## 13. Success Criteria

- Run-once plans are never executed by daemons
- Plan overwrite always requires explicit user intent
- CLI behavior reflects real approval state
- Daemon execution remains unchanged
- Debugging via run-once is reliable and faithful

---
