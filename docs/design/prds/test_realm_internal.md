# Yggdrasil Internal Test Realm PRD (Dev-Only) — v2.0

## 1. Purpose

Create an internal **dev-only test realm** that generates **predictable, configurable plans** from a “scenario document” stored in CouchDB.

This realm exists to enable repeatable validation of:
- Plan generation (realm side)
- Plan persistence into `yggdrasil_plans` (via the normal core persistence path)
- Engine execution of synthetic steps (success / controlled failure / delay / artifact emission)
- Ops event emission (so `yggdrasil_ops` reflects step progress)

**Important:** The realm does **not** own plan approval, ownership, overwrite policy, daemon vs run-once behavior, or plan rerun semantics. Those are **system behaviors** tested by operating Yggdrasil against plans produced by this realm.

---

## 2. Non-Goals

- Not a production realm; **must never run outside dev mode** (modes, e.g. `dev`, can be picked up by YggSession)
- Not a replacement for unit tests / pytest
- Not responsible for:
  - approval workflow (`draft` → `approved`)
  - execution targeting (`daemon` vs `run_once`)
  - overwrite prompts / `--force`
  - plan reruns (`run_token` / `executed_run_token`)
  - Genstat UI behavior or contracts (beyond being compatible with the plan schema)

---

## 3. High-Level Concept

### 3.1 Input
A **scenario document** in CouchDB (recommended: `yggdrasil` DB), identified via:
- `type: "ygg_test_scenario"`

The scenario doc declares:
- which plan recipe to build
- optional recipe parameters
- (optionally) an explicit custom step list

### 3.2 Output
A **Plan draft** returned by the realm and persisted into `yggdrasil_plans` using the standard persistence mechanism.

The persisted plan must be executable by the existing Engine.

---

## 4. Requirements

### 4.1 Dev-Only Access (Hard Gate)
**R1.** The test realm is loadable **only** when dev mode is enabled (e.g. `--dev` or config flag consumed by `YggSession`).

**R2.** In non-dev mode, the test realm must not:
- register watchers
- register handlers
- accept events
- generate plans

**R3.** In dev mode, log once at startup:
- `TestRealm enabled (dev mode).`

### 4.2 Watcher & Event Source
**R4.** The test realm registers a watcher pointed at the **`yggdrasil` DB**.

**R5.** The watcher filters for scenario docs via:
- `doc.type == "ygg_test_scenario"`

**R6.** Any update to a scenario doc should cause the handler to run (normal change-driven behavior).

### 4.3 Scenario Document Contract
**R7.** A scenario doc must contain:
- `_id`: scenario doc id (string)
- `type`: `"ygg_test_scenario"`
- `recipe`: recipe name (string; see §4.5)
- `params`: dict (optional; recipe-specific)
- `steps`: list (optional; overrides recipe step list; see §4.5.3)
- `notes`: string (optional)

**R8.** Scenario docs support explicit regeneration by changing a dedicated field (optional):
- `regen_token: str` (recommended) or `scenario_revision: int`

Note: any edit triggers a change event; `regen_token` just makes “regen” edits explicit.

### 4.4 Plan Generation Rules
**R9. Deterministic plan_id.** For a given scenario doc `_id`, the realm must generate a deterministic `Plan.plan_id`.
Example:
- `pln_test_<scenario_doc_id>_v1`

**R10. Stable scope.** The realm must set:
- `plan.scope = {"kind": "test_scenario", "id": "<scenario_doc_id>"}`

**R11. StepSpec validity.** Each generated `StepSpec` must include:
- `step_id` deterministic within plan
- `name`
- `fn_ref` resolvable by the engine’s `resolve_callable`
- `params` JSON-serializable (`params` must be persistable to CouchDB. During persistence, non-JSON types like `pathlib.Path` are converted to JSON-safe values (e.g., `Path` → `str`). At execution, the engine will type-coerce params back to `Path` for step parameters annotated as `Path` / `Optional[Path]` / `Path` | `None`.)
- `deps` correct for the requested topology
- `scope` optional (defaults to plan.scope if omitted)

**R12. Small & safe by default.** Recipes should default to short runtimes and non-destructive behavior.

### 4.5 Recipes and Step Library

#### 4.5.1 Step library (must-have)
Provide a small set of **dev-only test steps**:

1) `step_echo`
- Emits message (event/log), returns success

2) `step_sleep`
- Sleeps N seconds, returns success

3) `step_fail`
- Raises exception with message (controlled failure)

4) `step_write_file` (recommended)
- Writes a small file into a sandbox dir
- Records an artifact via `ctx.record_artifact(...)` (or emits equivalent event)
- Returns success

5) `step_random_fail` (optional)
- Fails with probability `p` (default off)

**R13.** Steps must be importable so `fn_ref` resolves reliably.

#### 4.5.2 Built-in recipes (must-have)
Implement at least these recipes:

**T1. happy_path**
- echo → sleep(1) → echo

**T2. fail_fast**
- fail

**T3. fail_mid_plan**
- echo → fail → echo (with `deps` so final echo depends on fail)

**T4. long_running**
- sleep(N) where N defaults to 40

**T5. artifact_write**
- write_file → echo

Recipes are selected via `scenario.recipe`.

#### 4.5.3 Optional recipe override
**R14.** If `scenario.steps` is present, it overrides the recipe.
A minimal override schema:
- `steps: [{ "name": "step_echo", "params": {...}, "deps": ["prev_step_name"] }]`

(If you want to keep this ultra-simple, you may omit overrides initially and rely on recipes only.)

### 4.6 Repo Placement
Preferred:
- `lib/realms/test_realm/`
  - `__init__.py`
  - `watcher.py`
  - `handler.py`
  - `steps.py`
  - `recipes.py`

**R15.** The realm must be dev-gated at load time regardless of placement.

---

## 5. Integration Points (Realm-only)

### 5.1 Registration
- Register the test realm only in dev mode via the **current realm loading mechanism**.
- No requirement to use legacy `module_registry.json` unless your dev boot path still relies on it.

### 5.2 Handler contract
- Handler consumes scenario doc, builds a `Plan`, returns a plan draft in the same shape as other realms.

### 5.3 Plan persistence
- Realm does **not** write to `yggdrasil_plans` directly.
- Core persists the plan draft using the shared persistence mechanism.

---

## 6. Safety

**R16.** All filesystem writes must go to a sandbox directory, default:
- `sim_out/tmp/ygg_test_realm}/<scenario_doc_id>/...`
(or a subdir under `ctx.scope_dir`, if that’s the established convention)

**R17.** No deletion of external files, no touching real project dirs, no network access.

---

## 7. Acceptance Criteria (Realm-only)

- Test realm loads and operates **only in dev mode**
- Writing/updating a scenario doc produces a deterministic, valid plan draft
- Plans execute successfully for recipes T1/T4/T5
- Plans fail predictably for recipes T2/T3
- Step events are emitted and can be consumed into ops

---

## Appendix A — Operator Playbook (Not Realm Requirements)

This is how you use the realm to validate **system** behaviors:

1) Start Yggdrasil in dev mode (daemon or CLI)
2) Create a scenario doc in `yggdrasil` with `type=ygg_test_scenario`
3) Observe the plan created in `yggdrasil_plans`
4) Approve the plan by editing `status="approved"` (to test approval flow)
5) Regenerate plan by editing the scenario doc (or changing `regen_token`)
6) Test reruns by incrementing plan-level `run_token` (system behavior)
7) Test overwrite/collisions by re-triggering same scenario doc id (system behavior)
8) Test run-once vs daemon by running the respective mode (system behavior)
