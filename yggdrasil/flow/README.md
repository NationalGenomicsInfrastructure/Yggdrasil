# yggdrasil.flow – minimal execution API

**What this gives you**
- **@step** decorator (`step.py`): standardizes step I/O, artifact emission, and progress.
- **Planner** (`planner.py`): protocol for generating execution plans from trigger events. Takes a `PlanningContext` (realm, scope, source document, reason) and returns a `PlanDraft`. 
- **Engine** (`engine.py`): runs a `Plan` (list of `StepSpec`), creates per-step workdirs, computes fingerprints (params + declared inputs), writes `success.fingerprint`, emits events.
- **Events** (`events/emitter.py`): pluggable emitters (FileSpool, Tee). FileSpool writes one JSON per event under:
`$YGG_EVENT_SPOOL//<plan_id>/<step_id>/<run_id>/*.json`

**Core dataclasses** (`model.py`)
- `Plan`, `StepSpec` (planner → engine)
- `Artifact`, `StepResult` (step return, emitted as `step.succeeded`)
- `StepContext` (what a step sees)
- `PlanningContext` (trigger context: realm, scope, source document, reason)
- `PlanDraft` (planner output: plan + auto-run flag + approvals + notes)

**Conventions**
- Fingerprint = sha256(JSON of params + digests of *declared* inputs).
- Declare inputs either in `StepSpec.inputs` or `@step(..., input_keys=(...))`.
- Artifacts have `role`, `path`, `digest` (`sha256:<hex>` or `dirhash:<hex>`).

**Env vars**
- `YGG_WORK_ROOT`: engine work root (defaults to `/tmp/ygg_work`)
- `YGG_EVENT_SPOOL`: event spool root (defaults to `/tmp/ygg_events`)