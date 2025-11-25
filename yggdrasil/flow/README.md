# yggdrasil.flow – minimal execution API

**What this gives you**
- **@step** decorator (`step.py`): standardizes step I/O, artifact emission, and progress. Supports `Annotated[..., In(...)]` / `Annotated[..., Out(...)]` for typed inputs/outputs.
- **Planner** (`planner/api.py`): protocol for generating execution plans from trigger events. Takes a `PlanningContext` (realm, scope, scope_dir, source document, reason, optional realm_config) and returns a `PlanDraft`. 
- **Engine** (`engine.py`): runs a `Plan` (list of `StepSpec`), creates per-step workdirs, computes fingerprints (params + digests of declared inputs), writes `success.fingerprint`, emits events.
- **Events** (`events/emitter.py`): pluggable emitters (FileSpoolEmitter, TeeEmitter, CouchEmitter). FileSpoolEmitter writes one JSON per event under:  
`$YGG_EVENT_SPOOL/<realm>/<plan_id>/<step_id>/<run_id>/*.json`  
Plan-level events (e.g., `plan.started`) omit `<step_id>` directory.
- **Artifacts** (`artifacts.py`): `ArtifactRefProtocol` for pluggable artifact resolution. Includes `SimpleArtifactRef` for common use cases.

**Core dataclasses** (`model.py`)
- `Plan`, `StepSpec` (planner → engine)
- `Artifact`, `StepResult` (step return, emitted as `step.succeeded`)
- `StepContext` (what a step sees: realm, scope, plan_id, step_id, step_name, workdir, scope_dir, emitter, run_mode, fingerprint, run_id)
- `PlanningContext` (trigger context: realm, scope, scope_dir, emitter, source_doc, reason, optional realm_config)
- `PlanDraft` (planner output: plan + auto_run flag + approvals_required + notes + preview)
- `FactsProvider` (abstract base for normalizing source documents into facts dicts)

**Conventions**
- Fingerprint = sha256(JSON of params + digests of *declared* inputs).
- Declare inputs via `Annotated[..., In(artifact_id)]` or in `StepSpec.inputs`.
- Artifacts have `key`, `path`, `digest` (`sha256:<hex>` or `dirhash:<hex>`).
- Use `ctx.record_artifact(ref, path=..., digest=...)` to register artifacts (replaces `add_artifact_ref`; back-compat alias kept temporarily).
- Artifact refs must implement `key()` and `resolve_path(scope_dir)`.

**Env vars**
- `YGG_WORK_ROOT`: engine work root (defaults to `/tmp/ygg_work`)
- `YGG_EVENT_SPOOL`: event spool root (defaults to `/tmp/ygg_events`)