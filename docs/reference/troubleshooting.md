# Troubleshooting

Common failure modes and how to resolve them.

---

## CouchDB connectivity

### Daemon fails to connect — host unreachable

**Symptom:** Startup log shows `ConnectTimeoutError` or `requests.exceptions.ConnectionError`.

**Checks:**
1. Verify network access to the CouchDB host
2. Verify CouchDB is running: `curl http://<host>:<port>/`
3. Check `main.json` → `external_systems.endpoints.<name>.url` matches the running instance

### Daemon fails to connect — wrong database name

**Symptom:** Log shows `ConnectionError: Database <name> does not exist` (or similar). The daemon continues running but the watcher for that connection is non-functional.

**Resolution:** Check the `resource.db` value under the relevant `connections` entry in `main.json`. Correct the name and restart the daemon.

> **Note:** The daemon does not fail fast on a missing database. A future improvement will address this. For now, watch the startup log carefully.

### Authentication failures

**Symptom:** `401 Unauthorized` in logs.

**Resolution:** Set credentials as environment variables:
```bash
export YGG_COUCH_USER=admin
export YGG_COUCH_PASS=yourpassword
yggdrasil --dev daemon
```

Credentials are resolved from `external_systems.endpoints.<name>.auth.user_env` and `auth.pass_env` in `main.json`.

---

## Configuration

### Missing or invalid config key

**Symptom:** A `RuntimeError` or `KeyError` appears in logs during daemon initialization — but not at the very first moment the process starts. Config errors surface in two phases:

1. **During DB manager initialization** — a `RuntimeError` is raised if a required CouchDB endpoint block is missing from `external_systems.endpoints` in `main.json`.
2. **During watcher initialization** — a `KeyError` is raised when a watcher backend config is missing required keys. This happens after realm discovery, when `WatcherManager` starts the backends.

> **Note:** `ConfigLoader` returns `None` (silently) for any key that is absent from the config file. Mistakes that don't touch the two paths above will not surface until the code path that uses the value is actually exercised at runtime.

**Resolution:** Check `main.json` in the resolved configuration directory. If `YGG_HOME` is set, Yggdrasil reads `$YGG_HOME/common/configurations/main.json`; otherwise it falls back to `yggdrasil_workspace/common/configurations/main.json`. See [configuration.md](../getting_started/configuration.md) for the full structure.

---

## Realm not discovered

### Handler not receiving events

**Symptom:** Events fire in CouchDB but the handler is never called.

**Checks:**
1. Confirm the entry point is registered in `pyproject.toml`:
   ```toml
   [project.entry-points."ygg.realm"]
   my_realm = "my_realm:get_realm_descriptor"
   ```
2. Re-install the package after editing `pyproject.toml`: `pip install -e .`
3. Check startup log for: `Discovered realm my_realm` — if absent, the realm was not found
4. If using dev-mode gating, verify `yggdrasil --dev daemon` is used

### WatchSpec validation error at startup

**Symptom:** `RuntimeError: WatchSpec from realm 'X' references unknown handler_id 'Y'`

**Resolution:** The `target_handlers` list in your `WatchSpec` must match `handler_id` class attributes exactly. Check for typos.

**Symptom:** `RuntimeError: realm_id 'X' already registered`

**Resolution:** Two realms share the same `realm_id`. Each realm must have a globally unique `realm_id` in its `RealmDescriptor`.

---

## Plan not created

### CouchDB document changed but no plan appears in `yggdrasil_plans`

**Checks:**
1. Verify the document matches your `filter_expr`. Test the JSON Logic predicate manually:
   ```python
   from json_logic import jsonLogic
   jsonLogic(filter_expr, {"doc": your_doc, "deleted": False, "meta": {}})
   ```
2. Check `build_payload` returns a dict with the fields your handler expects
3. Confirm `connection` name in the WatchSpec matches its key in `external_systems`
4. Search the log for `Planning refused to replace plan`: a plan already stored under that ID may have been left as it was (see [Plan not replaced: planning refused](#plan-not-replaced-planning-refused)).

### Plan not replaced: planning refused

**Symptom:** The log says, at ERROR, `Planning refused to replace plan '<plan_id>': its <field> <value> is <reason> (supported plan-document families: [1]). The draft was not persisted; the stored plan is unchanged.`

**Explanation:** Planning replaces the whole document stored under a plan's ID, and first checks what that document is. It refuses a document whose `schema_version` is invalid (`"1"`, `1.0`, `null`, ...) or names a family this release does not support, such as a family-2 plan written by a newer release. It also refuses a document whose `created_at` it cannot keep: missing, not a real date-time, or spelled in a way it may not re-spell, such as with a fraction of a second. Nothing is written, and planning goes on with its other drafts. `run-doc --force` does not bypass the check. See [When planning replaces a plan](document_contract.md#when-planning-replaces-a-plan).

**Resolution:** It depends on the field. Never choose a value just to make the check pass: the marker states which contract the whole document follows, and `created_at` records when the document was created.

- `schema_version ... is unsupported`: the plan belongs to a newer release. Plan it with software that supports that family; do not change the marker.
- `schema_version ... is invalid`: the marker was damaged, or written outside the contract. A value such as `"1"` does not show that the document follows family 1, so do not replace it with a valid-looking one. Restore the marker only when you know what it was, for instance from a backup or from the record of the edit that damaged it; if the document had no marker before, restoring it means removing the field. Otherwise, keep a copy of the document, delete it, and let the realm plan again.
- `created_at is missing`, or `created_at ... is malformed`: the creation instant is unknown. Restore it only from a known source, spelled canonically (`2026-10-22T10:00:00Z`). Otherwise, keep a copy of the document, delete it, and let the realm plan again: the new document records a new creation time.
- `created_at ... is unsupported`: the value is a real date-time that planning may not re-spell. A whole second with another offset, or with `+00:00` in a family-1 document, can be written as the same instant in `Z`: `2026-10-22T12:00:00+02:00` becomes `2026-10-22T10:00:00Z`. A fraction of a second has no canonical spelling: dropping it changes the recorded instant, so do that only when the change is acceptable, and otherwise delete the document as above.

Write a repair conditionally on the `_rev` you read, changing only that field, as in step 3 of [Approving or rerunning by hand](document_contract.md#approving-or-rerunning-by-hand). Deleting the document discards its approval and its recorded results: the realm's new plan is a new document, which needs approving again unless its realm runs it automatically.

---

## Plan created but not executing

### Plan stays in `status="draft"` indefinitely

**Explanation:** `auto_run=False` in the `PlanDraft` sets initial status to `"draft"`. The plan waits for manual approval.

**Resolution:** Yggdrasil does not currently ship an approval UI or command.
Use an operator-provided integration with the configured plan store to set
`status="approved"`, following
[Approving or rerunning by hand](document_contract.md#approving-or-rerunning-by-hand).
For SQLite, that integration must also advance the
plan change sequence transactionally; editing only the stored JSON will
not notify PlanWatcher.

### Plan is `status="approved"` but Engine never runs it

**Checks:**
1. Confirm PlanWatcher is started: `grep "PlanWatcher" yggdrasil.log`
2. Search the log for `is not executed: its schema_version`. A plan document without a `schema_version`, such as one written before this release, or with a value other than `1`, is never executed, and is logged at WARNING each time it is observed. Regenerate it through its realm (see [Plans stopped running after an upgrade](#plans-stopped-running-after-an-upgrade)).
3. Check the stored tokens. `run_token` and `executed_run_token` must both be JSON integers, not strings, floats or booleans; `run_token` from 0 to 2^53 − 1, `executed_run_token` from −1 to 2^53 − 1, and `executed_run_token` not above `run_token`. Otherwise the plan is not eligible, and nothing is defaulted or converted. PlanWatcher skips such a plan with a DEBUG line only (`Skipping ineligible plan: <plan_id> (Not eligible: invalid tokens (...))`, or, in run-once's recovery scan, `Recovery: requested plan <plan_id> is not eligible (...)`). Debug logging is on only with `--dev`, which also selects the development configuration, so do not enable it on a production service to see the reason: compare the stored tokens with the rule instead.
4. Check `run_token > executed_run_token` in the plan document. If they are equal, the last request is already finished, whatever its outcome (see [A finished plan does not run again](#a-finished-plan-does-not-run-again)).
5. Search the log for `does not describe an executable request`. A plan missing an execution field, for the plan or one of its steps, or whose `failure_policy` is missing or unknown, is refused at admission, logged at ERROR with each problem named. Fix the realm's planning, and let it regenerate the plan.
6. Verify the configured internal plan store (`yggdrasil_plans` database) exists and is accessible: the CouchDB
   plans connection in production, or the SQLite file in dev mode.
7. Search the log for `pending finalization`: the plan may have an unrecorded result (see [Result could not be recorded](#result-could-not-be-recorded)).
8. Search the log for `the daemon is stopping`. A plan that became eligible while the daemon was shutting down was not started, and stays eligible. A daemon restarted from its saved checkpoint resumes after that change and does not see it again. Re-emit an observable change for the plan once the daemon is running.

See [What Yggdrasil refuses](document_contract.md#when-a-plan-is-admitted-for-execution) for the whole order of admission checks.

### Plans stopped running after an upgrade

**Symptom:** After upgrading to a release with the document contract, approved plans that ran before no longer run, and the log says, at WARNING, `Plan '<plan_id>' is not executed: its schema_version is absent, so it is unversioned (supported plan-document families: [1])`.

**Explanation:** Documents written by earlier releases carry no `schema_version`. They stay unversioned: nothing stamps them, and Yggdrasil never executes or updates them. Planning may replace them, which is how a plan comes into the contract.

**Resolution:** Regenerate the plans you still want through their realm. Each comes out as a family-1 document of a new generation, which needs approving again unless its realm runs it automatically. In a development store, list the documents by version state first, or reset the disposable state instead. See [Upgrading and rolling back](document_contract.md#upgrading-and-rolling-back).

---

## Plan finished, but not as expected

### A finished plan does not run again

**Symptom:** `status="approved"`, `executed_run_token == run_token`, and nothing runs, even though `last_finalized_execution.outcome` is `"failed"`.

**Explanation:** A `continue_independent` plan finishes its request once every step that could run has run, even if some failed. Its result and its token are recorded together, and the plan does not run again by itself. Approving it again changes nothing.

**Resolution:** Fix the cause, then raise `run_token` by one, leaving `status` as `"approved"`. Write it conditionally on the plan's revision; on SQLite, the same transaction must also advance the plan-change sequence. Steps that succeeded are reused, and failed or blocked steps run again. See [Plan Execution](plan_execution.md#requesting-a-rerun).

### Steps were blocked

**Symptom:** The snapshot shows steps in state `step.blocked`, and the attempt's outcome is `"failed"`.

**Explanation:** Under `continue_independent`, a step whose prerequisite failed, or was itself blocked, is never invoked. Its `failed_ancestors` name the failed steps to fix; `direct_blockers` name its immediate prerequisites. A blocked step is not a failure of its own, and it runs on the next request once its prerequisites succeed.

If a step was blocked by a step it should not need, the dependency is in the plan: `deps` are the only thing that blocks. See [Dependencies and failure policy](../realm_authoring/guide.md#dependencies-and-failure-policy).

### Snapshot shows an attempt still running

**Symptom:** The snapshot's `attempt.state` stays `"running"`, but none of the plan's steps is running.

**Explanation:** An attempt shows as running until its report is published. No report is published when event publication had already failed during the attempt (the log says `Not publishing the report of attempt '<execution_id>'`), when publishing the report itself failed, or when the process was killed. None of these endings finished the request.

**Resolution:** Search the log for the attempt's execution ID to find the cause, such as an unwritable event spool. The plan document confirms that the request is unfinished: `executed_run_token` is unchanged, and the plan stays eligible. Once the cause is fixed, the next attempt at the plan replaces the stale one in the snapshot. See [Attempt reports](../flow_api/overview.md#attempt-reports).

### Plan rejected before any step ran

**Symptom:** The log and the attempt report show `termination_reason: "preflight_rejected"` and a diagnostic such as a duplicate step ID, a step ID that cannot name a work directory of its own (not a nonempty string, absolute, or with an empty, `.` or `..` part) or is not distinct from another when letter case and Unicode normalization are ignored, an unknown dependency, a dependency cycle, or a malformed or unresolvable `fn_ref`.

**Explanation:** The engine validates the whole plan before running anything, and rejects it without side effects. A rejected `fail_fast` plan stays eligible. A rejected `continue_independent` request is finished with a failed outcome, since the same plan would be rejected again.

A plan document whose `failure_policy` is missing or unknown never gets this far. Daemon and run-once execution refuse it at admission, so no attempt starts and there is no report: the log says, at ERROR, `Plan '<plan_id>' does not describe an executable request (...)`, and the plan stays eligible. Only a `Plan` passed directly to `Engine.run` reaches preflight with an explicitly unknown policy; there, an omitted policy defaults to `fail_fast`.

Only confirmed defects of the plan are rejected. A step module that exists but cannot be imported is a different case (see [`fn_ref` cannot be resolved or imported](#fn_ref-cannot-be-resolved-or-imported)).

**Resolution:** Fix the realm's planning, and let it regenerate the plan.

### Result could not be recorded

**Symptom:** The log says `finished, but its result could not be recorded`, followed by `keeping it pending`. The snapshot shows the attempt as finished, but the plan document's `executed_run_token` was not advanced. `run-doc --run-once` exits with code `1`.

**Explanation:** Recording the result failed. Either every retry failed, because the plan store was unreachable, failed, or kept losing write races, or a read returned something other than the plan's stored document, such as an error response from a proxy, which is never retried. Or the finalizer refused it over the plan's tokens: the stored tokens are not a valid pair, or recording the result would leave `executed_run_token` above `run_token`. The log then also says, at ERROR, `Cannot finalize execution ... the plan's tokens are not a valid pair (...)` or `... would leave run_token <n> / executed_run_token <m>, which is not a valid pair`. The process keeps the result in memory and does not run the plan again. Other plans are unaffected.

A pending result is dropped instead, and never recorded, when the plan's document changes family. The log then says, at WARNING, `Dropping the pending finalization of execution '<execution_id>': the schema_version of plan '<plan_id>' is now ...`. The attempt's report stays in the event spool.

**Resolution:** Once the plan store is healthy, raise `run_token`. If the tokens are the reason, first correct them so that they form a valid pair, with `run_token` above the pending request's token. The daemon records the pending result first, then runs the new request. A restart loses the pending result, and the old request then runs again, so realm steps must tolerate that. See [Plan Execution](plan_execution.md#when-a-result-cannot-be-recorded).

---

## Step execution failures

### `fn_ref` cannot be resolved or imported

Every step's `fn_ref` is resolved before any step runs. What happens next depends on whether the reference itself is wrong.

**Symptom:** The plan is rejected by preflight with `Malformed fn_ref for step '<step_id>'`, `Unresolvable fn_ref for step '<step_id>': '<fn_ref>' names module '<module>', which does not exist.`, or `... module '<module>' does not define '<name>'.`

**Resolution:** The reference is wrong, which is a defect of the plan:
- The `fn_ref` in your `StepSpec` must be a valid dotted Python path to a `@step`-decorated function
- The function must exist and be importable in the daemon's Python environment
- Check for typos in the module path

**Symptom:** The attempt aborts with `Importing the module for step '<step_id>' (fn_ref='<fn_ref>') failed: ...`, and `termination_reason: "orchestration_error"`.

**Explanation:** The module exists, but importing it failed: one of its own dependencies is missing, or it raised while being imported. That is a broken environment, not a malformed plan, so the plan is not rejected. The request stays eligible, and runs again once the environment is fixed and the plan changes again.

### `PreflightValidationError` — undecorated step function

**Symptom:** Plan execution is rejected with:
```
PreflightValidationError: Undecorated step function detected for step '<step_id>' (fn_ref='<fn_ref>'). Decorate it with '@step' from 'yggdrasil.flow.step'.
```

**Explanation:** The Engine resolves and validates every step's callable before any step runs. If `fn_ref` resolves to a plain function missing the `@step` decorator, the plan is rejected, with no step run. `PreflightValidationError` is a `ValueError`. The check prevents silent loss of lifecycle events (`step.started`, `step.succeeded`, `step.failed`), which `@step` is responsible for emitting.

**Resolution:** Decorate the function with `@step`:
```python
from yggdrasil.flow.step import step
from yggdrasil.flow.model import StepContext, StepResult

@step                     # required
def my_step(ctx: StepContext, **params) -> StepResult:
    ...
```

---

## DataAccess errors

### `DataAccessDeniedError` when accessing a connection

**Symptom:** Step or plan generation fails with `DataAccessDeniedError: realm 'X' has no permissions for connection 'Y' in phase 'Z'`

**Explanation:** The connection `Y` has a `data_access.realms` block that does not include your realm for the requested phase, or the realm's `permissions` list for that phase is missing `"read"` (or `"write"` for writes).

**Resolution:** Add your realm with the correct phase permissions to the connection's `data_access.realms` block in `main.json`:
```json
{
  "external_systems": {
    "endpoints": {
      "main_couchdb": {
        "backend": "couchdb",
        "url": "<host>:<port>",
        "auth": { "user_env": "YGG_COUCH_USER", "pass_env": "YGG_COUCH_PASS" }
      }
    },
    "connections": {
      "my_db": {
        "endpoint": "main_couchdb",
        "resource": { "db": "my_database" },
        "data_access": {
          "realms": {
            "my_realm": {
              "planning":  { "permissions": ["read"] },
              "execution": { "permissions": ["read", "write"] }
            }
          },
          "options": { "max_limit": 50 }
        }
      }
    }
  }
}
```

> **Note:** `"write"` permission does not grant `"read"`. A realm that only has `"write"` can call `save()` but calling `get()` or `find()` will still raise `DataAccessDeniedError`. Grant `"read"` explicitly if reads are also needed.

### `DataAccessDeniedError` — no data_access policy for connection

**Symptom:** `DataAccessDeniedError: connection 'X' has no data_access policy`

**Explanation:** Connections without a `data_access` block are not accessible via `ctx.data.connection()`. This is intentional — opt-in only.

---

## Daemon lock

### "Another local Yggdrasil daemon is already running in … mode"

One daemon may run **per mode** per user per host: `daemon.lock` (prod)
and `daemon-dev.lock` (dev) in the user runtime directory. The error
message and the lock metadata (pid, hostname, user, started_at) identify
the holder.

**Resolution:**

1. Identify the holding process from the metadata in the error message.
2. Stop the existing daemon. To run another daemon in the other mode or on
   another machine, configure it to use separate internal storage (see
   [Running prod and dev side by side](../getting_started/configuration.md#running-prod-and-dev-side-by-side)).
   Changing mode or machine only bypasses the local lock; it does not make
   sharing the same internal storage safe.
3. **Never delete a lock file while a daemon is running** — the advisory
   `flock` dies with the process, and deleting a *held* file lets a second
   daemon start. Stale lock files from exited daemons are harmless and can
   stay.

**Limits:** the lock does not coordinate across OS users, containers, or
hosts, and it is intentionally not database-aware — it cannot detect two
daemons sharing a CouchDB environment (the documentation and checkpoint
conflict warnings are the safeguard there).

---

## Internal storage (dev SQLite)

### Daemon refuses to start: invalid `internal_storage` configuration

Explicit configuration errors are fatal by design; Yggdrasil never falls
back to another storage backend. The message names the offending role,
connection, or path.

### "not an Yggdrasil internal-storage database" / schema version errors

The SQLite file failed lifecycle validation (unrelated file, malformed
content, or unsupported schema version). The dev database is disposable:
move the file aside — or delete it — and restart to create fresh state.
Never point the config at a file you care about; Yggdrasil refuses
symlinks and foreign-owned files.

### Approved a plan but the dev daemon does nothing

Work through the checks in
[Plan is approved but Engine never runs it](#plan-is-statusapproved-but-engine-never-runs-it):
`schema_version`, the token rule, `run_token > executed_run_token` and the
plan's execution fields. Also confirm that PlanWatcher is
running and that the approval was recorded as an observable change by
the configured backend.

A daemon with no checkpoint can miss a plan approved before it started.
After confirming PlanWatcher is running, the operator-provided storage
integration can re-emit an observable change for that plan. Re-emitting a
plan that is already executing does not start a second attempt: the daemon
runs one attempt per plan at a time, and once the running attempt is finished
it checks the plan again, running it only if a newer run was requested
meanwhile.

---

## Event spool

### No event files appearing in `$YGG_EVENT_SPOOL`

**Checks:**
1. Confirm `YGG_EVENT_SPOOL` is set (defaults to `/tmp/ygg_events`, or `/tmp/ygg_events_dev` under `--dev`)
2. Verify the directory is writable: `ls -la $YGG_EVENT_SPOOL`

### Finding events for a specific plan

```bash
find $YGG_EVENT_SPOOL -path "*/test_realm/*" -name "*.json" | sort
```

### Finding one attempt's events

Every attempt has its own directory, named after its `execution_id`, holding all of its events (see [Event spool layout](../flow_api/overview.md#event-spool-layout)). For IDs the engine allocated, name order is attempt order, so the last one in sorted order is normally the newest attempt (see [Execution IDs and attempt order](../flow_api/overview.md#execution-ids-and-attempt-order) for the limits). A directory holding neither `plan_attempt_started.json` nor `plan_attempt_report.json` is a reservation whose attempt was cancelled or killed before it recorded anything; the snapshot never shows it. An attempt's report may be missing too (see [Snapshot shows an attempt still running](#snapshot-shows-an-attempt-still-running)).

```bash
ls $YGG_EVENT_SPOOL/<realm>/<plan_id>/attempts/ | sort | tail -1
find $YGG_EVENT_SPOOL/<realm>/<plan_id>/attempts/<execution_id> -name "*.json" | sort
```

---

## See also

- [Configuration](../getting_started/configuration.md) — config file layout and environment variables
- [Test Realm](test_realm.md) — test scenarios for validating the pipeline end-to-end
- [Architecture Overview](../architecture/overview.md) — understanding the event flow
