# Plan Execution

What happens between an approved plan and its recorded result, and how to read and steer that as an operator. For how the engine runs the steps themselves, see the [Flow API](../flow_api/overview.md#engine-yggdrasilcoreengine).

---

## The plan document

Each persisted plan is one document in the plan store (`yggdrasil_plans` on CouchDB, or the dev SQLite file). Its full format, and who may write which field, are described in [Document Contract](document_contract.md). The fields that govern execution:

| Field | Meaning |
|---|---|
| `schema_version` | The document's compatibility family: `1` in this release. A document without it, or with any other value, is never executed (see [Versions and compatibility](document_contract.md#versions-and-compatibility)) |
| `status` | Approval only: `"draft"` or `"approved"`. Never changed by executing the plan |
| `run_token` | The latest execution request. Raise it to request another run |
| `executed_run_token` | The latest request that was *finished*. Starts at `-1` |
| `plan_generation` | Opaque ID of this planned version. Kept by approval and token changes, replaced when the plan is regenerated |
| `execution_authority` / `execution_owner` | Who may execute the plan: `"daemon"`, or one `"run_once"` session |
| `plan.failure_policy` | `"fail_fast"` or `"continue_independent"` (see [Failure policies](../flow_api/overview.md#failure-policies)) |
| `last_finalized_execution` | The result of the latest finished request: `execution_id`, `plan_generation`, `run_token`, `failure_policy`, `outcome`, `termination_reason`, `finalized_at`, and the full attempt `report` |

A plan is **eligible** to run when `status == "approved"`, its tokens are a valid pair, and `run_token > executed_run_token`. The tokens are a valid pair when both are JSON integers, not booleans, `run_token` is from 0 to 2^53 − 1, `executed_run_token` is from −1 to 2^53 − 1, and `executed_run_token` is not above `run_token`. Anything else makes the plan ineligible, such as a missing token, `"5"`, `5.0` or `true`: nothing is defaulted or converted. The caller must also hold its execution authority: the daemon runs `"daemon"` plans, and a `run-doc --run-once` session runs only the plans it created.

When a plan becomes eligible, the executing process reads the plan document afresh, with one plain read that writes nothing. The plan, its policy, family, generation, run token and authority all come from that one read, never from the change event that prompted it. The read counts only when it returns the plan's stored document, whose `_id` is the plan ID and whose `_rev` is a non-empty string; anything else is a storage failure (`admission_failed`), and the plan stays eligible. The document's `schema_version` is checked first: a document without one, or of a family this release does not support, is not executed (`unsupported_document`). The plan and each of its steps must then carry every execution field, and `plan.failure_policy` must be a known policy; otherwise the request is refused (`invalid_document`). A refused request runs nothing and writes nothing; see [What Yggdrasil refuses](document_contract.md#when-a-plan-is-admitted-for-execution) for the whole order of checks. Within one process, a plan has at most one attempt in flight. A request that arrives while it runs is checked again once that attempt is done.

---

## Approval status is not an execution outcome

`status` says only whether a plan may run. It stays `"approved"` after a run, however the run went. The outcome of a finished request is recorded in `last_finalized_execution.outcome`, which is `"succeeded"` or `"failed"`.

The plan summary's `last_finalized_outcome` reflects `last_finalized_execution`. The ops snapshot reflects something else: the newest observed attempt, which may be newer than the last finalized request, or not finalized at all. For example, if request 0 succeeds and request 1 then fails under `fail_fast`, the snapshot shows request 1's failure, while `last_finalized_execution` still records request 0's success. To relate the two, compare their `execution_id`, `plan_generation` and `run_token`.

`executed_run_token == run_token` means the latest request is **finished**, not that it succeeded. A `continue_independent` plan finishes its request even when steps failed, so it can have equal tokens and a failed outcome. To tell whether the latest request succeeded, check all three:

```text
executed_run_token == run_token
last_finalized_execution.run_token == run_token
last_finalized_execution.outcome == "succeeded"
```

---

## Which endings finish a request

Every attempt closes a report when it ends, but only some endings finish the request, recording its result and its token together:

| How the attempt ended | `fail_fast` | `continue_independent` |
|---|---|---|
| Every step succeeded or was reused | Finished, `succeeded` | Finished, `succeeded` |
| Every step that could run ran, with some failed or blocked | Not possible: the attempt stops at the first failure | Finished, `failed` |
| A step failed and the attempt stopped (`failed_fast`) | **Not finished**: stays eligible | Not possible |
| Rejected by preflight: invalid graph, step IDs that cannot name a work directory of their own, malformed output declarations, or a step reference that is malformed or names a missing module or function | **Not finished**: stays eligible | Finished, `failed`: rerunning an unchanged plan cannot help |
| Cancelled (daemon shutdown, Ctrl+C) before every step had run | Not finished: stays eligible | Not finished: stays eligible |
| Aborted by an infrastructure failure, such as the event spool being unwritable, or a step module that exists but fails to import | Not finished: stays eligible | Not finished: stays eligible |

A plan document whose `failure_policy` is missing or unknown belongs to neither column. Daemon and run-once execution refuse it at admission (`invalid_document`), so no attempt starts and there is no report. Its request stays eligible. A `Plan` passed directly to `Engine.run` is treated differently: an omitted policy defaults to `fail_fast`, and only an explicitly unknown one is rejected, by preflight.

A request that is not finished stays eligible. Nothing retries it on a timer: the daemon runs it again the next time it observes a change to the plan document. A `fail_fast` plan therefore keeps its established behavior: after a failure, it runs again on its next change.

A `continue_independent` plan whose request finished with failures does **not** run again by itself, and approving it again changes nothing. It needs a new request.

Once an attempt has run every step it could, its result is recorded even if the caller then shuts down. Stopping the daemon at that point does not throw the finished result away.

The log line that resolves each request follows the same distinction. A finished request that succeeded is logged at INFO. A `continue_independent` request that ran every step it could and finished `failed` is logged at WARNING: its failures are recorded truthfully, and nothing malfunctioned. A finished preflight rejection, an unfinished request, and a result that could not be recorded are logged at ERROR. The step failures themselves are logged where they happen.

---

## Requesting a rerun

To run a plan again, raise `run_token` by one from the value you read, and leave `status` as `"approved"`. This is the only way to rerun a finished `continue_independent` request, and it works for any plan. Follow [Approving or rerunning by hand](document_contract.md#approving-or-rerunning-by-hand): check that the document is of a supported family and of the generation you reviewed, write the change conditionally on its revision (`_rev`), and if the plan changed since it was read, read it again and check it again. If a retry finds `run_token` already moved, stop rather than raise it again. On the dev SQLite backend, the write must also advance the plan-change sequence in the same transaction, or PlanWatcher never observes it (see **Manual plan approval** in [Configuration](../getting_started/configuration.md)).

The new attempt runs in the same work directories as the previous one:

- A step whose earlier success is still valid is **reused** (`step.skipped`): its fingerprint is unchanged and its declared outputs exist.
- A step that failed runs again. So does a step that was blocked, once its prerequisites succeed. If they fail again, it is blocked again.
- To force a successful step to run again, remove its `success.fingerprint` from `<work_root>/<plan_id>/<step_id>/`, or one of its declared outputs.

Regenerating the plan, when a realm handler saves a new version of it, starts a new `plan_generation` with its tokens reset. That new version is a new request, and it runs as such. A result still in flight from the old version is never recorded onto the new one.

---

## When a result cannot be recorded

A finished request is recorded by a conditional write that commits the outcome and the executed token together. It refuses to write into a document that is no longer of the family the request was admitted under, and to overwrite a newer generation, a newer result or a changed authority. It also refuses while the plan's tokens are not a valid pair, or when recording the result would make them invalid by leaving `executed_run_token` above `run_token`. A write that loses a race or hits a transient storage error is tried again, up to three tries in all, 0.5 s and then 1 s apart. Those retries only record the result. A failed step is never retried.

If the result still cannot be recorded, after three failed tries or one non-retryable storage error, or because the plan's tokens are not a valid pair or recording would make them invalid, it is kept in memory as a **pending finalization**. A read that does not return the plan's stored document counts as a non-retryable storage error. What that looks like:

- The log reports `Plan '<plan_id>' execution '<execution_id>' ... finished, but its result could not be recorded; keeping it pending, and not executing the plan again until it is recorded or superseded`. When the plan's tokens are the reason, the finalizer's refusal follows `could not be recorded` in parentheses: `(Cannot finalize execution ...: the plan's tokens are not a valid pair (...) ...)`.
- The ops snapshot shows the attempt as `finished`, but the plan document still has the previous `executed_run_token`, and no `last_finalized_execution` for that `run_token`.
- `run-doc --run-once` exits with code 1.
- Other plans keep running.

While the result is pending, this process does not execute the plan again, and further change events for it retry nothing. To recover once the plan store is healthy again, raise `run_token`. The process then tries once more to record the pending result, with the same bounded tries, and runs the new request only once the old result is recorded or definitively refused. If recording fails again, the new request waits too: raise `run_token` again to try again. Each newer `run_token` buys one round of tries. If the plan's tokens are what keeps the result pending, first make them a valid pair again, with `run_token` above the pending request's token.

A pending result stays pending while the plan's tokens are not a valid pair, or recording it would make them invalid, and through a failed storage read or one that does not return the plan's stored document. It is dropped, without anything being written:

- when the plan's document is no longer of the family the request was admitted under. This is logged at WARNING; the result is never recorded and never retried, and its report stays in the event spool;
- when the plan is regenerated or deleted, whatever its tokens say;
- when its authority changes, once its tokens are a valid pair;
- when its result turns out to have been recorded after all.

A pending result lives only in memory. If the process restarts, it is lost, and the old request is still eligible, so it may run again. Realm steps must therefore tolerate running again over their own earlier work.

If recording is refused for good, because the plan was regenerated, deleted or reassigned while it ran, or its document changed family, the result is dropped (`superseded`). Run-once exits with code 1.

---

## Operational snapshots (`plan_status`)

The ops consumer reads the event spool periodically in daemon mode, and once when `run-doc --run-once` exits. It keeps one `plan_status` snapshot per plan in the operations store (`yggdrasil_ops` on CouchDB, or the dev SQLite file). Each cycle builds every plan's snapshot again, but writes only those whose content, apart from `updated_at`, changed since that consumer last wrote them, so a plan with no new events gets no new revision. A consumer that has just started, the daemon's after a start or the one `run-doc` runs, writes every plan once. A snapshot deleted from the operations store is written again when the plan's snapshot next changes, or when a consumer next starts.

A snapshot is the **execution status of one attempt**: the latest *observed* attempt, finished or not. An attempt is observed once it has published its start record or its report. Among those, the one whose execution ID orders highest is shown, which is normally the most recently started one; replaying an old attempt's events cannot change the choice. An attempt whose directory is still empty, because it was cancelled or killed between reserving its ID and publishing its start record, is never shown. How far the ordering can be relied on, across restarts, clocks and concurrent processes, is described in [Execution IDs and attempt order](../flow_api/overview.md#execution-ids-and-attempt-order). Every step is shown as that attempt left it. A step never takes an earlier attempt's state.

An attempt shows as `running` until its report is published. An attempt that ended without publishing one, because event publication failed or the process was killed, keeps showing as `running` until a newer attempt is observed. It did not finish its request, so the plan document does not record it. See [Attempt reports](../flow_api/overview.md#attempt-reports).

| Field | Content |
|---|---|
| `schema_version` | The snapshot's compatibility family: `1` in this release (see [The `plan_status` snapshot](document_contract.md#the-plan_status-snapshot)) |
| `type`, `realm`, `plan_id`, `scope`, `updated_at` | Identity of the plan, and when the snapshot was last written |
| `projection` | `"attempt"`, or `"legacy"` for a plan with no observed attempt, whose spool holds only events published before attempts were correlated. A legacy projection shows each step's latest run, which need not belong to one attempt |
| `attempt` | The attempt shown: `execution_id`, the `plan_generation` and `run_token` it captured, `failure_policy`, `execution_authority`, `execution_owner`, `started_at`, and `state` (`"running"` or `"finished"`). Once finished: `ended_at`, `termination_reason`, `outcome`, `is_drained`, `counts` per outcome plus `unreached` (every step without an outcome, interrupted ones included), and any attempt-level `diagnostic`, such as a preflight rejection. `null` for a legacy projection |
| `steps` | One entry per planned step, keyed by step ID: `step_name`, `state`, `outcome`, `run_id`, `fingerprint`, `progress`, `artifacts`, `metrics`, `job`, `ts`, `error` for a failed step, and `direct_blockers` and `failed_ancestors` for a blocked one |

A step's `state` is the event type that records its outcome: `step.succeeded`, `step.skipped` (reused), `step.failed` or `step.blocked`. While it runs, it is its latest lifecycle event. A step with no outcome is `pending` while the attempt runs. Once the attempt has ended, it is `interrupted` if it had started and `unreached` if it never did.

The snapshot's `plan_generation` is the one the attempt captured. It is not necessarily the stored plan's current generation, which only the plan document holds.

### Resetting the event spool

Snapshots are read from the attempt directories described in [Event spool layout](../flow_api/overview.md#event-spool-layout). Event files in the earlier layout, such as `<plan_id>/<execution_id>_plan_attempt_started.json` beside the plan's step directories, are not read, and they neither affect new attempts nor block them. To start from a clean spool, stop Yggdrasil first, then delete the spool's contents or point `YGG_EVENT_SPOOL` at a new directory. Nothing else needs resetting, and Yggdrasil deletes nothing itself:

- Plan documents, run tokens, work directories, artifacts and cache markers are unaffected. Clearing events does not request a rerun, and does not make a finished request run again.
- A plan's stored `plan_status` snapshot stays as it was until new events are consumed for that plan. Deleting a plan's event directory does not remove its snapshot.
- Deleting attempt directories erases what orders new attempts after a restart; see the limits in [Execution IDs and attempt order](../flow_api/overview.md#execution-ids-and-attempt-order).

---

## See also

- [Flow API](../flow_api/overview.md) — scheduling, failure policies, declared outputs and events
- [CLI](../getting_started/cli.md) — `run-doc --run-once` exit codes
- [Troubleshooting](troubleshooting.md) — plans that do not run, or do not run again
