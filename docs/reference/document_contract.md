# Document Contract

Yggdrasil's plan document and its `plan_status` snapshot are written and read by programs that are released and deployed separately: Yggdrasil's planning and execution, approval and rerun integrations, and interfaces that display plans. Their format is therefore a versioned contract. This page describes what each document holds, who may write which field, how versions work, what Yggdrasil refuses to act on, how to check a document, and how to approve or rerun a plan by hand safely.

This release provides:

- JSON Schemas for the plan document, the `plan_status` snapshot and the attempt report, and `validate` to check a document against them (see [Checking a document](#checking-a-document));
- `schema_version`, written on every plan document and snapshot that Yggdrasil builds;
- small runtime checks in `yggdrasil.contract`: the version reader (`versions.version_of`), the token rule (`controls.token_pair`), the failure-policy and execution-field checks (`controls`) and the canonical timestamp check (`timestamps`);
- their enforcement wherever Yggdrasil acts on a stored plan document: when planning replaces a plan's document, on both stores and in run-once too; when the daemon or run-once admits a plan for execution; and when a result is recorded.

It provides no library for reading these documents, and no helpers for approving or rerunning a plan. A program that reads or changes them applies the rules on this page itself. A direct `Engine.run(plan)` call receives a `Plan` model, not a stored document, so no version check applies to it.

---

## Documents in the contract

| Document | Store and ID | Written by | Read by | Schema |
|---|---|---|---|---|
| Plan document | `yggdrasil_plans` on CouchDB, or the `plans` namespace of the dev SQLite file; `_id` is the plan ID | Yggdrasil planning, which creates and regenerates it; Yggdrasil's finalizer, which records results; approval and rerun actors | PlanWatcher, the execution coordinator, the engine, approval integrations and interfaces | `plan_document`, family 1 |
| `plan_status` snapshot | `yggdrasil_ops` on CouchDB, or the `operations_snapshots` namespace of the dev SQLite file; `_id` is `<partition>:plan_status:<realm>:<plan_id>` | the ops consumer | interfaces that display execution status | `plan_status`, family 1 |
| Attempt report | Inside the plan document, as `last_finalized_execution.report`; also in the event spool, as the attempt's `plan.attempt_report` event | the engine | the finalizer, the ops consumer, interfaces | `attempt_report`, a component that the plan-document schema references. It has no `schema_version` of its own |

The snapshot does not embed the report. Its `attempt` copies six of the report's fields (`ended_at`, `termination_reason`, `outcome`, `is_drained`, `counts` and `diagnostic`), and its step entries are derived from the report's outcomes and failures.

These have no published shape and are not part of the contract: the event records in the spool, whose only reader is the ops consumer (the report a `plan.attempt_report` event carries is the attempt report above, but the event around it is not); watcher checkpoints and store metadata; the source documents that realms watch, such as project documents; work directories and their `success.fingerprint` markers; the SQLite file's table layout; and the plan summary that the plan stores return (`get_plan_summary`), which fills in display defaults for missing fields.

---

## The plan document

Family 1. Every family-1 plan document carries the required fields below. A missing, `null` or malformed one is a defect of the document, never a reason to assume a default. The fields that execution depends on are checked before Yggdrasil acts on a document (see [What Yggdrasil refuses](#what-yggdrasil-refuses)).

### Required fields

| Field | Content |
|---|---|
| `schema_version` | `1`, the document's compatibility family (see [Versions and compatibility](#versions-and-compatibility)) |
| `_id` | The plan ID |
| `realm` | The realm that planned it |
| `scope` | `{"kind": ..., "id": ...}`, both non-empty strings. Any other key is the realm's |
| `status` | `"draft"` or `"approved"`. Approval only, never an execution outcome |
| `plan` | The execution definition: `plan_id`, `realm`, `scope`, `steps` and `failure_policy`. Each step has `step_id`, `name`, `fn_ref`, `params`, `deps`, `scope`, `inputs` and `outputs`. Neither `plan` nor a step has any other key |
| `preview` | An object for display, defined by the realm; `{}` when it supplies none |
| `plan_generation` | A non-empty string naming this planned version of the plan. It is opaque: compare it only for equality |
| `run_token` | The latest execution request: an integer from 0 to 2^53 − 1 |
| `executed_run_token` | The latest request recorded as finished: an integer from −1 (none yet) to 2^53 − 1, never above `run_token` |
| `execution_authority` | Who may execute the plan: `"daemon"` or `"run_once"` |
| `execution_owner` | The run-once session that owns the plan, or `null` when there is none, as for every daemon plan. The key is always present |
| `created_at` | When the document was created, not the generation. Regenerating the plan keeps it |
| `updated_at` | When the document was last written, by any writer |

`_id` and `_rev` are the storage envelope. `_rev` is an opaque, non-empty string that the backend sets on every write, on CouchDB and SQLite alike. Compare it only for equality, and pass it back unchanged on a conditional write.

### Optional fields

| Field | Content |
|---|---|
| `source_doc_id`, `source_doc_rev` | The source document planning was given, if any |
| `notes` | The realm's notes on the plan |
| `last_executed_at`, `last_finalized_execution` | Written together by the finalizer, once a request is finished: neither appears without the other. `last_finalized_execution` holds `execution_id`, `plan_id`, `plan_generation`, `run_token`, `failure_policy`, `outcome`, `termination_reason`, `finalized_at` and the attempt's full `report` |
| `approved_at`, `approved_by` | The latest approval of this generation |
| `run_requested_at`, `run_requested_by` | The latest rerun request in this generation |
| `approvals_required` | An array of strings, for information only. Yggdrasil neither writes nor reads it |
| `x_<writer>_<name>` | A field of a writer outside the contract (see [Extension fields](#extension-fields)) |

An approved plan document as stored, with one step:

```json
{
  "_id": "pln_example",
  "_rev": "2-7d3f9e1c0b6a5d4e8f2a1b3c4d5e6f70",
  "schema_version": 1,
  "realm": "test_realm",
  "scope": {"kind": "project", "id": "P1"},
  "status": "approved",
  "plan": {
    "plan_id": "pln_example",
    "realm": "test_realm",
    "scope": {"kind": "project", "id": "P1"},
    "steps": [
      {
        "step_id": "echo",
        "name": "echo",
        "fn_ref": "yggdrasil.realms.test_realm.steps.step_echo",
        "params": {"message": "Hello"},
        "deps": [],
        "scope": {},
        "inputs": {},
        "outputs": {}
      }
    ],
    "failure_policy": "fail_fast"
  },
  "preview": {},
  "plan_generation": "19ba8ec8e02efe85c36b6a8b3f8c39fe",
  "run_token": 0,
  "executed_run_token": -1,
  "execution_authority": "daemon",
  "execution_owner": null,
  "created_at": "2026-10-22T09:00:00Z",
  "updated_at": "2026-10-22T09:30:00Z",
  "approved_at": "2026-10-22T09:30:00Z",
  "approved_by": "alice"
}
```

### Vocabularies

Values are matched exactly, letter case included. Any other value is unknown.

| Field | Values |
|---|---|
| `status` | `draft`, `approved` |
| `execution_authority` | `daemon`, `run_once` |
| `plan.failure_policy`, `last_finalized_execution.failure_policy` | `fail_fast`, `continue_independent` |
| `outcome` | `succeeded`, `failed` |
| `termination_reason` | `completed`, `failed_fast`, `cancelled`, `preflight_rejected`, `orchestration_error` |
| A step's outcome | `succeeded`, `reused`, `failed`, `blocked` |

`pending`, which earlier documentation described as a legacy status, is an unknown value like any other.

---

## The `plan_status` snapshot

Family 1. The ops consumer's projection of one plan's latest observed attempt. How it chooses that attempt, and what each step state means, is described in [Operational snapshots](plan_execution.md#operational-snapshots-plan_status).

| Field | Content |
|---|---|
| `schema_version` | `1` |
| `_id` | `<partition>:plan_status:<realm>:<plan_id>` |
| `type` | `"plan_status"` |
| `realm`, `plan_id` | The plan |
| `scope` | The scope the attempt's events carry, with `kind` and `id` |
| `projection` | `"attempt"`, or `"legacy"` for a plan whose spool holds no observed attempt |
| `attempt` | `null` for a legacy projection. Otherwise `execution_id`, `plan_generation`, `run_token`, `failure_policy`, `execution_authority`, `execution_owner`, `started_at` and `state` (`"running"` or `"finished"`), then the six fields copied from the report: `ended_at`, `termination_reason`, `outcome`, `is_drained`, `counts` and `diagnostic` |
| `steps` | One entry per step, keyed by step ID: `step_name`, `state`, `outcome`, `run_id`, `fingerprint`, `progress`, `artifacts`, `metrics`, `job`, `ts`, `error`, `direct_blockers` and `failed_ancestors` |
| `updated_at` | When the snapshot was last written |

A step's `state` is the type of the step event its entry was read from (`step.succeeded`, `step.skipped`, `step.failed`, `step.blocked`, or the latest event of a step still running). A step without one is `pending`, `interrupted` or `unreached`, or, in a legacy projection, `unknown`.

A step's `error` has two shapes. While the attempt runs, it is what the step's `step.failed` event carries: `error`, `kind`, `code` and `advice`. Once the attempt has ended, it is the report's failure record, which also holds `step_id` and `error_type`. `fingerprint`, `progress`, `artifacts`, `metrics`, `job` and `ts` are copied as the step's events carry them. `artifacts` is normally a list and `metrics` an object, both with realm-defined content. `ts` is the event's own time, not a contract timestamp.

`updated_at` is when the snapshot was last written. That is normally after its content changed, but a consumer that has just started writes every plan's snapshot once. So `updated_at` is not the time the content last changed, and it is no sign that the consumer or the daemon is running. When the consumer writes is not part of the contract.

---

## The attempt report

What one execution attempt did, as the engine reports it. The report is versioned through the plan document that embeds it, and has no `schema_version` of its own.

| Field | Content |
|---|---|
| `execution_id`, `plan_id`, `plan_generation`, `run_token`, `failure_policy` | The attempt's identity, as captured with its execution request |
| `started_at`, `ended_at` | When the attempt started and ended |
| `termination_reason`, `outcome`, `is_drained` | How it ended, and whether every step that could run did |
| `step_ids` | The plan's step IDs, in plan order |
| `step_outcomes` | The outcome of each step that reached one, by step ID |
| `unreached_step_ids` | The planned steps with no outcome, in plan order |
| `failures` | How each failed step failed: `step_id`, `error`, `kind`, `code`, `advice` and `error_type` |
| `direct_blockers`, `failed_ancestors` | For each blocked step, the prerequisites that blocked it, and every failed step upstream of it |
| `diagnostic` | An attempt-level failure that no single step accounts for, such as a preflight rejection: `message`, `error_type` and `details` |
| `publication_failure` | Why the report could not be published, in the same shape |
| `counts` | The number of steps per outcome, plus `unreached` |

The report describes what happened, including to plans that were malformed. `step_ids` and `unreached_step_ids` keep the plan's step IDs exactly as the plan gave them, so when preflight rejected a plan because a step ID was not a string, the report's entry is not a string either. `failure_policy` is the policy the attempt ran under as recorded, which preflight may have rejected. Such a report is valid even though the plan it describes is not.

---

## Null and opaque values

A `null` says something about its own field only. Depending on the field, it means the value is not available, has not been produced yet, or is legitimately absent. A `null` field alone does not prove that the record it comes from is missing, and a reader never invents a replacement value.

| Field | `null` means |
|---|---|
| Plan document `execution_owner` | The plan has no run-once owner, as for every daemon plan |
| Report `plan_generation`, `run_token` | The caller captured none, as in a direct `Engine.run` call |
| Report `diagnostic` | No attempt-level failure was recorded |
| Report `publication_failure` | No failure to publish the report was recorded |
| `error_type` of a failure or diagnostic | It did not come from an exception, such as a required output the step did not produce |
| Snapshot `attempt` | A legacy projection: no attempt was observed |
| Snapshot `attempt.plan_generation`, `run_token`, `failure_policy`, `started_at` | The record they are copied from lacks them: the attempt's start record, or its report when there is no start record |
| Snapshot `attempt.execution_authority` | The attempt's start record is missing |
| Snapshot `attempt.execution_owner` | The attempt has no run-once owner, or its start record is missing |
| Snapshot `attempt.ended_at`, `termination_reason`, `outcome`, `is_drained`, `counts` | No report is published yet: as far as the spool shows, the attempt is running |
| Snapshot `attempt.diagnostic` | The same, or, in a finished attempt, no attempt-level failure |
| Snapshot step `outcome` | The step has no outcome in this attempt |
| Snapshot step `run_id` | No event names an evaluation of the step, as for a blocked or unreached step |
| Snapshot step `error` | The step did not fail |
| Snapshot step `fingerprint`, `job`, `ts` | No event of the step carried one |

A failure's `code` and `advice` are opaque: they are whatever the realm's step error supplied, kept unchanged. Readers show them and draw no control conclusion from them. The same holds for the realm-defined payloads: `params`, `preview`, a step's `scope`, `metrics` and `artifacts`.

---

## Who writes which field

A plan document has two lifetimes. The **document** lives under one plan ID from its creation until it is deleted. A **generation** is one planned version of the plan, and ends when Yggdrasil regenerates the plan under the same ID.

For example, Alice approves generation A. Corrected inputs then make Yggdrasil regenerate the plan as generation B, under the same plan ID. B does not carry Alice's approval: Alice never saw B.

| Fields | Who may write them | When the plan is regenerated |
|---|---|---|
| `_id` | Yggdrasil, at creation | Kept |
| `_rev` | The storage backend | A new revision |
| `schema_version` | The Yggdrasil writer that builds the document | Written by the builder |
| `realm`, `scope`, `plan`, `preview`, `notes`, `source_doc_id`, `source_doc_rev`, `plan_generation` | Yggdrasil planning only | Rebuilt, with a new generation |
| `status` | Planning sets the first value: `"draft"`, or `"approved"` when the realm runs the plan automatically. The approval operation changes `"draft"` to `"approved"` | Set by planning |
| `approved_at`, `approved_by` | The approval operation | Removed |
| `run_token` | Planning sets 0; the rerun operation raises it by one | Reset to 0 |
| `run_requested_at`, `run_requested_by` | The rerun operation | Removed |
| `executed_run_token`, `last_finalized_execution`, `last_executed_at` | Yggdrasil's finalizer | `executed_run_token` reset to −1; the others removed |
| `execution_authority`, `execution_owner` | Yggdrasil: planning, and the run-once command | Set by planning |
| `created_at` | Yggdrasil, at the document's creation | Kept |
| `updated_at` | Every writer, on every write | Set |
| Any other field | – | Not kept |

The rules:

1. A writer that updates a document changes only its own fields, and keeps everything else exactly as read, fields it does not know included. It does not vouch for what it kept.
2. Regeneration keeps only `_id` and `created_at`. Fields that other writers added do not survive into the next generation. How `created_at` is kept is described in [When planning replaces a plan](#when-planning-replaces-a-plan).
3. Approval and rerun never change the plan definition, the generation, or `execution_authority` and `execution_owner`. Moving a plan from `run_once` to `daemon` authority is not a supported operation.
4. The actor fields record the latest action of each kind within a generation, not a history: Bob's rerun request replaces Alice's `run_requested_by`.
5. `created_at` is the document's creation, not the generation's.
6. Ownership defines the supported operations. The database does not enforce it.

**The execution definition** is what the plan document says will run: steps, parameters, inputs, outputs, dependencies and failure policy. Changing it requires a new generation, and therefore a new approval. Changing a step's `params.lanes` from `[1, 2]` to `[1, 2, 3]`, for example, needs a new generation. The code that implements the steps, and the environment it runs in, are not part of the definition in this contract: upgrading a realm package changes no field, so the generation and its approval stay.

### Extension fields

A field that a writer outside the contract needs, such as an operator's script, a hand edit in Fauxton or another team's tool, is named `x_<writer>_<name>`: for example `x_ops_ticket`. The contract never defines a field starting with `x_`, so such a field can never collide with a later contract field. Writers that are part of the contract get contract fields, with an owner and a lifetime, never extensions.

Nothing forces a foreign writer to use the prefix: CouchDB accepts any JSON, and nothing validates on the write path. A writer that ignores it takes the risk of a collision. The schema makes such a field visible: the plan document's top level allows only contract fields and `x_` names, so `validate` reports an unprefixed foreign field (`unexpected_field`). Update writers still keep such a field, as rule 1 says, and readers still ignore it. A regeneration drops extension fields like any other field that planning does not write.

---

## Versions and compatibility

`schema_version` is a positive integer per document kind (`plan_document`, `plan_status`). It names a **compatibility family**: documents with the same number can be read by the same reader. It is not a software version, and it does not change with every schema edit. The Yggdrasil writer that builds a document writes the number: planning for plan documents, the ops consumer for snapshots. Writers that update a document keep the number they read.

A document is in exactly one of four version states:

| State | `schema_version` | What it means |
|---|---|---|
| Supported | A family this release supports: `1` | The document is read under its family's rules |
| Unsupported | Another positive integer, such as `2` | A family this release does not know, such as one written by a newer release |
| Unversioned | Absent | A pre-contract document. It is never read as family 1. Planning may replace it with a new generation, which is how an old plan comes into the contract |
| Invalid | Anything else: `"1"`, `1.0`, `true`, `null`, `0`, `-1` | A damaged marker. Kept apart from unversioned, so a diagnostic can tell a pre-contract document from a damaged one. It never falls back to family 1 either |

`yggdrasil.contract.versions.version_of(kind, document)` returns the state, the family and the value as recorded, whatever the document holds.

**A compatible change keeps the number.** A change is compatible when a reader or writer that does not know about it can ignore it and still reach correct conclusions and take correct actions. A new optional `jobs` list in snapshot step entries, or a `job_label` on a step, would be compatible.

**An incompatible change raises the number:** removing or renaming a field, changing a type or a meaning, changing an ID scheme, or adding a field or value that changes whether a plan may run or may be approved. An optional field `requires_second_approval: true`, meant to hold execution until a second person approves, would be incompatible: a daemon that does not know it would run the plan after one approval. It needs a new family even though the field is optional.

**A document's family never changes within a generation.** Moving a plan to another family means writing a new generation, which needs a new approval. An approval, an execution request and a review all stay tied to one family.

**The attempt report has no number of its own.** Its shape is versioned through the plan document that embeds it. An incompatible change to the report raises the family of each document whose exposed shape or meaning changes: the plan document always, the snapshot only if a field it copies changes.

**Editions and families.** A Yggdrasil release identifies the exact schema edition and code; the family identifies compatibility. A released schema edition is never redefined. Within a family, a document written by an older release still validates against a newer release's schema, because compatible changes only add optional fields. That promise covers what Yggdrasil writes, not arbitrary extensions.

**What each kind supports in this release.** Every program that reads or writes these documents states, per kind, which families it supports for inspection, for changing a document, and for execution. For Yggdrasil in this release:

| Kind | Writes | Supports | Operations |
|---|---|---|---|
| `plan_document` | Family 1 | Family 1, for inspection, change and execution | Planning replaces it; the coordinator admits it; the finalizer updates it |
| `plan_status` | Family 1 | Family 1 | A projection the consumer rebuilds from the event spool. Nothing updates a snapshot in place or decides anything from one |
| `attempt_report` | As part of the plan document | Through the plan document | A component embedded in the plan document, and copied in part into the snapshot. It has no marker |

A document outside a program's supported families is **inspect-only** for that program. The program shows what it can, labelled with the document's version state and its recorded `schema_version`. It withholds every answer and action that depends on the document's meaning: for a plan, whether it is approved, whether a request is pending or finished, and approving or requesting a rerun. Supporting a new family neither requires keeping the old ones forever nor retires them. Retiring a family is an explicit release decision with a stated way to migrate or inspect such documents; a document of a retired family stays identifiable as unsupported and is never read as a current one.

---

## Reading the documents

**Producers are strict.** Yggdrasil's tests check what each of its writers produces against the schema of the same release. Objects that only Yggdrasil writes have closed key sets, so a misspelled key fails a test: the snapshot, `plan` and its steps, `last_finalized_execution` and the attempt report. The plan document's top level allows the contract's fields and any `x_` field, and nothing else. Realm-defined payloads are open: `params`, `preview`, a step's `scope`, and a snapshot step's `metrics` and `artifacts`.

Strict means honest, not idealized. The schemas describe what producers deliberately record: reports of plans that were malformed, of attempts that were cancelled or aborted by an orchestration error or whose report could not be published, and legacy projections.

**Readers are tolerant.** A reader does not use the schemas as a gate. It:

- checks the fields it needs for each answer, and only those;
- ignores fields it does not know;
- reads an unknown vocabulary value as unknown and keeps the recorded text: it never guesses `draft`, `daemon` or `failed`;
- treats an unversioned, invalid or unsupported document as inspect-only (see [Versions and compatibility](#versions-and-compatibility));
- lets a problem affect only the answers that depend on it: a malformed progress value does not hide a request's outcome, and an unsupported snapshot beside a supported plan does not block approving the plan.

Suppose a later release adds an optional field to snapshot step entries. A reader built for this release ignores it and shows everything else. Validating that snapshot against this release's closed schema would reject it, which is why readers do not validate.

**Control state is explicit.** A family-1 plan document carries every control field listed under [Required fields](#required-fields). A missing, `null` or malformed control field is invalid control state, never a default: there is no "an absent `run_token` means 0" and no "an absent failure policy means `fail_fast`". A reader keeps the diagnostic and refuses the conclusions and actions that depend on the field. The execution tokens follow one rule, the same everywhere, including in Yggdrasil's executor (see [When a plan is admitted for execution](#when-a-plan-is-admitted-for-execution)).

### Timestamps

Every timestamp that Yggdrasil writes in a plan document or a snapshot is an RFC 3339 date-time in UTC, to the whole second, ending in `Z`: `YYYY-MM-DDTHH:MM:SSZ`, such as `2026-10-22T10:00:00Z`. With one spelling per instant, two timestamps of the same instant are also equal as text. The schemas publish this spelling as a `pattern`, so any JSON Schema validator enforces it.

Earlier releases wrote plan-document timestamps ending in `+00:00`. Readers accept any valid RFC 3339 UTC form; writers produce only the canonical one.

---

## What Yggdrasil refuses

The version boundary is a small check of `schema_version` and of the control fields that execution depends on, not full schema validation. It applies wherever Yggdrasil acts on a stored plan document. Every refusal writes nothing: the stored document, its `_rev` and, on SQLite, the plan-change sequence are as they were. Other plans are not affected.

### When planning replaces a plan

Planning replaces the whole document in a plan's slot, on both stores: in the daemon, with `run-doc --plan-only`, and with `run-doc --run-once`. It may replace:

- an empty slot;
- an unversioned document, which it replaces with a family-1 document of a new generation;
- a document of a supported family.

It refuses a document whose `schema_version` is invalid or names a family this release does not support. A daemon that supports only family 1 does not replace a family-2 plan written by a newer release with an older family-1 one.

The replacement keeps the document's creation instant, `created_at`, spelled canonically:

- Over an unversioned document, a valid value that an earlier release wrote, a whole-second UTC time ending in `Z` or `+00:00`, is re-spelled with `Z`. The instant is the same.
- Over a supported document, the value must already be canonical, and it is kept as it is.
- Anything else is refused: a missing value, one that is not a real RFC 3339 date-time, a fraction of a second, another offset. The build time is never substituted, and precision is never truncated.

Planning checks no other timestamp, and eligibility, admission and finalization check none.

A refusal is logged at ERROR, once, and planning goes on with its other drafts:

```text
Planning refused to replace plan '<plan_id>': its schema_version 2 is unsupported (supported plan-document families: [1]). The draft was not persisted; the stored plan is unchanged.
```

The field is `schema_version` (`invalid`, `unsupported`) or `created_at` (`missing`, `malformed`, `unsupported`). `run-doc --force` overwrites an existing plan without asking, but it does not bypass this check. The slot stays as it is until an operator corrects or deletes the stored document (see [Plan not replaced: planning refused](troubleshooting.md#plan-not-replaced-planning-refused)).

### When a plan is admitted for execution

The daemon and run-once execute plans through one coordinator. It reads the plan document with one plain read that writes nothing, and takes every decision from that read, in this order:

1. The read returns the plan's stored document (see [When a read is not the stored document](#when-a-read-is-not-the-stored-document)). Otherwise: `ADMISSION_FAILED`, and the plan stays eligible.
2. **The family comes first.** A document whose `schema_version` is absent, invalid or unsupported is not executed: `UNSUPPORTED_DOCUMENT`, logged at WARNING. A pending result for the plan is dropped (see [When a result is recorded](#when-a-result-is-recorded)).
3. The plan exists (`NOT_FOUND`), and it is the caller's to execute: its `execution_authority` and `execution_owner` match the daemon or the run-once session (`NOT_AUTHORIZED`).
4. The plan is eligible: `status` is `"approved"`, its tokens are a valid pair, and `run_token` is greater than `executed_run_token` (`NOT_ELIGIBLE`).
5. `plan` and each of its steps carry every execution field, none of them `null`, and `plan.failure_policy` is `fail_fast` or `continue_independent`. These are checked before the plan is deserialized, so no default can stand in for a missing field. Otherwise: `INVALID_DOCUMENT`, logged at ERROR, naming each problem once.
6. The plan deserializes, embeds its own plan ID, and the document has a `plan_generation` that is a non-empty string (`INVALID_DOCUMENT`).

A refused request is allocated no execution ID: nothing is reserved, published or written, and there is no attempt and no report. A refusal changes nothing in the plan, which is checked again the next time it changes. Status, generation and authority fail closed too: an absent or unknown status is not approved, a missing generation is refused, and an unknown authority matches no caller.

**The token rule.** `run_token` and `executed_run_token` are a valid pair when:

- both are present JSON integers, not booleans;
- `run_token` is from 0 to 2^53 − 1 (9007199254740991);
- `executed_run_token` is from −1 to 2^53 − 1;
- `executed_run_token` is not above `run_token`.

Nothing is defaulted or converted: a missing token, `"5"`, `5.0` and `true` are all invalid. The upper bound keeps every token exactly representable in every JSON client, browsers included. `yggdrasil.contract.controls.token_pair(document)` applies the rule; eligibility, admission and finalization all use it.

An eligible plan reaches the coordinator whatever its `schema_version`, because eligibility judges only status and tokens. So every approved, eligible pre-contract plan is logged as `UNSUPPORTED_DOCUMENT` each time it is observed. A plan whose tokens are invalid is not eligible, and PlanWatcher skips it without handing it over (see [Plan is approved but Engine never runs it](troubleshooting.md#plan-is-statusapproved-but-engine-never-runs-it)).

**Refused, or rejected by preflight.** A refused request never starts an attempt, so it has no report. That includes every refusal above, and a plan whose realm or plan ID cannot name one directory, which the engine refuses before an attempt starts (`ADMISSION_FAILED` through the coordinator). A step graph that preflight rejects belongs to an attempt that has started: that attempt has a report, with `termination_reason: "preflight_rejected"` (see [Which endings finish a request](plan_execution.md#which-endings-finish-a-request)).

### When a result is recorded

Recording a finished request is an update: the finalizer changes only `executed_run_token`, `last_finalized_execution`, `last_executed_at` and `updated_at`. It keeps every other field exactly as read, including `schema_version`, fields it does not know, and a `run_token` that already names a newer request. It writes conditionally on the `_rev` it read. Before writing, and again on its reread after a lost race, it judges the document in this order:

1. **The family**, which must be supported and equal to the family captured at admission. Otherwise the result is dropped for good (`SUPERSEDED`, reason `unsupported_document`), even when the document already records this very completion.
2. The generation, which must equal the captured one. Otherwise: superseded (`generation_changed`), whatever the tokens say.
3. The stored tokens, which must be a valid pair. Otherwise: `INVALID_CONTROL_STATE`.
4. Whether the document already records this exact completion. If so, nothing needs writing.
5. The authority and owner, which must be unchanged. Otherwise: superseded (`authority_changed`).
6. Whether `executed_run_token` already reaches the captured token. If so: superseded (`request_already_finalized`).
7. The pair the write would produce, which must be valid too. Otherwise: `INVALID_CONTROL_STATE`. For example, request 5 finishes, while someone has edited the stored tokens to 4 / −1. That pair is valid, but recording 5 would leave 4 / 5.

`INVALID_CONTROL_STATE` is logged at ERROR. The result is not recorded, not dropped, and not retried until the document changes: it is kept as a **pending finalization**, and the plan is not executed again while it is pending (see [When a result cannot be recorded](plan_execution.md#when-a-result-cannot-be-recorded)). A pending result is:

- kept pending while the plan's tokens are not a valid pair, or recording the result would make them invalid, and through a failed storage read or one that is not the stored document;
- dropped, without writing, when the plan's document is no longer of the family the result was captured under. This is logged at WARNING with the execution ID; the result is never recorded and never retried, and its report stays in the event spool and in the snapshot;
- dropped when the plan is regenerated or deleted, and when its authority changes, though only once its tokens are valid;
- settled when its result turns out to be recorded already.

### When a read is not the stored document

The coordinator's read and the finalizer's reads accept what the store returns only when it is a JSON object whose `_id` is the requested plan ID and whose `_rev` is a non-empty string. Anything else, such as an object-shaped error response from a proxy, another document's body or a body without a revision, is a storage failure, never a missing or unversioned document: nothing is concluded from it. Only this envelope is checked, not the schema.

- At admission, the cycle ends as `ADMISSION_FAILED`. The plan stays eligible.
- In the finalizer, the read raises a `PlanStoreError` that is not retried, and the result stays `FINALIZATION_PENDING`.

A pending result survives either.

### What has no version check

- **Snapshots.** The sinks that store them check no version. A snapshot is a projection the consumer can always rebuild from the event spool, so an outdated one is replaced the next time the consumer writes that plan's snapshot.
- **Deleting a plan** is an explicit operator action.
- **A direct `Engine.run(plan)` call** receives a `Plan` model, not a stored document.

### Log lines

| Situation | Level | Message |
|---|---|---|
| Planning refused to replace a plan | ERROR | `Planning refused to replace plan '<plan_id>': its <field> <value> is <reason> (supported plan-document families: [1]). The draft was not persisted; the stored plan is unchanged.` |
| A plan of no supported family is observed | WARNING | `Plan '<plan_id>' is not executed: its schema_version is <value>, so it is <state> (supported plan-document families: [1]); nothing ran and nothing was written`. `<value>` is `absent` for an unversioned document |
| Its pending result is dropped | WARNING | `Dropping the pending finalization of execution '<execution_id>': the schema_version of plan '<plan_id>' is now <value>, so it is <state> (supported plan-document families: [1]). The result is not recorded and is not retried; its report remains in the event spool` |
| A missing execution field, or an unknown failure policy | ERROR | `Plan '<plan_id>' does not describe an executable request (<problems>); cannot execute`, with problems such as `plan.steps[1].deps is missing` or `plan.failure_policy 'Fail_Fast' is not one of ['continue_independent', 'fail_fast']` |
| Invalid tokens, seen by PlanWatcher | DEBUG | `Skipping ineligible plan: <plan_id> (Not eligible: invalid tokens (<problems>))`, with problems such as `run_token '5' is a str, not a JSON integer` |
| Invalid tokens at finalization | ERROR | `Cannot finalize execution '<execution_id>' of plan '<plan_id>' (...): the plan's tokens are not a valid pair (<problems>); nothing was written, and the result cannot be recorded until the plan's tokens change`, or `... would leave run_token <n> / executed_run_token <m>, which is not a valid pair (...)` |
| A read that is not the stored document, at admission | ERROR | `Could not read plan '<plan_id>' to admit it for execution (PlanStoreError: The read of plan '<plan_id>' returned <what was read>, which is not the plan's stored document; the plan's stored state is unknown); it stays eligible` |

---

## Checking a document

`validate` checks a document against the published schema of its kind and family. It is a development and test tool: no runtime path calls it, and readers never use it as a gate. It needs the `jsonschema` package from the `dev` extra (`pip install -e ".[dev]"`), which it imports only when it is called.

```python
from yggdrasil.contract.schema import ContractError, validate

try:
    validate("plan_document", document)
except ContractError as error:
    for problem in error.problems:
        print(problem.path, problem.code, problem.message)
```

The kinds are `plan_document`, `plan_status` and `attempt_report`. Validate the stored form, as read from the store, with `_id` and `_rev`.

For a plan document or a snapshot, `validate` reads `schema_version` first. An unversioned, invalid or unsupported document has no schema to check it against, so it gets a single problem, at `schema_version`, with the code `unversioned`, `invalid_version` or `unsupported_version`. A supported family selects its schema. An attempt report is checked against the family that this release's plan documents embed.

`ContractError.problems` lists every problem, once per path and code, sorted by path. Each has a `path` such as `plan.steps[0].deps` (a key that is not a plain name is quoted: `steps["lane_1/demux"].state`), a stable `code`, and a `message` showing the recorded value, shortened. The codes are `unversioned`, `invalid_version`, `unsupported_version`, `missing_field`, `unexpected_field`, `wrong_type`, `not_in_vocabulary`, `out_of_range`, `bad_timestamp`, `invalid_tokens`, and `schema` for any other rule of the schema. A plan document with an unprefixed foreign field, a token stored as a string and a `+00:00` timestamp gives:

```text
The plan_document does not conform to its contract (3 problems):
  [wrong_type] run_token '1' is a string, not an integer
  [unexpected_field] ticket is not a contract field, and its name matches none of ['^x_']
  [bad_timestamp] updated_at '2026-10-22T09:30:00+00:00' is not a timestamp spelled canonically: whole seconds, in UTC, ending in Z, as in 2026-10-22T10:00:00Z
```

Besides the schema, `validate` checks what JSON Schema cannot express: an integer is a JSON integer, never `1.0` or `true`; a timestamp names a date and time that exist; and a plan document's `executed_run_token` is not above its `run_token`. It checks no rule across fields or documents, such as whether a recorded execution matches the report it carries.

The schemas themselves are JSON Schema draft 2020-12, in [`yggdrasil/contract/schemas/`](https://github.com/NationalGenomicsInfrastructure/Yggdrasil/tree/main/yggdrasil/contract/schemas), and `yggdrasil.contract.schema.schema_for(kind, family)` returns a copy of one. Each has an `$id` naming its kind and family, such as `urn:yggdrasil:contract:attempt_report:1`, and the plan-document and snapshot schemas refer to the report's by that `$id`. Another validator therefore needs all three registered, and no reference needs a download. Any validator enforces the timestamp `pattern`. Checking that a date and time exist needs format assertion switched on, which `validate` does. A plain JSON Schema `integer` also accepts `1.0`, which `validate` rejects.

**Worked examples.** [`tests/contract/documents/current/`](https://github.com/NationalGenomicsInfrastructure/Yggdrasil/tree/main/tests/contract/documents/current) holds real plan documents and snapshots in their stored form, built by this release's writers. Its `manifest.json` gives each one's scenario, how it was made, and its expectation: valid, or the exact problems `validate` reports. Each tagged release adds its positive examples as a frozen baseline under `released/<tag>/`, and later releases must keep them valid against their family's schema. How a baseline is prepared is described in the corpus [README](https://github.com/NationalGenomicsInfrastructure/Yggdrasil/blob/main/tests/contract/documents/README.md#released-frozen-baselines).

---

## Approving or rerunning by hand

Yggdrasil ships no approval or rerun command. An integration that approves a plan or requests a rerun writes the plan document itself, and does it as follows.

1. **Read the document and check it.** Require a supported `schema_version` (`1` in this release) and a usable `plan_generation` (a non-empty string), both equal to the ones that were reviewed. Refuse invalid control state rather than supplying a default: an approval needs `status` to be `"draft"`, and a rerun needs a valid token pair. Full schema validation is not a gate for reading or updating a document.
2. **Change only the permitted fields.**
    - An approval sets `status` from `"draft"` to `"approved"`, with `approved_at` and `approved_by`. It leaves both tokens as they are: a new plan's 0 / −1 already allows it to run once it is approved.
    - A rerun request sets `run_token` to the value you read plus one, with `run_requested_at` and `run_requested_by`. It needs room to increment: refuse when `run_token` is already 2^53 − 1.
    - Both set `updated_at`. Every timestamp is canonical.
    - Keep everything else exactly as read. Never touch `schema_version`, the definition (`plan`), `plan_generation`, `execution_authority` or `execution_owner`, `created_at`, or the finalizer's fields (`executed_run_token`, `last_finalized_execution`, `last_executed_at`). Fields of your own are `x_<writer>_<name>`.
3. **Write conditionally on the `_rev` you read.** On CouchDB, write the whole document with its `_rev`; a `409 Conflict` means it changed. On the dev SQLite store, write with `SQLiteInternalStore.put_document("plans", plan_id, document, bump_plan_seq=True, expected_rev=<the _rev you read>)`, which also advances the plan-change sequence in the same transaction, without which PlanWatcher never observes the change (see **Manual plan approval** in [Configuration](../getting_started/configuration.md#internal-storage)). A `RevisionConflictError` means it changed.
4. **On a conflict, read again and check again before retrying.** Recheck that the family is supported and still the one reviewed, that the generation is the one reviewed, and the control state the action depends on: `status` for an approval, the token pair for a rerun. Keep the target token fixed from the reviewed state: if a rerun's retry finds `run_token` already moved, stop rather than raise it again. An approval's retry that finds the generation already approved stops too. Seeing a request or an approval does not establish who made it.
5. **Act only on documents in the contract.** Do not approve or request a rerun on an unversioned, invalid-version or unsupported-version document. Regenerate a wanted pre-contract plan through its realm (see [Upgrading and rolling back](#upgrading-and-rolling-back)). A newer, unsupported family needs software that supports it, not an edited marker.

A rerun request against the dev SQLite store, for example:

```python
from datetime import UTC, datetime

from yggdrasil.contract.controls import MAX_TOKEN, token_pair
from yggdrasil.contract.timestamps import CANONICAL_FORMAT
from yggdrasil.contract.versions import SUPPORTED, version_of
from yggdrasil.storage.errors import RevisionConflictError


def request_rerun(store, plan_id, reviewed, actor):
    """Raise run_token by one from the reviewed state; return why not, or None."""
    for _ in range(3):
        doc = store.get_document("plans", plan_id)
        if doc is None:
            return "the plan no longer exists"
        version = version_of("plan_document", doc)
        if version.state != SUPPORTED or version.family != reviewed["schema_version"]:
            return "the document is not of the reviewed family"
        if doc.get("plan_generation") != reviewed["plan_generation"]:
            return "the plan was regenerated: review the new generation"
        tokens = token_pair(doc)
        if not tokens.valid:
            return "the tokens are not a valid pair: " + "; ".join(tokens.problems)
        if tokens.run != reviewed["run_token"]:
            return f"run_token has moved to {tokens.run}: not raising it again"
        if tokens.run == MAX_TOKEN:
            return "run_token cannot be raised any further"
        now = datetime.now(UTC).strftime(CANONICAL_FORMAT)
        changed = {
            **doc,
            "run_token": tokens.run + 1,
            "run_requested_at": now,
            "run_requested_by": actor,
            "updated_at": now,
        }
        try:
            store.put_document(
                "plans", plan_id, changed, bump_plan_seq=True, expected_rev=doc["_rev"]
            )
            return None
        except RevisionConflictError:
            continue  # the plan changed since it was read: check it again
    return "the plan kept changing"
```

Here `store` is the `SQLiteInternalStore` of the dev database, and `reviewed` holds the `schema_version`, `plan_generation` and `run_token` that were reviewed. An approval follows the same shape, checking `status` instead of the tokens.

---

## Upgrading and rolling back

Upgrading changes no stored document. Documents written by earlier releases carry no `schema_version`, so they stay unversioned: Yggdrasil refuses to execute or update them until their realm regenerates them, and nothing stamps them. A document that already carries family 1 keeps running.

1. Upgrade every daemon and every host that runs `run-doc --run-once`.
2. List the store's unversioned, invalid and unsupported documents (below).
3. Regenerate the wanted plans through their realm. Planning replaces an unversioned slot with a family-1 document of a new generation, and keeps its `created_at` as described in [When planning replaces a plan](#when-planning-replaces-a-plan). Like any new generation, it needs a new approval unless its realm runs it automatically. Disposable development state can be reset instead: the dev SQLite file can be deleted.
4. Upgrade the programs that read these documents and apply the version rules last. Before the plans are regenerated, they would show every plan as unversioned.

Snapshots need nothing. Every snapshot's content changes with this release, since each one now carries `schema_version`, so the ops consumer writes every plan's snapshot again on its first cycle after the upgrade. Nothing is stamped or deleted automatically.

To list a store's documents by version state:

```python
from collections import Counter

from yggdrasil.contract.diagnostics import describe_recorded
from yggdrasil.contract.versions import SUPPORTED, version_of


def report_versions(kind, documents):
    """Print each document whose version is not supported, then a count per state."""
    states = Counter()
    for document in documents:
        version = version_of(kind, document)
        states[version.state] += 1
        if version.state != SUPPORTED:
            print(
                f"{version.state:11} {describe_recorded(document.get('_id'))} "
                f"schema_version={describe_recorded(version.recorded)}"
            )
    print(kind, dict(states))
```

On the dev SQLite store, pass it the documents of the `plans` and `operations_snapshots` namespaces. The path must name the existing file, since opening a missing one creates an empty store:

```python
from pathlib import Path

from yggdrasil.storage.sqlite import SQLiteInternalStore

path = Path("/path/to/yggdrasil.sqlite3")
assert path.is_file(), path
store = SQLiteInternalStore(path)
report_versions("plan_document", store.list_documents("plans"))
report_versions("plan_status", store.list_documents("operations_snapshots"))
```

On CouchDB, pass it the `doc` of each row of `_all_docs?include_docs=true` in `yggdrasil_plans`, and in `yggdrasil_ops` those whose `type` is `"plan_status"`, leaving out design documents (`_design/...`).

**Rolling back** to a release from before the contract is tolerable only while every stored document is family 1. Such a release ignores `schema_version` and executes family-1 documents correctly, but the documents it regenerates come out unversioned, and need regenerating again after rolling forward. Once any document of family 2 exists, rolling back to a release without the version boundary is not allowed.

Events and the full report in the event spool have no published schema, but the engine that writes them and the ops consumer that reads them must agree. Do not run them across an incompatible change of what events or reports mean, and do not roll back across retained spool data of a newer meaning.

---

## See also

- [Plan Execution](plan_execution.md) — eligibility, admission, finalization and snapshots
- [Troubleshooting](troubleshooting.md) — plans that do not run, refusals and pending results
- [Configuration](../getting_started/configuration.md#internal-storage) — the plan stores, and manual plan approval
- [Glossary](glossary.md#document-contract) — contract terms
