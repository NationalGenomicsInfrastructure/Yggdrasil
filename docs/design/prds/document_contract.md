# Yggdrasil – Document Contract: Schemas, Versioning and Compatibility (PRD)

## Version
v0.9 (2026-10-22): synchronized with the owner's decisions taken while
planning the implementation. Bifrost's adoption is separate work (9.4, 13,
D1). `created_at` when planning replaces a document (section 4 rule 2, 7.1,
D13). A storage envelope for the executor's and the finalizer's reads (7.2).
The canonical timestamp spelling published as a schema `pattern` (6.1). A
regression corpus owned by Yggdrasil that keeps released examples (9.2
item 8).

v0.8 (2026-10-22): standalone presentation and named references to related
PRDs; companion draft status clarified. Requirements are unchanged from v0.7.

Existing-behavior claims were verified against Yggdrasil `v0.5.0`
(000d596) and `dev` 418c96d, and Bifrost `main` (38c09cd). That `dev`
revision differs from `v0.5.0` only in the ops consumer (PR #110). The
implementation started from that revision, on `feature/document_contract`.

## Status
**Implemented and reviewed (2026-10-29).** Accepted by the owner
(2026-10-21). Every decision in section 14 is taken. All requirements for
this release are stated here; historical design notes and reviews are not
required inputs. Section 9.0 lists the verification work done while the
schemas were written. Other PRD titles identify follow-on or deferred work,
not additional requirements for this release.

This PRD approves the foundation and does not approve its companion
capabilities. **Document Contract: Shared Reader** proposes one that builds
on this foundation, and **Document Contract: Approve and Rerun Helpers** one
that builds on both. Each needs its own owner review and explicit approval
before implementation, and completing a dependency does not approve the
next. The dependency order is not a numbering scheme or a global roadmap
order.

| PRD | Delivers | Needed by |
| --- | --- | --- |
| **Document Contract: Schemas, Versioning and Compatibility (this PRD)** | Schemas, `schema_version`, compatibility and reading policy, field ownership, the version boundary for every Yggdrasil writer and the executor, the executor's control checks (one strict token rule), the reference page | everything below; any future incompatible document |
| **Document Contract: Shared Reader** | Proposed shared reader `yggdrasil.contract.read`: one set of reading rules for Yggdrasil and Bifrost, reusing this PRD's control checks | Bifrost adapter swap |
| **Document Contract: Approve and Rerun Helpers** | Proposed approve and rerun helpers `yggdrasil.contract.actions`: pure functions that prepare a safe conditional write | **Bifrost: Write Actions and Authentication** |

## 1. Overview

Yggdrasil's plan document is its API. Realms create it, operators approve
and rerun by changing it, the daemon executes what it says and writes the
result back, and Bifrost displays it. The programs on each side are released
and deployed separately, so the document format is the contract between
them. This PRD makes that contract explicit and versioned, and makes every
Yggdrasil writer refuse to act on a document it does not understand.

This first release:

- publishes JSON Schemas for the plan document, the `plan_status` snapshot
  and the attempt report component;
- writes an integer `schema_version` on every new plan document and
  snapshot;
- defines which writer owns which field, what counts as a compatible change,
  and what a reader does with a document it only partly understands;
- adds a version boundary: Yggdrasil executes, finalizes and replaces only
  documents of a family it supports, and refuses everything else without
  writing to it;
- removes two write paths that bypass all checks;
- replaces `docs/GENSTAT_PLAN_CONTRACT.md` with
  `docs/reference/document_contract.md`.

Documents written before this release carry no version. They are outside the
contract (owner, 2026-10-20): they can be inspected, and a realm can
regenerate them, but nothing interprets, executes or updates them.

## 2. Problem Statement

Verified at `v0.5.0`:

- **Two copies of the rules.** Bifrost's adapter
  (`backend/src/bifrost/yggdrasil/`, about 1,000 lines) re-implements
  `docs/reference/plan_execution.md`. Moving the pin to `v0.5.0` needed a
  hand review of eight modules to confirm that the rules had not changed.
- **The copies already disagree.** A plan document with `"run_token": "5"`
  is eligible to Yggdrasil, which calls `int(value)` in five places, and
  unreadable to Bifrost, which accepts only JSON integers. The same holds for
  `true` (Yggdrasil reads 1) and `4.7` (Yggdrasil reads 4). This release makes
  Yggdrasil strict (section 7.2); **Document Contract: Shared Reader** gives
  Bifrost the same function.
- **The only written protocol for changing plan documents is unsafe.**
  `docs/GENSTAT_PLAN_CONTRACT.md` (v1.1, 2026-01-28) describes fields that do
  not exist, gives two different rerun rules ("set `run_token` to
  `executed_run_token + 1`" and "increment by 1", which differ whenever a
  request is unfinished), re-applies an approval after a conflict to whatever
  the refetch returns (possibly a regenerated plan nobody reviewed), and lists
  adoption (`run_once` to `daemon`) as a UI action. Genstat is not an active
  consumer (owner, 2026-10).
- **Every write path overwrites whatever it finds.** Planning
  (`save_plan`, both stores) replaces the existing document in a slot.
  Generation initialization (`ensure_plan_generation`) writes during the
  coordinator's read, before admission. Finalization checks generation,
  authority and tokens, not what kind of document it writes into.
  `update_executed_token` (both stores) writes the executed token with no
  check at all, and nothing in Yggdrasil calls it.
- **Three writers share the plan document:** Yggdrasil, the approver and the
  rerun requester. Yggdrasil validates nothing an actor writes beyond the
  eligibility rule.
- **Timestamps differ by writer:** plan documents end in `+00:00`, events and
  snapshots in `Z`.
- **Shape changes are queued:** job records in snapshot steps
  (**Named Connections and Step Toolkit v1**), the lease document
  (**Daemon Lease, Heartbeat and Recovery Sweep**) with realm sets
  (**Realm Loading Policy: Selection and Failure Handling**), the
  dynamic-planning plan document.
- **No document names its version,** so nothing can stop an old daemon from
  executing a future document it does not understand. A marker alone cannot
  do that: the check has to be in every writer and in the executor, and it
  has to be deployed before the first incompatible document exists.

## 3. Documents in the Contract

| Document | Store and ID | Written by | Read by | Schema in this release |
| --- | --- | --- | --- | --- |
| Plan document | `yggdrasil_plans` (CouchDB) or the SQLite `plans` namespace; `_id` = plan ID | `build_plan_document` (create, regenerate); finalization; approval and rerun actors | PlanWatcher, coordinator, engine, Bifrost | `plan_document`, family 1 |
| `plan_status` snapshot | `yggdrasil_ops` (CouchDB) or the SQLite `operations_snapshots` namespace; `{partition}:plan_status:{realm}:{plan_id}` | ops consumer | Bifrost | `plan_status`, family 1 |
| Attempt report | full report in the plan document's `last_finalized_execution.report` and in the internal `plan.attempt_report` event | engine (`AttemptReport.to_dict`) | finalizer, ops consumer, Bifrost | `attempt_report`, a shared component referenced by the plan-document schema; no marker of its own |
| Daemon lease | `yggdrasil`; `daemon_lease:<mode>` | daemon (**Daemon Lease, Heartbeat and Recovery Sweep**) | Bifrost | authored with the **Daemon Lease, Heartbeat and Recovery Sweep** PRD under these rules |
| Event records | file spool; CouchDB with `CouchEmitter` | engine, step wrapper, step context, DataAccess traces | the ops consumer only | none: no reader outside Yggdrasil |

The snapshot does not embed the report. It copies six report fields into its
attempt summary (`ended_at`, `termination_reason`, `outcome`, `is_drained`,
`counts`, `diagnostic`) and derives step entries from the report's outcomes
and failures (`ops/snapshot.py`, `project_attempt`).

Not in the contract: watcher checkpoints and `store_metadata`, external
source documents (`projects`, flowcell documents), work directories and
`success.fingerprint` markers, the SQLite table layout. The plan summary
(`plan_summary_from_document`) is a return value; it moves into the shared
reader with **Document Contract: Shared Reader**.

### 3.1 Plan document, family 1

**Required** (every family-1 document has them; a document missing one has
invalid control state): `schema_version` (1), `_id`, `realm`, `scope {kind,
id}`, `status` (`draft` | `approved`), `plan` (`plan_id`, `realm`, `scope`,
`steps[]` of `step_id`, `name`, `fn_ref`, `params`, `deps`, `scope`,
`inputs`, `outputs`; `failure_policy`), `preview` (object, realm-defined),
`plan_generation` (non-empty string), `run_token`, `executed_run_token`,
`execution_authority` (`daemon` | `run_once`), `execution_owner` (string or
null; the key is present), `created_at`, `updated_at`.

**Optional:** `source_doc_id`, `source_doc_rev`, `notes`; after a finished
request, `last_executed_at` and `last_finalized_execution` (`execution_id`,
`plan_id`, `plan_generation`, `run_token`, `failure_policy`, `outcome`,
`termination_reason`, `finalized_at`, `report`); the actor fields
`approved_at`, `approved_by`, `run_requested_at`, `run_requested_by`;
`approvals_required` (array of strings, informational; Yggdrasil does not
write it).

The storage envelope `_rev` is an opaque string on both backends.

Vocabularies: `status`, `execution_authority`, `failure_policy`
(`fail_fast` | `continue_independent`), `outcome` (`succeeded` | `failed`),
`termination_reason` (`completed`, `failed_fast`, `cancelled`,
`preflight_rejected`, `orchestration_error`), step outcomes (`succeeded`,
`reused`, `failed`, `blocked`).

### 3.2 `plan_status`, family 1

`schema_version` (1), `_id`, `type: "plan_status"`, `realm`, `plan_id`,
`scope`, `projection` (`attempt` | `legacy`), `attempt` (null for `legacy`;
otherwise `execution_id`, `plan_generation`, `run_token`, `failure_policy`,
`execution_authority`, `execution_owner`, `started_at`, `state` (`running` |
`finished`), `ended_at`, `termination_reason`, `outcome`, `is_drained`,
`counts`, `diagnostic`), `steps` keyed by step ID, `updated_at`.

A step entry has `step_name`, `state`, `outcome`, `run_id`, `fingerprint`,
`progress`, `artifacts` (a list by default; realm-defined content), `metrics`
(realm-defined), `job`, `ts`, `error`, `direct_blockers`,
`failed_ancestors`. `error` has two shapes: from a live `step.failed` event
it holds `error`, `kind`, `code` and `advice`; from a finished attempt it is
the report's failure record, which also holds `step_id` and `error_type`.
The **Named Connections and Step Toolkit v1** PRD later adds `jobs[]` per
step, a compatible addition.

The snapshot's `updated_at` is the time assigned to the last persisted
update of the projection: normally after its content changed, but also
refreshed when a consumer starts and writes every plan again (`dev` 418c96d,
PR #110). It is not the time the content last changed, and not a sign that
the consumer or the daemon is running (see the **Daemon Lease, Heartbeat and
Recovery Sweep** PRD). When the consumer writes is not part of the contract:
PR #110 is an interim fix, and
the consumer may be refactored or replaced.

### 3.3 Attempt report

`AttemptReport.to_dict`: identity (`execution_id`, `plan_id`,
`plan_generation`, `run_token`, `failure_policy`), `started_at`, `ended_at`,
`termination_reason`, `outcome`, `is_drained`, `step_ids`, `step_outcomes`,
`unreached_step_ids`, `failures`, `direct_blockers`, `failed_ancestors`,
`diagnostic`, `publication_failure`, `counts`.

The report describes what happened, including to plans that were malformed.
`step_ids` and `unreached_step_ids` keep the plan's step IDs exactly as
given, so an entry may not be a string when preflight rejected the plan
(`flow/outcomes.py`, `unreached_step_ids`); `failure_policy` may be the raw
recorded value. The schema allows that (section 6).

## 4. Field Ownership and Lifecycle

A plan document has two lifetimes. The **document** lives under one plan ID
from creation until deletion. A **generation** is one planned version of the
plan and ends when Yggdrasil regenerates the plan under the same ID.

Example: Alice approves generation A. Corrected inputs make Yggdrasil
regenerate the plan as generation B, under the same plan ID. B must not carry
Alice's approval: Alice never saw B.

| Fields | Who may write them | On regeneration (same ID, new generation) |
| --- | --- | --- |
| `_id` | Yggdrasil, at creation | kept |
| `_rev` | the storage backend | new revision |
| `schema_version` | the Yggdrasil writer that builds the document | written by the builder |
| `realm`, `scope`, `plan`, `preview`, `notes`, `source_doc_id`, `source_doc_rev`, `plan_generation` | Yggdrasil planning (`build_plan_document`) only | rebuilt; new generation |
| `status` | planning sets the first value (`draft`, or `approved` when the realm auto-runs); the approval operation changes `draft` to `approved` | set by planning |
| `approved_at`, `approved_by` | the approval operation | removed |
| `run_token` | planning sets 0; the rerun operation raises it by one | reset to 0 |
| `run_requested_at`, `run_requested_by` | the rerun operation | removed |
| `executed_run_token`, `last_finalized_execution`, `last_executed_at` | Yggdrasil's finalizer | reset to -1; others removed |
| `execution_authority`, `execution_owner` | Yggdrasil (planning, the run-once CLI) | set by planning |
| `created_at` | Yggdrasil, at document creation | kept |
| `updated_at` | every writer, on every write | set |
| any other field | – | not kept |

Rules:

1. A writer that updates a document changes only its own fields and keeps
   everything else exactly as read, including fields it does not know. It
   does not certify what it kept (section 5, rule 9).
2. Regeneration keeps only `_id` and `created_at`. Fields added by others do
   not survive into the next generation. `created_at` keeps its instant in
   the canonical spelling (D5). A supported predecessor's value must already
   be canonical. When planning replaces an unversioned document, a valid
   legacy value (a whole-second UTC time ending in `Z` or `+00:00`) is
   re-spelled with `Z`. A missing, malformed or other value is refused without
   writing (section 7.1): the build time is never substituted and precision is
   never truncated. This is a planning rule only; eligibility, admission and
   finalization check no timestamps.
3. Approval and rerun never change the plan definition, the generation or
   `execution_authority`/`execution_owner`. Adoption (moving a plan from
   `run_once` to `daemon`) is not a supported operation and is out of scope.
4. The actor stamps record the **latest** action of each kind within a
   generation, not a history: Bob's rerun replaces Alice's
   `run_requested_by`. A durable action history is out of scope.
5. `created_at` is the document's creation, not the generation's.
6. Ownership defines the supported operations; the database does not
   enforce it.
7. **Extension fields.** A field a writer needs is defined in the contract
   when we control the writer: Bifrost's fields, for example, become schema
   fields with an owner and a lifetime, not extensions. Extensions are for
   writers we do not control (someone's script, a hand edit in Fauxton,
   another team's tool). For those, the contract promises never to define a
   field starting with `x_`, so a field named `x_<writer>_<name>` (for example
   `x_ops_ticket`) can never collide with a later contract field. Nothing can
   force a foreign writer to use the prefix, since CouchDB accepts any JSON and
   nothing validates on the write path; a writer that ignores it carries the
   collision risk. The schema makes violations visible: the plan document's
   top level allows only contract fields and `x_` names, so `validate` flags
   an unprefixed foreign field in a stored document. Update writers still
   keep such a field (rule 1), and readers still ignore it.

**What "execution definition" means.** The execution definition is what the
plan document says: steps, parameters, inputs, outputs, dependencies and
failure policy. Changing it requires a new generation, and therefore a new
approval. Example: changing a step's `params.lanes` from `[1, 2]` to
`[1, 2, 3]` needs a new generation. The code that implements the steps and
the environment it runs in are not part of the definition **in this
contract**: upgrading demux_realm from 1.2 to 1.3 changes no field, so the
generation and its approval stay. This describes the current boundary, not a
permanent product decision. Whether results from 1.2 may be reused under 1.3
is covered by **Code Identity in Fingerprints**; whether approval should be
bound to code or environment is covered by **Execution Context and
Upgrades**, and binding it would be an incompatible contract change.

## 5. Versioning

1. `schema_version` is a positive integer per document kind
   (`plan_document`, `plan_status`, later `daemon_lease`). It names a
   **compatibility family**: documents with the same number can be read by
   the same reader. It is not a software version, and it does not change for
   every schema edit.
2. The Yggdrasil writer that builds a document writes the number:
   `build_plan_document` for plan documents, the ops consumer's snapshot
   builder for snapshots. Writers that update a document keep the number
   they read.
3. **Absent** means unversioned: a pre-contract document. It is never read
   as family 1. Readers keep it visible for inspection and draw no control
   conclusions from it; Yggdrasil never executes or updates it. Planning may
   replace it with a new generation (section 7.1), which is how an old plan
   comes into the contract. Nothing stamps old documents silently.
4. **Present but not a positive integer** (`"1"`, `1.0`, `true`, `null`,
   `0`, `-1`) is an invalid version. Missing and invalid are separate
   diagnostic conditions; neither falls back to family 1.
5. A **compatible** change keeps the number. A change is compatible when a
   reader or writer that does not know about it can ignore it and still reach
   correct conclusions and take correct actions. Examples: a new optional
   field `jobs[]` in snapshot steps; a `job_label` on a step.
6. An **incompatible** change raises the number: removing or renaming a
   field, changing a type or a meaning, changing an ID scheme, or adding a
   field or value that changes whether a plan may run or may be approved.
   Example: an optional field `requires_second_approval: true`, meant to hold
   execution until a second person approves. An old daemon would ignore it
   and run the plan after one approval, so it needs family 2 even though the
   field is optional.
7. **A document's family never changes within a generation.** Moving a plan
   to another family means writing a new generation, which needs a new
   approval. This keeps an approval, a captured execution request and a
   review context all tied to one family.
8. The attempt report has no number of its own. Its full shape is versioned
   through the plan document that embeds it. An incompatible report change
   raises the family of each public document whose exposed shape or meaning
   changes: the plan document always, the snapshot only if a projected field
   changes. The internal event that carries the report has no public schema,
   but its producer and consumer must still agree (section 11).
9. **Builders and updaters promise different things.** A
   builder's complete output conforms to its release's schema; producer
   tests check that, never the write path. An update writer makes changes to
   the fields it owns that conform to the family the document carries, and
   keeps everything else as read; the update does not certify what it kept.
   Example: the finalizer records a result in a document that also carries a
   colleague's unprefixed `ticket` field, or an optional field added by a
   newer release. The finalizer's own fields conform; the kept field is not
   its business, and `validate` may still report it. If an update writer
   cannot make its own change conform to the document's family (a newer
   finalizer whose report shape belongs to another family), it does not
   write (section 7).
10. The next number goes to the first incompatible change, whatever it is.
    The dynamic-planning plan document is the expected first one; no number
    is reserved.
11. **Editions and families.** The Yggdrasil release tag identifies the exact
    schema edition and code; the family identifies compatibility. Producer
    conformance is checked against the schema of the release that produced
    the document. Within a family, an older document's canonical output
    validates against a newer schema of the same family, because compatible
    changes only add optional fields. That does not extend to arbitrary
    extensions (rule 7 of section 4 protects against name collisions). A
    released schema edition is never redefined.
12. **Support and retirement.** Each consumer declares, per document kind,
    which families it supports for inspection, for mutation and for
    execution. Supporting a new family neither requires keeping old families
    forever nor retires them. Retiring a family is an explicit release
    decision with a stated migration or inspection route; documents of a
    retired family stay identifiable as unsupported, never reinterpreted as
    current. Unpublished experiments in disposable state need no family
    number; a shape that has been persisted or read outside such an
    experiment needs an explicit compatibility decision before its meaning
    changes.

Integer families were chosen by the owner on 2026-10-20: every runtime
decision needs only the family, and the release tag already identifies the
precise schema edition.

## 6. Reading and Strictness

Two jobs, two policies.

**Producer checks are strict.** Yggdrasil's tests and Bifrost's fixture
generator validate what a writer produced against the schema of the same
release. Objects only Yggdrasil writes have closed key sets
(`additionalProperties: false`), so a misspelled key fails a test: the
snapshot, `plan`, `last_finalized_execution` and the attempt report. The plan
document's top level allows the contract fields plus any `x_` extension
(`patternProperties`), and nothing else, so an unprefixed foreign field is
reported (section 4, rule 7). Realm-defined payloads (`params`, `preview`,
step `metrics` and `artifacts`) are open.

Strict means honest, not idealized: the schemas describe what producers
deliberately record, including reports about malformed plans, cancelled and
orchestration-failed attempts, publication failures and legacy projections.
A report about a rejected plan is valid even though the plan was not
(section 3.3).

**Readers are tolerant.** A reader does not use the schemas as a gate. It:

- checks the fields it needs for each answer, and only those;
- ignores fields it does not know;
- reads an unknown vocabulary value as unknown and keeps the recorded text;
  it never guesses `draft`, `daemon` or `failed`;
- treats an unversioned, invalid or unsupported document as inspect-only:
  it shows what it can, labelled as such, and refuses the answers and actions
  that depend on its meaning;
- lets a problem affect only the answers that depend on it: a malformed
  progress percentage does not hide the request outcome; an unsupported
  snapshot next to a supported plan does not block approving the plan.

Example: Yggdrasil 0.7 adds `jobs[]` to snapshot steps. Bifrost, still pinned
to 0.6, ignores `jobs` and shows everything else. Validating that snapshot
against 0.6's closed schema would reject it, which is exactly why readers do
not validate.

**Control state is explicit.** Family-1 documents carry every control field
listed as required in section 3.1. A missing, null or malformed control field
is invalid control state, never a default: there is no "absent `run_token`
means 0" rule in family 1. Readers keep the diagnostics and refuse the
dependent conclusions and actions. The canonical statuses are `draft` and
`approved`; the Genstat-era `pending` is an unknown value like any other.

This release makes the executor keep that promise (section 7.2): today's
code defaults an absent `run_token` to 0 and an absent `plan.failure_policy`
to `fail_fast`, and reads `"5"` as 5. A version check alone would leave a
hand-edited document with `schema_version: 1` and no `run_token` executable.
These are small standard-library checks, not runtime schema validation.

### 6.1 Schema artifacts and tooling

- **Format.** The published structural contract is JSON Schema draft
  2020-12, shipped as package data of `yggdrasil.contract`, one resource per
  kind and family, handwritten and reviewed like code. There is no Python
  model of the whole plan document to generate them from, and the
  closed/open and diagnostic choices of this section are contract decisions a
  generator would not make. Producer tests catch drift between code and
  schema.
- **References** resolve offline from the bundled resources: each resource
  has an `$id` naming its kind and family (for example
  `urn:yggdrasil:contract:attempt_report:1`; exact syntax is an
  implementation detail), the loader builds an in-memory registry, and an
  unresolvable reference is an error. Validation never downloads anything.
- **Validation tooling.** `jsonschema` is a development dependency, imported
  only when validation is requested (tests, fixture checks, an explicit
  command). `validate(kind, document)` runs the schema and the procedural
  checks JSON Schema cannot express: JSON Schema's `integer` type accepts
  `1.0`, which this release rejects as both a version and a token. It reports
  the failing path and reason. Timestamp `format`
  checking is switched on explicitly in producer validation; a `format`
  annotation alone enforces nothing. The canonical spelling itself (whole
  seconds, `Z`) is published in the schemas as a `pattern`, which every JSON
  Schema validator applies; the `format` check adds whether the date and time
  exist.
- **Runtime checks** (the version boundary, readers, action preparation)
  are small standard-library functions. They never require a document to
  pass full schema validation or to build an executable Python model before
  it can be inspected. Cross-document rules (identity, generations, tokens,
  conditional writes) are explicit Python rules.
- No migration to Pydantic or cattrs is needed or implied. Putting full
  schema validation on the daemon's ordinary path, or adding a model library
  to the contract package, would be a design change.

## 7. Version Boundary for Writers and the Executor

The behaviour this release adds. It is a small check of `schema_version`,
not full schema validation.

### 7.1 Writers

Yggdrasil has two kinds of plan-document writers, with one rule each.

| Writer | Writes over | Rule |
| --- | --- | --- |
| **Update writers**: finalization (including a pending finalization); approval and rerun (**Document Contract: Approve and Rerun Helpers**) | an existing document whose other fields it keeps | On every read it may write over, including the reread after a conflict, the document's family must be supported **and** equal to the family captured with the request (at admission, or in the review context). Otherwise it writes nothing and returns a distinct refusal. |
| **Replace writers**: planning (`save_plan`, both stores, including run-once) | the whole document in a slot | It may replace an empty slot, an unversioned (pre-contract) document or a document of a supported family. It refuses a malformed marker or an unsupported family: nothing is written, the refusal names the plan and the recorded value, and other plans continue. It refuses, in the same way, a slot whose `created_at` cannot be kept (section 4, rule 2). Ordinary planning is never an implicit downgrade. |

Example of the replace rule: a daemon that supports only family 1 re-plans a
slot that a newer daemon already wrote as family 2. It refuses, instead of
replacing the family-2 plan with an older family-1 one.

Snapshot sinks do not check versions. A snapshot is a projection the consumer
can always rebuild from the event spool, so a wrong or outdated one is
replaced the next time the consumer writes that plan (with the consumer at
`dev` 418c96d: at the next content change or the next start). When the
consumer writes is an implementation detail, not contract. Mixing consumers
across an incompatible change is excluded by the rollout rules (section 11).

Deleting a plan (`delete_plan`, no caller in Yggdrasil) is an explicit
operator action and has no version check.

### 7.2 The executor

- In `PlanExecutionCoordinator._run_cycle`, the plan is read with a plain
  fetch (no generation initialization). The fetch returns a document only
  when its storage envelope is intact: `_id` equals the requested ID and
  `_rev` is a non-empty string. Anything else, such as an object-shaped error
  response, is a storage failure, never an unversioned document: nothing is
  concluded from it, and a pending result stays pending. The finalizer's
  reads apply the same rule. The document's family is checked **first**:
  before a pending finalization is settled and before `_admit`, on the first
  read and on the reread after settling.
- A missing, invalid or unsupported version stops the cycle with its own
  status (for example `UNSUPPORTED_DOCUMENT`, distinct from a malformed plan)
  and a log line naming the plan, the recorded value and the supported set.
  Nothing is written. Other plans are not affected.
- A pending finalization whose document fails the update-writer rule is
  dropped from memory with a warning, as a regenerated plan's is today. Its
  report stays in the event spool and in the snapshot's attempt projection.
  It is never reported as recorded and never retried as if storage had
  failed.
- The family is captured at admission with the generation, token and
  authority, and the finalizer requires it on every read
  (`check_finalization` gains one comparison and one supersession reason).
- **Control checks.** One function decides
  whether a token pair is valid: both present; JSON integers that are not
  booleans (in Python, `type(v) is int`); `0 <= run_token <= 2^53 − 1` and
  `-1 <= executed_run_token <= 2^53 − 1`; executed not above requested. The
  upper bound keeps tokens exactly representable by browser clients. The
  same function replaces all five `int(...)` parses: `is_plan_eligible`,
  `get_eligibility_reason`, `_admit`, `_settle_pending` and the finalizer's
  `_executed_token`. Invalid tokens make the plan ineligible, stop admission,
  keep a pending result pending and stop finalization, writing nothing.
  Admission also requires `plan.failure_policy` to be present and known
  (`Plan.from_dict` defaults it to `fail_fast` today); otherwise
  `INVALID_DOCUMENT`. `status`, `plan_generation` and `execution_authority`
  already fail closed: an absent or unknown status is not eligible, a missing
  generation is refused, an unknown authority matches no claim.

### 7.3 Retired write paths

- **Generation initialization** (`plan_updates.initialize_plan_generation`,
  `PlanStore.ensure_plan_generation`, both stores). A family-1 document
  always carries a generation; one without is invalid control state and is
  refused (`_admit` already refuses a missing generation). Pre-contract
  documents get a generation through regeneration. The coordinator was the
  only caller.
- **`PlanStore.update_executed_token`** (both stores and the protocol). It
  writes the executed token without checking generation, outcome or version,
  and nothing in Yggdrasil calls it; finalization replaced it.

### 7.4 Acceptance cases

1. An unsupported marker on a plan without a generation: nothing is written.
2. An unsupported marker with a pending finalization: the pending result is
   dropped with a warning; nothing is written.
3. The marker changes between a finalization's read and its conflict reread:
   the reread refuses; nothing is written.
4. Planning meets an unsupported or malformed marker in the slot: nothing is
   written; the refusal is logged.
5. Planning meets an unversioned document: it writes a family-1 document with
   a new generation.
6. Another valid plan in the same store still progresses in each case.
7. An unversioned plan is not executed; a family-1 plan runs.
8. A family-1 plan with no `run_token`, with `"run_token": "5"`, `5.0` or
   `true`, with executed above requested, or with no `plan.failure_policy` is
   not executed and nothing is written.

## 8. Package (this release)

```text
yggdrasil/contract/
    __init__.py     CURRENT_FAMILIES = {"plan_document": 1, "plan_status": 1}
                    SUPPORTED_FAMILIES = {"plan_document": {1}, "plan_status": {1}}
    schemas/        plan_document.v1.json, plan_status.v1.json,
                    attempt_report.v1.json (package data; v1 = family 1)
    schema.py       schema_for(kind, family), validate(kind, document);
                    ContractError names the failing path; jsonschema is
                    imported inside validate only
    versions.py     version_of(document) -> VersionInfo
                        state: "supported" | "unversioned" | "invalid" | "unsupported"
                        family: int | None    recorded: the raw value
                    supports(kind, document) -> bool
    controls.py     token_pair(document) -> TokenPair (valid, run, executed,
                        problems); failure_policy_of(document)
```

`VersionInfo` keeps the raw recorded value for diagnostics. Field-name
constants and reading functions (`fields.py`, `read.py`) arrive with
**Document Contract: Shared Reader**, which reuses `controls.py`; the action
helpers (`actions.py`) arrive with
**Document Contract: Approve and Rerun Helpers**. The package never reads
the clock: callers pass every timestamp in.

Layering: `yggdrasil.contract` joins the bottom layer (`config`,
`logging_utils`, `errors`), imports only the standard library, and is added
to `tests/test_import_lightness.py`. `storage` uses it to write the marker and
apply the writer rules, `daemon` to apply the executor rule.
`UnsupportedDocumentError` joins `storage.errors`.

Bifrost's runtime imports nothing from Yggdrasil in this release. Its fixture
generator (a development tool) already loads modules from the pinned
checkout; it loads `yggdrasil.contract` the same way and calls `validate`.

## 9. Delivery

The marker writers and the version boundary ship in the same release. A
release with the boundary but without the writers would refuse every plan.

### 9.0 Verification while writing the schemas

Inventory every producer and the regression fixtures before the schemas are
accepted: the shapes of `plan.steps[].inputs` and `outputs`
(`flow/outputs.py`), `artifacts`, `metrics`, `job` and both `error` shapes in
snapshot steps, and every report field that can carry raw input. Encode the
container shapes that producers actually write; leave a value open only where
it really is realm-defined.

### 9.1 Yggdrasil code

- The package of section 8; `package-data` gains `contract/schemas/*.json`;
  `jsonschema` joins the `dev` extra.
- `build_plan_document` and the snapshot builder write `schema_version: 1`.
- One timestamp formatter (decision D5): the existing
  `flow.utils.ygg_time.utcnow_iso()`, which already writes
  `%Y-%m-%dT%H:%M:%SZ` and is what events and snapshots use. The
  plan-document builder, the finalizer and the SQLite store import it
  directly. `storage` already imports `flow`, so this adds no layering
  edge; move the formatter to a neutral low-level module only if an import
  turns out unwanted.
- The writer rules of 7.1 in `save_plan` (both stores) and
  `check_finalization`; the executor rules of 7.2, including the control
  checks at the five token sites and the failure-policy check at admission.
- The removals of 7.3, with their tests.

### 9.2 Yggdrasil tests

1. Every plan document and snapshot a **builder** produces in the existing
   tests validates (`test_storage_plan_updates.py`,
   `test_plan_store_conditional_updates.py`, `test_ops_consumer*.py`,
   `test_ops_snapshot.py`, `test_core_engine_events.py`). Updates are
   checked as in item 5, not by validating whatever input a test fed them.
2. Truthful diagnostics validate: reports and snapshots of a preflight
   rejection with non-string step IDs, a cancelled attempt, an
   orchestration error, a publication failure, a legacy projection, and
   attempts missing their start or their report. The malformed plan inputs
   themselves fail validation, as expected.
3. Versions: absent is `unversioned`; `"1"`, `1.0`, `true`, `null`, `0` and
   `-1` are `invalid`; `2` is `unsupported`; `1` is `supported`. `validate`
   rejects `1.0` through the procedural check.
4. Closed and open parts: actor fields and `approvals_required` validate; an
   `x_`-prefixed extension validates; an unprefixed unknown top-level field
   fails validation; an unknown key inside `plan` fails; a wrong vocabulary
   value fails; `pending` fails; a missing required control field fails.
5. Updates: finalizing a builder-produced, valid document yields a
   valid document. Finalizing a document that carries an unprefixed foreign
   field keeps that field and `schema_version` exactly as read, and
   `validate` then reports that one field and nothing else.
6. Regeneration drops approval and request stamps and unknown fields, keeps
   `created_at`, writes `schema_version: 1` and a new generation.
7. The acceptance cases of section 7.4.
8. A regression corpus of real documents in `tests/contract/documents/`,
   built by this release's writers from the contract's own scenarios. The
   corpus is Yggdrasil's: Bifrost's catalogue and generator are not inputs. A
   released version's positive examples are kept as compatibility baselines;
   later releases add examples rather than replace them.
9. `import yggdrasil.contract` passes the lightness test.

### 9.3 Documentation

`docs/reference/document_contract.md` in the mkdocs nav: the inventory
(section 3), the ownership table and rules (section 4), the versioning and
reading rules (sections 5–6), the version boundary (section 7), how to
validate a document, and a short safe recipe for approving or rerunning by
hand: check that the generation is the one reviewed; write only the permitted
fields; on conflict, check the generation again before retrying; a rerun
writes the token you saw plus one, and if a retry finds the token already
moved, it stops. `docs/GENSTAT_PLAN_CONTRACT.md` becomes a pointer to the new
page; its field tables and unsafe recipes are not carried over.
`docs/reference/plan_execution.md` describes the boundary and drops the
legacy generation initialization.

### 9.4 Bifrost (one pull request, when the pin moves to this release)

Separate work, not part of this release's implementation (owner,
2026-10-22). Bifrost adopts the published contract in its own pull request;
this section records what that pull request does.

- The fixture generator checks every fixture it writes with
  `yggdrasil.contract.validate` from the pinned checkout before comparing
  bytes. Each fixture declares what it expects: a **positive**
  fixture must validate; a **negative** fixture (unversioned, invalid or
  unsupported version, an unprefixed foreign field) must be rejected for the
  declared reason. `--check` fails when either expectation is not met.
  Fixtures are regenerated with the new writers.
- **The adapter adopts the marker rule in the same change**: a plan document
  or snapshot whose `schema_version` is not the integer 1 is shown
  inspect-only, with its state (`unversioned`, `invalid`, `unsupported`) and
  the recorded value; approval state and request conclusions are withheld.
  New fixtures cover the three states. Without this, Bifrost would show an
  unversioned plan as runnable while the daemon refuses it.

### 9.5 Development stores

Before running this release against an existing development store, list its
unversioned, invalid and unsupported documents. Nothing is stamped
automatically. Wanted plans are regenerated through their realm (planning
replaces unversioned slots). Snapshots need nothing: this release adds
`schema_version` to every snapshot, so every snapshot's content changes and
is written on the consumer's first cycle after the upgrade (the consumer at
`dev` 418c96d also writes every plan once when it starts). That covers each
plan still in the spool, whatever write policy a refactored consumer adopts,
as long as changed content is written.
Disposable test state can be reset. This is a development choice; the
release itself deletes nothing.

## 10. What Each Capability Needs

| Capability | Needs | PRD |
| --- | --- | --- |
| Bifrost shows live CouchDB data | while pinned to `v0.5.0`: nothing new; from the pin move to this release: the adapter's marker rule (9.4) | **Bifrost: CouchDB PlanReader and `_changes` Refresh Hints** |
| Bifrost checks its fixtures against Yggdrasil's schemas | this release | this PRD |
| Yggdrasil's executor applies the strict control checks | this release | this PRD |
| Bifrost applies Yggdrasil's reading rules, tokens included | the shared reader; Bifrost depends on `yggdrasil` at the tag | **Document Contract: Shared Reader** |
| Bifrost approves and requests reruns safely | the action helpers; the live reader; authentication | **Document Contract: Approve and Rerun Helpers**; **Bifrost: CouchDB PlanReader and `_changes` Refresh Hints**; **Bifrost: Write Actions and Authentication** |
| Yggdrasil writes plan documents of family 2 | this release's boundary on every daemon and run-once host; family-2 documents written as new generations; Bifrost reading family 2 or showing it inspect-only | this PRD and the PRD introducing family 2 |
| Bifrost shows the daemon heartbeat | the lease document | **Daemon Lease, Heartbeat and Recovery Sweep** |

## 11. Rollout, Rollback and Future Compatibility

- **Precondition (confirmed by the owner, 2026-10-21).** No deployment needs
  its existing plans to keep running across this upgrade. All existing
  documents become unversioned and stop executing until regenerated. If that
  ever changes for some deployment, add a one-off stamping step for it.
- **Order.** Upgrade every daemon and run-once host to this release;
  regenerate the wanted plans or reset development state; then move
  Bifrost's pin (9.4). Moving Bifrost first would show every plan as
  unversioned.
- **Rollback.** A daemon released before this contract ignores markers.
  Rolling back to one is tolerable only while every stored document is family 1, which is today's
  shape: it executes them correctly, but the documents it regenerates come out
  unversioned and need regenerating again after rolling forward. Once any
  document of family 2 exists, rolling back to a release without the boundary
  is not allowed.
- **Spool and internal events.** Events and the full report have no public
  schema, but the producer (engine) and the consumer (ops consumer) must
  agree. Do not run producers and consumers across an incompatible change of
  event or report meaning, and do not roll back across retained spool data of
  a newer meaning, without explicit support, isolation or migration. A
  feature that changes those meanings defines its transition first.
- **Future families.** A family-2 writer may be deployed only when every host
  that executes or updates plans runs this release or later and declares
  support for family 2 (or refuses it), and Bifrost reads it or shows it
  inspect-only. Approvals never transfer between families (section 5,
  rule 7).

## 12. Out of Scope, and Where It Went

| Topic | Where |
| --- | --- |
| Reading rules (reusing this release's control checks), Bifrost's adapter swap | **Document Contract: Shared Reader** |
| Approve and rerun helpers | **Document Contract: Approve and Rerun Helpers** |
| Live reading, refresh hints, missed-update recovery | **Bifrost: CouchDB PlanReader and `_changes` Refresh Hints** |
| Authentication, carrying the review from page to click, retry policy, wording | **Bifrost: Write Actions and Authentication** |
| Recovery sweep, heartbeat display, lease fencing | **Daemon Lease, Heartbeat and Recovery Sweep** |
| Code identity, environments, what approval means across an upgrade | **Code Identity in Fingerprints**; **Execution Context and Upgrades** |
| Adoption, durable action history, public event schemas, migration tooling for pre-contract data | Future work, outside this release; migration tooling only if a deployment ever needs it |

## 13. Success Criteria

- New plan documents and snapshots carry `schema_version: 1` and validate,
  including the diagnostic cases of 9.2.
- In a test store: a family-1 plan runs; an unversioned, invalid or
  family-2 plan is not executed, not finalized and not replaced by planning
  (except that planning replaces an unversioned one), and its document is
  unchanged; the log names it; other plans run.
- A family-1 plan with missing or non-integer tokens, or without
  `plan.failure_policy`, is not executed; no `int(...)` remains on a token
  anywhere in Yggdrasil.
- `ensure_plan_generation`, `initialize_plan_generation` and
  `update_executed_token` are gone.
- `import yggdrasil.contract` passes the lightness test.
- `docs/reference/document_contract.md` is published; the Genstat document no
  longer offers the blind conflict retry, the `executed_run_token + 1` rule or
  adoption.

Bifrost's adoption (section 9.4) is separate work, judged on its own: its
`--check` passes, with its positive fixtures validating and its negative
fixtures rejected for their declared reasons.

## 14. Decisions

All decisions below are settled. **Owner** identifies an explicit owner
decision; **agreed** identifies an accepted design decision;
**engineering** identifies a settled technical choice. Their requirements
and rationale are stated in this PRD.

| # | Decision | Answer | Status |
| --- | --- | --- | --- |
| D1 | Public scope of the first release | Schemas for the plan document, the snapshot and the report component; markers; the version boundary; the reference page; Bifrost's fixture validation (Bifrost's side is separate work, section 9.4). No public event schema. Lease with **Daemon Lease, Heartbeat and Recovery Sweep**. | agreed |
| D2 | Version notation | Positive integer compatibility family per kind; the release identifies the schema edition. | **owner** (2026-10-20) |
| D3 | Strictness | Strict and honest producer checks; tolerant readers (section 6). | agreed |
| D4 | Actor fields | `approved_at`, `approved_by`, `run_requested_at`, `run_requested_by`; stable actor identifier; latest-action attribution only. | agreed |
| D5 | Timestamps | New Yggdrasil timestamps use RFC 3339 with `Z`, seconds precision; readers accept any valid RFC 3339 UTC form; producer validation checks `format` explicitly. Why: one spelling per instant (today `+00:00` in plan documents and `Z` elsewhere, and the same instant in the two spellings is unequal as text), and one exact format for producer tests to check. Tidiness, not safety. One formatter, `ygg_time.utcnow_iso()` (9.1). | **owner** (2026-10-21) |
| D6 | Placement | `yggdrasil.contract` in the bottom layer, standard-library imports, lightness test. | agreed |
| D7 | Schema tooling | Handwritten JSON Schema 2020-12 resources; offline references; `jsonschema` for development and explicit validation only; `validate` adds procedural checks; standard-library runtime checks; no model-library migration (section 6.1). | agreed; handwritten authoring: engineering |
| D8 | Version boundary | Update writers require a supported family equal to the captured one on every read; replace writers refuse malformed or unsupported slots; the executor checks first (section 7). | agreed; the two writer kinds: engineering |
| D9 | Genstat document | Replaced by the reference page; no notification. | agreed |
| D10 | `pending` | No alias; canonical statuses `draft` and `approved`. | agreed |
| D11 | Report marker | None; report containment as in section 3 and rule 8 of section 5. | agreed |
| D12 | Pre-contract documents | Unversioned, outside the contract; no `absent = 1`, no legacy defaults, no stamping. | **owner** (2026-10-20); rollout precondition confirmed 2026-10-21 |
| D13 | Planning over an unversioned slot | Allowed; produces a family-1 document with a new generation, keeping `created_at` as section 4 rule 2 describes (refused when it cannot be kept). This is how old plans enter the contract, without any migration tool. | **owner** (2026-10-21; `created_at` 2026-10-22) |
| D14 | Retired write paths | Remove generation initialization and `update_executed_token`. | engineering (follows D12) |
| D15 | Family within a generation | Never changes; a new family means a new generation and a new approval. | engineering |
| D16 | Extension names | Fields of writers we control are contract fields in the schema; foreign writers use the `x_<writer>_` prefix, which the contract never defines; the schema flags unprefixed foreign fields, it cannot prevent them (section 4, rule 7). | **owner** (2026-10-21), with the enforcement clarified |
| D17 | Rollback and spool | As in section 11. | agreed |
| D18 | Executor control checks | The strict token rule at all five sites and the failure-policy check at admission ship with this PRD, not **Document Contract: Shared Reader**, so that this PRD's "never a default" holds from its first release (sections 6, 7.2). | agreed |
| D19 | Updates versus conformance | Builders' output conforms; update writers conform in the fields they own and do not certify what they keep; fixtures declare positive or negative expectations (section 5 rule 9, 9.2, 9.4). | agreed |

## 15. Open Questions

None. Implementation details that do not need a decision: the exact `$id`
syntax, the names of the new error class, supersession reason and
coordinator status.

## 16. Implementation Outcome

Implementation is complete. The entries below record the implemented
behavior, accepted deviations and verification evidence.

**Contract core and markers (2026-10-22, committed 2026-10-23).**
`yggdrasil.contract` is the bottom-layer package of section 8, standard
library only. `tests/test_import_lightness.py` shows that importing it loads
nothing else (section 9.2 item 9, D6). It holds the kinds, the families
(`CURRENT_FAMILIES`, and `SUPPORTED_FAMILIES`, both read-only), and three
runtime modules:

- `versions.version_of(kind, document)` returns a `VersionInfo` (state,
  family, recorded value) in one of the four states of section 5 rules 3
  and 4;
- `controls.token_pair`, `failure_policy_of` and
  `execution_definition_problems` each return a verdict with one
  description per problem. The last checks that every execution field of
  the plan and of its steps is present, before deserialization could
  default one: section 7.2 names the failure policy, and the other fields
  were added when the implementation was planned;
- `timestamps` holds the canonical format and pattern, `is_canonical`,
  `is_date_time` and `canonical_from_legacy`.

None of them raises on stored content of any shape, and tests feed them
hostile values. The package reads no clock.

`build_plan_document` writes `schema_version: 1`. The snapshot envelope
moved into `snapshot_document`, a pure function in `ops/snapshot.py` that
`build_plan_snapshot` calls and that adds `schema_version` and `type`. Plan
documents, finalization and the SQLite row metadata now spell timestamps as
events and snapshots do, ending in `Z` instead of `+00:00` (D5). Every
snapshot's content changes, so the consumer rewrites each plan's snapshot
once after the upgrade. Nothing read the marker yet: plans executed as
before, and a `created_at` kept across regeneration kept its stored
spelling until the planning replace rule (next entry).

Accepted choices (owner, 2026-10-22):

- `version_of` takes the kind, because whether a family is supported
  depends on it; section 8's `version_of(document)` is illustrative.
- `contract/timestamps.py`, which section 8's listing does not name,
  defines the canonical format once.
- The snapshot envelope is a pure function, so producer tests and the
  corpus can build stored snapshots without a spool.

Implementation choices:

- `is_date_time` follows RFC 3339's case-insensitive grammar (`t` and `z`
  are valid), and rejects a leap second (`:60`), which Python's `datetime`
  cannot represent and no Yggdrasil writer produces.
- A missing `plan.failure_policy` is reported by both
  `execution_definition_problems` and `failure_policy_of`, so admission
  names each problem once.

**Planning replace rule (2026-10-23).** `save_plan`, on both stores and in
run-once, replaces an empty slot, an unversioned document or a document of
a supported family. It refuses an invalid marker or an unsupported family
without writing (section 7.1; cases 4 to 6 of section 7.4; D13). One
helper, `check_replaceable` in `storage/plan_documents.py`, decides, and
`build_plan_document` applies it to the slot's document before anything is
built, so both stores refuse the same slots. The marker is judged first.

`created_at` follows section 4 rule 2 (owner, 2026-10-22). Over an
unversioned document, a whole-second UTC value ending in `Z` or `+00:00` is
re-spelled with `Z`. A supported document's value must already be
canonical. Anything else is refused. No timestamp check was added to
eligibility, admission or finalization.

The refusal is `UnsupportedDocumentError` in `storage.errors`. It carries
the plan ID, the field (`schema_version` or `created_at`), the recorded
value, a reason code and the supported families. The reasons are
`invalid` and `unsupported` for the marker, and `missing`, `malformed` and
`unsupported` for `created_at`. The daemon's planning, `run-doc
--plan-only` and run-once log it once, at ERROR, and go on with their other
drafts; `--force` does not bypass it. Every refusal is shown to write
nothing on both stores, and another plan in the same store still
progresses.

Follow-up (2026-10-23): the refusal's message shows the recorded value
through a bounded representation that cannot fail, while
`error.recorded` keeps the value itself. Before, an integer beyond
Python's default 4300-digit conversion limit made `repr` raise, and
planning failed with `ValueError` instead of the refusal. That
representation became `describe_recorded` with the executor control checks
(below).

Accepted choices (owner, 2026-10-22): the rule lives in one helper that the
builder applies; the refusal is one error type, which core catches per
draft.

**Schemas and validation (2026-10-23).** The three handwritten JSON Schema
2020-12 resources of section 8 ship as package data
(`contract/schemas/*.v1.json`), each with an `$id` of the form
`urn:yggdrasil:contract:<kind>:<family>` (sections 6.1, 9.1, D7).
`jsonschema` (4.18 or later) joins the `dev` extra and is imported only
when validating. `schema.schema_for(kind, family)` returns a copy of a
schema. `schema.validate(kind, document)` reads `schema_version` first,
resolves references only within the bundle, and raises `ContractError`,
which lists every problem with a path and a stable code. Beyond the
schema, it checks that an integer is a JSON integer (never `1.0` or
`true`), that a timestamp names a date and time that exist, and that
`executed_run_token` is not above `run_token`. Every timestamp definition
carries the canonical spelling as a `pattern`, with `maxLength: 20`
(owner, 2026-10-22).

The producer inventory of section 9.0 came first, and the field types were
fixed after it. Tests characterize the realm values that producers keep
raw, and show that every builder's output conforms, the diagnostic cases of
section 9.2 item 2 included. They also cover the closed and open parts, the
version states and the updates of items 3 to 5. Existing tests that build
plan documents, snapshots or reports now validate what they build.

Accepted deviations and choices (review, 2026-10-23):

- A report failure's `code` and `advice` stay open: a realm's step error
  values survive into the report unchanged. A failure's `kind` is a plain
  string, not a vocabulary.
- A snapshot attempt's identity fields are nullable, `started_at`
  included, and a finished attempt is not required to carry them: the
  consumer copies them from whichever record exists.
- Report conformance is checked in `tests/test_core_engine_events.py`,
  since `tests/test_flow_attempt_records.py` builds no reports. The builder
  checks also apply to `tests/test_planning_replace_rule.py`.
- `TestAttemptProjection` in `tests/test_ops_snapshot.py` opts out: its
  hand-built reports are partial on purpose.
- A nonconforming builder output fails its test at cleanup, because the
  engine turns an exception raised while publishing into a publication
  failure.
- The schemas are stricter than section 3 states in three places:
  identifiers are non-empty, the report's tokens are range-checked, and
  `dependentRequired` makes `last_executed_at` and
  `last_finalized_execution` appear together.

**Engine name check (2026-10-26).** Engine entry points now reject realm and
plan IDs that cannot name a single directory component. Rejection raises
`ValueError` before allocation, reservation, publication or work writes; no
attempt starts and no report is produced. Attempt report plan IDs remain
nonempty strings. The three schemas are unchanged by this correction.

**Executor control checks (2026-10-26, completed 2026-10-27).** Eligibility,
admission and finalization read the execution tokens only through the strict
token rule (`yggdrasil.contract.controls.token_pair`; sections 6 and 7.2,
D18). Nothing is defaulted or converted:

- An approved plan whose tokens are not a valid pair is not eligible.
- Admission requires every execution field of the plan and of each step,
  checked before deserialization could default one, and a known
  `failure_policy`. Otherwise the request is `INVALID_DOCUMENT`, naming each
  problem once, and nothing is allocated, reserved or written.
- An unknown policy is therefore refused at admission, for daemon and
  run-once execution alike, with no attempt and no report. A direct
  `Engine.run` call still gets preflight's rejection.

Finalization judges, in order:

1. the generation;
2. the stored token pair;
3. whether this completion is already recorded;
4. the authority and owner;
5. whether the executed token already reaches the captured one;
6. the pair the write would produce.

If the stored pair is not valid, or the write would leave
`executed_run_token` above `run_token`, the result is the new
`FinalizationStatus.INVALID_CONTROL_STATE`. Nothing is written, no
supersession reason is given, and it is logged at ERROR. The executor keeps
such a result pending, on its first finalization and on a retry alike:

- a regeneration drops it whatever the tokens say;
- an authority change drops it only once the tokens are valid;
- a newer run request buys one retry.

Messages that show a stored value go through one renderer,
`yggdrasil.contract.diagnostics.describe_recorded` (owner decision,
2026-10-26). It is bounded to 200 characters, never raises, and
shows a value it cannot render by its type (`<unrepresentable int>`). It
serves:

- eligibility reasons;
- the planning refusal;
- control and `validate` problems;
- finalization diagnostics, including, since 2026-10-27, the owner captured
  at admission;
- since 2026-10-27, the executor's refusals for a foreign authority or owner
  and for an embedded plan ID.

Log lines that `logging` formats itself (PlanWatcher's skip lines, the
overwrite display) still show stored values unbounded. Correcting the
logging handler is separate work.

Accepted deviations and choices:

- PlanWatcher's eligible-plan log line reads both tokens without a default
  (owner, 2026-10-26).
- `plan_summary_from_document` keeps its display defaults; that change
  belongs to the companion shared reader.
- Control and `validate` problems now show a value in up to 80 characters
  (previously 60), and `<unrepresentable int>` replaces `<int>`.
- A result kept pending over its tokens carries the refusing finalization
  result.
- The new status is named `INVALID_CONTROL_STATE`; section 15 leaves such
  names open.

**Log record preparation (2026-10-27).** This is the logging handler
correction that the previous entry leaves as separate work, done as part of
the executor control checks.

A malformed log record no longer interrupts application processing. Before,
`DeduplicatingHandler`, which wraps Yggdrasil's file and console handlers,
built each message before forwarding it. A format that does not fit its
arguments, or an argument that cannot be converted to text, raised into the
code that logged. One example is `%d` given a string, which is ordinary JSON
data. Another is a stored integer too long to convert.

Such a record is now passed to the handler's `handleError` and dropped:

- The record and the duplicate-suppression state are left as they were.
- Interruptions such as `KeyboardInterrupt` and `SystemExit` still
  propagate.
- Normal logging behavior is unchanged: message text, truncation of long
  sequence values, repetition summaries, exception information,
  configuration failures and the wrapped handlers.

Whether the failure is visible follows logging's standard policy:
`handleError` reports it on stderr while `logging.raiseExceptions` is true,
and may stay silent when it is false. Not every malformed record therefore
produces a visible diagnostic. One exception remains while
`logging.raiseExceptions` is true: a `RecursionError` from rendering a
deeply nested argument still reaches the caller, because `handleError`
re-raises it while printing the record's arguments.

Stored values in these log lines remain unbounded. Log levels, routing and
development-mode behavior are unchanged.

**Version boundary (2026-10-28).** The executor and the finalizer act only
on plan documents of a family this release supports (sections 7.1 to 7.3,
D8, D14, D15):

- The coordinator reads a plan with a plain read that writes nothing
  (`PlanStore.read_plan_document`). A read counts as the plan's document
  only when its storage envelope is intact: a JSON object whose `_id` is the
  requested ID and whose `_rev` is a non-empty string. One helper,
  `require_storage_envelope`, applies this rule in both stores, to the
  coordinator's reads and to the finalizer's first read and conflict
  reread. Anything else is a non-retryable `PlanStoreError`, never a missing
  or unversioned document. A malformed read by the coordinator ends the
  cycle as `ADMISSION_FAILED`, and one by the finalizer keeps the result
  pending. A pending result stays pending either way.
- The family is checked first, on the first read and on the reread after
  settling. A document whose `schema_version` is absent, invalid or
  unsupported ends the cycle as the new
  `ExecutionStatus.UNSUPPORTED_DOCUMENT`, logged at WARNING. The message
  names the plan, the recorded value (or "absent") and the supported
  families, spelled `[1]` as in the planning refusal. Nothing is written,
  and other plans are not affected.
- A pending result for such a plan is dropped with a warning that names its
  execution ID and says that its report stays in the event spool. It is
  never recorded and never retried.
- Admission captures the family, and `ExecutionFinalization.schema_version`
  carries it: a required positive integer that is not part of the request's
  identity or of the persisted record. `check_finalization` judges it
  before anything else. A document of another family, or of none, is
  `SUPERSEDED` with the new `SupersessionReason.UNSUPPORTED_DOCUMENT`, even
  when it already carries this completion.
- Generation initialization and `update_executed_token` are removed from
  the protocol and both stores. A family-1 document without a usable
  generation is refused at admission (`INVALID_DOCUMENT`), and nothing adds
  one.
- Run-once needed no change: it records any unsuccessful resolution as an
  error.

Acceptance cases 1–3, 6 and 7 of section 7.4 run on both stores, through
the coordinator and end to end through the daemon's event handling and
run-once. Every refusal is shown to write nothing: the stored document
(with its `_rev`) and the SQLite plan-change sequence are unchanged, no
execution ID is reserved, and nothing appears in the test's sandbox.

Accepted choices (owner review, 2026-10-28):

- Messages show an unversioned document's `schema_version` as "absent"
  (`describe_schema_version`), in the coordinator and the finalizer alike.
- The protocol's `finalize_execution` docstring also states the token
  condition and `INVALID_CONTROL_STATE`, which it had omitted since the
  executor control checks.
- CouchDB's message for a read that is not a JSON object now comes from
  the shared helper, so its wording changed. Its error class and
  retryability did not.

Cleanup after review (2026-10-28), with no behavior change:

- The CouchDB store's private aliases are removed. `_utc_now_iso`, whose
  only callers were in `update_executed_token`, goes with its import.
  `_json_safe` and `_validate_execution_authority` had no users: the store
  calls `json_safe` and `validate_execution_authority` by their own names.
- `plan_documents.utc_now_iso()`, which only forwarded to
  `ygg_time.utcnow_iso()`, is removed. The plan-document builder, the
  finalizer and the SQLite store import `ygg_time.utcnow_iso()` directly,
  and section 9.1 now describes that reuse. The formatter stays where it
  is. Its own tests and the producer timestamp tests remain; the wrapper's
  test is removed.

**Regression corpus (2026-10-28, corrected 2026-10-29).** The corpus of
section 9.2 item 8 is in `tests/contract/documents/`. `current/` holds 17
plan documents and 9 snapshots in their stored form, with a manifest.
`tests/contract/corpus.py` builds them from fixed inputs, through this
release's writers and producers:

- plan documents through `build_plan_document` and the finalizer's
  `build_finalized_document`, recording real engine attempts. Each attempt
  is first admitted the way the coordinator admits it, and each result is
  judged by `check_finalization` before it is written;
- snapshots through the ops consumer's `build_plan_snapshot`, over the
  published spool.

The approval, the rerun request and a foreign writer stand in for writers
this release does not have. Most negative documents are a positive one
changed by hand. The manifest names every field set by hand, and the entry
a document was changed from. Each entry also gives the document's
expectation (valid, or every problem `validate` reports, as path and code)
and its provenance. Two plan histories are recorded in order: a request's
lifecycle from draft to regeneration, and a regeneration over an
unversioned document.

The tests check that:

- every document meets its expectation exactly;
- a rebuild reproduces every file byte for byte, wherever it runs and
  whatever the configuration of the machine running it;
- a document changed by hand differs from its base only in its declared
  fields;
- each history keeps the lifecycle rules. Within a generation, the family,
  the definition and the generation never change, and the run token never
  decreases (section 5 rule 7). A new generation carries only what planning
  writes.

`released/<tag>/` holds a released version's positive examples, frozen:
never regenerated or edited, and each must keep validating against its
family's current schema (section 5 rule 11). Negative examples stay in
`current/`. Released-baseline behavior was tested on editions released
from `current/` into a temporary directory: such an edition contains
exactly the positive examples and passes the released checks, an edition
that fails them is reported, and rebuilding `current/` leaves released
editions untouched.

Accepted choices (owner, 2026-10-28; review, 2026-10-29):

- The two histories; snapshots from the real spool emitter and the
  consumer rather than a capturing emitter; and, as the unversioned
  document, the approved plan as an earlier release stored it: no
  `schema_version`, `+00:00` timestamps and an unprefixed `ticket`.
- The exact-difference check applies only to documents changed by hand. A
  negative document that a writer produced keeps the writer's output: the
  finalized preflight rejection of a plan whose step ID is not a string.
  Released checks do not depend on the negative documents left out of them.
- One clock is frozen at its source, `ygg_time`'s `datetime`. This also
  holds the report's `started_at`, whose default factory refers to the
  formatter itself.
- An approved plan's finalized document is added: the only one in which the
  finalizer keeps approval stamps.
- The cancelled snapshot comes from a step interrupted by
  `KeyboardInterrupt`, which leaves `interrupted` and `unreached` steps.
  Cooperative cancellation is a different case.
- A negative document must report exactly its declared problems.
- The README is written by hand. The documents and the manifest are
  generated, and regeneration writes only `current/`.

Correction after review (2026-10-29): the build reads no configuration. The
engine gives each step's DataAccess no configuration, so DataAccess loaded
`main.json`. Under pytest that was the test session's temporary file; from
the standalone command, the developer's own. A file that could not be
loaded failed the build, with the attempt ending in `ORCHESTRATION_ERROR`.
Within each attempt, the builder now replaces the engine's `DataAccess`
with the real constructor and an empty external-systems configuration, so
each step keeps a real DataAccess with the engine's trace context. A test
makes loading the configuration raise and checks that the rebuild is
unchanged. No engine code changed.

Correction of the release order (2026-10-29), added to this entry: a
release's baseline is prepared before the release is tagged, under the
release's intended tag name, and is part of the tagged source. The tag goes
on a commit that contains the edition, and a published tag is not moved to
add one later. The corpus README's release step, which had the baseline
added when a version was tagged, now follows that order.

**Documentation (2026-10-29).** `docs/reference/document_contract.md` is
published, in the navigation after Plan execution (section 9.3). It covers
the documents in the contract and their fields, what `null` means per field
and which values are opaque, field ownership and its rules, versions and
compatibility, the reading policy, the version boundary with the log lines
an operator sees, checking a document with `validate`, the safe recipe for
approving or rerunning by hand, and upgrading and rolling back, with a
snippet that lists a store's documents by version state (section 9.5). It
describes what this release ships, and states what a program that reads or
changes these documents has to do itself.

`docs/GENSTAT_PLAN_CONTRACT.md` is now a pointer to the reference page: none
of its field tables or procedures survive (section 13). The pages that the
earlier entries made stale were updated:

- `docs/reference/plan_execution.md`: `schema_version` in both field
  tables, the token rule in eligibility, the order of admission checks, the
  unknown failure policy now refused at admission, which pending results
  are kept and which dropped, and the rerun recipe;
- `docs/reference/troubleshooting.md`: why an approved plan does not run
  (schema version, tokens, execution fields), the planning refusal, an
  upgraded store's plans, and the reasons a result stays pending or is
  dropped;
- `docs/reference/glossary.md`: the contract's terms;
- `docs/getting_started/configuration.md`: manual approval follows the
  recipe, keeps `schema_version` as read, and observes the token rule;
- `CLAUDE.md`: `contract` in the bottom layer and in the namespacing, and
  the rule for plan documents in tests.

With the owner's approval, five statements outside that list were
corrected too: the failure-policy default in `docs/flow_api/overview.md`
and the glossary (a `Plan` model's default, not a stored document's), the
package table in `docs/architecture/overview.md`, `--force` in
`docs/getting_started/cli.md`, the approval example in
`docs/reference/test_realm.md` (which raised `run_token` along with the
approval), and the dev SQLite entry of the troubleshooting page.

Section 9.2 item 1 now names `tests/test_core_engine_events.py`, where
report conformance is checked, in place of
`tests/test_flow_attempt_records.py`, which builds no reports. This
factual correction was approved by the owner and changes no decision.
Section 9.1 already described the direct reuse of `ygg_time.utcnow_iso()`
and needed no change.

Test counts, measured the same way at both ends: `pytest tests/` in a
working tree that holds the two git-ignored local tool tests
(`tests/test_dev_plan_approval_tool.py` and
`tests/test_internal_storage_smoke_tool.py`, 50 tests at the branch head).
A clean clone runs 50 fewer.

| Measure | `dev` 418c96d | Branch head |
| --- | --- | --- |
| `pytest tests/` | 2650 passed, 937 subtests | 2983 passed, 2623 subtests |
| `ruff check .` findings | 32 | 32 |
| `black --check .`, `mypy .` | clean | clean |

The documentation adds no tests. `mkdocs build --strict` passes, and every
link with an anchor resolves.

Bifrost's adoption of the published contract (section 9.4) stays separate
work.

**Final review and verification (2026-10-29).** The review of the
documentation, the last part of the implementation to be reviewed, covered
the branch at 4a98da9, the last of the documentation commits 00fb721,
06ea6f9 and 4a98da9. It asked for four corrections and accepted the other
choices. The corrections are commit 41d1cc7:

- The reference page's rerun example refuses a document without a usable
  `plan_generation`. Before, a missing, empty or `null` generation that
  matched the reviewed state still had a rerun request written.
- A `Plan` passed directly to `Engine.run` defaults an omitted failure
  policy to `fail_fast`; only an explicitly unknown one is rejected, by
  preflight. `docs/reference/plan_execution.md` said that preflight rejects
  a missing policy too, and so did a sentence of
  `docs/reference/troubleshooting.md`, corrected with it.
- When planning refuses to replace a plan, the troubleshooting page no
  longer says to correct the field. An invalid `schema_version` is restored
  only when its value is known, from a backup or the record of the edit
  that damaged it, and is never chosen to pass the check. Otherwise the
  document is kept as a copy, deleted and planned again. Each `created_at`
  reason has its own repair. A repair is a conditional write of that field
  alone, and deleting the document discards its approval and its results.
  The reference page's description of the refusal says "repairs" where it
  said "corrects".
- In the table of `null` values, a snapshot attempt's `null` identity field
  can come from an explicit `null` in the record it is copied from, not
  only from a field that record lacks: a direct `Engine.run` call captures
  no generation or token, and its start record holds `null` for both.

Accepted choices (review, 2026-10-29):

- The reference page keeps its rerun example for the dev SQLite store, now
  corrected.
- Listing a CouchDB store is described in prose, without a snippet, since
  none could be run against a CouchDB server.
- The public documentation does not depend on Bifrost.

The review reports, for 4a98da9, a strict documentation build, all 1,719
links with an anchor resolving, and the corpus tests passing (12 tests, 66
subtests).

Before 41d1cc7 was committed, its changes were checked in the working
tree. The reference page's examples ran against a temporary SQLite store.
With a missing, empty or `null` generation, the corrected rerun example
leaves the document, its `_rev` and the plan-change sequence unchanged,
while the example as it stood at 4a98da9 writes a rerun in all three
cases. A strict build and the anchor check passed, and so did `pytest
tests/contract tests/test_flow_outcomes.py` (221 tests, 874 subtests).

At 41d1cc7, these checks were rerun:

| Check at 41d1cc7 | Result |
| --- | --- |
| `pytest tests/` | 2983 passed, 2623 subtests |
| `ruff check .` findings | 32 |
| `black --check .`, `mypy .` | clean |
| `mkdocs build --strict` | passes |
| Links with an anchor | 1,723, all resolving |
| `python -m tests.contract.corpus` | no differences |
| The reference page's examples | as above |

The test suite, ruff, Black and mypy ran in the working tree, measured as
in the Documentation entry: the tree also holds the two git-ignored tool
tests. The documentation build and the anchor check ran on an export of
the commit. The anchor check also counts 1,719 links at 4a98da9; the
corrections add one link to the recipe and three line anchors in the
corrected example.

The full-suite figures of the Documentation entry were measured before
the documentation was committed, on the working tree over d1bac76. The
documentation changes no code and no test, and the rerun at 41d1cc7 gives
the same counts. This document is committed after 41d1cc7: the
documentation build excludes it, and no test reads it.
