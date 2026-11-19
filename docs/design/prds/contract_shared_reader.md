# Yggdrasil – Document Contract: Shared Reader (PRD)

## Version
v0.20 (2026-11-19): closes the PRD after the `v0.6.0` release. Adds
section 14, Implementation Outcome: what shipped, the accepted
deviations, and the release evidence (the frozen baseline, the schema
correction made before the tag, Bifrost's validation, the tag, and the
installation checks). Other PRDs are named by title instead of by
number, with links to those in this repository, and references to files
outside this repository are replaced by descriptions. Requirements and
decisions are unchanged.

v0.19 (2026-11-16): records a display refinement the owner decided on
2026-11-16, in the review of the plan summary's implementation. The summary
gains `unrecognised_status` and `unrecognised_execution_authority`: the
recorded text of `status` or `execution_authority` when it is text but not a
word of the field's vocabulary, else `None`. The interpreted `status` and
`execution_authority` stay `None`, and an unrecognised word is still not a
problem. The "Existing Plan Found" box shows that text beside "unknown" on
the existing line, bounded and escaped by `describe_recorded`. Reader
diagnostics, eligibility, execution and the overwrite guard are unchanged
(5.4).

v0.18 (2026-11-16): records the compatibility decision the owner took on
2026-11-16, in the review of the eligibility switch. `storage.plan_eligibility`
is removed in this release instead of re-exporting `is_plan_eligible`,
`get_eligibility_reason` and `APPROVED_STATUS` for one: callers import
`contract.read.is_eligible`, `contract.read.eligibility_reason` and
`contract.fields.APPROVED` (5.4, 9). The enum adapter for `finishes_request`
stays.

v0.17 (2026-11-13): records two changes the owner approved on 2026-11-13,
in the review of the value-helper moves' implementation. Snapshot IDs use
the scope's kind as recorded: every partition is `{kind}-{id}`, so a project
`P1` is `project-P1` and a flowcell `FC1` is `flowcell-FC1`, where the
writer abbreviated them to `proj-P1` and `fc-FC1`; `test_scenario-S1` and
every other kind's partition are unchanged. The reader and both writers
derive the new IDs through `partition_name`, and the writer's acceptance
and `ValueError` do not change. Snapshots already stored keep their IDs;
after the upgrade, the ops consumer writes each plan's snapshot under its
new ID where the plan's spool remains, and cleaning up or copying the old
snapshots is a separate rollout step (5.3, 8 step 4, 9, D11).
`contract.read.generation_of` is the only generation helper:
`storage.plan_documents.plan_generation_of` is removed rather than
re-exported for a release, while the eligibility re-exports stay (5.4, 9).

v0.16 (2026-11-12): records a clarification the owner made on 2026-11-12,
in the review of the snapshot reader's implementation. A step entry without
its required `outcome` or `state` key is not read as `null`: its status is
`unknown`, the absent key is `missing_field`, and no contradiction is
flagged. Explicit `null` keeps the step-status table's rows, including the
contradiction of a terminal event beside a `null` outcome (4.8). Reader
interpretation only.

v0.15 (2026-11-12): records two clarifications the owner accepted on
2026-11-12, in the review of the plan-document reader's implementation.
`request_state` returns `RequestState | None`, as `read_plan(...).request`
does: None when the plan cannot supply a request state, and no request state
is made up for it (5.3, D17). `execution_owner` is required text that may be
`null`, as the schemas define it, not an identifier: non-empty text is the
owner; empty text and `null` name no owner, without a problem; an absent
field is `missing_field` and any other value `wrong_type`. A snapshot
attempt's `execution_owner` is read the same way (5.3). The non-empty rule
for IDs and generations (section 3) is unchanged. Both concern reader
interpretation only; execution-authority checks do not change.

v0.14 (2026-11-10): records a reader correction the owner accepted on
2026-11-10, found while reviewing the implementation of 4.10. Python 3.11's
parser accepts text that bypasses the six-digit guard and the exact-offset
requirement: a fraction after the seconds that a colon introduces or that
follows with no separator, in the time or the offset, and text after a NUL.
The reader refuses those, and the colon or digit date-time separators that a
text check cannot tell apart from them (4.10, 8 step 6, 9, D22). The
correction narrows D22's union and needs no separate amendment. Producer
spelling and the named cases are unchanged.

v0.13 (2026-11-09): reconciles the PRD with its implementation plans before
implementation, at the owner's request. Clarifications and corrections: the
current request's outcome needs recorded completion as well as a usable
finalized record of that request (4.4 rule 2, 5.3, D7);
`finalized_is_current` states recorded request identity, under the evidence
rule it shares with the new correlation verdict (5.3, owner); an absent
`direct_blockers` or `failed_ancestors` is a structural fault on any step
entry, while blockers are still interpreted only for blocked steps (4.3,
4.9); the stored step state `unknown` is listed (4.8); an absent
`plan.steps` or step `deps` leaves the planned inventory unavailable, and a
`null` attempt diagnostic is no diagnostic (5.3); the call-site paragraph
follows the adapter table and the code (5.4); the plan summary's `problems`
key and bounded Problems line are recorded (5.4, 8 step 5); delivery names
every deviation from Bifrost and the test updates the version-first rule
requires (8 steps 2 and 4). Interface addition:
`Correlation.request_and_observation`, whether the observed attempt belongs
to the current request (`current`, `other`, `unknown`), with closed reasons
and their precedence (5.3). The v0.12 approval does not cover this wording.

v0.12 (2026-11-09): corrects 4.10's case-handling sentence: only lowercase
`z` handling is confined to the RFC 3339 path; the unchanged ISO input may
already contain lowercase `t`. Adds a compact lowercase-`t` regression case
and an implementation note/test pair for exact sub-second timezone offsets
whose whole-second part is zero. These clarify the existing reader
requirements; producer spelling and timestamp policy are unchanged.
Approved by the owner on 2026-11-09.

v0.11 (2026-11-06): writes the owner's two clarifications of D22 into 4.10
and its tests: named accepted and rejected cases with asserted UTC
instants, kept as compatibility checks for Python upgrades (an acceptance
change is reviewed; no claim that a finite list detects every grammar
expansion, and no separate grammar engine); lowercase `z` confined to the
RFC 3339 path; the six-digit guard applied to fractions introduced by `.`
or `,` in time and offset components before parsing. Canonical producer
spelling is unchanged. Reader interpretation only (4.10, 8, 9, 11, D22).

v0.10 (2026-11-06): accepts the review decisions with the owner's clarified
reader intent: preserve readable realm-supplied ISO timestamps as well as
RFC 3339 (D22). Records the supersession of D5 of
[Document Contract: Schemas, Versioning and Compatibility](document_contract.md)
explicitly, without rewriting its historical decision. Warning policy keeps the existing
change-feed cursor behaviour: no repeated plan-refusal warnings on idle
polls. Additional warning deduplication is deferred (4.7, D9, D21).

v0.9 (2026-11-06): follow-up review. Adds the clock-free canonical formatter
(D20), keeps the scoped malformed-control WARNING change in this PRD (D21),
settles the D10 review note, and corrects version-check ordering and the
pending-finalization description. Corrects Python grammar claims and local
references. Following the owner's RFC 3339 preference and delegated choice,
the shared reader uses bounded RFC 3339 (D22); section 13 records the
resolved review decisions. Yggdrasil `dev` rechecked at
7ace54d; Python grammar checked against the Python 3.11 documentation.

v0.8 (2026-11-06): review of v0.7 against Yggdrasil `dev` 7ace54d,
[Document Contract: Schemas, Versioning and Compatibility](document_contract.md)
as promoted, the corpus README, and the drafts
**Document Contract: Approve and Rerun Helpers**,
**Bifrost – CouchDB PlanReader and `_changes` Refresh Hints**
and **PlanWatcher Reliability and Operational Logging**. 4.10 holds where it states
code facts: the writers pass a supplied `now` through unchanged, so
preservation costs nothing; `canonical_from_legacy` shares `is_canonical`,
so the guard it asks for is needed; an existing test ties the three schema
definitions to `CANONICAL_PATTERN`, which therefore changes with them; a
Python-backed validator admits a final newline under `$` once `maxLength`
grows (checked with jsonschema 4.26); `datetime.fromisoformat` truncates a
seventh digit (checked on 3.12, same code on 3.11). Two things 4.10 left
open are questions for the owner (13): the reader grammar, which v0.7
described as two grammars that do not coincide (Q1, with the recommendation
written into 4.10), and a canonical formatter for fractional values (Q2).
Reading non-UTC offsets is now stated as a change to the reader rule of
D5 in [Document Contract: Schemas, Versioning and Compatibility](document_contract.md)
(4.10, 8 step 6, 9). The first frozen release baseline is tied to its
definition in section 16 of that PRD and the corpus README (7, 8 step 7).
The severity note is handed to the draft
**PlanWatcher Reliability and Operational Logging**, where the
same question is filed; the note on the two references to this PRD in the
draft **Document Contract: Approve and Rerun Helpers** is corrected (13). Status
refreshed. The withdrawn amendment is kept as a historical copy, outside
this repository.

v0.7 (2026-11-06): the owner withdrew the timestamp-ownership amendment.
This PRD follows [Document Contract: Schemas, Versioning and Compatibility](document_contract.md)
directly and incorporates fractional-second allowance in
4.10 (D18), while framework clocks continue writing whole seconds. Event
timestamp overrides and raw projection remain supported (D19); 4.9 now
interprets step time tolerantly instead of requiring canonical spelling.
Delivery, compatibility, tests and the release candidate include the
fractional allowance. Other v0.6 review points remain separate. Yggdrasil
`dev` rechecked at 7ace54d on 2026-11-06; Bifrost's 38c09cd audit remains
the characterization baseline.

v0.6 (2026-11-03): acceptance candidate after the owner's review of v0.5
with GPT-6 Astra (2026-11-03). Scope narrowed to Yggdrasil: what Bifrost
does with the reader moved to the companion document on Bifrost's adoption
of the reader, a separate draft (v0.5's section 7, the Bifrost steps
of 8, the Bifrost half of 9 and 11). The four questions of v0.5 are decided
(D14, D15, D16 and D4 as amended): one tag carries
[Document Contract: Schemas, Versioning and Compatibility](document_contract.md)
and this PRD; `plan_summary_from_document`
stays in `storage` as a projection of the reader and exists for every stored
document; Bifrost's dependency is a git source at the tag, locked, with the
footprint measured early; the interpretation of a step's progress, error and
blockers moves with the status rule. Amendments from the review: typed
equality (3, 4.3, 4.4); `request_state` takes the expected ID (5.3); which
functions check the version and which only extract (5.3); `finishes_request`
keeps today's truth table exactly (4.1, D10); callers report the refusals
the reader now makes earlier, and the pending-finalization case is specified
and tested (4.7, D9); the snapshot ID shares its formatting without
tightening the writer (5.3, D11); control-check problems become structured at
their source (5.2, D12); constants are shared as spellings only (5.1, D13);
Bifrost's integration is validated against the candidate commit before the
tag (7, 8). Facts as in v0.5: Yggdrasil `dev` 7ace54d, Bifrost `main`
38c09cd.

v0.5 (2026-11-02): rebased on
[Document Contract: Schemas, Versioning and Compatibility](document_contract.md)
as implemented (`dev` at 7ace54d, merge of PR #111) and on the order: that
PRD, this PRD, the draft
**Bifrost – CouchDB PlanReader and `_changes` Refresh Hints**,
then the draft **Document Contract: Approve and Rerun Helpers**; corrected the claim that
Bifrost's adapter "already applies the same rule"; filled in the interface,
the rules as tables, and the delivery plan; added D7–D13 and four owner
questions. v0.4 (2026-10-21): the strict token rule and its five Yggdrasil
call sites moved to v0.7 of
[Document Contract: Schemas, Versioning and Compatibility](document_contract.md)
(its decision D18). v0.3 (2026-10-21): the owner confirmed D2. v0.2
(2026-10-20; v0.1 2026-10-19) followed v0.5 of that PRD and applied
findings A3, A5 and A8 of Codex's review of the Document Contract drafts,
as reconciled in that review's second round. Built from the
v0.9 working draft (sections 11.3–11.4) and Bifrost's
`docs/design/operational-views.md` sections 6–7 at 38c09cd.

## Status
**Implemented and released in `v0.6.0` (2026-11-18).** Section 14 records
what shipped, the accepted deviations and the release evidence. Approved
(v0.12) by the owner on 2026-11-09. v0.13 (2026-11-09)
reconciles the PRD with its implementation plans before implementation, at
the owner's request; its new wording is not covered by the v0.12 approval.
v0.14 (2026-11-10) records the reader correction the owner accepted on
2026-11-10 (4.10, D22); it needs no separate amendment.
v0.15 (2026-11-12) records two clarifications the owner accepted on
2026-11-12, the `request_state` return type and the reading of
`execution_owner` (5.3); they need no separate amendment.
v0.16 (2026-11-12) records the owner's clarification of an absent step
`state` or `outcome` key (4.8); it needs no separate amendment.
v0.17 (2026-11-13) records the two changes the owner approved on
2026-11-13: snapshot IDs from the scope's kind as recorded (5.3, 8 step 4,
9, D11), and no re-export of `plan_generation_of` (5.4, 9).
v0.18 (2026-11-16) records the owner's decision to remove
`storage.plan_eligibility` in this release, with no re-exports (5.4, 9).
v0.19 (2026-11-16) records the owner's display refinement of the plan
summary: an unrecognised status or authority is shown beside "unknown"
(5.4); it needs no separate amendment.
v0.20 (2026-11-19) closes the PRD after the release; it changes no
requirement or decision.
v0.7 added fractional
allowance (D18, D19) after the ownership amendment was withdrawn (a
historical copy of that amendment is kept with the drafts, outside this
repository).
The v0.12 approval covers the PRD as a whole at v0.12, including its reader
grammar, formatter, D10 and scoped warning policy; section 13 records the
review decisions. Second of
the three Document Contract PRDs: immediately after
[Document Contract: Schemas, Versioning and Compatibility](document_contract.md),
with no intervening timestamp amendment, and before the follow-on draft
**Document Contract: Approve and Rerun Helpers**. This PRD changes Yggdrasil only.
Bifrost's adoption of the reader is the companion document's; this PRD
gives it the interface (5) and the validation clause (7).

## 1. Overview

Put the rules for reading plan documents and `plan_status` snapshots into one
place, `yggdrasil.contract.read`, used by Yggdrasil's own decisions
(eligibility, admission, pending finalization, finalization, the existing-plan
check) and by any consumer that reads the stored documents.
[Document Contract: Schemas, Versioning and Compatibility](document_contract.md)
defined the documents and enforces their safety rules at the writer and the
executor; this PRD gives every reader one interpretation of what a stored document
says. Most rules exist today, split between four Yggdrasil modules and
Bifrost's adapter (`backend/src/bifrost/yggdrasil/`, about 1,000 lines that
re-derive Yggdrasil's rules at a pinned revision). This PRD moves the
Yggdrasil rules under `contract`, ports the rules Bifrost re-derives with
Bifrost's tests as the pin on their behaviour, and adds four rules from the
v0.9 discussion. It also widens the contract timestamp profile to admit
fractional seconds (4.10), before the first combined release. This does not
change who supplies event timestamps.

The purpose row for this PRD: *give Yggdrasil and Bifrost one shared
interpretation of those documents.* The follow-on draft
**Bifrost – CouchDB PlanReader and `_changes` Refresh Hints**
connects that interpretation to live CouchDB data; the follow-on draft
**Document Contract: Approve and Rerun Helpers** prepares approval and rerun changes
on top of it. Neither
is touched here beyond the boundary notes in section 6.

### 1.1 Starting point: what *Document Contract: Schemas, Versioning and Compatibility* delivered and what it left to this PRD

Landed on `dev` (section 16 of
[Document Contract: Schemas, Versioning and Compatibility](document_contract.md)),
and reused except for the explicit changes
specified below:

- `yggdrasil.contract` (stdlib only; `tests/test_import_lightness.py`
  enforces it): the kinds `PLAN_DOCUMENT`, `PLAN_STATUS`, `ATTEMPT_REPORT`;
  `CURRENT_FAMILIES` and `SUPPORTED_FAMILIES`.
- `versions.version_of(kind, document) -> VersionInfo(state, family,
  recorded)`, with the four states `supported`, `unsupported`, `unversioned`,
  `invalid`; `supports(kind, document)`. It takes the kind (a deviation from
  section 8 of that PRD that this PRD adopts).
- `controls.token_pair(document) -> TokenPair(valid, run, executed,
  problems)`, `controls.failure_policy_of(document) -> FailurePolicyCheck`,
  `controls.execution_definition_problems(document)`, `FAILURE_POLICIES`,
  `MAX_TOKEN`. Their problems are tuples of strings (5.2).
- `timestamps` (canonical spelling), `diagnostics.describe_recorded` (bounded,
  never raising), `schema.validate` (development and tests only; needs
  `jsonschema`), the v1 schemas, and `schema.ContractProblem(path, code,
  message)` with its problem codes.
  Section 4.10 extends the timestamp profile and adds tolerant parsing; the
  existing seconds-only implementation is the starting point, not a
  prerequisite amendment.
- The regression corpus `tests/contract/documents/current/` (17 plan
  documents, 9 snapshots, a manifest naming each scenario and whether it must
  validate), built by the real writers from fixed inputs.
- The executor applies the token rule, the family gate and the control checks
  at admission, pending-finalization settling and finalization (D18 of that PRD); the
  finalizer refuses `INVALID_CONTROL_STATE` and reports `SUPERSEDED`
  (`UNSUPPORTED_DOCUMENT`) when the document is no longer of the family the
  request was captured under.

Deferred by [Document Contract: Schemas, Versioning and Compatibility](document_contract.md)
to this PRD, explicitly or by its package listing:

- `contract/fields.py` and `contract/read.py` (section 8 of that PRD: "arrive with the
  Shared Reader").
- `plan_summary_from_document` keeps its display defaults (`status`
  "unknown", `execution_authority` "daemon", `updated_at` "unknown",
  `run_token` 0, `executed_run_token` -1): "that change belongs to the
  companion shared reader" (section 16 of that PRD).
- The reference page `docs/reference/document_contract.md` says the contract
  "provides no library for reading these documents" and that a reader
  "applies the rules on this page itself". That sentence is this PRD's to
  retire.
- Bifrost's adapter adopts the version marker rule only in the pin-move pull
  request (section 9.4 of that PRD), which has not happened: at 38c09cd the adapter
  reads no `schema_version` at all. Under D14 that pull request and the
  adoption of the reader are one Bifrost change, specified in the companion.

Accepted deviations in [Document Contract: Schemas, Versioning and Compatibility](document_contract.md)
that this PRD must read correctly: the snapshot
attempt's `plan_generation` and `run_token` are nullable (schema
`attempt.v1`), so "missing" below always means absent *or* `null`.

## 2. Problem Statement

- Bifrost's adapter re-derives Yggdrasil's rules, and every pin move needs a
  hand review to confirm they still hold. The review of the draft
  **Bifrost – CouchDB PlanReader and `_changes` Refresh Hints** found
  four places where they already do not (that review's findings C1 and C2,
  and this audit):
  1. an absent `run_token` is read as 0 and an absent `executed_run_token`
     as -1 (`plan_document.py`, `_token`), where section 6 of
     [Document Contract: Schemas, Versioning and Compatibility](document_contract.md) makes a
     family-1 document without a token invalid and the daemon refuses it;
  2. negative tokens down to -(2^53 - 1) are accepted (`values.integer`),
     where that PRD allows -1 only for `executed_run_token`;
  3. the approval status `pending` is read as `draft` ("the documented legacy
     alias", `vocabulary.approval_status`), where that PRD's D10 has no alias;
  4. an absent `plan.failure_policy` is read as `fail_fast`
     (`vocabulary.plan_failure_policy`), where that PRD's `failure_policy_of`
     reports it and the daemon refuses the plan at admission.
  All four disappear by construction when the adapter calls the shared
  reader. The companion lists them as visible changes on Bifrost's side.
- The readers also disagreed on tokens before
  [Document Contract: Schemas, Versioning and Compatibility](document_contract.md)
  (`int(value)` in five Yggdrasil places; JSON integers only in Bifrost).
  That PRD made Yggdrasil strict. This PRD gives every reader the same function, so they cannot drift apart
  again.
- Some cases neither reader covers: an executed token above the run token
  (now invalid by the rule of
  [Document Contract: Schemas, Versioning and Compatibility](document_contract.md)); a finalized result from another generation
  carrying the current token number; two finished records of the same attempt
  that disagree on how it ended; a finalized record that disagrees with its
  own embedded report.
- The order changed: this PRD now precedes the draft
  **Bifrost – CouchDB PlanReader and `_changes` Refresh Hints**.
  Bifrost's adoption therefore lands on its fixtures reader, and that draft
  builds its CouchDB reader against `contract.read` from the start, instead
  of keeping the mapping in the adapter until this PRD lands (its v0.3,
  section 3).

## 3. Principles

Keep five facts separate. Never merge them into one "plan status":

1. **Approval**: `status`.
2. **Current request**: the plan's `plan_generation` and `run_token`, and
   whether its completion is recorded (`executed_run_token == run_token`).
3. **Current request's outcome**: known only when its completion is
   recorded and the last finalized result belongs to that same request.
4. **Latest observed attempt**: from the snapshot. It may belong to another
   request or another generation.
5. **Last finalized result**: from `last_finalized_execution`. It may belong
   to an earlier request.

Example: Alice's request 4 succeeded. Alice asks for a rerun, so the tokens
are now 5/4, and the snapshot still shows request 4's attempt. The correct
reading is "Request 5: not finished. Last finished request (4): succeeded."
Showing "succeeded" would claim a result for work that may not have started.

Further principles:

- A recorded `running` is what was observed, not proof that anything is
  alive now.
- The plan and the snapshot are read at different moments; disagreement from
  lag is normal, not corruption.
- Missing (confirmed absent), unavailable (the read failed) and invalid (read,
  but unusable) are different.
- A problem affects only the answers that depend on it.
- IDs are compared for equality only, never parsed; a missing ID never counts
  as a match. Snapshot IDs are built forwards from a plan and never taken
  apart (the reader offers no parser).
- **Equality is typed.** Two recorded values match only when both have the
  type the contract gives the field and are equal: an ID, a generation, a
  policy, an outcome or a termination reason is a non-empty string; a token
  is an integer that is not a boolean. `true` never equals `1`, `"5"` never
  equals `5`. A value of the wrong type is reported (`wrong_type`) and
  treated as missing by every comparison, so it can neither confirm nor
  contradict anything.
- Control state is explicit, never a default (section 6 of
  [Document Contract: Schemas, Versioning and Compatibility](document_contract.md)). The reader fills
  in nothing: an absent control field is a reported problem and an unknown
  answer.
- The reader is pure and silent. It never reads storage, the clock or
  configuration, and it never logs. A caller that refuses, skips or omits a
  document on the reader's verdict reports that itself (4.7).

## 4. Rules

### 4.0 Version first; inspect-only documents

Every whole-document read starts with `version_of(kind, document)` (from
[Document Contract: Schemas, Versioning and Compatibility](document_contract.md)). Only
a document whose state is `supported` is read by the rules below. An
`unversioned`, `invalid` or `unsupported` document is **inspect-only**: the
result carries its version state, one problem at `schema_version` (code
`unversioned`, `invalid_version` or `unsupported_version`), and the document
body untouched for a consumer that wants to show recorded values as text. The
reader derives nothing from it: no request state, no eligibility, no identity
check, no correlation verdict other than `unknown`, no step status. The
family says where identity and control fields live; outside a supported
family the reader does not know where to look.

A document that is not a JSON object is **invalid** (problem at the root,
code `wrong_type`) and likewise yields no conclusion.

The value helpers of 5.3 (`generation_of`, `finishes_request`,
`step_status`, the snapshot ID functions, the control checks of
[Document Contract: Schemas, Versioning and Compatibility](document_contract.md)) do not check
the version: they are for a caller that has already established the family,
as the daemon's gates do by running `version_of` first.

### 4.1 Request state

Tokens are written `run_token / executed_run_token`; the table assumes a
supported family, valid tokens (4.2) and matching identities (4.3). "Request
4's attempt" is an observed attempt whose `request_and_observation` verdict
is `current` (5.3).

| Evidence | Reading |
| --- | --- |
| `4 / 3`, no attempt observed for request 4 | Request 4 is not finished. No attempt is known; execution is not ruled out. |
| `4 / 3`, snapshot shows request 4's attempt running | An attempt was observed running. The request is not finished. Present liveness is unknown. |
| `4 / 3`, snapshot shows request 4's attempt finished and successful | A successful attempt was observed; its completion is not recorded on the plan. Do not guess why. |
| `4 / 3`, snapshot shows request 4's attempt ended `failed_fast` | The attempt failed and ended; under `fail_fast` the request stays unfinished and eligible. A finished attempt is not a finished request. |
| `4 / 4`, finalized result for request 4 says failed | Request 4 finished and failed. Equal tokens do not mean success. |
| `4 / 4`, no usable finalized result | Request 4 finished; its outcome is unknown. |
| `4 / 4`, finalized result carries token 4 but another `plan_generation` or `plan_id` | Request 4 finished; its outcome is unknown. The result is shown as the last finalized result, labelled with its own generation (4.4 rule 2). |
| `4 / 4`, finalized result disagrees with its own `report` | Request 4 finished; its outcome is not stated; the record is flagged (4.4 rule 4). |
| `5 / 4`, finalized result and snapshot describe request 4 | Request 5 is not finished. Request 4's result is the last finished request's, not request 5's outcome. |
| Tokens invalid (4.2), including `3 / 4` | Request state unknown; the plan is not eligible; actions are refused. Nothing is repaired by reading. |

Which endings finish a request is `finishes_request`, today in
`yggdrasil/storage/plan_updates.py` over `flow` enums, moved here with plain
string inputs and **today's truth table exactly**:

| `outcome` | `termination_reason` | `failure_policy` | Finishes the request |
| --- | --- | --- | --- |
| `succeeded` | `completed` | any value, recognised or not | yes |
| `succeeded` | anything else | any | no |
| `failed` | `completed` (drained) | `continue_independent` | yes |
| `failed` | `preflight_rejected` | `continue_independent` | yes |
| `failed` | `completed` or `preflight_rejected` | `fail_fast`, or any other value | no |
| `failed` | `failed_fast`, `cancelled`, `orchestration_error`, or any other value | any | no |
| any other value | any | any | no |

A succeeded, completed attempt finishes the request whatever the policy
says, as today: the policy governs only how a failure ends. The port
tightens nothing (D10). The rows for unrecognised values are new only
because the enum signature could not express them; the string version
requires `outcome == "failed"` explicitly in the failure branch, where the
enum version had it by exhaustion.

### 4.2 Tokens: one rule everywhere

The rule is defined in [Document Contract: Schemas, Versioning and Compatibility](document_contract.md)
(`contract.controls.token_pair`) and applied at the Yggdrasil sites since
that PRD's release. The reader calls the same function, and
every consumer through it. Restated because the reader's answers depend on
it. A token pair is valid when:

- both `run_token` and `executed_run_token` are present;
- each is a JSON integer that is not a boolean (in Python: `type(v) is int`);
- `0 <= run_token <= 2^53 - 1` and `-1 <= executed_run_token <= 2^53 - 1`;
- `executed_run_token <= run_token`.

Anything else is invalid control state: absent, `null`, text, a float
(including `5.0`), a boolean, out of range, or executed above requested. The
upper bound is the largest integer a browser reads back exactly; with one
rerun per click it is never reached in practice, and the follow-on draft
**Document Contract: Approve and Rerun Helpers** refuses a rerun that
would pass it. Tokens are never rounded or converted anywhere, including in a
browser.

**No data check is needed before release.** Under
[Document Contract: Schemas, Versioning and Compatibility](document_contract.md) every supported
document is written by a release with this rule: planning writes 0 and -1,
the finalizer writes the admitted integer, actors will write through the
helpers of **Document Contract: Approve and Rerun Helpers**.
Pre-contract documents are unsupported anyway. A non-integer token in a
family-1 document can only come from a hand edit, and the right answer to it
is "invalid".

### 4.3 Identity and correlation

Ported from operational-views sections 6.2–6.4 and 7, with the version check
of 4.0 in front, `null` treated as missing, and every comparison typed
(section 3): a generation or execution ID that is not a non-empty string, or
a token that is not an integer, is reported as `wrong_type` and treated as
missing, so it never matches and never conflicts.

| Situation | Reading |
| --- | --- |
| Plan document's `_id` differs from the ID it was requested or stored under | Plan invalid as a whole (problem code `identity_mismatch` at `_id`): diagnostics only, no control state, no eligibility, no action. A body never establishes its own identity. |
| Snapshot names a different `plan_id` or `realm` | Snapshot invalid as a whole for this plan (`identity_mismatch`), excluded from its view and flagged; the plan's facts stay available. |
| Snapshot is an attempt projection without an attempt object, or without a non-empty `execution_id`; a legacy projection that carries an attempt; an unrecognised `projection`; a `steps` value that is not a mapping, a step ID that is empty, a step entry that is not an object, or, on any step entry, a `direct_blockers` or `failed_ancestors` that is absent or not a list of non-empty strings | Snapshot invalid as a whole (`invalid_structure`): a partly readable step inventory is never returned. |
| Generations both present and equal | `same_generation`: steps may be associated by step ID when the observation is an attempt projection and both step lists are readable. Steps present on one side only stay visible. |
| Generations both present and different | `different_generation`: the observation is shown separately, labelled with its generation; its results never apply to the current generation. Which is older is unknown. |
| A generation absent, `null` or not a non-empty string on either side; legacy projection; no observation; observation unavailable, invalid or inspect-only; plan inspect-only | `unknown`, with the reason. |
| Finalized record and observed attempt with equal `execution_id`, and generation and token not in conflict | `same_execution`. |
| Different `execution_id` | `different_execution`: one request can have several attempts. |
| Equal `execution_id`, conflicting generation or token (both present, of the right type, and different) | `identity_conflict`: flagged; no shared identity is claimed. |
| Finalized record absent, unusable (4.4 rule 4) or without an `execution_id`; observation missing, unavailable, invalid, inspect-only or legacy | `unknown`, with the reason. |

An absent blocker list is the same structural fault as one of the wrong
type: the family-1 schema requires both `direct_blockers` and
`failed_ancestors` on every step entry (`plan_status.v1.json`), and every
writer output carries them, legacy projections included. The structure is
checked on every step entry, whatever its status; what the lists mean is
read only for a blocked step (4.9). Bifrost today reads an absent list as
empty; the companion lists the visible change.

### 4.4 Rules added from the v0.9 discussion

1. **Executed above requested** is invalid control state (4.1, 4.2). Already
   enforced by `token_pair`, from
   [Document Contract: Schemas, Versioning and Compatibility](document_contract.md);
   stated here because it changes the request
   state table's last row.
2. **A finalized result belongs to the current request** only when its
   `plan_id` equals the ID the plan was read under, its `plan_generation`
   equals the plan's and its `run_token` equals the plan's `run_token`, all
   three as typed comparisons. The current request's outcome and
   termination reason are supplied only when completion is recorded
   (`finished is True`) and the finalized record is usable and belongs to
   that request (fact 3). Otherwise the record is fact 5 only, shown with its
   own generation and token. A record never establishes completion by
   itself: with tokens `4 / 3` and a usable record for request 4, request 4
   is not finished, its outcome and termination reason are unknown, and the
   record stays available as the last finalized result. Bifrost today
   matches on the token alone; the companion lists the visible change.
3. **Conflicting endings** exist only between two explicit, usable terminal
   claims about the same attempt, after the identity checks of 4.3 pass: a
   finalized record and a snapshot attempt in state `finished`, with equal
   `execution_id` and no `identity_conflict`, whose `outcome` or
   `termination_reason` are both recognised values and differ. Both values
   are shown and the conflict is flagged (`conflicting_endings`); no winner
   is picked. A snapshot that still shows the attempt `running`, or has no
   outcome yet, next to a finalized result is ordinary lag, not a conflict.
   Missing or unrecognised values are incomplete evidence, not disagreement.
4. **Record against its own report.** `last_finalized_execution` and its
   embedded `report` must agree on `execution_id`, `plan_id`,
   `plan_generation`, `run_token`, `failure_policy`, `outcome` and
   `termination_reason`: the seven fields of the finalizer's own identity
   (`ExecutionFinalization.identity()`), which `build_finalized_document`
   copies from the report. Each of the seven must be present in both, of the
   contract's type (six non-empty strings; `run_token` an integer that is not
   a boolean) and equal; types are checked before values are compared, so
   `true` and `1` never count as equal tokens. Otherwise the record is
   **unusable**: the request's outcome is not stated, the record is excluded
   from correlation (4.3) and the disagreement is flagged
   (`record_report_disagree`, naming the fields). The finalizer never writes
   such a record, so this means a foreign or hand-made write. The report's
   own `plan_generation` and `run_token` are nullable in the attempt-report
   schema for the engine's sake; a finalized record is only written for an
   admitted request, which always has both. A test over the corpus confirms
   every finalized corpus document passes this rule (section 8).

### 4.5 Plan-local conclusions and composition

`read_plan(document, plan_id=...)` gives what the plan alone establishes
(4.0, 4.1, 4.2, 4.4 rules 1, 2 and 4, eligibility 4.7). `read_snapshot`
gives what the snapshot alone establishes (4.0, its identity and structure
checks of 4.3, step statuses 4.8, step observations 4.9). `correlate(plan,
snapshot)` adds what the pair establishes (4.3's verdicts, the observed
attempt's request identity of 5.3, 4.4 rule 3) and qualifies a plan-local conclusion that the other record contradicts, without
overwriting it: both the recorded facts and the qualification stay visible.
`correlate` never changes a `PlanRead` or `SnapshotRead`; it returns a third
record.

### 4.6 What the caller tells the reader

For each document the caller passes a body, `MISSING` (the store confirmed it
does not exist) or `UNAVAILABLE` (the read failed). Bifrost's `PlanReader`
port already expresses the three (a body; `None` or an absent key for
missing; `PlanStoreUnavailable` / `OperationsStoreUnavailable` raised for
unavailable), and its live reader (the draft
**Bifrost – CouchDB PlanReader and `_changes` Refresh Hints**) keeps that mapping.

| Input | Reading |
| --- | --- |
| Snapshot `MISSING` | "No observation." Not evidence that nothing ran. |
| Snapshot `UNAVAILABLE` | "Observation unavailable", with the problem; never shown as absent. |
| Snapshot ID cannot be derived (plan identity or scope unusable, 5.3) | "Lookup not possible"; not the same as absent. The caller never asks the store. |
| Snapshot inspect-only (4.0) | Labelled with its version state; no attempt, no step statuses, no correlation from it. |
| Snapshot is a legacy projection | Labelled legacy step observations; no current attempt and no request completion from it. |
| Plan `MISSING` | Approval and request state unavailable; a snapshot already at hand can be shown as diagnostics; no action. |
| Plan `UNAVAILABLE`, invalid or inspect-only | Plan state unavailable, invalid or inspect-only; deletion is never inferred from a failed read. |

A readable, supported plan stays available when its snapshot is missing,
unavailable, invalid, unversioned or unsupported, and those snapshot problems
alone never block approval or rerun.

### 4.7 Eligibility, in order

`eligibility(document)` is document-local (no expected ID: the daemon checks
the embedded `plan.plan_id` against the stored ID separately at admission,
and the recovery queries judge documents as they come from the store). It
returns a structured decision with a closed reason code and the reason text
the daemon logs today, in this order:

| Check | Not eligible, reason code |
| --- | --- |
| Not a JSON object | `invalid_document` |
| Version state not `supported` (4.0) | `unsupported_document` |
| Token pair invalid (4.2) | `invalid_tokens` |
| `status` absent | `status_missing` |
| `status` not `approved` (including `pending`, `draft`, or any other text) | `not_approved` |
| `run_token == executed_run_token` | `already_executed` |
| otherwise | eligible, code `eligible` |

The order after the version check is `plan_eligibility._judge`'s today.
`read_plan` checks identity after the supported-family check and before
eligibility: a stored ID differing from the requested one yields
`identity_mismatch`, which the document-local function cannot see.
Inspect-only documents get no identity interpretation (4.0).
`is_eligible` and `eligibility_reason` are derived from the same
decision; the text is not a machine protocol, the code is.

**Where the version check is new.** Three callers judge documents straight
from the store without a family gate: `query_approved_pending` in
`couchdb/plan_store.py` and `sqlite.py` (the recovery listing), and
`watchers/plan_watcher.py`, which calls `is_plan_eligible` on every change
and in scoped recovery. They stop treating unsupported-family documents as
eligible. Admission refused those documents already, at WARNING
(`ExecutionStatus.UNSUPPORTED_DOCUMENT`; the code's own comment: every
pre-contract plan resolves this way until it is regenerated, an operator's
task, not a malfunction). Its message names the plan, the recorded
`schema_version` as a bounded description, the version state and the
supported plan-document families, and says that nothing ran and nothing was
written.
The daemon's own gate order (family gate, authority, eligibility, control
checks, deserialization, embedded ID, generation) is unchanged.

**Reporting refusals (D9).** The reader logs nothing. Because the three
callers now refuse earlier what admission refused at WARNING, each reports
the refusal itself, or an operator would lose the only signal that a plan
needs regenerating:

| Caller | `unsupported_document` | Other codes |
| --- | --- | --- |
| PlanWatcher, change path | WARNING when evaluating a refused change: the plan ID, the recorded `schema_version` described as admission describes it, the version state and the supported families. Nothing is emitted. | `invalid_tokens` / `status_missing`: WARNING within authority/owner scope; other skips DEBUG |
| PlanWatcher, scoped recovery | WARNING, same content | `invalid_tokens` / `status_missing`: WARNING within authority/owner scope; other skips DEBUG |
| `query_approved_pending`, both stores | WARNING per omitted document during recovery listing, same content; the "Found N eligible plans" INFO line stays | omitted silently, as today |
| Admission | unchanged | unchanged |

**Malformed-control warnings (D21).** The watcher reports `invalid_tokens`
and `status_missing` at WARNING for supported-family documents within its
execution-authority/owner scope, including malformed drafts: token validity
is checked before approval status. Include the plan ID, reason code and
bounded diagnostic; emit no execution event. Ordinary `not_approved` and
`already_executed` skips stay DEBUG. Place these warnings after scope
filtering on both change and scoped-recovery paths; scoped recovery
currently judges eligibility first, so changing a log-level call alone is
insufficient. Preserve eligibility and authority decisions. The store
listings' other-code policy stays as in the table. Section 5 of the draft
**PlanWatcher Reliability and Operational Logging** owns broader logging
investigations, not this bounded change.

**No plan-refusal warnings on idle polls (D9, D21).** Preserve the existing
change-feed cursor and checkpoint behaviour: processed changes advance the
cursor even when the plan is skipped. A later empty poll does not evaluate
that document again or repeat its warning. Emit these warnings where a
change or recovery document is evaluated, not from a periodic poll timer;
do not add a recovery scan to every idle poll. At 7ace54d the change stream
and watcher already provide this behaviour.

Actual document changes (including unrelated edits), replay, restart and
explicit recovery scans may report the same refusal again. This is accepted
for this PRD. Do not add warning caches, hashes, remembered refusal state or
extra processed-change tracking. Revisit warning-volume reduction only if
operational experience shows a problem; it is not a prerequisite here.
Test a refused change followed by successive empty polls: the cursor moves
past the change, no execution event is emitted, and no further plan-refusal
warning appears. The shared reader, including timestamp interpretation,
remains silent. Admission's existing logs are unchanged.

**Pending finalization (D9).** Today the daemon's family gate drops a pending
finalization with a warning when it reads an unsupported document, before
admission. After this PRD the watcher no longer hands such a document to
the daemon, so the gate does not see it: the pending entry stays in the
executor until a later coordinator read applies the existing settling
rules. Restoring the captured family and generation with the same token
leaves it pending; a strictly newer token permits one bounded retry cycle
for that token. A changed generation supersedes the pending result without
writing it into the new generation when the coordinator next reads it.
Regeneration normally creates a draft, so that read is not triggered by
regeneration alone: subsequent approval or an explicit coordinator request
is needed. Pending state is memory-only and does not survive restart.
The watcher's WARNING is the operator's signal meanwhile. The family gate
stays for callers that bypass the watcher. Test unsupported filtering,
same-token retention, draft regeneration without a coordinator call, and
supersession on the later approval/read. No new cleanup mechanism is added.

### 4.8 Step status

The rule Bifrost's `snapshot.step_status` applies today, pinned by its tests
and moved as is (D4). A step entry has a `state` (one of `pending`,
`interrupted`, `unreached` or `unknown`, or the type of its latest `step.*`
event; the projection writes `unknown` for a legacy step with no lifecycle
event) and an `outcome` (`succeeded`, `reused`, `failed`, `blocked`, or
`null`). The
projection writes an outcome only beside the event that records it:
`step.succeeded` → `succeeded`, `step.skipped` → `reused`, `step.failed` →
`failed`, `step.blocked` → `blocked`.

| `outcome` | `state` | Status | Contradiction flagged |
| --- | --- | --- | --- |
| absent (no `outcome` key) | any, or absent | `unknown` | no; `missing_field` at the outcome |
| any, or absent | absent (no `state` key) | `unknown` | no; `missing_field` at the state |
| recognised | the event that records it | the outcome | no |
| recognised | anything else | `unknown` | yes: outcome contradicts state |
| present but unrecognised (any type) | any | `unknown` | no |
| `null` | `pending`, `interrupted`, `unreached`, `unknown` | that word | no |
| `null` | a terminal event type (`step.succeeded`, `step.skipped`, `step.failed`, `step.blocked`) | `unknown` | yes: terminal state without outcome |
| `null` | another `step.*` event (`step.started`, `step.progress`) | `started` | no |
| `null` | anything else, including non-text | `unknown` | no |

Both keys are required on every step entry, and an absent key is not
`null`. The entry records no value to derive a status from, so the first
two rows apply before any other: each absent key gets its own
`missing_field`, two when both are absent; the status is `unknown`; and no
contradiction is flagged. The entry's independent fields are still read as
4.9 says, each with its own diagnostics. `read_snapshot` applies these
rows; `step_status` takes recorded values and reads `None` as `null`.
Explicit `null` follows the other rows: a terminal state beside a `null`
outcome, and a recognised outcome beside a `null` state, are
contradictions. Bifrost today reads an absent key as `null`; the companion
lists the visible change.

`started` is recorded past activity, not proof of present activity. The
status vocabulary is `pending`, `started`, `succeeded`, `reused`, `failed`,
`blocked`, `interrupted`, `unreached`, `unknown`, as in Bifrost today. A new
step lifecycle event is a contract change to assess under section 5 of
[Document Contract: Schemas, Versioning and Compatibility](document_contract.md),
rules 5 and 6 (compatible keeps the number, incompatible raises it): an
older reader shows it as `started`.

**Settled.** An attempt's results are provisional until its report closes
them: `settled` is true when the attempt's `state` is `finished`, false when
`running`, unknown otherwise. The reader states it; what depends on it is
4.9's failed-upstream rule and, beyond that, the consumer's.

### 4.9 Step observations: progress, error, blockers

The rest of what a step entry means, today in Bifrost's `_observed_step` and
`_step_error` and pinned by its tests (D4 as amended, 2026-11-03). Each is a
rule about Yggdrasil's documents, not about display, so leaving it behind
would leave part of the interpretation duplicated. Bounded to the fields the
projection writes today; nothing about jobs, events or artifacts is added.

| Field | Rule |
| --- | --- |
| `ts` | `recorded_at`: a usable timezone-aware timestamp, parsed tolerantly through 4.10, or unknown with `bad_timestamp`. `null` means no available time without a timestamp problem; an absent required key is `missing_field`. Preserve the raw value in the entry. It is the event time retained by the projection, potentially supplied by a realm; it is not necessarily the terminal outcome's time or a framework-assigned publication time. A bad time alone does not invalidate the step, snapshot or other conclusions. |
| `progress` | A percentage only when `state` is `step.progress`: the projection writes 0 for any other event and 100 for a completed one, which are not reports of progress. The value must be a number that is not a boolean, finite, from 0 to 100, taken as is (no rounding, no clamping); otherwise `out_of_range` or `wrong_type` and unknown. |
| `error` | `null`: no failure. Not an object: `wrong_type`, unknown. Otherwise one reading for both stored shapes (the `step.failed` event's `error`, `kind`, `code`, `advice` while the attempt runs; the report's failure record, which also names `error_type`, once it has ended): `message` from `error`, `kind` recognised as `permanent` or `transient` else unknown beside the recorded text, `code`, `advice` and `error_type` as text or unknown. |
| `direct_blockers`, `failed_ancestors` | Interpreted only for a step whose status is `blocked`. Their structure is checked on every step entry, whatever its status: both present, each a list of non-empty strings, else the snapshot is invalid (4.3). Each ID is returned with whether it names a step in the same snapshot's inventory (`found`). `failed_upstream` is given only when the attempt is settled (4.8): the projection records a blocked step's failed ancestors in full only when the report closes, so until then the stored list is provisional and the reader returns "not established yet", never the partial list. |
| `step_name` | text or unknown. |
| `run_id`, `fingerprint`, `job`, `artifacts`, `metrics` | Not interpreted. Passed through in the raw entry for a consumer that shows them. |

What a consumer still does with the result: rows, association with the
planned steps (4.3, `same_generation`), counts, the latest step time, links,
wording and formatting (section 6).

### 4.10 Timestamps: fractional allowance and tolerant reading

This is a bounded addition to this PRD, agreed by the owner on 2026-11-06.
This PRD follows [Document Contract: Schemas, Versioning and Compatibility](document_contract.md)
directly. The separate timestamp-ownership amendment is
withdrawn; none of its publication or projection changes is a prerequisite.

**Canonical producer profile.** Extend that PRD's timestamp spelling to
`YYYY-MM-DDTHH:MM:SS[.fraction]Z`: ASCII digits, uppercase `T` and `Z`, UTC,
and a real calendar date/time in years 0001–9999. An optional fraction has
1–6 digits and ends in a nonzero digit; omit an all-zero fraction and trim
insignificant trailing zeros when formatting a new value. This keeps one
canonical spelling per represented instant. Leap seconds, impossible dates
and precision beyond six digits are outside this producer profile.

`contract.timestamps.is_canonical` and the timestamp definitions in
`plan_document.v1.json`, `plan_status.v1.json` and `attempt_report.v1.json`
must agree. Set `maxLength` to 27 and use a strict end-of-string constraint,
for example:

```regex
^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{0,5}[1-9])?Z(?![\s\S])
```

Keep the explicit calendar/date-time check. Merely increasing the old
length limit while retaining `$` would admit some final-newline strings
in Python-backed schema validators. The generic `is_date_time` check may
remain broader than canonical producer conformance.

**Reader interpretation is separate from producer conformance.** Add a
clock-free `contract.timestamps.parse_timestamp(value: object) -> datetime
| None`, returning an aware UTC value or `None` for unusable input, without
raising for stored content. Use it consistently for the plan, finalized
record, attempt, snapshot-write and step-observation timestamps in 5.3.
Canonical spelling is not a reader gate. The reader accepts the union
(D22) of RFC 3339 `date-time` as `is_date_time`, from
[Document Contract: Schemas, Versioning and Compatibility](document_contract.md), recognises it and the
timezone-aware forms accepted by Bifrost's existing Python 3.11
`datetime.fromisoformat` path, subject to the precision and representability
limits below. The accepted and rejected forms are characterized by the
named cases below; this is not a promise to support every ISO 8601 form.
Read numeric offsets, noncanonical fractional zeros and lowercase `t`/`z`,
plus the existing space separator, compact date/time and week-date forms.
Python 3.11 does not accept ordinal dates today. Convert usable instants to
UTC while retaining the recorded text. An RFC-only validation gate must not
prevent the ISO path from being tried. Lowercase `z` handling is confined
to the RFC 3339 path. Pass the original text unchanged to the ISO path,
preserving its existing acceptance of lowercase `t`, including the compact
`20261106t102030Z`. Do not case-normalize the text before trying the ISO
path: that would also accept `2026-11-06 10:20:30z`, which neither grammar
accepts and which stays unknown. This concerns accepted input only;
canonical producers continue writing uppercase `T` and `Z`.

Built-in clocks emit canonical seconds, but realm overrides can already put
these extra ISO forms into snapshots. Adopting the shared reader must not
make those observations unreadable merely because they are not canonical
producer output or RFC 3339. This interpretation belongs in the shared
reader, so external consumers need no private fallback. An input without a
timezone is unknown; no local timezone is assumed. Additional spellings can
be supported later without rewriting stored observations; precision beyond
microseconds is a separate future representation decision.
Calendar errors and UTC-conversion overflow are reported by the caller as
`bad_timestamp`. The caller also distinguishes a valid nullable field from
an absent required field. Helpers do not log or rewrite recorded text.

Guard fractional components before parsing: at most six digits, with no
silent rounding or truncation. Excess precision returns unknown plus
`bad_timestamp`, retaining the raw value. This precision bound is an
explicit exception to the otherwise tolerant reader allowance of
[Document Contract: Schemas, Versioning and Compatibility](document_contract.md) and corrects
Bifrost's current `datetime.fromisoformat` path, which silently truncates
excess digits (`.1234567` reads as `.123456`, and `,1234567` likewise). The
guard covers a fraction introduced by `.` or `,` wherever an otherwise
accepted time or offset form permits one, on both grammar paths, before
parsing can truncate anything; do not let the ISO fallback bypass it. Test
six-digit acceptance and seven-digit rejection for both separators in both
the time and the offset component. The remaining existing acceptance is
covered by the characterization tests ported in delivery step 2. Canonical
producer spelling is unchanged: a canonical fraction uses `.`; this guard
and the cases below concern reader interpretation only. A timestamp
problem affects only time-dependent answers, not request state, step
outcome or identity correlation.

**Narrow exceptions to Python's reading (accepted 2026-11-10).** Three kinds
of text that Python 3.11's parser accepts are unknown, with a field problem
and the raw value retained. Text containing a NUL: the parser can stop
checking at one, and reads `2026-11-06T10:20:30Z\x00junk` as a timestamp.
Digits after the seconds of the time or of the offset that neither `.` nor
`,` introduces: the parser reads them as a fraction after a colon
(`2026-11-06T10:20:30:1234567+00:00`, `2026-11-06T10:20:30+00:00:00:000001`)
or directly (`2026-11-06T1020301234567+00:00`,
`2026-11-06T10:20:30+000000000001`), truncating it beyond six digits or
losing it from an offset whose whole seconds are zero, and drops a single
such digit before the zone (`2026-11-06T10:20:307Z`). This extends the guard
above to fractions it cannot see. And because that check reads the text
without parsing the date, a colon or a digit used between the date and the
time looks the same and is refused too (`2026-11-06:10:20:30Z`,
`202611065102030Z`), though the parser accepts any one character there.
These are text checks before both grammar paths, not a grammar engine; every
other case in this section keeps its answer.

| Example | Canonical producer value | Reader |
| --- | --- | --- |
| `2026-11-06T10:20:30Z` | yes | exact instant |
| `2026-11-06T10:20:30.1Z`, `2026-11-06T10:20:30.000001Z`, `2026-11-06T10:20:30.123456Z` | yes | exact fraction retained |
| `2026-11-06T10:20:30.100Z`, `2026-11-06T10:20:30.000000Z` | no | exact instant; raw spelling retained |
| `2026-11-06T10:20:30+00:00`, `2026-11-06T11:20:30+01:00` | no | same UTC instant as the first row |
| `2026-11-06t10:20:30z` | no | same instant (lowercase `z` is newly readable; lowercase `t` with uppercase `Z` already works in Bifrost) |
| `2026-11-06 10:20:30Z`, `20261106T102030Z`, `20261106t102030Z`, `2026-W45-5T10:20:30Z` | no | same instant as the first row; existing ISO reader support retained, including lowercase `t` |
| `2026-11-06T10:20Z`, `2026-11-06T10Z`, `2026-11-06T10:20:30,5Z`, `2026-11-06T10:20:30+01:00:00` | no | existing ISO forms, asserted as instants: 10:20:00, 10:00:00, 10:20:30.5 and 09:20:30 UTC on 2026-11-06 |
| `2026-11-06T10:20:30.1234567Z`, `2026-11-06T10:20:30,1234567Z`, `2026-11-06T10:20:30+01:00:00.1234567` | no | unknown with a field problem: a seventh fractional digit after `.` or `,`, in the time or in the offset |
| `2026-310T10:20:30Z`, `2026-11-06T24:00:00Z`, `2026-11-06T10:20:30+24:00`, `2026-11-06 10:20:30z`, timezone-free text, impossible date, wrong type | no | unknown with a field problem |
| `null` in step `ts` | unconstrained observation field | unknown, no timestamp problem |

**Characterization cases (D22).** The reader's acceptance is pinned by
named cases, each asserting the expected UTC instant rather than mere
parsing success. Accepted, beside the table's examples: the no-seconds form
`2026-11-06T10:20Z`, the hour-only form `2026-11-06T10Z`, the comma
fraction `2026-11-06T10:20:30,5Z` and the seconds-bearing offset
`2026-11-06T10:20:30+01:00:00`. Rejected, unknown with `bad_timestamp` and
the raw value retained: an ordinal date (`2026-310T10:20:30Z`),
`2026-11-06T24:00:00Z`, an offset whose magnitude is 24 hours or more
(`+24:00`), timezone-free text (`2026-11-06T10:20:30`,
`2026-11-06 10:20:30`) and `2026-11-06 10:20:30z`. Six digits after `.`
or `,` are accepted and seven rejected, in the time and in the offset
component alike. These tests are compatibility checks for Python upgrades:
they detect a change in the cases they name, not every possible expansion
of the interpreter's grammar, and a change in what they accept is reviewed
as a reader change, never absorbed silently. No separate grammar engine is
written for this.

**Implementation note: exact sub-second offsets.** Test offsets with a
nonzero fractional component and a zero whole-second component, for both
signs and both fraction separators. In CPython 3.11,
`datetime.fromisoformat` treats these as UTC and loses the offset fraction
([`tzinfo_from_isoformat_results`](https://github.com/python/cpython/blob/3.11/Modules/_datetimemodule.c#L1401-L1411)).
For example, `2026-11-06T10:20:30+00:00:00.000001` must read as
`2026-11-06T10:20:29.999999Z`; the corresponding negative offset must read
as `2026-11-06T10:20:30.000001Z`. Include comma equivalents. The reader must
preserve these exact instants rather than delegate blindly to that parser.
This is an implementation requirement under the accepted offset grammar,
not an additional timestamp-policy decision.

**Supersession of D5 in Document Contract: Schemas, Versioning and
Compatibility (accepted 2026-11-06).** This PRD changes the whole-second canonical profile of
[Document Contract: Schemas, Versioning and Compatibility](document_contract.md)
to permit the fraction above, and replaces
its promise of reading "any valid RFC 3339 UTC form" with the supported
timezone-aware RFC 3339 / ISO union, numeric-offset conversion to UTC and
explicit six-digit precision limit. Producer conformance and reader
interpretation remain distinct. The original decision of that PRD remains the
historical record; delivery step 6 updates the maintained contract reference
and promoted specification with a dated note attributing the changes to
this PRD's 4.10, D18 and D22. No separate amendment or intervening release
is added.

**Production and replacement.** `utcnow_iso()` and all current clock-driven
writers continue emitting whole seconds. There is no precision setting or
fractional clock rollout here. Preserve explicitly supplied canonical
fractions through `build_plan_document(now=...)`,
`build_finalized_document(..., now=...)`, `finalize_execution(..., now=...)`
and the attempt report's supplied `started_at` / `ended_at`. For planning
over a supported-family document, accept canonical fractional `created_at`
and keep it verbatim. Do not round, truncate or replace it with build time.
For an unversioned document, keep `canonical_from_legacy` explicitly limited
to the existing whole-second uppercase `Z` / `+00:00` conversion; widening
`is_canonical` must not accidentally widen this separate migration rule
(today the function re-spells the suffix and then calls `is_canonical`, so
the limit has to be written in, not assumed).

**Canonical formatting (D20).** Add the clock-free
`contract.timestamps.canonical_text(instant: datetime) -> str` here. Accept
an aware datetime, convert its instant to UTC, and produce the canonical
profile above with exact microseconds, trimming only insignificant trailing
zeros. Always pad the year to four digits, including years below 1000;
do not rely on platform-specific `strftime` year padding. A non-datetime
argument raises `TypeError`; a naive datetime or an instant whose UTC
conversion leaves years 0001–9999 raises `ValueError`. This is a typed
formatting API, not a stored-content reader; no clock or local timezone is
consulted. `CANONICAL_FORMAT` may remain the whole-second format constant.
Test canonical output, calendar boundaries, timezone conversion and
`parse_timestamp(canonical_text(x)) == x.astimezone(UTC)`, including
fractional inputs. Current clock producers keep whole seconds. The
follow-on draft **Document Contract: Approve and Rerun Helpers** may
reuse this formatter for supplied click instants; its precision policy is
decided there. Bifrost response serialization need not adopt producer
spelling, but must preserve the parsed instant.

**Event time stays observational.** Snapshot `steps.*.ts` remains required
but unconstrained in the schema and is copied from events as today. Realms
may override it. This work adds no publication facade, override warning,
historical normalizer, snapshot rebuilding requirement or spool migration.
The reader keeps unusable raw observations available for inspection. It
does not infer that an event time was generated by Yggdrasil. Ordering of
execution continues using existing identities and sequences; consumers
that compare times compare parsed instants, not mixed-precision strings.

## 5. Interface

Names are the proposal; exact spellings are settled while writing the tests
(13). Every result is a frozen dataclass with `problems:
tuple[ContractProblem, ...]`. No result raises for stored content of any
shape; programmer errors (a non-string `plan_id` argument, say) may.

### 5.1 Modules

```text
yggdrasil/contract/
    timestamps.py existing module; widened canonical check and clock-free
                  tolerant parse_timestamp and canonical_text (4.10)
    problems.py   ContractProblem and the problem codes, moved out of
                  schema.py (which re-exports them); adds the reader's codes
    fields.py     field names and vocabularies as plain strings
    read.py       the reader (a subpackage with the same public names if it
                  outgrows one module)
```

`fields.py` holds, as constants and frozensets: the plan document's field
names (`_id`, `_rev`, `schema_version`, `realm`, `scope`, `status`, `plan`,
`preview`, `plan_generation`, `run_token`, `executed_run_token`,
`execution_authority`, `execution_owner`, `approvals_required`, `created_at`,
`updated_at`, `approved_at`, `approved_by`, `run_requested_at`,
`run_requested_by`, `last_executed_at`, `last_finalized_execution`, `notes`,
`source_doc_id`, `source_doc_rev`); the snapshot's (`_id`, `_rev`,
`schema_version`, `type`, `realm`, `scope`, `plan_id`, `projection`,
`attempt`, `steps`, `updated_at`) and its attempt's and step entry's; the
finalized record's; and
the vocabularies `APPROVAL_STATUSES` (`draft`, `approved`),
`EXECUTION_AUTHORITIES` (`daemon`, `run_once`), `FAILURE_POLICIES` (reused
from `controls`), `EXECUTION_OUTCOMES`, `TERMINATION_REASONS`,
`STEP_OUTCOMES`, `ERROR_KINDS` (`permanent`, `transient`), `PROJECTIONS`,
`ATTEMPT_STATES`, `STEP_STATES`, `STEP_EVENTS` (`step.started`,
`step.progress`, `step.succeeded`, `step.skipped`, `step.failed`,
`step.blocked`), `OUTCOME_EVENTS` (the four terminal ones, mapped to their
outcomes), and `STEP_STATUSES` (4.8).

**Spellings only (D13).** `fields` is the one place a name is spelled.
`storage.plan_documents` and `ops.snapshot` import their constants from it,
and so do the daemon's messages, but every module keeps its own sets built
from those spellings for its own purpose: `ops.snapshot`'s terminal-outcome
table and failed-event constant, the ops consumer's event sets, the
executor's status maps. No set is merged with another and the projection's
behaviour does not change. A test asserts each vocabulary in `fields` agrees
with its source of truth: the `flow.outcomes` enums, `flow.model`'s policy
constants, `ops.snapshot`'s constants, the event names emitted by `flow.step`
(`step.started`, `step.progress`, `step.succeeded`, `step.failed`) and
`core.engine` (`step.skipped`, and `step.blocked` through
`flow.events.attempt_records.STEP_BLOCKED_EVENT`), and the enumerations in
the v1 schemas. The test proves the spellings agree, not that the sets are
the same.

### 5.2 Problem codes

Reused from `schema`: `unversioned`, `invalid_version`,
`unsupported_version`, `missing_field`, `wrong_type`, `not_in_vocabulary`,
`out_of_range`, `bad_timestamp`, `invalid_tokens`. Added for the reader:
`identity_mismatch`, `invalid_structure` (the snapshot's whole-document
faults of 4.3), `record_report_disagree`, `conflicting_endings`,
`identity_conflict`, `step_record_contradiction`, `invalid_failure_policy`,
`invalid_execution_definition`. Paths follow `ContractProblem`'s convention
(`steps["lane_1/demux"].state`). Messages show recorded values through
`describe_recorded`.

**Control-check problems become structured at their source (D12).** The control checks of
[Document Contract: Schemas, Versioning and Compatibility](document_contract.md),
`token_pair`, `failure_policy_of` and `execution_definition_problems`, return
tuples of strings; the reader must not parse those messages for a path and
must not re-run the checks to learn one. So the checks themselves gain a
structured form, additively: `TokenPair.details` and
`FailurePolicyCheck.details` as `tuple[ContractProblem, ...]`, and a new
`execution_definition_check(document) -> DefinitionCheck(valid, problems,
details)`; the existing `problems` tuples and `execution_definition_problems`
become the `message` strings of those details, unchanged in wording, so the
daemon's log lines do not change. Paths are the checked field
(`run_token`, `executed_run_token`, `plan.failure_policy`, `plan.steps[...]`),
codes `invalid_tokens`, `invalid_failure_policy`,
`invalid_execution_definition`. The reader copies the details into its
results.

### 5.3 Functions and results

```text
MISSING, UNAVAILABLE                  sentinels the caller passes for a
                                      document it could not provide (4.6)

read_plan(document, *, plan_id) -> PlanRead
    plan_id          the ID it was requested or stored under
    version          VersionInfo
    usable           False when not an object, inspect-only, or identity
                     mismatch; nothing below is derived then
    recorded         realm, scope (kind, id), status, execution_authority,
                     execution_owner, notes, created_at, updated_at,
                     approved_at, approved_by, run_requested_at,
                     run_requested_by, last_executed_at: each as a Recorded
                     value (the recognised value or None, and the recorded
                     text), timestamps parsed through
                     contract.timestamps.parse_timestamp (4.10).
                     execution_owner is required text or null, recognised
                     when non-empty: empty text and null name no owner,
                     without a problem
    generation       str | None (non-empty string only)
    tokens           TokenPair
    failure_policy   FailurePolicyCheck
    approval         Recorded status: draft | approved | None
    request          RequestState | None: finished (bool | None); outcome and
                     termination_reason, supplied only when completion is
                     recorded (finished is True) and the finalized record is
                     usable and belongs to that request (4.4 rules 2 and 4);
                     finalized_is_current (bool | None), the record's
                     recorded request identity (below)
    finalized        FinalizedRecord | None: the seven identity fields,
                     finalized_at, counts, usable (4.4 rule 4)
    eligibility      Eligibility: eligible, code, reason (4.7)
    steps            tuple[PlannedStep, ...] | None: step_id, name, deps
                     (each with found-in-this-inventory); None, with a
                     problem at the path, when the inventory cannot be
                     returned complete (plan or plan.steps absent, null or
                     of the wrong type; a step that is not an object; a
                     missing or duplicate step_id; deps absent or not a list
                     of strings). An explicit empty plan.steps or deps list
                     is known empty, not unavailable. Bifrost today reads an
                     absent plan.steps or deps as empty; the companion lists
                     the visible change
    snapshot_id      str | None (snapshot_id_for, below)
    document         the body as given (for inspect-only display)
    problems

read_snapshot(document, *, plan_id, realm) -> SnapshotRead
    version, usable, document, problems   as above
    projection       attempt | legacy | None
    written_at       the snapshot's updated_at, parsed; a write time, not
                     execution progress
    attempt          Attempt | None: execution_id, plan_generation,
                     run_token, failure_policy, execution_authority,
                     execution_owner (read as on the plan), state,
                     started_at, ended_at,
                     termination_reason, outcome, is_drained, counts,
                     diagnostic (message, error_type, details; a null
                     diagnostic means no diagnostic and produces no
                     problem; a non-null value that is not an object
                     produces wrong_type), settled (4.8)
    steps            tuple[ObservedStep, ...] | None: step_id, name, status
                     and contradiction (4.8), recorded state and outcome,
                     recorded_at, progress_percent, error (StepError:
                     message, kind, code, advice, error_type),
                     direct_blockers and failed_upstream (tuples of
                     StepRef(step_id, found) | None, 4.9), and the raw
                     entry for the fields the reader does not interpret

correlate(plan, snapshot) -> Correlation
    plan             PlanRead
    snapshot         SnapshotRead | MISSING | UNAVAILABLE | LOOKUP_IMPOSSIBLE
    plan_and_observation       verdict same_generation | different_generation
                               | unknown, and a reason from a closed set
    finalized_and_observation  verdict same_execution | different_execution
                               | unknown, and a reason from a closed set
    request_and_observation    verdict current | other | unknown: whether
                               the observed attempt belongs to the plan's
                               current request (recorded request identity,
                               below), and a reason from a closed set
    conflicting_endings        None, or both endings (4.4 rule 3)
    problems

request_state(document, *, plan_id) -> RequestState | None
    read_plan(document, plan_id=plan_id).request. None when the plan cannot
    supply a request state: MISSING, UNAVAILABLE, not an object,
    inspect-only, or stored under another ID. No request state is made up
    for such a plan; a caller treats None as "request state unavailable".
    The expected ID is required, because rule 2 of 4.4 compares the
    finalized record's plan_id with the ID the plan was read under, never
    with the body's own _id
eligibility(document) -> Eligibility             4.7, document-local
is_eligible(document) -> bool
eligibility_reason(document) -> str
finishes_request(outcome, termination_reason, failure_policy) -> bool
generation_of(document) -> str | None
step_status(state, outcome) -> StepStatus(status, contradiction)
partition_name(kind, id) -> str                  formatting only (below)
snapshot_id(realm, scope_kind, scope_id, plan_id) -> str | None
snapshot_id_for(document, *, plan_id) -> str | None
```

`VersionInfo`, `TokenPair` and `FailurePolicyCheck` are the types defined
by [Document Contract: Schemas, Versioning and Compatibility](document_contract.md).

**Which functions check the version (D6).**

| Functions | Version check | For |
| --- | --- | --- |
| `read_plan`, `read_snapshot`, `request_state`, `eligibility`, `is_eligible`, `eligibility_reason`; `correlate` through its inputs | yes, first (4.0) | whole-document reads; the only functions that give conclusions about a document |
| `generation_of`, `finishes_request`, `step_status`, `partition_name`, `snapshot_id`, `snapshot_id_for`; the `token_pair`, `failure_policy_of` and `execution_definition_check` of [Document Contract: Schemas, Versioning and Compatibility](document_contract.md) | no | value helpers for a caller that has already established the family (the daemon's gates run `version_of` first); on an unsupported document they return whatever the named field holds, which is not a conclusion |

**Snapshot ID rule (D11).** `{partition}:plan_status:{realm}:{plan_id}`,
where the partition is `{kind}-{id}`, with the scope's kind as recorded,
for every kind: `project-P1`, `flowcell-FC1`, `test_scenario-S1` (owner,
2026-11-13). Until v0.17 the rule kept the writer's two abbreviations,
`proj-{id}` for scope kind `project` and `fc-{id}` for `flowcell`; they are
retired, and the writer follows this rule (section 9). The formatting is
shared as `partition_name(kind, id)`, which validates nothing, and every
snapshot ID is derived through it: by the reader's `snapshot_id`, and by
the CouchDB and SQLite writers (`couchdb/ops_sink.py`, `storage/sqlite.py`)
through `storage.partitions.partition_key`. The two callers keep their own
acceptance: the reader's `snapshot_id` and `snapshot_id_for` return `None`
unless `realm`, `scope.kind`, `scope.id` and the plan ID are non-empty
strings, and never raise; the writer's `partition_key(scope)` stays in
`storage.partitions` with exactly today's acceptance (any truthy `kind` and
`id`) and today's `ValueError`, formatting through `partition_name`. Nothing
the sink writes today is refused after this PRD.

**Recorded request identity.** `RequestState.finalized_is_current` and
`Correlation.request_and_observation` say whether a record belongs to the
plan's current request, from recorded identity only, compared typed
(section 3). A value counts only when it is valid on its own: the plan ID
and a generation as non-empty strings, a run token as an integer from 0 to
2^53 - 1 that is not a boolean. For the plan that is `TokenPair.run`, which
an invalid `executed_run_token` does not affect. The finalized record
compares its `plan_id` (with the ID the plan was read under),
`plan_generation` and `run_token`; the observed attempt compares its
`plan_generation` and `run_token` (`read_snapshot` has already checked the
snapshot's plan ID).

| Evidence | `finalized_is_current` | `request_and_observation` |
| --- | --- | --- |
| Every compared value counts on both sides, and all are equal | `True` | `current` |
| A comparison whose two values both count establishes a difference, whatever the other values; a differing generation is enough even when the tokens are missing | `False` | `other` |
| The plan is not usable; no usable finalized record, or no readable attempt (the other observation reasons below); or the evidence is insufficient and no difference is established | `None` | `unknown` |

Missing information alone is never a mismatch. Identity is independent of
the request's validity, completion and eligibility: with tokens `4 / "x"`, a
usable finalized record and an observed attempt, both of request 4 and of
the plan's generation, give `finalized_is_current` `True` and `current`,
while `finished` is `None`, the current outcome and termination reason are
unknown, and eligibility is `invalid_tokens`. The outcome guard of 4.4 rule
2 is unchanged. `request_and_observation` gives the first reason that
applies, in this order: `plan_not_usable`, `lookup_impossible`,
`no_observation`, `observation_unavailable`, `observation_inspect_only`,
`observation_invalid`, `legacy_projection`; then the differences
`generations_differ`, `run_tokens_differ`; then the insufficient evidence
`plan_generation_missing`, `attempt_generation_missing`,
`plan_run_token_missing`, `attempt_run_token_missing`; then
`generation_and_run_token_equal`. A difference therefore wins over missing
evidence. "Missing" covers absent, `null`, the wrong type and, for a token,
out of range.

**Correlation reasons** (closed sets): for plan-and-observation,
`generations_equal`, `generations_differ`, `plan_generation_missing`,
`attempt_generation_missing`, `legacy_projection`, `no_observation`,
`observation_unavailable`, `observation_invalid`, `observation_inspect_only`,
`lookup_impossible`, `plan_not_usable`; for finalized-and-observation, the
same observation reasons plus `nothing_finalized`, `finalized_unusable`,
`execution_id_missing`, `execution_ids_equal`, `execution_ids_differ`,
`identity_conflict`; for request-and-observation, the same observation
reasons (`plan_not_usable`, `lookup_impossible`, `no_observation`,
`observation_unavailable`, `observation_inspect_only`, `observation_invalid`,
`legacy_projection`) plus `generations_differ`, `run_tokens_differ`,
`plan_generation_missing`, `attempt_generation_missing`,
`plan_run_token_missing`, `attempt_run_token_missing`,
`generation_and_run_token_equal`. A consumer maps them onto its own
vocabulary.

### 5.4 Moves, adapters and re-exports

| Today | After this PRD | Old name |
| --- | --- | --- |
| `storage.plan_eligibility.is_plan_eligible`, `get_eligibility_reason`, `APPROVED_STATUS` | `contract.read.eligibility`, `is_eligible`, `eligibility_reason`; `contract.fields.APPROVED` | removed with the module, not re-exported (owner, 2026-11-16) |
| `storage.plan_updates.finishes_request(ExecutionOutcome, TerminationReason, str)` | `contract.read.finishes_request(str, str, str)` | kept with its enum signature as a thin adapter (`outcome.value`, `reason.value`); `ExecutionFinalization.__post_init__` and `daemon/plan_execution.py` keep calling it |
| `storage.plan_documents.plan_generation_of` | `contract.read.generation_of`, the only generation helper | removed, not re-exported (owner, 2026-11-13) |
| `storage.partitions.partition_key` | stays, with unchanged acceptance and `ValueError`; formats through `contract.read.partition_name`, so its partitions follow the rule of 5.3 | unchanged |
| `storage.plan_documents.plan_summary_from_document` | stays in `storage`, rebuilt as a projection of `read_plan` (D15) | unchanged name |
| `ops.snapshot` constants, `storage.plan_documents` field constants | spelled in `contract.fields`; the modules import the spellings and keep their own sets | unchanged names |
| `schema.ContractProblem` and codes | `contract.problems`; `schema` re-exports | unchanged |
| `controls.token_pair`, `failure_policy_of`, `execution_definition_problems` | gain `details` / `execution_definition_check` (5.2); string problems unchanged | unchanged |

The call sites switch to the `contract.read` names in the same change,
except where the table keeps an adapter: `daemon/plan_execution.py`
(eligibility at admission, `plan_generation_of`; its `finishes_request` call
keeps the enum adapter), `watchers/plan_watcher.py` (`is_plan_eligible`,
`get_eligibility_reason`, with the reporting of 4.7), `storage/plan_updates.py`
(`plan_generation_of`; `ExecutionFinalization.__post_init__` keeps the enum
adapter), `storage/plan_documents.py` (`plan_generation_of`, inside the plan
summary rebuilt below), `storage/couchdb/plan_store.py` and
`storage/sqlite.py` (eligibility in the recovery listing, with the reporting
of 4.7), `storage/couchdb/ops_sink.py` and `storage/sqlite.py`
(`partition_key`, with unchanged acceptance, formatting through
`partition_name`) and
`daemon/core.py` (`get_plan_summary`). The enum adapter only converts its
arguments and delegates to `contract.read.finishes_request`, so the truth
table has one implementation. No old eligibility or generation name is
kept. `storage.plan_eligibility` is removed in this release (owner,
2026-11-16): code outside the repository imports `contract.read.is_eligible`,
`contract.read.eligibility_reason` and `contract.fields.APPROVED` instead.
`plan_generation_of` is removed too: every caller reads a generation through
`contract.read.generation_of` (owner, 2026-11-13).

**The plan summary (D15).** `plan_summary_from_document` stays in
`storage.plan_documents` with its keys, rebuilt over `read_plan(document,
plan_id=doc_id)`: the stores pass the requested document ID in, so an
identity mismatch is reported rather than hidden. An unusable value is `None`
(the log box prints it as "unknown"); the defaults `"daemon"`, `"unknown"`,
`0` and `-1` go. One key is added (owner, 2026-11-09): `problems`, the
reader's problems as a tuple of `ContractProblem`. Two more are added for
display (owner, 2026-11-16): `unrecognised_status` and
`unrecognised_execution_authority`, the reader's recorded value of `status`
or `execution_authority` when it is text but not a word of the field's
vocabulary, such as `pending`, and `None` otherwise (absent, `null`, not
text, or recognised). The interpreted field stays `None`, and the reader
reports no problem for an unrecognised word; eligibility, execution and the
overwrite guard do not read these keys. The summary exists
whenever a document exists, readable or not, because `daemon/core.py`'s
`_check_plan_overwrite` treats `None` as "no existing plan" and would then
overwrite without the warning and without `--force`. So the stores return
`None` only when the store has no document under that ID; an inspect-only,
invalid or identity-mismatched document yields a summary whose fields are
`None` and whose problems say why. The "Existing Plan Found" box shows
"unknown" for each `None` field. For an unrecognised text status or
authority, the existing line adds the recorded text, bounded and escaped by
`describe_recorded`, so the line stays one line:
`unknown (recorded 'pending')` (owner, 2026-11-16). A value that is not text
is not shown there; its problem is. Only when there are problems, the box
adds one Problems line: the first three messages and a count of the rest,
each message already bounded by `describe_recorded`. The box still aborts
without `--force`.

### 5.5 Properties

- **Purity and layering.** `contract` stays in the bottom layer and imports
  neither `flow` nor `storage` nor `ops`; `tests/test_import_lightness.py`
  is extended to `fields`, `problems` and `read`. Rules that use `flow` enums
  today take plain strings; the agreement test of 5.1 keeps them honest.
- **Silence.** The reader never logs (section 3, 4.7).
- **No stored value makes a reader raise.** That covers malformed document
  content of any shape (the hostile-value sweep of section 8), not
  programmer errors or resource exhaustion. A partly readable step list is
  never returned as complete.
- **Results are immutable** and carry the recorded values beside the derived
  answers, so a consumer can always show what was stored.
- **Bounded diagnostics.** Recorded values in messages go through
  `describe_recorded`; nothing in a result is unbounded except the body a
  caller handed in.
- **Usable from outside the repository.** `import yggdrasil.contract.read`
  loads only the standard library (neither the CouchDB client nor the
  daemon); results are typed dataclasses and `yggdrasil/py.typed` ships in
  the package, so a consumer's strict type checking covers calls into the
  reader; `requires-python >= 3.11` as today. The package's runtime
  dependencies are installed with it; the reader imports none of them.

## 6. Boundary Between the Shared Reader and a Consumer

**Shared reader (Yggdrasil):** what a recorded value means; whether two
records belong together; the step status rule, `settled`, and the step
observation rules of 4.9; the snapshot ID rule; every rule Yggdrasil's own
code decides with. Rule of thumb: if a second consumer, for example a future
`yggdrasil plan status` command, would need the same answer, it belongs in
the shared reader.

**A consumer (Bifrost today):** its API models and field names; mapping the
reader's closed sets onto its own; building rows from planned and observed
steps and associating the two inventories (`associate`, `AssociatedSteps` /
`SeparateSteps`); step counts and the latest step time; list order, search
and filters; links; wording, relative times, refresh, freshness and
connection state. What Bifrost does with these is the companion document's.

**Bifrost – CouchDB PlanReader and `_changes` Refresh Hints** (a follow-on draft, after
this PRD): its CouchDB `PlanReader` returns bodies, missing and unavailable
(4.6) and calls `contract.read`; its v0.4 drops the sentence that the
mapping lives in `bifrost/yggdrasil/` until this PRD lands, and the
adapter-correction items of its review (C1, C2), which the companion makes
moot. The reader offers no snapshot-ID parser; a change hint's plan ID must
come from the changed document's body (that draft's review, G1).

**Document Contract: Approve and Rerun Helpers** (a follow-on draft, after
**Bifrost – CouchDB PlanReader and `_changes` Refresh Hints**): `review_context` and `prepare` can
take their facts from `PlanRead` (identity, version, generation, status,
tokens, authority, owner). This PRD adds no intent, digest or write helper.
That draft's two references to section 4.2 of this PRD correctly resolve to
the token rule restated here and need no change. When that draft is picked
up, specify its click-time precision and reuse
4.10's canonical formatter for supplied instants.

Releasing the step status rule with the engine does not upgrade a consumer
pinned to an older release. A new step lifecycle state is therefore a
contract change to assess under section 5 of
[Document Contract: Schemas, Versioning and Compatibility](document_contract.md),
rules 5 and 6: if an older
reader would show it as `started` while it means something else (say,
"waiting for a licence"), it changes meaning for that reader and is
incompatible.

## 7. Consumers, the Tag and the Companion Document

This PRD specifies nothing inside Bifrost. The companion document on
Bifrost's adoption of the reader, a separate draft, does: the dependency
and its lock, the adapter swap, the fixture regeneration and marker rule of section 9.4 of
[Document Contract: Schemas, Versioning and Compatibility](document_contract.md),
the visible changes and the tests. What this PRD owes a consumer is the
interface of section 5 and the properties of 5.5.

**One tag (D14).** [Document Contract: Schemas, Versioning and Compatibility](document_contract.md)
is merged but not tagged (latest tag `v0.5.0`). One release tag carries
that PRD and this one; Bifrost moves its pin once, to that tag. Fractional
allowance is part of this PRD; there is no intervening amendment.

**Validation before the tag (owner, 2026-11-03).** Before the tag is cut,
Bifrost's integration is validated against a fixed candidate commit of
Yggdrasil `dev` that contains this PRD's work, including 4.10, the verified
current corpus and the first frozen release baseline: Bifrost installs the package
from that commit, regenerates and schema-checks its fixtures as section 9.4
of [Document Contract: Schemas, Versioning and Compatibility](document_contract.md)
requires, swaps its adapter onto `contract.read`, and runs its suites,
with the installed footprint measured and recorded (D16). A problem found
there in the reader's interface or behaviour is fixed on `dev` and the
validation repeated. The tag is cut from the validated commit, and Bifrost's
pin then moves to the tag, which resolves to the same commit. The delivery
plan (8, step 7) and the success criteria (11) carry this clause.
After tagging, verify the locked installation and package-version metadata
against that same commit. The companion covers consumer serialization and
time aggregation, including preservation of fractional instants.

**The first frozen baseline.**
[Document Contract: Schemas, Versioning and Compatibility](document_contract.md)
defines it (its section 16, and
`tests/contract/documents/README.md`, "Releasing a baseline"):
`released/<tag>/` holds the positive examples of a released version,
prepared under the intended tag name before the tag is cut so that the
tagged source contains it; `released/` does not exist until the first one
is prepared, and a published tag is never moved to add one. Under D14 the
first baseline is this PRD's. It is prepared between delivery steps 6
and 7, from `current/` as rebuilt with the fractional cases, and is part of
the candidate commit the companion validates. If validation changes the
candidate and `current/` is rebuilt, the baseline is prepared again before
the new candidate is fixed; a baseline is frozen by its published tag, not
by its first preparation.

## 8. Delivery

In Yggdrasil, in this order, each step green before the next:

1. **`contract/problems.py`** extracted from `schema.py`; `fields.py` with
   the agreement test of 5.1; the structured `details` of 5.2 added to
   `controls`; `test_import_lightness.py` extended. In the existing
   `timestamps` module and the three schemas, implement 4.10 with its
   profile/parser/formatter and planning-replacement tests before porting callers.
2. **Characterization tests first.** Bifrost's adapter tests are ported as
   `unittest` tests of the shared reader before any rule moves, so the port
   is checked by behaviour, not by copying code: `test_plan_document.py` →
   `read_plan`, `request_state`, planned steps, finalized record;
   `test_snapshot.py` → `read_snapshot`, `step_status`, `settled`, the step
   observations of 4.9, snapshot ID; `test_scenarios.py` /
   `test_sequences.py` → `correlate`; `test_values.py` → the `Recorded`
   readers, integers and percentages. The deviations from Bifrost are
   ported with the contract's answer, not Bifrost's: the four of section 2,
   finalized-record ownership (4.4 rule 2), a finalized record's agreement
   with its report (4.4 rule 4), absent blocker lists (4.3), an absent step
   `state` or `outcome` key (4.8) and an absent planned inventory or `deps`
   (5.3). Timestamp tests preserve
   both RFC 3339 and existing timezone-aware ISO reading as specified in
   4.10: the named accepted and rejected cases with their asserted UTC
   instants, and the excess-precision correction for both fraction
   separators in the time and the offset component; step `ts` is not
   restricted to canonical producer spelling.
   The hostile-value sweep
   is ported over the regression corpus (`tests/contract/documents/current/`:
   every plan document and snapshot on its own, and the plan–snapshot pairs
   the manifest's sequences put together), with Bifrost's bar (more than
   2,000 single-value variations; nothing raises) and its two planted-fault
   tests that prove the sweep is not vacuous.
3. **Rule tests.** Every row of 4.1, 4.3, 4.6, 4.7, 4.8, 4.9 and 4.10, the
   `finishes_request` truth table (including that it equals today's function
   on every enum input), the typed comparisons of section 3 (`true` against
   `1`, `"5"` against `5`), the four additions of 4.4 (including "running
   beside finalized is lag", and the corpus check that every finalized corpus
   document agrees with its report on the seven fields), the reporting table
   of 4.7 (unsupported-document warnings at all three callers and scoped
   malformed-control warnings in both watcher paths, including no repeated
   refusal warning on subsequent empty polls), and the corrected
   pending-finalization sequence. Test valid drafts at DEBUG, malformed
   in-scope drafts at WARNING, and no new control-state warning for another
   authority/owner. The watcher still emits nothing for an ineligible plan.
   Timestamp coverage includes 1–6-digit canonical fractions, noncanonical
   but readable forms, null/missing step time, malformed types, excess
   precision and UTC conversion overflow; bad time must leave independent
   answers usable. Include the compact lowercase-`t` case and the exact
   signed sub-second offset cases of 4.10. Test the strict-end pattern in all
   three schemas through an independent validator, including whole-second and fractional strings
   with trailing newlines. Verify supplied fractions survive the writers in
   4.10 and regeneration on both stores, legacy conversion stays narrow,
   and current clock output remains whole-second. Preserve tests showing
   realm `ts` overrides and raw snapshot projection; these are supported.
   Cover the formatter's round trip, exact precision, padded years and
   invalid arguments as specified in 4.10.
4. **`read.py`**, then the moves and adapters of 5.4, with the daemon, the
   watcher and both stores switched to the new names. The existing daemon,
   watcher and store tests keep their intent and change only where this
   PRD changes an answer: eligibility reason wording; the new WARNING
   lines; the snapshot IDs of `project` and `flowcell` scopes (5.3, D11);
   an assertion that an unsupported-family document is eligible,
   which becomes `unsupported_document`; and ordinary hand-built plan
   fixtures without `schema_version`, which a supported-family builder
   helper replaces so that each test still reaches the check it is about.
   Deliberately unversioned, malformed and unsupported inputs stay as they
   are, and so do admission-refusal and no-write assertions. Missing
   `schema_version` is tested explicitly at the reader, the watcher (change
   path and scoped recovery) and both recovery listings. The handover lists
   every affected test.
5. **`plan_summary_from_document`** rebuilt over `read_plan` (D15, 5.4): a
   summary for every stored document, no defaults, a `problems` key, the
   requested ID passed in; `daemon/core.py`'s "Existing Plan Found" box, its
   only caller, shows "unknown" where a field is unusable, adds the bounded
   Problems line when there are problems, and still aborts without
   `--force`.
6. **Reference page.** `docs/reference/document_contract.md`: the sentence
   that the contract provides no reading library goes; "Reading the
   documents" points to `contract.read` and lists its guarantees (5.5); the
   plan-summary note says the summary is now a projection of the reader
   without display defaults. Update the promoted
   [`docs/design/prds/document_contract.md`](document_contract.md) and the reference page's
   timestamp and `created_at` rules for 4.10: fractional producer allowance,
   the supported timezone-aware RFC 3339 / ISO union and its narrow
   exceptions, numeric offsets converted to UTC, and at most six fractional
   digits. In the promoted
   specification, retain the original D5 decision as history and add a
   dated supersession note pointing to this PRD's 4.10, D18 and D22; make the
   current rule unambiguous. Do not silently rewrite that PRD as though it originally
   specified these changes, or edit archived completed copies. The current
   reference page states the effective rule with the same attribution.
   Preserve the distinction between canonical producer output,
   tolerant reading and unconstrained step-event time. Rebuild/check the
   current corpus; existing whole-second fixture bodies need no rewriting
   solely for this allowance. Add focused fractional cases and update schema
   expectations before preparing the first frozen release baseline.
7. **Release tag.** First the baseline: `released/<tag>/` prepared from the
   rebuilt `current/` under the intended tag name, as section 16 of
   [Document Contract: Schemas, Versioning and Compatibility](document_contract.md) and the
   corpus README prescribe (section 7). The candidate commit contains it.
   The tag is cut only after the companion's validation against that
   candidate commit has passed (section 7).

Bifrost's steps are the companion's.

## 9. Backward Compatibility

- Fractional allowance widens canonical timestamp acceptance before the
  first combined release. Existing whole-second documents remain valid;
  supported fractional `created_at` can now survive planning replacement.
  Readers accept RFC 3339 and the existing timezone-aware ISO spellings,
  with excess precision (after `.` or `,`, in the time or the offset)
  explicitly reported instead of silently truncated (4.10). The named
  cases of 4.10 are compatibility checks for Python upgrades: a change in
  what they accept is reviewed as a reader change, not absorbed. A non-UTC
  numeric offset is now read and converted to UTC, which D5 of
  [Document Contract: Schemas, Versioning and Compatibility](document_contract.md) did not
  promise; Bifrost's additional readable ISO forms remain supported, except
  the narrow exceptions of 4.10: text containing a NUL, digits after the
  seconds that neither `.` nor `,` introduces, and the colon or digit
  date-time separators that look like them are now unknown (4.10, D22). The
  supersession is explicit in 4.10, with that PRD's historical
  decision preserved. Snapshot step `ts`, realm overrides and
  event projection keep their existing behaviour.
- The daemon's decisions do not change: the same functions move, the
  `finishes_request` truth table is preserved on every input the enum
  version accepts (D10), and the enum-taking adapters keep every existing
  call site's behaviour. The executor's token and family rules are those of
  [Document Contract: Schemas, Versioning and Compatibility](document_contract.md)
  (D18).
- The recovery listing (`query_approved_pending` in both stores) and the
  PlanWatcher stop treating unsupported-family documents as eligible (4.7).
  Admission refused them already; recovery no longer wakes them, and the
  refusal is logged at WARNING where it happens, with the content admission
  logged before. Pending finalization may remain in memory until a later
  coordinator read can settle it under the existing rules (4.7).
- The watcher's in-scope `invalid_tokens` and `status_missing` skips become
  WARNING; eligibility and execution-authority decisions do not change.
  Existing cursor behaviour prevents repeated refusal warnings on idle
  polls; actual changes, replay and recovery may warn again. No new warning
  deduplication mechanism is introduced (4.7).
- Eligibility reason texts may be reworded; tests matching the text are
  updated. The reason code is new and stable.
- `get_plan_summary` returns no display defaults: an unusable field is
  `None`, never `"daemon"`, `"unknown"`, `0` or `-1`; it still returns a
  summary for every stored document (D15). Its only caller logs the values
  and aborts without `--force`, as today.
- `controls` gains structured `details` beside its string problems; no
  message changes (5.2).
- `partition_key` accepts and refuses exactly what it does today, with the
  same `ValueError` (5.3).
- The snapshot IDs of `project` and `flowcell` scopes change: their
  partitions are `project-{id}` and `flowcell-{id}`, where the writer wrote
  `proj-{id}` and `fc-{id}`. Every other kind's ID is unchanged (5.3, D11).
  The release moves, copies and deletes no stored snapshot, so one written
  before the upgrade stays under its old ID. The ops consumer's first cycle
  after a restart writes the snapshot of every plan whose spool is still
  available, under its new ID. A plan whose spool is gone keeps only its old
  snapshot, which a lookup by the new ID does not find. A consumer derives
  the IDs it looks up with `snapshot_id` or `snapshot_id_for` from the
  release whose writer stored them (section 7), never with a copy of the
  rule. Cleaning up or copying the old snapshots is a separate rollout step,
  outside this PRD (owner, 2026-11-13).
- `storage.plan_eligibility` is removed in this release, without re-exports
  (owner, 2026-11-16). Code that imported `is_plan_eligible`,
  `get_eligibility_reason` or `APPROVED_STATUS` from it imports
  `contract.read.is_eligible`, `contract.read.eligibility_reason` and
  `contract.fields.APPROVED` instead. Unlike the old functions, which
  judged status and tokens only, these check the family first, so an
  unversioned plan is not eligible; `contract.read.eligibility` gives the
  coded decision.
  `plan_generation_of` is removed without one: `contract.read.generation_of`
  is the only generation helper (owner, 2026-11-13). The enum adapter for
  `finishes_request` stays as long as the `flow` enums exist.
- What changes on a consumer's screen when it adopts the reader is the
  companion's list, not this PRD's.

## 10. Out of Scope

Changes inside Bifrost: its dependency, adapter, fixtures, tests and
documents (the companion). Approve and rerun helpers, digests and intents
(the follow-on draft **Document Contract: Approve and Rerun Helpers**).
Storage reads, the `PlanReader` implementation for CouchDB, retries,
timeouts, change feeds, refresh hints, freshness and connection state (the
follow-on draft **Bifrost – CouchDB PlanReader and `_changes` Refresh Hints**).
The plan list, its order, search and filters, and how a live store serves it
(Bifrost; that draft). Reading event records or job records; interpreting a step's
`job`, `run_id`, `fingerprint`, `artifacts` or `metrics`. A separate
`yggdrasil-contract` distribution (a D2 revisit, only on a concrete problem
with the measured footprint). Broader logging and verbosity redesign
(section 5 of the draft **PlanWatcher Reliability and Operational Logging**). Family 2 of any kind. Timestamp ownership
changes, an event publication facade, spool timestamp normalization and
switching clock production to fractional seconds.

## 11. Success Criteria

- `contract.read` holds every rule for tokens, eligibility, request
  finishing, generation comparison, finalized-record ownership, snapshot
  identity and structure, correlation verdicts, step status and step
  observations, and snapshot IDs; no other Yggdrasil module decides any of
  them, and a reviewer can confirm it by reading `storage`, `ops` and
  `daemon`.
- Every row of the section 4 tables is a test; the corpus-based
  hostile-value sweep raises nothing; the planted-fault tests fail when a
  fault is planted; `finishes_request` agrees with today's function on every
  enum input.
- A family-1 plan with `"run_token": "5"`, or with no `run_token`, is
  invalid, not eligible, with request state unknown, from the same function
  the daemon uses; the companion's validation shows a consumer getting the
  same answer.
- `tests/test_import_lightness.py` passes for `fields`, `problems` and
  `read`; the `fields` agreement test passes against `flow`, `ops` and the
  v1 schemas.
- Yggdrasil's daemon, watcher and store tests pass with the call sites
  switched (2,983 tests where the implementation of
  [Document Contract: Schemas, Versioning and Compatibility](document_contract.md)
  ended, plus this PRD's), with the WARNING
  lines of 4.7 present, no repeated refusal warnings on empty polls,
  and the pending-finalization sequence covered.
- An existing document that the reader cannot use still produces a summary,
  and the plan-only path still refuses to overwrite it without `--force`.
- Canonical whole-second and fractional values agree across helpers and
  schemas; tolerant reading preserves exact instants and raw observations,
  and excess precision is never silently lost; the named cases of 4.10 pass
  with their asserted UTC instants, and a seventh digit after `.` or `,`,
  in the time or the offset component, is reported rather than truncated. Supported fractional
  `created_at` survives regeneration on both stores, legacy conversion stays
  narrow, and clock-generated values remain whole-second (4.10).
- The tag is cut only after the companion's validation against the
  candidate commit passed; that commit includes the fractional allowance
  and first frozen baseline (section 7).

## 12. Decisions

Status key as in section 14 of
[Document Contract: Schemas, Versioning and Compatibility](document_contract.md).

| # | Decision | Answer | Status |
| --- | --- | --- | --- |
| D1 | Where the reader lives | Yggdrasil, `contract.read`, beside the `controls` and `versions` of [Document Contract: Schemas, Versioning and Compatibility](document_contract.md). | agreed |
| D2 | How a consumer gets it | Dependency on the `yggdrasil` package at the tag (decision of 2026-10-07); no vendoring; a separate distribution only on a concrete problem with the measured footprint. Precondition (no `lib` hook) met by [Package Consolidation: `lib` under `yggdrasil`](package_consolidation.md). | **owner** (confirmed 2026-10-21; footprint stance 2026-11-03) |
| D3 | Tokens | The complete rule of 4.2, with the 2^53 - 1 bound; no data check. Defined and applied in Yggdrasil by [Document Contract: Schemas, Versioning and Compatibility](document_contract.md) (its D18); reused here. | agreed (Codex A5; moved to that PRD, 2026-10-21) |
| D4 | What moves per step | The status rule and `settled` (4.8) and the interpretation of the existing progress, error and blocker fields (4.9), bounded by Bifrost's tests; rows, association, counts, links, wording stay with the consumer; no job or event reading. | **owner** (2026-11-03, Q4) |
| D5 | Conflicting endings | Only between explicit terminal claims after identity checks (4.4 rule 3). | agreed (Codex A3) |
| D6 | Unsupported documents | Inspect-only; version check first in every whole-document read; the reader derives nothing and passes the body through (4.0). The value helpers of 5.3 do not check and give no conclusions. | engineering (follows D12 of [Document Contract: Schemas, Versioning and Compatibility](document_contract.md); helpers clarified 2026-11-03) |
| D7 | Finalized record ownership | Current request's outcome only when completion is recorded and the usable finalized record's `plan_id`, `plan_generation` and `run_token` all match, typed (4.4 rule 2); `finalized_is_current` states that recorded identity, independently of completion and eligibility (5.3). | engineering (v0.5; kept 2026-11-03; completion guard explicit in v0.13); identity rule **owner** (2026-11-09) |
| D8 | Record versus report | All seven identity fields present in both, of the contract's type, and equal, else the record is unusable (4.4 rule 4). Types before values. | engineering (v0.5; typed 2026-11-03) |
| D9 | Eligibility order, codes and reporting | Object, version, tokens, status missing, not approved, already executed; closed reason codes (4.7). Unsupported-document refusals are reported where filtering occurs; existing cursor behaviour prevents repeats on idle polls. Pending results stay in memory until a later coordinator read applies existing settling rules; same-token restoration alone does not settle them. | engineering (v0.5; pending description corrected and idle-poll behaviour clarified 2026-11-06) |
| D10 | `finishes_request` | Plain strings in `contract.read`; the enum signature stays in `storage.plan_updates` as an adapter. Today's truth table exactly: a succeeded, completed attempt finishes the request under any policy; the failure branch requires `outcome == "failed"`, `continue_independent` and a drained or preflight-rejected ending; unrecognised values elsewhere never finish a request. No tightening. | engineering (v0.5; truth table fixed 2026-11-03, see 13) |
| D11 | Snapshot ID | `snapshot_id` / `snapshot_id_for` return `None` instead of raising; the formatting is shared as `partition_name`, and every partition is `{kind}-{id}` with the scope's kind as recorded (the `proj` and `fc` abbreviations retired); `partition_key` keeps today's acceptance and `ValueError`. | engineering (v0.5; writer untouched 2026-11-03); raw scope kinds **owner** (2026-11-13) |
| D12 | Problems | `ContractProblem` shared by `schema` and `read` through `contract/problems.py`; reader codes added; control checks produce structured details at their source, with their string messages unchanged (5.2). | engineering (v0.5; source-structured 2026-11-03) |
| D13 | Field constants | Spellings in `contract.fields`, imported by `storage` and `ops`; each module keeps its own sets; the agreement test checks spellings, not sets. | engineering (v0.5; narrowed 2026-11-03) |
| D14 | Release sequencing | One tag carries [Document Contract: Schemas, Versioning and Compatibility](document_contract.md) and this PRD; one Bifrost change moves the pin and adopts the reader (the companion), which makes findings C1 and C2 of the review of **Bifrost – CouchDB PlanReader and `_changes` Refresh Hints** moot. Validation against the candidate commit precedes the tag (7). | **owner** (2026-11-03, Q1) |
| D15 | Plan summary | Stays in `storage.plan_documents` as a projection of `read_plan` with no defaults; exists for every stored document; the requested ID is passed to the reader (5.4). | **owner** (2026-11-03, Q2) |
| D16 | Dependency form and footprint | Git source at the tag, locked to its commit by uv; the full package accepted provisionally; installed dependencies and import behaviour measured early, at the candidate commit; reconsidered only on a concrete problem. Mechanics in the companion. | **owner** (2026-11-03, Q3) |
| D17 | Request state and identity | `request_state` takes the expected plan ID; the body's own `_id` never stands in for it (5.3). A caller that already holds a `PlanRead` uses its `.request` directly. | engineering (2026-11-03, review) |
| D18 | Fractional timestamp allowance | Part of this PRD: canonical UTC `Z` timestamps admit an optional 1–6-digit fraction without trailing zeros; current clocks keep whole seconds. Tolerant reading, exact supplied-value preservation and narrow unversioned conversion follow 4.10. | **owner** (2026-11-06, allowance and scope); profile details as specified in 4.10 |
| D19 | Event timestamp ownership and sequence | Keep realm overrides and unconstrained snapshot step `ts`; interpret usable time without changing raw events or projections. The ownership amendment is withdrawn. This PRD follows [Document Contract: Schemas, Versioning and Compatibility](document_contract.md) directly, with one combined tag. | **owner** (2026-11-06) |
| D20 | Canonical formatter | Clock-free `canonical_text` formats an aware instant in UTC with exact microseconds and canonical zero trimming; typed input failures are explicit. Current clocks keep seconds precision. | review decision (2026-11-06; 4.10) |
| D21 | Malformed-control skip severity | WARNING for `invalid_tokens` and `status_missing` within the watcher's authority/owner scope, including malformed drafts; ordinary skips DEBUG. Both watcher paths apply scope before these warnings. No repeated refusal warnings on idle polls; changes, replay and recovery may warn again, with no added deduplication (4.7). Broader logging stays with section 5 of **PlanWatcher Reliability and Operational Logging**. | **owner** (clarified scope: no periodic idle-poll warnings; additional suppression deferred, 2026-11-06; 4.7) |
| D22 | Reader timestamp grammar | Union of RFC 3339 and existing timezone-aware ISO input forms, characterized against Python 3.11; real, representable instants normalized to UTC, at most six fractional digits and no guessed timezone. Canonical producer spelling remains stricter; raw values remain available. Explicitly supersedes D5 of [Document Contract: Schemas, Versioning and Compatibility](document_contract.md), as recorded in 4.10. Acceptance is pinned by named positive and negative cases with asserted UTC instants, kept as compatibility checks for Python upgrades (an acceptance change is reviewed; no separate grammar engine); lowercase `z` is read on the RFC 3339 path only; the six-digit guard covers `.` and `,` fractions in time and offset components. Narrow exceptions (4.10): text containing a NUL, digits after the seconds that neither `.` nor `,` introduces, and the colon or digit date-time separators that look like them are unknown. | **owner** (clarified and accepted 2026-11-06; characterization and separator notes the same day; narrow exceptions accepted 2026-11-10; 4.10) |

## 13. Review Decisions

The review questions are resolved below. The owner approved the whole PRD
at v0.12 on 2026-11-09.

1. **Reader grammar (4.10).** v0.7 asked `parse_timestamp` to retain both
   the valid RFC 3339 UTC forms of
   [Document Contract: Schemas, Versioning and Compatibility](document_contract.md)
   and "Bifrost's existing timezone-aware ISO input forms". Checked at
   7ace54d and 38c09cd, these are two grammars that do not coincide. That
   PRD's `is_date_time` is RFC 3339 only and
   case-insensitive: it accepts `2026-11-06t10:20:30z` and rejects a space
   separator and the compact form. Bifrost's `datetime.fromisoformat`
   accepts lowercase `t` with uppercase `Z`, rejects lowercase `z`, and
   accepts the space separator, compact `20261106T102030Z`, and week dates.
   Ordinal dates are unsupported in Python 3.11 already. The owner clarified
   that conforming to Yggdrasil's contract must not prevent interpreting
   readable realm-supplied ISO observations. D22 therefore accepts the union,
   with explicit precision and timezone limits (4.10). This preserves the
   existing readable ISO forms and adds lowercase `z`; Bifrost needs no
   separate parsing fallback. The supersession of that PRD's D5 is recorded in 4.10,
   and delivery step 6 preserves the historical decision while documenting
   the effective contract. No ownership amendment is revived.
   The owner's two later notes are written into 4.10 (v0.11): the named
   characterization cases, lowercase `z` confined to the RFC 3339 path, and
   the precision guard for both fraction separators in time and offset
   components. Those tests are compatibility checks for Python upgrades,
   not a proof against every grammar expansion.
2. **Formatter: included (D20).** One small pure formatter gives the
   canonical profile an implementation shared by tests and future writers;
   it does not activate fractional clock production.
3. **D10: preserve today's truth table.** The review accepts Codon's
   push-back. A succeeded, completed attempt finishes the request regardless
   of policy; policy governs failure endings. Admission still rejects an
   unrecognised policy. There is no proposed tightening of this helper.
4. **Severity: in this PRD (D21).** Implement the scoped WARNING rule in 4.7
   while preserving existing cursor behaviour: no repeated refusal warnings
   on idle polls. The owner accepts warnings on actual changes, replay and
   recovery; additional deduplication is deferred unless experience shows
   a need. The previously proposed warning-state mechanism is withdrawn.
   The relevant passage of the draft
   **PlanWatcher Reliability and Operational Logging** is in its section 5, not section 4, and its
   broader logging investigation is not a dependency for this change.
5. **Index and references.** In the index of PRDs kept with the drafts,
   outside this repository, this PRD's row ("Yggdrasil + Bifrost") is now
   Yggdrasil only, with a row for the companion. That index records the
   current order and resolves its completed and withdrawn references to
   available sources. The two references to section 4.2 of this PRD in the
   draft **Document Contract: Approve and Rerun Helpers** resolve to this
   PRD's token section and need no change (v0.6's note said otherwise).
   When that draft is picked up it should say which spelling its click
   times use, whole seconds or a canonical fraction; either is valid under
   D18, and its `updated_at` stays clock-driven. v0.4 of the draft
   **Bifrost – CouchDB PlanReader and `_changes` Refresh Hints** should point its
   sentence that the mapping lives in the adapter until this PRD lands to
   the companion.

The remaining names (result fields, reason strings, problem codes) are
settled while writing the tests.

## 14. Implementation Outcome

Implemented on `feature/contract_shared_reader`, branched from `7ace54d`
(the merge of PR #111), and merged into `dev` by PR #115 as `7141f44`
(2026-11-16). A correction made before the tag was merged by PR #116 as
`eeb9359` (2026-11-17), the release candidate. PR #117 merged `dev` into
`main` as `724ef60`, and `v0.6.0` was tagged there (2026-11-18). Every
commit named below is in `v0.6.0`.

**Foundations (2026-11-09).** `contract.problems` holds `ContractProblem`
and the problem codes, moved out of `schema.py`, which re-exports them, and
adds the reader's codes. `contract.fields` holds the shared spellings and
vocabularies, with a test that checks them against `flow`, `ops` and the v1
schemas. `token_pair` and `failure_policy_of` report structured `details`
beside their unchanged messages, and `execution_definition_check` does the
same for the execution definition (5.2). Commits `cbaa229`, `d493b04` and
`2816916`.

**Timestamps (2026-11-09, corrected 2026-11-10).** As 4.10 specifies: the
producer profile admits a one-to-six-digit fraction, in
`contract.timestamps` and in the three schemas (strict end of string,
`maxLength` 27); `parse_timestamp` reads the union of D22 with the six-digit
guard and exact sub-second offsets; `canonical_text` formats an instant;
legacy conversion stays whole-second; supplied fractions survive the
writers and regeneration on both stores; the corpus gains focused
fractional cases. The review found that Python 3.11 also reads, as a
fraction, digits after the seconds that a colon introduces or that follow
directly, which bypassed the guard. The reader now reads a fraction only
after `.` or `,` (the narrow exceptions of 4.10, v0.14). Five other Python
3.11 behaviours are accepted as Python reads them and pinned with their UTC
instants (`TestPythonQuirks` in `tests/contract/test_timestamps.py`): a
stray character before the zone, a fraction after the hour or the minute
counted in seconds, offset minutes and seconds that roll over, an empty
fraction, and unchecked text between a six-digit fraction and the zone.
Commits `629a137`, `02695dd`, `94e9743`, `a80ce88` and `bb6ee08`.

**Reading plan documents (2026-11-10 to 2026-11-12).** `contract.read`, a
subpackage: `read_plan` with `PlanRead`, `RequestState`, `FinalizedRecord`
and the planned steps; `request_state`; `eligibility`, `is_eligible` and
`eligibility_reason`; `finishes_request`; `generation_of`; and the snapshot
ID functions (5.3). `contract.versions.describe_schema_version` gives one
description of a document's `schema_version` for the reader's refusals and
the daemon's messages. Bifrost's plan-document and value tests were ported
first, each deviation from Bifrost asserting the contract's answer (8 step
2). Accepted as built: `PlanRead` and `SnapshotRead` carry a presence
(`present`, `missing` or `unavailable`), and for a body
`PlanRead.eligibility` is never empty: an unusable plan gets the refusal
that explains it (`invalid_document`, `unsupported_document` or
`identity_mismatch`). The review's two clarifications are v0.15. Commits
`6acd8e6`, `0f08a5f`, `0763563`, `65a5a49` and `2913097`.

**Reading snapshots and correlating (2026-11-12 to 2026-11-13).** `read_snapshot` with
`SnapshotRead`, `Attempt` (which states `settled`), `ObservedStep` and
`StepError`; `step_status`; the step observations of 4.9; and `correlate`
with `Correlation`, including `request_and_observation` (5.3). Bifrost's
snapshot, scenario and sequence tests were ported first. The hostile-value
sweep runs in the normal suite, over every corpus plan document and
snapshot and every plan–snapshot pair of the manifest's sequences. When it
was added it made 66,737 single-value variations and 48 whole-body ones,
more than 2,000 for each pair, and nothing raised; both planted faults are
caught. Review corrections: an absent step `state` or `outcome` key is told
apart from `null` (v0.16), and a finished attempt without a recognised
outcome is incomplete evidence, never a conflicting ending; the pair sweep
reads the snapshot at hand when a plan names no snapshot ID. Accepted as
built: a legacy projection is read with its `attempt` key absent, although
the writer schema requires an explicit `null`. Commits `2a8e229`,
`7c50bb3`, `94dc1ec`, `928f82a`, `a5d4c8b`, `f0e44a5` and `4bb00b1`.

**Value helpers and spellings (2026-11-13).**
`storage.plan_updates.finishes_request` keeps its enum signature as an
adapter over `contract.read.finishes_request`, which agrees with it on
every enum input. `contract.read.generation_of` is the only generation
helper. `partition_key` formats through `partition_name`, so the snapshots
of `project` and `flowcell` scopes are stored as `project-<id>` and
`flowcell-<id>` (v0.17); the corpus's nine snapshots were regenerated and
differ only in `_id`. `plan_documents`, `plan_updates`, `ops.snapshot` and
the daemon's field reads take document spellings from `contract.fields`.
Accepted scope for that move (D13): the plan-document, snapshot and
finalized-record fields and words that `contract.fields` already spells.
Keys from spool events, start records and the attempt report alone, and the
other `storage` and `ops` modules, keep their literals, and nothing was
added to `fields`. Commits `5905d65`, `538b2a4` and `b063186`, then the
review's `0916f8a`, `e2a9ae0`, `c67332a` and `e20d8c8`. The full suite
passes at `e20d8c8`, but not at the three commits before it.

**Eligibility and refusal reporting (2026-11-16).** Admission, PlanWatcher
(its change path and scoped recovery, with scope applied first) and both
recovery listings judge eligibility through `contract.read` (4.7). An
unsupported-family document is refused and reported at WARNING, with the
plan ID and a description of its `schema_version`. In-scope
`invalid_tokens` and `status_missing` skips are WARNING, and a refused
change followed by empty polls warns once. The pending-finalization
sequence of 4.7 is tested. Ordinary hand-built plan fixtures in existing
tests now come from a builder-based helper of the supported family,
`tests/plan_store_support.supported_plan_document`. Deliberately
unversioned, invalid and unsupported inputs, and the admission-refusal and
no-write assertions, are kept. `storage.plan_eligibility` is removed
(v0.18). Accepted behaviour: a run-once session whose plan another writer
takes out of the family warns at once, and exits `1` at its configured
timeout unless it observes the plan eligible again
(`docs/getting_started/cli.md`). Commits `00869df`, `e26b12b`, `3afe279`
and `bfe79ac`.

**Plan summary (2026-11-16).** `plan_summary_from_document` is a projection
of `read_plan`, with the requested ID passed in (D15): no display defaults,
a `problems` key, a summary for every stored document, and
`unrecognised_status` and `unrecognised_execution_authority` (v0.19). The
"Existing Plan Found" box shows `unknown`, with an unrecognised recorded
word beside it; it adds a bounded Problems line when there are problems,
and it still aborts without `--force`. Commits `70fe5da`, `9eace0f` and
`6569dc3`.

**Documentation (2026-11-16).** `docs/reference/document_contract.md`
describes `contract.read` and its guarantees, the fractional producer
profile and the reader's timestamp union with its narrow exceptions, the
plan summary, eligibility, the refusal log lines with their loggers, and
the upgrade notes. Troubleshooting, plan execution, the test realm page and
`CLAUDE.md` follow.
[Document Contract: Schemas, Versioning and Compatibility](document_contract.md)
gains the dated supersession note of 8 step 6. Commits `0dbab15`,
`e195459`, `b333abe`, `0eeb479` and `f874a8f`. The reference page's "Since
v0.6.0" paragraph and that note name this PRD by title; the change that
adds this PRD to the repository links both to it.

**Frozen baseline (2026-11-16).** `tests/contract/documents/released/v0.6.0/`
holds the 22 valid entries of the rebuilt `current/` (14 plan documents and
8 snapshots), byte for byte, and a manifest of exactly those entries, in
order: git tree `562defc47f0204a9db31531cae02f9488ebf5b08`. Commit
`0b015de`. Counted there with the two git-ignored tool test files, the
suite gives 3545 tests and 6100 subtests, against 2983 and 2623 at
`7ace54d`.

**Schema correction before the tag (2026-11-17).** A realm's handler,
called for the CouchDB design document `_design/views`, produced a plan ID
containing `/`, which the schemas accepted and the engine refuses. The
schemas now hold every plan ID and realm to the engine's rule, one
directory component, and the corpus gains one negative document. The
baseline is unchanged. The reader still reads such a stored document under
its ID, with no schema problem (`tests/contract/test_conformance.py`). The
correction is recorded in section 16 of
[Document Contract: Schemas, Versioning and Compatibility](document_contract.md),
"Plan IDs and realms in the schemas (2026-11-17)". Commits `60717fd`,
`a4b3457`, `bf362a6` and `cef2854`, then `4c5eabc` and `10c586f`, merged as
`eeb9359` (PR #116).

**Release candidate (2026-11-17).** At
`eeb935928c926eb2cd6fc0f12875d3dbf57902dc`, `python -m tests.contract.corpus`
reports no differences, and `released/v0.6.0/` is still tree `562defc…`:
each of its 22 documents is the same blob as its `current/` file. CI passed
on the push to upstream `dev`: Tests (run 44122057279, 3504 passed, without
the two git-ignored files), Lint and Type Check (run 44122057393, Black and
mypy clean) and Secrets scan (run 44122057748).

**Bifrost's validation (2026-11-18).** Bifrost moved its pin to the
candidate and reported every check passing, with no incompatibility.
Between its previous pin and the candidate, the only change it saw in
Yggdrasil was in the schemas. Its regenerated fixtures differ from the
committed ones only in the line that records the Yggdrasil revision, and
all 22 documents of `released/v0.6.0/` read cleanly. These results are
recorded as Bifrost reported them: the report names no Bifrost commit or
run, and does not itemize the footprint measurement of D16.

**The tag (2026-11-18).** `724ef605e70fad070237631865af502a94614443` is the
merge of PR #117. Its parents are the previous `main` (`000d596`, tagged
`v0.5.0`) and the candidate, and its tree is the candidate's. The annotated
tag `v0.6.0` (tag object `b200343266ba53fb45152766409d45365613d0b7`) is on
that merge, not on the candidate itself as section 7 says, so that the tag
and `main`'s head are one commit. The owner accepted this: every file
Bifrost validated is unchanged. CI passed on the merge, pushed to upstream
`main`: Tests (run 44269573659, 3504 passed), Lint and Type Check (run
44269573544, Black and mypy clean), Secrets scan (run 44269573615) and the
strict documentation build (run 44269573694).

**After the tag (2026-11-19).** `git describe` on the tagged commit gives
`v0.6.0`, and `released/v0.6.0/` there is tree `562defc…`. `pip install
"yggdrasil @ git+https://github.com/NationalGenomicsInfrastructure/Yggdrasil@v0.6.0"`
into a new virtual environment (Python 3.11.0) installed the package and 21
third-party distributions. Run from outside the source checkout:

- `importlib.metadata.version("yggdrasil")` reports `0.6.0`, and pip
  records the commit `724ef60` for the revision `v0.6.0`;
- `yggdrasil.contract.read` imports from the installed package;
- `py.typed` and the three schemas are installed, and the schemas load
  through `schema_for` and equal the tagged files;
- the 125 installed modules equal the tagged ones.

Moving Bifrost's pin to the tag, and checking its locked installation, are
the companion's (section 7).
