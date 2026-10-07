# Yggdrasil – Repository Cleanup: Legacy Realms, Remnants and Dependencies (PRD)

## Version
v0.2

## Status
Implemented; outcome and deviations recorded in section 13

---

## 1. Overview

Yggdrasil's tree still carries code from the era in which realms owned their
own execution: the SmartSeq3, 10x and delivery realms built on
`AbstractProject`, the module registry that located them, two root-level
entry scripts, and ten runtime dependencies that only that code imports.
None of it participates in the current architecture (watchers, realm
descriptors, plans, the Engine, internal storage), and the current realm model
is already exercised end to end by `test_realm` and by the external
`demux_realm`.

This document specifies how that code leaves the tree, in distinct cycles that
each end with the full check suite green, and what is deliberately left in
place because its position is not yet decided.

The legacy realms will be re-implemented as external realms later. They are not
copied anywhere verbatim; the local `trashcan/` directory keeps them at hand
for reference while they are rewritten.

---

## 2. Problem Statement

- Ten of the sixteen runtime dependencies (`loompy`, `matplotlib`, `networkx`,
  `numba`, `numpy`, `pandas`, `pdf2image`, `Pillow`, `reportlab`,
  `ruamel.yaml`) are imported only by the legacy realms, or by nothing at all
  (`networkx`, `numba`). Installed, they account for roughly 400 MB of a
  650 MB environment; the core's own dependencies come to about 10 MB.
- Every consumer that installs Yggdrasil, external realms today and Bifrost
  tomorrow, pays for those dependencies without using them.
- The legacy code misleads realm authors about the current model: it shows
  realms submitting their own jobs and reading the database directly, which is
  no longer how a realm is written.
- The tree carries tracked junk from that era: thirteen `.DS_Store` files under
  `lib/realms/smartseq3/report/assets/`, a root `config.json` superseded by
  `main.json`, and `lib/handlers/bp_analysis_handler.py`, which declares itself
  dead code in its own header.
- Every later packaging change (folding `lib/` under `yggdrasil/`, a public
  contract for stored documents) is simpler once this code is gone.

---

## 3. Design Principles

1. **One cycle, one pull request, one green suite.** Each cycle is reviewable
   on its own and ends with `pytest`, `ruff check .`, `black --check .` and
   `mypy .` passing, and with a fresh `pip install .[dev]` succeeding.
2. **Nothing in the tree depends on anything in `trashcan/`.** A cycle is
   complete only when that holds, which the suite proves.
3. **`trashcan/` is a local convenience, not an archive.** It is gitignored,
   so for the repository, CI and every other clone a move into it is a
   deletion. The archive is git history: each removal commit names what it removed,
   so any file can be retrieved with `git checkout <commit>^ -- <path>`. No
   tag is dedicated to the cleanup.
4. **Paths under `trashcan/` mirror the originals** (`trashcan/lib/realms/tenx/`,
   `trashcan/tests/test_module_resolver.py`), so provenance stays obvious.
5. **No behaviour change for current users.** `test_realm`, `demux_realm`, the
   CLI, the daemon and Bifrost's fixture generator (which calls Yggdrasil's
   document, report and projection functions) behave identically after every
   cycle.
6. **Undecided things stay.** Code whose future position is open
   (`lib/module_utils`) is kept, tested and untouched; deciding it is a
   separate document.

---

## 4. Inventory

| Item | Classification | Cycle |
| --- | --- | --- |
| `lib/realms/smartseq3/` (incl. `report/assets/`, 13 `.DS_Store`) | Legacy realm | 1 |
| `lib/realms/tenx/` (incl. `handler.py`, `planner_old.py`) | Legacy realm; an abandoned partial port | 1 |
| `lib/realms/delivery/` | Legacy realm | 1 |
| `module_registry.json`, `lib/core_utils/module_resolver.py`, `tests/test_module_resolver.py` | Located legacy realms by name | 2 |
| Module-registry lookup in `lib/couchdb/project_db_manager.py` and the tests exercising it in `tests/test_project_db_manager.py` | Same mechanism | 2 |
| `ygg_trunk.py`, `ygg-mule.py` | Legacy entry scripts; the CLI replaced them | 2 |
| `config.json` (repository root) | Superseded by `main.json` | 2 |
| `lib/base/` (`abstract_project.py`, `abstract_sample.py`) | Base classes of the legacy realms only | 2 |
| `lib/handlers/bp_analysis_handler.py` | Self-declared dead code | 2 |
| `lib/handlers/base_handler.py` | Re-export shim of `yggdrasil.flow.base_handler`; `yggdrasil_core.py` still imports through it | 2 |
| Commented-out `module_resolver` import, `lib/core_utils/yggdrasil_core.py:860` | Remnant | 2 |
| `lib/module_utils/ngi_report_generator.py`, `tests/test_ngi_report_generator.py` | Organisation-specific (calls NGI's reporting tools); not generic | 2 |
| Ten runtime dependencies listed in section 2 | Unused by the core | 2 |
| `lib/module_utils/{sjob_manager,slurm_utils,hpc_submission_policy,report_transfer}.py`, `lib/mocks/mock_sjob_manager.py`, their tests, the CLI `--manual-submit` flag and the `YggSession` flag behind it | Generic HPC helpers whose position is undecided | kept; see section 7 |
| `lib/realms/test_realm/` | Current model, dev-only | kept |

No test imports the legacy realms directly; their only coverage is indirect,
through the module-registry tests listed above.

---

## 5. Cycle 1 – Legacy Realms

### 5.1 Steps

1. Write a commit message that lists the removed directories, so the removal
   is easy to find in history. No tag is created for the cleanup.
2. `git mv` is not used, because the destination is ignored: move
   `lib/realms/smartseq3/`, `lib/realms/tenx/` and `lib/realms/delivery/` to
   `trashcan/lib/realms/` locally and `git rm -r` them from the tree. The
   `.DS_Store` files go with them and are not kept anywhere.
3. Leave `lib/realms/__init__.py` and `lib/realms/test_realm/` as they are.
4. Update the three documentation lines that describe `lib/realms/` as
   "internal realm implementations and `test_realm`": `README.md` (project
   structure table), `CLAUDE.md` (Architecture, item 2) and
   `docs/architecture/overview.md` (key directories). After this cycle the
   directory holds only the dev-only test realm.
5. Run the full check suite and a fresh install.

### 5.2 Verification

- `pytest`: the full suite passes with the same test count as before, minus
  nothing (no test imports these realms).
- `ruff check .`, `black --check .`: both respect `.gitignore`, so
  `trashcan/` is skipped without configuration changes.
- `mypy .`: `trashcan/.*` is already excluded in `pyproject.toml`.
- `grep -r "lib.realms.\(smartseq3\|tenx\|delivery\)"` over `yggdrasil/`,
  `lib/`, `tests/` and `docs/` returns nothing except historical examples in
  `docs/GENSTAT_PLAN_CONTRACT.md`, which name `tenx` as an example realm ID
  and need no change.
- `yggdrasil --dev daemon` starts and discovers `test_realm`; `demux_realm`'s
  own suite passes against the result.

### 5.3 Acceptance criteria

- The three directories are absent from the tree and from `git ls-files`.
- All checks in 5.2 pass.
- Documentation no longer implies that production realms live in `lib/realms/`.

---

## 6. Cycle 2 – Remnants and Dependencies

### 6.1 Steps

1. Move to `trashcan/` (mirroring paths) and `git rm`: `module_registry.json`,
   `lib/core_utils/module_resolver.py`, `tests/test_module_resolver.py`,
   `ygg_trunk.py`, `ygg-mule.py`, `config.json`, `lib/base/`,
   `lib/handlers/bp_analysis_handler.py`, and
   `lib/module_utils/ngi_report_generator.py` with its test.
2. Remove the module registry from `lib/couchdb/project_db_manager.py`: its
   constructor loads `module_registry.json` through `ConfigLoader`, and one
   method resolves a library-construction method to a module through it.
   Delete both and the tests that exercise them (`tests/test_project_db_manager.py`
   names `lib.realms.tenx…`, `…smartseq3…` and a `mars` realm as registry
   data); keep the manager's document operations and their tests. Without this,
   removing the registry file would break the manager's construction.
3. Make `lib/core_utils/yggdrasil_core.py` and `tests/test_base_handler.py`
   import `BaseHandler` from `yggdrasil.flow.base_handler` (both go through
   the `lib.handlers.base_handler` shim today), then remove `lib/handlers/`
   entirely.
   Delete the commented-out `module_resolver` import.
4. In `pyproject.toml`, reduce `dependencies` to `appdirs`, `cattrs`,
   `ibmcloudant`, `panzi_json_logic`, `requests`, `rich` and `watchdog`
   (existing versions as pinned today; pin `cattrs` to the version the
   lock resolves). `setuptools-scm` stays a build requirement. `cattrs` is
   declared because `yggdrasil/flow/utils/codec.py` imports it behind a `try`
   with a passthrough fallback, so without the declaration two installs can
   behave differently.
5. Regenerate `requirements/lock.txt` with
   `pip-compile --strip-extras -o requirements/lock.txt` (the pre-commit hook
   does this).
6. Update `CLAUDE.md` (CLI section: the legacy entry scripts) and any other
   line that names a removed file.
7. `docs/TECH_DEBT_LEDGER.md` exists only as an untracked local file. Its
   entry 15 covers `bp_analysis_handler.py` and also names
   `ScenarioDocWatcher` in `lib/realms/test_realm/watcher.py` as dead and
   unwired. The implementer reads the entry, verifies both claims (grep and
   the suite), removes `ScenarioDocWatcher` in this cycle if it is indeed
   unwired, and then updates, resolves or defers the entry as the result
   warrants, together with every docstring or comment that cites it.

### 6.2 Verification

- In a fresh virtual environment, `pip install .[dev]` succeeds and installs
  none of the ten removed packages (check with `pip list`).
- `python -c "import yggdrasil.cli, lib.core_utils.yggdrasil_core"` succeeds
  in that environment.
- Full check suite green; `demux_realm`'s suite green against the result.
- `grep -rn "module_registry\|module_resolver\|lib\.base\|lib\.handlers"`
  over `yggdrasil/`, `lib/`, `tests/`, `docs/` returns nothing.
- Bifrost's `scripts/generate-source-fixtures --check` against the new
  revision still reports the committed fixtures as matching (the generator
  calls `lib.storage.plan_documents`, `lib.ops.snapshot` and
  `yggdrasil.flow.outcomes`, none of which change).

### 6.3 Acceptance criteria

- The items in 6.1 are absent from the tree.
- The dependency list is the seven packages above.
- All checks in 6.2 pass.

---

## 7. Kept for a Later Decision: `lib/module_utils`

`SlurmJobManager`, `generate_slurm_script`, `HPCSubmissionPolicy`,
`transfer_report`, the mock job manager and the CLI's `--manual-submit` flag
were written as help for realms when realms executed their own plans. That
authority now sits with the Engine, and a realm expresses work as `@step`
functions. Three positions are possible for these helpers, and this document
does not choose:

- a public, optional toolkit inside Yggdrasil (`yggdrasil.toolkit.slurm`,
  `yggdrasil.toolkit.transfer`) that step authors call;
- a separate package maintained alongside the realms;
- removal, leaving each realm to bring its own.

Whatever the decision, two properties of the current code bear on it: the
helpers import no third-party packages, so keeping them costs nothing in
dependencies, and `plan_status` snapshots already carry a per-step `job` field,
so the Engine's observation model has a place for a submitted job's identity.
Until the decision, the helpers, their mock and their tests stay where they
are and must keep passing. The decision is the subject of a separate
document ("Step toolkit and named endpoints").

---

## 8. How Tooling Treats `trashcan/`

| Tool | Behaviour | Change needed |
| --- | --- | --- |
| git | Ignored (`.gitignore` line 136) | none |
| ruff, black | Respect `.gitignore` by default | none |
| mypy | `trashcan/.*` excluded in `pyproject.toml` | none |
| pytest (CI) | Runs `pytest tests/` | none |
| pytest (pre-commit, push) | Runs with `--ignore=trashcan` | none |
| mkdocs | Reads `docs/` only | none |

Because the directory is ignored, its contents exist only in the clone where
the move was made. Anyone else wanting the code retrieves it from the parent of the removal
commit.

---

## 9. Backward Compatibility

- The `yggdrasil` CLI, its flags and the daemon are unchanged. `ygg_trunk.py`
  and `ygg-mule.py` have had no supported role since the CLI replaced them.
- External realms (`demux_realm`) import `yggdrasil.flow.*`,
  `yggdrasil.core.realm`, `lib.core_utils.event_types` and
  `lib.watchers.watchspec`; none of these move.
- The deprecated `ygg.handler` entry-point group is not touched by this
  document.
- Documents written to `yggdrasil_plans`, `yggdrasil_ops` and the event spool
  are unchanged, so Bifrost's committed fixtures remain valid.

---

## 10. Out of Scope

- Re-implementing the SmartSeq3, 10x and delivery realms as external realms.
- Folding `lib/` under `yggdrasil/` and retiring the `lib.*` aliasing import
  hook (a later packaging change that this cleanup makes smaller).
- A public contract for stored documents (schemas, read model, plan-document
  protocol), to be discussed and specified separately.
- Deciding the position of `lib/module_utils` (section 7).
- Any change to supported Python versions or to the CouchDB layer.

---

## 11. Success Criteria

- After cycle 1: the legacy realms are gone from the tree; the suite,
  linters, type check and a fresh install pass; documentation is accurate.
- After cycle 2: the remnants are gone; the runtime dependency list is seven
  packages; a fresh install contains none of the removed
  packages; the suite, linters, type check, `demux_realm`'s suite and Bifrost's
  fixture check all pass.
- Nothing under `trashcan/` is imported by anything under `yggdrasil/`,
  `lib/` or `tests/`.

---

## 12. Decisions

1. `cattrs` is declared as a runtime dependency (section 6.1, step 4).
2. `ngi_report_generator.py` and its test go in cycle 2.
3. No tag is dedicated to the cleanup; descriptive commit messages are the
   record, and git history is the archive.
4. The tech-debt ledger is a local, untracked file; its entry 15 is handled by
   the implementer as part of cycle 2 (section 6.1, step 7).
5. A public contract for stored documents is not part of this document and
   will have a PRD of its own.

---

## 13. Implementation Outcome

Each cycle landed as one commit: "refactor: remove legacy smartseq3, tenx and
delivery realms" and "refactor: remove legacy remnants and unused runtime
dependencies". Removed files are retrieved from the parent of the respective
commit.

| Measure | Before | After cycle 1 | After cycle 2 |
| --- | --- | --- | --- |
| `pytest tests/` | 2813 passed | 2813 passed | 2772 passed |
| `ruff check .` findings | 37 | 35 | 34 |
| Runtime dependencies | 16 | 16 | 7 |
| Fresh `.[dev]` environment | not measured | 848 MB | 187 MB |

The 41 tests removed in cycle 2 are the module resolver's (21), the NGI report
generator's (11) and the `ProjectDBManager` registry tests (9). `cattrs` is
pinned to 26.2.1. `demux_realm`'s suite passed after each cycle, and the dev
daemon discovered `test_realm` and started its watchers.

Where the implementation departed from the plan:

1. **Ruff was not green on the base revision.** It reported 37 findings
   before the cleanup; the pre-commit hook runs ruff with `--exit-zero`, so
   they were never enforced. The cleanup introduced none, and the remaining
   34 are out of scope here. "Ruff passes" in sections 3, 5.2 and 11 held as
   "no new findings".
2. **Bare `pytest` collects `trashcan/tests/`**, which fail to import. The
   suite was run as `pytest tests/`, as CI does.
3. **`ProjectDBManager.fetch_changes()` was removed entirely**, since its only
   job was the registry lookup. The commented-out `run_once` block in
   `yggdrasil_core.py` was deleted whole rather than just its import line,
   because it was built on the module resolver and
   `BestPracticeAnalysisHandler`.
4. **Tech-debt ledger entry 15 was resolved.** Neither `ScenarioDocWatcher`
   nor `BestPracticeAnalysisHandler` was referenced anywhere; both were
   removed, and the docs that described them were updated.
5. **An external realm depended on the removed shim.** `tenx_realm` imported
   `yggdrasil.handlers.base_handler`, which the `lib.*` aliasing import hook
   resolved to `lib/handlers/base_handler.py`. Section 9 did not list it.
   After cycle 2 the daemon logged the failed load and skipped that handler;
   `tenx_realm` now imports `yggdrasil.flow.base_handler` and loads again.
6. **Bifrost's fixture check was not run.** Its generator refuses any
   Yggdrasil revision other than its pinned `SOURCE_REVISION`, which is the
   cleanup's base revision. Of the modules it loads, only
   `lib/couchdb/project_db_manager.py` changed, and the generator never
   imports it, since it replaces `lib.couchdb` with an empty package and loads
   only `partitions`. Checking against the new revision requires moving
   Bifrost's pin.
