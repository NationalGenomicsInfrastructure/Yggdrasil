# Yggdrasil – Package Consolidation: `lib` under `yggdrasil` (PRD)

## Version
v0.3 (2026-10-09; v0.2 reviewed by the owner, decisions folded in; the second
review's three execution clarifications added to sections 5.4 and 12)

## Status
Implemented; outcome and deviations recorded in section 18. The record covers
the `refactor/package-consolidation` branch: cycles 1 to 3 and the
documentation step of cycle 4.

---

## 1. Overview

Fold the top-level `lib` package into `yggdrasil/`, dissolve the `core_utils`
grab-bag into packages named for what they are, gather the CouchDB storage
backend in one place, retire the `lib.*` meta-path import hook and the
`ygg.handler` legacy entry-point group, and move the code that the current
architecture has already replaced into the trashcan. The result is one
coherent, installable package that a second project (Bifrost) can depend on
from a Git tag, and in which the real layering of the code is visible from
its paths.

Behaviour does not change. Classes keep their names; code moves between
files only where section 7 says so.

---

## 2. Problem Statement

Verified against `dev` (caacacc, the merge of `main`/v0.4.0 into `dev` by
PR #106; its tree is identical to f35b031, on which the investigation was
done), demux_realm `master` and Bifrost `main`:

- `lib/` holds 66 Python files (17 730 lines) in seven subpackages; the
  daemon, coordinator, watchers, ops consumer, storage and realm loading all
  live there, while the Flow API, Engine and realm descriptor live in
  `yggdrasil/` (41 files, 7 543 lines).
- The split is not the layering it claims to be. `yggdrasil.flow` imports
  `lib.core_utils.event_types`, `lib.core_utils.runtime_paths` and
  `lib.core_utils.external_systems_resolver`; `yggdrasil.core` imports
  `lib.watchers.watchspec` and `lib.core_utils.logging_utils`. "Public
  `yggdrasil` on top of internal `lib`" is already false.
- External code imports both halves: demux_realm's descriptor and handler
  import `lib.core_utils.event_types` and `lib.watchers.watchspec` next to
  `yggdrasil.core.realm` and `yggdrasil.flow.*`; its tests import
  `lib.core_utils.plan_eligibility`, `lib.core_utils.plan_execution`,
  `lib.storage` and `lib.storage.sqlite`. Bifrost's fixture generator maps
  `lib.storage`, `lib.ops`, `lib.couchdb` and imports
  `lib.storage.plan_documents`, `lib.storage.plan_updates`, `lib.ops.snapshot`
  and `lib.couchdb.partitions`. The realm-authoring guide, cookbook, glossary
  and test-realm reference all teach `lib.*` imports.
- `yggdrasil/__init__.py` installs a meta-path finder that aliases
  `yggdrasil.<pkg>` onto `lib.<pkg>` and inserts the repository root into
  `sys.path`. The repository-cleanup outcome records that `tenx_realm` broke
  through exactly such an alias. Nothing in the three repositories imports
  the alias path today; only `tests/test_init.py` exercises it.
- `lib/core_utils` is a grab-bag: process configuration and mode, logging,
  cross-cutting errors, the trigger-event enum, the 1 621-line daemon
  orchestrator, the 1 320-line execution coordinator, the daemon lock, two
  helpers, and a pure plan-document rule.
- The two internal-storage backends are shaped differently: SQLite is one
  self-contained module (`lib/storage/sqlite.py`), while the CouchDB backend
  is spread over `lib/couchdb/plan_db_manager.py`,
  `lib/watchers/backends/checkpoint_store.py`, `lib/ops/sinks/couch.py` and
  `lib/storage/couch.py`. `lib/couchdb` mixes a generic client layer with
  Yggdrasil-database managers.
- Code the current architecture replaced is still in the tree: an orphaned
  checkpoint store, the legacy project-document model and its manager API,
  a deprecated CouchDB watcher, a dead `_changes` poller, the module-registry
  helpers, a draft-publishing method of the ops sink, and the `ygg.handler`
  discovery path (section 6, each with its replacement).
- `pyproject.toml` lists `packages = ["yggdrasil", "lib"]` and registers the
  test realm as `lib.realms.test_realm:get_realm_descriptor`.
- `lib/couchdb/`, `lib/ops/` and `lib/ops/sinks/` have no `__init__.py`: they
  are implicit namespace packages. They happen to be included in the built
  wheel today, but the new tree gives every package a real `__init__.py`.

---

## 3. Design Principles

1. **One package, real layering visible.** Bottom: `config`, `logging_utils`,
   `errors`. Above: `flow`, `watchers`, `couchdb`. Then `storage`, `ops`.
   On top: `daemon`, `cli`. Realms plug in through `yggdrasil.core.realm`.
2. **Move, do not rewrite.** Behaviour is identical after every cycle, with
   one deliberate exception for library importers (section 8: logging setup
   leaves import time). Class and function names do not change. Code moves
   between files only where section 7 lists it.
3. **Nothing is deleted.** Every removed file goes to `trashcan/lib/<original
   path>` (tests to `trashcan/tests/<original path>`) before `git rm`. A file
   that only loses code keeps a full copy of its pre-change version in the
   trashcan at its original path. `trashcan/` is gitignored; git history is
   the archive, as in the repository-cleanup PRD.
4. **Light `__init__` modules.** Importing `yggdrasil.flow` or the data types
   of `yggdrasil.watchers` must not import the CouchDB client library
   (section 8).
5. **One cycle, one reviewed pull request, one green suite; one merge into
   `dev`.** Each cycle is a pull request against an integration branch off
   `dev` and ends with `pytest tests/`, `ruff check .` (no new findings),
   `black --check .`, `mypy .`, a fresh `pip install .[dev]`, and
   demux_realm's suite passing. The integration branch merges into `dev`
   once, after cycle 4 (section 12).
6. **No shims.** The `lib` name leaves the distribution in one release;
   consumers update their imports in the same release.
7. **Names say what a package is, not where its code came from.**
   `core_utils`, `module_utils` and `mocks` disappear.

---

## 4. Target Layout

```
yggdrasil/
  __init__.py            version only (finder and sys.path insertion removed)
  __main__.py, cli.py, logo_utils.py, assets/, py.typed
  errors.py              cross-cutting exceptions
  logging_utils.py       custom_logger, configure_logging
  config/                process configuration and mode
    __init__.py          exports ConfigLoader, YggSession, workspace helpers only
    loader.py            ConfigLoader
    session.py           YggSession
    workspace.py         workspace_path, config_dir, get_path
    external_systems.py  resolver, normalize_url, DEFAULT_USER_ENV/PASS_ENV
    runtime_paths.py     work root and event spool resolution
  core/                  unchanged: engine, scheduler, execution_ids, realm/
  flow/                  unchanged
  watchers/              ingress framework
    __init__.py          light exports (section 8)
    events.py            EventType, YggdrasilEvent
    watchspec.py, filter_eval.py, config_validation.py, manager.py
    abstract_watcher.py  AbstractWatcher
    plan_watcher.py, seq_data_watcher.py
    backends/__init__.py, base.py, couchdb.py
    backends/checkpoint_store.py   InMemoryCheckpointStore
  couchdb/               generic client layer (+ one external-database handler)
    __init__.py (light)
    connection.py, models.py, changes_fetcher.py, defaults.py
    project_db_manager.py          external `projects` database (run-doc path)
  storage/               backend-neutral internal storage and document rules
    __init__.py, protocols.py, config.py, factory.py, errors.py
    plan_documents.py, plan_updates.py, plan_eligibility.py, partitions.py
    sqlite.py
    couchdb/__init__.py (light)
    couchdb/bundle.py              CouchPlanChangeSource, build_couchdb_bundle
    couchdb/plan_store.py          PlanDBManager
    couchdb/checkpoint_store.py    CouchDBCheckpointStore
    couchdb/ops_sink.py            OpsWriter
    couchdb/yggdrasil_db_manager.py  YggdrasilDBManager (coordination database)
  ops/                   event spool → plan_status projection
    __init__.py (light)
    consumer.py, snapshot.py
  daemon/                the service and its one-shot sibling
    __init__.py (light)
    core.py              YggdrasilCore
    plan_execution.py    PlanExecutionCoordinator
    lock.py              DaemonLock
    ops_service.py       OpsConsumerService
    worker_futures.py, singleton.py
  realms/
    __init__.py
    test_realm/          __init__, handler, recipes, steps, validate_*.py
  toolkit/               transitional: pre-Flow-API HPC helpers, reshaped by draft 01
    __init__.py, sjob_manager.py, slurm_utils.py, hpc_submission_policy.py,
    report_transfer.py, mock_sjob_manager.py
```

`yggdrasil.core` keeps its current meaning (Engine, scheduler, execution
IDs, realm descriptor); the orchestrator lives in `yggdrasil.daemon`. Freeing
the name `core` for the orchestrator is deferred (section 14).

---

## 5. File Mapping

Every file under `lib/`, with one destination. "move" is a `git mv` plus
import rewriting; "trim" means code is removed from the file and its
pre-change version is copied to the trashcan (section 6); "split" and
"merge" are the code moves of section 7.

### 5.1 `lib/core_utils` (dissolved)

| Current | Destination | Action |
| --- | --- | --- |
| `lib/core_utils/__init__.py` | – | trashcan (empty file) |
| `lib/core_utils/common.py` | `yggdrasil/config/workspace.py` (`workspace_path`, `config_dir`, `get_path`, `DEFAULT_WORKSPACE_PATH`, `CONFIG_DIR`); `normalize_url` → `yggdrasil/config/external_systems.py` | split; trim (dead members removed) |
| `lib/core_utils/config_loader.py` | `yggdrasil/config/loader.py` | move |
| `lib/core_utils/ygg_session.py` | `yggdrasil/config/session.py` | move |
| `lib/core_utils/external_systems_resolver.py` | `yggdrasil/config/external_systems.py` | move; receives `normalize_url`; owns `DEFAULT_USER_ENV`/`DEFAULT_PASS_ENV` |
| `lib/core_utils/runtime_paths.py` | `yggdrasil/config/runtime_paths.py` | move |
| `lib/core_utils/logging_utils.py` | `yggdrasil/logging_utils.py` | move |
| `lib/core_utils/errors.py` | `yggdrasil/errors.py` | move |
| `lib/core_utils/event_types.py` | `yggdrasil/watchers/events.py` | move; merge `YggdrasilEvent` into it |
| `lib/core_utils/plan_eligibility.py` | `yggdrasil/storage/plan_eligibility.py` | move |
| `lib/core_utils/yggdrasil_core.py` | `yggdrasil/daemon/core.py` | move; trim (`ygg.handler` discovery removed) |
| `lib/core_utils/plan_execution.py` | `yggdrasil/daemon/plan_execution.py` | move |
| `lib/core_utils/daemon_lock.py` | `yggdrasil/daemon/lock.py` | move |
| `lib/core_utils/worker_futures.py` | `yggdrasil/daemon/worker_futures.py` | move |
| `lib/core_utils/singleton_decorator.py` | `yggdrasil/daemon/singleton.py` | move |

### 5.2 `lib/couchdb`

| Current | Destination | Action |
| --- | --- | --- |
| `lib/couchdb/couchdb_connection.py` | `yggdrasil/couchdb/connection.py` | move |
| `lib/couchdb/couchdb_models.py` | `yggdrasil/couchdb/models.py` | move |
| `lib/couchdb/changes_fetcher.py` | `yggdrasil/couchdb/changes_fetcher.py` | move |
| `lib/couchdb/couchdb_defaults.py` | `yggdrasil/couchdb/defaults.py` | move; imports the env-var defaults and `normalize_url` from `config/external_systems.py` and re-exports the defaults |
| `lib/couchdb/partitions.py` | `yggdrasil/storage/partitions.py` | move |
| `lib/couchdb/plan_db_manager.py` | `yggdrasil/storage/couchdb/plan_store.py` | move |
| `lib/couchdb/yggdrasil_db_manager.py` | `yggdrasil/storage/couchdb/yggdrasil_db_manager.py` | move; trim (constructor only) |
| `lib/couchdb/project_db_manager.py` | `yggdrasil/couchdb/project_db_manager.py` | move; trim (`get_changes` removed) |
| `lib/couchdb/watcher_checkpoint_store.py` | – | trashcan |
| `lib/couchdb/yggdrasil_document.py` | – | trashcan |

### 5.3 `lib/storage`

| Current | Destination | Action |
| --- | --- | --- |
| `lib/storage/__init__.py` | `yggdrasil/storage/__init__.py` | move |
| `lib/storage/protocols.py` | `yggdrasil/storage/protocols.py` | move |
| `lib/storage/config.py` | `yggdrasil/storage/config.py` | move |
| `lib/storage/factory.py` | `yggdrasil/storage/factory.py` | move |
| `lib/storage/errors.py` | `yggdrasil/storage/errors.py` | move |
| `lib/storage/plan_documents.py` | `yggdrasil/storage/plan_documents.py` | move |
| `lib/storage/plan_updates.py` | `yggdrasil/storage/plan_updates.py` | move |
| `lib/storage/sqlite.py` | `yggdrasil/storage/sqlite.py` | move |
| `lib/storage/couch.py` | `yggdrasil/storage/couchdb/bundle.py` | move |

### 5.4 `lib/ops`

| Current | Destination | Action |
| --- | --- | --- |
| `lib/ops/snapshot.py` | `yggdrasil/ops/snapshot.py` | move |
| `lib/ops/consumer.py` | `yggdrasil/ops/consumer.py` | move |
| `lib/ops/consumer_service.py` | `yggdrasil/daemon/ops_service.py` | move |
| `lib/ops/sinks/couch.py` | `yggdrasil/storage/couchdb/ops_sink.py` | move; trim (`upsert_plan_draft` removed); keeps its own endpoint resolution, imports only the shared constants and `normalize_url` (section 7, item 4) |

### 5.5 `lib/watchers`

| Current | Destination | Action |
| --- | --- | --- |
| `lib/watchers/__init__.py` | `yggdrasil/watchers/__init__.py` | rewrite (section 8) |
| `lib/watchers/watchspec.py` | `yggdrasil/watchers/watchspec.py` | move |
| `lib/watchers/filter_eval.py` | `yggdrasil/watchers/filter_eval.py` | move |
| `lib/watchers/config_validation.py` | `yggdrasil/watchers/config_validation.py` | move |
| `lib/watchers/manager.py` | `yggdrasil/watchers/manager.py` | move |
| `lib/watchers/abstract_watcher.py` | `yggdrasil/watchers/abstract_watcher.py` | move; `YggdrasilEvent` merged into `events.py` |
| `lib/watchers/plan_watcher.py` | `yggdrasil/watchers/plan_watcher.py` | move |
| `lib/watchers/seq_data_watcher.py` | `yggdrasil/watchers/seq_data_watcher.py` | move |
| `lib/watchers/couchdb_watcher.py` | – | trashcan |
| `lib/watchers/backends/__init__.py` | `yggdrasil/watchers/backends/__init__.py` | move |
| `lib/watchers/backends/base.py` | `yggdrasil/watchers/backends/base.py` | move |
| `lib/watchers/backends/couchdb.py` | `yggdrasil/watchers/backends/couchdb.py` | move |
| `lib/watchers/backends/checkpoint_store.py` | `yggdrasil/watchers/backends/checkpoint_store.py` (`InMemoryCheckpointStore`) and `yggdrasil/storage/couchdb/checkpoint_store.py` (`CouchDBCheckpointStore`) | split |

### 5.6 `lib/realms`, `lib/module_utils`, `lib/mocks`, `lib/__init__.py`

| Current | Destination | Action |
| --- | --- | --- |
| `lib/realms/__init__.py` | `yggdrasil/realms/__init__.py` | move |
| `lib/realms/test_realm/__init__.py` | `yggdrasil/realms/test_realm/__init__.py` | move |
| `lib/realms/test_realm/handler.py` | `yggdrasil/realms/test_realm/handler.py` | move |
| `lib/realms/test_realm/recipes.py` | `yggdrasil/realms/test_realm/recipes.py` | move |
| `lib/realms/test_realm/steps.py` | `yggdrasil/realms/test_realm/steps.py` | move |
| `lib/realms/test_realm/validate_custom_steps.py` | `yggdrasil/realms/test_realm/validate_custom_steps.py` | move; usage line in docstring corrected |
| `lib/realms/test_realm/validate_random_recipe.py` | `yggdrasil/realms/test_realm/validate_random_recipe.py` | move; usage line in docstring corrected |
| `lib/module_utils/__init__.py` | `yggdrasil/toolkit/__init__.py` | move; docstring states the package is transitional and its API is defined by draft 01 |
| `lib/module_utils/sjob_manager.py` | `yggdrasil/toolkit/sjob_manager.py` | move |
| `lib/module_utils/slurm_utils.py` | `yggdrasil/toolkit/slurm_utils.py` | move |
| `lib/module_utils/hpc_submission_policy.py` | `yggdrasil/toolkit/hpc_submission_policy.py` | move |
| `lib/module_utils/report_transfer.py` | `yggdrasil/toolkit/report_transfer.py` | move |
| `lib/mocks/mock_sjob_manager.py` | `yggdrasil/toolkit/mock_sjob_manager.py` | move |
| `lib/mocks/__init__.py` | – | trashcan |
| `lib/__init__.py` | – | trashcan (empty file) |

### 5.7 Files under `yggdrasil/` that change, and new files

| File | Change |
| --- | --- |
| `yggdrasil/__init__.py` | Rewritten: docstring and `__version__` only |
| `yggdrasil/flow/base_handler.py`, `yggdrasil/flow/events/emitter.py`, `yggdrasil/flow/data_access/*.py`, `yggdrasil/core/engine.py`, `yggdrasil/core/realm/*.py`, `yggdrasil/cli.py` | Import lines only |
| `yggdrasil/config/__init__.py`, `yggdrasil/daemon/__init__.py`, `yggdrasil/storage/couchdb/__init__.py`, `yggdrasil/couchdb/__init__.py`, `yggdrasil/ops/__init__.py` | New, light (section 8); the last two replace implicit namespace packages |
| `yggdrasil/watchers/events.py`, `yggdrasil/storage/couchdb/checkpoint_store.py` | New, by merge/split (section 7) |
| `yggdrasil/config/workspace.py` | `common.py` moved whole in cycle 2; `YggdrasilUtilities` dissolved into functions in cycle 3 |

### 5.8 Tests

| Test file | Action |
| --- | --- |
| `tests/test_init.py` | trashcan (tested the alias finder) |
| `tests/test_couchdb_watcher.py` | trashcan |
| `tests/test_watcher_checkpoint_store.py` | trashcan |
| `tests/test_yggdrasil_document.py` | trashcan |
| `tests/test_yggdrasil_db_manager.py` | trim to constructor tests; copy to trashcan |
| `tests/test_project_db_manager.py` | trim (`get_changes` tests); copy to trashcan |
| `tests/test_common.py` | trim (dead-member tests); copy to trashcan |
| `tests/test_setup_realms.py` | trim (11 `ygg.handler` cases); copy to trashcan |
| `tests/test_ops_sinks_couch.py` | trim (`upsert_plan_draft` cases, 17 mentions); copy to trashcan |
| all other test files | import lines and `patch()` targets rewritten; files stay in place |
| `tests/test_import_lightness.py` | new (section 8) |

---

## 6. Removals and Trims

Each item was checked in git history for what replaced it, not only for a
missing caller.

| Item | Replacement and evidence |
| --- | --- |
| `lib/couchdb/watcher_checkpoint_store.py` (`WatcherCheckpointStore`) | Phase 1 of the approval workflow (01bba71). `PlanWatcher` moved to `CouchDBCheckpointStore` in 04b1297, the test-realm watcher in b2b0baf. Both write `watcher_checkpoint:<name>` documents; the newer class implements the `CheckpointStore` protocol and is what the storage bundle uses. Only its own test imports it. |
| `lib/couchdb/yggdrasil_document.py` and the project/sample API of `YggdrasilDBManager` (`create_project`, `save_document`, `get_document_by_project_id`, `check_project_exists`, `add_sample`, `update_sample_status`, `add_ngi_report_entry`, `update_sample_slurm_job_id`, `@auto_load_and_save`) | The per-project tracking document of the legacy realms. At 2dbdd9c its callers were `lib/base/abstract_project.py`, `lib/realms/delivery/deliver.py`, `bp_analysis_handler.py` and `test_realm/watcher.py`, all removed by the cleanup. Plan documents (`storage/plan_documents.py`) and `plan_status` snapshots replaced what it tracked. Live callers of the manager (checkpoint store, PlanWatcher's legacy default, the bundle builder, the REPL examples in `docs/reference/test_realm.md`) use only `CouchDBHandler` members, so the class keeps its constructor: a handler bound to the coordination database, which draft 03's lease document will use again through a `LeaseStore` protocol. |
| `ProjectDBManager.get_changes`, `YggdrasilUtilities.get_last_processed_seq` / `save_last_processed_seq`, the `.last_processed_seq` file | The pre-WatchSpec polling loop fed to the old `CouchDBWatcher(changes_fetcher=…)`. The approval-workflow phases document records the extraction of `ChangesFetcher` from it; `CouchDBBackend` + `WatchSpec` replaced project watching. `YggdrasilCore` calls only `pdm.fetch_document_by_id`. The trimmed class stays as the handler of the external `projects` database for the run-doc path, which is itself due for redesign (section 14). |
| `OpsWriter.upsert_plan_draft` | Introduced with the ops consumer (f25ff5e) to publish a `plan_draft` document with `approved: False` into the ops database. Its only callers ever were the legacy `tenx` and `tenx_ext` handlers, removed in 4c76c10. `PlanStore.save_plan` into `yggdrasil_plans` (called by `YggdrasilCore._persist_plan_draft`) replaced the mechanism; nothing reads `plan_draft` documents. |
| `lib/watchers/couchdb_watcher.py` (`CouchDBWatcher`) | Carries a `DeprecationWarning` naming `CouchDBBackend` + `WatchSpec`; the guide documents the migration; only its own test imports it. |
| `YggdrasilUtilities.load_realm_class`, `load_module`, `module_cache`, `env_variable` | Module-registry era; `module_resolver.py` and `module_registry.json` were removed by the cleanup. No caller outside `tests/test_common.py`. |
| `ygg.handler` entry-point discovery (`_discover_legacy_handlers`, `_derive_realm_id` and the legacy dedup state in `YggdrasilCore`) | `ygg.realm` with a `RealmDescriptor` is the current mechanism (this repository, demux_realm). Retired in this release (decision 5). |
| Duplicate definitions of `DEFAULT_USER_ENV` / `DEFAULT_PASS_ENV` (`couchdb_defaults.py`, `ops/sinks/couch.py`, `external_systems_resolver.py`) | One definition in `config/external_systems.py`; `couchdb/defaults.py` re-exports it; `OpsWriter` imports it. `OpsWriter`'s own endpoint resolution is kept (section 7, item 4). |

Kept, deliberately: `seq_data_watcher.py` and the `watchdog` dependency (the
only implementation of the marker-aggregation semantics the watcher
architecture PRD defers to its filesystem backend), the two `validate_*.py`
scripts (standalone validation scripts for the test realm; both run at HEAD),
the singleton decorator on `YggdrasilCore`, the legacy `EventType` members
(`PROJECT_CHANGE` is read by the run-doc path), the CLI flag
`--manual-submit` and `YggSession.init_manual_submit` (draft 01 retires
them; note that `is_manual_submit()` has had no reader since the cleanup).

---

## 7. Splits and Merges

The only code that moves between files. Each is one commit with its tests.

1. **`YggdrasilEvent` → `yggdrasil/watchers/events.py`**, next to
   `EventType`. `abstract_watcher.py` keeps `AbstractWatcher` and imports the
   event class from `events`.
2. **`YggdrasilUtilities` dissolved.** `workspace_path`, `config_dir`,
   `get_path`, `DEFAULT_WORKSPACE_PATH`, `CONFIG_DIR` become module-level
   functions and constants in `config/workspace.py` (same directory depth as
   today, so the checkout default still resolves to the repository root).
   `normalize_url` moves to `config/external_systems.py`. The seven call
   sites that use the `Ygg.` alias are updated by hand. The class is not
   kept.
3. **`CouchDBCheckpointStore` → `storage/couchdb/checkpoint_store.py`**;
   `InMemoryCheckpointStore` stays in `watchers/backends/checkpoint_store.py`.
   The moved class is typed against `CouchDBHandler` (it uses only
   `fetch_document_by_id`, `.server`, `.db_name`); the lazy default
   construction of a `YggdrasilDBManager` inside it stays, importing from
   its new module.
4. **`OpsWriter`**: `upsert_plan_draft` removed (cycle 1), which also drops
   the import of `yggdrasil.flow.planner`. Its endpoint resolution
   (`_get_couchdb_endpoint_config`) is kept as it is; the class only stops
   defining its own `DEFAULT_USER_ENV`/`DEFAULT_PASS_ENV` and imports them,
   with `normalize_url`, from `config/external_systems.py`. Replacing the
   resolution with `resolve_couchdb_params` would change accepted behaviour
   (verified): an explicit URL with default credential-variable names and no
   configured `couchdb` endpoint is accepted by `OpsWriter` and rejected by
   the resolver, and the resolver reads `main.json` even when every argument
   is explicit. Unifying the two is recorded as a tech-debt ledger entry
   (section 14.1), added in cycle 3, and `OpsWriter`'s docstring names it.
5. **Credential-name defaults**: `DEFAULT_USER_ENV`, `DEFAULT_PASS_ENV` and
   `normalize_url` are defined once in `config/external_systems.py`;
   `couchdb/defaults.py` imports and re-exports the two constants so its
   callers are unchanged. Direction of imports is `couchdb → config`, never
   the reverse.
6. **`YggdrasilDBManager`** trimmed to its constructor; `yggdrasil_document.py`
   and `auto_load_and_save` go to the trashcan with it.
7. **`ProjectDBManager`** trimmed to its constructor.
8. **`YggdrasilCore`**: `setup_realms` discovers `ygg.realm` only;
   `_discover_legacy_handlers`, `_derive_realm_id` and the legacy dedup
   state are removed. The run-doc paths are untouched.
9. **`yggdrasil/watchers/__init__.py`** rewritten (section 8).
10. **Logging setup leaves import time**: the one-letter level names
    (`DEBUG` → `D`, …) and the third-party logger levels (`matplotlib`,
    `numba`, `h5py`, `PIL`, `watchdog`, `ibm-cloud-sdk-core`,
    `ibmcloudant.cloudant_v1`, `urllib3.connectionpool`) move from module
    scope in `logging_utils` into `configure_logging()`. The CLI calls it
    first thing, so daemon and run-doc output is unchanged; an importer that
    never calls it (tests, Bifrost) no longer has its logging altered by an
    import. Tests that depended on the import-time state are adjusted.

---

## 8. Package `__init__` Policy

Verified on `dev`: a plain `import yggdrasil.flow` today imports neither the
CouchDB client nor `logging_utils` (`base_handler` imports `DataAccess` only
under `TYPE_CHECKING`; `watchspec.py` imports `RawWatchEvent` only under
`TYPE_CHECKING`), whereas `backends/base.py` and `abstract_watcher.py` import
`custom_logger` at module level, and `logging_utils` has import-time side
effects (one-letter level names, third-party logger levels). Moving
`EventType` into `watchers/events.py` makes `import yggdrasil.flow` execute
`watchers/__init__.py`, so that module must be as light as `flow` is now.

- `yggdrasil/__init__.py`: docstring and `__version__` (from
  `importlib.metadata`, with the `setuptools_scm` fallback for a checkout
  without install). No finder, no `sys.path` changes. Running from a
  checkout requires an editable install, which is already the documented
  development setup.
- `yggdrasil/watchers/events.py` imports only the standard library: no
  `custom_logger`.
- `yggdrasil/watchers/__init__.py` exports eagerly only `EventType`,
  `YggdrasilEvent`, `WatchSpec` and `BoundWatchSpec`, whose modules import
  nothing beyond `events`. Every other name — `RawWatchEvent`, `Checkpoint`,
  `CheckpointStore`, `WatcherBackend` (their module imports
  `custom_logger`), `FilterResult`, `evaluate_filter`,
  `WatcherConfigValidationIssue`, `WatcherConfigurationError`,
  `validate_watcher_config_wiring` (they reach the external-systems resolver
  and logging), `WatcherManager`, `WatcherBackendGroup`, `CouchDBBackend`,
  `InMemoryCheckpointStore` — is exported lazily through a module-level
  `__getattr__`, so the same names stay importable from the package.
  `CouchDBCheckpointStore` is no longer exported from `watchers` (it lives
  in `storage.couchdb`).
- `yggdrasil/config/__init__.py` exports `ConfigLoader` and `YggSession`
  from cycle 2; `workspace_path`, `config_dir`, `get_path` are added in
  cycle 3, when `YggdrasilUtilities` is dissolved. `external_systems` and
  `runtime_paths` are imported by module path, which keeps the existing
  cycle harmless: `logging_utils` imports `config.loader` and
  `config.session`; `config.external_systems` imports `logging_utils`.
- `yggdrasil/daemon/__init__.py`, `yggdrasil/storage/couchdb/__init__.py`,
  `yggdrasil/couchdb/__init__.py`, `yggdrasil/ops/__init__.py`: docstring only.
- `yggdrasil/storage/__init__.py`, `yggdrasil/watchers/backends/__init__.py`:
  unchanged content, rewritten paths.
- Process-wide logging setup happens in `configure_logging()`, not at import
  (section 7, item 10).
- Guard: `tests/test_import_lightness.py` imports `yggdrasil.flow` and
  `yggdrasil.watchers` in a subprocess and asserts that neither
  `ibmcloudant` nor `yggdrasil.logging_utils` is in `sys.modules`, that
  `logging.getLevelName(logging.DEBUG)` is still `"DEBUG"`, and that the
  `PIL` and `ibmcloudant.cloudant_v1` loggers still have level `NOTSET`.

---

## 9. Packaging and Release

- `pyproject.toml`: `[tool.setuptools] packages = ["yggdrasil"]`; entry
  point `test_realm = "yggdrasil.realms.test_realm:get_realm_descriptor"`;
  `package-data` unchanged. A wheel built from f35b031 with the current
  explicit list contains every subpackage (107 modules; verified), so the
  explicit form is kept; cycle 2 verifies again that the built wheel holds
  no `lib/` entry and all `yggdrasil/` subpackages.
- Dependencies unchanged (seven packages; `watchdog` stays). The lock file
  is regenerated and expected to be identical.
- The logger names change with the module paths
  (`lib.watchers.manager` → `yggdrasil.watchers.manager`); operators' log
  filters, if any, follow.
- Release: tag `v0.5.0` on the `main` merge commit, as `v0.4.0` was, then
  merge `main` back into `dev` (as PR #106 did for v0.4.0) so the tag stays
  reachable from `dev` and development builds report `0.5.0.postN`. Bifrost
  and demux_realm pin the tag. Until it exists, Bifrost's interim pin (draft
  09's pin move) should be caacacc or later, from which `v0.4.0` is reachable
  (a wheel from it reports `0.4.0.post6`); demux_realm needs no interim pin
  (section 12).

---

## 10. Consumers and Documentation

### 10.1 demux_realm

demux_realm's `ygg` extra keeps tracking `@dev` throughout (decision 14);
the import update and the move to the tag land together in cycle 4,
immediately after the single merge into `dev`.

| File | Change |
| --- | --- |
| `demux_realm/descriptor.py` lines 3–4 | `from yggdrasil.watchers import EventType, WatchSpec` |
| `demux_realm/handler.py` line 5 | `from yggdrasil.watchers import EventType` |
| `tests/test_realm_execution.py` lines 14–21 | `yggdrasil.storage.plan_eligibility`, `yggdrasil.daemon.plan_execution`, `yggdrasil.storage`, `yggdrasil.storage.sqlite` |
| `pyproject.toml` `ygg` extra | unchanged (`@dev`) until cycle 4; then `@v0.5.0`, in the same commit as the import update |

### 10.2 tenx_realm (external)

Any `lib.*` import becomes its `yggdrasil.*` counterpart;
`yggdrasil.flow.base_handler` is unchanged. If it still registers under
`ygg.handler`, it must register a `RealmDescriptor` under `ygg.realm`
(decision 5). The owner does not require it to keep working unchanged.

### 10.3 Bifrost

`backend/tools/generate_source_fixtures.py`: `MODULE_PATHS` and the imports
become `yggdrasil.storage.plan_documents`, `yggdrasil.storage.plan_updates`,
`yggdrasil.storage.partitions`, `yggdrasil.ops.snapshot`, with
`yggdrasil.flow.*` and `yggdrasil.core.execution_ids` unchanged. The
committed fixtures contain no `lib.` strings and stay valid. Done when the
pin moves (draft 09).

### 10.4 Documentation in this repository

| File | Change |
| --- | --- |
| `CLAUDE.md` | Architecture paths; the "Namespacing" line becomes: one package; realm-facing API is `yggdrasil.flow`, `yggdrasil.watchers` (`EventType`, `WatchSpec`), `yggdrasil.core.realm`; logging convention `custom_logger` from `yggdrasil.logging_utils`; docstring examples `yggdrasil/daemon/lock.py`, `yggdrasil/watchers/backends/base.py` |
| `README.md`, `docs/architecture/overview.md` | Project-structure / key-directories tables; file references at overview lines 33, 43, 68, 105, 118, 126 |
| `docs/realm_authoring/guide.md` | Imports at lines 26, 96–97, 287; the "From CouchDBWatcher" migration section (line 407) is removed with the class |
| `docs/realm_authoring/cookbook.md` line 13; `docs/reference/glossary.md` line 61 | Import paths |
| `docs/reference/test_realm.md` | REPL examples (806–846) import `YggdrasilDBManager` from its new module; paths at 992–994 |
| `docs/design/**` | Historical; unchanged |

---

## 11. Tests

The suite has 256 `lib.*` import lines and about 540 `patch("lib.…")`
target strings. Every one is a mapping-table rewrite: a script applies the
old-module → new-module table of section 5 (longest match first) to
`yggdrasil/`, `tests/`, `docs/` and `pyproject.toml`, covering import
statements, `patch()` strings, `assertLogs` names and dotted mentions in
docstrings and comments. Only the `Ygg.` call sites (section 7, item 2) are
edited by hand. Trashcanned and trimmed tests are listed in 5.8. One new
test guards import lightness (section 8). Test files are not moved; mirroring
the new tree under `tests/` is not part of this document.

---

## 12. Implementation Sequence

### Integration: one merge into `dev`

The four cycles are developed and reviewed one at a time, each as a pull
request against an integration branch off `dev`; that branch merges into
`dev` once, after cycle 4. The reason is demux_realm: its `ygg` extra tracks
`yggdrasil @ …@dev`, and realm discovery logs an import failure and
continues, so a fresh `demux-realm[ygg]` install made while `dev` holds a
consolidation cycle but demux_realm still imports `lib.*` would pair
incompatible versions and start the daemon without the realm. With a single
merge, demux_realm's import update follows within the same release step
(cycle 4, step 3), the window is minutes and under the owner's control, and
an interim pin of demux_realm to a fixed revision — considered during
review — would be one more thing to think about for no gain. demux_realm's
extra therefore stays on `@dev` until it moves to the tag.

### Cycle 1 – Removals and trims (in place, under `lib/`)

1. Copy every file of section 6, every file about to be trimmed, and every
   test file of 5.8 marked trim or trashcan to `trashcan/lib/<path>` or
   `trashcan/tests/<path>`; `git rm` the removed files and their tests
   (`watcher_checkpoint_store.py`, `yggdrasil_document.py`,
   `couchdb_watcher.py`, `lib/mocks/__init__.py`, `test_couchdb_watcher.py`,
   `test_watcher_checkpoint_store.py`, `test_yggdrasil_document.py`).
2. Trim in place: `YggdrasilDBManager` (constructor only; `auto_load_and_save`
   goes with the removed methods), `ProjectDBManager` (`get_changes`),
   `YggdrasilUtilities` (the six dead members), `OpsWriter`
   (`upsert_plan_draft`), `YggdrasilCore` (`ygg.handler` discovery); trim
   the five tests accordingly.
3. Remove the "From CouchDBWatcher" section from the realm-authoring guide.

Verification: full check suite; `grep -rn "ygg.handler\|upsert_plan_draft\|
YggdrasilDocument\|WatcherCheckpointStore\|CouchDBWatcher\|load_realm_class\|
last_processed_seq\|auto_load_and_save"` over `yggdrasil/`, `lib/`,
`tests/`, `docs/` (excluding `docs/design/`) returns nothing; demux_realm's
suite passes against the checkout unchanged (no import path has moved yet);
the daemon smoke test below.

Acceptance: the items of section 6 are absent from the tree and present
under `trashcan/` at their original paths; the test count equals the
previous count minus the removed and trimmed cases; checks green.

### Cycle 2 – Relocation

1. Rewrite `yggdrasil/__init__.py` (version only); move `tests/test_init.py`
   to the trashcan.
2. `git mv` every remaining file per section 5 into its destination,
   creating `config/`, `daemon/`, `storage/couchdb/`, `toolkit/`, `realms/`;
   add the five light `__init__` files of 5.7, with `config/__init__.py`
   exporting `ConfigLoader` and `YggSession` only. `lib/watchers/__init__.py`
   and `lib/watchers/backends/__init__.py` keep their content with rewritten
   paths; the light rewrite of `watchers/__init__.py` is cycle 3. In this
   cycle `watchers/__init__.py` additionally exports `EventType` from
   `yggdrasil.watchers.events`, so the final realm-facing import
   (`from yggdrasil.watchers import EventType, WatchSpec`) works from cycle 2
   on and demux_realm's imports are changed once, not twice.
3. Run the rewrite script over `yggdrasil/`, `tests/`, `docs/` and
   `pyproject.toml`; update `packages` and the entry point;
   `ruff check --select I --fix` for import order; `black .`.
4. No code moves between files in this cycle. Files whose final shape is a
   split or a merge (section 7) move whole and are reshaped in cycle 3:
   `common.py` → `config/workspace.py` with `YggdrasilUtilities` intact
   (callers import it from there); `watchers/backends/checkpoint_store.py` →
   `yggdrasil/watchers/backends/checkpoint_store.py` complete, with
   `CouchDBCheckpointStore` still inside (extracted to `storage/couchdb/` in
   cycle 3); `event_types.py` → `watchers/events.py` holding `EventType`
   only; `abstract_watcher.py` → `watchers/abstract_watcher.py` with
   `YggdrasilEvent` still inside (merged into `events.py` in cycle 3).

Verification: `grep -rn "\blib\." yggdrasil tests docs pyproject.toml`
returns only `docs/design/`; full check suite; demux_realm's suite against
the checkout with its imports updated on a branch (not merged yet);
`pip wheel . --no-deps` produces a wheel without `lib/` and with every
`yggdrasil/` subpackage; the daemon smoke test.

Acceptance: `lib/` is absent from the tree; all checks pass; the test count
equals cycle 1's minus the `test_init` cases.

### Cycle 3 – Splits, merges and light packages

One commit per item of section 7, each with its tests. `config/__init__.py`
gains the workspace exports when item 2 lands; the `watchers/__init__.py`
rewrite, the `configure_logging()` move and `tests/test_import_lightness.py`
come last.

Verification: full check suite; the lightness test passes; the daemon smoke
test; in a fresh install, `python -c "import yggdrasil.flow,
yggdrasil.watchers"` imports neither `ibmcloudant` nor
`yggdrasil.logging_utils` (the same check, by hand).

### Cycle 4 – Documentation, consumers, release

1. Documentation changes of 10.4; `CLAUDE.md`.
2. Merge the integration branch into `dev` (the single merge); merge `dev`
   into `main`; tag `v0.5.0`; merge `main` back into `dev`.
3. Immediately after: demux_realm's import update (10.1) and move to the
   tag, in one commit on its `master`; tenx_realm notified or updated
   (10.2); Bifrost's pin move is draft 09.

Verification, in an empty virtual environment and from a directory outside
the source checkout: `pip install "yggdrasil @ git+…@v0.5.0"` (or the built
wheel); `python -c "import lib"` fails with `ModuleNotFoundError`;
`python -c "import yggdrasil.daemon.core, yggdrasil.cli"` succeeds;
`python -c "import yggdrasil.storage.plan_documents,
yggdrasil.storage.plan_updates, yggdrasil.storage.partitions,
yggdrasil.ops.snapshot, yggdrasil.flow.model, yggdrasil.flow.outcomes,
yggdrasil.flow.attempt, yggdrasil.core.execution_ids"` (the modules
Bifrost's generator loads) succeeds; `yggdrasil --version` prints 0.5.0;
demux_realm's suite passes against the tag.

### Daemon smoke test

Every cycle's smoke test runs in an isolated environment, never against the
developer's workspace, databases or event spool: `YGG_HOME` points at a
throwaway workspace whose `dev_main.json` selects SQLite internal storage and
points the test realm's watch connection at a disposable CouchDB database (a
local container, or a scratch database on the development server);
`YGG_WORK_ROOT` and `YGG_EVENT_SPOOL` point under the same throwaway root.
`yggdrasil --dev daemon` must discover `test_realm` (and `dmx_realm` when
demux_realm is installed), start its watchers, and stop cleanly on
interrupt. `--dev` alone does not isolate anything.

---

## 13. Backward Compatibility

- Every `lib.*` import path breaks, by design; no shim is provided. The
  `yggdrasil.<libpkg>` alias path (never used by any consumer) disappears
  with the finder.
- `ygg.handler` entry points are no longer discovered.
- demux_realm's `ygg` extra tracks `@dev`; between the merge into `dev` and
  demux_realm's import update, a fresh `demux-realm[ygg]` install would pair
  incompatible versions and the daemon would start without the realm. The
  single merge followed immediately by demux_realm's commit (section 12)
  keeps that window to minutes; there is no interim pin.
- Documents written to `yggdrasil_plans`, `yggdrasil_ops`, the coordination
  database and the event spool are unchanged in shape. One exception: plan
  documents store each step's function as a module path (`fn_ref`); plans
  written before this release whose steps live under
  `lib.realms.test_realm` cannot be re-executed afterwards
  (`ModuleNotFoundError` at `resolve_callable`). This affects only test-realm
  plans in development databases; the owner deletes them and regenerates
  them. External realms' `fn_ref`s point at their own packages. Fingerprints
  hash params, inputs and outputs, not `fn_ref`, so no success marker is
  invalidated.
- CLI, flags, configuration files, dependencies and database names are
  unchanged. Logger names change with module paths.
- Running Yggdrasil from a checkout now requires an editable install (the
  `sys.path` insertion is gone).

---

## 14. Out of Scope

- Renaming `yggdrasil.core` to free the name for the orchestrator.
- The filesystem watcher backend (turning `SeqDataDetector` into a
  `WatcherBackend`).
- The toolkit API, `ctx.jobs` / `ctx.transfer` and the final position of the
  `toolkit/` contents (draft 01).
- The document contract, schemas and timestamp-format unification
  (`…Z` in events versus `+00:00` in plan documents) (draft 08).
- Removing PlanWatcher's legacy bare-construction defaults (always inject
  from the bundle): ledger entry, section 14.1.
- Redesigning run-doc as a generic tool and removing Yggdrasil's internal
  dependency on the external `projects` database (`ProjectDBManager`).
- Realm selection (include/exclude) and the policy for realms that fail to
  load (draft 12).
- Splitting `storage/sqlite.py` into a package; moving tests to mirror the
  new tree.
- Unifying `OpsWriter`'s endpoint resolution with `resolve_couchdb_params`
  (section 7, item 4): ledger entry, section 14.1.

### 14.1 Tech-debt ledger entries added by this PRD

Follow-ups that this document identifies but does not execute are recorded
where the repository keeps such debt, `docs/TECH_DEBT_LEDGER.md`, during
cycle 3, so they survive the completion of this PRD. The executing agent
appends the two entries below with the next free numbers and references
them from the named docstrings, as CLAUDE.md prescribes.

Entry A — `OpsWriter` endpoint resolution. *`OpsWriter`
(`yggdrasil/storage/couchdb/ops_sink.py`) resolves its CouchDB endpoint with
a private copy of the lookup instead of `resolve_couchdb_params`
(`yggdrasil/couchdb/defaults.py`). The two differ: an explicit URL with
default credential-variable names and no configured `couchdb` endpoint is
accepted by `OpsWriter` and rejected by the resolver, and the resolver reads
`main.json` even when every argument is explicit. Unify them on one
documented rule (explicit arguments are used without reading configuration;
the endpoint is consulted only for missing values), move `OpsWriter` onto
it, and test both cases.* Referenced from the `OpsWriter` class docstring.

Entry B — `PlanWatcher` legacy wiring. *`PlanWatcher`
(`yggdrasil/watchers/plan_watcher.py`) constructs `PlanDBManager`,
`YggdrasilDBManager` and `CouchPlanChangeSource` itself when no store is
injected. This legacy default is the only reason the `watchers` package
imports the CouchDB storage backend. Make injection mandatory
(`YggdrasilCore` always passes the bundle's stores), remove the defaults and
the imports, and adjust the tests that rely on bare construction.*
Referenced from the `PlanWatcher` constructor docstring.

The same items are listed in the local PRD working set's README
(`/project/prds/00_README.md`, "Follow-ups"), which is where new drafts are
planned from.

---

## 15. Success Criteria

- In a fresh environment installed from the tag, `python -c "import lib"`
  fails and the wheel contains no `lib/` entry.
- `pytest tests/`, `ruff check .` (no new findings), `black --check .`,
  `mypy .` and a fresh `pip install .[dev]` pass after every cycle.
- demux_realm's suite passes against the tag after the import update of
  10.1; `yggdrasil --dev daemon` discovers `test_realm` and `dmx_realm` and
  starts their watchers.
- Bifrost's generator loads modules from `yggdrasil.*` only (after draft
  09's pin move).
- `grep -rn "\blib\." yggdrasil tests docs` returns nothing outside
  `docs/design/`.
- Importing `yggdrasil.flow` and `yggdrasil.watchers` imports neither
  `ibmcloudant` nor `yggdrasil.logging_utils` and leaves the logging
  configuration untouched (`tests/test_import_lightness.py`).
- From an empty virtual environment outside the checkout, with the package
  installed from the tag, every module Bifrost's generator loads imports.
- Every removed or trimmed file exists under `trashcan/` at its original
  path; nothing under `trashcan/` is imported by `yggdrasil/` or `tests/`.

---

## 16. Decisions

1. Scope: full consolidation as laid out here (dissolve `core_utils`, gather
   the CouchDB backend, client layer in `couchdb/`), implemented in the four
   cycles of section 12.
2. The orchestrator package is `yggdrasil.daemon`; `yggdrasil.core` keeps
   its meaning; freeing `core` is deferred.
3. `module_utils` and `mocks` move to `yggdrasil/toolkit/`, provisionally:
   draft 01 decides what stays importable there and what becomes a
   `StepContext` service.
4. No shims; demux_realm and tenx_realm update their imports in the same
   release.
5. The `ygg.handler` entry-point group is retired in this release.
6. `YggdrasilDBManager` (coordination database, internal storage) is
   trimmed to its constructor and lives in `storage/couchdb/`;
   `ProjectDBManager` (external `projects` database) is trimmed to its
   constructor and lives in `couchdb/` until the run-doc redesign removes
   it. Neither is inlined.
7. `plan_watcher.py` stays in `watchers/`; `consumer_service.py` becomes
   `daemon/ops_service.py`.
8. `seq_data_watcher.py` and `watchdog` stay; the filesystem backend is a
   later feature.
9. The `validate_*.py` scripts stay with the test realm.
10. The singleton decorator stays.
11. Every removal goes to the trashcan at its original path; trimmed files
    keep a full pre-change copy there.
12. Old test-realm plan documents are deleted and regenerated by the owner;
    no migration code.
13. Realm load-failure policy and realm selection are draft 12.
14. The four cycles merge into `dev` once, after cycle 4, from an
    integration branch; demux_realm's `ygg` extra stays on `@dev` (no
    interim pin) and moves to the tag together with its import update,
    immediately after the merge.
15. `OpsWriter` keeps its own endpoint resolution; only the duplicated
    constants are unified.
16. Logging setup leaves import time (`configure_logging()`); the
    lightness guard covers logging state as well as the CouchDB client.
17. Follow-ups identified here but not executed are recorded as tech-debt
    ledger entries during cycle 3 (section 14.1) and in the PRD working
    set's follow-ups list, not only in this document.

---

## 17. Open Questions

- Final home of the `toolkit/` contents once draft 01 defines `ctx.jobs`
  and `ctx.transfer`.
- Whether the mirrored test tree (`tests/daemon/`, `tests/storage/`, …) is
  worth doing in a later cycle.
- Whether `storage/sqlite.py` should become `storage/sqlite/` when draft 03
  adds the lease store.

---

## 18. Implementation Outcome

The work is on the branch `refactor/package-consolidation`: 20 commits on
`dev` (caacacc), not counting the one that adds this document. Cycle 1 is six
commits (five removals covering section 6, then the guide and `CLAUDE.md`),
cycle 2 is two (the relocation and the documentation paths that follow from
it), and cycle 3 is nine (the open items of section 7, the docstring
references to the new ledger entries, and the lightness guard). Three more
follow: package discovery (departure 2), the documentation of 10.4, and a
second lightness guard (departure 5).

| Measure | `dev` | After cycle 1 | After cycle 2 | After cycle 3 | Branch head |
| --- | --- | --- | --- | --- | --- |
| `pytest tests/` | 2722 passed | 2589 passed | 2579 passed | 2589 passed | 2592 passed |
| `ruff check .` findings | 33 | 32 | 31 | 31 | 31 |

The figures are for the committed tree, measured in a clean clone. A working
tree that also holds the two git-ignored tool tests
(`tests/test_dev_plan_approval_tool.py`,
`tests/test_internal_storage_smoke_tool.py`) runs 50 more, which matches the
2772 that the repository-cleanup PRD reports for the tree this work starts
from.

Cycle 1 removed 133 tests: the 69 of the three deleted test files, 59 trimmed
from `test_yggdrasil_db_manager.py` (27), `test_common.py` (18),
`test_project_db_manager.py` (8) and `test_ops_sinks_couch.py` (6), and five
`ygg.handler` cases (departure 3). Cycle 2 removed the ten of
`tests/test_init.py`. Cycle 3 added ten: four in the lightness guard, four in
`tests/watchers/test_package_exports.py`, and one each in
`test_couchdb_defaults.py` and `test_logging_utils.py`. The second guard added
three. No ruff rule's count rose, and `black --check .` and `mypy .` are clean
at the branch head.

A wheel built from the branch head holds 107 modules in 20 packages, all under
`yggdrasil/`, and no `lib/` entry. Installed into an empty virtual environment
and used from a directory outside the checkout: `import lib` fails with
`ModuleNotFoundError`; `yggdrasil.daemon.core`, `yggdrasil.cli` and the eight
modules Bifrost's generator loads import; importing `yggdrasil.flow` and
`yggdrasil.watchers` leaves `ibmcloudant` and `yggdrasil.logging_utils`
unimported and the `DEBUG` level name unchanged. The seven runtime
dependencies and `requirements/lock.txt` are unchanged. `yggdrasil --version`
reports a `0.4.0.postN` development version until the tag exists.

In the committed tree both greps of section 12 return nothing outside
`docs/design/`. Every file that section 5 marks as trashcan or trim is under
`trashcan/` at its original path, identical to its version on `dev`, and
nothing in `yggdrasil/` or `tests/` imports from there. With the import update
of 10.1 applied to a demux_realm checkout, its suite passes against the branch
head (134 passed), and realm discovery in dev mode returns `dmx_realm` and
`test_realm`.

As of 2026-10-10 the branch is pushed and not merged: `dev` is still at
caacacc. What remains is cycle 4 from step 2 on: the merge and a potential
tag; demux_realm's commit (10.1); tenx_realm (10.2), which is no longer
discovered because it still registers `tenx_project` under `ygg.handler`,
and which imports `yggdrasil.core_utils.event_types`, a path that only the
removed alias finder served.

Where the implementation departed from the plan:

1. **One branch instead of four pull requests.** The cycles are consecutive
   commit ranges on `refactor/package-consolidation`, not pull requests
   against an integration branch (principle 5, section 12). The single merge
   into `dev` that decision 14 asks for is the merge of that branch.
2. **Package discovery replaced the explicit package list (section 9).**
   `packages = ["yggdrasil"]` builds the same 107-module wheel, but only
   because the subpackages ride along as package data: setuptools warns for
   each of the 19 that it "would be ignored". `pyproject.toml` now uses
   `[tool.setuptools.packages.find]` with
   `include = ["yggdrasil", "yggdrasil.*"]`, which builds without the
   warning. It sets `namespaces = true` because `yggdrasil/flow/utils/` still
   has no `__init__.py`: section 2 promises every package a real one, and the
   packages that came from `lib/` have it, but `flow/` was left unchanged
   (section 4). The same commit excludes `build/` from mypy.
3. **The `ygg.handler` test trim differed from 5.8.**
   `tests/test_setup_realms.py` lost one case; eleven others were kept and
   only stopped patching the legacy entry-point lookup. Four
   `_derive_realm_id` cases were removed from `tests/test_yggdrasil_core.py`,
   which 5.8 does not list.
4. **Ledger entry B is wider than drafted (section 14.1).** The entries are
   21 (`OpsWriter`) and 22. `WatcherManager` also constructs a
   `CouchDBCheckpointStore` when none is injected and imports it at module
   level, so `PlanWatcher`'s default was not the only reason the `watchers`
   package imports the CouchDB storage backend. Entry 22 covers both classes
   and both docstrings name it. The ledger is still a local, untracked file.
5. **Tests beyond 5.8.** `tests/watchers/test_package_exports.py` checks
   that every name in the `watchers` package's `__all__` resolves, eagerly or
   on first access. The lightness guard gained a second class that imports
   `yggdrasil.logging_utils` itself and asserts that the level names, the
   eight third-party logger levels and the root logger are untouched; the
   guard of section 8 cannot see a regression there, because `logging_utils`
   is outside the realm-facing import graph.
6. **`docs/diagrams/ygg_architecture.mmd` was updated** with the
   documentation of 10.4, which does not list it.
