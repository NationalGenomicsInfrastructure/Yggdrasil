import asyncio
import importlib.metadata
import logging
import os
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from lib.core_utils.plan_eligibility import is_plan_eligible
from lib.core_utils.singleton_decorator import singleton
from lib.couchdb.plan_db_manager import PlanDBManager
from lib.couchdb.project_db_manager import ProjectDBManager
from lib.handlers.base_handler import BaseHandler
from lib.ops.consumer_service import OpsConsumerService

# from lib.handlers.flowcell_handler import FlowcellHandler
from lib.watchers.abstract_watcher import YggdrasilEvent
from lib.watchers.plan_watcher import PlanWatcher
from yggdrasil.core.engine import Engine

# NOTE: Import EventType via `yggdrasil.*` namespace (not `lib.*`), to match external handlers (enum identity issue)
from yggdrasil.core_utils.event_types import EventType  # type: ignore
from yggdrasil.flow.events.emitter import FileSpoolEmitter
from yggdrasil.flow.planner.api import PlanDraft, PlanningContext

# from lib.core_utils.event_types import EventType


def _generate_run_once_owner() -> str:
    """
    Generate unique execution owner token for run-once invocation.

    Format: run_once:<uuid4>

    This token uniquely identifies a CLI session, ensuring concurrent
    run-once invocations don't interfere with each other.
    """
    return f"run_once:{uuid.uuid4()}"


@singleton
class YggdrasilCore:
    """
    Central orchestrator that manages:
    - Multiple watchers (file system, CouchDB, etc.)
    - Event handlers (one or more handlers for specific event types)
    - (future) Semi-automatic (CLI) calls that bypass watchers
    """

    def __init__(self, config: Mapping[str, Any], logger: logging.Logger | None = None):
        """
        Args:
            config: A dictionary of global Yggdrasil settings.
            logger: If not provided, a default named logger is created.
        """
        self.config = config
        self._logger = logger or logging.getLogger("YggdrasilCore")
        self._running = False

        # Watchers: a list of classes that inherit from AbstractWatcher
        self.watchers: list = []

        # Handlers per event
        self.subscriptions: dict[EventType, list[BaseHandler]] = {}

        # Collection of realms
        self._realm_registry: dict[str, type] = {}  # realm_id -> handler class

        # Ops consumer service (for writing plan_status to CouchDB)
        self.ops_consumer = OpsConsumerService(interval_sec=2.0)

        # Engine for plan execution (handlers now return plans; core executes them)
        self.engine = Engine(
            work_root=config.get("work_root") or os.environ.get("YGG_WORK_ROOT"),
            emitter=FileSpoolEmitter(
                spool_dir=os.environ.get("YGG_EVENT_SPOOL", "/tmp/ygg_events")
            ),
        )

        self._init_db_managers()

        self._logger.info("YggdrasilCore initialized.")

    def _persist_plan_draft(
        self,
        draft: PlanDraft,
        handler_realm: str,
        *,
        execution_authority: str = "daemon",
        execution_owner: str | None = None,
    ) -> str:
        """
        Persist a PlanDraft to the yggdrasil_plans database.

        Creates or updates the plan document with status based on auto_run flag.
        On regeneration, execution tokens are reset (plan becomes eligible again).

        Args:
            draft: The PlanDraft returned by the handler
            handler_realm: The realm_id from the handler that generated this draft
            execution_authority: Who owns execution - "daemon" (default) or "run_once"
            execution_owner: Unique token for run_once isolation (e.g., "run_once:<uuid>")

        Returns:
            str: The database document ID of the persisted plan
        """
        plan_doc_id = self.plan_dbm.save_plan(
            plan=draft.plan,
            realm=handler_realm,
            scope=draft.plan.scope,
            auto_run=draft.auto_run,
            execution_authority=execution_authority,
            execution_owner=execution_owner,
            preview=draft.preview,
            notes=draft.notes,
        )

        self._logger.info(
            "Persisted plan '%s' to yggdrasil_plans (realm=%s, auto_run=%s, authority=%s)",
            plan_doc_id,
            handler_realm,
            draft.auto_run,
            execution_authority,
        )

        return plan_doc_id

    def _init_db_managers(self):
        """
        Initializes database managers or other central resources.

        Managers initialized:
        - pdm: ProjectDBManager for 'projects' database
        - ydm: YggdrasilDBManager for 'yggdrasil' database
        - plan_dbm: PlanDBManager for 'yggdrasil_plans' database

        All managers share the singleton CouchDBConnectionManager, so this
        is safe and efficient for both daemon and CLI modes.
        """
        self._logger.info("Initializing DB managers...")

        from lib.couchdb.yggdrasil_db_manager import YggdrasilDBManager

        self.pdm = ProjectDBManager()
        self.ydm = YggdrasilDBManager()
        self.plan_dbm = PlanDBManager()

        self._logger.info("DB managers initialized.")

    def _derive_realm_id(self, handler, ep=None) -> None:
        realm_id = getattr(handler, "realm_id", None)
        if not realm_id:
            # deterministic fallback, but still explicit:
            # prefer entry-point dist name; else top-level module
            realm_id = (
                (
                    getattr(getattr(ep, "dist", None), "name", None)
                    or handler.__module__.split(".")[0]
                )
                .replace("-", "_")
                .lower()
            )
            # surface it back to the handler for consistency
            setattr(handler, "realm_id", realm_id)

        # Enforce uniqueness across classes
        prev = self._realm_registry.get(realm_id)
        if prev and prev is not handler.__class__:
            raise RuntimeError(
                f"Duplicate realm_id '{realm_id}' claimed by "
                f"{prev.__module__}.{prev.__qualname__} and "
                f"{handler.__class__.__module__}.{handler.__class__.__qualname__}. "
                f"Set a unique `realm_id` on your handler."
            )
        self._realm_registry[realm_id] = handler.__class__

    def _make_planning_ctx(
        self, handler, scope: dict[str, Any], *, doc: dict[str, Any], reason: str
    ) -> PlanningContext:
        work_root = Path(os.getenv("YGG_WORK_ROOT", "/tmp/ygg_work"))
        scope_dir = work_root / handler.realm_id / scope["id"]
        emitter = FileSpoolEmitter(
            spool_dir=os.getenv("YGG_EVENT_SPOOL", "/tmp/ygg_events")
        )
        # ops_db = os.getenv("OPS_DB", "yggdrasil_ops")
        return PlanningContext(
            realm=handler.realm_id,
            scope=scope,
            scope_dir=scope_dir,
            emitter=emitter,
            source_doc=doc,
            reason=reason,
            # ops_db=ops_db,
        )

    def register_watcher(self, watcher) -> None:
        """
        Attach a watcher (e.g. CouchDBWatcher, PlanWatcher).
        The watchers will be started/stopped by YggdrasilCore.
        """
        self._logger.debug(f"Registering watcher: {watcher}")
        self.watchers.append(watcher)

    def register_handler(self, event_type: EventType, handler: BaseHandler) -> None:
        """
        Register a handler for a given event type.
        `subscriptions` is the mapping: EventType -> [handlers...]
        De-duplicates by handler class (module + qualname) per event.
        """

        subs = self.subscriptions.setdefault(event_type, [])
        handler_key = handler.class_key()  # (module, qualname)

        already_registered = any(handler.class_key() == handler_key for handler in subs)
        if already_registered:
            self._logger.warning(
                "Handler already registered for '%s': '%s'; skipping",
                event_type.name,
                handler.class_qualified_name(),
            )
            return

        subs.append(handler)
        self._logger.debug(
            "Registered handler '%s' for '%s'",
            handler.class_qualified_name(),
            event_type.name,
        )

    def _as_event_type(self, maybe_enum: Any) -> EventType | None:
        """Best-effort normalize unknown enums/strings to our EventType.

        Accepts EventType OR a look-alike enum with same .value OR a raw string.
        """
        try:
            if isinstance(maybe_enum, EventType):
                return maybe_enum
            if hasattr(maybe_enum, "value"):
                val = getattr(maybe_enum, "value")
                return next((e for e in EventType if e.value == val), None)
            if isinstance(maybe_enum, str):
                return next((e for e in EventType if e.value == maybe_enum), None)
        except Exception:
            return None
        return None

    def auto_register_external_handlers(self) -> None:
        """Discover external handlers via entry_points group 'ygg.handler'."""
        count: int = 0
        eps_list = list(importlib.metadata.entry_points(group="ygg.handler"))

        # Deduplicate entry points (workaround for importlib.metadata returning duplicates)
        seen = set()
        unique_eps = []
        for ep in eps_list:
            key = (ep.name, ep.value)
            if key not in seen:
                seen.add(key)
                unique_eps.append(ep)

        self._logger.debug(
            "Found %d entry point(s) in 'ygg.handler' group (%d unique)",
            len(eps_list),
            len(unique_eps),
        )

        for idx, ep in enumerate(unique_eps, 1):
            self._logger.debug(
                "Processing (unique) entry point %d/%d: '%s' (%s)",
                idx,
                len(unique_eps),
                ep.name,
                ep.value,
            )
            try:
                handler_cls = ep.load()
            except Exception as e:
                self._logger.exception("✘  '%s' load failed: %s", ep.name, e)
                continue

            event_type_raw = getattr(handler_cls, "event_type", None)
            event_type = self._as_event_type(event_type_raw)
            if not event_type:
                self._logger.error(
                    "✘  '%s' skipped: invalid event_type '%r'", ep.name, event_type_raw
                )
                continue

            try:
                handler = handler_cls()  # type: ignore[call-arg]
                # derive & enforce realm_id uniqueness
                self._derive_realm_id(handler, ep)
                self.register_handler(event_type, handler)
                self._logger.info(
                    "✓  registered external handler '%s' for event type '%s'",
                    ep.name,
                    event_type.name,
                )
                count += 1
            except Exception as e:
                self._logger.exception("✘  '%s' instantiation failed: %s", ep.name, e)

        if count == 0:
            self._logger.warning(
                "No external handlers discovered in group 'ygg.handler'."
            )
        else:
            self._logger.info("Total external handlers registered: %d", count)

    def setup_handlers(self) -> None:
        """
        Instantiate and register all event handlers.
        """
        self._logger.info("Setting up event handlers...")
        # 1. Auto-register external handlers from entry points
        self.auto_register_external_handlers()

        # 2. Register built-in handlers
        # from lib.handlers.bp_analysis_handler import BestPracticeAnalysisHandler

        # Best‑practice analysis for new/changed ProjectDB docs
        # project_handler = BestPracticeAnalysisHandler()
        # self.register_handler(EventType.PROJECT_CHANGE, project_handler)
        # # Demultiplexing / downstream pipeline for newly-ready flowcells
        # flowcell_handler = FlowcellHandler()
        # self.register_handler(EventType.FLOWCELL_READY, flowcell_handler)
        # NOTE: When we have a CLI‑triggered event type, e.g. 'manual_run', register it here too
        # cli_handler = CLIHandler()
        # self.register_handler(EventType.<whatever>, cli_handler)

        # 3. Register test realm handler (dev mode only)
        self._register_test_realm_handler()

        # Pretty summary: EVENT_TYPE(count, ...)
        if not getattr(self, "subscriptions", None):
            self._logger.warning("No handler subscriptions found.")
            return

        summary = ", ".join(
            f"{event_type.name}({len(self.subscriptions.get(event_type, []))})"
            for event_type in self.subscriptions.keys()
        )
        self._logger.debug("Handler Registrations: %s", summary)

    def _register_test_realm_handler(self) -> None:
        """
        Register the test realm handler if running in dev mode.

        The test realm provides controllable test scenarios for validating
        the execution pipeline. Only available with --dev flag.
        """
        from lib.realms.test_realm import get_handler, is_test_realm_enabled

        if not is_test_realm_enabled():
            self._logger.debug("Test realm disabled (not in dev mode)")
            return

        handler = get_handler()
        if handler is None:
            self._logger.warning("Test realm enabled but handler creation failed")
            return

        self._derive_realm_id(handler)
        self.register_handler(EventType.TEST_SCENARIO_CHANGE, handler)
        self._logger.info(
            "✓  registered internal handler 'test_realm' for event type '%s' (dev mode)",
            EventType.TEST_SCENARIO_CHANGE.name,
        )

    def setup_watchers(self):
        """
        Calls specialized methods to set up watchers of different types
        without cluttering the main method.
        """
        self._logger.info("Setting up watchers...")
        self._setup_plan_watcher()
        # self._setup_cdb_watchers()
        self._setup_test_realm_watcher()
        # NOTE: FS/CouchDB watchers will be set up via WatchSpecs in Phase 2.
        self._logger.info("Watchers setup done.")

    def _setup_test_realm_watcher(self) -> None:
        """
        Set up the test realm watcher if running in dev mode.

        The test realm watcher monitors the yggdrasil database for
        documents with type="ygg_test_scenario".
        """
        from lib.realms.test_realm import get_watcher, is_test_realm_enabled

        if not is_test_realm_enabled():
            self._logger.debug("Test realm watcher disabled (not in dev mode)")
            return

        watcher = get_watcher(on_event=self.handle_event, logger=self._logger)
        if watcher is None:
            self._logger.warning("Test realm enabled but watcher creation failed")
            return

        self.register_watcher(watcher)
        self._logger.info("✓  registered ScenarioDocWatcher for test realm (dev mode)")

    def _setup_cdb_watchers(self):
        """
        Builds CouchDB watchers if config["couchdb"] is present.
        """

        from lib.watchers.couchdb_watcher import CouchDBWatcher

        self._logger.info("Setting up CouchDB watchers...")

        poll_interval = self.config.get("couchdb", {}).get("poll_interval", 5)

        # Project DB
        cdb_pdm_watcher = CouchDBWatcher(
            on_event=self.handle_event,
            event_type=EventType.PROJECT_CHANGE,
            name="ProjectDBWatcher",
            changes_fetcher=self.pdm.fetch_changes,
            poll_interval=poll_interval,
            logger=self._logger,
        )
        self.register_watcher(cdb_pdm_watcher)
        self._logger.debug("Registered CouchDBWatcher for ProjectDB.")

        # TODO
        # Yggdrasil DB
        # cdb_ydm_watcher = CouchDBWatcher(
        #     on_event=self.handle_event,
        #     name="YggdrasilDBWatcher",
        #     changes_fetcher=self.ydm.fetch_changes,
        #     poll_interval=poll_interval,
        #     logger=self._logger
        # )
        # self.register_watcher(cdb_ydm_watcher)
        # self._logger.debug("Registered CouchDBWatcher for YggdrasilDB.")

    def _setup_plan_watcher(self) -> None:
        """
        Set up the PlanWatcher for daemon mode.

        The daemon's PlanWatcher is configured to only process plans with
        execution_authority='daemon', skipping 'run_once' plans which are
        managed by their respective CLI sessions.

        The watcher emits PLAN_EXECUTION_EVENT for eligible plans, which
        triggers execute_approved_plan() via handle_plan_execution_event().
        """
        poll_interval = self.config.get("plan_watcher_poll_interval", 5.0)

        plan_watcher = PlanWatcher(
            on_event=self._handle_plan_execution_event,
            poll_interval_sec=poll_interval,
            execution_authority_filter="daemon",  # Skip run_once plans
            logger=self._logger,
        )
        self.register_watcher(plan_watcher)
        self._plan_watcher = plan_watcher  # Keep reference for recovery
        self._logger.info(
            "Registered PlanWatcher for daemon mode (poll_interval=%.1fs, authority_filter='daemon')",
            poll_interval,
        )

    def _handle_plan_execution_event(self, event: YggdrasilEvent) -> None:
        """
        Handle EventType.PLAN_EXECUTION from PlanWatcher.

        This is the callback invoked when PlanWatcher detects an eligible plan.
        It schedules execution via execute_approved_plan().

        Args:
            event: YggdrasilEvent with payload containing plan_doc_id and plan_doc
        """
        if event.event_type != EventType.PLAN_EXECUTION:
            self._logger.warning(
                "Unexpected event type in plan execution handler: %s", event.event_type
            )
            return

        payload = event.payload or {}
        plan_doc_id = payload.get("plan_doc_id")
        plan_doc = payload.get("plan_doc")

        if not plan_doc_id:
            self._logger.error("Plan execution event missing 'plan_doc_id'")
            return

        self._logger.info(
            "Received plan execution event for '%s' from '%s'",
            plan_doc_id,
            event.source,
        )

        # Schedule execution (non-blocking)
        asyncio.create_task(self._execute_approved_plan(plan_doc_id, plan_doc))

    async def _execute_approved_plan(
        self,
        plan_doc_id: str,
        plan_doc: dict[str, Any] | None = None,
    ) -> None:
        """
        Execute an approved plan via Engine and update executed_run_token.

        This is the core execution logic triggered by PlanWatcher events.
        On success, updates executed_run_token to prevent re-execution.
        On failure, leaves token unchanged so plan remains eligible for retry.

        Args:
            plan_doc_id: The plan document ID in yggdrasil_plans
            plan_doc: Optional plan document (if already fetched by watcher)
        """
        try:
            # Fetch plan document if not provided
            if plan_doc is None:
                plan_doc = self.plan_dbm.fetch_plan(plan_doc_id)
                if not plan_doc:
                    self._logger.error(
                        "Plan document '%s' not found; cannot execute", plan_doc_id
                    )
                    return

            # Re-verify eligibility (race protection)
            if not is_plan_eligible(plan_doc):
                self._logger.info(
                    "Plan '%s' no longer eligible; skipping execution", plan_doc_id
                )
                return

            # Get the Plan model for execution
            plan = self.plan_dbm.fetch_plan_as_model(plan_doc_id)
            if not plan:
                self._logger.error(
                    "Failed to deserialize plan '%s'; cannot execute", plan_doc_id
                )
                return

            run_token = plan_doc.get("run_token", 0)
            realm = plan_doc.get("realm", "unknown")

            self._logger.info(
                "Executing plan '%s' (realm=%s, run_token=%d)",
                plan_doc_id,
                realm,
                run_token,
            )

            # Execute via Engine in a thread pool to avoid blocking the event loop.
            # This keeps watchers responsive during long-running plan executions.
            await asyncio.to_thread(self.engine.run, plan)

            self._logger.info(
                "✓ Plan '%s' execution completed successfully", plan_doc_id
            )

            # Update executed_run_token (marks as executed, prevents re-run)
            success = self.plan_dbm.update_executed_token(plan_doc_id, run_token)
            if success:
                self._logger.info(
                    "Updated executed_run_token=%d for plan '%s'",
                    run_token,
                    plan_doc_id,
                )
            else:
                self._logger.warning(
                    "Failed to update executed_run_token for plan '%s'; "
                    "plan may be re-executed on restart",
                    plan_doc_id,
                )

        except Exception as exc:
            self._logger.exception("Failed to execute plan '%s': %s", plan_doc_id, exc)
            # Token NOT updated → plan remains eligible for retry

    async def _recover_approved_plans(self) -> None:
        """
        Startup recovery: execute any approved plans that were missed.

        This is called when the PlanWatcher checkpoint is missing or invalid.
        It queries all eligible plans and executes them.

        The recovery process:
        1. Query all approved pending plans
        2. Execute each via _execute_approved_plan()
        3. Initialize checkpoint after recovery

        Note: This is a fallback mechanism. Normal operation uses the
        _changes feed via PlanWatcher for incremental updates.
        """
        if not hasattr(self, "_plan_watcher") or self._plan_watcher is None:
            self._logger.warning("No PlanWatcher configured; skipping recovery")
            return

        self._logger.info("Starting approved plan recovery...")

        try:
            # Use PlanWatcher's recovery method (which queries + emits)
            eligible_plans = await self._plan_watcher.recover_pending_plans()

            self._logger.info(
                "Recovery complete: %d plans queued for execution",
                len(eligible_plans),
            )

        except Exception as exc:
            self._logger.exception("Error during plan recovery: %s", exc)

    async def start(self) -> None:
        """
        Start all watchers in parallel. Typically called once at system startup.
        This will run indefinitely until watchers exit or self.stop() is called.
        """
        if self._running:
            self._logger.warning("YggdrasilCore is already running.")
            return

        self._running = True

        self._logger.info("Starting operations consumer service...")
        # Start the ops consumer service
        self.ops_consumer.start()

        self._logger.info("Starting all watchers...")

        # Start watchers as async tasks
        tasks = [asyncio.create_task(w.start()) for w in self.watchers]
        self._logger.info(f"Running {len(tasks)} watchers in parallel.")

        # Wait until all watchers exit (or are stopped)
        await asyncio.gather(*tasks, return_exceptions=True)
        self._logger.info("All watchers have exited or been stopped.")

    async def stop(self) -> None:
        """
        Stop all watchers gracefully. This sets _running=False, so watchers that
        poll or wait in loops will naturally exit. Then we wait for them to finish.
        """
        if not self._running:
            self._logger.debug("YggdrasilCore stop called, but not running.")
            return

        self._logger.info("Stopping all watchers...")
        self._running = False

        # Each watcher has its own stop() method
        stop_tasks = [asyncio.create_task(w.stop()) for w in self.watchers]
        await asyncio.gather(*stop_tasks)
        self._logger.info("All watchers stopped.")

        # Stop the ops consumer service
        try:
            await self.ops_consumer.stop()
            self._logger.info("Ops consumer service stopped.")
        except asyncio.CancelledError:
            # Task was cancelled during shutdown (expected)
            self._logger.debug("Ops consumer task cancelled (expected during shutdown)")

    # def run_once(self, doc_id: str):
    #     """
    #     Fetch the project doc, build the payload, and synchronously
    #     drive the BestPracticeAnalysisHandler without starting watchers.
    #     """
    #     from lib.core_utils.module_resolver import get_module_location
    #     from lib.couchdb.project_db_manager import ProjectDBManager

    #     pdm = ProjectDBManager()
    #     doc = pdm.fetch_document_by_id(doc_id)
    #     if not doc:
    #         self._logger.error(f"No project with ID {doc_id}")
    #         return

    #     module_loc = get_module_location(doc)
    #     if not module_loc:
    #         self._logger.error(f"No module for project {doc_id}")
    #         return

    #     payload = {"document": doc, "module_location": module_loc}

    #     # Use the appropriate registered earlier
    #     handler = self.handlers.get(EventType.PROJECT_CHANGE)
    #     if not handler:
    #         self._logger.error(
    #             "No handler for '%s' event type", EventType.PROJECT_CHANGE
    #         )
    #         return

    #     if not hasattr(handler, "run_now"):
    #         raise RuntimeError(
    #             f"Handler {handler!r} must implement `.run_now(payload)` for one-off mode"
    #         )
    #     handler.run_now(payload)

    #     # 2) After the step(s) emitted events, do a single consume pass
    #     # TODO: Put the imports at the top when this is stable
    #     import os
    #     from pathlib import Path

    #     from lib.ops.consumer import FileSpoolConsumer
    #     from lib.ops.sinks.couch import OpsWriter

    #     spool = Path(os.environ.get("YGG_EVENT_SPOOL", "/tmp/ygg_events"))
    #     FileSpoolConsumer(
    #         spool, OpsWriter(db_name=os.environ.get("OPS_DB", "yggdrasil_ops"))
    #     ).consume()

    def run_once(self, doc_id: str):
        """
        Fetch one project doc and synchronously invoke the associated
        PROJECT_CHANGE handlers.
        Then do a single pass over the event spool to flush to ops.
        """
        import os
        from pathlib import Path

        from lib.ops.consumer import FileSpoolConsumer
        from lib.ops.sinks.couch import OpsWriter

        self._logger.info("run_once: fetching project %s", doc_id)
        doc = self.pdm.fetch_document_by_id(doc_id)
        if not doc:
            self._logger.error("No project with ID %s", doc_id)
            return

        handlers = self.subscriptions.get(EventType.PROJECT_CHANGE) or []
        if not handlers:
            self._logger.error(
                "No handlers registered for %s", EventType.PROJECT_CHANGE.name
            )
            return

        # Build the minimal, consistent payload the handlers expect
        reason: str = f"run_once:{doc.get('project_id') or doc_id}"
        payload: dict[str, Any] = {
            "doc": doc,
            "reason": reason,
        }

        self._logger.info(
            "Invoking %d 'PROJECT_CHANGE' handler(s) (run_now)", len(handlers)
        )
        for handler in handlers:
            try:
                # Must have a scope: prefer realm-provided derive_scope
                if hasattr(handler, "derive_scope") and callable(handler.derive_scope):
                    scope = handler.derive_scope(doc)  # {'kind':..., 'id':...}
                else:
                    self._logger.error(
                        "Handler %s lacks derive_scope; refusing to assume 'project'. Skipping.",
                        handler.class_qualified_name(),
                    )
                    continue

                ctx = self._make_planning_ctx(handler, scope, doc=doc, reason=reason)
                payload["planning_ctx"] = ctx

                # NEW: Use new handler interface
                self._logger.info(
                    "Generating plan draft via %s", handler.class_qualified_name()
                )
                draft = handler.run_now(payload)  # returns PlanDraft

                # Persist the plan
                realm_id = getattr(handler, "realm_id", "unknown_realm")
                plan_doc_id = self._persist_plan_draft(draft, realm_id)
                self._logger.info("Persisted plan '%s' to database", plan_doc_id)

                # Execute via Engine (CLI mode skips approval checks)
                self._logger.info("Executing plan '%s' via Engine", plan_doc_id)
                self.engine.run(draft.plan)
                self._logger.info("✓ Plan '%s' execution completed", plan_doc_id)

            except Exception as e:
                self._logger.exception(
                    "Handler %s raised during run_once: %s",
                    handler.class_qualified_name(),
                    e,
                )

        # Single consume pass: spool must match the emitter used in the handler
        spool_root = Path(os.environ.get("YGG_EVENT_SPOOL", "/tmp/ygg_events"))
        self._logger.info("Consuming event spool once at %s", spool_root)
        FileSpoolConsumer(
            spool_root,
            OpsWriter(db_name=os.environ.get("OPS_DB", "yggdrasil_ops")),
        ).consume()

        self._logger.info("run_once: done.")

    # --------------------------------------------------------------------------
    # CLI Mode Methods (--plan-only, --run-once)
    # --------------------------------------------------------------------------

    def _check_plan_overwrite(
        self,
        plan_doc_id: str,
        force: bool,
    ) -> tuple[str, bool]:
        """
        Check if plan already exists and handle overwrite logic.

        Uses the actual plan_doc_id (from draft.plan.plan_id) to ensure
        the check matches the exact document that will be persisted.

        Args:
            plan_doc_id: The exact plan document ID to check
            force: Whether to force overwrite

        Returns:
            tuple: (plan_doc_id, should_continue)
                - should_continue=False means caller should abort
        """
        summary = self.plan_dbm.get_plan_summary(plan_doc_id)
        if summary is None:
            # No existing plan, proceed
            return plan_doc_id, True

        # Plan exists - display warning
        self._logger.warning(
            "\n╭─ Existing Plan Found ─────────────────────────────────────╮\n"
            "│ Plan ID:    %s\n"
            "│ Status:     %s\n"
            "│ Origin:     %s\n"
            "│ Updated:    %s\n"
            "│ Run Token:  %d (executed: %d)\n"
            "╰───────────────────────────────────────────────────────────╯",
            plan_doc_id,
            summary["status"],
            summary["execution_authority"],
            summary["updated_at"],
            summary["run_token"],
            summary["executed_run_token"],
        )

        if force:
            self._logger.info("--force specified; overwriting existing plan.")
            return plan_doc_id, True

        # Not forced - abort
        self._logger.error(
            "Plan '%s' already exists. Use --force to overwrite.",
            plan_doc_id,
        )
        return plan_doc_id, False

    def create_plan_from_doc(
        self,
        doc_id: str,
        *,
        force_overwrite: bool = False,
    ) -> str | None:
        """
        Create and persist a plan from a project document (no execution).

        This is the --plan-only mode: creates a plan with execution_authority='daemon'
        for later approval via Genstat and execution by the daemon.

        Args:
            doc_id: Project document ID
            force_overwrite: If True, overwrite existing plan without prompting

        Returns:
            str: Plan document ID if successful, None otherwise
        """
        self._logger.info("create_plan_from_doc: fetching project %s", doc_id)
        doc = self.pdm.fetch_document_by_id(doc_id)
        if not doc:
            self._logger.error("No project with ID %s", doc_id)
            return None

        handlers = self.subscriptions.get(EventType.PROJECT_CHANGE) or []
        if not handlers:
            self._logger.error(
                "No handlers registered for %s", EventType.PROJECT_CHANGE.name
            )
            return None

        plan_doc_id: str | None = None

        # Process with first matching handler (typical case: one handler per event)
        for handler in handlers:
            try:
                # Derive scope
                if not (
                    hasattr(handler, "derive_scope") and callable(handler.derive_scope)
                ):
                    self._logger.error(
                        "Handler %s lacks derive_scope; skipping.",
                        handler.class_qualified_name(),
                    )
                    continue

                scope = handler.derive_scope(doc)
                realm_id = getattr(handler, "realm_id", "unknown_realm")

                # Build planning context
                reason = f"plan-only:{doc.get('project_id') or doc_id}"
                ctx = self._make_planning_ctx(handler, scope, doc=doc, reason=reason)
                payload = {
                    "doc": doc,
                    "reason": reason,
                    "planning_ctx": ctx,
                }

                # Generate plan draft FIRST (to get actual plan_doc_id)
                self._logger.info(
                    "Generating plan draft via %s", handler.class_qualified_name()
                )
                draft = handler.run_now(payload)

                # Get the actual plan_doc_id from the draft (single source of truth)
                actual_plan_doc_id = draft.plan.plan_id

                # Check for existing plan using the ACTUAL plan_doc_id
                _, should_continue = self._check_plan_overwrite(
                    actual_plan_doc_id, force_overwrite
                )
                if not should_continue:
                    return None

                # Force draft status for plan-only mode
                draft.auto_run = False

                # Persist with daemon origin (for later daemon execution)
                plan_doc_id = self._persist_plan_draft(
                    draft,
                    realm_id,
                    execution_authority="daemon",
                    execution_owner=None,
                )
                self._logger.info(
                    "✓ Plan '%s' created (status=draft, authority=daemon). "
                    "Awaiting approval via Genstat.",
                    plan_doc_id,
                )
                break  # Only process first handler

            except Exception as e:
                self._logger.exception(
                    "Handler %s raised during create_plan_from_doc: %s",
                    handler.class_qualified_name(),
                    e,
                )

        return plan_doc_id

    def run_once_with_watcher(
        self,
        doc_id: str,
        *,
        force_overwrite: bool = False,
        timeout_seconds: int = 1800,
    ) -> int:
        """
        Create and execute plan(s) via scoped PlanWatcher (--run-once mode).

        IMPORTANT: This method uses a single execution route via PlanWatcher
        regardless of auto_run status. The watcher is the sole path to execution.

        Creates plans for ALL matching handlers (not just the first).
        Plans are executed when they become eligible as observed from the DB.

        Args:
            doc_id: Project document ID
            force_overwrite: If True, overwrite existing plans without prompting
            timeout_seconds: Maximum seconds to wait for approval (default 1800)

        Returns:
            int: Exit code (0=success, 1=error, 130=interrupted)
        """
        from lib.ops.consumer import FileSpoolConsumer
        from lib.ops.sinks.couch import OpsWriter

        # Generate unique owner token for this entire session
        execution_owner = _generate_run_once_owner()
        self._logger.info(
            "run_once_with_watcher: session owner=%s, timeout=%ds",
            execution_owner,
            timeout_seconds,
        )

        # ─────────────────────────────────────────────────────────────────
        # Phase 1: Fetch document and create plans for ALL matching handlers
        # ─────────────────────────────────────────────────────────────────
        doc = self.pdm.fetch_document_by_id(doc_id)
        if not doc:
            self._logger.error("No project with ID %s", doc_id)
            return 1

        handlers = self.subscriptions.get(EventType.PROJECT_CHANGE) or []
        if not handlers:
            self._logger.error(
                "No handlers registered for %s", EventType.PROJECT_CHANGE.name
            )
            return 1

        # Create plans for ALL handlers (not just first)
        pending_plan_ids: list[str] = []

        for handler in handlers:
            plan_doc_id = self._create_run_once_plan_for_handler(
                handler=handler,
                doc=doc,
                doc_id=doc_id,
                execution_owner=execution_owner,
                force_overwrite=force_overwrite,
            )
            if plan_doc_id:
                pending_plan_ids.append(plan_doc_id)
            # Continue to next handler even if one fails

        if not pending_plan_ids:
            self._logger.error("Failed to create any plans for doc_id=%s", doc_id)
            return 1

        self._logger.info(
            "Created %d plan(s): %s",
            len(pending_plan_ids),
            ", ".join(pending_plan_ids),
        )

        # ─────────────────────────────────────────────────────────────────
        # Phase 2: Start scoped watcher and wait for all plans to complete
        # ─────────────────────────────────────────────────────────────────
        exit_code = asyncio.run(
            self._run_once_watcher_loop(
                pending_plan_ids=pending_plan_ids,
                execution_owner=execution_owner,
                timeout_seconds=timeout_seconds,
            )
        )

        # ─────────────────────────────────────────────────────────────────
        # Phase 3: Consume event spool
        # ─────────────────────────────────────────────────────────────────
        spool_root = Path(os.environ.get("YGG_EVENT_SPOOL", "/tmp/ygg_events"))
        self._logger.info("Consuming event spool at %s", spool_root)
        FileSpoolConsumer(
            spool_root,
            OpsWriter(db_name=os.environ.get("OPS_DB", "yggdrasil_ops")),
        ).consume()

        return exit_code

    def _create_run_once_plan_for_handler(
        self,
        handler: BaseHandler,
        doc: dict[str, Any],
        doc_id: str,
        execution_owner: str,
        force_overwrite: bool,
    ) -> str | None:
        """
        Create a single plan for one handler in run-once mode.

        IMPORTANT: Overwrite check uses the actual plan_doc_id from the draft,
        NOT a scope-derived ID, to avoid divergence from persisted _id.

        Args:
            handler: The handler to generate the plan
            doc: Source document
            doc_id: Document ID (fallback for reason string)
            execution_owner: Unique session token
            force_overwrite: Whether to overwrite existing plans

        Returns:
            str: Plan document ID if successful, None otherwise
        """
        try:
            if not (
                hasattr(handler, "derive_scope") and callable(handler.derive_scope)
            ):
                self._logger.error(
                    "Handler %s lacks derive_scope; skipping.",
                    handler.class_qualified_name(),
                )
                return None

            scope = handler.derive_scope(doc)
            realm_id = getattr(handler, "realm_id", "unknown_realm")

            # Build planning context
            reason = f"run-once:{doc.get('project_id') or doc_id}"
            ctx = self._make_planning_ctx(handler, scope, doc=doc, reason=reason)
            payload = {
                "doc": doc,
                "reason": reason,
                "planning_ctx": ctx,
            }

            # Generate plan draft FIRST (to get actual plan_doc_id)
            self._logger.info(
                "Generating plan draft via %s", handler.class_qualified_name()
            )
            draft = handler.run_now(payload)

            # Get the actual plan_doc_id from the draft (single source of truth)
            plan_doc_id = draft.plan.plan_id

            # Check for existing plan using the ACTUAL plan_doc_id
            _, should_continue = self._check_plan_overwrite(
                plan_doc_id, force_overwrite
            )
            if not should_continue:
                return None

            # Persist with run_once origin and our owner token
            # NOTE: auto_run determines status (approved/draft), but we do NOT
            # branch on it for execution - watcher handles all execution
            persisted_id = self._persist_plan_draft(
                draft,
                realm_id,
                execution_authority="run_once",
                execution_owner=execution_owner,
            )
            self._logger.info(
                "Plan '%s' created (realm=%s, status=%s, owner=%s)",
                persisted_id,
                realm_id,
                "approved" if draft.auto_run else "draft",
                execution_owner,
            )
            return persisted_id

        except Exception as e:
            self._logger.exception(
                "Handler %s raised during plan creation: %s",
                handler.class_qualified_name(),
                e,
            )
            return None

    async def _run_once_watcher_loop(
        self,
        pending_plan_ids: list[str],
        execution_owner: str,
        timeout_seconds: int,
    ) -> int:
        """
        Run scoped watcher loop until all plans complete, timeout, or interrupt.

        IMPORTANT: This is the SOLE execution path for run-once mode.
        Whether auto_run=True or False, plans are executed here when eligible.

        Completion is tracked in session-local memory only. The following
        behaviors are explicitly prohibited:
        - Writing completion markers back to the plan document
        - Modifying executed_run_token for plans not executed by this process
        - Updating plan status during ownership transfer detection

        Args:
            pending_plan_ids: List of plan doc IDs to track
            execution_owner: Our unique session token
            timeout_seconds: Max wait time

        Returns:
            int: Exit code (0=all completed, 1=error/timeout, 130=interrupted)
        """
        import signal

        # Track completion: plan_doc_id -> completed (True/False)
        # This is SESSION-LOCAL state only; never persisted to DB
        completion_status: dict[str, bool] = {pid: False for pid in pending_plan_ids}
        completion_event = asyncio.Event()
        interrupted = False
        error_occurred = False

        def on_plan_eligible(event: YggdrasilEvent) -> None:
            """
            Callback when watcher detects an eligible plan.

            Validates ownership, executes if still ours, updates local completion.
            """
            nonlocal error_occurred

            payload = event.payload or {}
            plan_doc_id = payload.get("plan_doc_id")
            plan_doc = payload.get("plan_doc")

            # IMPORTANT: Ignore plans not created in THIS session.
            # This prevents "owner collision" side effects (unlikely with UUID, but safe).
            if plan_doc_id not in completion_status:
                self._logger.debug(
                    "Ignoring event for plan not in this session: %s", plan_doc_id
                )
                return

            if completion_status[plan_doc_id]:
                # Already completed (locally)
                return

            # Fetch fresh doc to verify ownership (source of truth)
            if plan_doc is None:
                plan_doc = self.plan_dbm.fetch_plan(plan_doc_id)

            if not plan_doc:
                self._logger.error("Plan '%s' not found in DB", plan_doc_id)
                error_occurred = True
                completion_status[plan_doc_id] = True
                _check_all_completed()
                return

            # Check if ownership was transferred (Genstat changed execution_authority)
            if plan_doc.get("execution_authority") != "run_once":
                self._logger.info(
                    "Plan '%s' transferred to daemon; marking completed (no action)",
                    plan_doc_id,
                )
                # Do NOT write anything to DB; just mark locally completed
                completion_status[plan_doc_id] = True
                _check_all_completed()
                return

            if plan_doc.get("execution_owner") != execution_owner:
                self._logger.warning(
                    "Plan '%s' owner changed to '%s'; marking completed (no action)",
                    plan_doc_id,
                    plan_doc.get("execution_owner"),
                )
                completion_status[plan_doc_id] = True
                _check_all_completed()
                return

            # Re-verify eligibility from DB (source of truth)
            if not is_plan_eligible(plan_doc):
                self._logger.debug(
                    "Plan '%s' no longer eligible; will retry on next poll",
                    plan_doc_id,
                )
                return

            # Execute!
            self._logger.info("Executing plan '%s' via Engine...", plan_doc_id)
            try:
                plan = self.plan_dbm.fetch_plan_as_model(plan_doc_id)
                if not plan:
                    self._logger.error("Failed to deserialize plan '%s'", plan_doc_id)
                    error_occurred = True
                    completion_status[plan_doc_id] = True
                    _check_all_completed()
                    return

                run_token = plan_doc.get("run_token", 0)
                self.engine.run(plan)
                self._logger.info("✓ Plan '%s' execution completed", plan_doc_id)

                # Update executed token (we DID execute this plan)
                self.plan_dbm.update_executed_token(plan_doc_id, run_token)
                completion_status[plan_doc_id] = True
                _check_all_completed()

            except Exception as exc:
                self._logger.exception(
                    "Plan '%s' execution failed: %s", plan_doc_id, exc
                )
                error_occurred = True
                completion_status[plan_doc_id] = True
                _check_all_completed()

        def _check_all_completed() -> None:
            """Signal completion_event if all plans are done."""
            if all(completion_status.values()):
                completion_event.set()

        # Create scoped watcher (filters by execution_owner only)
        scoped_watcher = PlanWatcher(
            on_event=on_plan_eligible,
            poll_interval_sec=2.0,  # Responsive for interactive use
            execution_owner_filter=execution_owner,
            # NOTE: No execution_authority_filter; we check origin in callback
            logger=self._logger,
        )

        # Handle Ctrl+C
        def handle_interrupt(signum: int, frame: Any) -> None:
            nonlocal interrupted
            interrupted = True
            completion_event.set()

        original_handler = signal.signal(signal.SIGINT, handle_interrupt)

        try:
            # Start watcher task
            watcher_task = asyncio.create_task(scoped_watcher.start())

            # Wait with timeout
            try:
                await asyncio.wait_for(
                    completion_event.wait(),
                    timeout=float(timeout_seconds),
                )
            except TimeoutError:
                pending = [pid for pid, done in completion_status.items() if not done]
                self._logger.error(
                    "Timeout after %ds waiting for plans: %s\n"
                    "Plans left in DB for manual handling.",
                    timeout_seconds,
                    ", ".join(pending),
                )
                await scoped_watcher.stop()
                watcher_task.cancel()
                try:
                    await watcher_task
                except asyncio.CancelledError:
                    pass
                return 1

            # Stop watcher
            await scoped_watcher.stop()
            watcher_task.cancel()
            try:
                await watcher_task
            except asyncio.CancelledError:
                pass

            if interrupted:
                pending = [pid for pid, done in completion_status.items() if not done]
                self._logger.info(
                    "\nInterrupted. Plan(s) left in current state: %s",
                    ", ".join(pending) if pending else "(all completed)",
                )
                return 130  # Standard SIGINT exit code

            return 1 if error_occurred else 0

        finally:
            signal.signal(signal.SIGINT, original_handler)

    def handle_event(self, event: YggdrasilEvent) -> None:
        """
        Watchers call this to deliver events.

        New Flow:
        1. Handler generates a PlanDraft (doesn't execute)
        2. Core persists the draft to database
        3. Core checks approval status (if approvals_required)
        4. Core executes plan via Engine (if approved/auto_run)

        NOTE! Broadcasts to ALL subscribers for a particular EventType.
        """
        self._logger.info(
            "Received event '%s' from '%s'", event.event_type, event.source
        )
        handlers = self.subscriptions.get(event.event_type) or []

        if not handlers:
            self._logger.warning(
                "No subscribers registered for event_type '%s'", event.event_type
            )
            return

        for handler in handlers:
            try:
                payload = dict(event.payload) if isinstance(event.payload, dict) else {}
                scope = payload.get("scope")
                doc = payload.get("doc", {})

                # If scope not provided by watcher, let realm derive it from doc
                if scope is None:
                    if (
                        doc
                        and hasattr(handler, "derive_scope")
                        and callable(handler.derive_scope)
                    ):
                        scope = handler.derive_scope(doc)
                    else:
                        self._logger.error(
                            "No 'scope' in payload and handler %s has no derive_scope; skipping.",
                            handler.class_qualified_name(),
                        )
                        continue

                reason = (
                    payload.get("reason") or f"{event.event_type} from {event.source}"
                )
                payload["planning_ctx"] = self._make_planning_ctx(
                    handler, scope, doc=doc, reason=reason
                )

                # Schedule async plan generation (PlanWatcher handles execution)
                self._logger.debug(
                    "Scheduling plan generation for %s via %s",
                    event.event_type,
                    handler.class_qualified_name(),
                )
                asyncio.create_task(self._generate_and_persist_plan(handler, payload))

            except Exception as exc:
                self._logger.error(
                    f"Error handling '{event.event_type}' with handler '{handler.class_qualified_name()}': {exc}",
                    exc_info=True,
                )

    async def _generate_and_persist_plan(
        self, handler: BaseHandler, payload: dict[str, Any]
    ) -> None:
        """
        Generate and persist a plan from a handler (daemon mode).

        In daemon mode, this method ONLY generates and persists the plan.
        Execution is handled exclusively by PlanWatcher, which detects
        eligible plans (approved + run_token > executed_run_token) and
        triggers execution via _execute_approved_plan().

        This separation ensures:
        - Single execution path (no double-execution bugs)
        - Consistent execution tracking via executed_run_token
        - Proper support for approval workflows

        Args:
            handler: The handler that will generate the plan
            payload: Event payload with planning_ctx
        """
        try:
            # Step 1: Generate plan draft
            self._logger.info(
                "Generating plan draft via %s", handler.class_qualified_name()
            )
            draft = await handler.generate_plan_draft(payload)

            # Step 2: Persist plan to database
            # PlanWatcher will detect eligible plans and trigger execution
            realm_id = getattr(handler, "realm_id", "unknown_realm")
            plan_doc_id = self._persist_plan_draft(draft, realm_id)

            if draft.auto_run:
                self._logger.info(
                    "Plan '%s' persisted (status=approved, auto_run=True). ",
                    plan_doc_id,
                )
            else:
                self._logger.info(
                    "Plan '%s' persisted (status=draft). "
                    "Awaiting for approval (approvals_required=%s).",
                    plan_doc_id,
                    draft.approvals_required,
                )

            # NOTE: No inline execution here. PlanWatcher handles all execution
            # in daemon mode, ensuring single execution path and proper tracking.

        except Exception as exc:
            self._logger.exception(
                "Failed to generate/persist plan via %s: %s",
                handler.class_qualified_name(),
                exc,
            )

    # ---------------------------------
    # CLI or Semi-Automatic calls
    # ---------------------------------
    def process_cli_command(self, command_name: str, **kwargs) -> None:
        """
        Example method for manual (CLI-based) triggers that bypass watchers.
        E.g. 'ygg-mule reprocess-flowcell <id>' -> calls this method.
        """
        self._logger.info(f"Processing CLI command '{command_name}' with args={kwargs}")
        # Potentially route or handle an event, or do domain logic directly.
        # E.g. self.handle_event(YggdrasilEvent("manual_trigger", {"flowcell_id": kwargs["flowcell_id"]}, "CLI"))
        # Or run HPC submission logic, etc.
