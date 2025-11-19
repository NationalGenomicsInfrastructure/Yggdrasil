import asyncio
import importlib.metadata
import logging
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from lib.core_utils.singleton_decorator import singleton
from lib.handlers.base_handler import BaseHandler
from lib.ops.consumer_service import OpsConsumerService

# from lib.handlers.flowcell_handler import FlowcellHandler
from lib.watchers.couchdb_watcher import CouchDBWatcher
from lib.watchers.seq_data_watcher import SeqDataWatcher, YggdrasilEvent

# NOTE: Import EventType via `yggdrasil.*` namespace (not `lib.*`), to match external handlers (enum identity issue)
from yggdrasil.core_utils.event_types import EventType  # type: ignore
from yggdrasil.flow.events.emitter import FileSpoolEmitter
from yggdrasil.flow.planner.api import PlanningContext

# from lib.core_utils.event_types import EventType


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

        self._init_db_managers()

        self._logger.info("YggdrasilCore initialized.")

    def _init_db_managers(self):
        """
        Initializes database managers or other central resources.
        You can also place HPC or Prefect orchestrator initialization here.
        """
        self._logger.info("Initializing DB managers...")

        # Example usage
        from lib.couchdb.project_db_manager import ProjectDBManager
        from lib.couchdb.yggdrasil_db_manager import YggdrasilDBManager

        self.pdm = ProjectDBManager()
        self.ydm = YggdrasilDBManager()

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
        Attach a watcher (e.g. SeqDataWatcher, CouchDBWatcher).
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

        # Pretty summary: EVENT_TYPE(count, ...)
        if not getattr(self, "subscriptions", None):
            self._logger.warning("No handler subscriptions found.")
            return

        summary = ", ".join(
            f"{event_type.name}({len(self.subscriptions.get(event_type, []))})"
            for event_type in self.subscriptions.keys()
        )
        self._logger.debug("Handler Registrations: %s", summary)

    def setup_watchers(self):
        """
        Calls specialized methods to set up watchers of different types
        without cluttering the main method.
        """
        self._logger.info("Setting up watchers...")
        self._setup_fs_watchers()
        # self._setup_cdb_watchers()
        # Potentially more: self._setup_hpc_watchers(), etc.
        self._logger.info("Watchers setup done.")

    def _setup_fs_watchers(self):
        """
        Builds file-system watchers for each instrument specified in config["instrument_watch"].
        """
        instruments = self.config.get("instrument_watch", [])
        # Example config:
        # [
        #   {"name": "NextSeq", "directory_to_watch": "/data/illumina/nextseq", "marker_files": ["RTAComplete.txt"]},
        #   {"name": "Aviti", ...},
        # ]
        instruments = [
            {
                "name": "MiSeq",
                "directory": "sim_out/ngi2016003/flowcell_sync/illumina/miseq",
                "marker_files": ["RTAComplete.txt"],
            }
        ]

        for instrument in instruments:
            name = instrument.get("name", "UnnamedInstrument")
            watcher = SeqDataWatcher(
                on_event=self.handle_event,
                event_type=EventType.FLOWCELL_READY,
                name=f"SeqDataWatcher-{name}",
                config={
                    "instrument_name": name,
                    "directory_to_watch": instrument.get("directory", ""),
                    "marker_files": set(instrument.get("marker_files", ["test.txt"])),
                },
                recursive=True,
                logger=self._logger,
            )
            self.register_watcher(watcher)
            self._logger.debug(f"Registered SeqDataWatcher for {name}")

    def _setup_cdb_watchers(self):
        """
        Builds CouchDB watchers if config["couchdb"] is present.
        """

        self._logger.info("Setting up CouchDB watchers...")

        poll_interval = self.config.get("couchdb_poll_interval", 5)

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
        await self.ops_consumer.stop()
        self._logger.info("Ops consumer service stopped.")

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

        from lib.couchdb.project_db_manager import ProjectDBManager
        from lib.ops.consumer import FileSpoolConsumer
        from lib.ops.sinks.couch import OpsWriter

        self._logger.info("run_once: fetching project %s", doc_id)
        pdm = ProjectDBManager()
        doc = pdm.fetch_document_by_id(doc_id)
        if not doc:
            self._logger.error("No project with ID %s", doc_id)
            return

        handlers = self.subscriptions.get(EventType.PROJECT_CHANGE) or []
        if not handlers:
            self._logger.error(
                "No handlers registered for %s", EventType.PROJECT_CHANGE.name
            )
            return

        # Build the minimal, consistent payload the Tenx handler expects
        payload = {
            "doc": doc,
            "reason": f"run_once:{doc.get('project_id') or doc_id}",
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

                ctx = self._make_planning_ctx(
                    handler, scope, doc=doc, reason=payload["reason"]
                )
                payload["planning_ctx"] = ctx
                handler.run_now(payload)
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

    def handle_event(self, event: YggdrasilEvent) -> None:
        """
        Watchers call this to deliver events.
        NOTE! Broadcast's to ALL subscribers for a particular EventType.
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
                self._logger.debug("Dispatching %s to %s", event.event_type, handler)
                handler(payload)
            except Exception as exc:
                self._logger.error(
                    f"Error handling '{event.event_type}' with handler '{handler.class_qualified_name()}': {exc}",
                    exc_info=True,
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
