import asyncio
import importlib.metadata
import logging
from collections.abc import Mapping
from typing import Any

from lib.core_utils.singleton_decorator import singleton
from lib.handlers.base_handler import BaseHandler
from lib.ops.consumer_service import OpsConsumerService

# from lib.handlers.flowcell_handler import FlowcellHandler
from lib.watchers.couchdb_watcher import CouchDBWatcher
from lib.watchers.seq_data_watcher import SeqDataWatcher, YggdrasilEvent

# NOTE: Import EventType via `yggdrasil.*` namespace (not `lib.*`), to match external handlers (enum identity issue)
from yggdrasil.core_utils.event_types import EventType  # type: ignore

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

        # Handlers: event_type -> function(event_payload)
        self.handlers: dict[str, BaseHandler] = {}

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

    def register_watcher(self, watcher) -> None:
        """
        Attach a watcher (e.g. SeqDataWatcher, CouchDBWatcher).
        The watchers will be started/stopped by YggdrasilCore.
        """
        self._logger.debug(f"Registering watcher: {watcher}")
        self.watchers.append(watcher)

    def register_handler(self, event_type: str, handler_func: BaseHandler) -> None:
        """
        Attach a function that processes events of a certain type.
        Example handler_func: your code that triggers HPC jobs, Prefect flows, etc.
        """
        self.handlers[event_type] = handler_func
        self._logger.debug(f"Registered handler for event_type='{event_type}'")

    def auto_register_external_handlers(self):
        for entry_point in importlib.metadata.entry_points(group="ygg.handler"):
            handler_cls = entry_point.load()

            # every external handler **must** expose a class-attr event_type
            event_type = getattr(handler_cls, "event_type", None)

            # Check if event_type is a valid EventType - handle enum identity issues
            # by checking both isinstance and value membership
            if not (
                isinstance(event_type, EventType)
                or (
                    event_type is not None
                    and hasattr(event_type, "value")
                    and event_type.value in [e.value for e in EventType]
                )
            ):
                self._logger.error(
                    "✘  %s skipped: event_type %r is not a valid EventType",
                    entry_point.name,
                    event_type,
                )
                continue

            self.register_handler(event_type, handler_cls())  # type: ignore

            self._logger.info(
                "✓  registered external handler %s for %s",
                entry_point.name,
                event_type.name,  # type: ignore
            )

    def setup_handlers(self) -> None:
        """
        Instantiate and register all event handlers.
        """
        self._logger.info("Setting up event handlers...")
        # 1. Auto-register external handlers from entry points
        self.auto_register_external_handlers()

        # 2. Register built-in handlers
        from lib.handlers.bp_analysis_handler import BestPracticeAnalysisHandler

        # Best‑practice analysis for new/changed ProjectDB docs
        project_handler = BestPracticeAnalysisHandler()
        self.register_handler(EventType.PROJECT_CHANGE, project_handler)

        # # Demultiplexing / downstream pipeline for newly-ready flowcells
        # flowcell_handler = FlowcellHandler()
        # self.register_handler(EventType.FLOWCELL_READY, flowcell_handler)

        # NOTE: When we have a CLI‑triggered event type, e.g. 'manual_run', register it here too
        # cli_handler = CLIHandler()
        # self.register_handler(EventType.<whatever>, cli_handler)

        self._logger.debug(
            "Registered handlers for events: %s", ", ".join(self.handlers.keys())
        )

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

    def run_once(self, doc_id: str):
        """
        Fetch the project doc, build the payload, and synchronously
        drive the BestPracticeAnalysisHandler without starting watchers.
        """
        from lib.core_utils.module_resolver import get_module_location
        from lib.couchdb.project_db_manager import ProjectDBManager

        pdm = ProjectDBManager()
        doc = pdm.fetch_document_by_id(doc_id)
        if not doc:
            self._logger.error(f"No project with ID {doc_id}")
            return

        module_loc = get_module_location(doc)
        if not module_loc:
            self._logger.error(f"No module for project {doc_id}")
            return

        payload = {"document": doc, "module_location": module_loc}

        # Use the appropriate registered earlier
        handler = self.handlers.get(EventType.PROJECT_CHANGE)
        if not handler:
            self._logger.error(
                "No handler for '%s' event type", EventType.PROJECT_CHANGE
            )
            return

        if not hasattr(handler, "run_now"):
            raise RuntimeError(
                f"Handler {handler!r} must implement `.run_now(payload)` for one-off mode"
            )
        handler.run_now(payload)

        # 2) After the step(s) emitted events, do a single consume pass
        # TODO: Put the imports at the top when this is stable
        import os
        from pathlib import Path

        from lib.ops.consumer import FileSpoolConsumer
        from lib.ops.sinks.couch import OpsWriter

        spool = Path(os.environ.get("YGG_EVENT_SPOOL", "/tmp/ygg_events"))
        FileSpoolConsumer(
            spool, OpsWriter(db_name=os.environ.get("OPS_DB", "yggdrasil_ops"))
        ).consume()

    def handle_event(self, event: YggdrasilEvent) -> None:
        """
        The callback watchers call to pass events to YggdrasilCore.
        You can store or route these events to 'handlers' or do direct logic here.
        """
        self._logger.info(f"Received event '{event.event_type}' from '{event.source}'")
        handler = self.handlers.get(event.event_type)
        if handler:
            try:
                self._logger.debug(
                    f"Dispatching event_type='{event.event_type}' to its handler."
                )
                handler(event.payload)
            except Exception as exc:
                self._logger.error(
                    f"Error while handling event '{event.event_type}': {exc}",
                    exc_info=True,
                )
        else:
            self._logger.warning(
                f"No handler registered for event_type='{event.event_type}'"
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
