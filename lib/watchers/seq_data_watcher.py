import asyncio
import logging
from collections.abc import Callable, Coroutine
from pathlib import Path
from typing import Any

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from lib.core_utils.event_types import EventType
from lib.core_utils.logging_utils import custom_logger
from lib.watchers.abstract_watcher import AbstractWatcher, YggdrasilEvent


class SeqDataDetector(FileSystemEventHandler):
    """
    Watchdog event handler that checks if newly created files match a set of
    required marker files for a subfolder. Once all markers are present,
    it notifies the parent watcher (FileSystemWatcher).
    """

    def __init__(
        self,
        instrument_name: str,
        marker_files: set[str],
        emit_coroutine: Callable[[Any, str | None], Coroutine[Any, Any, None]],
        async_loop: asyncio.AbstractEventLoop,
        logger: logging.Logger,
        discovered_subfolders: dict[str, set[str]] = {},
    ):
        """
        Args:
            instrument_name: For logging & identification (e.g. "Illumina")
            marker_files: The set of filenames that must all appear before we trigger an event
            emit_coroutine: The watcher's method to call (like watcher.emit(...))
            async_loop: The event loop to use for scheduling the emit coroutine
            logger: Logger instance
            discovered_subfolders: Shared dictionary that tracks which markers
                have been found in each subfolder. If not provided, a new one is created.
        """
        super().__init__()
        self.instrument_name = instrument_name
        self.marker_files = marker_files
        self.emit_coroutine = emit_coroutine  # the async method from watcher
        self.loop = async_loop
        self.logger = logger
        self.discovered_subfolders = discovered_subfolders

    def on_created(self, event):
        """
        When a new file is created, check if it's one of the required marker files.
        If so, update the set for the corresponding subfolder. If we have discovered
        *all* required marker files in that subfolder, emit an event exactly once.
        """
        if event.is_directory:
            return

        path = Path(str(event.src_path))
        filename = path.name
        if filename not in self.marker_files:
            return  # not a relevant marker

        subfolder = str(path.parent)
        self.logger.debug(
            f"[{self.instrument_name}] New file {filename} in {subfolder}"
        )

        # Add the filename to the discovered set for this subfolder
        if subfolder not in self.discovered_subfolders:
            self.discovered_subfolders[subfolder] = set()
        self.discovered_subfolders[subfolder].add(filename)

        # Check if all markers are found
        if self.discovered_subfolders[subfolder] == self.marker_files:
            payload = {"instrument": self.instrument_name, "subfolder": subfolder}
            self.logger.info(
                f"{self.instrument_name}: Found all markers in {subfolder}"
            )

            # We cannot directly 'await' emit_coroutine because on_created is sync.
            # Instead, we schedule it on the watcher's loop.
            coro = self.emit_coroutine(payload, None)
            asyncio.run_coroutine_threadsafe(coro, self.loop)

            # Remove from discovered_subfolders so we don't double-emit
            del self.discovered_subfolders[subfolder]


class SeqDataWatcher(AbstractWatcher):
    """
    A parametric file system watcher that can be configured for each instrument.

    Example usage:
        illumina_config = {
            "instrument_name": "Illumina",
            "directory_to_watch": "/data/illumina",
            "marker_files": {"RTAComplete.txt", "CopyComplete.txt"}
        }

        illu_watcher = FileSystemWatcher(on_event=core.handle_event, config=illumina_config)
        core.register_watcher(illu_watcher)
    """

    def __init__(
        self,
        on_event: Callable[[YggdrasilEvent], None],
        config: dict[str, Any],
        event_type: EventType = EventType.FLOWCELL_READY,
        name: str = "SeqDataWatcher",
        recursive: bool = True,
        logger: logging.Logger | None = None,
    ):
        """
        Args:
            on_event: The YggdrasilCore callback to receive events

            logger: Optional logger. If None, one is created.
            recursive: Whether to watch subdirectories under directory_to_watch.
        """
        super().__init__(on_event, event_type, name)
        self.name = name
        config = config or {}
        self.instrument_name = config.get("instrument_name", "UnknownInstrument")
        self.directory_to_watch = config.get("directory_to_watch", "/tmp")
        self.marker_files = set(config.get("marker_files", []))
        self.recursive = recursive

        self._observer = Observer()
        # self._discovered_subfolders: Dict[str, Set[str]] = {}

        # For storing the loop in start()
        self._loop: asyncio.AbstractEventLoop | None = None

        self._logger = logger or custom_logger(f"{__name__}.{type(self).__name__}")

    async def start(self):
        """
        Starts the Watchdog observer in a background thread,
        and watch for newly created files.

        Grabs the current event loop, since the observer runs in its own thread.
        """
        if self._running:
            return  # already running, do nothing

        self._running = True
        self._loop = asyncio.get_running_loop()  # store reference to the current loop

        self._logger.debug(f"Starting FileSystemWatcher for {self.instrument_name} ...")

        event_detector = SeqDataDetector(
            instrument_name=self.instrument_name,
            marker_files=self.marker_files,
            # discovered_subfolders=self._discovered_subfolders,
            emit_coroutine=self.emit,  # pass our async emit
            async_loop=self._loop,
            logger=self._logger,
        )

        self._observer.schedule(
            event_detector, path=self.directory_to_watch, recursive=self.recursive
        )
        self._observer.start()

        self._logger.info(f"Started FileSystemWatcher for {self.instrument_name}.")

        # The observer runs in its own thread, but we still need to
        # keep an async loop alive until stop() is called.
        while self._running:
            await asyncio.sleep(0.5)

        self._logger.info(f"FileSystemWatcher '{self.instrument_name}' stopped.")

    async def stop(self):
        """
        Stop the Watchdog observer and end the async loop in start().
        """
        if not self._running:
            return

        self._running = False
        self._logger.info(f"Stopping FileSystemWatcher for {self.instrument_name}...")
        self._observer.stop()
        self._observer.join()
