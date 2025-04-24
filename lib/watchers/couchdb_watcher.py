import asyncio
import logging
from typing import Any, AsyncIterable, Callable, Optional

from lib.watchers.abstract_watcher import AbstractWatcher, YggdrasilEvent


class CouchDBWatcher(AbstractWatcher):
    """
    A concrete watcher that polls a CouchDB 'fetch_changes' asynchronous generator
    and emits YggdrasilEvent objects for each change detected.

    The 'changes_fetcher' is expected to be an async function returning an
    async iterator of (doc_data, module_loc) tuples:
        async def fetch_changes() -> AsyncIterable[tuple[Any, Any]]:
            ...
            yield (doc_data, module_loc)
            ...

    Example:
        async def fetch_changes():
            # loop or use the _changes feed
            yield ({"doc_id": "123"}, "realm_module")
    """

    def __init__(
        self,
        on_event: Callable[[YggdrasilEvent], None],
        changes_fetcher: Callable[[], AsyncIterable[tuple[Any, Any]]],
        event_type: str = "document_change",
        poll_interval: float = 5,
        name: str = "CouchDBWatcher",
        logger: Optional[logging.Logger] = None,
    ):
        """
        Args:
            on_event: A callback that consumes YggdrasilEvent objects.
            changes_fetcher: An async function (or method) returning an async
                iterator of (doc_data, module_loc) items.
            poll_interval: Seconds to wait between fetch cycles.
            name: Identifier for logging purposes.
            logger: Optional logger instance. Defaults to named logger.
        """
        super().__init__(on_event, event_type, name)
        self.changes_fetcher = changes_fetcher
        self.poll_interval = poll_interval
        self.name = name
        self._logger = logger or logging.getLogger(self.name)

    async def start(self):
        """
        Start polling the CouchDB changes feed via 'changes_fetcher' in
        a loop. For each (doc_data, module_loc) yielded, emit a 'document_change'
        event. If '_running' is set to False, polling stops gracefully.
        """
        if self._running:
            return

        self._running = True
        self._logger.info(f"Starting CouchDBWatcher: {self.name}")

        while self._running:
            try:
                # changes_fetcher should be an async generator
                async for doc_data, module_loc in self.changes_fetcher():
                    if not self._running:
                        break
                    payload = {"document": doc_data, "module_location": module_loc}
                    await self.emit(payload)

            except Exception as e:
                self._logger.error(
                    f"Error in {self.name} watcher loop: {e}", exc_info=True
                )

            # Sleep between poll cycles, or break if stopped
            if self._running:
                await asyncio.sleep(self.poll_interval)

        self._logger.info(f"CouchDBWatcher '{self.name}' stopped.")

    async def stop(self):
        """
        Stop polling. This sets '_running' to False; the while loop in
        'start()' will exit after the next iteration or after finishing
        the current fetch cycle.
        """
        if not self._running:
            return

        self._logger.info(f"Stopping CouchDBWatcher: {self.name}...")
        self._running = False
