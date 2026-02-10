"""
Test realm watcher for monitoring test scenario documents.

ScenarioDocWatcher monitors the 'yggdrasil' database for documents with
type="ygg_test_scenario" and emits TEST_SCENARIO_CHANGE events.
"""

import asyncio
import json
import logging
from collections.abc import Callable

from requests import Response

from lib.core_utils.event_types import EventType
from lib.couchdb.watcher_checkpoint_store import WatcherCheckpointStore
from lib.couchdb.yggdrasil_db_manager import YggdrasilDBManager
from lib.watchers.abstract_watcher import AbstractWatcher


class ScenarioDocWatcher(AbstractWatcher):
    """
    Watcher that monitors the yggdrasil database for test scenario documents.

    Watches for documents matching:
        - type == "ygg_test_scenario"

    Emits TEST_SCENARIO_CHANGE events with the document in payload.
    """

    SCENARIO_DOC_TYPE = "ygg_test_scenario"

    def __init__(
        self,
        on_event: Callable,
        poll_interval: float = 5.0,
        name: str = "ScenarioDocWatcher",
        logger: logging.Logger | None = None,
    ):
        """
        Args:
            on_event: Callback to invoke with YggdrasilEvent
            poll_interval: Seconds between poll cycles
            name: Watcher identifier for logging
            logger: Optional logger instance
        """
        super().__init__(
            on_event=on_event,
            event_type=EventType.TEST_SCENARIO_CHANGE,
            name=name,
            logger=logger,
        )
        self.poll_interval = poll_interval
        # Use the real yggdrasil DB manager so we can persist checkpoints
        self._db_handler = YggdrasilDBManager()
        self.checkpoint_store = WatcherCheckpointStore(
            watcher_name="ScenarioDocWatcher",
            db_handler=self._db_handler,
            logger=self._logger,
        )
        self._last_seq: str | None = None

    async def start(self) -> None:
        """
        Start monitoring the yggdrasil database for scenario documents.

        Uses CouchDB's _changes feed with continuous mode.
        Filters for documents with type="ygg_test_scenario".
        """
        if self._running:
            self._logger.warning("ScenarioDocWatcher already running")
            return

        self._running = True
        self._logger.info("Starting ScenarioDocWatcher...")

        # Load checkpoint (dev-only watcher, but persist to mirror real realms)
        checkpoint = self.checkpoint_store.get_checkpoint()
        if checkpoint:
            self._logger.info(
                "ScenarioDocWatcher resuming from checkpoint seq='%s'", checkpoint
            )
            self._last_seq = checkpoint
        else:
            # Start from 'now' to avoid replaying historical scenarios
            self._logger.info("ScenarioDocWatcher no checkpoint; starting from 'now'")
            self._last_seq = "now"

        while self._running:
            try:
                await self._poll_changes()
            except Exception as e:
                self._logger.error(
                    "Error in ScenarioDocWatcher poll cycle: %s", e, exc_info=True
                )

            # Sleep between poll cycles
            if self._running:
                await asyncio.sleep(self.poll_interval)

        self._logger.info("ScenarioDocWatcher stopped.")

    async def stop(self) -> None:
        """Stop the watcher gracefully."""
        if not self._running:
            return

        self._logger.info("Stopping ScenarioDocWatcher...")
        self._running = False

    async def _poll_changes(self) -> None:
        """
        Poll CouchDB _changes feed for scenario documents.

        Filters for type="ygg_test_scenario" documents and emits events.
        Only saves checkpoint when at least one relevant document was processed.
        """
        try:
            # Use post_changes for filtered changes
            # Start from "now" by default to avoid replaying historical docs
            response: dict = self._db_handler.server.post_changes(
                db=self._db_handler.db_name,
                since=self._last_seq or "now",
                include_docs=True,
                filter="_selector",
                selector={"type": {"$eq": self.SCENARIO_DOC_TYPE}},
            ).get_result()  # type: ignore[assignment]

            results = response.get("results", []) if response else []
            last_seq = response.get("last_seq") if response else None

            # Track whether we processed any relevant docs
            processed_any = False

            for change in results:
                if not self._running:
                    break

                doc = change.get("doc", {})
                if not doc:
                    continue

                # Skip deleted documents
                if change.get("deleted"):
                    self._logger.debug(
                        "Skipping deleted scenario: %s", change.get("id")
                    )
                    continue

                # Verify type (belt and suspenders)
                if doc.get("type") != self.SCENARIO_DOC_TYPE:
                    continue

                self._logger.info(
                    "Detected scenario document change: %s", change.get("id")
                )

                # Build payload for handler
                payload = {
                    "doc": doc,
                    "reason": f"scenario_change:{doc.get('_id')}",
                }

                await self.emit(payload, source=self.name)
                processed_any = True

            # Only persist checkpoint if we actually processed something.
            # last_seq can advance due to cluster activity even with empty results;
            # saving checkpoint when we processed nothing is wasteful and noisy.
            if last_seq:
                self._last_seq = last_seq  # Always track in memory for next poll
                if processed_any:
                    self.checkpoint_store.save_checkpoint_with_retry(last_seq)

        except Exception as e:
            self._logger.error("Error polling changes: %s", e, exc_info=True)
            raise

    async def _poll_changes_streaming(self) -> None:
        """
        Alternative: Stream _changes feed continuously.

        This method uses iter_lines for truly continuous monitoring.
        Currently not used - keeping for reference/future use.
        """
        try:
            response = self._db_handler.server.post_changes_as_stream(
                db=self._db_handler.db_name,
                feed="continuous",
                since=self._last_seq or "now",
                include_docs=True,
                filter="_selector",
                selector={"type": {"$eq": self.SCENARIO_DOC_TYPE}},
                timeout=30000,  # 30 second timeout
            ).get_result()

            # Type assertion for streaming response
            stream: Response = response  # type: ignore

            for line in stream.iter_lines():
                if not self._running:
                    break

                if not line:
                    continue

                try:
                    change = json.loads(line)
                except json.JSONDecodeError:
                    continue

                # Skip heartbeats and non-document entries
                if "id" not in change:
                    continue

                doc = change.get("doc", {})
                if not doc or doc.get("type") != self.SCENARIO_DOC_TYPE:
                    continue

                if change.get("deleted"):
                    continue

                self._logger.info(
                    "Detected scenario document (streaming): %s", change.get("id")
                )

                payload = {
                    "doc": doc,
                    "reason": f"scenario_change:{doc.get('_id')}",
                }

                await self.emit(payload, source=self.name)

                # Update sequence
                if "seq" in change:
                    self._last_seq = change["seq"]
                    if self._last_seq is not None:
                        self.checkpoint_store.save_checkpoint_with_retry(self._last_seq)

        except Exception as e:
            self._logger.error("Error in streaming changes: %s", e, exc_info=True)
            raise
