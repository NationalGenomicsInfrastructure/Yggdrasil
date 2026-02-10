"""
CouchDB watcher backend.

This backend watches a CouchDB database's _changes feed and emits
RawWatchEvent objects for each change.

Features:
- Polls _changes feed with configurable interval
- Checkpoint-based resume (saves after each poll batch)
- Exponential backoff retry on transient errors
- Graceful shutdown on stop()
- Uses existing CouchDBHandler infrastructure for connection management
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from lib.couchdb.couchdb_connection import CouchDBHandler
from lib.watchers.backends.base import CheckpointStore, RawWatchEvent, WatcherBackend

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class CouchDBBackend(WatcherBackend):
    """
    CouchDB _changes feed watcher backend.

    Polls the _changes endpoint of a CouchDB database and emits
    RawWatchEvent objects for each document change.

    Config schema (after resolution by WatcherManager):
        {
            "url": str,              # CouchDB server URL
            "db": str,               # Database name
            "user": str | None,      # Resolved username (from env var)
            "pass_env_name": str | None,  # Env var NAME for password (not resolved)
            "include_docs": bool,    # Include full doc in changes (default True)
            "poll_interval": float,  # Seconds between polls (default 1.0)
            "start_seq": str,        # Starting seq if no checkpoint (default "0")
            "limit": int,            # Max changes per poll (default 100)
        }

    Checkpoint strategy:
        - Checkpoints are saved after each poll batch using last_seq
        - This is more efficient than per-event checkpointing
        - On restart, at most one batch may be replayed (idempotency assumed)

    Retry strategy:
        - Transient errors trigger exponential backoff (2^n seconds, max 60)
        - After max_retries (5), logs error and resets retry count
        - Backend continues running (doesn't crash on persistent errors)
        # TODO: Consider alerting/metrics for repeated failures (beyond scope of this implementation)
        # TODO: Consider more exponential backoff strategies (jitter, etc.) since this is a long-running backend that may encounter various failure modes in production

    Example:
        backend = CouchDBBackend(
            backend_key="couchdb:projects_db",
            config={
                "url": "https://couch.example.org",
                "db": "projects",
                "user": "admin",
                "pass_env_name": "COUCH_PASS",  # env var name, not password
            },
            checkpoint_store=store,
        )
        await backend.start()

        async for event in backend.events():
            print(event.id, event.deleted)

        await backend.stop()
    """

    # Default configuration values
    DEFAULT_POLL_INTERVAL = 1.0
    DEFAULT_START_SEQ = "0"  # CouchDB _changes feed can start from "0" (from start) or "now" to only get new changes
    DEFAULT_INCLUDE_DOCS = True
    DEFAULT_LIMIT = 100  # Max changes to fetch per poll
    MAX_RETRIES = 5
    MAX_BACKOFF = 60  # Max backoff time in seconds (1 minute)

    def __init__(
        self,
        backend_key: str,
        config: dict[str, Any],
        checkpoint_store: CheckpointStore,
        queue_maxsize: int = 1000,
        logger: logging.Logger | None = None,
    ):
        """
        Initialize the CouchDB backend.

        Args:
            backend_key: Unique identifier (format: "couchdb:{connection}")
            config: Resolved configuration with url, db, credentials
            checkpoint_store: Storage for checkpoint persistence
            queue_maxsize: Maximum size of internal event queue
            logger: Optional logger instance

        Raises:
            KeyError: If required config keys (url, db) are missing
        """
        super().__init__(
            backend_key=backend_key,
            config=config,
            checkpoint_store=checkpoint_store,
            queue_maxsize=queue_maxsize,
            logger=logger or logging.getLogger(f"{__name__}.CouchDBBackend"),
        )

        # Validate required config
        if "db" not in config:
            raise KeyError(
                f"Missing required config key 'db' for backend {backend_key}"
            )

        # Extract config with defaults
        self._db_name = config["db"]
        self._include_docs = config.get("include_docs", self.DEFAULT_INCLUDE_DOCS)
        self._poll_interval = config.get("poll_interval", self.DEFAULT_POLL_INTERVAL)
        self._start_seq = config.get("start_seq", self.DEFAULT_START_SEQ)
        self._limit = config.get("limit", self.DEFAULT_LIMIT)

        # Credentials from resolved config (may be None for legacy mode)
        # Note: pass_env_name is the env var NAME, not the resolved password
        self._url = config.get("url")
        self._user = config.get("user")
        self._pass_env_name = config.get("pass_env_name")

        # Handler initialized lazily in _produce_events
        self._handler: CouchDBHandler | None = None

    def _create_handler(self) -> CouchDBHandler:
        """
        Create CouchDB handler using the shared connection infrastructure.

        If url/user/pass_env_name are provided in config (from endpoint resolution),
        the handler will use CouchDBConnectionManager.get_server() to obtain a
        cached client for that endpoint. Otherwise, falls back to the default client.

        Returns:
            Configured CouchDBHandler instance for the target database.

        Raises:
            ConnectionError: If database doesn't exist or connection fails
            RuntimeError: If pass_env_name is specified but env var is missing
        """
        return CouchDBHandler(
            db_name=self._db_name,
            url=self._url,
            user=self._user,
            pass_env_name=self._pass_env_name,
        )

    async def _produce_events(self) -> None:
        """
        Poll CouchDB _changes feed and emit RawWatchEvent objects.

        Checkpoint strategy:
            - Save checkpoint AFTER each poll batch using last_seq
            - This is more efficient than per-event checkpointing
            - At most one batch replayed on restart (idempotency assumed)

        Error handling:
            - Transient errors: retry with exponential backoff
            - After MAX_RETRIES: log and reset (continue running)
            - CancelledError: exit cleanly

        Uses the shared CouchDBHandler infrastructure for connection management.
        """
        self._logger.info(
            "Starting CouchDB producer for %s (db=%s, interval=%.1fs)",
            self.backend_key,
            self._db_name,
            self._poll_interval,
        )

        # Initialize handler using shared infrastructure
        try:
            self._handler = self._create_handler()
        except Exception as e:
            self._logger.error(
                "Failed to create CouchDB handler for %s: %s",
                self.backend_key,
                e,
                exc_info=True,
            )
            # Use finally block's put_nowait pattern
            try:
                self._event_queue.put_nowait(None)
            except asyncio.QueueFull:
                pass
            return

        # Load checkpoint or use start_seq
        checkpoint = self.load_checkpoint()
        # Handle None checkpoint value explicitly
        if checkpoint and checkpoint.value is not None:
            since: str | int = checkpoint.value
            self._logger.info(
                "Resuming from checkpoint: %s (updated %s)",
                since,
                checkpoint.updated_at,
            )
        else:
            since = self._start_seq
            self._logger.info("No checkpoint found, starting from: %s", since)

        retry_count = 0

        try:
            while self._running:
                try:
                    # Poll for changes using the handler's post_changes method
                    result: dict[str, Any] = await asyncio.to_thread(
                        self._handler.post_changes,
                        since=since,
                        include_docs=self._include_docs,
                        limit=self._limit,
                    )
                    changes: list[dict[str, Any]] = result.get("results", [])

                    if changes:
                        self._logger.debug(
                            "Received %d changes from %s (since=%s)",
                            len(changes),
                            self._db_name,
                            since,
                        )

                    # Emit events for each change
                    for change in changes:
                        if not self._running:
                            break

                        event = self._change_to_event(change)
                        await self._event_queue.put(event)

                    # Save checkpoint AFTER batch using last_seq
                    # last_seq is always present in _changes response
                    last_seq = result.get("last_seq")
                    if last_seq is not None:
                        since = last_seq
                        self.save_checkpoint(since)

                    # Reset retry count on success
                    retry_count = 0

                    # Wait before next poll
                    await asyncio.sleep(self._poll_interval)

                except asyncio.CancelledError:
                    self._logger.debug("Producer cancelled for %s", self.backend_key)
                    raise  # Re-raise to exit the while loop and hit finally

                except Exception as e:
                    retry_count += 1
                    self._logger.warning(
                        "Error polling changes for %s (attempt %d/%d): %s",
                        self.backend_key,
                        retry_count,
                        self.MAX_RETRIES,
                        e,
                    )

                    if retry_count >= self.MAX_RETRIES:
                        self._logger.error(
                            "Max retries (%d) reached for %s; resetting and continuing",
                            self.MAX_RETRIES,
                            self.backend_key,
                            exc_info=True,
                        )
                        retry_count = 0

                    # Exponential backoff
                    backoff = min(2**retry_count, self.MAX_BACKOFF)
                    await asyncio.sleep(backoff)

        except asyncio.CancelledError:
            # Normal cancellation from stop()
            pass
        finally:
            # Always put sentinel using non-blocking put_nowait to avoid deadlock
            self._logger.debug(
                "Producer exiting for %s, sending sentinel", self.backend_key
            )
            try:
                self._event_queue.put_nowait(None)
            except asyncio.QueueFull:
                self._logger.warning(
                    "Queue full during shutdown for %s; consumer should check _running",
                    self.backend_key,
                )

    def _change_to_event(self, change: dict[str, Any]) -> RawWatchEvent:
        """
        Convert a CouchDB _changes entry to a RawWatchEvent.

        Args:
            change: A single entry from the _changes results array

        Returns:
            RawWatchEvent with id, doc, seq, deleted, and meta fields
        """
        doc_id = change.get("id", "")
        deleted = change.get("deleted", False)
        seq = change.get("seq")

        # Don't include doc for deletions (it would be None anyway)
        doc = change.get("doc") if not deleted else None

        # Preserve additional CouchDB-specific info in meta
        meta: dict[str, Any] = {}
        if "changes" in change:
            meta["changes"] = change["changes"]

        return RawWatchEvent(
            id=doc_id,
            doc=doc,
            seq=seq,
            deleted=deleted,
            meta=meta,
        )
