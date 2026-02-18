"""
CouchDB watcher backend.

This backend watches a CouchDB database's _changes feed and emits
RawWatchEvent objects for each change.

Features:
- Polls _changes feed with configurable interval
- Checkpoint-based resume (saves after each poll batch)
- Delegates polling retry/backoff to ChangesFetcher
- Graceful shutdown on stop()
- Uses existing CouchDBHandler infrastructure for connection management
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any, cast

from lib.couchdb.changes_fetcher import ChangesFetcher
from lib.couchdb.couchdb_connection import CouchDBHandler
from lib.watchers.backends.base import CheckpointStore, RawWatchEvent, WatcherBackend

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__.split(".")[-1])


class CouchDBBackend(WatcherBackend):
    """
    CouchDB _changes feed watcher backend.

    Polls the _changes endpoint of a CouchDB database and emits
    RawWatchEvent objects for each document change.

    Config schema (after resolution by WatcherManager):
        {
            "url": str,              # CouchDB server URL (normalized with scheme)
            "db": str,               # Database name
            "user_env": str,         # Env var name for username
            "pass_env": str,         # Env var name for password
            "backend": str,          # Optional, used for validation only
            "include_docs": bool,    # Include full doc in changes (default True)
            "poll_interval": float,  # Seconds between polls (default 1.0)
            "start_seq": str,        # Starting seq if no checkpoint (default "0")
            "limit": int,            # Max changes per poll (default 100)
        }

    Required keys: db, url, user_env, pass_env

    Checkpoint strategy:
        - Checkpoints are saved after each poll batch using last_seq
        - This is more efficient than per-event checkpointing
        - On restart, at most one batch may be replayed (idempotency assumed)

    Retry strategy:
        - Delegated to ChangesFetcher.stream_changes_continuously()

    Example:
        backend = CouchDBBackend(
            backend_key="couchdb:projects_db",
            config={
                "url": "https://couch.example.org:5984",
                "db": "projects",
                "user_env": "COUCH_USER",
                "pass_env": "COUCH_PASS",
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
    DEFAULT_FEED = "normal"
    DEFAULT_LONGPOLL_TIMEOUT_MS = 5000

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
            KeyError: If required config keys (url, db, user_env, pass_env) are missing
        """
        super().__init__(
            backend_key=backend_key,
            config=config,
            checkpoint_store=checkpoint_store,
            queue_maxsize=queue_maxsize,
            logger=logger or logging.getLogger(f"{__name__}.CouchDBBackend"),
        )

        # Validate required config keys (must be present AND non-empty)
        required_keys = ["db", "url", "user_env", "pass_env"]
        missing = [k for k in required_keys if k not in config]
        if missing:
            raise KeyError(
                f"Missing required config keys {missing} for backend {backend_key}"
            )
        empty = [k for k in required_keys if not config.get(k)]
        if empty:
            raise ValueError(
                f"Empty values for required config keys {empty} for backend {backend_key}"
            )

        # Extract config with defaults
        self._db_name = config["db"]
        self._url = config["url"]
        self._user_env = config["user_env"]
        self._pass_env = config["pass_env"]

        self._include_docs = config.get("include_docs", self.DEFAULT_INCLUDE_DOCS)
        self._poll_interval = config.get("poll_interval", self.DEFAULT_POLL_INTERVAL)
        self._start_seq = config.get("start_seq", self.DEFAULT_START_SEQ)
        self._limit = config.get("limit", self.DEFAULT_LIMIT)
        self._feed = str(config.get("feed", self.DEFAULT_FEED)).lower()
        self._longpoll_timeout_ms = int(
            config.get("longpoll_timeout_ms", self.DEFAULT_LONGPOLL_TIMEOUT_MS)
        )

        # Handler and fetcher initialized lazily in stream()
        self._handler: CouchDBHandler | None = None
        self._fetcher: ChangesFetcher | None = None

    def _create_handler(self) -> CouchDBHandler:
        """
        Create CouchDB handler using the stateless client factory.

        Each handler gets its own CloudantV1 client instance.
        No shared clients, no connection pooling.

        Returns:
            Configured CouchDBHandler instance for the target database.

        Raises:
            ConnectionError: If database doesn't exist or connection fails
            RuntimeError: If required env var is missing
            ValueError: If URL is missing scheme
        """
        handler_ctor = cast(Any, CouchDBHandler)
        return handler_ctor(
            db_name=self._db_name,
            url=self._url,
            user_env=self._user_env,
            pass_env=self._pass_env,
        )

    def stream(self) -> AsyncIterator[RawWatchEvent]:
        """
        Yield CouchDB changes as RawWatchEvent objects.

        Checkpoint strategy:
            - Load checkpoint once at startup
            - Save checkpoint when emitted event seq advances

        Error handling:
            - Handled by ChangesFetcher policy layer

        No direct post_changes calls or backend-local polling loops.
        """

        async def _gen() -> AsyncIterator[RawWatchEvent]:
            self._logger.info(
                "Starting CouchDB stream for %s (db=%s, feed=%s, interval=%.1fs)",
                self.backend_key,
                self._db_name,
                self._feed,
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
                return

            self._fetcher = ChangesFetcher(
                db_handler=self._handler,
                include_docs=self._include_docs,
                logger=self._logger,
            )

            # Load checkpoint or use start_seq
            checkpoint = self.load_checkpoint()
            # Handle None checkpoint value explicitly
            if checkpoint and checkpoint.value is not None:
                current_seq = str(checkpoint.value)
                self._logger.info(
                    "Resuming from checkpoint ='%s' (updated %s)",
                    current_seq,
                    checkpoint.updated_at,
                )
            else:
                current_seq = str(self._start_seq)
                self._logger.info(
                    "No checkpoint found, starting from ='%s'", current_seq
                )

            fetcher = cast(ChangesFetcher, self._fetcher)

            async for change in fetcher.stream_changes_continuously(
                since=current_seq,
                poll_interval_sec=self._poll_interval,
            ):
                if not self._running:
                    break

                event = self._change_to_event(change)
                yield event

                seq = change.get("seq")
                if seq is not None:
                    seq_str = str(seq)
                    if seq_str != current_seq:
                        current_seq = seq_str
                        self.save_checkpoint(current_seq)

        return _gen()

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
