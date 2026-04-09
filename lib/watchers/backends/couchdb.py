"""
CouchDB watcher backend.

This backend watches a CouchDB database's _changes feed and emits
RawWatchEvent objects for each change.

Features:
- Polls _changes feed with raw HTTP GET (feed=normal until caught up, then longpoll)
- Per-row checkpointing after each successfully processed or intentionally skipped row
- Internal docs (_design/*, _local/*) silently skipped
- 404 on non-deleted row: skip, log WARNING, advance checkpoint (no event)
- Transient doc fetch errors retried up to max_observation_retries times
- Non-retriable fetch errors: skip, log ERROR, advance checkpoint (no event)
- Graceful shutdown on stop()
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Any

from lib.core_utils.logging_utils import custom_logger
from lib.couchdb.couchdb_connection import (
    CouchDBHandler,
    is_transient_doc_fetch_error,
    is_transient_poll_error,
)
from lib.couchdb.couchdb_models import ChangesBatch, ChangesRow, FeedMode
from lib.watchers.backends.base import CheckpointStore, RawWatchEvent, WatcherBackend

_INTERNAL_PREFIXES = ("_design/", "_local/")


def _is_internal_doc(doc_id: str) -> bool:
    """Return True if the document ID belongs to a CouchDB-internal namespace."""
    return doc_id.startswith(_INTERNAL_PREFIXES)


class CouchDBBackend(WatcherBackend):
    """
    CouchDB _changes feed watcher backend.

    Polls the _changes endpoint of a CouchDB database and emits
    RawWatchEvent objects for each document change.

    Config schema (after resolution by WatcherManager):
        {
            "url": str,                      # CouchDB server URL (with scheme)
            "db": str,                       # Database name
            "user_env": str,                 # Env var name for username
            "pass_env": str,                 # Env var name for password
            "backend": str,                  # Optional, used for validation only
            "poll_interval": float,          # Seconds between normal polls (default 5.0)
            "start_seq": str,                # Starting seq if no checkpoint (default "0")
            "limit": int | None,             # Max changes per poll (default None)
            "longpoll_timeout_ms": int,      # CouchDB longpoll timeout (default 60000)
            "max_observation_retries": int,  # Doc fetch retries (default 3, from global policy)
            "observation_retry_delay_s": float,  # Delay between retries (default 1.0)
        }

    Required keys: db, url, user_env, pass_env

    Checkpoint strategy:
        Checkpoints are saved per-row, immediately after each successfully
        processed or intentionally skipped row. On restart, at most one event
        (the last yielded before shutdown) may be replayed; downstream handling
        is assumed idempotent.

    Feed mode:
        Starts in NORMAL mode. Transitions to LONGPOLL when batch.pending == 0
        (feed is caught up). Returns to NORMAL when pending > 0 again.

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

    DEFAULT_POLL_INTERVAL = 5.0
    DEFAULT_START_SEQ = "0"
    DEFAULT_LIMIT = None
    DEFAULT_LONGPOLL_TIMEOUT_MS = 60_000
    DEFAULT_MAX_RETRIES = 3
    DEFAULT_RETRY_DELAY = 1.0

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
            config: Resolved configuration with url, db, credentials, and policy keys
            checkpoint_store: Storage for checkpoint persistence
            queue_maxsize: Maximum size of internal event queue
            logger: Optional logger instance

        Raises:
            KeyError: If required config keys (url, db, user_env, pass_env) are missing
            ValueError: If required config values are empty
        """
        super().__init__(
            backend_key=backend_key,
            config=config,
            checkpoint_store=checkpoint_store,
            queue_maxsize=queue_maxsize,
            logger=logger or custom_logger(f"{__name__}.CouchDBBackend"),
        )

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

        self._db_name = config["db"]
        self._url = config["url"]
        self._user_env = config["user_env"]
        self._pass_env = config["pass_env"]

        self._poll_interval = float(
            config.get("poll_interval", self.DEFAULT_POLL_INTERVAL)
        )
        self._start_seq = str(config.get("start_seq", self.DEFAULT_START_SEQ))
        self._limit: int | None = config.get("limit", self.DEFAULT_LIMIT)
        self._longpoll_timeout_ms = int(
            config.get("longpoll_timeout_ms", self.DEFAULT_LONGPOLL_TIMEOUT_MS)
        )
        self._max_retries = int(
            config.get("max_observation_retries", self.DEFAULT_MAX_RETRIES)
        )
        self._retry_delay = float(
            config.get("observation_retry_delay_s", self.DEFAULT_RETRY_DELAY)
        )

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
        return CouchDBHandler(
            db_name=self._db_name,
            url=self._url,
            user_env=self._user_env,
            pass_env=self._pass_env,
        )

    def _load_checkpoint_value(self) -> str | None:
        """Return the persisted checkpoint seq, or None if no checkpoint exists."""
        checkpoint = self.load_checkpoint()
        if checkpoint is not None and checkpoint.value is not None:
            return str(checkpoint.value)
        return None

    async def stream(self) -> AsyncIterator[RawWatchEvent]:  # type: ignore[override]
        """
        Yield CouchDB changes as RawWatchEvent objects.

        Connects to CouchDB, then enters a polling loop:
        - Uses NORMAL feed until caught up (pending == 0), then LONGPOLL
        - Filters internal docs (_design/*, _local/*)
        - Emits events for deleted rows immediately
        - Fetches doc for non-deleted rows with retry
        - Checkpoints after each row (processed or skipped)
        - Retries _changes poll indefinitely on transient failures

        Stops when self._running becomes False.
        """
        self._logger.info(
            "Starting CouchDB stream for %s (db=%s, poll_interval=%.1fs)",
            self.backend_key,
            self._db_name,
            self._poll_interval,
        )

        try:
            handler = self._create_handler()
        except Exception as e:
            self._logger.error(
                "Failed to create CouchDB handler for %s: %s",
                self.backend_key,
                e,
                exc_info=True,
            )
            return

        # Load checkpoint or fall back to start_seq
        since: str | None = self._load_checkpoint_value()
        if since is not None:
            self._logger.info("Resuming from checkpoint seq='%s'", since)
        else:
            since = self._start_seq
            self._logger.info("No checkpoint found, starting from seq='%s'", since)

        feed_mode = FeedMode.NORMAL

        while self._running:

            # ── Poll _changes ─────────────────────────────────────────────────
            try:
                batch: ChangesBatch = await asyncio.to_thread(
                    handler.fetch_changes_raw,
                    since=since,
                    feed=feed_mode.value,
                    limit=self._limit,
                    timeout_ms=self._longpoll_timeout_ms,
                )
            except Exception as exc:
                if is_transient_poll_error(exc):
                    self._logger.warning(
                        "_changes poll failed (transient) for %s: %s",
                        self.backend_key,
                        exc,
                    )
                else:
                    self._logger.error(
                        "_changes poll failed (permanent) for %s: %s",
                        self.backend_key,
                        exc,
                    )
                await asyncio.sleep(self._retry_delay)
                continue

            # ── Feed mode transition ──────────────────────────────────────────
            new_mode = FeedMode.LONGPOLL if batch.pending == 0 else FeedMode.NORMAL
            if new_mode != feed_mode:
                self._logger.debug(
                    "Feed mode: %s → %s (pending=%d)",
                    feed_mode.value,
                    new_mode.value,
                    batch.pending,
                )
                feed_mode = new_mode

            # ── Process rows ──────────────────────────────────────────────────
            for row in batch.rows:
                if not self._running:
                    break

                # 1. Internal doc filter
                if _is_internal_doc(row.id):
                    self._logger.debug("Skipping internal doc: %s", row.id)
                    self.save_checkpoint(row.seq)
                    continue

                # 2. Deleted row — emit without fetching doc
                if row.deleted:
                    event = self._change_to_event(row, doc=None)
                    yield event
                    self.save_checkpoint(row.seq)
                    continue

                # 3. Non-deleted row — fetch with retry
                doc, skip = await self._fetch_doc_with_retry(
                    handler, row, self._max_retries, self._retry_delay
                )

                if skip:
                    # 404 or permanent error: already logged; advance checkpoint
                    self.save_checkpoint(row.seq)
                    continue

                event = self._change_to_event(row, doc=doc)
                yield event
                self.save_checkpoint(row.seq)

            # ── Advance since ─────────────────────────────────────────────────
            if batch.last_seq is not None:
                since = str(batch.last_seq)

            # ── Avoid busy-loop in normal mode with no rows ───────────────────
            if feed_mode == FeedMode.NORMAL and not batch.rows:
                await asyncio.sleep(self._poll_interval)

    async def _fetch_doc_with_retry(
        self,
        handler: CouchDBHandler,
        row: ChangesRow,
        max_retries: int,
        retry_delay: float,
    ) -> tuple[dict[str, Any] | None, bool]:
        """
        Fetch a document with retry logic.

        Returns:
            (doc, skip) tuple.
            skip=False means a doc was fetched successfully.
            skip=True means the row should be skipped (404, non-retriable, or exhausted).
        """
        last_exc: Exception | None = None

        for attempt in range(max_retries + 1):  # 0..max_retries inclusive
            try:
                doc = await asyncio.to_thread(handler.fetch_document_by_id, row.id)
                if doc is None:
                    # 404: non-fatal, no retry
                    self._logger.warning(
                        "Doc %s not found (404) for non-deleted _changes row (seq=%s)",
                        row.id,
                        row.seq,
                    )
                    return None, True  # skip
                return doc, False  # success

            except Exception as exc:
                if not is_transient_doc_fetch_error(exc):
                    # Permanent error — no retry
                    self._logger.error(
                        "Permanent error fetching doc %s (seq=%s): %s",
                        row.id,
                        row.seq,
                        exc,
                    )
                    return None, True  # skip

                last_exc = exc
                if attempt < max_retries:
                    self._logger.warning(
                        "Transient error fetching doc %s (attempt %d/%d): %s",
                        row.id,
                        attempt + 1,
                        max_retries,
                        exc,
                    )
                    await asyncio.sleep(retry_delay)

        # Exhausted retries
        self._logger.error(
            "Doc fetch for %s failed after %d retries (seq=%s): %s",
            row.id,
            max_retries,
            row.seq,
            last_exc,
        )
        return None, True  # skip

    def _change_to_event(
        self, row: ChangesRow, doc: dict[str, Any] | None
    ) -> RawWatchEvent:
        """
        Convert a parsed ChangesRow and fetched doc to a RawWatchEvent.

        Args:
            row: Parsed change row from _changes feed
            doc: Fetched document dict, or None for deleted rows

        Returns:
            RawWatchEvent with id, seq, deleted, and doc fields
        """
        return RawWatchEvent(
            id=row.id,
            seq=row.seq,
            deleted=row.deleted,
            doc=doc,
        )
