"""
Generic CouchDB _changes feed fetcher.

This module provides a reusable, checkpoint-agnostic fetcher for streaming
document changes from a CouchDB database. It decouples the generic streaming logic
from checkpoint management (handled by callers).

Key design principle: Fetcher is filtering-agnostic and checkpoint-agnostic.
Callers apply their own filters and manage checkpoints independently.
"""

import asyncio
import logging
from collections.abc import AsyncGenerator
from typing import Any

from ibm_cloud_sdk_core.api_exception import ApiException
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import RequestException
from urllib3.exceptions import HTTPError as Urllib3HTTPError
from urllib3.exceptions import ProtocolError

from lib.core_utils.logging_utils import custom_logger


class ChangesFetcher:
    """
    Generic fetcher for CouchDB _changes feed.

    Yields raw document changes from the _changes endpoint, with configurable
    retry logic for transient failures.

    Attributes:
        db_handler: CouchDB connection handler
        include_docs: Whether to include full documents in _changes response
        retry_delay_sec: Initial delay (in seconds) before retry
        max_retries: Maximum number of retries on transient errors
        _last_seq: Last seen CouchDB sequence from most recent fetch batch
    """

    def __init__(
        self,
        db_handler: Any,  # CouchDBHandler
        include_docs: bool = True,
        retry_delay_sec: float = 2.0,
        max_retries: int = 3,
        logger: logging.Logger | None = None,
    ):
        """
        Initialize the ChangesFetcher.

        Args:
            db_handler: CouchDB connection (typically CouchDBHandler instance)
            include_docs: Include full documents in _changes feed (default True)
            retry_delay_sec: Delay in seconds before retrying (default 2.0)
            max_retries: Max retry attempts on transient errors (default 3)
            logger: Optional logger instance; uses module logger if None
        """
        self.db_handler = db_handler
        self.include_docs = include_docs
        self.retry_delay_sec = retry_delay_sec
        self.max_retries = max_retries
        self._logger = logger or custom_logger(__name__.split(".")[-1])
        self._last_seq: str | None = None

    @property
    def last_seq(self) -> str | None:
        """Last sequence token observed by ``fetch_changes``."""
        return self._last_seq

    async def fetch_changes(
        self,
        since: str | None = None,
    ) -> AsyncGenerator[dict[str, Any], None]:
        """
        Fetch changes from the _changes feed, starting from 'since' seq.

        This is a single-pass fetch that streams all pending changes and then stops.
        For continuous polling, use stream_changes_continuously() instead.

        Args:
            since: CouchDB sequence number to start from (None = start from beginning)

        Yields:
            Dict containing change entry: {"id": doc_id, "seq": seq, "doc": doc_dict, ...}

        Raises:
            ApiException: On connection failures (caller handles retry logic)
        """
        if since is None:
            since = "0"

        # Single-pass batch from handler wrapper.
        # No retry logic here; exceptions propagate to caller (stream_changes_continuously)
        # for centralized retry/backoff handling.
        results, last_seq = await asyncio.to_thread(
            self.db_handler.fetch_changes_batch,
            since=since,
            include_docs=self.include_docs,
            feed="normal",
        )
        self._last_seq = last_seq

        for change in results:
            if "id" in change and "seq" in change:
                yield change

    async def stream_changes_continuously(
        self,
        since: str | None = None,
        poll_interval_sec: float = 5.0,
    ) -> AsyncGenerator[dict[str, Any], None]:
        """
        Stream changes continuously with polling.

        Repeatedly fetches changes, sleeping between polls. On transient errors,
        retries with exponential backoff up to max_retries. Persistent errors
        are logged but do not stop the stream (caller's responsibility to manage lifecycle).

        Args:
            since: Starting sequence number (None = start from beginning)
            poll_interval_sec: Sleep duration (seconds) between polls (default 5.0)

        Yields:
            Dict containing change entry (same as fetch_changes)
        """
        if since is None:
            since = "0"

        current_seq = since
        retry_count = 0

        while True:
            try:
                self._logger.debug(
                    "Polling changes from db='%s' since='%s'",
                    self.db_handler.db_name,
                    current_seq,
                )

                # Fetch next batch
                async for change in self.fetch_changes(since=current_seq):
                    if "seq" in change:
                        current_seq = change["seq"]
                    yield change

                # Advance cursor even when no rows were returned.
                # CouchDB can advance last_seq on empty batches.
                if self._last_seq is not None and self._last_seq != current_seq:
                    current_seq = self._last_seq

                if retry_count > 0:
                    self._logger.debug(
                        "Recovered after retries; sleeping %.1fs before next poll",
                        poll_interval_sec,
                    )
                retry_count = 0

            except ApiException as e:
                if e.code == 404:
                    # DB doesn't exist or was deleted
                    self._logger.error(
                        "Database '%s' not found (404)", self.db_handler.db_name
                    )
                    raise
                elif e.code in (500, 503):
                    # Transient server error
                    retry_count += 1
                    if retry_count > self.max_retries:
                        self._logger.error(
                            "Max retries (%d) exceeded; aborting continuous stream",
                            self.max_retries,
                        )
                        raise
                    backoff = self.retry_delay_sec * (2 ** (retry_count - 1))
                    self._logger.warning(
                        "Transient error (code=%d); retry %d/%d after %.1fs",
                        e.code,
                        retry_count,
                        self.max_retries,
                        backoff,
                    )
                    await asyncio.sleep(backoff)
                else:
                    # Other API errors
                    self._logger.exception("Unexpected API error: %s", e)
                    raise

            except (
                ConnectionResetError,
                OSError,
                RequestsConnectionError,
                RequestException,
                ProtocolError,
                Urllib3HTTPError,
            ) as e:
                retry_count += 1
                if retry_count > self.max_retries:
                    self._logger.error(
                        "Max retries (%d) exceeded after transient connection errors; aborting continuous stream",
                        self.max_retries,
                    )
                    raise
                backoff = self.retry_delay_sec * (2 ** (retry_count - 1))
                self._logger.warning(
                    "Transient connection error; retry %d/%d after %.1fs: %s",
                    retry_count,
                    self.max_retries,
                    backoff,
                    e,
                )
                await asyncio.sleep(backoff)

            except Exception as e:
                self._logger.exception("Unexpected error in continuous stream: %s", e)
                raise

            # Sleep before next poll
            await asyncio.sleep(poll_interval_sec)
