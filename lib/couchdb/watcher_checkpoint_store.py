"""
Watcher checkpoint persistence store.

Manages checkpoint storage for watchers in the 'yggdrasil' database.
Each checkpoint is scoped to a watcher name and uses _rev-based optimistic
locking for race-safe concurrent updates.

Checkpoint documents are keyed as: _id = "watcher_checkpoint:<WatcherName>"
This enables multiple watchers to maintain independent checkpoint state.
"""

import logging
from datetime import UTC
from typing import Any

from ibm_cloud_sdk_core.api_exception import ApiException

from lib.core_utils.logging_utils import custom_logger


class WatcherCheckpointStore:
    """
    Per-watcher checkpoint persistence in the 'yggdrasil' database.

    Provides conflict-safe upsert semantics via CouchDB's _rev mechanism.
    Checkpoints are typically CouchDB sequence numbers for _changes feeds.

    Attributes:
        watcher_name: Name of the watcher (used in doc ID scoping)
        db_handler: CouchDB connection to 'yggdrasil' database
    """

    def __init__(
        self,
        watcher_name: str,
        db_handler: Any,  # CouchDBHandler (yggdrasil DB)
        logger: logging.Logger | None = None,
    ):
        """
        Initialize checkpoint store for named watcher.

        Args:
            watcher_name: Unique watcher identifier (e.g. "PlanWatcher", "ProjectDBWatcher")
            db_handler: CouchDB connection to 'yggdrasil' database
            logger: Optional logger; uses module logger if None
        """
        self.watcher_name = watcher_name
        self.db_handler = db_handler
        self._logger = logger or custom_logger(f"{__name__}.{type(self).__name__}")

    def checkpoint_doc_id(self) -> str:
        """
        Return canonical document ID for this watcher's checkpoint.

        Format: "watcher_checkpoint:<WatcherName>"

        Returns:
            str: Document ID for checkpoint storage
        """
        return f"watcher_checkpoint:{self.watcher_name}"

    def get_checkpoint(self) -> str | None:
        """
        Fetch the last saved checkpoint sequence number.

        Returns None if the checkpoint document doesn't exist (watcher has never run).
        Treats empty/missing sequence as None (caller may interpret as "0" or start fresh).

        Returns:
            str: Last checkpoint seq, or None if never saved

        Raises:
            ApiException: On unexpected database errors (not 404)
        """
        doc_id = self.checkpoint_doc_id()
        try:
            doc = self.db_handler.fetch_document_by_id(doc_id)
            if doc:
                seq = doc.get("last_seq")
                if seq:
                    self._logger.debug(
                        "Retrieved checkpoint for '%s': seq='%s'",
                        self.watcher_name,
                        seq,
                    )
                    return seq
                else:
                    self._logger.debug("Checkpoint doc exists but has no last_seq")
                    return None
            else:
                self._logger.debug(
                    "No checkpoint doc found for '%s'", self.watcher_name
                )
                return None
        except Exception as e:
            self._logger.exception(
                "Error fetching checkpoint for '%s': %s", self.watcher_name, e
            )
            raise

    def save_checkpoint(self, seq: str) -> bool:
        """
        Save checkpoint sequence number with conflict-safe upsert.

        Uses optimistic locking (_rev) to prevent lost updates in concurrent scenarios.
        On conflict (409), returns False; caller should retry.

        Args:
            seq: Sequence number to persist (typically from CouchDB _changes feed)

        Returns:
            bool: True on success, False on conflict (caller retries)

        Raises:
            ApiException: On unexpected database errors (not 409)
        """
        doc_id = self.checkpoint_doc_id()

        try:
            # Fetch current document to get _rev
            current_doc = self.db_handler.fetch_document_by_id(doc_id)

            # No-op if the checkpoint is unchanged to avoid futile writes
            if current_doc and current_doc.get("last_seq") == seq:
                self._logger.debug(
                    "Checkpoint for '%s' unchanged (seq='%s'); skipping save",
                    self.watcher_name,
                    seq,
                )
                return True

            rev = None
            if current_doc and "_rev" in current_doc:
                rev = current_doc["_rev"]

            # Build checkpoint document
            checkpoint_doc = {
                "_id": doc_id,
                "type": "watcher_checkpoint",
                "watcher_name": self.watcher_name,
                "last_seq": seq,
                "updated_at": self._get_utc_now_iso(),
                "updated_by": "yggdrasil-core",
            }

            # Include _rev for conflict-safe update
            if rev:
                checkpoint_doc["_rev"] = rev

            # Attempt upsert
            self.db_handler.server.put_document(
                db=self.db_handler.db_name,
                doc_id=doc_id,
                document=checkpoint_doc,
            ).get_result()

            self._logger.debug(
                "Saved checkpoint for '%s': seq='%s'",
                self.watcher_name,
                seq,
            )
            return True

        except ApiException as e:
            if e.code == 409:
                # Conflict: document was modified between fetch and update
                self._logger.debug(
                    "Checkpoint conflict for '%s' (409); caller should retry",
                    self.watcher_name,
                )
                return False
            else:
                self._logger.exception(
                    "Unexpected error saving checkpoint for '%s': %s",
                    self.watcher_name,
                    e,
                )
                raise
        except Exception as e:
            self._logger.exception(
                "Error saving checkpoint for '%s': %s",
                self.watcher_name,
                e,
            )
            raise

    def save_checkpoint_with_retry(self, seq: str, max_retries: int = 3) -> bool:
        """
        Save checkpoint with automatic retry on conflict.

        Useful for callers that don't want to implement retry logic themselves.

        Args:
            seq: Sequence number to persist
            max_retries: Maximum retry attempts on conflict (default 3)

        Returns:
            bool: True on success, False if max_retries exceeded

        Raises:
            ApiException: On unexpected database errors
        """
        for attempt in range(1, max_retries + 1):
            success = self.save_checkpoint(seq)
            if success:
                return True
            self._logger.debug(
                "Checkpoint save conflict; retry %d/%d for '%s'",
                attempt,
                max_retries,
                self.watcher_name,
            )

        self._logger.error(
            "Failed to save checkpoint for '%s' after %d retries",
            self.watcher_name,
            max_retries,
        )
        return False

    def clear_checkpoint(self) -> bool:
        """
        Delete the checkpoint document (useful for testing/reset).

        Returns:
            bool: True if deleted, False if not found

        Raises:
            ApiException: On unexpected errors
        """
        doc_id = self.checkpoint_doc_id()
        try:
            current_doc = self.db_handler.fetch_document_by_id(doc_id)
            if not current_doc:
                self._logger.debug(
                    "No checkpoint to delete for '%s'", self.watcher_name
                )
                return False

            self.db_handler.server.delete_document(
                db=self.db_handler.db_name,
                doc_id=doc_id,
                rev=current_doc.get("_rev"),
            ).get_result()

            self._logger.info("Deleted checkpoint for '%s'", self.watcher_name)
            return True

        except ApiException as e:
            if e.code == 404:
                return False
            self._logger.exception(
                "Error deleting checkpoint for '%s': %s", self.watcher_name, e
            )
            raise

    @staticmethod
    def _get_utc_now_iso() -> str:
        """Return current UTC timestamp in ISO 8601 format."""
        from datetime import datetime

        return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
