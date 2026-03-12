"""
Unit tests for WatcherCheckpointStore.

Tests checkpoint persistence with conflict handling and optimistic locking.
All CouchDB operations are mocked.
"""

import unittest
from unittest.mock import Mock, patch

from lib.couchdb.watcher_checkpoint_store import WatcherCheckpointStore


class MockApiException(Exception):
    """Mock ApiException for testing."""

    def __init__(self, code, message="Test error"):
        super().__init__(message)
        self.code = code
        self.message = message


class TestWatcherCheckpointStore(unittest.TestCase):
    """Unit tests for WatcherCheckpointStore."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_db_handler = Mock()
        self.mock_db_handler.db_name = "yggdrasil"
        self.mock_db_handler.server = Mock()

    def test_checkpoint_doc_id(self):
        """Test canonical doc ID construction."""
        store = WatcherCheckpointStore("PlanWatcher", self.mock_db_handler)
        self.assertEqual(store.checkpoint_doc_id(), "watcher_checkpoint:PlanWatcher")

    def test_checkpoint_doc_id_with_spaces(self):
        """Test doc ID with watcher name containing special chars."""
        store = WatcherCheckpointStore("My Watcher-123", self.mock_db_handler)
        self.assertEqual(store.checkpoint_doc_id(), "watcher_checkpoint:My Watcher-123")

    def test_get_checkpoint_not_found(self):
        """Test get_checkpoint when checkpoint doesn't exist."""
        self.mock_db_handler.fetch_document_by_id.return_value = None

        store = WatcherCheckpointStore("PlanWatcher", self.mock_db_handler)
        result = store.get_checkpoint()

        self.assertIsNone(result)
        self.mock_db_handler.fetch_document_by_id.assert_called_once_with(
            "watcher_checkpoint:PlanWatcher"
        )

    def test_get_checkpoint_found(self):
        """Test get_checkpoint when checkpoint exists."""
        mock_doc = {"_id": "watcher_checkpoint:PlanWatcher", "last_seq": "123-abc"}
        self.mock_db_handler.fetch_document_by_id.return_value = mock_doc

        store = WatcherCheckpointStore("PlanWatcher", self.mock_db_handler)
        result = store.get_checkpoint()

        self.assertEqual(result, "123-abc")

    def test_get_checkpoint_empty_seq(self):
        """Test get_checkpoint when seq field is empty."""
        mock_doc = {"_id": "watcher_checkpoint:PlanWatcher", "last_seq": ""}
        self.mock_db_handler.fetch_document_by_id.return_value = mock_doc

        store = WatcherCheckpointStore("PlanWatcher", self.mock_db_handler)
        result = store.get_checkpoint()

        self.assertIsNone(result)

    def test_get_checkpoint_missing_seq_field(self):
        """Test get_checkpoint when seq field is missing."""
        mock_doc = {"_id": "watcher_checkpoint:PlanWatcher"}
        self.mock_db_handler.fetch_document_by_id.return_value = mock_doc

        store = WatcherCheckpointStore("PlanWatcher", self.mock_db_handler)
        result = store.get_checkpoint()

        self.assertIsNone(result)

    def test_save_checkpoint_new_doc(self):
        """Test saving checkpoint for new doc (no existing _rev)."""
        self.mock_db_handler.fetch_document_by_id.return_value = None
        mock_result = Mock()
        mock_result.get_result.return_value = {}
        self.mock_db_handler.server.put_document.return_value = mock_result

        store = WatcherCheckpointStore("PlanWatcher", self.mock_db_handler)
        result = store.save_checkpoint("456-def")

        self.assertTrue(result)

        # Verify put_document was called
        put_call = self.mock_db_handler.server.put_document.call_args
        self.assertEqual(put_call[1]["doc_id"], "watcher_checkpoint:PlanWatcher")

        # Verify document content
        doc = put_call[1]["document"]
        self.assertEqual(doc["last_seq"], "456-def")
        self.assertEqual(doc["watcher_name"], "PlanWatcher")
        self.assertEqual(doc["type"], "watcher_checkpoint")
        self.assertNotIn("_rev", doc)  # No _rev for new doc

    def test_save_checkpoint_with_existing_rev(self):
        """Test saving checkpoint with existing _rev (update case)."""
        mock_existing = {
            "_id": "watcher_checkpoint:PlanWatcher",
            "_rev": "1-xyz",
            "last_seq": "100",
        }
        self.mock_db_handler.fetch_document_by_id.return_value = mock_existing
        mock_result = Mock()
        mock_result.get_result.return_value = {"_rev": "2-xyz"}
        self.mock_db_handler.server.put_document.return_value = mock_result

        store = WatcherCheckpointStore("PlanWatcher", self.mock_db_handler)
        result = store.save_checkpoint("200-xyz")

        self.assertTrue(result)

        # Verify _rev was included
        put_call = self.mock_db_handler.server.put_document.call_args
        doc = put_call[1]["document"]
        self.assertEqual(doc["_rev"], "1-xyz")
        self.assertEqual(doc["last_seq"], "200-xyz")

    @patch("lib.couchdb.watcher_checkpoint_store.ApiException", MockApiException)
    def test_save_checkpoint_conflict_409(self):
        """Test save_checkpoint returns False on conflict (409)."""
        self.mock_db_handler.fetch_document_by_id.return_value = None
        mock_result = Mock()
        mock_result.get_result.side_effect = MockApiException(409, "Conflict")
        self.mock_db_handler.server.put_document.return_value = mock_result

        store = WatcherCheckpointStore("PlanWatcher", self.mock_db_handler)
        result = store.save_checkpoint("300")

        self.assertFalse(result)

    @patch("lib.couchdb.watcher_checkpoint_store.ApiException", MockApiException)
    def test_save_checkpoint_unexpected_error(self):
        """Test save_checkpoint re-raises unexpected errors."""
        self.mock_db_handler.fetch_document_by_id.return_value = None
        mock_result = Mock()
        mock_result.get_result.side_effect = MockApiException(500, "Server error")
        self.mock_db_handler.server.put_document.return_value = mock_result

        store = WatcherCheckpointStore("PlanWatcher", self.mock_db_handler)

        with self.assertRaises(MockApiException) as ctx:
            store.save_checkpoint("400")

        self.assertEqual(ctx.exception.code, 500)

    def test_save_checkpoint_with_retry_success_first_try(self):
        """Test save_checkpoint_with_retry succeeds on first try."""
        self.mock_db_handler.fetch_document_by_id.return_value = None
        mock_result = Mock()
        mock_result.get_result.return_value = {}
        self.mock_db_handler.server.put_document.return_value = mock_result

        store = WatcherCheckpointStore("PlanWatcher", self.mock_db_handler)
        result = store.save_checkpoint_with_retry("500")

        self.assertTrue(result)
        self.mock_db_handler.server.put_document.assert_called_once()

    @patch("lib.couchdb.watcher_checkpoint_store.ApiException", MockApiException)
    def test_save_checkpoint_with_retry_succeeds_after_conflict(self):
        """Test retry succeeds after initial conflict."""
        mock_existing = {"_id": "watcher_checkpoint:PlanWatcher", "_rev": "1-old"}
        self.mock_db_handler.fetch_document_by_id.return_value = mock_existing

        # First call: conflict, second call: success
        mock_result_fail = Mock()
        mock_result_fail.get_result.side_effect = MockApiException(409, "Conflict")

        mock_result_success = Mock()
        mock_result_success.get_result.return_value = {"_rev": "2-new"}

        self.mock_db_handler.server.put_document.side_effect = [
            mock_result_fail,
            mock_result_success,
        ]

        store = WatcherCheckpointStore("PlanWatcher", self.mock_db_handler)
        result = store.save_checkpoint_with_retry("600", max_retries=2)

        self.assertTrue(result)
        # Should have retried
        self.assertEqual(self.mock_db_handler.server.put_document.call_count, 2)

    @patch("lib.couchdb.watcher_checkpoint_store.ApiException", MockApiException)
    def test_save_checkpoint_with_retry_max_exceeded(self):
        """Test retry fails after max_retries exceeded."""
        self.mock_db_handler.fetch_document_by_id.return_value = None
        mock_result = Mock()
        mock_result.get_result.side_effect = MockApiException(409, "Conflict")
        self.mock_db_handler.server.put_document.return_value = mock_result

        store = WatcherCheckpointStore("PlanWatcher", self.mock_db_handler)
        result = store.save_checkpoint_with_retry("700", max_retries=2)

        self.assertFalse(result)
        # With max_retries=2, should try exactly 2 times (not initial + retries)
        self.assertEqual(self.mock_db_handler.server.put_document.call_count, 2)

    def test_clear_checkpoint_exists(self):
        """Test clear_checkpoint when doc exists."""
        mock_doc = {
            "_id": "watcher_checkpoint:PlanWatcher",
            "_rev": "1-abc",
        }
        self.mock_db_handler.fetch_document_by_id.return_value = mock_doc
        mock_result = Mock()
        mock_result.get_result.return_value = {}
        self.mock_db_handler.server.delete_document.return_value = mock_result

        store = WatcherCheckpointStore("PlanWatcher", self.mock_db_handler)
        result = store.clear_checkpoint()

        self.assertTrue(result)
        self.mock_db_handler.server.delete_document.assert_called_once()
        delete_call = self.mock_db_handler.server.delete_document.call_args
        self.assertEqual(delete_call[1]["rev"], "1-abc")

    def test_clear_checkpoint_not_found(self):
        """Test clear_checkpoint when doc doesn't exist."""
        self.mock_db_handler.fetch_document_by_id.return_value = None

        store = WatcherCheckpointStore("PlanWatcher", self.mock_db_handler)
        result = store.clear_checkpoint()

        self.assertFalse(result)
        self.mock_db_handler.server.delete_document.assert_not_called()

    @patch("lib.couchdb.watcher_checkpoint_store.ApiException", MockApiException)
    def test_clear_checkpoint_api_error(self):
        """Test clear_checkpoint re-raises API errors."""
        mock_doc = {"_id": "watcher_checkpoint:PlanWatcher", "_rev": "1-abc"}
        self.mock_db_handler.fetch_document_by_id.return_value = mock_doc
        mock_result = Mock()
        mock_result.get_result.side_effect = MockApiException(500, "Server error")
        self.mock_db_handler.server.delete_document.return_value = mock_result

        store = WatcherCheckpointStore("PlanWatcher", self.mock_db_handler)

        with self.assertRaises(MockApiException) as ctx:
            store.clear_checkpoint()

        self.assertEqual(ctx.exception.code, 500)

    def test_utc_now_iso_format(self):
        """Test UTC timestamp is in ISO format."""
        timestamp = WatcherCheckpointStore._get_utc_now_iso()
        # Should match ISO 8601 format with Z suffix
        self.assertRegex(timestamp, r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


if __name__ == "__main__":
    unittest.main()
