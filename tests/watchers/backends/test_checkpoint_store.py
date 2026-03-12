"""
Unit tests for lib.watchers.backends.checkpoint_store module.

Tests the checkpoint store implementations: InMemoryCheckpointStore and
CouchDBCheckpointStore.
"""

import unittest
from unittest.mock import MagicMock

from ibm_cloud_sdk_core.api_exception import ApiException

from lib.watchers.backends.base import Checkpoint
from lib.watchers.backends.checkpoint_store import (
    CouchDBCheckpointStore,
    InMemoryCheckpointStore,
)


class TestInMemoryCheckpointStore(unittest.TestCase):
    """Tests for InMemoryCheckpointStore."""

    def test_load_missing_checkpoint(self):
        """Test load() returns None for missing checkpoint."""
        store = InMemoryCheckpointStore()
        result = store.load("unknown_key")
        self.assertIsNone(result)

    def test_save_and_load(self):
        """Test save() and load() roundtrip."""
        store = InMemoryCheckpointStore()

        cp = Checkpoint(
            backend_key="couchdb:projects",
            value="100-abc",
            updated_at="2024-01-15T12:00:00Z",
        )
        store.save(cp)

        loaded = store.load("couchdb:projects")
        assert loaded is not None
        self.assertEqual(loaded.backend_key, "couchdb:projects")
        self.assertEqual(loaded.value, "100-abc")
        self.assertEqual(loaded.updated_at, "2024-01-15T12:00:00Z")

    def test_overwrite_checkpoint(self):
        """Test that save() overwrites existing checkpoint."""
        store = InMemoryCheckpointStore()

        cp1 = Checkpoint(
            backend_key="test_key",
            value="first",
            updated_at="2024-01-15T12:00:00Z",
        )
        store.save(cp1)

        cp2 = Checkpoint(
            backend_key="test_key",
            value="second",
            updated_at="2024-01-15T13:00:00Z",
        )
        store.save(cp2)

        loaded = store.load("test_key")
        assert loaded is not None
        self.assertEqual(loaded.value, "second")
        self.assertEqual(loaded.updated_at, "2024-01-15T13:00:00Z")

    def test_multiple_keys(self):
        """Test storing multiple keys."""
        store = InMemoryCheckpointStore()

        cp1 = Checkpoint(backend_key="key1", value="v1", updated_at="ts1")
        cp2 = Checkpoint(backend_key="key2", value="v2", updated_at="ts2")
        store.save(cp1)
        store.save(cp2)

        loaded1 = store.load("key1")
        assert loaded1 is not None
        self.assertEqual(loaded1.value, "v1")
        loaded2 = store.load("key2")
        assert loaded2 is not None
        self.assertEqual(loaded2.value, "v2")

    def test_clear_method(self):
        """Test clear() removes all checkpoints."""
        store = InMemoryCheckpointStore()

        cp = Checkpoint(backend_key="test_key", value="test", updated_at="ts")
        store.save(cp)
        self.assertIsNotNone(store.load("test_key"))

        store.clear()
        self.assertIsNone(store.load("test_key"))


class TestCouchDBCheckpointStore(unittest.TestCase):
    """Tests for CouchDBCheckpointStore with mocked YggdrasilDBManager."""

    def setUp(self):
        """Set up mock for YggdrasilDBManager."""
        self.mock_dbm = MagicMock()

    def test_doc_id_format(self):
        """Test checkpoint document ID format."""
        store = CouchDBCheckpointStore(db_manager=self.mock_dbm)
        doc_id = store._make_doc_id("couchdb:projects")
        self.assertEqual(doc_id, "watcher_checkpoint:couchdb:projects")

    def test_load_existing_checkpoint(self):
        """Test load() returns checkpoint from CouchDB."""
        store = CouchDBCheckpointStore(db_manager=self.mock_dbm)

        # Mock existing document
        mock_doc = {
            "_id": "watcher_checkpoint:couchdb:projects",
            "_rev": "1-abc",
            "backend_key": "couchdb:projects",
            "value": "100-xyz",
            "updated_at": "2024-01-15T12:00:00Z",
        }
        self.mock_dbm.fetch_document_by_id.return_value = mock_doc

        loaded = store.load("couchdb:projects")
        assert loaded is not None
        self.assertEqual(loaded.backend_key, "couchdb:projects")
        self.assertEqual(loaded.value, "100-xyz")
        self.assertEqual(loaded.updated_at, "2024-01-15T12:00:00Z")

    def test_load_missing_checkpoint(self):
        """Test load() returns None for missing checkpoint."""
        store = CouchDBCheckpointStore(db_manager=self.mock_dbm)

        # Mock missing document
        self.mock_dbm.fetch_document_by_id.return_value = None

        loaded = store.load("unknown_key")
        self.assertIsNone(loaded)

    def test_load_errors_return_none(self):
        """Test load() returns None on errors (doesn't propagate)."""
        store = CouchDBCheckpointStore(db_manager=self.mock_dbm)

        # Mock error
        self.mock_dbm.fetch_document_by_id.side_effect = ApiException(
            code=500, message="internal_error"
        )

        # load() catches exceptions and returns None (logs error)
        result = store.load("some_key")
        self.assertIsNone(result)

    def test_save_creates_new_document(self):
        """Test save() creates new checkpoint document."""
        store = CouchDBCheckpointStore(db_manager=self.mock_dbm)

        # Mock no existing document
        self.mock_dbm.fetch_document_by_id.return_value = None

        # Mock the server.put_document call
        mock_result = MagicMock()
        mock_result.get_result.return_value = {"ok": True}
        self.mock_dbm.server.put_document.return_value = mock_result

        cp = Checkpoint(
            backend_key="couchdb:projects",
            value="200-abc",
            updated_at="2024-01-15T14:00:00Z",
        )
        store.save(cp)

        # Verify put_document was called
        self.mock_dbm.server.put_document.assert_called_once()
        call_args = self.mock_dbm.server.put_document.call_args
        self.assertEqual(
            call_args.kwargs["doc_id"], "watcher_checkpoint:couchdb:projects"
        )
        doc = call_args.kwargs["document"]
        self.assertEqual(doc["backend_key"], "couchdb:projects")
        self.assertEqual(doc["value"], "200-abc")

    def test_save_updates_existing_document(self):
        """Test save() preserves _rev when updating existing document."""
        store = CouchDBCheckpointStore(db_manager=self.mock_dbm)

        # Mock existing document
        mock_existing = {
            "_id": "watcher_checkpoint:couchdb:projects",
            "_rev": "1-oldrev",
            "backend_key": "couchdb:projects",
            "value": "old_value",
            "updated_at": "2024-01-15T10:00:00Z",
        }
        self.mock_dbm.fetch_document_by_id.return_value = mock_existing

        # Mock the server.put_document call
        mock_result = MagicMock()
        mock_result.get_result.return_value = {"ok": True}
        self.mock_dbm.server.put_document.return_value = mock_result

        cp = Checkpoint(
            backend_key="couchdb:projects",
            value="new_value",
            updated_at="2024-01-15T14:00:00Z",
        )
        store.save(cp)

        # Verify _rev was preserved
        call_args = self.mock_dbm.server.put_document.call_args
        doc = call_args.kwargs["document"]
        self.assertEqual(doc["_rev"], "1-oldrev")
        self.assertEqual(doc["value"], "new_value")

    def test_save_skips_unchanged_value(self):
        """Test save() skips write when checkpoint value is unchanged."""
        store = CouchDBCheckpointStore(db_manager=self.mock_dbm)

        self.mock_dbm.fetch_document_by_id.return_value = {
            "_id": "watcher_checkpoint:couchdb:projects",
            "_rev": "3-same",
            "backend_key": "couchdb:projects",
            "value": "same_value",
            "updated_at": "2024-01-15T10:00:00Z",
        }

        cp = Checkpoint(
            backend_key="couchdb:projects",
            value="same_value",
            updated_at="2024-01-15T14:00:00Z",
        )
        store.save(cp)

        self.mock_dbm.server.put_document.assert_not_called()

    def test_save_with_integer_value(self):
        """Test save() handles integer checkpoint values."""
        store = CouchDBCheckpointStore(db_manager=self.mock_dbm)

        self.mock_dbm.fetch_document_by_id.return_value = None

        # Mock the server.put_document call
        mock_result = MagicMock()
        mock_result.get_result.return_value = {"ok": True}
        self.mock_dbm.server.put_document.return_value = mock_result

        cp = Checkpoint(
            backend_key="postgres:events",
            value=12345,  # Integer value
            updated_at="2024-01-15T14:00:00Z",
        )
        store.save(cp)

        call_args = self.mock_dbm.server.put_document.call_args
        doc = call_args.kwargs["document"]
        self.assertEqual(doc["value"], 12345)

    def test_custom_db_manager(self):
        """Test CouchDBCheckpointStore can use custom db manager."""
        custom_dbm = MagicMock()
        store = CouchDBCheckpointStore(db_manager=custom_dbm)
        self.assertIs(store._dbm, custom_dbm)


if __name__ == "__main__":
    unittest.main()
