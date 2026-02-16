"""
Tests for CouchDB connection utilities.

Tests for:
- CouchDBClientFactory: Stateless factory for creating CloudantV1 clients
- CouchDBHandler: Base class for database operations
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch


# Create mocks for IBM Cloud SDK classes
class MockApiException(Exception):
    def __init__(self, message, code=None):
        super().__init__(message)
        self.code = code
        self.message = message


# Mock the IBM Cloud SDK modules to avoid import errors in test environment
mock_api_exception_module = MagicMock()
mock_api_exception_module.ApiException = MockApiException
sys.modules["ibm_cloud_sdk_core"] = MagicMock()
sys.modules["ibm_cloud_sdk_core.api_exception"] = mock_api_exception_module
sys.modules["ibmcloudant"] = MagicMock()
sys.modules["ibmcloudant.cloudant_v1"] = MagicMock()

# Import after mocks
import lib.couchdb.couchdb_connection

lib.couchdb.couchdb_connection.ApiException = MockApiException

from lib.couchdb.couchdb_connection import CouchDBClientFactory, CouchDBHandler


class TestCouchDBClientFactory(unittest.TestCase):
    """Tests for CouchDBClientFactory."""

    @patch("lib.couchdb.couchdb_connection.cloudant_v1.CloudantV1")
    @patch("lib.couchdb.couchdb_connection.CouchDbSessionAuthenticator")
    @patch.dict(os.environ, {"TEST_USER": "admin", "TEST_PASS": "secret"})
    def test_create_client_success(self, mock_auth, mock_cloudant):
        """Test successful client creation."""
        mock_client = MagicMock()
        mock_client.get_server_information.return_value.get_result.return_value = {
            "version": "3.1.1"
        }
        mock_cloudant.return_value = mock_client

        client = CouchDBClientFactory.create_client(
            url="http://localhost:5984",
            user_env="TEST_USER",
            pass_env="TEST_PASS",
        )

        # Verify client was created and configured
        mock_auth.assert_called_once_with("admin", "secret")
        mock_cloudant.assert_called_once()
        mock_client.set_service_url.assert_called_once_with("http://localhost:5984")
        mock_client.get_server_information.assert_called_once()
        self.assertEqual(client, mock_client)

    @patch("lib.couchdb.couchdb_connection.cloudant_v1.CloudantV1")
    @patch("lib.couchdb.couchdb_connection.CouchDbSessionAuthenticator")
    @patch.dict(os.environ, {"TEST_USER": "admin", "TEST_PASS": "secret"})
    def test_create_client_skip_verification(self, mock_auth, mock_cloudant):
        """Test client creation without connection verification."""
        mock_client = MagicMock()
        mock_cloudant.return_value = mock_client

        client = CouchDBClientFactory.create_client(
            url="http://localhost:5984",
            user_env="TEST_USER",
            pass_env="TEST_PASS",
            verify_connection=False,
        )

        # Verify no ping was attempted
        mock_client.get_server_information.assert_not_called()
        self.assertEqual(client, mock_client)

    def test_create_client_missing_scheme_raises(self):
        """Test that URL without scheme raises ValueError."""
        with self.assertRaises(ValueError) as ctx:
            CouchDBClientFactory.create_client(
                url="localhost:5984",
                user_env="TEST_USER",
                pass_env="TEST_PASS",
            )
        self.assertIn("must include scheme", str(ctx.exception))

    @patch.dict(os.environ, {"TEST_PASS": "secret"}, clear=False)
    def test_create_client_missing_user_env_raises(self):
        """Test that missing user env var raises RuntimeError."""
        # Ensure TEST_USER is not set
        os.environ.pop("TEST_USER", None)

        with self.assertRaises(RuntimeError) as ctx:
            CouchDBClientFactory.create_client(
                url="http://localhost:5984",
                user_env="TEST_USER",
                pass_env="TEST_PASS",
            )
        self.assertIn("TEST_USER", str(ctx.exception))

    @patch.dict(os.environ, {"TEST_USER": "admin"}, clear=False)
    def test_create_client_missing_pass_env_raises(self):
        """Test that missing password env var raises RuntimeError."""
        # Ensure TEST_PASS is not set
        os.environ.pop("TEST_PASS", None)

        with self.assertRaises(RuntimeError) as ctx:
            CouchDBClientFactory.create_client(
                url="http://localhost:5984",
                user_env="TEST_USER",
                pass_env="TEST_PASS",
            )
        self.assertIn("TEST_PASS", str(ctx.exception))

    @patch("lib.couchdb.couchdb_connection.cloudant_v1.CloudantV1")
    @patch("lib.couchdb.couchdb_connection.CouchDbSessionAuthenticator")
    @patch.dict(os.environ, {"TEST_USER": "admin", "TEST_PASS": "secret"})
    def test_create_client_connection_failure_raises(self, mock_auth, mock_cloudant):
        """Test that connection failure raises ConnectionError."""
        mock_cloudant.side_effect = Exception("Connection refused")

        with self.assertRaises(ConnectionError) as ctx:
            CouchDBClientFactory.create_client(
                url="http://localhost:5984",
                user_env="TEST_USER",
                pass_env="TEST_PASS",
            )
        self.assertIn("Failed to connect", str(ctx.exception))


class TestCouchDBHandler(unittest.TestCase):
    """Tests for CouchDBHandler."""

    @patch("lib.couchdb.couchdb_connection.CouchDBClientFactory.create_client")
    def test_init_creates_client_and_verifies_db(self, mock_create_client):
        """Test handler initialization creates client and verifies db exists."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client

        handler = CouchDBHandler(
            db_name="test_db",
            url="http://localhost:5984",
            user_env="TEST_USER",
            pass_env="TEST_PASS",
        )

        # Verify factory was called correctly
        mock_create_client.assert_called_once_with(
            url="http://localhost:5984",
            user_env="TEST_USER",
            pass_env="TEST_PASS",
        )

        # Verify database was checked
        mock_client.get_database_information.assert_called_once_with(db="test_db")

        self.assertEqual(handler.db_name, "test_db")
        self.assertEqual(handler.server, mock_client)

    @patch("lib.couchdb.couchdb_connection.CouchDBClientFactory.create_client")
    def test_init_raises_on_missing_db(self, mock_create_client):
        """Test handler raises ConnectionError if database doesn't exist."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_client.get_database_information.side_effect = MockApiException(
            "not found", code=404
        )

        with self.assertRaises(ConnectionError) as ctx:
            CouchDBHandler(
                db_name="nonexistent_db",
                url="http://localhost:5984",
                user_env="TEST_USER",
                pass_env="TEST_PASS",
            )
        self.assertIn("does not exist", str(ctx.exception))

    @patch("lib.couchdb.couchdb_connection.CouchDBClientFactory.create_client")
    def test_fetch_document_by_id_success(self, mock_create_client):
        """Test fetching a document by ID."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_client.get_document.return_value.get_result.return_value = {
            "_id": "doc123",
            "name": "Test Doc",
        }

        handler = CouchDBHandler(
            db_name="test_db",
            url="http://localhost:5984",
            user_env="TEST_USER",
            pass_env="TEST_PASS",
        )

        doc = handler.fetch_document_by_id("doc123")

        mock_client.get_document.assert_called_with(db="test_db", doc_id="doc123")
        self.assertEqual(doc["_id"], "doc123")

    @patch("lib.couchdb.couchdb_connection.CouchDBClientFactory.create_client")
    def test_fetch_document_by_id_not_found(self, mock_create_client):
        """Test fetching a non-existent document returns None."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_client.get_document.side_effect = MockApiException("not found", code=404)

        handler = CouchDBHandler(
            db_name="test_db",
            url="http://localhost:5984",
            user_env="TEST_USER",
            pass_env="TEST_PASS",
        )

        doc = handler.fetch_document_by_id("missing_doc")
        self.assertIsNone(doc)

    @patch("lib.couchdb.couchdb_connection.CouchDBClientFactory.create_client")
    def test_post_changes_success(self, mock_create_client):
        """Test fetching changes from database."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_client.post_changes.return_value.get_result.return_value = {
            "results": [{"id": "doc1", "seq": "1-abc"}],
            "last_seq": "1-abc",
        }

        handler = CouchDBHandler(
            db_name="test_db",
            url="http://localhost:5984",
            user_env="TEST_USER",
            pass_env="TEST_PASS",
        )

        result = handler.post_changes(since="0", include_docs=True, limit=10)

        mock_client.post_changes.assert_called_with(
            db="test_db",
            since="0",
            include_docs=True,
            limit=10,
        )
        self.assertEqual(len(result["results"]), 1)
        self.assertEqual(result["last_seq"], "1-abc")

    @patch("lib.couchdb.couchdb_connection.CouchDBClientFactory.create_client")
    def test_post_changes_normalizes_since_to_string(self, mock_create_client):
        """Test that since parameter is normalized to string."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_client.post_changes.return_value.get_result.return_value = {
            "results": [],
            "last_seq": "5",
        }

        handler = CouchDBHandler(
            db_name="test_db",
            url="http://localhost:5984",
            user_env="TEST_USER",
            pass_env="TEST_PASS",
        )

        # Pass int, should be converted to string
        handler.post_changes(since=5)

        mock_client.post_changes.assert_called_with(
            db="test_db",
            since="5",  # Converted to string
            include_docs=True,
            limit=100,
        )


if __name__ == "__main__":
    unittest.main()
