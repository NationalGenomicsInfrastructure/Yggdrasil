"""
Tests for CouchDB connection utilities.

Tests for:
- CouchDBClientFactory: Stateless factory for creating CloudantV1 clients
- CouchDBHandler: Base class for database operations
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, Mock, patch

import requests
import requests.exceptions


# Create mocks for IBM Cloud SDK classes
class MockApiException(Exception):
    def __init__(self, message="", code=None):
        super().__init__(message)
        self.code = code
        self.status_code = code
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

from lib.couchdb.couchdb_connection import (
    CouchDBClientFactory,
    CouchDBHandler,
    is_transient_doc_fetch_error,
    is_transient_poll_error,
)


class TestCouchDBClientFactory(unittest.TestCase):
    """Tests for CouchDBClientFactory."""

    def setUp(self):
        # Isolate the dedup set between tests
        self._saved_connections = CouchDBClientFactory._logged_connections.copy()
        CouchDBClientFactory._logged_connections.clear()

    def tearDown(self):
        CouchDBClientFactory._logged_connections.clear()
        CouchDBClientFactory._logged_connections.update(self._saved_connections)

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

    @patch("lib.couchdb.couchdb_connection.cloudant_v1.CloudantV1")
    @patch("lib.couchdb.couchdb_connection.CouchDbSessionAuthenticator")
    @patch.dict(os.environ, {"TEST_USER": "admin", "TEST_PASS": "secret"})
    def test_create_client_non_dict_server_info_uses_unknown_version(
        self, mock_auth, mock_cloudant
    ):
        """Test that non-dict server info results in version='unknown'."""
        mock_client = MagicMock()
        # Return a truthy non-dict value so `info or {}` keeps it and isinstance fails
        mock_client.get_server_information.return_value.get_result.return_value = (
            "not-a-dict"
        )
        mock_cloudant.return_value = mock_client

        # Should not raise; just log version="unknown"
        client = CouchDBClientFactory.create_client(
            url="http://localhost:5984",
            user_env="TEST_USER",
            pass_env="TEST_PASS",
        )
        self.assertEqual(client, mock_client)

    @patch("lib.couchdb.couchdb_connection.cloudant_v1.CloudantV1")
    @patch("lib.couchdb.couchdb_connection.CouchDbSessionAuthenticator")
    @patch.dict(os.environ, {"TEST_USER": "admin", "TEST_PASS": "secret"})
    def test_create_client_dedup_logs_debug_on_reconnect(
        self, mock_auth, mock_cloudant
    ):
        """Second connection to same (url, user) logs DEBUG instead of INFO."""
        mock_client = MagicMock()
        mock_client.get_server_information.return_value.get_result.return_value = {
            "version": "3.1.1"
        }
        mock_cloudant.return_value = mock_client

        with self.assertLogs("lib.couchdb.couchdb_connection", level="DEBUG") as cm:
            CouchDBClientFactory.create_client(
                url="http://unique-dedup.example:5984",
                user_env="TEST_USER",
                pass_env="TEST_PASS",
            )
            CouchDBClientFactory.create_client(
                url="http://unique-dedup.example:5984",
                user_env="TEST_USER",
                pass_env="TEST_PASS",
            )

        messages = [r.getMessage() for r in cm.records]
        info_msgs = [m for m in messages if "Connected to CouchDB" in m]
        debug_msgs = [m for m in messages if "Reconnected to CouchDB" in m]
        self.assertEqual(len(info_msgs), 1, "First connect should log INFO once")
        self.assertEqual(len(debug_msgs), 1, "Second connect should log DEBUG once")


class TestCouchDBHandler(unittest.TestCase):
    """Tests for CouchDBHandler."""

    @patch.dict(os.environ, {"TEST_USER": "admin", "TEST_PASS": "secret"})
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

    @patch.dict(os.environ, {"TEST_USER": "admin", "TEST_PASS": "secret"})
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

    @patch.dict(os.environ, {"TEST_USER": "admin", "TEST_PASS": "secret"})
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

    @patch.dict(os.environ, {"TEST_USER": "admin", "TEST_PASS": "secret"})
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

    @patch.dict(os.environ, {"TEST_USER": "admin", "TEST_PASS": "secret"})
    @patch("lib.couchdb.couchdb_connection.CouchDBClientFactory.create_client")
    def test_fetch_document_by_id_non_dict_response_returns_none(
        self, mock_create_client
    ):
        """Test that a non-dict response from get_document returns None."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        # Simulate SDK returning a non-dict (e.g. a string or list)
        mock_client.get_document.return_value.get_result.return_value = "not-a-dict"

        handler = CouchDBHandler(
            db_name="test_db",
            url="http://localhost:5984",
            user_env="TEST_USER",
            pass_env="TEST_PASS",
        )

        doc = handler.fetch_document_by_id("some_doc")
        self.assertIsNone(doc)

    @patch.dict(os.environ, {"TEST_USER": "admin", "TEST_PASS": "secret"})
    @patch("lib.couchdb.couchdb_connection.CouchDBClientFactory.create_client")
    def test_fetch_document_by_id_non_404_api_exception_re_raises(
        self, mock_create_client
    ):
        """Test that a non-404 ApiException from get_document is re-raised."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_client.get_document.side_effect = MockApiException(
            "server error", code=500
        )

        handler = CouchDBHandler(
            db_name="test_db",
            url="http://localhost:5984",
            user_env="TEST_USER",
            pass_env="TEST_PASS",
        )

        with self.assertRaises(MockApiException):
            handler.fetch_document_by_id("some_doc")

    @patch.dict(os.environ, {"TEST_USER": "admin", "TEST_PASS": "secret"})
    @patch("lib.couchdb.couchdb_connection.CouchDBClientFactory.create_client")
    def test_fetch_document_by_id_generic_exception_re_raises(self, mock_create_client):
        """Test that a generic exception from get_document is re-raised."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_client.get_document.side_effect = RuntimeError("unexpected")

        handler = CouchDBHandler(
            db_name="test_db",
            url="http://localhost:5984",
            user_env="TEST_USER",
            pass_env="TEST_PASS",
        )

        with self.assertRaises(RuntimeError):
            handler.fetch_document_by_id("some_doc")

    @patch.dict(os.environ, {"TEST_USER": "admin", "TEST_PASS": "secret"})
    @patch("lib.couchdb.couchdb_connection.CouchDBClientFactory.create_client")
    def test_init_non_404_api_exception_re_raises(self, mock_create_client):
        """Test that a non-404 ApiException during db verification is re-raised."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_client.get_database_information.side_effect = MockApiException(
            "forbidden", code=403
        )

        with self.assertRaises(MockApiException):
            CouchDBHandler(
                db_name="test_db",
                url="http://localhost:5984",
                user_env="TEST_USER",
                pass_env="TEST_PASS",
            )

    @patch.dict(os.environ, {"TEST_USER": "admin", "TEST_PASS": "secret"})
    @patch("lib.couchdb.couchdb_connection.CouchDBClientFactory.create_client")
    def test_find_documents_success(self, mock_create_client):
        """Test a successful Mango query returns the docs list."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_client.post_find.return_value.get_result.return_value = {
            "docs": [{"_id": "a"}, {"_id": "b"}]
        }

        handler = CouchDBHandler(
            db_name="test_db",
            url="http://localhost:5984",
            user_env="TEST_USER",
            pass_env="TEST_PASS",
        )

        docs = handler.find_documents({"status": "ready"})
        self.assertEqual(len(docs), 2)
        self.assertEqual(docs[0]["_id"], "a")
        mock_client.post_find.assert_called_once_with(
            db="test_db", selector={"status": "ready"}, fields=[], limit=200
        )

    @patch.dict(os.environ, {"TEST_USER": "admin", "TEST_PASS": "secret"})
    @patch("lib.couchdb.couchdb_connection.CouchDBClientFactory.create_client")
    def test_find_documents_non_dict_result_returns_empty(self, mock_create_client):
        """Test that a non-dict post_find result returns an empty list."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_client.post_find.return_value.get_result.return_value = "unexpected"

        handler = CouchDBHandler(
            db_name="test_db",
            url="http://localhost:5984",
            user_env="TEST_USER",
            pass_env="TEST_PASS",
        )

        docs = handler.find_documents({"status": "ready"})
        self.assertEqual(docs, [])

    @patch.dict(os.environ, {"TEST_USER": "admin", "TEST_PASS": "secret"})
    @patch("lib.couchdb.couchdb_connection.CouchDBClientFactory.create_client")
    def test_find_documents_api_exception_re_raises(self, mock_create_client):
        """Test that an ApiException from post_find is re-raised."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_client.post_find.side_effect = MockApiException("bad request", code=400)

        handler = CouchDBHandler(
            db_name="test_db",
            url="http://localhost:5984",
            user_env="TEST_USER",
            pass_env="TEST_PASS",
        )

        with self.assertRaises(MockApiException):
            handler.find_documents({"status": "ready"})

    @patch.dict(os.environ, {"TEST_USER": "admin", "TEST_PASS": "secret"})
    @patch("lib.couchdb.couchdb_connection.CouchDBClientFactory.create_client")
    def test_find_documents_generic_exception_re_raises(self, mock_create_client):
        """Test that a generic exception from post_find is re-raised."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_client.post_find.side_effect = RuntimeError("network failure")

        handler = CouchDBHandler(
            db_name="test_db",
            url="http://localhost:5984",
            user_env="TEST_USER",
            pass_env="TEST_PASS",
        )

        with self.assertRaises(RuntimeError):
            handler.find_documents({"status": "ready"})


class TestCouchDBHandlerFetchChangesRaw(unittest.TestCase):
    """Tests for CouchDBHandler.fetch_changes_raw using mocked requests.get."""

    def setUp(self):
        """Create a handler with mocked factory and env vars."""
        with (
            patch(
                "lib.couchdb.couchdb_connection.CouchDBClientFactory.create_client"
            ) as mock_factory,
            patch.dict(os.environ, {"FC_USER": "admin", "FC_PASS": "secret"}),
        ):
            mock_factory.return_value = MagicMock()
            self.handler = CouchDBHandler(
                db_name="test_db",
                url="http://localhost:5984",
                user_env="FC_USER",
                pass_env="FC_PASS",
            )
        # handler._url = "http://localhost:5984", handler._auth = ("admin", "secret")

    def _make_response(self, results=None, last_seq="1-abc", pending=0):
        """Helper: return a mock requests.Response with the given JSON payload."""
        mock_resp = Mock()
        mock_resp.json.return_value = {
            "results": results or [],
            "last_seq": last_seq,
            "pending": pending,
        }
        mock_resp.raise_for_status = Mock()
        return mock_resp

    @patch("lib.couchdb.couchdb_connection.requests.get")
    def test_fetch_changes_raw_basic(self, mock_get):
        """Test basic success: correct URL, params, and returned ChangesBatch."""
        mock_get.return_value = self._make_response(
            results=[
                {"id": "doc1", "seq": "1-abc", "changes": [{"rev": "1-r1"}]},
            ],
            last_seq="1-abc",
            pending=0,
        )

        batch = self.handler.fetch_changes_raw(since="0")

        mock_get.assert_called_once()
        call_kwargs = mock_get.call_args
        self.assertIn("http://localhost:5984/test_db/_changes", call_kwargs[0][0])
        self.assertEqual(call_kwargs[1]["params"]["feed"], "normal")
        self.assertEqual(call_kwargs[1]["params"]["since"], "0")
        self.assertEqual(call_kwargs[1]["params"]["include_docs"], "false")
        self.assertEqual(call_kwargs[1]["auth"], ("admin", "secret"))

        self.assertEqual(len(batch.rows), 1)
        self.assertEqual(batch.rows[0].id, "doc1")
        self.assertEqual(batch.rows[0].rev, "1-r1")
        self.assertEqual(batch.last_seq, "1-abc")
        self.assertEqual(batch.pending, 0)

    @patch("lib.couchdb.couchdb_connection.requests.get")
    def test_fetch_changes_raw_since_none_defaults_to_zero(self, mock_get):
        """Test that since=None sends '0' to CouchDB."""
        mock_get.return_value = self._make_response()

        self.handler.fetch_changes_raw(since=None)

        params = mock_get.call_args[1]["params"]
        self.assertEqual(params["since"], "0")

    @patch("lib.couchdb.couchdb_connection.requests.get")
    def test_fetch_changes_raw_limit_param(self, mock_get):
        """Test that limit is included in params when specified."""
        mock_get.return_value = self._make_response()

        self.handler.fetch_changes_raw(since="0", limit=50)

        params = mock_get.call_args[1]["params"]
        self.assertEqual(params["limit"], 50)

    @patch("lib.couchdb.couchdb_connection.requests.get")
    def test_fetch_changes_raw_no_limit_omits_param(self, mock_get):
        """Test that limit is omitted from params when not specified."""
        mock_get.return_value = self._make_response()

        self.handler.fetch_changes_raw(since="0")

        params = mock_get.call_args[1]["params"]
        self.assertNotIn("limit", params)

    @patch("lib.couchdb.couchdb_connection.requests.get")
    def test_fetch_changes_raw_longpoll_mode(self, mock_get):
        """Test that longpoll feed adds timeout param and uses correct socket timeout."""
        mock_get.return_value = self._make_response()

        self.handler.fetch_changes_raw(since="5", feed="longpoll", timeout_ms=30_000)

        call_kwargs = mock_get.call_args[1]
        params = call_kwargs["params"]
        self.assertEqual(params["feed"], "longpoll")
        self.assertEqual(params["timeout"], 30_000)
        # Socket timeout = 30_000 / 1000 + 5 = 35.0
        self.assertAlmostEqual(call_kwargs["timeout"], 35.0)

    @patch("lib.couchdb.couchdb_connection.requests.get")
    def test_fetch_changes_raw_normal_mode_no_timeout_param(self, mock_get):
        """Test that normal feed does not add the CouchDB timeout param."""
        mock_get.return_value = self._make_response()

        self.handler.fetch_changes_raw(since="0", feed="normal")

        params = mock_get.call_args[1]["params"]
        self.assertNotIn("timeout", params)

    @patch("lib.couchdb.couchdb_connection.requests.get")
    def test_fetch_changes_raw_deleted_row(self, mock_get):
        """Test that deleted=True in a result row is captured correctly."""
        mock_get.return_value = self._make_response(
            results=[
                {
                    "id": "doc-deleted",
                    "seq": "5-xyz",
                    "deleted": True,
                    "changes": [{"rev": "3-r"}],
                }
            ],
            last_seq="5-xyz",
        )

        batch = self.handler.fetch_changes_raw(since="4")

        self.assertEqual(len(batch.rows), 1)
        self.assertTrue(batch.rows[0].deleted)
        self.assertEqual(batch.rows[0].id, "doc-deleted")

    @patch("lib.couchdb.couchdb_connection.requests.get")
    def test_fetch_changes_raw_pending_extracted(self, mock_get):
        """Test that the pending count is extracted from the response."""
        mock_get.return_value = self._make_response(pending=42, last_seq="10-z")

        batch = self.handler.fetch_changes_raw(since="0")

        self.assertEqual(batch.pending, 42)
        self.assertEqual(batch.last_seq, "10-z")

    @patch("lib.couchdb.couchdb_connection.requests.get")
    def test_fetch_changes_raw_row_without_changes_has_none_rev(self, mock_get):
        """Test that a row with no changes list produces rev=None."""
        mock_get.return_value = self._make_response(
            results=[{"id": "doc1", "seq": "1-abc"}],  # no "changes" key
            last_seq="1-abc",
        )

        batch = self.handler.fetch_changes_raw(since="0")

        self.assertIsNone(batch.rows[0].rev)

    @patch("lib.couchdb.couchdb_connection.requests.get")
    def test_fetch_changes_raw_http_error_propagates(self, mock_get):
        """Test that an HTTP error from raise_for_status propagates."""
        mock_resp = Mock()
        mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=Mock(status_code=503)
        )
        mock_get.return_value = mock_resp

        with self.assertRaises(requests.exceptions.HTTPError):
            self.handler.fetch_changes_raw(since="0")


class TestTransientErrorClassifiers(unittest.TestCase):
    """Tests for is_transient_poll_error and is_transient_doc_fetch_error."""

    # --- is_transient_poll_error ---

    def test_poll_error_requests_timeout_is_transient(self):
        self.assertTrue(is_transient_poll_error(requests.exceptions.Timeout()))

    def test_poll_error_requests_connection_error_is_transient(self):
        self.assertTrue(is_transient_poll_error(requests.exceptions.ConnectionError()))

    def test_poll_error_5xx_http_error_is_transient(self):
        resp = Mock()
        resp.status_code = 503
        exc = requests.exceptions.HTTPError(response=resp)
        self.assertTrue(is_transient_poll_error(exc))

    def test_poll_error_4xx_http_error_is_not_transient(self):
        resp = Mock()
        resp.status_code = 404
        exc = requests.exceptions.HTTPError(response=resp)
        self.assertFalse(is_transient_poll_error(exc))

    def test_poll_error_http_error_no_response_is_not_transient(self):
        exc = requests.exceptions.HTTPError(response=None)
        self.assertFalse(is_transient_poll_error(exc))

    def test_poll_error_generic_exception_is_not_transient(self):
        self.assertFalse(is_transient_poll_error(ValueError("unexpected")))

    # --- is_transient_doc_fetch_error ---

    def test_doc_fetch_error_500_is_transient(self):
        exc = MockApiException("server error", code=500)
        self.assertTrue(is_transient_doc_fetch_error(exc))

    def test_doc_fetch_error_503_is_transient(self):
        exc = MockApiException("unavailable", code=503)
        self.assertTrue(is_transient_doc_fetch_error(exc))

    def test_doc_fetch_error_429_is_transient(self):
        exc = MockApiException("rate limited", code=429)
        self.assertTrue(is_transient_doc_fetch_error(exc))

    def test_doc_fetch_error_404_is_not_transient(self):
        exc = MockApiException("not found", code=404)
        self.assertFalse(is_transient_doc_fetch_error(exc))

    def test_doc_fetch_error_400_is_not_transient(self):
        exc = MockApiException("bad request", code=400)
        self.assertFalse(is_transient_doc_fetch_error(exc))

    def test_doc_fetch_error_requests_timeout_is_transient(self):
        self.assertTrue(is_transient_doc_fetch_error(requests.exceptions.Timeout()))

    def test_doc_fetch_error_requests_connection_error_is_transient(self):
        self.assertTrue(
            is_transient_doc_fetch_error(requests.exceptions.ConnectionError())
        )

    def test_doc_fetch_error_generic_exception_is_not_transient(self):
        self.assertFalse(is_transient_doc_fetch_error(RuntimeError("unexpected")))


if __name__ == "__main__":
    unittest.main()
