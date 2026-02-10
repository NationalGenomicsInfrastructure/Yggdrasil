import os
import sys
import unittest
from unittest.mock import MagicMock, call, patch


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

# Also make the ApiException available in the connection module
import lib.couchdb.couchdb_connection

# Import the modules AFTER setting up the mocks
from lib.core_utils.singleton_decorator import SingletonMeta
from lib.couchdb.couchdb_connection import CouchDBConnectionManager

lib.couchdb.couchdb_connection.ApiException = MockApiException


class TestCouchDBConnectionManager(unittest.TestCase):
    def setUp(self):
        # Clear singleton instances to ensure test isolation
        if CouchDBConnectionManager in SingletonMeta._instances:
            del SingletonMeta._instances[CouchDBConnectionManager]

        # Common configuration that will be returned by ConfigLoader.load_config
        self.mock_config = {
            "couchdb": {
                "url": "localhost:5984",
                "default_user": "admin",
                "default_password": "secret",
            }
        }

    def tearDown(self):
        # Clear singleton instances after each test
        if CouchDBConnectionManager in SingletonMeta._instances:
            del SingletonMeta._instances[CouchDBConnectionManager]

    @patch("lib.couchdb.couchdb_connection.ConfigLoader")
    @patch("lib.couchdb.couchdb_connection.os.getenv")
    @patch("lib.couchdb.couchdb_connection.cloudant_v1.CloudantV1")
    @patch("lib.couchdb.couchdb_connection.CouchDbSessionAuthenticator")
    def test_initialization_with_defaults(
        self, mock_auth, mock_cloudant, mock_getenv, mock_config_loader
    ):
        # Mock configuration loading
        mock_config_loader.return_value.load_config.return_value = {
            "couchdb": {
                "url": "localhost:5984",
                "default_user": "admin",
                "default_password": "secret",
            }
        }

        # Configure getenv mock to return default values
        def getenv_side_effect(key, default=None):
            return default

        mock_getenv.side_effect = getenv_side_effect

        # Mock a successful server connection
        mock_server = MagicMock()
        mock_server.get_server_information.return_value.get_result.return_value = {
            "version": "3.1.1"
        }
        mock_cloudant.return_value = mock_server

        manager = CouchDBConnectionManager()

        # Verify that the manager used config defaults
        self.assertEqual(manager.db_url, "http://localhost:5984")
        self.assertEqual(manager.db_user, "admin")
        self.assertEqual(manager.db_password, "secret")

        # Verify that server is connected
        self.assertIsNotNone(manager.server)
        mock_server.get_server_information.assert_called_once()

    @patch("lib.couchdb.couchdb_connection.cloudant_v1.CloudantV1")
    @patch("lib.couchdb.couchdb_connection.CouchDbSessionAuthenticator")
    def test_singleton_returns_same_instance(self, mock_auth, mock_cloudant):
        # First instantiation
        mock_server = MagicMock()
        mock_server.get_server_information.return_value.get_result.return_value = {
            "version": "3.1.1"
        }
        mock_cloudant.return_value = mock_server

        manager1 = CouchDBConnectionManager()
        manager2 = CouchDBConnectionManager()  # Same instance since it's a singleton

        self.assertIs(manager1, manager2)

    @patch("lib.couchdb.couchdb_connection.cloudant_v1.CloudantV1")
    @patch("lib.couchdb.couchdb_connection.CouchDbSessionAuthenticator")
    def test_connect_server_failure(self, mock_auth, mock_cloudant):
        # Simulate connection failure
        mock_cloudant.side_effect = Exception("Connection failed")

        with self.assertRaises(ConnectionError) as cm:
            CouchDBConnectionManager()
        self.assertEqual(str(cm.exception), "Failed to connect to CouchDB server")

    @patch(
        "lib.couchdb.couchdb_connection.ConfigLoader.load_config",
        return_value={
            "couchdb": {
                "url": "localhost:5984",
                "default_user": "admin",
                "default_password": "secret",
            }
        },
    )
    @patch("lib.couchdb.couchdb_connection.os.getenv")
    @patch("lib.couchdb.couchdb_connection.cloudant_v1.CloudantV1")
    @patch("lib.couchdb.couchdb_connection.CouchDbSessionAuthenticator")
    def test_ensure_db_success(
        self, mock_auth, mock_cloudant, mock_getenv, mock_load_config
    ):
        """Test successful database verification with ensure_db."""
        # Configure getenv mock to return default values (handle 1 or 2 args)
        mock_getenv.side_effect = lambda key, default=None: default
        mock_server = MagicMock()
        mock_server.get_server_information.return_value.get_result.return_value = {
            "version": "3.1.1"
        }
        mock_server.get_database_information.return_value = {"db_name": "testdb"}
        mock_cloudant.return_value = mock_server

        manager = CouchDBConnectionManager()
        result = manager.ensure_db("testdb")

        self.assertEqual(result, "testdb")
        mock_server.get_database_information.assert_called_once_with(db="testdb")

    @patch("lib.couchdb.couchdb_connection.ConfigLoader")
    @patch("lib.couchdb.couchdb_connection.os.getenv")
    @patch("lib.couchdb.couchdb_connection.cloudant_v1.CloudantV1")
    @patch("lib.couchdb.couchdb_connection.CouchDbSessionAuthenticator")
    def test_ensure_db_not_found(
        self, mock_auth, mock_cloudant, mock_getenv, mock_config_loader
    ):
        """Test ensure_db when database does not exist (404 error)."""
        # Mock configuration loading
        mock_config_loader.return_value.load_config.return_value = {
            "couchdb": {
                "url": "localhost:5984",
                "default_user": "admin",
                "default_password": "secret",
            }
        }

        # Configure getenv mock to return default values
        def getenv_side_effect(key, default=None):
            return default

        mock_getenv.side_effect = getenv_side_effect

        # Create a proper ApiException mock
        api_exception = MockApiException("Not Found", code=404)

        mock_server = MagicMock()
        mock_server.get_server_information.return_value.get_result.return_value = {
            "version": "3.1.1"
        }
        # Simulate database not found (404)
        mock_server.get_database_information.side_effect = api_exception
        mock_cloudant.return_value = mock_server

        manager = CouchDBConnectionManager()

        with self.assertRaises(ConnectionError) as cm:
            manager.ensure_db("missingdb")

        self.assertEqual(str(cm.exception), "Database missingdb does not exist")
        mock_server.get_database_information.assert_called_once_with(db="missingdb")

    @patch("lib.couchdb.couchdb_connection.ConfigLoader")
    @patch("lib.couchdb.couchdb_connection.os.getenv")
    @patch("lib.couchdb.couchdb_connection.cloudant_v1.CloudantV1")
    @patch("lib.couchdb.couchdb_connection.CouchDbSessionAuthenticator")
    def test_ensure_db_unexpected_error(
        self, mock_auth, mock_cloudant, mock_getenv, mock_config_loader
    ):
        """Test ensure_db when an unexpected API error occurs."""
        # Mock configuration loading
        mock_config_loader.return_value.load_config.return_value = {
            "couchdb": {
                "url": "localhost:5984",
                "default_user": "admin",
                "default_password": "secret",
            }
        }

        # Configure getenv mock to return default values
        def getenv_side_effect(key, default=None):
            return default

        mock_getenv.side_effect = getenv_side_effect

        # Create a proper ApiException mock with different error code
        api_exception = MockApiException("Internal Server Error", code=500)

        mock_server = MagicMock()
        mock_server.get_server_information.return_value.get_result.return_value = {
            "version": "3.1.1"
        }
        # Simulate unexpected API error (500)
        mock_server.get_database_information.side_effect = api_exception
        mock_cloudant.return_value = mock_server

        manager = CouchDBConnectionManager()

        with self.assertRaises(ConnectionError) as cm:
            manager.ensure_db("errordb")

        self.assertEqual(str(cm.exception), "Database errordb does not exist")
        mock_server.get_database_information.assert_called_once_with(db="errordb")

    @patch(
        "lib.couchdb.couchdb_connection.ConfigLoader.load_config",
        return_value={
            "couchdb": {
                "url": "localhost:5984",
                "default_user": "admin",
                "default_password": "secret",
            }
        },
    )
    @patch("lib.couchdb.couchdb_connection.os.getenv")
    @patch("lib.couchdb.couchdb_connection.cloudant_v1.CloudantV1")
    @patch("lib.couchdb.couchdb_connection.CouchDbSessionAuthenticator")
    def test_ensure_db_no_server_connection(
        self, mock_auth, mock_cloudant, mock_getenv, mock_load_config
    ):
        """Test ensure_db when server is not connected."""
        mock_getenv.side_effect = lambda key, default=None: default
        mock_server = MagicMock()
        mock_server.get_server_information.return_value.get_result.return_value = {
            "version": "3.1.1"
        }
        mock_cloudant.return_value = mock_server

        manager = CouchDBConnectionManager()
        # Simulate server disconnection
        manager.server = None

        with self.assertRaises(ConnectionError) as cm:
            manager.ensure_db("testdb")

        self.assertEqual(str(cm.exception), "Server not connected")

    @patch(
        "lib.couchdb.couchdb_connection.ConfigLoader.load_config",
        return_value={
            "couchdb": {
                "url": "localhost:5984",
                "default_user": "admin",
                "default_password": "secret",
            }
        },
    )
    @patch("lib.couchdb.couchdb_connection.os.getenv")
    @patch("lib.couchdb.couchdb_connection.cloudant_v1.CloudantV1")
    @patch("lib.couchdb.couchdb_connection.CouchDbSessionAuthenticator")
    def test_ensure_db_multiple_calls_same_database(
        self, mock_auth, mock_cloudant, mock_getenv, mock_load_config
    ):
        """Test ensure_db called multiple times with the same database name."""
        mock_getenv.side_effect = lambda key, default=None: default
        mock_server = MagicMock()
        mock_server.get_server_information.return_value.get_result.return_value = {
            "version": "3.1.1"
        }
        mock_server.get_database_information.return_value = {"db_name": "testdb"}
        mock_cloudant.return_value = mock_server

        manager = CouchDBConnectionManager()

        # Call ensure_db multiple times
        result1 = manager.ensure_db("testdb")
        result2 = manager.ensure_db("testdb")
        result3 = manager.ensure_db("testdb")

        # All calls should return the same result
        self.assertEqual(result1, "testdb")
        self.assertEqual(result2, "testdb")
        self.assertEqual(result3, "testdb")

        # Verify the API was called each time (no caching)
        self.assertEqual(mock_server.get_database_information.call_count, 3)

    @patch(
        "lib.couchdb.couchdb_connection.ConfigLoader.load_config",
        return_value={
            "couchdb": {
                "url": "localhost:5984",
                "default_user": "admin",
                "default_password": "secret",
            }
        },
    )
    @patch("lib.couchdb.couchdb_connection.os.getenv")
    @patch("lib.couchdb.couchdb_connection.cloudant_v1.CloudantV1")
    @patch("lib.couchdb.couchdb_connection.CouchDbSessionAuthenticator")
    def test_ensure_db_different_databases(
        self, mock_auth, mock_cloudant, mock_getenv, mock_load_config
    ):
        """Test ensure_db with different database names."""
        mock_getenv.side_effect = lambda key, default=None: default
        mock_server = MagicMock()
        mock_server.get_server_information.return_value.get_result.return_value = {
            "version": "3.1.1"
        }
        mock_server.get_database_information.return_value = {"db_name": "dummy"}
        mock_cloudant.return_value = mock_server

        manager = CouchDBConnectionManager()

        # Test with different database names
        result1 = manager.ensure_db("db1")
        result2 = manager.ensure_db("db2")
        result3 = manager.ensure_db("db3")

        self.assertEqual(result1, "db1")
        self.assertEqual(result2, "db2")
        self.assertEqual(result3, "db3")

        # Verify each database was checked
        expected_calls = [
            call(db="db1"),
            call(db="db2"),
            call(db="db3"),
        ]
        mock_server.get_database_information.assert_has_calls(expected_calls)

    @patch("lib.couchdb.couchdb_connection.ConfigLoader")
    @patch("lib.couchdb.couchdb_connection.os.getenv")
    @patch("lib.couchdb.couchdb_connection.cloudant_v1.CloudantV1")
    @patch("lib.couchdb.couchdb_connection.CouchDbSessionAuthenticator")
    def test_ensure_db_special_database_names(
        self, mock_auth, mock_cloudant, mock_getenv, mock_config_loader
    ):
        """Test ensure_db with special database names (edge cases)."""
        # Mock configuration loading
        mock_config_loader.return_value.load_config.return_value = {
            "couchdb": {
                "url": "localhost:5984",
                "default_user": "admin",
                "default_password": "secret",
            }
        }

        # Configure getenv mock to return default values
        # Ensure TEST_RUN_PIPE is available if the adapter expects it
        def getenv_side_effect(key, default=None):
            if key == "TEST_RUN_PIPE":
                # Return a dummy value if not already set, to satisfy adapter expectations
                return os.environ.get(key, "/dev/null")
            return default

        mock_getenv.side_effect = getenv_side_effect

        mock_server = MagicMock()
        mock_server.get_server_information.return_value.get_result.return_value = {
            "version": "3.1.1"
        }
        mock_server.get_database_information.return_value = {"db_name": "dummy"}
        mock_cloudant.return_value = mock_server

        manager = CouchDBConnectionManager()

        # Test with various special database names
        special_names = [
            "test-db",  # hyphen
            "test_db",  # underscore
            "123db",  # starts with number
            "db123",  # ends with number
            "a",  # single character
            "very_long_database_name_with_many_characters_123",  # long name
        ]

        # Test each special database name with subTest for clear per-case reporting
        # Note: We ensure os.getenv is properly mocked to avoid adapter conflicts
        for db_name in special_names:
            with self.subTest(db_name=db_name):
                self.assertEqual(manager.ensure_db(db_name), db_name)


class TestCouchDBHandler(unittest.TestCase):
    """
    Comprehensive tests for CouchDBHandler class.
    Tests initialization, fetch_document_by_id, and error handling.
    """

    def setUp(self):
        """Set up test fixtures and clear singleton instances for test isolation."""
        # Clear singleton instances to ensure test isolation

        if CouchDBConnectionManager in SingletonMeta._instances:
            del SingletonMeta._instances[CouchDBConnectionManager]

        # Mock document data
        self.mock_doc = {
            "_id": "doc123",
            "_rev": "1-abc",
            "project_id": "P12345",
            "name": "Test Project",
            "status": "active",
        }

    def tearDown(self):
        """Clean up singleton instances after each test."""
        if CouchDBConnectionManager in SingletonMeta._instances:
            del SingletonMeta._instances[CouchDBConnectionManager]

    @patch("lib.couchdb.couchdb_connection.ConfigLoader")
    @patch("lib.couchdb.couchdb_connection.os.getenv")
    @patch("lib.couchdb.couchdb_connection.cloudant_v1.CloudantV1")
    @patch("lib.couchdb.couchdb_connection.CouchDbSessionAuthenticator")
    def test_handler_initialization_success(
        self, mock_auth, mock_cloudant, mock_getenv, mock_config_loader
    ):
        """Test successful initialization of CouchDBHandler."""
        # Mock configuration
        mock_config_loader.return_value.load_config.return_value = {
            "couchdb": {
                "url": "localhost:5984",
                "default_user": "admin",
                "default_password": "secret",
            }
        }
        mock_getenv.side_effect = lambda key, default=None: default

        # Mock server and database check
        mock_server = MagicMock()
        mock_server.get_server_information.return_value.get_result.return_value = {
            "version": "3.1.1"
        }
        mock_server.get_database_information.return_value = {"db_name": "testdb"}
        mock_cloudant.return_value = mock_server

        # Import here to use mocked modules
        from lib.couchdb.couchdb_connection import CouchDBHandler

        handler = CouchDBHandler("testdb")

        # Verify handler was initialized correctly
        self.assertEqual(handler.db_name, "testdb")
        self.assertIsNotNone(handler.server)
        self.assertIsNotNone(handler.connection_manager)
        mock_server.get_database_information.assert_called_once_with(db="testdb")

    @patch("lib.couchdb.couchdb_connection.ConfigLoader")
    @patch("lib.couchdb.couchdb_connection.os.getenv")
    @patch("lib.couchdb.couchdb_connection.cloudant_v1.CloudantV1")
    @patch("lib.couchdb.couchdb_connection.CouchDbSessionAuthenticator")
    def test_handler_initialization_database_not_found(
        self, mock_auth, mock_cloudant, mock_getenv, mock_config_loader
    ):
        """Test CouchDBHandler initialization when database doesn't exist."""
        # Mock configuration
        mock_config_loader.return_value.load_config.return_value = {
            "couchdb": {
                "url": "localhost:5984",
                "default_user": "admin",
                "default_password": "secret",
            }
        }
        mock_getenv.side_effect = lambda key, default=None: default

        # Mock server with database not found
        mock_server = MagicMock()
        mock_server.get_server_information.return_value.get_result.return_value = {
            "version": "3.1.1"
        }
        api_exception = MockApiException("Database not found", code=404)
        mock_server.get_database_information.side_effect = api_exception
        mock_cloudant.return_value = mock_server

        # Import here to use mocked modules
        from lib.couchdb.couchdb_connection import CouchDBHandler

        # Should raise ConnectionError due to database not existing
        with self.assertRaises(ConnectionError) as cm:
            CouchDBHandler("missing_db")

        self.assertIn("missing_db does not exist", str(cm.exception))

    @patch("lib.couchdb.couchdb_connection.ConfigLoader")
    @patch("lib.couchdb.couchdb_connection.os.getenv")
    @patch("lib.couchdb.couchdb_connection.cloudant_v1.CloudantV1")
    @patch("lib.couchdb.couchdb_connection.CouchDbSessionAuthenticator")
    def test_fetch_document_by_id_success(
        self, mock_auth, mock_cloudant, mock_getenv, mock_config_loader
    ):
        """Test successful document fetch by ID."""
        # Mock configuration
        mock_config_loader.return_value.load_config.return_value = {
            "couchdb": {
                "url": "localhost:5984",
                "default_user": "admin",
                "default_password": "secret",
            }
        }
        mock_getenv.side_effect = lambda key, default=None: default

        # Mock server
        mock_server = MagicMock()
        mock_server.get_server_information.return_value.get_result.return_value = {
            "version": "3.1.1"
        }
        mock_server.get_database_information.return_value = {"db_name": "testdb"}
        mock_server.get_document.return_value.get_result.return_value = self.mock_doc
        mock_cloudant.return_value = mock_server

        # Import and create handler
        from lib.couchdb.couchdb_connection import CouchDBHandler

        handler = CouchDBHandler("testdb")

        # Fetch document
        result = handler.fetch_document_by_id("doc123")

        # Verify result
        self.assertIsNotNone(result)
        assert isinstance(result, dict)  # Type narrowing for Pylance
        self.assertEqual(result, self.mock_doc)
        self.assertEqual(result["_id"], "doc123")
        self.assertEqual(result["project_id"], "P12345")
        mock_server.get_document.assert_called_once_with(db="testdb", doc_id="doc123")

    @patch("lib.couchdb.couchdb_connection.ConfigLoader")
    @patch("lib.couchdb.couchdb_connection.os.getenv")
    @patch("lib.couchdb.couchdb_connection.cloudant_v1.CloudantV1")
    @patch("lib.couchdb.couchdb_connection.CouchDbSessionAuthenticator")
    @patch("lib.couchdb.couchdb_connection.logging")
    def test_fetch_document_by_id_not_found(
        self, mock_logging, mock_auth, mock_cloudant, mock_getenv, mock_config_loader
    ):
        """Test fetch_document_by_id when document doesn't exist (404)."""
        # Mock configuration
        mock_config_loader.return_value.load_config.return_value = {
            "couchdb": {
                "url": "localhost:5984",
                "default_user": "admin",
                "default_password": "secret",
            }
        }
        mock_getenv.side_effect = lambda key, default=None: default

        # Mock server with document not found
        mock_server = MagicMock()
        mock_server.get_server_information.return_value.get_result.return_value = {
            "version": "3.1.1"
        }
        mock_server.get_database_information.return_value = {"db_name": "testdb"}
        api_exception = MockApiException("Document not found", code=404)
        mock_server.get_document.side_effect = api_exception
        mock_cloudant.return_value = mock_server

        # Import and create handler
        from lib.couchdb.couchdb_connection import CouchDBHandler

        handler = CouchDBHandler("testdb")

        # Fetch non-existent document
        result = handler.fetch_document_by_id("nonexistent")

        # Verify result is None and debug was logged (404 is normal, not an error)
        self.assertIsNone(result)
        mock_server.get_document.assert_called_once_with(
            db="testdb", doc_id="nonexistent"
        )
        # Verify debug logging (not error - 404 is expected behavior)
        mock_logging.debug.assert_called()

    @patch("lib.couchdb.couchdb_connection.ConfigLoader")
    @patch("lib.couchdb.couchdb_connection.os.getenv")
    @patch("lib.couchdb.couchdb_connection.cloudant_v1.CloudantV1")
    @patch("lib.couchdb.couchdb_connection.CouchDbSessionAuthenticator")
    @patch("lib.couchdb.couchdb_connection.logging")
    def test_fetch_document_by_id_api_error(
        self, mock_logging, mock_auth, mock_cloudant, mock_getenv, mock_config_loader
    ):
        """Test fetch_document_by_id with non-404 API error."""
        # Mock configuration
        mock_config_loader.return_value.load_config.return_value = {
            "couchdb": {
                "url": "localhost:5984",
                "default_user": "admin",
                "default_password": "secret",
            }
        }
        mock_getenv.side_effect = lambda key, default=None: default

        # Mock server with API error
        mock_server = MagicMock()
        mock_server.get_server_information.return_value.get_result.return_value = {
            "version": "3.1.1"
        }
        mock_server.get_database_information.return_value = {"db_name": "testdb"}
        api_exception = MockApiException("Unauthorized", code=401)
        mock_server.get_document.side_effect = api_exception
        mock_cloudant.return_value = mock_server

        # Import and create handler
        from lib.couchdb.couchdb_connection import CouchDBHandler

        handler = CouchDBHandler("testdb")

        # Fetch document with API error
        result = handler.fetch_document_by_id("doc123")

        # Verify result is None and error was logged
        self.assertIsNone(result)
        mock_logging.error.assert_called()

    @patch("lib.couchdb.couchdb_connection.ConfigLoader")
    @patch("lib.couchdb.couchdb_connection.os.getenv")
    @patch("lib.couchdb.couchdb_connection.cloudant_v1.CloudantV1")
    @patch("lib.couchdb.couchdb_connection.CouchDbSessionAuthenticator")
    @patch("lib.couchdb.couchdb_connection.logging")
    def test_fetch_document_by_id_general_exception(
        self, mock_logging, mock_auth, mock_cloudant, mock_getenv, mock_config_loader
    ):
        """Test fetch_document_by_id with general exception."""
        # Mock configuration
        mock_config_loader.return_value.load_config.return_value = {
            "couchdb": {
                "url": "localhost:5984",
                "default_user": "admin",
                "default_password": "secret",
            }
        }
        mock_getenv.side_effect = lambda key, default=None: default

        # Mock server with general exception
        mock_server = MagicMock()
        mock_server.get_server_information.return_value.get_result.return_value = {
            "version": "3.1.1"
        }
        mock_server.get_database_information.return_value = {"db_name": "testdb"}
        mock_server.get_document.side_effect = Exception("Network error")
        mock_cloudant.return_value = mock_server

        # Import and create handler
        from lib.couchdb.couchdb_connection import CouchDBHandler

        handler = CouchDBHandler("testdb")

        # Fetch document with exception
        result = handler.fetch_document_by_id("doc123")

        # Verify result is None and error was logged
        self.assertIsNone(result)
        mock_logging.error.assert_called()

    @patch("lib.couchdb.couchdb_connection.ConfigLoader")
    @patch("lib.couchdb.couchdb_connection.os.getenv")
    @patch("lib.couchdb.couchdb_connection.cloudant_v1.CloudantV1")
    @patch("lib.couchdb.couchdb_connection.CouchDbSessionAuthenticator")
    @patch("lib.couchdb.couchdb_connection.logging")
    def test_fetch_document_by_id_non_dict_response(
        self, mock_logging, mock_auth, mock_cloudant, mock_getenv, mock_config_loader
    ):
        """Test fetch_document_by_id with non-dict response."""
        # Mock configuration
        mock_config_loader.return_value.load_config.return_value = {
            "couchdb": {
                "url": "localhost:5984",
                "default_user": "admin",
                "default_password": "secret",
            }
        }
        mock_getenv.side_effect = lambda key, default=None: default

        # Mock server with non-dict response
        mock_server = MagicMock()
        mock_server.get_server_information.return_value.get_result.return_value = {
            "version": "3.1.1"
        }
        mock_server.get_database_information.return_value = {"db_name": "testdb"}
        # Return a string instead of dict
        mock_server.get_document.return_value.get_result.return_value = "not a dict"
        mock_cloudant.return_value = mock_server

        # Import and create handler
        from lib.couchdb.couchdb_connection import CouchDBHandler

        handler = CouchDBHandler("testdb")

        # Fetch document with non-dict response
        result = handler.fetch_document_by_id("doc123")

        # Verify result is None and warning was logged
        self.assertIsNone(result)
        mock_logging.warning.assert_called()

    @patch("lib.couchdb.couchdb_connection.ConfigLoader")
    @patch("lib.couchdb.couchdb_connection.os.getenv")
    @patch("lib.couchdb.couchdb_connection.cloudant_v1.CloudantV1")
    @patch("lib.couchdb.couchdb_connection.CouchDbSessionAuthenticator")
    def test_fetch_document_by_id_multiple_documents(
        self, mock_auth, mock_cloudant, mock_getenv, mock_config_loader
    ):
        """Test fetching multiple different documents."""
        # Mock configuration
        mock_config_loader.return_value.load_config.return_value = {
            "couchdb": {
                "url": "localhost:5984",
                "default_user": "admin",
                "default_password": "secret",
            }
        }
        mock_getenv.side_effect = lambda key, default=None: default

        # Mock server
        mock_server = MagicMock()
        mock_server.get_server_information.return_value.get_result.return_value = {
            "version": "3.1.1"
        }
        mock_server.get_database_information.return_value = {"db_name": "testdb"}

        # Setup different responses for different doc IDs
        doc1 = {"_id": "doc1", "name": "Document 1"}
        doc2 = {"_id": "doc2", "name": "Document 2"}
        doc3 = {"_id": "doc3", "name": "Document 3"}

        def get_document_side_effect(db, doc_id):
            mock_result = MagicMock()
            if doc_id == "doc1":
                mock_result.get_result.return_value = doc1
            elif doc_id == "doc2":
                mock_result.get_result.return_value = doc2
            elif doc_id == "doc3":
                mock_result.get_result.return_value = doc3
            return mock_result

        mock_server.get_document.side_effect = get_document_side_effect
        mock_cloudant.return_value = mock_server

        # Import and create handler
        from lib.couchdb.couchdb_connection import CouchDBHandler

        handler = CouchDBHandler("testdb")

        # Fetch multiple documents
        result1 = handler.fetch_document_by_id("doc1")
        result2 = handler.fetch_document_by_id("doc2")
        result3 = handler.fetch_document_by_id("doc3")

        # Verify results
        self.assertEqual(result1, doc1)
        self.assertEqual(result2, doc2)
        self.assertEqual(result3, doc3)
        self.assertEqual(mock_server.get_document.call_count, 3)

    @patch("lib.couchdb.couchdb_connection.ConfigLoader")
    @patch("lib.couchdb.couchdb_connection.os.getenv")
    @patch("lib.couchdb.couchdb_connection.cloudant_v1.CloudantV1")
    @patch("lib.couchdb.couchdb_connection.CouchDbSessionAuthenticator")
    def test_fetch_document_by_id_special_characters_in_id(
        self, mock_auth, mock_cloudant, mock_getenv, mock_config_loader
    ):
        """Test fetching document with special characters in ID."""
        # Mock configuration
        mock_config_loader.return_value.load_config.return_value = {
            "couchdb": {
                "url": "localhost:5984",
                "default_user": "admin",
                "default_password": "secret",
            }
        }
        mock_getenv.side_effect = lambda key, default=None: default

        # Mock server
        mock_server = MagicMock()
        mock_server.get_server_information.return_value.get_result.return_value = {
            "version": "3.1.1"
        }
        mock_server.get_database_information.return_value = {"db_name": "testdb"}

        # Test various special character IDs
        special_ids = [
            "doc-with-hyphens",
            "doc_with_underscores",
            "doc.with.dots",
            "doc:with:colons",
            "doc/with/slashes",
            "doc@with@at",
        ]

        for doc_id in special_ids:
            mock_doc = {"_id": doc_id, "data": "test"}
            mock_server.get_document.return_value.get_result.return_value = mock_doc
            mock_cloudant.return_value = mock_server

            # Import and create handler
            from lib.couchdb.couchdb_connection import CouchDBHandler

            handler = CouchDBHandler("testdb")

            # Fetch document with special ID
            result = handler.fetch_document_by_id(doc_id)

            # Verify result
            self.assertIsNotNone(result)
            assert isinstance(result, dict)  # Type narrowing for Pylance
            self.assertEqual(result["_id"], doc_id)

    @patch("lib.couchdb.couchdb_connection.ConfigLoader")
    @patch("lib.couchdb.couchdb_connection.os.getenv")
    @patch("lib.couchdb.couchdb_connection.cloudant_v1.CloudantV1")
    @patch("lib.couchdb.couchdb_connection.CouchDbSessionAuthenticator")
    def test_fetch_document_by_id_with_complex_document(
        self, mock_auth, mock_cloudant, mock_getenv, mock_config_loader
    ):
        """Test fetching document with complex nested structure."""
        # Mock configuration
        mock_config_loader.return_value.load_config.return_value = {
            "couchdb": {
                "url": "localhost:5984",
                "default_user": "admin",
                "default_password": "secret",
            }
        }
        mock_getenv.side_effect = lambda key, default=None: default

        # Mock server with complex document
        complex_doc = {
            "_id": "complex_doc",
            "_rev": "2-xyz",
            "project_id": "P12345",
            "details": {
                "name": "Complex Project",
                "metadata": {
                    "created": "2025-01-01",
                    "modified": "2025-11-21",
                },
            },
            "samples": [
                {"id": "S001", "status": "pending"},
                {"id": "S002", "status": "completed"},
            ],
            "user_info": {
                "owner": {"name": "John Doe", "email": "john@example.com"},
                "pi": {"name": "Jane Smith", "email": "jane@example.com"},
            },
        }

        mock_server = MagicMock()
        mock_server.get_server_information.return_value.get_result.return_value = {
            "version": "3.1.1"
        }
        mock_server.get_database_information.return_value = {"db_name": "testdb"}
        mock_server.get_document.return_value.get_result.return_value = complex_doc
        mock_cloudant.return_value = mock_server

        # Import and create handler
        from lib.couchdb.couchdb_connection import CouchDBHandler

        handler = CouchDBHandler("testdb")

        # Fetch complex document
        result = handler.fetch_document_by_id("complex_doc")

        # Verify all nested structure is preserved
        self.assertIsNotNone(result)
        assert isinstance(result, dict)  # Type narrowing for Pylance
        self.assertEqual(result["_id"], "complex_doc")
        self.assertEqual(result["details"]["name"], "Complex Project")
        self.assertEqual(len(result["samples"]), 2)
        self.assertEqual(result["user_info"]["owner"]["name"], "John Doe")

    @patch("lib.couchdb.couchdb_connection.ConfigLoader")
    @patch("lib.couchdb.couchdb_connection.os.getenv")
    @patch("lib.couchdb.couchdb_connection.cloudant_v1.CloudantV1")
    @patch("lib.couchdb.couchdb_connection.CouchDbSessionAuthenticator")
    def test_fetch_document_by_id_empty_document(
        self, mock_auth, mock_cloudant, mock_getenv, mock_config_loader
    ):
        """Test fetching document that is an empty dict."""
        # Mock configuration
        mock_config_loader.return_value.load_config.return_value = {
            "couchdb": {
                "url": "localhost:5984",
                "default_user": "admin",
                "default_password": "secret",
            }
        }
        mock_getenv.side_effect = lambda key, default=None: default

        # Mock server with empty document
        mock_server = MagicMock()
        mock_server.get_server_information.return_value.get_result.return_value = {
            "version": "3.1.1"
        }
        mock_server.get_database_information.return_value = {"db_name": "testdb"}
        mock_server.get_document.return_value.get_result.return_value = {}
        mock_cloudant.return_value = mock_server

        # Import and create handler
        from lib.couchdb.couchdb_connection import CouchDBHandler

        handler = CouchDBHandler("testdb")

        # Fetch empty document
        result = handler.fetch_document_by_id("empty_doc")

        # Verify result is empty dict (still valid)
        self.assertEqual(result, {})
        self.assertIsInstance(result, dict)


if __name__ == "__main__":
    unittest.main()
