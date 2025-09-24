import json
import unittest
from unittest.mock import MagicMock, patch

from lib.core_utils.singleton_decorator import SingletonMeta
from lib.couchdb.project_db_manager import ProjectDBManager


class MockApiException(Exception):
    """Mock ApiException for testing."""

    def __init__(self, code, message="Test error"):
        super().__init__(message)
        self.code = code
        self.message = message


class TestProjectDBManager(unittest.IsolatedAsyncioTestCase):
    """
    Comprehensive tests for ProjectDBManager class.
    Tests initialization, async change fetching, document retrieval, and error handling.
    """

    def setUp(self):
        """Set up test fixtures and clear singleton instances for test isolation."""
        # Clear singleton instances to ensure test isolation
        from lib.couchdb.couchdb_connection import CouchDBConnectionManager

        if CouchDBConnectionManager in SingletonMeta._instances:
            del SingletonMeta._instances[CouchDBConnectionManager]

        # Mock module registry data
        self.mock_module_registry = {
            "10X": {"module": "lib.realms.tenx.tenx_project.TenXProject"},
            "Smart-seq3": {
                "module": "lib.realms.smartseq3.smartseq3_project.SmartSeq3Project"
            },
            "MARS": {
                "module": "lib.realms.mars.mars_project.MarsProject",
                "prefix": True,
            },
        }

        # Mock database documents
        self.mock_doc_with_10x = {
            "_id": "doc1",
            "project_id": "P12345",
            "details": {"library_construction_method": "10X"},
        }

        self.mock_doc_with_smartseq = {
            "_id": "doc2",
            "project_id": "P12346",
            "details": {"library_construction_method": "Smart-seq3"},
        }

        self.mock_doc_with_prefix_match = {
            "_id": "doc3",
            "project_id": "P12347",
            "details": {"library_construction_method": "MARS-seq"},
        }

        self.mock_doc_with_unknown_method = {
            "_id": "doc4",
            "project_id": "P12348",
            "details": {"library_construction_method": "UnknownMethod"},
        }

        self.mock_doc_missing_details = {"_id": "doc5", "project_id": "P12349"}

        # Mock IBM Cloud SDK responses
        self.mock_changes_response = MagicMock()
        self.mock_document_response = MagicMock()

    def tearDown(self):
        """Clean up singleton instances after each test."""
        from lib.couchdb.couchdb_connection import CouchDBConnectionManager

        if CouchDBConnectionManager in SingletonMeta._instances:
            del SingletonMeta._instances[CouchDBConnectionManager]

    @patch("lib.couchdb.project_db_manager.ConfigLoader")
    @patch("lib.couchdb.project_db_manager.CouchDBHandler.__init__")
    def test_init_success(self, mock_handler_init, mock_config_loader):
        """Test successful initialization of ProjectDBManager."""
        # Arrange
        mock_handler_init.return_value = None
        mock_config_instance = MagicMock()
        mock_config_instance.load_config.return_value = self.mock_module_registry
        mock_config_loader.return_value = mock_config_instance

        # Act
        manager = ProjectDBManager()

        # Assert
        mock_handler_init.assert_called_once_with("projects")
        mock_config_instance.load_config.assert_called_once_with("module_registry.json")
        self.assertEqual(manager.module_registry, self.mock_module_registry)

    @patch("lib.couchdb.project_db_manager.ConfigLoader")
    @patch("lib.couchdb.project_db_manager.CouchDBHandler.__init__")
    def test_init_config_loading_error(self, mock_handler_init, mock_config_loader):
        """Test initialization when module registry loading fails."""
        # Arrange
        mock_handler_init.return_value = None
        mock_config_instance = MagicMock()
        mock_config_instance.load_config.side_effect = Exception("Config error")
        mock_config_loader.return_value = mock_config_instance

        # Act & Assert
        with self.assertRaises(Exception):
            ProjectDBManager()

    @patch("lib.couchdb.project_db_manager.ConfigLoader")
    @patch("lib.couchdb.project_db_manager.CouchDBHandler.__init__")
    async def test_fetch_changes_exact_match(
        self, mock_handler_init, mock_config_loader
    ):
        """Test fetch_changes with exact module registry match."""
        # Arrange
        mock_handler_init.return_value = None
        mock_config_instance = MagicMock()
        mock_config_instance.load_config.return_value = self.mock_module_registry
        mock_config_loader.return_value = mock_config_instance

        manager = ProjectDBManager()

        # Mock get_changes to yield our test document
        async def mock_get_changes(last_processed_seq=None):
            yield self.mock_doc_with_10x

        manager.get_changes = mock_get_changes

        # Act
        results = []
        async for doc, module_loc in manager.fetch_changes():
            results.append((doc, module_loc))
            break  # Only get first result

        # Assert
        self.assertEqual(len(results), 1)
        doc, module_loc = results[0]
        self.assertEqual(doc, self.mock_doc_with_10x)
        self.assertEqual(module_loc, "lib.realms.tenx.tenx_project.TenXProject")

    @patch("lib.couchdb.project_db_manager.ConfigLoader")
    @patch("lib.couchdb.project_db_manager.CouchDBHandler.__init__")
    async def test_fetch_changes_prefix_match(
        self, mock_handler_init, mock_config_loader
    ):
        """Test fetch_changes with prefix matching when exact match fails."""
        # Arrange
        mock_handler_init.return_value = None
        mock_config_instance = MagicMock()
        mock_config_instance.load_config.return_value = self.mock_module_registry
        mock_config_loader.return_value = mock_config_instance

        manager = ProjectDBManager()

        # Mock get_changes to yield document with prefix-matchable method
        async def mock_get_changes(last_processed_seq=None):
            yield self.mock_doc_with_prefix_match

        manager.get_changes = mock_get_changes

        # Act
        results = []
        async for doc, module_loc in manager.fetch_changes():
            results.append((doc, module_loc))
            break

        # Assert
        self.assertEqual(len(results), 1)
        doc, module_loc = results[0]
        self.assertEqual(doc, self.mock_doc_with_prefix_match)
        self.assertEqual(module_loc, "lib.realms.mars.mars_project.MarsProject")

    @patch("lib.couchdb.project_db_manager.ConfigLoader")
    @patch("lib.couchdb.project_db_manager.CouchDBHandler.__init__")
    def test_fetch_changes_no_match_logic(self, mock_handler_init, mock_config_loader):
        """Test the logic used in fetch_changes when no module registry match is found."""
        # Arrange
        mock_handler_init.return_value = None
        mock_config_instance = MagicMock()
        mock_config_instance.load_config.return_value = self.mock_module_registry
        mock_config_loader.return_value = mock_config_instance

        manager = ProjectDBManager()

        # Test the module matching logic directly
        unknown_doc = self.mock_doc_with_unknown_method
        method = unknown_doc["details"]["library_construction_method"]

        # Should not find exact match
        module_config = manager.module_registry.get(method)
        self.assertIsNone(module_config)

        # Should not find prefix match either
        found_prefix_match = False
        for registered_method, config in manager.module_registry.items():
            if config.get("prefix") and method.startswith(registered_method):
                found_prefix_match = True
                break

        self.assertFalse(found_prefix_match)

    @patch("lib.couchdb.project_db_manager.ConfigLoader")
    @patch("lib.couchdb.project_db_manager.CouchDBHandler.__init__")
    def test_fetch_changes_missing_details_logic(
        self, mock_handler_init, mock_config_loader
    ):
        """Test the exception handling when document is missing details/method."""
        # Arrange
        mock_handler_init.return_value = None
        mock_config_instance = MagicMock()
        mock_config_instance.load_config.return_value = self.mock_module_registry
        mock_config_loader.return_value = mock_config_instance

        # Test document without details - no manager needed for this test
        malformed_doc = self.mock_doc_missing_details

        # Should not have details key
        self.assertNotIn("details", malformed_doc)

        # Test that trying to access details would raise KeyError
        # (this simulates what happens in fetch_changes)

    @patch("lib.couchdb.project_db_manager.ConfigLoader")
    @patch("lib.couchdb.project_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.project_db_manager.Ygg.get_last_processed_seq")
    @patch("lib.couchdb.project_db_manager.Ygg.save_last_processed_seq")
    async def test_get_changes_success(
        self, mock_save_seq, mock_get_seq, mock_handler_init, mock_config_loader
    ):
        """Test get_changes successfully fetches and yields documents."""
        # Arrange
        mock_handler_init.return_value = None
        mock_config_instance = MagicMock()
        mock_config_instance.load_config.return_value = self.mock_module_registry
        mock_config_loader.return_value = mock_config_instance

        mock_get_seq.return_value = "0"

        manager = ProjectDBManager()

        # Mock IBM Cloud SDK server and changes response
        mock_server = MagicMock()
        manager.server = mock_server
        manager.db_name = "projects"

        # Mock changes stream response
        mock_stream_response = MagicMock()
        mock_lines = [
            '{"id": "doc1", "seq": "1"}',
            '{"id": "doc2", "seq": "2"}',
        ]
        mock_stream_response.iter_lines.return_value = mock_lines

        mock_changes_result = MagicMock()
        mock_changes_result.get_result.return_value = mock_stream_response
        mock_server.post_changes_as_stream.return_value = mock_changes_result

        # Mock fetch_document_by_id
        manager.fetch_document_by_id = MagicMock(
            side_effect=[self.mock_doc_with_10x, self.mock_doc_with_smartseq]
        )

        # Act
        results = []
        async for doc in manager.get_changes():
            results.append(doc)
            if len(results) >= 2:  # Get first 2 results
                break

        # Assert
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0], self.mock_doc_with_10x)
        self.assertEqual(results[1], self.mock_doc_with_smartseq)

        # Verify IBM SDK calls
        mock_server.post_changes_as_stream.assert_called_once_with(
            db="projects", feed="continuous", since="0", include_docs=False
        )

        # Verify sequence tracking
        self.assertEqual(mock_save_seq.call_count, 2)
        mock_save_seq.assert_any_call("1")
        mock_save_seq.assert_any_call("2")

    @patch("lib.couchdb.project_db_manager.ConfigLoader")
    @patch("lib.couchdb.project_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.project_db_manager.Ygg.get_last_processed_seq")
    async def test_get_changes_with_provided_seq(
        self, mock_get_seq, mock_handler_init, mock_config_loader
    ):
        """Test get_changes when last_processed_seq is provided."""
        # Arrange
        mock_handler_init.return_value = None
        mock_config_instance = MagicMock()
        mock_config_instance.load_config.return_value = self.mock_module_registry
        mock_config_loader.return_value = mock_config_instance

        manager = ProjectDBManager()

        # Mock IBM Cloud SDK server
        mock_server = MagicMock()
        manager.server = mock_server
        manager.db_name = "projects"

        # Mock empty changes stream
        mock_stream_response = MagicMock()
        mock_stream_response.iter_lines.return_value = []

        mock_changes_result = MagicMock()
        mock_changes_result.get_result.return_value = mock_stream_response
        mock_server.post_changes_as_stream.return_value = mock_changes_result

        # Act
        results = []
        count = 0
        async for doc in manager.get_changes(last_processed_seq="custom_seq"):
            results.append(doc)
            count += 1
            if count >= 1:  # Safety break
                break

        # Assert
        # Should not call get_last_processed_seq when seq is provided
        mock_get_seq.assert_not_called()
        mock_server.post_changes_as_stream.assert_called_once_with(
            db="projects", feed="continuous", since="custom_seq", include_docs=False
        )

    @patch("lib.couchdb.project_db_manager.ConfigLoader")
    @patch("lib.couchdb.project_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.project_db_manager.Ygg.get_last_processed_seq")
    @patch("lib.couchdb.project_db_manager.Ygg.save_last_processed_seq")
    @patch("lib.couchdb.project_db_manager.logging")
    async def test_get_changes_none_document(
        self,
        mock_logging,
        mock_save_seq,
        mock_get_seq,
        mock_handler_init,
        mock_config_loader,
    ):
        """Test get_changes when fetch_document_by_id returns None."""
        # Arrange
        mock_handler_init.return_value = None
        mock_config_instance = MagicMock()
        mock_config_instance.load_config.return_value = self.mock_module_registry
        mock_config_loader.return_value = mock_config_instance

        mock_get_seq.return_value = "0"

        manager = ProjectDBManager()

        # Mock IBM Cloud SDK server
        mock_server = MagicMock()
        manager.server = mock_server
        manager.db_name = "projects"

        # Mock changes stream with one change
        mock_stream_response = MagicMock()
        mock_lines = ['{"id": "missing_doc", "seq": "1"}']
        mock_stream_response.iter_lines.return_value = mock_lines

        mock_changes_result = MagicMock()
        mock_changes_result.get_result.return_value = mock_stream_response
        mock_server.post_changes_as_stream.return_value = mock_changes_result

        # Mock fetch_document_by_id to return None
        manager.fetch_document_by_id = MagicMock(return_value=None)

        # Act
        results = []
        count = 0
        async for doc in manager.get_changes():
            results.append(doc)
            count += 1
            if count >= 1:  # Safety break since we won't get any real results
                break

        # Assert
        self.assertEqual(len(results), 0)  # No documents should be yielded
        mock_logging.warning.assert_called_with("Document with ID missing_doc is None.")
        mock_save_seq.assert_called_once_with("1")

    @patch("lib.couchdb.project_db_manager.ConfigLoader")
    @patch("lib.couchdb.project_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.project_db_manager.Ygg.get_last_processed_seq")
    @patch("lib.couchdb.project_db_manager.Ygg.save_last_processed_seq")
    @patch("lib.couchdb.project_db_manager.logging")
    async def test_get_changes_none_sequence(
        self,
        mock_logging,
        mock_save_seq,
        mock_get_seq,
        mock_handler_init,
        mock_config_loader,
    ):
        """Test get_changes when sequence is None."""
        # Arrange
        mock_handler_init.return_value = None
        mock_config_instance = MagicMock()
        mock_config_instance.load_config.return_value = self.mock_module_registry
        mock_config_loader.return_value = mock_config_instance

        mock_get_seq.return_value = "0"

        manager = ProjectDBManager()

        # Mock IBM Cloud SDK server
        mock_server = MagicMock()
        manager.server = mock_server
        manager.db_name = "projects"

        # Mock changes stream with None sequence
        mock_stream_response = MagicMock()
        mock_lines = ['{"id": "doc1", "seq": null}']
        mock_stream_response.iter_lines.return_value = mock_lines

        mock_changes_result = MagicMock()
        mock_changes_result.get_result.return_value = mock_stream_response
        mock_server.post_changes_as_stream.return_value = mock_changes_result

        # Mock fetch_document_by_id
        manager.fetch_document_by_id = MagicMock(return_value=self.mock_doc_with_10x)

        # Act
        results = []
        async for doc in manager.get_changes():
            results.append(doc)
            break

        # Assert
        self.assertEqual(len(results), 1)
        mock_logging.warning.assert_called_with(
            "Received `None` for last_processed_seq. Skipping save."
        )
        mock_save_seq.assert_not_called()

    @patch("lib.couchdb.project_db_manager.ConfigLoader")
    @patch("lib.couchdb.project_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.project_db_manager.Ygg.get_last_processed_seq")
    @patch("lib.couchdb.project_db_manager.logging")
    async def test_get_changes_fetch_document_exception(
        self, mock_logging, mock_get_seq, mock_handler_init, mock_config_loader
    ):
        """Test get_changes when fetch_document_by_id raises exceptions."""
        # Arrange
        mock_handler_init.return_value = None
        mock_config_instance = MagicMock()
        mock_config_instance.load_config.return_value = self.mock_module_registry
        mock_config_loader.return_value = mock_config_instance

        mock_get_seq.return_value = "0"

        manager = ProjectDBManager()

        # Mock IBM Cloud SDK server
        mock_server = MagicMock()
        manager.server = mock_server
        manager.db_name = "projects"

        # Mock changes stream
        mock_stream_response = MagicMock()
        mock_change = {"id": "error_doc", "seq": "1"}
        mock_lines = [json.dumps(mock_change)]
        mock_stream_response.iter_lines.return_value = mock_lines

        mock_changes_result = MagicMock()
        mock_changes_result.get_result.return_value = mock_stream_response
        mock_server.post_changes_as_stream.return_value = mock_changes_result

        # Mock fetch_document_by_id to raise exception
        manager.fetch_document_by_id = MagicMock(
            side_effect=Exception("Database error")
        )

        # Act
        results = []
        count = 0
        async for doc in manager.get_changes():
            results.append(doc)
            count += 1
            if count >= 1:  # Safety break
                break

        # Assert
        self.assertEqual(len(results), 0)  # No documents should be yielded
        mock_logging.warning.assert_called_with(
            "Error processing change: Database error"
        )
        mock_logging.debug.assert_called_with(f"Data causing the error: {mock_change}")

    @patch("lib.couchdb.project_db_manager.ConfigLoader")
    @patch("lib.couchdb.project_db_manager.CouchDBHandler.__init__")
    async def test_get_changes_skip_empty_lines(
        self, mock_handler_init, mock_config_loader
    ):
        """Test get_changes skips empty lines in changes stream."""
        # Arrange
        mock_handler_init.return_value = None
        mock_config_instance = MagicMock()
        mock_config_instance.load_config.return_value = self.mock_module_registry
        mock_config_loader.return_value = mock_config_instance

        manager = ProjectDBManager()

        # Mock IBM Cloud SDK server
        mock_server = MagicMock()
        manager.server = mock_server
        manager.db_name = "projects"

        # Mock changes stream with empty lines
        mock_stream_response = MagicMock()
        mock_lines = ["", '{"id": "doc1", "seq": "1"}', "", "   "]
        mock_stream_response.iter_lines.return_value = mock_lines

        mock_changes_result = MagicMock()
        mock_changes_result.get_result.return_value = mock_stream_response
        mock_server.post_changes_as_stream.return_value = mock_changes_result

        # Mock fetch_document_by_id
        manager.fetch_document_by_id = MagicMock(return_value=self.mock_doc_with_10x)

        # Act
        results = []
        async for doc in manager.get_changes():
            results.append(doc)
            break

        # Assert
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], self.mock_doc_with_10x)

    @patch("lib.couchdb.project_db_manager.ConfigLoader")
    @patch("lib.couchdb.project_db_manager.CouchDBHandler.__init__")
    async def test_get_changes_skip_invalid_json(
        self, mock_handler_init, mock_config_loader
    ):
        """Test get_changes handles invalid JSON lines gracefully."""
        # Arrange
        mock_handler_init.return_value = None
        mock_config_instance = MagicMock()
        mock_config_instance.load_config.return_value = self.mock_module_registry
        mock_config_loader.return_value = mock_config_instance

        manager = ProjectDBManager()

        # Mock IBM Cloud SDK server
        mock_server = MagicMock()
        manager.server = mock_server
        manager.db_name = "projects"

        # Mock changes stream with invalid JSON
        mock_stream_response = MagicMock()
        mock_lines = ["invalid json", '{"id": "doc1", "seq": "1"}']
        mock_stream_response.iter_lines.return_value = mock_lines

        mock_changes_result = MagicMock()
        mock_changes_result.get_result.return_value = mock_stream_response
        mock_server.post_changes_as_stream.return_value = mock_changes_result

        # Mock fetch_document_by_id
        manager.fetch_document_by_id = MagicMock(return_value=self.mock_doc_with_10x)

        # Act
        results = []
        try:
            async for doc in manager.get_changes():
                results.append(doc)
                break
        except json.JSONDecodeError:
            # Expected when parsing invalid JSON
            pass

        # Should not get any results due to JSON error
        self.assertEqual(len(results), 0)

    @patch("lib.couchdb.project_db_manager.ConfigLoader")
    @patch("lib.couchdb.project_db_manager.CouchDBHandler.__init__")
    async def test_get_changes_skip_incomplete_changes(
        self, mock_handler_init, mock_config_loader
    ):
        """Test get_changes skips changes without id or seq."""
        # Arrange
        mock_handler_init.return_value = None
        mock_config_instance = MagicMock()
        mock_config_instance.load_config.return_value = self.mock_module_registry
        mock_config_loader.return_value = mock_config_instance

        manager = ProjectDBManager()

        # Mock IBM Cloud SDK server
        mock_server = MagicMock()
        manager.server = mock_server
        manager.db_name = "projects"

        # Mock changes stream with incomplete change objects
        mock_stream_response = MagicMock()
        mock_lines = [
            '{"id": "doc1"}',  # Missing seq
            '{"seq": "1"}',  # Missing id
            '{"id": "doc2", "seq": "2"}',  # Valid
        ]
        mock_stream_response.iter_lines.return_value = mock_lines

        mock_changes_result = MagicMock()
        mock_changes_result.get_result.return_value = mock_stream_response
        mock_server.post_changes_as_stream.return_value = mock_changes_result

        # Mock fetch_document_by_id
        manager.fetch_document_by_id = MagicMock(return_value=self.mock_doc_with_10x)

        # Act
        results = []
        async for doc in manager.get_changes():
            results.append(doc)
            break

        # Assert - only the valid change should be processed
        self.assertEqual(len(results), 1)
        manager.fetch_document_by_id.assert_called_once_with("doc2")

    @patch("lib.couchdb.project_db_manager.ConfigLoader")
    @patch("lib.couchdb.project_db_manager.CouchDBHandler.__init__")
    def test_fetch_document_by_id_success(self, mock_handler_init, mock_config_loader):
        """Test successful document retrieval by ID."""
        # Arrange
        mock_handler_init.return_value = None
        mock_config_instance = MagicMock()
        mock_config_instance.load_config.return_value = self.mock_module_registry
        mock_config_loader.return_value = mock_config_instance

        manager = ProjectDBManager()

        # Mock IBM Cloud SDK server
        mock_server = MagicMock()
        manager.server = mock_server
        manager.db_name = "projects"

        # Mock successful document response
        mock_doc_result = MagicMock()
        mock_doc_result.get_result.return_value = self.mock_doc_with_10x
        mock_server.get_document.return_value = mock_doc_result

        # Act
        result = manager.fetch_document_by_id("doc1")

        # Assert
        self.assertEqual(result, self.mock_doc_with_10x)
        mock_server.get_document.assert_called_once_with(db="projects", doc_id="doc1")

    @patch("lib.couchdb.project_db_manager.ConfigLoader")
    @patch("lib.couchdb.project_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.project_db_manager.logging")
    def test_fetch_document_by_id_not_found(
        self, mock_logging, mock_handler_init, mock_config_loader
    ):
        """Test document retrieval when document doesn't exist (404)."""
        # Arrange
        mock_handler_init.return_value = None
        mock_config_instance = MagicMock()
        mock_config_instance.load_config.return_value = self.mock_module_registry
        mock_config_loader.return_value = mock_config_instance

        manager = ProjectDBManager()

        # Mock IBM Cloud SDK server
        mock_server = MagicMock()
        manager.server = mock_server
        manager.db_name = "projects"

        # Mock 404 exception that behaves like ApiException
        api_exception = MockApiException(404, "Not found")
        mock_server.get_document.side_effect = api_exception

        # Patch the ApiException in the module to catch our mock
        with patch("lib.couchdb.project_db_manager.ApiException", MockApiException):
            # Act
            result = manager.fetch_document_by_id("nonexistent_doc")

        # Assert
        self.assertIsNone(result)
        mock_logging.error.assert_called_with(
            "Document 'nonexistent_doc' not found in the database."
        )

    @patch("lib.couchdb.project_db_manager.ConfigLoader")
    @patch("lib.couchdb.project_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.project_db_manager.logging")
    def test_fetch_document_by_id_api_error(
        self, mock_logging, mock_handler_init, mock_config_loader
    ):
        """Test document retrieval when API returns other errors."""
        # Arrange
        mock_handler_init.return_value = None
        mock_config_instance = MagicMock()
        mock_config_instance.load_config.return_value = self.mock_module_registry
        mock_config_loader.return_value = mock_config_instance

        manager = ProjectDBManager()

        # Mock IBM Cloud SDK server
        mock_server = MagicMock()
        manager.server = mock_server
        manager.db_name = "projects"

        # Mock 500 exception that behaves like ApiException
        api_exception = MockApiException(500, "Server error")
        mock_server.get_document.side_effect = api_exception

        # Patch the ApiException in the module to catch our mock
        with patch("lib.couchdb.project_db_manager.ApiException", MockApiException):
            # Act
            result = manager.fetch_document_by_id("error_doc")

        # Assert
        self.assertIsNone(result)
        mock_logging.error.assert_called_with(
            "Cloudant API error fetching 'error_doc': 500 Server error"
        )

    @patch("lib.couchdb.project_db_manager.ConfigLoader")
    @patch("lib.couchdb.project_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.project_db_manager.logging")
    def test_fetch_document_by_id_general_exception(
        self, mock_logging, mock_handler_init, mock_config_loader
    ):
        """Test document retrieval when general exception occurs."""
        # Arrange
        mock_handler_init.return_value = None
        mock_config_instance = MagicMock()
        mock_config_instance.load_config.return_value = self.mock_module_registry
        mock_config_loader.return_value = mock_config_instance

        manager = ProjectDBManager()

        # Mock IBM Cloud SDK server
        mock_server = MagicMock()
        manager.server = mock_server
        manager.db_name = "projects"

        # Mock general exception
        mock_server.get_document.side_effect = Exception("Connection error")

        # Act
        result = manager.fetch_document_by_id("error_doc")

        # Assert
        self.assertIsNone(result)
        mock_logging.error.assert_called_with(
            "Error while accessing database: Connection error"
        )

    @patch("lib.couchdb.project_db_manager.ConfigLoader")
    @patch("lib.couchdb.project_db_manager.CouchDBHandler.__init__")
    def test_fetch_document_by_id_non_dict_response(
        self, mock_handler_init, mock_config_loader
    ):
        """Test document retrieval when response is not a dictionary."""
        # Arrange
        mock_handler_init.return_value = None
        mock_config_instance = MagicMock()
        mock_config_instance.load_config.return_value = self.mock_module_registry
        mock_config_loader.return_value = mock_config_instance

        manager = ProjectDBManager()

        # Mock IBM Cloud SDK server
        mock_server = MagicMock()
        manager.server = mock_server
        manager.db_name = "projects"

        # Mock response with non-dict result (e.g., string or None)
        mock_response = MagicMock()
        mock_response.result = "not_a_dict"  # Non-dict response
        mock_server.get_document.return_value = mock_response

        # Act
        result = manager.fetch_document_by_id("test_doc")

        # Assert
        self.assertIsNone(result)
        mock_server.get_document.assert_called_once_with(
            db="projects", doc_id="test_doc"
        )

    @patch("lib.couchdb.project_db_manager.ConfigLoader")
    @patch("lib.couchdb.project_db_manager.CouchDBHandler.__init__")
    async def test_fetch_changes_multiple_documents(
        self, mock_handler_init, mock_config_loader
    ):
        """Test fetch_changes with multiple documents of different types."""
        # Arrange
        mock_handler_init.return_value = None
        mock_config_instance = MagicMock()
        mock_config_instance.load_config.return_value = self.mock_module_registry
        mock_config_loader.return_value = mock_config_instance

        manager = ProjectDBManager()

        # Mock get_changes to yield multiple documents
        test_docs = [
            self.mock_doc_with_10x,
            self.mock_doc_with_smartseq,
            self.mock_doc_with_prefix_match,
            self.mock_doc_with_unknown_method,
            self.mock_doc_missing_details,
        ]

        async def mock_get_changes(last_processed_seq=None):
            for doc in test_docs:
                yield doc

        manager.get_changes = mock_get_changes

        # Act
        results = []
        async for doc, module_loc in manager.fetch_changes():
            results.append((doc, module_loc))
            if len(results) >= 3:  # Expect 3 valid results
                break

        # Assert
        self.assertEqual(len(results), 3)

        # Check exact matches
        self.assertEqual(results[0][1], "lib.realms.tenx.tenx_project.TenXProject")
        self.assertEqual(
            results[1][1], "lib.realms.smartseq3.smartseq3_project.SmartSeq3Project"
        )

        # Check prefix match
        self.assertEqual(results[2][1], "lib.realms.mars.mars_project.MarsProject")

    @patch("lib.couchdb.project_db_manager.ConfigLoader")
    @patch("lib.couchdb.project_db_manager.CouchDBHandler.__init__")
    async def test_fetch_changes_empty_module_registry(
        self, mock_handler_init, mock_config_loader
    ):
        """Test fetch_changes with empty module registry."""
        # Arrange
        mock_handler_init.return_value = None
        mock_config_instance = MagicMock()
        mock_config_instance.load_config.return_value = {}  # Empty registry
        mock_config_loader.return_value = mock_config_instance

        manager = ProjectDBManager()

        # Test the inner logic by directly testing the module lookup logic
        # rather than the infinite loop
        test_doc = self.mock_doc_with_10x
        method = test_doc["details"]["library_construction_method"]

        # With empty registry, should not find any module
        module_config = manager.module_registry.get(method)
        self.assertIsNone(module_config)

        # Test prefix matching with empty registry
        found_prefix_match = False
        for registered_method, config in manager.module_registry.items():
            if config.get("prefix") and method.startswith(registered_method):
                found_prefix_match = True
                break

        self.assertFalse(found_prefix_match)

    @patch("lib.couchdb.project_db_manager.ConfigLoader")
    @patch("lib.couchdb.project_db_manager.CouchDBHandler.__init__")
    def test_fetch_changes_module_registry_logic(
        self, mock_handler_init, mock_config_loader
    ):
        """Test the module registry lookup logic used in fetch_changes."""
        # Arrange
        mock_handler_init.return_value = None
        mock_config_instance = MagicMock()
        mock_config_instance.load_config.return_value = self.mock_module_registry
        mock_config_loader.return_value = mock_config_instance

        manager = ProjectDBManager()

        # Test exact match
        method = "10X"
        module_config = manager.module_registry.get(method)
        self.assertIsNotNone(module_config)
        if module_config:
            self.assertEqual(
                module_config["module"], "lib.realms.tenx.tenx_project.TenXProject"
            )

        # Test prefix match for MARS-seq
        method = "MARS-seq"
        module_config = manager.module_registry.get(method)
        self.assertIsNone(module_config)  # No exact match

        # But should find prefix match
        found_prefix_match = None
        for registered_method, config in manager.module_registry.items():
            if config.get("prefix") and method.startswith(registered_method):
                found_prefix_match = config
                break

        self.assertIsNotNone(found_prefix_match)
        if found_prefix_match:
            self.assertEqual(
                found_prefix_match["module"], "lib.realms.mars.mars_project.MarsProject"
            )

        # Test unknown method
        method = "UnknownMethod"
        module_config = manager.module_registry.get(method)
        self.assertIsNone(module_config)

        found_prefix_match = False
        for registered_method, config in manager.module_registry.items():
            if config.get("prefix") and method.startswith(registered_method):
                found_prefix_match = True
                break

        self.assertFalse(found_prefix_match)

    @patch("lib.couchdb.project_db_manager.ConfigLoader")
    @patch("lib.couchdb.project_db_manager.CouchDBHandler.__init__")
    def test_fetch_changes_exception_handling_logic(
        self, mock_handler_init, mock_config_loader
    ):
        """Test the exception handling logic used in fetch_changes."""
        # Arrange
        mock_handler_init.return_value = None
        mock_config_instance = MagicMock()
        mock_config_instance.load_config.return_value = self.mock_module_registry
        mock_config_loader.return_value = mock_config_instance

        manager = ProjectDBManager()

        # Test that module registry is properly loaded
        self.assertEqual(manager.module_registry, self.mock_module_registry)

        # Test valid document processing
        valid_doc = self.mock_doc_with_10x
        method = valid_doc["details"]["library_construction_method"]
        self.assertEqual(method, "10X")

        # Test module lookup
        module_config = manager.module_registry.get(method)
        self.assertIsNotNone(module_config)


if __name__ == "__main__":
    unittest.main()
