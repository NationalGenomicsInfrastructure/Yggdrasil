import json
import unittest
from unittest.mock import ANY, MagicMock, patch

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
        """Set up test fixtures."""
        # Mock endpoint config
        self.mock_endpoint_config = {
            "url": "http://localhost:5984",
            "auth": {
                "user_env": "COUCHDB_USER",
                "pass_env": "COUCHDB_PASS",
            },
        }
        self.mock_couchdb_params = MagicMock(
            url=self.mock_endpoint_config["url"],
            user_env=self.mock_endpoint_config["auth"]["user_env"],
            pass_env=self.mock_endpoint_config["auth"]["pass_env"],
        )

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

        # Mock IBM Cloud SDK responses
        self.mock_changes_response = MagicMock()
        self.mock_document_response = MagicMock()

    def tearDown(self):
        """Clean up after each test."""
        pass

    @patch("lib.couchdb.project_db_manager.resolve_couchdb_params")
    @patch("lib.couchdb.project_db_manager.CouchDBHandler.__init__")
    def test_init_success(self, mock_handler_init, mock_resolve_params):
        """Test successful initialization of ProjectDBManager."""
        # Arrange
        mock_handler_init.return_value = None
        mock_resolve_params.return_value = self.mock_couchdb_params

        # Act
        ProjectDBManager()

        # Assert
        mock_handler_init.assert_called_once_with(
            "projects",
            url="http://localhost:5984",
            user_env="COUCHDB_USER",
            pass_env="COUCHDB_PASS",
            logger=ANY,
        )

    @patch("lib.couchdb.project_db_manager.resolve_couchdb_params")
    @patch("lib.couchdb.project_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.project_db_manager.Ygg.get_last_processed_seq")
    @patch("lib.couchdb.project_db_manager.Ygg.save_last_processed_seq")
    async def test_get_changes_success(
        self,
        mock_save_seq,
        mock_get_seq,
        mock_handler_init,
        mock_get_endpoint,
    ):
        """Test get_changes successfully fetches and yields documents."""
        # Arrange
        mock_handler_init.return_value = None
        mock_get_endpoint.return_value = self.mock_couchdb_params

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

    @patch("lib.couchdb.project_db_manager.resolve_couchdb_params")
    @patch("lib.couchdb.project_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.project_db_manager.Ygg.get_last_processed_seq")
    async def test_get_changes_with_provided_seq(
        self, mock_get_seq, mock_handler_init, mock_get_endpoint
    ):
        """Test get_changes when last_processed_seq is provided."""
        # Arrange
        mock_handler_init.return_value = None
        mock_get_endpoint.return_value = self.mock_couchdb_params

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

    @patch("lib.couchdb.project_db_manager.resolve_couchdb_params")
    @patch("lib.couchdb.project_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.project_db_manager.Ygg.get_last_processed_seq")
    @patch("lib.couchdb.project_db_manager.Ygg.save_last_processed_seq")
    @patch("lib.couchdb.project_db_manager.custom_logger")
    async def test_get_changes_none_document(
        self,
        mock_logging,
        mock_save_seq,
        mock_get_seq,
        mock_handler_init,
        mock_get_endpoint,
    ):
        """Test get_changes when fetch_document_by_id returns None."""
        # Arrange
        mock_handler_init.return_value = None
        mock_get_endpoint.return_value = self.mock_couchdb_params

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
        mock_logging.return_value.warning.assert_called_with(
            "Document with ID missing_doc is None."
        )
        mock_save_seq.assert_called_once_with("1")

    @patch("lib.couchdb.project_db_manager.resolve_couchdb_params")
    @patch("lib.couchdb.project_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.project_db_manager.Ygg.get_last_processed_seq")
    @patch("lib.couchdb.project_db_manager.Ygg.save_last_processed_seq")
    @patch("lib.couchdb.project_db_manager.custom_logger")
    async def test_get_changes_none_sequence(
        self,
        mock_logging,
        mock_save_seq,
        mock_get_seq,
        mock_handler_init,
        mock_get_endpoint,
    ):
        """Test get_changes when sequence is None."""
        # Arrange
        mock_handler_init.return_value = None
        mock_get_endpoint.return_value = self.mock_couchdb_params

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
        mock_logging.return_value.warning.assert_called_with(
            "Received `None` for last_processed_seq. Skipping save."
        )
        mock_save_seq.assert_not_called()

    @patch("lib.couchdb.project_db_manager.resolve_couchdb_params")
    @patch("lib.couchdb.project_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.project_db_manager.Ygg.get_last_processed_seq")
    @patch("lib.couchdb.project_db_manager.custom_logger")
    async def test_get_changes_fetch_document_exception(
        self,
        mock_logging,
        mock_get_seq,
        mock_handler_init,
        mock_get_endpoint,
    ):
        """Test get_changes when fetch_document_by_id raises exceptions."""
        # Arrange
        mock_handler_init.return_value = None
        mock_get_endpoint.return_value = self.mock_couchdb_params

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
        mock_logging.return_value.warning.assert_called_with(
            "Error processing change: Database error"
        )
        mock_logging.return_value.debug.assert_called_with(
            f"Data causing the error: {mock_change}"
        )

    @patch("lib.couchdb.project_db_manager.resolve_couchdb_params")
    @patch("lib.couchdb.project_db_manager.CouchDBHandler.__init__")
    async def test_get_changes_skip_empty_lines(
        self, mock_handler_init, mock_get_endpoint
    ):
        """Test get_changes skips empty lines in changes stream."""
        # Arrange
        mock_handler_init.return_value = None
        mock_get_endpoint.return_value = self.mock_couchdb_params

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

    @patch("lib.couchdb.project_db_manager.resolve_couchdb_params")
    @patch("lib.couchdb.project_db_manager.CouchDBHandler.__init__")
    async def test_get_changes_skip_invalid_json(
        self, mock_handler_init, mock_get_endpoint
    ):
        """Test get_changes handles invalid JSON lines gracefully."""
        # Arrange
        mock_handler_init.return_value = None
        mock_get_endpoint.return_value = self.mock_couchdb_params

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

    @patch("lib.couchdb.project_db_manager.resolve_couchdb_params")
    @patch("lib.couchdb.project_db_manager.CouchDBHandler.__init__")
    async def test_get_changes_skip_incomplete_changes(
        self, mock_handler_init, mock_get_endpoint
    ):
        """Test get_changes skips changes without id or seq."""
        # Arrange
        mock_handler_init.return_value = None
        mock_get_endpoint.return_value = self.mock_couchdb_params

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

    @patch("lib.couchdb.project_db_manager.resolve_couchdb_params")
    @patch("lib.couchdb.project_db_manager.CouchDBHandler.__init__")
    def test_fetch_document_by_id_success(self, mock_handler_init, mock_get_endpoint):
        """Test successful document retrieval by ID (inherited from CouchDBHandler).

        Note: Detailed error handling tests for fetch_document_by_id are in
        test_db_connection_manager.py since the method is now in CouchDBHandler.
        This test verifies that ProjectDBManager correctly inherits the method.
        """
        # Arrange
        mock_handler_init.return_value = None
        mock_get_endpoint.return_value = self.mock_couchdb_params

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


if __name__ == "__main__":
    unittest.main()
