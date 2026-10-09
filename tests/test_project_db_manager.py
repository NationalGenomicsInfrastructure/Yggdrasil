import unittest
from unittest.mock import ANY, MagicMock, patch

from yggdrasil.couchdb.project_db_manager import ProjectDBManager


class TestProjectDBManager(unittest.TestCase):
    """Tests for ProjectDBManager construction and inherited document access."""

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

        self.mock_doc_with_10x = {
            "_id": "doc1",
            "project_id": "P12345",
            "details": {"library_construction_method": "10X"},
        }

    @patch("yggdrasil.couchdb.project_db_manager.resolve_couchdb_params")
    @patch("yggdrasil.couchdb.project_db_manager.CouchDBHandler.__init__")
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

    @patch("yggdrasil.couchdb.project_db_manager.resolve_couchdb_params")
    @patch("yggdrasil.couchdb.project_db_manager.CouchDBHandler.__init__")
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
