import sys
import unittest
from unittest.mock import MagicMock, patch


# Mock IBM Cloud SDK modules before importing the module under test
class MockApiException(Exception):
    def __init__(self, message, code=None):
        super().__init__(message)
        self.code = code
        self.message = message


# Mock the IBM Cloud SDK modules to avoid import errors
mock_api_exception_module = MagicMock()
mock_api_exception_module.ApiException = MockApiException
sys.modules["ibm_cloud_sdk_core"] = MagicMock()
sys.modules["ibm_cloud_sdk_core.api_exception"] = mock_api_exception_module
sys.modules["ibmcloudant"] = MagicMock()
sys.modules["ibmcloudant.cloudant_v1"] = MagicMock()

# Make the ApiException available in the module
import lib.couchdb.yggdrasil_db_manager
from lib.core_utils.singleton_decorator import SingletonMeta
from lib.couchdb.yggdrasil_db_manager import YggdrasilDBManager, auto_load_and_save

lib.couchdb.yggdrasil_db_manager.ApiException = MockApiException


class TestYggdrasilDBManager(unittest.TestCase):
    """
    Comprehensive tests for YggdrasilDBManager class.
    Tests initialization, document CRUD operations, decorated methods, and error handling.
    """

    def setUp(self):
        """Set up test fixtures and clear singleton instances for test isolation."""
        # Clear singleton instances to ensure test isolation
        from lib.couchdb.couchdb_connection import CouchDBConnectionManager

        if CouchDBConnectionManager in SingletonMeta._instances:
            del SingletonMeta._instances[CouchDBConnectionManager]

        # Mock project data
        self.mock_project_data = {
            "project_id": "P12345",
            "projects_reference": "ref_12345",
            "project_name": "Test Project",
            "method": "10X",
            "project_status": "ongoing",
            "start_date": "2024-01-01T00:00:00",
            "end_date": "",
            "samples": [
                {"sample_id": "S001", "status": "pending", "slurm_job_id": ""},
                {"sample_id": "S002", "status": "completed", "slurm_job_id": "123456"},
            ],
            "user_info": {
                "owner": {"name": "John Doe", "email": "john@example.com"},
                "pi": {"name": "Jane Smith", "email": "jane@example.com"},
            },
            "delivery_info": {"sensitive": True},
            "ngi_reports": [
                {"report_id": "R001", "status": "delivered", "timestamp": "2024-01-01"}
            ],
        }

        # Mock user info for testing
        self.mock_user_info: dict[str, dict[str, str | None]] = {
            "owner": {"name": "John Doe", "email": "john@example.com"},
            "pi": {"name": "Jane Smith", "email": "jane@example.com"},
        }

        # Mock NGI report data
        self.mock_ngi_report = {
            "report_id": "R002",
            "status": "delivered",
            "timestamp": "2024-01-02",
        }

    def tearDown(self):
        """Clean up singleton instances after each test."""
        from lib.couchdb.couchdb_connection import CouchDBConnectionManager

        if CouchDBConnectionManager in SingletonMeta._instances:
            del SingletonMeta._instances[CouchDBConnectionManager]

    @patch("lib.couchdb.yggdrasil_db_manager.CouchDBHandler.__init__")
    def test_init_success(self, mock_handler_init):
        """Test successful initialization of YggdrasilDBManager."""
        # Arrange
        mock_handler_init.return_value = None

        # Act
        YggdrasilDBManager()

        # Assert
        mock_handler_init.assert_called_once_with("yggdrasil")

    @patch("lib.couchdb.yggdrasil_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.yggdrasil_db_manager.YggdrasilDocument")
    @patch("lib.couchdb.yggdrasil_db_manager.logging")
    def test_create_project_success(
        self, mock_logging, mock_ygg_doc_class, mock_handler_init
    ):
        """Test successful project creation."""
        # Arrange
        mock_handler_init.return_value = None
        mock_ygg_doc = MagicMock()
        mock_ygg_doc_class.return_value = mock_ygg_doc

        manager = YggdrasilDBManager()
        manager.save_document = MagicMock()

        # Act
        result = manager.create_project(
            project_id="P12345",
            projects_reference="ref_12345",
            project_name="Test Project",
            method="10X",
            user_info=self.mock_user_info,
            sensitive=True,
        )

        # Assert
        mock_ygg_doc_class.assert_called_once_with(
            project_id="P12345",
            projects_reference="ref_12345",
            project_name="Test Project",
            method="10X",
        )
        # Check that user info and sensitive flag were set
        self.assertEqual(mock_ygg_doc.user_info, self.mock_user_info)
        mock_ygg_doc.delivery_info.__setitem__.assert_called_with("sensitive", True)
        manager.save_document.assert_called_once_with(mock_ygg_doc)
        mock_logging.info.assert_called_with(
            "New project with ID 'P12345' created successfully."
        )
        self.assertEqual(result, mock_ygg_doc)

    @patch("lib.couchdb.yggdrasil_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.yggdrasil_db_manager.YggdrasilDocument")
    def test_create_project_without_user_info(
        self, mock_ygg_doc_class, mock_handler_init
    ):
        """Test project creation without user info."""
        # Arrange
        mock_handler_init.return_value = None
        mock_ygg_doc = MagicMock()
        mock_ygg_doc_class.return_value = mock_ygg_doc

        manager = YggdrasilDBManager()
        manager.save_document = MagicMock()

        # Act
        result = manager.create_project(
            project_id="P12345",
            projects_reference="ref_12345",
            project_name="Test Project",
            method="10X",
        )

        # Assert
        # Verify the document was created correctly
        mock_ygg_doc_class.assert_called_once_with(
            project_id="P12345",
            projects_reference="ref_12345",
            project_name="Test Project",
            method="10X",
        )
        # sensitive should default to True
        mock_ygg_doc.delivery_info.__setitem__.assert_called_with("sensitive", True)
        # Document should be saved
        manager.save_document.assert_called_once_with(mock_ygg_doc)
        # Should return the created document
        self.assertEqual(result, mock_ygg_doc)

    @patch("lib.couchdb.yggdrasil_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.yggdrasil_db_manager.logging")
    def test_save_document_new_document(self, mock_logging, mock_handler_init):
        """Test saving a new document (no existing _rev)."""
        # Arrange
        mock_handler_init.return_value = None
        mock_server = MagicMock()

        # Mock get_document to raise 404 (document doesn't exist)
        api_exception = MockApiException("Not Found", code=404)
        mock_server.get_document.side_effect = api_exception

        # Mock successful put_document
        mock_server.put_document.return_value.get_result.return_value = {"ok": True}

        manager = YggdrasilDBManager()
        manager.server = mock_server
        manager.db_name = "yggdrasil"

        mock_doc = MagicMock()
        mock_doc._id = "P12345"
        mock_doc.to_dict.return_value = {"_id": "P12345", "data": "test"}

        # Act
        manager.save_document(mock_doc)

        # Assert
        mock_server.get_document.assert_called_once_with(
            db="yggdrasil", doc_id="P12345"
        )
        mock_server.put_document.assert_called_once_with(
            db="yggdrasil", doc_id="P12345", document={"_id": "P12345", "data": "test"}
        )
        mock_logging.info.assert_called_with(
            "Document with ID 'P12345' saved successfully in 'yggdrasil' DB."
        )

    @patch("lib.couchdb.yggdrasil_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.yggdrasil_db_manager.logging")
    def test_save_document_existing_document(self, mock_logging, mock_handler_init):
        """Test saving an existing document (preserves _rev)."""
        # Arrange
        mock_handler_init.return_value = None
        mock_server = MagicMock()

        # Mock get_document to return existing document with _rev
        mock_server.get_document.return_value.get_result.return_value = {
            "_id": "P12345",
            "_rev": "1-abc123",
            "old": "data",
        }

        # Mock successful put_document
        mock_server.put_document.return_value.get_result.return_value = {"ok": True}

        manager = YggdrasilDBManager()
        manager.server = mock_server
        manager.db_name = "yggdrasil"

        mock_doc = MagicMock()
        mock_doc._id = "P12345"
        mock_doc.to_dict.return_value = {"_id": "P12345", "data": "test"}

        # Act
        manager.save_document(mock_doc)

        # Assert
        mock_server.get_document.assert_called_once_with(
            db="yggdrasil", doc_id="P12345"
        )
        expected_save_data = {"_id": "P12345", "data": "test", "_rev": "1-abc123"}
        mock_server.put_document.assert_called_once_with(
            db="yggdrasil", doc_id="P12345", document=expected_save_data
        )

    @patch("lib.couchdb.yggdrasil_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.yggdrasil_db_manager.logging")
    def test_save_document_exception(self, mock_logging, mock_handler_init):
        """Test save_document when an exception occurs."""
        # Arrange
        mock_handler_init.return_value = None
        mock_server = MagicMock()
        mock_server.get_document.side_effect = Exception("Database error")

        manager = YggdrasilDBManager()
        manager.server = mock_server
        manager.db_name = "yggdrasil"

        mock_doc = MagicMock()
        mock_doc._id = "P12345"

        # Act
        manager.save_document(mock_doc)

        # Assert
        mock_logging.error.assert_called_with("Error saving document: Database error")

    @patch("lib.couchdb.yggdrasil_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.yggdrasil_db_manager.YggdrasilDocument")
    def test_get_document_by_project_id_success(
        self, mock_ygg_doc_class, mock_handler_init
    ):
        """Test successful document retrieval by project ID."""
        # Arrange
        mock_handler_init.return_value = None
        mock_server = MagicMock()
        mock_server.get_document.return_value.get_result.return_value = (
            self.mock_project_data
        )

        mock_ygg_doc = MagicMock()
        mock_ygg_doc_class.from_dict.return_value = mock_ygg_doc

        manager = YggdrasilDBManager()
        manager.server = mock_server
        manager.db_name = "yggdrasil"

        # Act
        result = manager.get_document_by_project_id("P12345")

        # Assert
        mock_server.get_document.assert_called_once_with(
            db="yggdrasil", doc_id="P12345"
        )
        mock_ygg_doc_class.from_dict.assert_called_once_with(self.mock_project_data)
        self.assertEqual(result, mock_ygg_doc)

    @patch("lib.couchdb.yggdrasil_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.yggdrasil_db_manager.logging")
    def test_get_document_by_project_id_not_found(
        self, mock_logging, mock_handler_init
    ):
        """Test document retrieval when document doesn't exist."""
        # Arrange
        mock_handler_init.return_value = None
        mock_server = MagicMock()

        # Mock ApiException for 404 (document not found)
        api_exception = MockApiException("Not Found", code=404)
        mock_server.get_document.side_effect = api_exception

        manager = YggdrasilDBManager()
        manager.server = mock_server
        manager.db_name = "yggdrasil"

        # Act
        result = manager.get_document_by_project_id("nonexistent")

        # Assert
        self.assertIsNone(result)
        mock_logging.info.assert_called_with("Project with ID 'nonexistent' not found.")

    @patch("lib.couchdb.yggdrasil_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.yggdrasil_db_manager.logging")
    def test_get_document_by_project_id_exception(
        self, mock_logging, mock_handler_init
    ):
        """Test document retrieval when an exception occurs."""
        # Arrange
        mock_handler_init.return_value = None
        mock_server = MagicMock()
        mock_server.get_document.side_effect = Exception("Database error")

        manager = YggdrasilDBManager()
        manager.server = mock_server
        manager.db_name = "yggdrasil"

        # Act
        result = manager.get_document_by_project_id("P12345")

        # Assert
        self.assertIsNone(result)
        mock_logging.error.assert_called_with(
            "Error accessing project 'P12345': Database error"
        )

    @patch("lib.couchdb.yggdrasil_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.yggdrasil_db_manager.logging")
    def test_get_document_by_project_id_api_exception_other_codes(
        self, mock_logging, mock_handler_init
    ):
        """Test document retrieval when API exception with non-404 code occurs."""
        # Arrange
        mock_handler_init.return_value = None
        mock_server = MagicMock()

        # Mock ApiException for 500 (server error)
        api_exception = MockApiException("Internal Server Error", code=500)
        mock_server.get_document.side_effect = api_exception

        manager = YggdrasilDBManager()
        manager.server = mock_server
        manager.db_name = "yggdrasil"

        # Act
        result = manager.get_document_by_project_id("P12345")

        # Assert
        self.assertIsNone(result)
        mock_logging.error.assert_called_with(
            "Error accessing project 'P12345': 500 Internal Server Error"
        )

    @patch("lib.couchdb.yggdrasil_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.yggdrasil_db_manager.logging")
    def test_check_project_exists_true(self, mock_logging, mock_handler_init):
        """Test check_project_exists when project exists."""
        # Arrange
        mock_handler_init.return_value = None
        manager = YggdrasilDBManager()

        mock_doc = MagicMock()
        manager.get_document_by_project_id = MagicMock(return_value=mock_doc)

        # Act
        result = manager.check_project_exists("P12345")

        # Assert
        self.assertTrue(result)
        manager.get_document_by_project_id.assert_called_once_with("P12345")
        mock_logging.info.assert_called_with("Project with ID 'P12345' exists.")

    @patch("lib.couchdb.yggdrasil_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.yggdrasil_db_manager.logging")
    def test_check_project_exists_false(self, mock_logging, mock_handler_init):
        """Test check_project_exists when project doesn't exist."""
        # Arrange
        mock_handler_init.return_value = None
        manager = YggdrasilDBManager()

        manager.get_document_by_project_id = MagicMock(return_value=None)

        # Act
        result = manager.check_project_exists("nonexistent")

        # Assert
        self.assertFalse(result)
        manager.get_document_by_project_id.assert_called_once_with("nonexistent")
        mock_logging.info.assert_called_with(
            "Project with ID 'nonexistent' does not exist."
        )

    @patch("lib.couchdb.yggdrasil_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.yggdrasil_db_manager.logging")
    def test_add_sample_success(self, mock_logging, mock_handler_init):
        """Test successful sample addition using the decorated method."""
        # Arrange
        mock_handler_init.return_value = None
        mock_doc = MagicMock()

        manager = YggdrasilDBManager()
        manager.get_document_by_project_id = MagicMock(return_value=mock_doc)
        manager.save_document = MagicMock()

        # Act
        manager.add_sample("P12345", "S003", "pending")

        # Assert
        manager.get_document_by_project_id.assert_called_once_with("P12345")
        mock_doc.add_sample.assert_called_once_with(sample_id="S003", status="pending")
        manager.save_document.assert_called_once_with(mock_doc)
        mock_logging.info.assert_called_with(
            "Sample 'S003' added with status 'pending'."
        )

    @patch("lib.couchdb.yggdrasil_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.yggdrasil_db_manager.logging")
    def test_add_sample_project_not_found(self, mock_logging, mock_handler_init):
        """Test add_sample when project doesn't exist."""
        # Arrange
        mock_handler_init.return_value = None

        manager = YggdrasilDBManager()
        manager.get_document_by_project_id = MagicMock(return_value=None)
        manager.save_document = MagicMock()

        # Act
        result = manager.add_sample("nonexistent", "S003", "pending")

        # Assert
        self.assertIsNone(result)
        manager.get_document_by_project_id.assert_called_once_with("nonexistent")
        manager.save_document.assert_not_called()
        mock_logging.error.assert_called_with(
            "Project 'nonexistent' not found in Yggdrasil DB."
        )

    @patch("lib.couchdb.yggdrasil_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.yggdrasil_db_manager.logging")
    def test_add_sample_exception_in_method(self, mock_logging, mock_handler_init):
        """Test add_sample when an exception occurs in the decorated method."""
        # Arrange
        mock_handler_init.return_value = None
        mock_doc = MagicMock()
        mock_doc.add_sample.side_effect = Exception("Sample error")

        manager = YggdrasilDBManager()
        manager.get_document_by_project_id = MagicMock(return_value=mock_doc)
        manager.save_document = MagicMock()

        # Act
        result = manager.add_sample("P12345", "S003", "pending")

        # Assert
        self.assertIsNone(result)
        manager.save_document.assert_not_called()
        mock_logging.error.assert_called_with(
            "Error in add_sample for project P12345: Sample error"
        )

    @patch("lib.couchdb.yggdrasil_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.yggdrasil_db_manager.logging")
    def test_update_sample_status_success(self, mock_logging, mock_handler_init):
        """Test successful sample status update."""
        # Arrange
        mock_handler_init.return_value = None
        mock_doc = MagicMock()

        manager = YggdrasilDBManager()
        manager.get_document_by_project_id = MagicMock(return_value=mock_doc)
        manager.save_document = MagicMock()

        # Act
        manager.update_sample_status("P12345", "S001", "completed")

        # Assert
        manager.get_document_by_project_id.assert_called_once_with("P12345")
        mock_doc.update_sample_status.assert_called_once_with(
            sample_id="S001", status="completed"
        )
        manager.save_document.assert_called_once_with(mock_doc)
        mock_logging.info.assert_called_with(
            "Sample 'S001' status updated to 'completed'."
        )

    @patch("lib.couchdb.yggdrasil_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.yggdrasil_db_manager.logging")
    def test_add_ngi_report_entry_success(self, mock_logging, mock_handler_init):
        """Test successful NGI report entry addition."""
        # Arrange
        mock_handler_init.return_value = None
        mock_doc = MagicMock()
        mock_doc.add_ngi_report_entry.return_value = True

        manager = YggdrasilDBManager()
        manager.get_document_by_project_id = MagicMock(return_value=mock_doc)
        manager.save_document = MagicMock()

        # Act
        result = manager.add_ngi_report_entry("P12345", self.mock_ngi_report)

        # Assert
        self.assertTrue(result)
        manager.get_document_by_project_id.assert_called_once_with("P12345")
        mock_doc.add_ngi_report_entry.assert_called_once_with(self.mock_ngi_report)
        manager.save_document.assert_called_once_with(mock_doc)
        mock_logging.info.assert_called_with("NGI report entry added to the document.")

    @patch("lib.couchdb.yggdrasil_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.yggdrasil_db_manager.logging")
    def test_add_ngi_report_entry_failure(self, mock_logging, mock_handler_init):
        """Test NGI report entry addition when document method returns False."""
        # Arrange
        mock_handler_init.return_value = None
        mock_doc = MagicMock()
        mock_doc.add_ngi_report_entry.return_value = False

        manager = YggdrasilDBManager()
        manager.get_document_by_project_id = MagicMock(return_value=mock_doc)
        manager.save_document = MagicMock()

        # Act
        result = manager.add_ngi_report_entry("P12345", self.mock_ngi_report)

        # Assert
        self.assertFalse(result)
        mock_doc.add_ngi_report_entry.assert_called_once_with(self.mock_ngi_report)
        manager.save_document.assert_called_once_with(
            mock_doc
        )  # Still saves even if method returns False
        mock_logging.warning.assert_called_with(
            "NGI report entry failed to be added to the document."
        )

    @patch("lib.couchdb.yggdrasil_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.yggdrasil_db_manager.logging")
    def test_update_sample_slurm_job_id_success(self, mock_logging, mock_handler_init):
        """Test successful SLURM job ID update."""
        # Arrange
        mock_handler_init.return_value = None
        mock_doc = MagicMock()
        mock_doc.update_sample_field.return_value = True

        manager = YggdrasilDBManager()
        manager.get_document_by_project_id = MagicMock(return_value=mock_doc)
        manager.save_document = MagicMock()

        # Act
        manager.update_sample_slurm_job_id("P12345", "S001", "789012")

        # Assert
        manager.get_document_by_project_id.assert_called_once_with("P12345")
        mock_doc.update_sample_field.assert_called_once_with(
            "S001", "slurm_job_id", "789012"
        )
        manager.save_document.assert_called_once_with(mock_doc)
        mock_logging.info.assert_called_with(
            "Sample 'S001' slurm_job_id set to '789012'."
        )

    @patch("lib.couchdb.yggdrasil_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.yggdrasil_db_manager.logging")
    def test_update_sample_slurm_job_id_failure(self, mock_logging, mock_handler_init):
        """Test SLURM job ID update when document method returns False."""
        # Arrange
        mock_handler_init.return_value = None
        mock_doc = MagicMock()
        mock_doc.update_sample_field.return_value = False

        manager = YggdrasilDBManager()
        manager.get_document_by_project_id = MagicMock(return_value=mock_doc)
        manager.save_document = MagicMock()

        # Act
        manager.update_sample_slurm_job_id("P12345", "S001", "789012")

        # Assert
        mock_doc.update_sample_field.assert_called_once_with(
            "S001", "slurm_job_id", "789012"
        )
        manager.save_document.assert_called_once_with(
            mock_doc
        )  # Still saves even if method returns False
        mock_logging.warning.assert_called_with(
            "Failed to update slurm_job_id for sample 'S001'."
        )

    @patch("lib.couchdb.yggdrasil_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.yggdrasil_db_manager.YggdrasilDocument")
    @patch("lib.couchdb.yggdrasil_db_manager.logging")
    def test_create_project_with_default_sensitive_flag(
        self, mock_logging, mock_ygg_doc_class, mock_handler_init
    ):
        """Test project creation with default sensitive flag."""
        # Arrange
        mock_handler_init.return_value = None
        mock_ygg_doc = MagicMock()
        mock_ygg_doc_class.return_value = mock_ygg_doc

        manager = YggdrasilDBManager()
        manager.save_document = MagicMock()

        # Act - not passing sensitive parameter to test default
        manager.create_project(
            project_id="P12345",
            projects_reference="ref_12345",
            project_name="Test Project",
            method="10X",
        )

        # Assert
        # sensitive should default to True when not specified
        mock_ygg_doc.delivery_info.__setitem__.assert_called_with("sensitive", True)

    @patch("lib.couchdb.yggdrasil_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.yggdrasil_db_manager.YggdrasilDocument")
    def test_create_project_with_sensitive_false(
        self, mock_ygg_doc_class, mock_handler_init
    ):
        """Test project creation with sensitive flag set to False."""
        # Arrange
        mock_handler_init.return_value = None
        mock_ygg_doc = MagicMock()
        mock_ygg_doc_class.return_value = mock_ygg_doc

        manager = YggdrasilDBManager()
        manager.save_document = MagicMock()

        # Act
        manager.create_project(
            project_id="P12345",
            projects_reference="ref_12345",
            project_name="Test Project",
            method="10X",
            sensitive=False,
        )

        # Assert
        mock_ygg_doc.delivery_info.__setitem__.assert_called_with("sensitive", False)


class TestAutoLoadAndSaveDecorator(unittest.TestCase):
    """Test the auto_load_and_save decorator functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_manager = MagicMock()
        self.mock_doc = MagicMock()

    def test_decorator_success(self):
        """Test decorator with successful method execution."""
        # Arrange
        self.mock_manager.get_document_by_project_id.return_value = self.mock_doc
        self.mock_manager.save_document = MagicMock()

        @auto_load_and_save
        def test_method(manager, doc, arg1, kwarg1=None):
            return f"result_{arg1}_{kwarg1}"

        # Act
        result = test_method(
            self.mock_manager, "P12345", "test_arg", kwarg1="test_kwarg"
        )

        # Assert
        self.assertEqual(result, "result_test_arg_test_kwarg")
        self.mock_manager.get_document_by_project_id.assert_called_once_with("P12345")
        self.mock_manager.save_document.assert_called_once_with(self.mock_doc)

    @patch("lib.couchdb.yggdrasil_db_manager.logging")
    def test_decorator_project_not_found(self, mock_logging):
        """Test decorator when project is not found."""
        # Arrange
        self.mock_manager.get_document_by_project_id.return_value = None

        @auto_load_and_save
        def test_method(manager, doc, arg1):
            return f"result_{arg1}"

        # Act
        result = test_method(self.mock_manager, "nonexistent", "test_arg")

        # Assert
        self.assertIsNone(result)
        self.mock_manager.get_document_by_project_id.assert_called_once_with(
            "nonexistent"
        )
        mock_logging.error.assert_called_with(
            "Project 'nonexistent' not found in Yggdrasil DB."
        )

    @patch("lib.couchdb.yggdrasil_db_manager.logging")
    def test_decorator_method_exception(self, mock_logging):
        """Test decorator when the wrapped method raises an exception."""
        # Arrange
        self.mock_manager.get_document_by_project_id.return_value = self.mock_doc
        self.mock_manager.save_document = MagicMock()

        @auto_load_and_save
        def test_method(manager, doc, arg1):
            raise Exception("Method error")

        # Act
        result = test_method(self.mock_manager, "P12345", "test_arg")

        # Assert
        self.assertIsNone(result)
        self.mock_manager.get_document_by_project_id.assert_called_once_with("P12345")
        self.mock_manager.save_document.assert_not_called()
        mock_logging.error.assert_called_with(
            "Error in test_method for project P12345: Method error"
        )


class TestEdgeCasesAndIntegration(unittest.TestCase):
    """Test edge cases and integration scenarios for YggdrasilDBManager."""

    def setUp(self):
        """Set up test fixtures for edge case testing."""
        # Clear singleton instances to ensure test isolation
        from lib.couchdb.couchdb_connection import CouchDBConnectionManager

        if CouchDBConnectionManager in SingletonMeta._instances:
            del SingletonMeta._instances[CouchDBConnectionManager]

    def tearDown(self):
        """Clean up singleton instances after each test."""
        from lib.couchdb.couchdb_connection import CouchDBConnectionManager

        if CouchDBConnectionManager in SingletonMeta._instances:
            del SingletonMeta._instances[CouchDBConnectionManager]

    @patch("lib.couchdb.yggdrasil_db_manager.CouchDBHandler.__init__")
    def test_decorated_method_with_return_value(self, mock_handler_init):
        """Test decorated method that returns a value gets passed through correctly."""
        # Arrange
        mock_handler_init.return_value = None
        mock_doc = MagicMock()

        manager = YggdrasilDBManager()
        manager.get_document_by_project_id = MagicMock(return_value=mock_doc)
        manager.save_document = MagicMock()

        # Act
        result = manager.add_sample("P12345", "S001", "pending")

        # Assert - the decorator should pass through the return value from the method
        # In this case, add_sample doesn't explicitly return anything, so None is expected
        self.assertIsNone(result)

    @patch("lib.couchdb.yggdrasil_db_manager.CouchDBHandler.__init__")
    @patch("lib.couchdb.yggdrasil_db_manager.logging")
    def test_save_document_with_put_document_exception(
        self, mock_logging, mock_handler_init
    ):
        """Test save_document when put_document raises an exception."""
        # Arrange
        mock_handler_init.return_value = None
        mock_server = MagicMock()

        # Mock get_document succeeds, but put_document fails
        mock_server.get_document.return_value.get_result.return_value = {
            "_id": "P12345",
            "_rev": "1-abc",
        }
        mock_server.put_document.side_effect = Exception("Put failed")

        manager = YggdrasilDBManager()
        manager.server = mock_server
        manager.db_name = "yggdrasil"

        mock_doc = MagicMock()
        mock_doc._id = "P12345"
        mock_doc.to_dict.return_value = {"_id": "P12345", "data": "test"}

        # Act
        manager.save_document(mock_doc)

        # Assert
        mock_logging.error.assert_called_with("Error saving document: Put failed")

    @patch("lib.couchdb.yggdrasil_db_manager.CouchDBHandler.__init__")
    def test_multiple_decorated_methods_same_project(self, mock_handler_init):
        """Test multiple decorated method calls on the same project work correctly."""
        # Arrange
        mock_handler_init.return_value = None
        mock_doc = MagicMock()

        manager = YggdrasilDBManager()
        manager.get_document_by_project_id = MagicMock(return_value=mock_doc)
        manager.save_document = MagicMock()

        # Act - call multiple decorated methods on the same project
        manager.add_sample("P12345", "S001", "pending")
        manager.update_sample_status("P12345", "S001", "completed")
        manager.update_sample_slurm_job_id("P12345", "S001", "12345")

        # Assert - verify each method was called and document was saved each time
        self.assertEqual(manager.get_document_by_project_id.call_count, 3)
        self.assertEqual(manager.save_document.call_count, 3)
        mock_doc.add_sample.assert_called_once_with(sample_id="S001", status="pending")
        mock_doc.update_sample_status.assert_called_once_with(
            sample_id="S001", status="completed"
        )
        mock_doc.update_sample_field.assert_called_once_with(
            "S001", "slurm_job_id", "12345"
        )


if __name__ == "__main__":
    unittest.main()
