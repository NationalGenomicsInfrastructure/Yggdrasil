import unittest
from unittest.mock import ANY, MagicMock, patch

from yggdrasil.storage.couchdb.yggdrasil_db_manager import YggdrasilDBManager


class TestYggdrasilDBManager(unittest.TestCase):
    """Tests for the YggdrasilDBManager constructor."""

    @patch("yggdrasil.storage.couchdb.yggdrasil_db_manager.resolve_couchdb_params")
    @patch("yggdrasil.storage.couchdb.yggdrasil_db_manager.CouchDBHandler.__init__")
    def test_init_success(self, mock_handler_init, mock_get_config):
        """Test successful initialization of YggdrasilDBManager."""
        # Arrange
        mock_handler_init.return_value = None
        mock_params = MagicMock(
            url="http://localhost:5984", user_env="COUCH_USER", pass_env="COUCH_PASS"
        )
        mock_get_config.return_value = mock_params

        # Act
        YggdrasilDBManager()

        # Assert
        mock_handler_init.assert_called_once_with(
            "yggdrasil",
            url="http://localhost:5984",
            user_env="COUCH_USER",
            pass_env="COUCH_PASS",
            logger=ANY,
        )


if __name__ == "__main__":
    unittest.main()
