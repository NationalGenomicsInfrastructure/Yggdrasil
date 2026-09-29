import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

from lib.core_utils.common import YggdrasilUtilities


class TestYggdrasilUtilities(unittest.TestCase):

    def setUp(self):
        # Backup original values
        self.original_module_cache = YggdrasilUtilities.module_cache.copy()
        self.original_config_dir = YggdrasilUtilities.CONFIG_DIR
        self.original_ygg_home = os.environ.get("YGG_HOME")

        # Reset module cache
        YggdrasilUtilities.module_cache = {}
        os.environ.pop("YGG_HOME", None)

        # Use a temporary config directory
        self.temp_config_dir = Path("/tmp/yggdrasil_test_config")
        self.temp_config_dir.mkdir(parents=True, exist_ok=True)
        YggdrasilUtilities.CONFIG_DIR = self.temp_config_dir

    def tearDown(self):
        # Restore original values
        YggdrasilUtilities.module_cache = self.original_module_cache
        YggdrasilUtilities.CONFIG_DIR = self.original_config_dir
        if self.original_ygg_home is None:
            os.environ.pop("YGG_HOME", None)
        else:
            os.environ["YGG_HOME"] = self.original_ygg_home

        # Clean up temporary config directory
        for item in self.temp_config_dir.glob("*"):
            item.unlink()
        self.temp_config_dir.rmdir()

    def test_workspace_path_defaults_to_local_workspace(self):
        expected = Path(__file__).parent.parent / "yggdrasil_workspace"

        result = YggdrasilUtilities.workspace_path()

        self.assertEqual(result, expected)

    def test_workspace_path_uses_ygg_home(self):
        ygg_home = "/tmp/yggdrasil_hpc_workspace"

        with patch.dict(os.environ, {"YGG_HOME": ygg_home}):
            result = YggdrasilUtilities.workspace_path()

        self.assertEqual(result, Path(ygg_home))

    def test_config_dir_uses_ygg_home(self):
        ygg_home = "/tmp/yggdrasil_hpc_workspace"

        with patch.dict(os.environ, {"YGG_HOME": ygg_home}):
            result = YggdrasilUtilities.config_dir()

        self.assertEqual(result, Path(ygg_home) / "common/configurations")

    def test_config_dir_defaults_to_config_dir_attribute(self):
        result = YggdrasilUtilities.config_dir()

        self.assertEqual(result, self.temp_config_dir)

    @patch("importlib.import_module")
    def test_load_realm_class_success(self, mock_import_module):
        # Mock module and class
        mock_module = MagicMock()
        mock_class = MagicMock()
        setattr(mock_module, "MockClass", mock_class)
        mock_import_module.return_value = mock_module

        module_path = "some.module.MockClass"
        result = YggdrasilUtilities.load_realm_class(module_path)

        self.assertEqual(result, mock_class)
        self.assertIn(module_path, YggdrasilUtilities.module_cache)
        mock_import_module.assert_called_with("some.module")

    @patch("importlib.import_module")
    def test_load_realm_class_module_not_found(self, mock_import_module):
        # Simulate ImportError
        mock_import_module.side_effect = ImportError("Module not found")

        module_path = "nonexistent.module.ClassName"
        result = YggdrasilUtilities.load_realm_class(module_path)

        self.assertIsNone(result)
        mock_import_module.assert_called_with("nonexistent.module")

    @patch("importlib.import_module")
    def test_load_realm_class_attribute_error(self, mock_import_module):
        # Creating a mock module with no attributes allowed
        # means any attribute access raises AttributeError.
        mock_module = MagicMock(spec=[])

        mock_import_module.return_value = mock_module

        module_path = "some.module.MissingClass"
        result = YggdrasilUtilities.load_realm_class(module_path)
        self.assertIsNone(result)
        mock_import_module.assert_called_with("some.module")

    @patch("importlib.import_module")
    def test_load_module_success(self, mock_import_module):
        # Mock module
        mock_module = MagicMock()
        mock_import_module.return_value = mock_module

        module_path = "some.module"
        result = YggdrasilUtilities.load_module(module_path)

        self.assertEqual(result, mock_module)
        self.assertIn(module_path, YggdrasilUtilities.module_cache)
        mock_import_module.assert_called_with("some.module")

    @patch("importlib.import_module")
    def test_load_module_import_error(self, mock_import_module):
        # Simulate ImportError
        mock_import_module.side_effect = ImportError("Module not found")

        module_path = "nonexistent.module"
        result = YggdrasilUtilities.load_module(module_path)

        self.assertIsNone(result)
        mock_import_module.assert_called_with("nonexistent.module")

    @patch("importlib.import_module")
    def test_load_realm_class_caching(self, mock_import_module):
        # First call: loads and caches the class
        mock_module = MagicMock()
        mock_import_module.return_value = mock_module
        module_path = "some.module.ExistingClass"
        first_result = YggdrasilUtilities.load_realm_class(module_path)
        self.assertIsNotNone(first_result)

        # Second call: should return the cached class without calling import_module again
        second_result = YggdrasilUtilities.load_realm_class(module_path)
        self.assertIs(first_result, second_result)
        mock_import_module.assert_called_once_with(
            "some.module"
        )  # Confirm only called once

    def test_get_path_file_exists(self):
        # Create a dummy config file
        file_name = "config.yaml"
        test_file = self.temp_config_dir / file_name
        test_file.touch()

        result = YggdrasilUtilities.get_path(file_name)

        self.assertEqual(result, test_file)

    def test_get_path_file_not_exists(self):
        file_name = "missing_config.yaml"
        result = YggdrasilUtilities.get_path(file_name)

        self.assertIsNone(result)

    def test_get_path_uses_ygg_home_config_dir(self):
        with tempfile.TemporaryDirectory() as workspace:
            ygg_home = Path(workspace)
            config_dir = ygg_home / "common/configurations"
            config_dir.mkdir(parents=True, exist_ok=True)
            test_file = config_dir / "config.yaml"
            test_file.touch()

            with patch.dict(os.environ, {"YGG_HOME": workspace}):
                result = YggdrasilUtilities.get_path("config.yaml")

            self.assertEqual(result, test_file)

    def test_env_variable_exists(self):
        with patch.dict(os.environ, {"TEST_ENV_VAR": "test_value"}):
            result = YggdrasilUtilities.env_variable("TEST_ENV_VAR")
            self.assertEqual(result, "test_value")

    def test_env_variable_not_exists_with_default(self):
        result = YggdrasilUtilities.env_variable(
            "NONEXISTENT_ENV_VAR", default="default_value"
        )
        self.assertEqual(result, "default_value")

    def test_env_variable_not_exists_no_default(self):
        result = YggdrasilUtilities.env_variable("NONEXISTENT_ENV_VAR")
        self.assertIsNone(result)

    @patch("builtins.open", new_callable=mock_open, read_data="123")
    def test_get_last_processed_seq_file_exists(self, mock_file):
        seq_file = self.temp_config_dir / ".last_processed_seq"
        seq_file.touch()

        with patch.object(YggdrasilUtilities, "get_path", return_value=seq_file):
            result = YggdrasilUtilities.get_last_processed_seq()

        self.assertEqual(result, "123")

    def test_get_last_processed_seq_file_not_exists(self):
        with patch.object(YggdrasilUtilities, "get_path", return_value=None):
            result = YggdrasilUtilities.get_last_processed_seq()
            self.assertEqual(result, "0")  # Default value as per method

    @patch("builtins.open", new_callable=mock_open)
    def test_save_last_processed_seq_success(self, mock_file):
        seq_file = self.temp_config_dir / ".last_processed_seq"

        with patch.object(YggdrasilUtilities, "get_path", return_value=seq_file):
            YggdrasilUtilities.save_last_processed_seq("456")

        mock_file.assert_called_with(seq_file, "w")
        mock_file().write.assert_called_with("456")

    def test_save_last_processed_seq_no_seq_file(self):
        with patch.object(YggdrasilUtilities, "get_path", return_value=None):
            # Should handle gracefully
            YggdrasilUtilities.save_last_processed_seq("789")

    def test_module_cache_persistence(self):
        # Mock module
        mock_module = MagicMock()
        with patch("importlib.import_module", return_value=mock_module) as mock_import:
            module_path = "some.module"

            # First call
            result1 = YggdrasilUtilities.load_module(module_path)
            # Second call should use cache
            result2 = YggdrasilUtilities.load_module(module_path)

            self.assertEqual(result1, result2)
            mock_import.assert_called_once_with("some.module")

    @patch("builtins.open", new_callable=mock_open, read_data="")
    def test_get_last_processed_seq_empty_file(self, mock_file):
        seq_file = self.temp_config_dir / ".last_processed_seq"
        seq_file.touch()

        with patch.object(YggdrasilUtilities, "get_path", return_value=seq_file):
            result = YggdrasilUtilities.get_last_processed_seq()

        self.assertEqual(result, "0")  # Assumes default when file is empty

    @patch("builtins.open", new_callable=mock_open, read_data="abc")
    def test_get_last_processed_seq_invalid_content(self, mock_file):
        seq_file = self.temp_config_dir / ".last_processed_seq"
        seq_file.touch()

        with patch.object(YggdrasilUtilities, "get_path", return_value=seq_file):
            result = YggdrasilUtilities.get_last_processed_seq()

        self.assertEqual(result, "abc")  # Returns content as-is

    @patch("builtins.open", side_effect=Exception("File error"))
    def test_get_last_processed_seq_file_error(self, mock_file):
        seq_file = self.temp_config_dir / ".last_processed_seq"

        with patch.object(YggdrasilUtilities, "get_path", return_value=seq_file):
            result = YggdrasilUtilities.get_last_processed_seq()
            self.assertEqual(result, "0")  # Should handle exception and return default

    @patch("builtins.open", side_effect=Exception("File error"))
    def test_save_last_processed_seq_file_error(self, mock_file):
        seq_file = self.temp_config_dir / ".last_processed_seq"

        with patch.object(YggdrasilUtilities, "get_path", return_value=seq_file):
            # Should handle exception gracefully
            YggdrasilUtilities.save_last_processed_seq("123")

    def test_get_path_with_relative_file_name(self):
        # Use relative path components in file name
        file_name = "../outside_config.yaml"
        result = YggdrasilUtilities.get_path(file_name)
        self.assertIsNone(result)  # Should not allow navigating outside config dir

    def test_get_path_with_absolute_file_name(self):
        # Use absolute path
        file_name = "/etc/passwd"
        result = YggdrasilUtilities.get_path(file_name)
        self.assertIsNone(result)  # Should not allow absolute paths

    @patch.object(YggdrasilUtilities, "CONFIG_DIR", Path("/some/base/path"))
    def test_get_path_resolve_error(self):
        # Mock the config_file.resolve() call to raise an Exception
        # to trigger the exception block in get_path
        with patch(
            "lib.core_utils.common.Path.resolve", side_effect=Exception("Resolve error")
        ):
            result = YggdrasilUtilities.get_path("somefile")
            self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
