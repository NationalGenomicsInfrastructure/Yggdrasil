import json
import types
import unittest
from pathlib import Path
from unittest.mock import mock_open, patch

from yggdrasil.config.loader import ConfigLoader


class TestConfigLoader(unittest.TestCase):

    def setUp(self):
        # Create a ConfigLoader instance for testing
        self.config_loader = ConfigLoader()
        self.mock_config_data = {"key1": "value1", "key2": "value2"}
        self.mock_config_json = json.dumps(self.mock_config_data)

    def test_init(self):
        # Test that _config is initialized to None
        self.assertIsNone(self.config_loader._config)

    def test_load_config_success(self):
        # Test loading config from a file name using Ygg.get_path
        with (
            patch("yggdrasil.config.loader.Ygg.get_path") as mock_get_path,
            patch("builtins.open", mock_open(read_data=self.mock_config_json)),
        ):
            mock_get_path.return_value = Path("/path/to/config.json")
            config = self.config_loader.load_config("config.json")
            self.assertEqual(config, types.MappingProxyType(self.mock_config_data))
            self.assertEqual(
                self.config_loader._config,
                types.MappingProxyType(self.mock_config_data),
            )

    def test_load_config_path_success(self):
        # Test loading config from a full path
        with patch("builtins.open", mock_open(read_data=self.mock_config_json)):
            config = self.config_loader.load_config_path("/path/to/config.json")
            self.assertEqual(config, types.MappingProxyType(self.mock_config_data))
            self.assertEqual(
                self.config_loader._config,
                types.MappingProxyType(self.mock_config_data),
            )

    def test_load_config_file_not_found(self):
        # Test behavior when config file is not found
        with patch("yggdrasil.config.loader.Ygg.get_path") as mock_get_path:
            mock_get_path.return_value = None
            config = self.config_loader.load_config("nonexistent.json")
            self.assertEqual(config, types.MappingProxyType({}))
            self.assertEqual(self.config_loader._config, types.MappingProxyType({}))

    def test_load_config_path_file_not_found(self):
        # Test behavior when config file path is invalid
        with patch("pathlib.Path.open", side_effect=FileNotFoundError()):
            with self.assertRaises(FileNotFoundError):
                self.config_loader.load_config_path("/invalid/path/config.json")

    def test_load_config_invalid_json(self):
        # Test behavior when config file contains invalid JSON
        invalid_json = "{key1: value1"  # Missing quotes and closing brace
        with (
            patch("yggdrasil.config.loader.Ygg.get_path") as mock_get_path,
            patch("builtins.open", mock_open(read_data=invalid_json)),
        ):
            mock_get_path.return_value = Path("/path/to/config.json")
            with self.assertRaises(json.JSONDecodeError):
                self.config_loader.load_config("config.json")

    def test_load_config_empty_file(self):
        # Test behavior when config file is empty
        empty_json = ""
        with (
            patch("yggdrasil.config.loader.Ygg.get_path") as mock_get_path,
            patch("builtins.open", mock_open(read_data=empty_json)),
        ):
            mock_get_path.return_value = Path("/path/to/config.json")
            with self.assertRaises(json.JSONDecodeError):
                self.config_loader.load_config("config.json")

    def test_getitem_existing_key(self):
        # Test __getitem__ with an existing key
        self.config_loader._config = types.MappingProxyType(self.mock_config_data)
        self.assertEqual(self.config_loader["key1"], "value1")

    def test_getitem_nonexistent_key(self):
        # Test __getitem__ with a nonexistent key
        self.config_loader._config = types.MappingProxyType(self.mock_config_data)
        self.assertIsNone(self.config_loader["nonexistent_key"])

    def test_getitem_no_config_loaded(self):
        # Test __getitem__ when no config has been loaded
        self.config_loader._config = None
        self.assertIsNone(self.config_loader["key1"])

    def test_config_immutable(self):
        # Test that the configuration data is immutable
        self.config_loader._config = types.MappingProxyType(self.mock_config_data)
        with self.assertRaises(TypeError):
            self.config_loader._config["key1"] = "new_value"  # type: ignore

    def test_load_config_type_error(self):
        # Test handling of TypeError during json.load
        # Clear cache to avoid interference from other tests
        ConfigLoader._cache.clear()
        with (
            patch("yggdrasil.config.loader.Ygg.get_path") as mock_get_path,
            patch("builtins.open", mock_open(read_data=self.mock_config_json)),
            patch("json.load", side_effect=TypeError("Type error")),
        ):
            mock_get_path.return_value = Path("/path/to/config.json")
            with self.assertRaises(TypeError):
                self.config_loader.load_config("config.json")

    def test_load_config_unexpected_exception(self):
        # Test handling of an unexpected exception during file loading
        with (
            patch("yggdrasil.config.loader.Ygg.get_path") as mock_get_path,
            patch("builtins.open", side_effect=Exception("Unexpected error")),
        ):
            mock_get_path.return_value = Path("/path/to/config.json")
            with self.assertRaises(Exception) as context:
                self.config_loader.load_config("config.json")
            self.assertEqual(str(context.exception), "Unexpected error")

    def test_load_config_path_unexpected_exception(self):
        # Test handling of an unexpected exception during file loading with load_config_path
        # Clear cache to avoid interference from other tests
        ConfigLoader._cache.clear()
        with patch("builtins.open", side_effect=Exception("Unexpected error")):
            with self.assertRaises(Exception) as context:
                self.config_loader.load_config_path("/path/to/config.json")
            self.assertEqual(str(context.exception), "Unexpected error")

    # Removed test_config_manager_instance: config_manager no longer exists

    # Removed test_configs_loaded: configs attribute no longer exists in config_loader

    def test_load_config_with_directory_traversal(self):
        # Test that directory traversal in file_name is handled safely
        with patch("yggdrasil.config.loader.Ygg.get_path") as mock_get_path:
            mock_get_path.return_value = None  # Simulate blocked traversal
            config = self.config_loader.load_config("../../../etc/passwd")
            self.assertEqual(config, types.MappingProxyType({}))
            self.assertEqual(self.config_loader._config, types.MappingProxyType({}))

    def test_load_config_path_with_invalid_path(self):
        # Test that invalid paths in load_config_path are handled
        with patch("pathlib.Path.open", side_effect=FileNotFoundError()):
            with self.assertRaises(FileNotFoundError):
                self.config_loader.load_config_path("/invalid/path/../../etc/passwd")


if __name__ == "__main__":
    unittest.main()
