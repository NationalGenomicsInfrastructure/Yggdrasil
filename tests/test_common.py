import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from yggdrasil.config import workspace


class TestWorkspacePaths(unittest.TestCase):

    def setUp(self):
        # Backup original values
        self.original_config_dir = workspace.CONFIG_DIR
        self.original_ygg_home = os.environ.get("YGG_HOME")

        os.environ.pop("YGG_HOME", None)

        # Use a temporary config directory
        self.temp_config_dir = Path("/tmp/yggdrasil_test_config")
        self.temp_config_dir.mkdir(parents=True, exist_ok=True)
        workspace.CONFIG_DIR = self.temp_config_dir

    def tearDown(self):
        # Restore original values
        workspace.CONFIG_DIR = self.original_config_dir
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

        result = workspace.workspace_path()

        self.assertEqual(result, expected)

    def test_workspace_path_uses_ygg_home(self):
        ygg_home = "/tmp/yggdrasil_hpc_workspace"

        with patch.dict(os.environ, {"YGG_HOME": ygg_home}):
            result = workspace.workspace_path()

        self.assertEqual(result, Path(ygg_home))

    def test_config_dir_uses_ygg_home(self):
        ygg_home = "/tmp/yggdrasil_hpc_workspace"

        with patch.dict(os.environ, {"YGG_HOME": ygg_home}):
            result = workspace.config_dir()

        self.assertEqual(result, Path(ygg_home) / "common/configurations")

    def test_config_dir_defaults_to_config_dir_attribute(self):
        result = workspace.config_dir()

        self.assertEqual(result, self.temp_config_dir)

    def test_get_path_file_exists(self):
        # Create a dummy config file
        file_name = "config.yaml"
        test_file = self.temp_config_dir / file_name
        test_file.touch()

        result = workspace.get_path(file_name)

        self.assertEqual(result, test_file)

    def test_get_path_file_not_exists(self):
        file_name = "missing_config.yaml"
        result = workspace.get_path(file_name)

        self.assertIsNone(result)

    def test_get_path_uses_ygg_home_config_dir(self):
        with tempfile.TemporaryDirectory() as tmp_home:
            ygg_home = Path(tmp_home)
            config_dir = ygg_home / "common/configurations"
            config_dir.mkdir(parents=True, exist_ok=True)
            test_file = config_dir / "config.yaml"
            test_file.touch()

            with patch.dict(os.environ, {"YGG_HOME": tmp_home}):
                result = workspace.get_path("config.yaml")

            self.assertEqual(result, test_file)

    def test_get_path_with_relative_file_name(self):
        # Use relative path components in file name
        file_name = "../outside_config.yaml"
        result = workspace.get_path(file_name)
        self.assertIsNone(result)  # Should not allow navigating outside config dir

    def test_get_path_with_absolute_file_name(self):
        # Use absolute path
        file_name = "/etc/passwd"
        result = workspace.get_path(file_name)
        self.assertIsNone(result)  # Should not allow absolute paths

    @patch.object(workspace, "CONFIG_DIR", Path("/some/base/path"))
    def test_get_path_resolve_error(self):
        # Mock the config_file.resolve() call to raise an Exception
        # to trigger the exception block in get_path
        with patch(
            "yggdrasil.config.workspace.Path.resolve",
            side_effect=Exception("Resolve error"),
        ):
            result = workspace.get_path("somefile")
            self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
