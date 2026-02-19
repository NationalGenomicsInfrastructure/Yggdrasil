import subprocess
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from lib.module_utils.report_transfer import transfer_report


class TestTransferReport(unittest.TestCase):

    def setUp(self):
        self.report_path = Path("/path/to/report")
        self.project_id = "project123"
        self.sample_id = "sample456"
        self.remote_dir_base = "/remote/destination"
        self.server = "example.com"
        self.user = "user"
        self.ssh_key = "/path/to/ssh_key"

    @patch("lib.module_utils.report_transfer.configs")
    @patch("lib.module_utils.report_transfer.subprocess.run")
    def test_transfer_report_success(self, mock_subprocess_run, mock_configs):
        # Set up configs
        mock_configs.__getitem__.return_value = {
            "server": self.server,
            "user": self.user,
            "destination": self.remote_dir_base,
            "ssh_key": self.ssh_key,
        }

        # Set up subprocess.run to succeed
        mock_subprocess_run.return_value = MagicMock(
            returncode=0, stdout="Transfer complete", stderr=""
        )

        # Call the function
        result = transfer_report(self.report_path, self.project_id, self.sample_id)

        # Assert the result is True
        self.assertTrue(result)

        # Assert subprocess.run was called with correct arguments
        expected_remote_dir = (
            f"{self.remote_dir_base}/{self.project_id}/{self.sample_id}"
        )
        expected_remote_path = f"{self.user}@{self.server}:{expected_remote_dir}/"
        expected_rsync_command = [
            "rsync",
            "-avz",
            "--rsync-path",
            f"mkdir -p '{expected_remote_dir}' && rsync",
            "-e",
            f"ssh -i {self.ssh_key}",
            str(self.report_path),
            expected_remote_path,
        ]
        mock_subprocess_run.assert_called_once_with(
            expected_rsync_command,
            check=True,
            text=True,
            capture_output=True,
        )

    @patch("lib.module_utils.report_transfer.configs")
    @patch("lib.module_utils.report_transfer.logger")
    def test_transfer_report_missing_config_key(self, mock_logging, mock_configs):
        # Set up configs to raise KeyError for missing 'server' key
        mock_configs.__getitem__.side_effect = KeyError("server")

        # Call the function
        result = transfer_report(self.report_path, self.project_id, self.sample_id)

        # Assert the result is False
        self.assertFalse(result)

        # Assert that logging.error was called with the missing key
        mock_logging.error.assert_called_with(
            "Missing configuration for report transfer: 'server'"
        )

    @patch("lib.module_utils.report_transfer.configs")
    @patch("lib.module_utils.report_transfer.subprocess.run")
    def test_transfer_report_subprocess_calledprocesserror(
        self, mock_subprocess_run, mock_configs
    ):
        # Set up configs
        mock_configs.__getitem__.return_value = {
            "server": self.server,
            "user": self.user,
            "destination": self.remote_dir_base,
            "ssh_key": self.ssh_key,
        }

        # Set up subprocess.run to raise CalledProcessError
        mock_subprocess_run.side_effect = subprocess.CalledProcessError(
            returncode=1, cmd="rsync", stderr="Error in rsync"
        )

        # Call the function
        result = transfer_report(self.report_path, self.project_id, self.sample_id)

        # Assert the result is False
        self.assertFalse(result)

        # Assert that subprocess.run was called
        mock_subprocess_run.assert_called_once()

    @patch("lib.module_utils.report_transfer.configs")
    @patch("lib.module_utils.report_transfer.subprocess.run")
    def test_transfer_report_general_exception(self, mock_subprocess_run, mock_configs):
        # Set up configs
        mock_configs.__getitem__.return_value = {
            "server": self.server,
            "user": self.user,
            "destination": self.remote_dir_base,
            "ssh_key": self.ssh_key,
        }

        # Set up subprocess.run to raise a general Exception
        mock_subprocess_run.side_effect = Exception("Unexpected error")

        # Mock logging
        with patch("lib.module_utils.report_transfer.logger") as mock_logging:
            # Call the function
            result = transfer_report(self.report_path, self.project_id, self.sample_id)

            # Assert the result is False
            self.assertFalse(result)

            # Assert that logging.error was called with the exception message
            mock_logging.error.assert_any_call(
                "Unexpected error during report transfer: Unexpected error"
            )
            mock_logging.error.assert_any_call(
                "RSYNC output: No output available due to early error."
            )

    @patch("lib.module_utils.report_transfer.configs")
    @patch("lib.module_utils.report_transfer.subprocess.run")
    @patch("lib.module_utils.report_transfer.logger")
    def test_transfer_report_general_exception_with_result(
        self, mock_logging, mock_subprocess_run, mock_configs
    ):
        # Set up a valid config
        mock_configs.__getitem__.return_value = {
            "server": self.server,
            "user": self.user,
            "destination": self.remote_dir_base,
            "ssh_key": self.ssh_key,
        }

        # Mock a successful subprocess run
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "Mocked RSYNC output"
        mock_subprocess_run.return_value = mock_result

        # Make logging.info raise an exception to simulate an error after success
        def info_side_effect(*args, **kwargs):
            raise Exception("Logging info error")

        mock_logging.info.side_effect = info_side_effect

        # Call the function
        result = transfer_report(self.report_path, self.project_id, self.sample_id)

        # Assert that the result is False because the exception should cause failure
        self.assertFalse(result)

        # Check that the unexpected error was logged
        # The code logs: "Unexpected error during report transfer: Logging info error"
        mock_logging.error.assert_any_call(
            "Unexpected error during report transfer: Logging info error"
        )

        # Check that the RSYNC output was logged
        mock_logging.error.assert_any_call("RSYNC output: Mocked RSYNC output")

    @patch("lib.module_utils.report_transfer.configs")
    @patch("lib.module_utils.report_transfer.subprocess.run")
    def test_transfer_report_no_ssh_key(self, mock_subprocess_run, mock_configs):
        # Set up configs without ssh_key
        mock_configs.__getitem__.return_value = {
            "server": self.server,
            "user": self.user,
            "destination": self.remote_dir_base,
            # ssh_key is optional
        }

        # Set up subprocess.run to succeed
        mock_subprocess_run.return_value = MagicMock(
            returncode=0, stdout="Transfer complete", stderr=""
        )

        # Call the function without sample_id
        result = transfer_report(self.report_path, self.project_id)

        # Assert the result is True
        self.assertTrue(result)

        # Assert subprocess.run was called with correct arguments
        expected_remote_dir = f"{self.remote_dir_base}/{self.project_id}"
        expected_remote_path = f"{self.user}@{self.server}:{expected_remote_dir}/"
        expected_rsync_command = [
            "rsync",
            "-avz",
            "--rsync-path",
            f"mkdir -p '{expected_remote_dir}' && rsync",
            "-e",
            "ssh",
            str(self.report_path),
            expected_remote_path,
        ]
        mock_subprocess_run.assert_called_once_with(
            expected_rsync_command,
            check=True,
            text=True,
            capture_output=True,
        )

    @patch("lib.module_utils.report_transfer.configs")
    @patch("lib.module_utils.report_transfer.subprocess.run")
    def test_transfer_report_without_sample_id(self, mock_subprocess_run, mock_configs):
        # Set up configs
        mock_configs.__getitem__.return_value = {
            "server": self.server,
            "user": self.user,
            "destination": self.remote_dir_base,
            "ssh_key": self.ssh_key,
        }

        # Set up subprocess.run to succeed
        mock_subprocess_run.return_value = MagicMock(
            returncode=0, stdout="Transfer complete", stderr=""
        )

        # Call the function without sample_id
        result = transfer_report(self.report_path, self.project_id)

        # Assert the result is True
        self.assertTrue(result)

        # Assert subprocess.run was called with correct arguments
        expected_remote_dir = f"{self.remote_dir_base}/{self.project_id}"
        expected_remote_path = f"{self.user}@{self.server}:{expected_remote_dir}/"
        expected_rsync_command = [
            "rsync",
            "-avz",
            "--rsync-path",
            f"mkdir -p '{expected_remote_dir}' && rsync",
            "-e",
            f"ssh -i {self.ssh_key}",
            str(self.report_path),
            expected_remote_path,
        ]
        mock_subprocess_run.assert_called_once_with(
            expected_rsync_command,
            check=True,
            text=True,
            capture_output=True,
        )

    @patch("lib.module_utils.report_transfer.configs")
    @patch("lib.module_utils.report_transfer.logger")
    def test_transfer_report_missing_destination(self, mock_logging, mock_configs):
        # Set up configs missing 'destination'
        mock_configs.__getitem__.return_value = {
            "server": self.server,
            "user": self.user,
            "ssh_key": self.ssh_key,
            # 'destination' key is missing
        }

        # Call the function
        result = transfer_report(self.report_path, self.project_id, self.sample_id)

        # Assert the result is False
        self.assertFalse(result)

        # Assert that logging.error was called with the missing key
        mock_logging.error.assert_called_with(
            "Missing configuration for report transfer: 'destination'"
        )

    @patch("lib.module_utils.report_transfer.configs")
    @patch("lib.module_utils.report_transfer.logger")
    def test_transfer_report_nonexistent_report_path(self, mock_logging, mock_configs):
        # Set up configs
        mock_configs.__getitem__.return_value = {
            "server": self.server,
            "user": self.user,
            "destination": self.remote_dir_base,
            "ssh_key": self.ssh_key,
        }

        # Assume report_path does not exist; since the function does not check this, it proceeds
        # Mock subprocess.run to simulate rsync failure due to nonexistent report_path
        with patch(
            "lib.module_utils.report_transfer.subprocess.run"
        ) as mock_subprocess_run:
            mock_subprocess_run.side_effect = subprocess.CalledProcessError(
                returncode=1, cmd="rsync", stderr="No such file or directory"
            )

            # Call the function
            result = transfer_report(self.report_path, self.project_id, self.sample_id)

            # Assert the result is False
            self.assertFalse(result)

            # Assert that logging.error was called with rsync error
            mock_logging.error.assert_called_with(
                "Failed to transfer report:\nNo such file or directory"
            )

    @patch("lib.module_utils.report_transfer.configs")
    @patch("lib.module_utils.report_transfer.subprocess.run")
    def test_transfer_report_unicode_characters(
        self, mock_subprocess_run, mock_configs
    ):
        # Set up configs with Unicode characters
        unicode_server = "例子.com"
        unicode_user = "用户"
        unicode_destination = "/远程/目的地"

        mock_configs.__getitem__.return_value = {
            "server": unicode_server,
            "user": unicode_user,
            "destination": unicode_destination,
            "ssh_key": self.ssh_key,
        }

        # Set up subprocess.run to succeed
        mock_subprocess_run.return_value = MagicMock(
            returncode=0, stdout="传输完成", stderr=""
        )

        # Call the function
        result = transfer_report(self.report_path, self.project_id, self.sample_id)

        # Assert the result is True
        self.assertTrue(result)

        # Assert subprocess.run was called with correct arguments containing Unicode characters
        expected_remote_dir = (
            f"{unicode_destination}/{self.project_id}/{self.sample_id}"
        )
        expected_remote_path = f"{unicode_user}@{unicode_server}:{expected_remote_dir}/"
        expected_rsync_command = [
            "rsync",
            "-avz",
            "--rsync-path",
            f"mkdir -p '{expected_remote_dir}' && rsync",
            "-e",
            f"ssh -i {self.ssh_key}",
            str(self.report_path),
            expected_remote_path,
        ]
        mock_subprocess_run.assert_called_once_with(
            expected_rsync_command,
            check=True,
            text=True,
            capture_output=True,
        )

    @patch("lib.module_utils.report_transfer.configs")
    @patch("lib.module_utils.report_transfer.logger")
    def test_transfer_report_invalid_config_type(self, mock_logging, mock_configs):
        # Set up configs['report_transfer'] to be None
        mock_configs.__getitem__.return_value = None

        # Call the function
        result = transfer_report(self.report_path, self.project_id, self.sample_id)

        # Assert the result is False
        self.assertFalse(result)

        # Assert that logging.error was called
        mock_logging.error.assert_called()

    @patch("lib.module_utils.report_transfer.configs")
    @patch("lib.module_utils.report_transfer.subprocess.run")
    def test_transfer_report_non_string_config_values(
        self, mock_subprocess_run, mock_configs
    ):
        # Set up configs with non-string value for 'server'
        mock_configs.__getitem__.return_value = {
            "server": 123,  # Non-string value
            "user": self.user,
            "destination": self.remote_dir_base,
            "ssh_key": self.ssh_key,
        }

        # Set up subprocess.run to succeed
        mock_subprocess_run.return_value = MagicMock(
            returncode=0, stdout="Transfer complete", stderr=""
        )

        # Call the function
        result = transfer_report(self.report_path, self.project_id, self.sample_id)

        # Assert the result is True
        self.assertTrue(result)

        # Assert subprocess.run was called with '123' converted to string
        expected_remote_dir = (
            f"{self.remote_dir_base}/{self.project_id}/{self.sample_id}"
        )
        expected_remote_path = f"{self.user}@123:{expected_remote_dir}/"
        expected_rsync_command = [
            "rsync",
            "-avz",
            "--rsync-path",
            f"mkdir -p '{expected_remote_dir}' && rsync",
            "-e",
            f"ssh -i {self.ssh_key}",
            str(self.report_path),
            expected_remote_path,
        ]
        mock_subprocess_run.assert_called_once_with(
            expected_rsync_command,
            check=True,
            text=True,
            capture_output=True,
        )


if __name__ == "__main__":
    unittest.main()
