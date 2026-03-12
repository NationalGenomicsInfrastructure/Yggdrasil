import subprocess
import unittest
from unittest.mock import MagicMock, patch

from lib.module_utils.ngi_report_generator import generate_ngi_report


class TestGenerateNgiReport(unittest.TestCase):

    def setUp(self):
        self.project_path = "/path/to/project"
        self.project_id = "P12345"
        self.user_name = "test_user"
        self.sample_list = ["sample1", "sample2", "sample3"]
        self.samples_str = "sample1 sample2 sample3"
        self.activate_env_cmd = "source activate ngi_env"

    @patch("lib.module_utils.ngi_report_generator.configs")
    @patch("lib.module_utils.ngi_report_generator.subprocess.run")
    def test_generate_ngi_report_success(self, mock_subprocess_run, mock_configs):
        # Setup configs
        mock_configs.get.return_value = {"activate_ngi_cmd": self.activate_env_cmd}

        # Setup subprocess.run to return success
        mock_subprocess_run.return_value = MagicMock(
            returncode=0, stdout="Report generated", stderr=""
        )

        result = generate_ngi_report(
            self.project_path, self.project_id, self.user_name, self.sample_list
        )

        self.assertTrue(result)
        # Verify that subprocess.run was called with the correct command
        expected_report_cmd = (
            f"ngi_reports project_summary -d {self.project_path} -p {self.project_id} "
            f"-s '{self.user_name}' -y --no_txt --samples {self.samples_str}"
        )
        expected_full_cmd = f"{self.activate_env_cmd} && {expected_report_cmd}"
        mock_subprocess_run.assert_called_once_with(
            expected_full_cmd,
            shell=True,
            text=True,
            capture_output=True,
            input="y\n",
        )

    @patch("lib.module_utils.ngi_report_generator.configs")
    def test_generate_ngi_report_missing_activate_env_cmd(self, mock_configs):
        # Configs return None for activate_ngi_cmd
        mock_configs.get.return_value = {}

        result = generate_ngi_report(
            self.project_path, self.project_id, self.user_name, self.sample_list
        )

        self.assertFalse(result)

    @patch("lib.module_utils.ngi_report_generator.configs")
    @patch("lib.module_utils.ngi_report_generator.subprocess.run")
    def test_generate_ngi_report_nonzero_returncode(
        self, mock_subprocess_run, mock_configs
    ):
        mock_configs.get.return_value = {"activate_ngi_cmd": self.activate_env_cmd}

        # Simulate subprocess.run returning non-zero exit code
        mock_subprocess_run.return_value = MagicMock(
            returncode=1, stdout="", stderr="Error generating report"
        )

        result = generate_ngi_report(
            self.project_path, self.project_id, self.user_name, self.sample_list
        )

        self.assertFalse(result)
        # Optionally, check that the error message was logged

    @patch("lib.module_utils.ngi_report_generator.configs")
    @patch("lib.module_utils.ngi_report_generator.subprocess.run")
    def test_generate_ngi_report_subprocess_error(
        self, mock_subprocess_run, mock_configs
    ):
        mock_configs.get.return_value = {"activate_ngi_cmd": self.activate_env_cmd}

        # Simulate subprocess.run raising SubprocessError
        mock_subprocess_run.side_effect = subprocess.SubprocessError(
            "Subprocess failed"
        )

        result = generate_ngi_report(
            self.project_path, self.project_id, self.user_name, self.sample_list
        )

        self.assertFalse(result)

    @patch("lib.module_utils.ngi_report_generator.configs")
    @patch("lib.module_utils.ngi_report_generator.subprocess.run")
    def test_generate_ngi_report_exception(self, mock_subprocess_run, mock_configs):
        mock_configs.get.return_value = {"activate_ngi_cmd": self.activate_env_cmd}

        # Simulate subprocess.run raising a general Exception
        mock_subprocess_run.side_effect = Exception("Unexpected error")

        result = generate_ngi_report(
            self.project_path, self.project_id, self.user_name, self.sample_list
        )

        self.assertFalse(result)

    @patch("lib.module_utils.ngi_report_generator.configs")
    @patch("lib.module_utils.ngi_report_generator.subprocess.run")
    def test_generate_ngi_report_empty_sample_list(
        self, mock_subprocess_run, mock_configs
    ):
        mock_configs.get.return_value = {"activate_ngi_cmd": self.activate_env_cmd}

        # Setup subprocess.run to return success
        mock_subprocess_run.return_value = MagicMock(
            returncode=0, stdout="Report generated", stderr=""
        )

        empty_sample_list = []
        samples_str = ""

        result = generate_ngi_report(
            self.project_path, self.project_id, self.user_name, empty_sample_list
        )

        self.assertTrue(result)
        # Verify that subprocess.run was called with the correct command
        expected_report_cmd = (
            f"ngi_reports project_summary -d {self.project_path} -p {self.project_id} "
            f"-s '{self.user_name}' -y --no_txt --samples {samples_str}"
        )
        expected_full_cmd = f"{self.activate_env_cmd} && {expected_report_cmd}"
        mock_subprocess_run.assert_called_once_with(
            expected_full_cmd,
            shell=True,
            text=True,
            capture_output=True,
            input="y\n",
        )

    @patch("lib.module_utils.ngi_report_generator.configs")
    @patch("lib.module_utils.ngi_report_generator.subprocess.run")
    def test_generate_ngi_report_special_characters(
        self, mock_subprocess_run, mock_configs
    ):
        mock_configs.get.return_value = {"activate_ngi_cmd": self.activate_env_cmd}

        # Setup subprocess.run to return success
        mock_subprocess_run.return_value = MagicMock(
            returncode=0, stdout="Report generated", stderr=""
        )

        # Use special characters in inputs
        special_project_path = "/path/with special/chars & spaces"
        special_user_name = "user & name"
        special_sample_list = ["sample1", "sample two", "sample&three"]
        samples_str = "sample1 sample two sample&three"

        result = generate_ngi_report(
            special_project_path,
            self.project_id,
            special_user_name,
            special_sample_list,
        )

        self.assertTrue(result)
        # Verify that subprocess.run was called with the correct command
        expected_report_cmd = (
            f"ngi_reports project_summary -d {special_project_path} -p {self.project_id} "
            f"-s '{special_user_name}' -y --no_txt --samples {samples_str}"
        )
        expected_full_cmd = f"{self.activate_env_cmd} && {expected_report_cmd}"
        mock_subprocess_run.assert_called_once_with(
            expected_full_cmd,
            shell=True,
            text=True,
            capture_output=True,
            input="y\n",
        )

    @patch("lib.module_utils.ngi_report_generator.configs")
    @patch("lib.module_utils.ngi_report_generator.subprocess.run")
    def test_generate_ngi_report_long_sample_list(
        self, mock_subprocess_run, mock_configs
    ):
        mock_configs.get.return_value = {"activate_ngi_cmd": self.activate_env_cmd}

        # Create a long list of samples
        long_sample_list = [f"sample{i}" for i in range(1000)]
        samples_str = " ".join(long_sample_list)

        # Setup subprocess.run to return success
        mock_subprocess_run.return_value = MagicMock(
            returncode=0, stdout="Report generated", stderr=""
        )

        result = generate_ngi_report(
            self.project_path, self.project_id, self.user_name, long_sample_list
        )

        self.assertTrue(result)
        # Verify that subprocess.run was called
        expected_report_cmd = (
            f"ngi_reports project_summary -d {self.project_path} -p {self.project_id} "
            f"-s '{self.user_name}' -y --no_txt --samples {samples_str}"
        )
        expected_full_cmd = f"{self.activate_env_cmd} && {expected_report_cmd}"
        mock_subprocess_run.assert_called_once_with(
            expected_full_cmd,
            shell=True,
            text=True,
            capture_output=True,
            input="y\n",
        )

    @patch("lib.module_utils.ngi_report_generator.configs")
    def test_generate_ngi_report_configs_error(self, mock_configs):
        # Simulate configs.get raising an exception
        mock_configs.get.side_effect = Exception("Configs error")

        result = generate_ngi_report(
            self.project_path, self.project_id, self.user_name, self.sample_list
        )

        self.assertFalse(result)

    @patch("lib.module_utils.ngi_report_generator.configs")
    @patch("lib.module_utils.ngi_report_generator.subprocess.run")
    def test_generate_ngi_report_unicode_characters(
        self, mock_subprocess_run, mock_configs
    ):
        mock_configs.get.return_value = {"activate_ngi_cmd": self.activate_env_cmd}

        # Use Unicode characters in inputs
        unicode_project_path = "/path/to/项目"
        unicode_user_name = "用户"
        unicode_sample_list = ["样品一", "样品二"]

        samples_str = " ".join(unicode_sample_list)

        # Setup subprocess.run to return success
        mock_subprocess_run.return_value = MagicMock(
            returncode=0, stdout="报告已生成", stderr=""
        )

        result = generate_ngi_report(
            unicode_project_path,
            self.project_id,
            unicode_user_name,
            unicode_sample_list,
        )

        self.assertTrue(result)
        # Verify that subprocess.run was called with the correct command
        expected_report_cmd = (
            f"ngi_reports project_summary -d {unicode_project_path} -p {self.project_id} "
            f"-s '{unicode_user_name}' -y --no_txt --samples {samples_str}"
        )
        expected_full_cmd = f"{self.activate_env_cmd} && {expected_report_cmd}"
        mock_subprocess_run.assert_called_once_with(
            expected_full_cmd,
            shell=True,
            text=True,
            capture_output=True,
            input="y\n",
        )

    @patch("lib.module_utils.ngi_report_generator.configs")
    @patch("lib.module_utils.ngi_report_generator.subprocess.run")
    def test_generate_ngi_report_input_injection(
        self, mock_subprocess_run, mock_configs
    ):
        mock_configs.get.return_value = {"activate_ngi_cmd": self.activate_env_cmd}

        # Attempt to inject additional commands via inputs
        malicious_user_name = "user_name'; rm -rf /; echo '"
        samples_str = "sample1 sample2"

        # Setup subprocess.run to return success
        mock_subprocess_run.return_value = MagicMock(
            returncode=0, stdout="Report generated", stderr=""
        )

        result = generate_ngi_report(
            self.project_path,
            self.project_id,
            malicious_user_name,
            self.sample_list[:2],
        )

        self.assertTrue(result)
        # Verify that subprocess.run was called with the correct (escaped) command
        expected_report_cmd = (
            f"ngi_reports project_summary -d {self.project_path} -p {self.project_id} "
            f"-s '{malicious_user_name}' -y --no_txt --samples {samples_str}"
        )
        expected_full_cmd = f"{self.activate_env_cmd} && {expected_report_cmd}"
        mock_subprocess_run.assert_called_once_with(
            expected_full_cmd,
            shell=True,
            text=True,
            capture_output=True,
            input="y\n",
        )


if __name__ == "__main__":
    unittest.main()
