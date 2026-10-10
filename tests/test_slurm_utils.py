import unittest
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

from yggdrasil.toolkit.slurm_utils import generate_slurm_script


class TestGenerateSlurmScript(unittest.TestCase):

    def setUp(self):
        self.args_dict = {"job_name": "test_job", "time": "01:00:00"}
        self.template_content = (
            "#!/bin/bash\n#SBATCH --job-name={job_name}\n#SBATCH --time={time}\n"
        )
        self.expected_script = (
            "#!/bin/bash\n#SBATCH --job-name=test_job\n#SBATCH --time=01:00:00\n"
        )
        self.template_fpath = "template.slurm"
        self.output_fpath = "output.slurm"

    @patch("yggdrasil.toolkit.slurm_utils.Path")
    @patch("builtins.open", new_callable=mock_open, read_data="")
    def test_generate_slurm_script_file_not_found(self, mock_file, mock_path):
        # Simulate FileNotFoundError when opening the template file
        mock_template_path = MagicMock()
        mock_template_path.open.side_effect = FileNotFoundError(
            "Template file not found"
        )
        mock_path.return_value = mock_template_path

        result = generate_slurm_script(
            self.args_dict, self.template_fpath, self.output_fpath
        )
        self.assertFalse(result)

    @patch("yggdrasil.toolkit.slurm_utils.Path")
    @patch("builtins.open", new_callable=mock_open, read_data="")
    def test_generate_slurm_script_missing_placeholder(self, mock_file, mock_path):
        # Simulate KeyError due to missing placeholder in args_dict
        incomplete_args_dict = {"job_name": "test_job"}  # Missing 'time' key
        mock_template_path = MagicMock()
        mock_template_path.open.return_value.__enter__.return_value.read.return_value = (
            self.template_content
        )
        mock_path.return_value = mock_template_path

        result = generate_slurm_script(
            incomplete_args_dict, self.template_fpath, self.output_fpath
        )
        self.assertFalse(result)

    @patch("yggdrasil.toolkit.slurm_utils.Path")
    @patch("builtins.open", new_callable=mock_open)
    def test_generate_slurm_script_success(self, mock_file, mock_path):
        # Mock reading the template file and writing the output file
        mock_template_file = mock_open(read_data=self.template_content).return_value
        mock_output_file = mock_open().return_value

        mock_template_path = MagicMock(spec=Path)
        mock_template_path.open.return_value = mock_template_file

        mock_output_path = MagicMock(spec=Path)
        mock_output_path.open.return_value = mock_output_file

        # Mock Path objects
        def side_effect(arg):
            if arg == self.template_fpath:
                return mock_template_path
            elif arg == self.output_fpath:
                return mock_output_path
            else:
                return Path(arg)

        mock_path.side_effect = side_effect

        result = generate_slurm_script(
            self.args_dict, self.template_fpath, self.output_fpath
        )
        self.assertTrue(result)
        mock_template_file.read.assert_called_once()
        mock_output_file.write.assert_called_once_with(self.expected_script)

    @patch("yggdrasil.toolkit.slurm_utils.Path")
    @patch("builtins.open", new_callable=mock_open)
    def test_generate_slurm_script_general_exception(self, mock_file, mock_path):
        # Simulate a general exception during file writing
        mock_template_file = mock_open(read_data=self.template_content).return_value
        mock_output_file = mock_open().return_value
        mock_output_file.write.side_effect = Exception("Write error")

        mock_template_path = MagicMock(spec=Path)
        mock_template_path.open.return_value = mock_template_file

        mock_output_path = MagicMock(spec=Path)
        mock_output_path.open.return_value = mock_output_file

        # Mock Path objects
        def side_effect(arg):
            if arg == self.template_fpath:
                return mock_template_path
            elif arg == self.output_fpath:
                return mock_output_path
            else:
                return Path(arg)

        mock_path.side_effect = side_effect

        result = generate_slurm_script(
            self.args_dict, self.template_fpath, self.output_fpath
        )
        self.assertFalse(result)

    @patch("yggdrasil.toolkit.slurm_utils.Path")
    @patch("builtins.open", new_callable=mock_open)
    def test_generate_slurm_script_empty_template(self, mock_file, mock_path):
        # Test with an empty template
        empty_template_content = ""
        mock_template_file = mock_open(read_data=empty_template_content).return_value
        mock_output_file = mock_open().return_value

        mock_template_path = MagicMock(spec=Path)
        mock_template_path.open.return_value = mock_template_file

        mock_output_path = MagicMock(spec=Path)
        mock_output_path.open.return_value = mock_output_file

        # Mock Path objects
        mock_path.side_effect = lambda arg: (
            mock_template_path if arg == self.template_fpath else mock_output_path
        )

        result = generate_slurm_script({}, self.template_fpath, self.output_fpath)
        self.assertTrue(result)
        mock_output_file.write.assert_called_once_with("")

    @patch("yggdrasil.toolkit.slurm_utils.Path")
    @patch("builtins.open", new_callable=mock_open)
    def test_generate_slurm_script_empty_args_dict(self, mock_file, mock_path):
        # Test with empty args_dict but placeholders in template
        mock_template_file = mock_open(read_data=self.template_content).return_value
        mock_output_file = mock_open().return_value

        mock_template_path = MagicMock(spec=Path)
        mock_template_path.open.return_value = mock_template_file

        mock_output_path = MagicMock(spec=Path)
        mock_output_path.open.return_value = mock_output_file

        mock_path.side_effect = lambda arg: (
            mock_template_path if arg == self.template_fpath else mock_output_path
        )

        result = generate_slurm_script({}, self.template_fpath, self.output_fpath)
        self.assertFalse(result)

    @patch("yggdrasil.toolkit.slurm_utils.Path")
    @patch("builtins.open", new_callable=mock_open)
    def test_generate_slurm_script_output_file_unwritable(self, mock_file, mock_path):
        # Simulate exception when opening output file for writing
        mock_template_file = mock_open(read_data=self.template_content).return_value

        mock_template_path = MagicMock(spec=Path)
        mock_template_path.open.return_value = mock_template_file

        mock_output_path = MagicMock(spec=Path)
        mock_output_path.open.side_effect = PermissionError(
            "Cannot write to output file"
        )

        # Mock Path objects
        mock_path.side_effect = lambda arg: (
            mock_template_path if arg == self.template_fpath else mock_output_path
        )

        result = generate_slurm_script(
            self.args_dict, self.template_fpath, self.output_fpath
        )
        self.assertFalse(result)

    @patch("yggdrasil.toolkit.slurm_utils.Path")
    @patch("builtins.open", new_callable=mock_open)
    def test_generate_slurm_script_non_string_args(self, mock_file, mock_path):
        # Test with non-string values in args_dict
        args_dict = {"job_name": "test_job", "nodes": 4, "time": "01:00:00"}
        template_content = "#!/bin/bash\n#SBATCH --job-name={job_name}\n#SBATCH --nodes={nodes}\n#SBATCH --time={time}\n"
        expected_script = "#!/bin/bash\n#SBATCH --job-name=test_job\n#SBATCH --nodes=4\n#SBATCH --time=01:00:00\n"

        mock_template_file = mock_open(read_data=template_content).return_value
        mock_output_file = mock_open().return_value

        mock_template_path = MagicMock(spec=Path)
        mock_template_path.open.return_value = mock_template_file

        mock_output_path = MagicMock(spec=Path)
        mock_output_path.open.return_value = mock_output_file

        # Mock Path objects
        mock_path.side_effect = lambda arg: (
            mock_template_path if arg == self.template_fpath else mock_output_path
        )

        result = generate_slurm_script(
            args_dict, self.template_fpath, self.output_fpath
        )
        self.assertTrue(result)
        mock_output_file.write.assert_called_once_with(expected_script)

    @patch("yggdrasil.toolkit.slurm_utils.Path")
    @patch("builtins.open", new_callable=mock_open)
    def test_generate_slurm_script_template_syntax_error(self, mock_file, mock_path):
        # Simulate ValueError due to invalid template syntax
        invalid_template_content = "#!/bin/bash\n#SBATCH --job-name={job_name\n"

        mock_template_file = mock_open(read_data=invalid_template_content).return_value
        mock_output_file = mock_open().return_value

        mock_template_path = MagicMock(spec=Path)
        mock_template_path.open.return_value = mock_template_file

        mock_output_path = MagicMock(spec=Path)
        mock_output_path.open.return_value = mock_output_file

        mock_path.side_effect = lambda arg: (
            mock_template_path if arg == self.template_fpath else mock_output_path
        )

        result = generate_slurm_script(
            self.args_dict, self.template_fpath, self.output_fpath
        )
        self.assertFalse(result)

    def test_generate_slurm_script_invalid_template_path_type(self):
        # Test with invalid type for template_fpath
        with self.assertRaises(TypeError):
            generate_slurm_script(self.args_dict, None, self.output_fpath)  # type: ignore

    def test_generate_slurm_script_invalid_output_path_type(self):
        # Test with invalid type for output_fpath
        with self.assertRaises(TypeError):
            generate_slurm_script(self.args_dict, self.template_fpath, None)  # type: ignore

    @patch("yggdrasil.toolkit.slurm_utils.Path")
    @patch("builtins.open", new_callable=mock_open)
    def test_generate_slurm_script_no_placeholders(self, mock_file, mock_path):
        # Test template with no placeholders
        template_content = "#!/bin/bash\n#SBATCH --partition=general\n"
        expected_script = template_content

        mock_template_file = mock_open(read_data=template_content).return_value
        mock_output_file = mock_open().return_value

        mock_template_path = MagicMock(spec=Path)
        mock_template_path.open.return_value = mock_template_file

        mock_output_path = MagicMock(spec=Path)
        mock_output_path.open.return_value = mock_output_file

        mock_path.side_effect = lambda arg: (
            mock_template_path if arg == self.template_fpath else mock_output_path
        )

        result = generate_slurm_script({}, self.template_fpath, self.output_fpath)
        self.assertTrue(result)
        mock_output_file.write.assert_called_once_with(expected_script)


if __name__ == "__main__":
    unittest.main()
