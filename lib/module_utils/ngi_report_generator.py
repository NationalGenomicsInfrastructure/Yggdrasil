import subprocess
from collections.abc import Mapping
from typing import Any

# from lib.core_utils.config_loader import configs
from lib.core_utils.config_loader import ConfigLoader
from lib.core_utils.logging_utils import custom_logger

logger = custom_logger(__name__)
configs: Mapping[str, Any] = ConfigLoader().load_config("main.json")


def generate_ngi_report(
    project_path: str, project_id: str, user_name: str, sample_list: list[str]
) -> bool:
    """Generate an NGI report for a specified project using external reporting tools.

    Args:
        project_path (str): Path to the project directory where the report will be generated.
        project_id (str): Unique identifier for the project.
        user_name (str): Name of the user generating the report.
        sample_list (List[str]): A list of sample identifiers to be included in the report.

    Returns:
        bool: True if the report was generated successfully, False otherwise.
    """
    # Convert list of samples into a space-separated string for the command line
    samples_str = " ".join(sample_list)

    try:
        # Command to activate the environment and run the NGI report generation
        activate_env_cmd = configs.get("yggdrasil", {}).get("activate_ngi_cmd")
        if not activate_env_cmd:
            logger.error(
                "NGI environment activation command not found in the configuration. "
                "NGI report will not be generated."
            )
            return False

        report_cmd = (
            f"ngi_reports project_summary -d {project_path} -p {project_id} "
            f"-s '{user_name}' -y --no_txt --samples {samples_str}"
        )
        full_cmd = f"{activate_env_cmd} && {report_cmd}"

        # Execute the combined command
        # NOTE: Perhaps use `check=False` to prevent raising
        #       CalledProcessError on non-zero exit codes
        process = subprocess.run(
            full_cmd,
            shell=True,
            text=True,
            capture_output=True,
            input="y\n",  # , check=False
        )

        # Check the outcome of the subprocess
        if process.returncode == 0:
            logger.info("NGI report generated successfully.")
            return True
        else:
            # Log the error message if the command failed
            logger.error(f"Failed to generate NGI report: {process.stderr.strip()}")
            return False
    except subprocess.SubprocessError as e:
        # Handle exceptions related to the subprocess module
        logger.error(f"Subprocess error occurred while generating the NGI report: {e}")
        return False
    except Exception as e:
        # Log any unexpected exceptions during the execution
        logger.exception(f"An error occurred while generating the NGI report: {str(e)}")
        return False
