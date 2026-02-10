import asyncio
import re
import subprocess
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from lib.core_utils.config_loader import ConfigLoader
from lib.core_utils.logging_utils import custom_logger

logging = custom_logger(__name__.split(".")[-1])


class SlurmManagerFactory:
    @staticmethod
    def get_manager(is_dev: bool):
        if is_dev:
            from lib.mocks.mock_sjob_manager import MockSlurmJobManager

            return MockSlurmJobManager()
        else:
            return SlurmJobManager()


#################################################################################################
######### CLASS BELOW ASSUMES ACCESS TO THE HOST SYSTEM TO SUBMIT SLURM JOBS ####################
#################################################################################################


class SlurmJobManager:
    """Manages the submission and monitoring of Slurm jobs.

    Attributes:
        polling_interval (float): Interval for polling job status in seconds.
        command_timeout (float): Timeout for Slurm commands in seconds.
    """

    slurm_end_states = [
        "COMPLETED",
        "FAILED",
        "CANCELLED",
        "CANCELLED+",
        "TIMEOUT",
        "OUT_OF_ME+",
    ]

    configs: Mapping[str, Any] = ConfigLoader().load_config("main.json")

    def __init__(
        self, polling_interval: float = 10.0, command_timeout: float = 8.0
    ) -> None:
        """Initialize the SlurmJobManager with specified polling interval and command timeout.

        Args:
            polling_interval (float, optional): Interval for polling job status in seconds.
                Defaults to 10.0 seconds.
            command_timeout (float, optional): Timeout for Slurm commands in seconds.
                Defaults to 8.0 seconds.
        """
        self.polling_interval: float = self.configs.get("yggdrasil", {}).get(
            "job_monitor_poll_interval", polling_interval
        )
        self.command_timeout: float = command_timeout

    async def submit_job(self, script_path: str | Path) -> str | None:
        """Submit a Slurm job using the specified script.

        Args:
            script_path (Union[str, Path]): Path to the Slurm script.

        Returns:
            Optional[str]: The job ID if submission is successful, None otherwise.
        """
        sbatch_command = ["sbatch", str(script_path)]

        if not Path(script_path).is_file():
            logging.error(f"Script file does not exist: {script_path}")
            return None

        try:
            process = await asyncio.create_subprocess_exec(
                *sbatch_command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                process.communicate(), self.command_timeout
            )

            if process.returncode != 0:
                logging.error(f"Error submitting job. Details: {stderr.decode()}")
                return None

            match = re.search(r"\d+", stdout.decode())
            job_id = match.group() if match else None

            # NOTE: Improved logic in case it's needed in the future
            # stdout_decoded = stdout.decode().strip()
            # match = re.search(r"Submitted batch job (\d+)", stdout_decoded)
            # job_id = match.group(1) if match else None

            if job_id:
                logging.info(f"Job submitted with ID: {job_id}")
                return job_id
            else:
                logging.error(
                    f"Failed to parse job ID from sbatch output: {stdout.decode().strip()}"
                )
        except TimeoutError:
            logging.error("Timeout while submitting job.")
        except Exception as e:
            logging.error(f"Unexpected error: {e}")

        return None

    async def monitor_job(self, job_id: str, sample: Any) -> None:
        """Monitor the specified job and handle its status accordingly.

        Continuously checks the status of a Slurm job until it completes or fails.
        Depending on the final status, it calls the `check_status` method to handle the sample.

        Args:
            job_id (str): The job ID.
            sample (Any): The sample object with `id` attribute.
        """
        logging.debug(f"[{sample.id}] Job {job_id} submitted for monitoring.")
        while True:
            status = await self._job_status(job_id)
            # if status in ["COMPLETED", "FAILED", "CANCELLED", "CANCELLED+", "TIMEOUT", "OUT_OF_ME+"]:
            if status in self.slurm_end_states:
                # logging.info(f"Job {job_id} status: {status}")
                # self.check_status(job_id, status, sample)
                self.check_status_new(job_id, status, sample)
                break
            await asyncio.sleep(self.polling_interval)

    async def _job_status(self, job_id: str) -> str | None:
        """Retrieve the status of a Slurm job.

        Args:
            job_id (str): The job ID.

        Returns:
            Optional[str]: The status of the job, or None if unable to retrieve.
        """
        sacct_command = f"sacct -n -X -o State -j {job_id}"
        try:
            process = await asyncio.create_subprocess_shell(
                sacct_command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()

            if stderr:
                logging.error(f"sacct stderr: {stderr.decode().strip()}")

            if stdout:
                stdout_decoded = stdout.decode().strip()
                logging.debug(f"sacct stdout for job {job_id}: {stdout_decoded}")
                return stdout_decoded
        except TimeoutError:
            logging.error(f"Timeout while checking status of job {job_id}.")
        except UnicodeDecodeError:
            logging.error(f"Failed to decode sbatch stdout for job {job_id}.")
        except Exception as e:
            logging.error(
                f"Unexpected error while checking status of job {job_id}: {e}"
            )

        return None

    @staticmethod
    def check_status(job_id: str, status: str, sample: Any) -> None:
        """Check the status of a job and update the sample accordingly.

        Args:
            job_id (str): The job ID.
            status (str): The status of the job.
            sample (object): The sample object with `id` and `status` attributes.
        """
        logging.info("\n")
        logging.debug(f"[{sample.id}] Job {job_id} status: {status}")
        if status == "COMPLETED":
            logging.info(f"[{sample.id}] Job completed successfully.")
            sample.status = "processed"
            sample.post_process()
            # sample.status = "completed"
        elif status in ["FAILED", "CANCELLED", "CANCELLED+", "TIMEOUT", "OUT_OF_ME+"]:
            sample.status = "processing_failed"
            logging.info(f"[{sample.id}] Job failed.")
        else:
            logging.warning(f"[{sample.id}] Job ended with unexpacted status: {status}")
            sample.status = "processing_failed"

    @staticmethod
    def check_status_new(job_id: str, status: str, sample: Any) -> None:
        """
        Called when SlurmJobManager.monitor_job determines the job is done or failed.
        We just set the sample status now. We do NOT call sample.post_process().
        """
        logging.info(f"[{sample.id}] Slurm job {job_id} ended with state '{status}'.")

        # Mark job complete or failed
        if status == "COMPLETED":
            sample.status = (
                "processed"  # HPC finished successfully, not yet post-processed
            )
        elif status in ["FAILED", "CANCELLED", "CANCELLED+", "TIMEOUT", "OUT_OF_ME+"]:
            sample.status = "processing_failed"
        else:
            logging.warning(f"[{sample.id}] Unexpected Slurm terminal state: {status}")
            sample.status = "processing_failed"
