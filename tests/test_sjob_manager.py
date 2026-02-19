import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from lib.module_utils.sjob_manager import SlurmJobManager


class Sample:
    """Mock sample object with id and status attributes."""

    def __init__(self, sample_id):
        self.id = sample_id
        self.status = None

    def post_process(self):
        pass  # Mock method to simulate post-processing


class TestSlurmJobManager(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.manager = SlurmJobManager()
        self.script_path = "test_script.sh"
        self.job_id = "12345"
        self.sample = Sample("sample1")

    @patch("lib.module_utils.sjob_manager.Path")
    @patch("lib.module_utils.sjob_manager.asyncio.create_subprocess_exec")
    async def test_submit_job_success(self, mock_create_subprocess_exec, mock_path):
        # Mock Path.is_file() to return True
        mock_path.return_value.is_file.return_value = True

        # Mock the subprocess
        process_mock = MagicMock()
        process_mock.communicate = AsyncMock(
            return_value=(b"Submitted batch job 12345\n", b"")
        )
        process_mock.returncode = 0
        mock_create_subprocess_exec.return_value = process_mock

        job_id = await self.manager.submit_job(self.script_path)
        self.assertEqual(job_id, "12345")

    @patch("lib.module_utils.sjob_manager.Path")
    async def test_submit_job_script_not_found(self, mock_path):
        # Mock Path.is_file() to return False
        mock_path.return_value.is_file.return_value = False

        job_id = await self.manager.submit_job(self.script_path)
        self.assertIsNone(job_id)

    @patch("lib.module_utils.sjob_manager.Path")
    @patch("lib.module_utils.sjob_manager.asyncio.create_subprocess_exec")
    async def test_submit_job_sbatch_error(
        self, mock_create_subprocess_exec, mock_path
    ):
        # Mock Path.is_file() to return True
        mock_path.return_value.is_file.return_value = True

        # Mock the subprocess to simulate sbatch error
        process_mock = MagicMock()
        process_mock.communicate = AsyncMock(
            return_value=(b"", b"Error submitting job")
        )
        process_mock.returncode = 1
        mock_create_subprocess_exec.return_value = process_mock

        job_id = await self.manager.submit_job(self.script_path)
        self.assertIsNone(job_id)

    @patch("lib.module_utils.sjob_manager.Path")
    @patch("lib.module_utils.sjob_manager.asyncio.create_subprocess_exec")
    async def test_submit_job_no_job_id(self, mock_create_subprocess_exec, mock_path):
        # Mock Path.is_file() to return True
        mock_path.return_value.is_file.return_value = True

        # Mock the subprocess to return output without job ID
        process_mock = MagicMock()
        process_mock.communicate = AsyncMock(
            return_value=(b"Submission output without job ID", b"")
        )
        process_mock.returncode = 0
        mock_create_subprocess_exec.return_value = process_mock

        job_id = await self.manager.submit_job(self.script_path)
        self.assertIsNone(job_id)

    @patch("lib.module_utils.sjob_manager.Path")
    @patch("lib.module_utils.sjob_manager.asyncio.create_subprocess_exec")
    async def test_submit_job_timeout(self, mock_create_subprocess_exec, mock_path):
        # Mock Path.is_file() to return True
        mock_path.return_value.is_file.return_value = True

        # Mock the subprocess to simulate a timeout
        async def mock_communicate():
            await asyncio.sleep(0.1)
            raise TimeoutError()

        # Mock the subprocess to simulate a timeout
        mock_create_subprocess_exec.side_effect = asyncio.TimeoutError

        job_id = await self.manager.submit_job(self.script_path)
        self.assertIsNone(job_id)

    @patch("lib.module_utils.sjob_manager.asyncio.sleep", new_callable=AsyncMock)
    @patch("lib.module_utils.sjob_manager.SlurmJobManager._job_status")
    async def test_monitor_job_completed(self, mock_job_status, mock_sleep):
        # Mock _job_status to return 'COMPLETED' after a few calls
        mock_job_status.side_effect = ["PENDING", "RUNNING", "COMPLETED"]

        await self.manager.monitor_job(self.job_id, self.sample)
        self.assertEqual(self.sample.status, "processed")

    @patch("lib.module_utils.sjob_manager.asyncio.sleep", new_callable=AsyncMock)
    @patch("lib.module_utils.sjob_manager.SlurmJobManager._job_status")
    async def test_monitor_job_failed(self, mock_job_status, mock_sleep):
        # Mock _job_status to return 'FAILED'
        mock_job_status.return_value = "FAILED"

        await self.manager.monitor_job(self.job_id, self.sample)
        self.assertEqual(self.sample.status, "processing_failed")

    @patch("lib.module_utils.sjob_manager.asyncio.sleep", new_callable=AsyncMock)
    @patch("lib.module_utils.sjob_manager.SlurmJobManager._job_status")
    async def test_monitor_job_unexpected_status(self, mock_job_status, mock_sleep):
        # Mock _job_status to return 'UNKNOWN_STATUS' a few times, then 'COMPLETED'
        mock_job_status.side_effect = ["UNKNOWN_STATUS"] * 3 + ["COMPLETED"]

        await self.manager.monitor_job(self.job_id, self.sample)
        self.assertEqual(self.sample.status, "processed")

    @patch("lib.module_utils.sjob_manager.asyncio.create_subprocess_shell")
    async def test_job_status_success(self, mock_create_subprocess_shell):
        # Mock the subprocess to return a valid status
        process_mock = MagicMock()
        process_mock.communicate = AsyncMock(return_value=(b"COMPLETED", b""))
        mock_create_subprocess_shell.return_value = process_mock

        status = await self.manager._job_status(self.job_id)
        self.assertEqual(status, "COMPLETED")

    @patch("lib.module_utils.sjob_manager.asyncio.create_subprocess_shell")
    async def test_job_status_error(self, mock_create_subprocess_shell):
        # Mock the subprocess to return stderr
        process_mock = MagicMock()
        process_mock.communicate = AsyncMock(return_value=(b"", b"sacct error"))
        mock_create_subprocess_shell.return_value = process_mock

        status = await self.manager._job_status(self.job_id)
        self.assertIsNone(status)

    @patch("lib.module_utils.sjob_manager.asyncio.create_subprocess_shell")
    async def test_job_status_timeout(self, mock_create_subprocess_shell):
        # Mock the subprocess to simulate a timeout
        mock_create_subprocess_shell.side_effect = asyncio.TimeoutError

        status = await self.manager._job_status(self.job_id)
        self.assertIsNone(status)

    def test_check_status_completed(self):
        # Test check_status with 'COMPLETED' status
        self.manager.check_status(self.job_id, "COMPLETED", self.sample)
        self.assertEqual(self.sample.status, "processed")

    def test_check_status_failed(self):
        # Test check_status with 'FAILED' status
        self.manager.check_status(self.job_id, "FAILED", self.sample)
        self.assertEqual(self.sample.status, "processing_failed")

    def test_check_status_unexpected(self):
        # Test check_status with an unexpected status
        self.manager.check_status(self.job_id, "UNKNOWN_STATUS", self.sample)
        self.assertEqual(self.sample.status, "processing_failed")

    @patch("lib.module_utils.sjob_manager.custom_logger")
    def test_init_with_configs(self, mock_custom_logger):
        # Mock configs to return custom polling interval (nested under yggdrasil)
        with patch(
            "lib.module_utils.sjob_manager.SlurmJobManager.configs",
            {"yggdrasil": {"job_monitor_poll_interval": 5.0}},
        ):
            manager = SlurmJobManager()
            self.assertEqual(manager.polling_interval, 5.0)

    @patch("lib.module_utils.sjob_manager.custom_logger")
    def test_init_with_default_configs(self, mock_custom_logger):
        # Mock configs to be empty (no yggdrasil section → falls back to default)
        with patch("lib.module_utils.sjob_manager.SlurmJobManager.configs", {}):
            manager = SlurmJobManager()
            self.assertEqual(manager.polling_interval, 10.0)

    @patch("lib.module_utils.sjob_manager.Path")
    @patch("lib.module_utils.sjob_manager.asyncio.create_subprocess_exec")
    async def test_submit_job_exception(self, mock_create_subprocess_exec, mock_path):
        # Mock Path.is_file() to return True
        mock_path.return_value.is_file.return_value = True

        # Simulate an exception during subprocess creation
        mock_create_subprocess_exec.side_effect = Exception("Unexpected error")

        job_id = await self.manager.submit_job(self.script_path)
        self.assertIsNone(job_id)

    @patch("lib.module_utils.sjob_manager.asyncio.create_subprocess_shell")
    async def test_job_status_exception(self, mock_create_subprocess_shell):
        # Simulate an exception during subprocess creation
        mock_create_subprocess_shell.side_effect = Exception("Unexpected error")

        status = await self.manager._job_status(self.job_id)
        self.assertIsNone(status)

    @patch("lib.module_utils.sjob_manager.SlurmJobManager._job_status")
    async def test_monitor_job_no_status(self, mock_job_status):
        # Mock _job_status to return None
        mock_job_status.return_value = None

        # We need to prevent an infinite loop; we'll let it run only once
        with patch(
            "lib.module_utils.sjob_manager.asyncio.sleep", new_callable=AsyncMock
        ) as mock_sleep:
            mock_sleep.side_effect = asyncio.CancelledError

            with self.assertRaises(asyncio.CancelledError):
                await self.manager.monitor_job(self.job_id, self.sample)

    def test_check_status_calls_post_process(self):
        # Mock the sample's post_process method
        self.sample.post_process = MagicMock()

        self.manager.check_status(self.job_id, "COMPLETED", self.sample)
        self.sample.post_process.assert_called_once()

    def test_check_status_does_not_call_post_process(self):
        # Mock the sample's post_process method
        self.sample.post_process = MagicMock()

        self.manager.check_status(self.job_id, "FAILED", self.sample)
        self.sample.post_process.assert_not_called()

    @patch("lib.module_utils.sjob_manager.asyncio.create_subprocess_shell")
    async def test_job_status_with_multiple_lines(self, mock_create_subprocess_shell):
        # Mock sacct output with multiple lines
        process_mock = MagicMock()
        process_mock.communicate = AsyncMock(
            return_value=(b"COMPLETED\nCOMPLETED", b"")
        )
        mock_create_subprocess_shell.return_value = process_mock

        status = await self.manager._job_status(self.job_id)
        self.assertEqual(status, "COMPLETED\nCOMPLETED")

    @patch("lib.module_utils.sjob_manager.asyncio.create_subprocess_shell")
    async def test_job_status_empty_output(self, mock_create_subprocess_shell):
        # Mock sacct output with empty stdout and stderr
        process_mock = MagicMock()
        process_mock.communicate = AsyncMock(return_value=(b"", b""))
        mock_create_subprocess_shell.return_value = process_mock

        status = await self.manager._job_status(self.job_id)
        self.assertIsNone(status)

    @patch("lib.module_utils.sjob_manager.asyncio.create_subprocess_shell")
    async def test_job_status_decode_error(self, mock_create_subprocess_shell):
        # Mock sacct output with bytes that cannot be decoded
        process_mock = MagicMock()
        process_mock.communicate = AsyncMock(return_value=(b"\xff\xfe", b""))
        mock_create_subprocess_shell.return_value = process_mock

        status = await self.manager._job_status(self.job_id)
        self.assertIsNone(status)

    @patch("lib.module_utils.sjob_manager.asyncio.create_subprocess_exec")
    async def test_submit_job_decode_error(self, mock_create_subprocess_exec):
        # Mock sbatch output with bytes that cannot be decoded
        process_mock = MagicMock()
        process_mock.communicate = AsyncMock(return_value=(b"\xff\xfe", b""))
        process_mock.returncode = 0
        mock_create_subprocess_exec.return_value = process_mock

        job_id = await self.manager.submit_job(self.script_path)
        self.assertIsNone(job_id)


if __name__ == "__main__":
    unittest.main()
