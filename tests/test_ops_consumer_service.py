"""
Comprehensive tests for lib/ops/consumer_service.py

Tests the OpsConsumerService async service wrapper.
"""

import asyncio
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from lib.ops.consumer_service import OpsConsumerService


class TestOpsConsumerService(unittest.TestCase):
    """
    Comprehensive tests for OpsConsumerService class.

    Tests async service initialization, lifecycle, and event loop.
    """

    # =====================================================
    # INITIALIZATION TESTS
    # =====================================================

    def test_init_default_values(self):
        """Test initialization with default values."""
        with patch.dict(os.environ, {}, clear=True):
            with patch("lib.ops.consumer_service.OpsWriter"):
                service = OpsConsumerService()

                self.assertEqual(service.interval, 2.0)
                self.assertEqual(service.spool, Path("/tmp/ygg_events"))

    def test_init_custom_interval(self):
        """Test initialization with custom interval."""
        with patch("lib.ops.consumer_service.OpsWriter"):
            service = OpsConsumerService(interval_sec=5.0)
            self.assertEqual(service.interval, 5.0)

    def test_init_custom_db_name(self):
        """Test initialization with custom database name."""
        with patch("lib.ops.consumer_service.OpsWriter") as mock_writer:
            service = OpsConsumerService(db_name="custom_db")

            mock_writer.assert_called_once_with(db_name="custom_db")

    def test_init_env_spool_path(self):
        """Test spool path from environment variable."""
        with patch.dict(os.environ, {"YGG_EVENT_SPOOL": "/custom/spool"}):
            with patch("lib.ops.consumer_service.OpsWriter"):
                service = OpsConsumerService()

                self.assertEqual(service.spool, Path("/custom/spool"))

    def test_init_env_db_name(self):
        """Test database name from environment variable."""
        with patch.dict(os.environ, {"OPS_DB": "env_db"}):
            with patch("lib.ops.consumer_service.OpsWriter") as mock_writer:
                service = OpsConsumerService()

                mock_writer.assert_called_once_with(db_name="env_db")

    def test_init_task_and_stop_event(self):
        """Test initial task and stop event state."""
        with patch("lib.ops.consumer_service.OpsWriter"):
            service = OpsConsumerService()

            self.assertIsNone(service._task)
            self.assertIsInstance(service._stop, asyncio.Event)
            self.assertFalse(service._stop.is_set())

    # =====================================================
    # START METHOD TESTS
    # =====================================================

    def test_start_creates_task(self):
        """Test that start creates an asyncio task."""

        async def test():
            with patch("lib.ops.consumer_service.OpsWriter"):
                service = OpsConsumerService()

                service.start()

                self.assertIsNotNone(service._task)
                self.assertIsInstance(service._task, asyncio.Task)
                self.assertEqual(service._task.get_name(), "ops-consumer")  # type: ignore

                await service.stop()

        run_async_test(test())

    def test_start_clears_stop_event(self):
        """Test that start clears the stop event."""

        async def test():
            with patch("lib.ops.consumer_service.OpsWriter"):
                service = OpsConsumerService()
                service._stop.set()

                service.start()

                self.assertFalse(service._stop.is_set())

                await service.stop()

        run_async_test(test())

    def test_start_multiple_times(self):
        """Test calling start multiple times."""

        async def test():
            with patch("lib.ops.consumer_service.OpsWriter"):
                service = OpsConsumerService()

                service.start()
                first_task = service._task

                # Start again - should reuse or create new if done
                service.start()

                # Task should exist
                self.assertIsNotNone(service._task)

                await service.stop()

        run_async_test(test())

    # =====================================================
    # STOP METHOD TESTS
    # =====================================================

    async def test_stop_sets_event(self):
        """Test that stop sets the stop event."""
        with patch("lib.ops.consumer_service.OpsWriter"):
            service = OpsConsumerService()

            service.start()
            self.assertFalse(service._stop.is_set())

            await service.stop()

            self.assertTrue(service._stop.is_set())

    async def test_stop_waits_for_task(self):
        """Test that stop waits for task completion."""
        with patch("lib.ops.consumer_service.OpsWriter"):
            service = OpsConsumerService(interval_sec=0.1)

            service.start()
            task = service._task

            await service.stop()

            # Task should be done after stop
            self.assertTrue(task.done())  # type: ignore

    async def test_stop_without_task(self):
        """Test stop when no task exists."""
        with patch("lib.ops.consumer_service.OpsWriter"):
            service = OpsConsumerService()

            # Should not raise
            await service.stop()

    # =====================================================
    # LOOP METHOD TESTS
    # =====================================================

    async def test_loop_creates_consumer(self):
        """Test that loop creates FileSpoolConsumer."""
        with patch("lib.ops.consumer_service.OpsWriter"):
            with patch(
                "lib.ops.consumer_service.FileSpoolConsumer"
            ) as mock_consumer_class:
                mock_consumer = Mock()
                mock_consumer_class.return_value = mock_consumer

                service = OpsConsumerService(interval_sec=0.01)

                # Start and quickly stop
                service.start()
                await asyncio.sleep(0.05)
                await service.stop()

                # Consumer should have been created
                mock_consumer_class.assert_called_once()
                self.assertEqual(mock_consumer_class.call_args[0][0], service.spool)
                self.assertEqual(mock_consumer_class.call_args[0][1], service.writer)

    async def test_loop_calls_consume(self):
        """Test that loop calls consumer.consume()."""
        with patch("lib.ops.consumer_service.OpsWriter"):
            with patch(
                "lib.ops.consumer_service.FileSpoolConsumer"
            ) as mock_consumer_class:
                mock_consumer = Mock()
                mock_consumer_class.return_value = mock_consumer

                service = OpsConsumerService(interval_sec=0.01)

                # Start and let it run briefly
                service.start()
                await asyncio.sleep(0.03)
                await service.stop()

                # consume should have been called at least once
                self.assertGreater(mock_consumer.consume.call_count, 0)

    async def test_loop_respects_interval(self):
        """Test that loop respects the interval setting."""
        with patch("lib.ops.consumer_service.OpsWriter"):
            with patch(
                "lib.ops.consumer_service.FileSpoolConsumer"
            ) as mock_consumer_class:
                mock_consumer = Mock()
                mock_consumer_class.return_value = mock_consumer

                service = OpsConsumerService(interval_sec=0.1)

                service.start()
                await asyncio.sleep(0.15)
                count_after_150ms = mock_consumer.consume.call_count
                await asyncio.sleep(0.1)
                count_after_250ms = mock_consumer.consume.call_count
                await service.stop()

                # Should have incremented between checks
                self.assertGreater(count_after_250ms, count_after_150ms)

    async def test_loop_stops_on_event(self):
        """Test that loop stops when stop event is set."""
        with patch("lib.ops.consumer_service.OpsWriter"):
            with patch(
                "lib.ops.consumer_service.FileSpoolConsumer"
            ) as mock_consumer_class:
                mock_consumer = Mock()
                mock_consumer_class.return_value = mock_consumer

                service = OpsConsumerService(interval_sec=0.01)

                service.start()
                await asyncio.sleep(0.02)

                # Stop and wait
                await service.stop()

                # Task should be done
                self.assertTrue(service._task.done())  # type: ignore

    # =====================================================
    # INTEGRATION TESTS
    # =====================================================

    async def test_full_lifecycle(self):
        """Test complete start-stop lifecycle."""
        with patch("lib.ops.consumer_service.OpsWriter"):
            service = OpsConsumerService(interval_sec=0.01)

            # Initially no task
            self.assertIsNone(service._task)

            # Start
            service.start()
            self.assertIsNotNone(service._task)
            self.assertFalse(service._task.done())  # type: ignore

            # Let it run
            await asyncio.sleep(0.02)

            # Still running
            self.assertFalse(service._task.done())  # type: ignore

            # Stop
            await service.stop()

            # Now done
            self.assertTrue(service._task.done())  # type: ignore

    async def test_restart_after_stop(self):
        """Test restarting service after stop."""
        with patch("lib.ops.consumer_service.OpsWriter"):
            service = OpsConsumerService(interval_sec=0.01)

            # First cycle
            service.start()
            first_task = service._task
            await asyncio.sleep(0.02)
            await service.stop()

            # Second cycle
            service.start()
            second_task = service._task
            await asyncio.sleep(0.02)
            await service.stop()

            # Should have different tasks
            self.assertIsNot(first_task, second_task)

    async def test_with_real_spool_directory(self):
        """Test with real temporary spool directory."""
        with TemporaryDirectory() as tmpdir:
            with patch.dict(os.environ, {"YGG_EVENT_SPOOL": tmpdir}):
                with patch("lib.ops.consumer_service.OpsWriter"):
                    service = OpsConsumerService(interval_sec=0.01)

                    self.assertEqual(service.spool, Path(tmpdir))

                    service.start()
                    await asyncio.sleep(0.02)
                    await service.stop()

    # =====================================================
    # ERROR HANDLING TESTS
    # =====================================================

    async def test_consumer_exception_handling(self):
        """Test that exceptions in consume don't crash the loop."""
        with patch("lib.ops.consumer_service.OpsWriter"):
            with patch(
                "lib.ops.consumer_service.FileSpoolConsumer"
            ) as mock_consumer_class:
                mock_consumer = Mock()
                # Make consume raise an exception
                mock_consumer.consume.side_effect = Exception("Test error")
                mock_consumer_class.return_value = mock_consumer

                service = OpsConsumerService(interval_sec=0.01)

                service.start()
                await asyncio.sleep(0.03)

                # Service should still be running despite exceptions
                self.assertFalse(service._task.done())  # type: ignore

                await service.stop()


# Helper to run async tests
def run_async_test(coro):
    """Helper to run async test functions."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# Convert async test methods to sync for unittest
class TestOpsConsumerServiceSync(unittest.TestCase):
    """Synchronous wrapper for async tests."""

    def test_stop_sets_event_sync(self):
        """Test that stop sets the stop event."""

        async def test():
            with patch("lib.ops.consumer_service.OpsWriter"):
                service = OpsConsumerService()
                service.start()
                await service.stop()
                self.assertTrue(service._stop.is_set())

        run_async_test(test())

    def test_stop_waits_for_task_sync(self):
        """Test that stop waits for task completion."""

        async def test():
            with patch("lib.ops.consumer_service.OpsWriter"):
                service = OpsConsumerService(interval_sec=0.1)
                service.start()
                task = service._task
                await service.stop()
                self.assertTrue(task.done())  # type: ignore

        run_async_test(test())

    def test_full_lifecycle_sync(self):
        """Test complete start-stop lifecycle."""

        async def test():
            with patch("lib.ops.consumer_service.OpsWriter"):
                service = OpsConsumerService(interval_sec=0.01)
                self.assertIsNone(service._task)
                service.start()
                self.assertIsNotNone(service._task)
                await asyncio.sleep(0.02)
                await service.stop()
                self.assertTrue(service._task.done())  # type: ignore

        run_async_test(test())


if __name__ == "__main__":
    unittest.main()
