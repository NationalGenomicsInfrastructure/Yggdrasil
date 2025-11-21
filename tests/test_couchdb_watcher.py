import asyncio
import logging
import unittest
from unittest.mock import MagicMock

from lib.watchers.abstract_watcher import YggdrasilEvent
from lib.watchers.couchdb_watcher import CouchDBWatcher


class TestCouchDBWatcher(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up class-level resources."""
        # Store original event loop policy
        cls.original_event_loop_policy = asyncio.get_event_loop_policy()

    @classmethod
    def tearDownClass(cls):
        """Clean up class-level resources and reset event loop state."""
        # Reset to default event loop policy for subsequent tests
        asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())

    def setUp(self):
        # Create a fresh event loop for each test
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        # Mock callback to verify event emissions
        self.mock_on_event = MagicMock()

        # Mock changes_fetcher that yields data only once per call
        self.changes_yielded = False

        async def mock_changes():
            if not self.changes_yielded:
                self.changes_yielded = True
                yield ({"doc_id": "123"}, "module.test")
                yield ({"doc_id": "456"}, "module.another")

        self.mock_changes_fetcher = mock_changes

        # Instantiate CouchDBWatcher
        self.watcher = CouchDBWatcher(
            on_event=self.mock_on_event,
            changes_fetcher=self.mock_changes_fetcher,
            poll_interval=0.01,  # short poll to speed tests
            name="ProjectDBTest",
        )

        # Set up logging capture
        self.log_output = []

        class CaptureHandler(logging.Handler):
            def __init__(self, log_list):
                super().__init__()
                self.log_list = log_list

            def emit(self, record):
                self.log_list.append(record.getMessage())

        # Attach the handler
        self.logger = logging.getLogger("ProjectDBTest")
        self.logger.setLevel(logging.DEBUG)
        self.handler = CaptureHandler(self.log_output)
        self.logger.addHandler(self.handler)

    def tearDown(self):
        self.loop.run_until_complete(self.watcher.stop())
        self.loop.close()
        self.logger.removeHandler(self.handler)

    def test_init(self):
        self.assertEqual(self.watcher.poll_interval, 0.01)
        self.assertEqual(self.watcher.name, "ProjectDBTest")
        self.assertFalse(self.watcher.is_running)

    def test_start_emits_events(self):
        async def run_test():
            # Start watcher
            start_task = asyncio.create_task(self.watcher.start())
            await asyncio.sleep(0.03)  # Give it time to process

            # Stop watcher
            await self.watcher.stop()
            await start_task

            self.assertFalse(self.watcher.is_running)
            # We expect exactly two events
            self.assertEqual(self.mock_on_event.call_count, 2)

            # Verify event contents
            first_call = self.mock_on_event.call_args_list[0][0][0]
            second_call = self.mock_on_event.call_args_list[1][0][0]

            self.assertIsInstance(first_call, YggdrasilEvent)
            self.assertIsInstance(second_call, YggdrasilEvent)
            self.assertEqual(first_call.payload["document"]["doc_id"], "123")
            self.assertEqual(second_call.payload["document"]["doc_id"], "456")

        self.loop.run_until_complete(run_test())

    def test_exception_handling_in_changes_fetcher(self):
        """If changes_fetcher raises an exception, watcher should log error and continue."""

        self.changes_yielded = False

        async def failing_changes():
            if not self.changes_yielded:
                self.changes_yielded = True
                yield ({"doc_id": "999"}, "module.ok")
                raise RuntimeError("DB fetch error")

        self.watcher.changes_fetcher = failing_changes

        async def run_test():
            start_task = asyncio.create_task(self.watcher.start())
            await asyncio.sleep(0.03)  # Give it time to process

            await self.watcher.stop()
            await start_task

            # Should get exactly one event before the error
            self.assertEqual(self.mock_on_event.call_count, 1)
            error_logs = [
                msg
                for msg in self.log_output
                if "Error in ProjectDBTest watcher" in msg
            ]
            self.assertTrue(
                error_logs, "Should have logged an error from failing_changes"
            )

        self.loop.run_until_complete(run_test())

    def test_stop_mid_iteration(self):
        """Ensure graceful stop during polling."""

        self.changes_yielded = False

        async def slow_changes():
            if not self.changes_yielded:
                self.changes_yielded = True
                yield ({"doc_id": "999"}, "module.slow")
                await asyncio.sleep(0.1)  # Simulate slow fetch
                yield ({"doc_id": "111"}, "module.slower")

        self.watcher.changes_fetcher = slow_changes

        async def run_test():
            start_task = asyncio.create_task(self.watcher.start())
            await asyncio.sleep(0.02)  # Let it get first doc
            await self.watcher.stop()  # Stop mid-iteration
            await start_task

            self.assertEqual(
                self.mock_on_event.call_count,
                1,
                "Should have only one event before stop",
            )

        self.loop.run_until_complete(run_test())

    def test_start_ignores_duplicate_start_calls(self):
        """Ensure that calling start() multiple times does not restart the watcher."""

        async def run_test():
            # Start watcher
            start_task_1 = asyncio.create_task(self.watcher.start())
            await asyncio.sleep(0.01)  # Allow watcher to start

            # Call start again while already running
            start_task_2 = asyncio.create_task(self.watcher.start())
            await asyncio.sleep(0.01)

            # Stop watcher
            await self.watcher.stop()
            await start_task_1
            await start_task_2

            # Verify that the watcher was running only once
            self.assertEqual(self.mock_on_event.call_count, 2)

        self.loop.run_until_complete(run_test())

    def test_start_logs_start_and_stop(self):
        """Ensure that start() and stop() log appropriate messages."""

        async def run_test():
            # Start watcher
            start_task = asyncio.create_task(self.watcher.start())
            await asyncio.sleep(0.01)  # Allow watcher to start

            # Stop watcher
            await self.watcher.stop()
            await start_task

            # Verify log messages
            start_logs = [
                msg for msg in self.log_output if "Starting CouchDBWatcher" in msg
            ]
            stop_logs = [
                msg
                for msg in self.log_output
                if "CouchDBWatcher 'ProjectDBTest' stopped" in msg
            ]

            self.assertTrue(start_logs, "Should log start message")
            self.assertTrue(stop_logs, "Should log stop message")

        self.loop.run_until_complete(run_test())

    def test_start_handles_empty_changes_fetcher(self):
        """Ensure watcher handles an empty changes_fetcher gracefully."""

        async def empty_changes():
            if not self.changes_yielded:
                self.changes_yielded = True
                return
                yield  # Empty generator

        self.watcher.changes_fetcher = empty_changes

        async def run_test():
            # Start watcher
            start_task = asyncio.create_task(self.watcher.start())
            await asyncio.sleep(0.02)  # Allow watcher to process

            # Stop watcher
            await self.watcher.stop()
            await start_task

            # Verify no events were emitted
            self.assertEqual(self.mock_on_event.call_count, 0)

        self.loop.run_until_complete(run_test())


if __name__ == "__main__":
    unittest.main()
