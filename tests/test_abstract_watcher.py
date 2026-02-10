import asyncio
import unittest
from unittest.mock import MagicMock

from lib.core_utils.event_types import EventType
from lib.watchers.abstract_watcher import AbstractWatcher, YggdrasilEvent


class MockWatcher(AbstractWatcher):
    """
    A concrete subclass of AbstractWatcher for testing purposes.
    """

    def __init__(
        self,
        on_event,
        event_type: EventType = EventType.PROJECT_CHANGE,
        name=None,
        logger=None,
    ):
        super().__init__(on_event, event_type, name, logger)

    async def start(self):
        self._running = True
        # Simulate detecting an event and emitting it immediately.
        await self.emit(payload={"dummy": 123}, source="mock_source")

    async def stop(self):
        self._running = False


class TestAbstractWatcher(unittest.TestCase):
    """
    Tests for the AbstractWatcher base class using a mock subclass.
    """

    def setUp(self):
        # Create a dedicated event loop for each test
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        # Prepare a mock callback to verify emit calls
        self.mock_callback = MagicMock()
        # Instantiate our mock watcher
        self.watcher = MockWatcher(on_event=self.mock_callback)

    def tearDown(self):
        # Clean up the event loop after each test
        self.loop.close()

    def test_initial_state(self):
        """
        Ensure the watcher starts in a non-running state.
        """
        self.assertFalse(self.watcher.is_running)

    def test_start_stop(self):
        """
        Test the watcher's lifecycle: after start(), is_running is True;
        after stop(), is_running is False.
        """
        self.loop.run_until_complete(self.watcher.start())
        self.assertTrue(self.watcher.is_running)

        self.loop.run_until_complete(self.watcher.stop())
        self.assertFalse(self.watcher.is_running)

    def test_emit_creates_event_and_calls_callback(self):
        """
        Verify that emitting an event calls the callback with a proper YggdrasilEvent.
        """
        self.loop.run_until_complete(self.watcher.start())

        # The mock watcher emits one event in start(), so we expect one callback call
        self.mock_callback.assert_called_once()
        call_args, _ = self.mock_callback.call_args
        event = call_args[0]

        self.assertIsInstance(event, YggdrasilEvent)
        self.assertEqual(event.event_type, EventType.PROJECT_CHANGE)
        self.assertEqual(event.payload, {"dummy": 123})
        self.assertEqual(event.source, "mock_source")
        self.assertTrue(self.watcher.is_running)
