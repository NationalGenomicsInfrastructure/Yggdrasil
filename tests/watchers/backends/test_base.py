"""
Unit tests for lib.watchers.backends.base module.

Tests the core abstractions: RawWatchEvent, Checkpoint, CheckpointStore,
and WatcherBackend.
"""

import asyncio
import logging
import unittest
from dataclasses import FrozenInstanceError
from typing import Any

from lib.watchers.backends.base import (
    Checkpoint,
    CheckpointStore,
    RawWatchEvent,
    WatcherBackend,
)


class TestRawWatchEvent(unittest.TestCase):
    """Tests for RawWatchEvent frozen dataclass."""

    def test_required_fields(self):
        """Test that RawWatchEvent requires id, seq, and deleted."""
        event = RawWatchEvent(
            id="doc123",
            seq="10-abc",
            deleted=False,
        )
        self.assertEqual(event.id, "doc123")
        self.assertEqual(event.seq, "10-abc")
        self.assertFalse(event.deleted)
        self.assertIsNone(event.doc)
        self.assertEqual(event.meta, {})

    def test_with_optional_fields(self):
        """Test RawWatchEvent with all fields."""
        doc = {"_id": "doc123", "type": "project"}
        meta = {"source": "couchdb"}
        event = RawWatchEvent(
            id="doc123",
            doc=doc,
            seq="10-abc",
            deleted=False,
            meta=meta,
        )
        self.assertEqual(event.doc, doc)
        self.assertEqual(event.meta, meta)

    def test_immutability(self):
        """Test that RawWatchEvent is frozen (immutable)."""
        event = RawWatchEvent(id="doc123", seq="10-abc", deleted=False)
        with self.assertRaises(FrozenInstanceError):
            event.id = "new_id"  # type: ignore

    def test_equality(self):
        """Test RawWatchEvent equality."""
        event1 = RawWatchEvent(id="doc123", seq="10-abc", deleted=False)
        event2 = RawWatchEvent(id="doc123", seq="10-abc", deleted=False)
        event3 = RawWatchEvent(id="doc456", seq="10-abc", deleted=False)

        self.assertEqual(event1, event2)
        self.assertNotEqual(event1, event3)

    def test_deleted_event(self):
        """Test RawWatchEvent for deleted documents."""
        event = RawWatchEvent(
            id="deleted_doc",
            seq="20-xyz",
            deleted=True,
            doc=None,
        )
        self.assertTrue(event.deleted)
        self.assertIsNone(event.doc)


class TestCheckpoint(unittest.TestCase):
    """Tests for Checkpoint dataclass."""

    def test_creation(self):
        """Test Checkpoint creation."""
        cp = Checkpoint(
            backend_key="couchdb:projects",
            value="10-abc",
            updated_at="2024-01-15T12:00:00Z",
        )
        self.assertEqual(cp.backend_key, "couchdb:projects")
        self.assertEqual(cp.value, "10-abc")
        self.assertEqual(cp.updated_at, "2024-01-15T12:00:00Z")

    def test_value_types(self):
        """Test Checkpoint can store string, int, or None values."""
        # String value
        cp1 = Checkpoint(backend_key="key", value="seq-abc", updated_at="ts")
        self.assertEqual(cp1.value, "seq-abc")

        # Integer value
        cp2 = Checkpoint(backend_key="key", value=12345, updated_at="ts")
        self.assertEqual(cp2.value, 12345)

        # None value
        cp3 = Checkpoint(backend_key="key", value=None, updated_at="ts")
        self.assertIsNone(cp3.value)


class TestCheckpointStoreProtocol(unittest.TestCase):
    """Tests for CheckpointStore abstract base class."""

    def test_cannot_instantiate_abc(self):
        """Test that CheckpointStore cannot be instantiated directly."""
        with self.assertRaises(TypeError):
            CheckpointStore()  # type: ignore

    def test_concrete_implementation(self):
        """Test that a concrete implementation can be created."""

        class InMemoryStore(CheckpointStore):
            def __init__(self):
                self._store: dict[str, Checkpoint] = {}

            def load(self, backend_key: str) -> Checkpoint | None:
                return self._store.get(backend_key)

            def save(self, checkpoint: Checkpoint) -> None:
                self._store[checkpoint.backend_key] = checkpoint

        store = InMemoryStore()
        self.assertIsNone(store.load("unknown"))

        cp = Checkpoint(backend_key="test", value="123", updated_at="now")
        store.save(cp)
        loaded = store.load("test")
        self.assertEqual(loaded, cp)


class ConcreteWatcherBackend(WatcherBackend):
    """Concrete implementation of WatcherBackend for testing."""

    def __init__(
        self,
        backend_key: str,
        config: dict[str, Any],
        checkpoint_store: CheckpointStore,
        queue_maxsize: int = 1000,
        logger: logging.Logger | None = None,
        events_to_produce: list[RawWatchEvent] | None = None,
    ):
        super().__init__(backend_key, config, checkpoint_store, queue_maxsize, logger)
        self._events_to_produce = events_to_produce or []
        self._produce_called = False

    async def _produce_events(self) -> None:
        """Produce test events."""
        self._produce_called = True
        try:
            for event in self._events_to_produce:
                if not self._running:
                    break
                await self._event_queue.put(event)
                await asyncio.sleep(0.01)  # Simulate work
        finally:
            # Producer puts sentinel on normal exit
            await self._event_queue.put(None)


class TestWatcherBackend(unittest.TestCase):
    """Tests for WatcherBackend abstract base class."""

    def setUp(self):
        """Create an in-memory checkpoint store."""

        class InMemoryStore(CheckpointStore):
            def __init__(self):
                self._store: dict[str, Checkpoint] = {}

            def load(self, backend_key: str) -> Checkpoint | None:
                return self._store.get(backend_key)

            def save(self, checkpoint: Checkpoint) -> None:
                self._store[checkpoint.backend_key] = checkpoint

        self.store = InMemoryStore()
        self.default_config: dict[str, Any] = {"url": "http://example.com"}

    def test_cannot_instantiate_abc(self):
        """Test that WatcherBackend cannot be instantiated directly."""
        with self.assertRaises(TypeError):
            WatcherBackend(  # type: ignore
                backend_key="test",
                config=self.default_config,
                checkpoint_store=self.store,
            )

    def test_backend_key_and_config(self):
        """Test backend_key and config are stored correctly."""
        config = {"url": "http://example.com"}
        backend = ConcreteWatcherBackend(
            backend_key="couchdb:projects",
            config=config,
            checkpoint_store=self.store,
        )
        self.assertEqual(backend.backend_key, "couchdb:projects")
        self.assertEqual(backend.config, config)

    def test_initial_state(self):
        """Test backend initial state before start()."""
        backend = ConcreteWatcherBackend(
            backend_key="test",
            config=self.default_config,
            checkpoint_store=self.store,
        )
        self.assertFalse(backend._running)
        self.assertIsNone(backend._producer_task)

    def test_start_sets_running_and_spawns_producer(self):
        """Test that start() sets _running and spawns producer task."""

        async def run_test():
            backend = ConcreteWatcherBackend(
                backend_key="test",
                config=self.default_config,
                checkpoint_store=self.store,
            )
            await backend.start()

            # Give producer a moment to actually start running
            await asyncio.sleep(0.02)

            self.assertTrue(backend._running)
            self.assertIsNotNone(backend._producer_task)
            self.assertTrue(backend._produce_called)

            await backend.stop()

        asyncio.run(run_test())

    def test_start_returns_quickly(self):
        """Test that start() returns before producer finishes."""
        import time

        async def run_test():
            # Create backend with slow events
            events = [
                RawWatchEvent(id=f"doc{i}", seq=f"{i}-abc", deleted=False)
                for i in range(10)
            ]
            backend = ConcreteWatcherBackend(
                backend_key="test",
                config=self.default_config,
                checkpoint_store=self.store,
                events_to_produce=events,
            )

            start_time = time.time()
            await backend.start()
            elapsed = time.time() - start_time

            # start() should return quickly (< 0.1s even with 10 events @ 0.01s each)
            self.assertLess(elapsed, 0.1)

            await backend.stop()

        asyncio.run(run_test())

    def test_events_yields_from_queue(self):
        """Test that events() yields RawWatchEvent from queue."""

        async def run_test():
            test_events = [
                RawWatchEvent(id="doc1", seq="1-abc", deleted=False),
                RawWatchEvent(id="doc2", seq="2-def", deleted=True),
            ]
            backend = ConcreteWatcherBackend(
                backend_key="test",
                config=self.default_config,
                checkpoint_store=self.store,
                events_to_produce=test_events,
            )

            await backend.start()

            received = []
            async for event in backend.events():
                received.append(event)

            self.assertEqual(len(received), 2)
            self.assertEqual(received[0].id, "doc1")
            self.assertEqual(received[1].id, "doc2")

        asyncio.run(run_test())

    def test_stop_cancels_producer(self):
        """Test that stop() cancels producer task."""

        async def run_test():
            # Create a backend that produces events slowly
            slow_events = [
                RawWatchEvent(id=f"doc{i}", seq=f"{i}-abc", deleted=False)
                for i in range(100)  # Many events
            ]
            backend = ConcreteWatcherBackend(
                backend_key="test",
                config=self.default_config,
                checkpoint_store=self.store,
                events_to_produce=slow_events,
            )

            await backend.start()
            await asyncio.sleep(0.02)  # Let producer start

            await backend.stop()

            self.assertFalse(backend._running)
            assert backend._producer_task is not None
            self.assertTrue(backend._producer_task.done())

        asyncio.run(run_test())

    def test_stop_avoids_duplicate_sentinels(self):
        """Test that stop() doesn't put duplicate sentinels."""

        async def run_test():
            # Create backend with no events (producer exits immediately with sentinel)
            backend = ConcreteWatcherBackend(
                backend_key="test",
                config=self.default_config,
                checkpoint_store=self.store,
                events_to_produce=[],
            )

            await backend.start()
            # Wait for producer to finish naturally
            await asyncio.sleep(0.05)

            # Now stop() - producer already put sentinel
            await backend.stop()

            # Queue should have exactly one None (not two)
            items = []
            while not backend._event_queue.empty():
                items.append(backend._event_queue.get_nowait())

            # Count None sentinels
            sentinels = [i for i in items if i is None]
            self.assertLessEqual(len(sentinels), 1)

        asyncio.run(run_test())

    def test_load_checkpoint(self):
        """Test load_checkpoint() delegates to store."""
        backend = ConcreteWatcherBackend(
            backend_key="couchdb:projects",
            config=self.default_config,
            checkpoint_store=self.store,
        )

        # No checkpoint initially
        self.assertIsNone(backend.load_checkpoint())

        # Save a checkpoint
        cp = Checkpoint(
            backend_key="couchdb:projects",
            value="100-xyz",
            updated_at="2024-01-15T12:00:00Z",
        )
        self.store.save(cp)

        # Now load should work
        loaded = backend.load_checkpoint()
        assert loaded is not None
        self.assertEqual(loaded.value, "100-xyz")

    def test_save_checkpoint(self):
        """Test save_checkpoint() creates Checkpoint with timestamp."""
        backend = ConcreteWatcherBackend(
            backend_key="couchdb:projects",
            config=self.default_config,
            checkpoint_store=self.store,
        )

        backend.save_checkpoint("200-abc")

        loaded = self.store.load("couchdb:projects")
        assert loaded is not None
        self.assertEqual(loaded.value, "200-abc")
        # Verify timestamp was set
        self.assertIsNotNone(loaded.updated_at)


class TestWatcherBackendQueueMaxsize(unittest.TestCase):
    """Tests for WatcherBackend queue configuration."""

    def setUp(self):
        """Create an in-memory checkpoint store."""

        class InMemoryStore(CheckpointStore):
            def __init__(self):
                self._store: dict[str, Checkpoint] = {}

            def load(self, backend_key: str) -> Checkpoint | None:
                return self._store.get(backend_key)

            def save(self, checkpoint: Checkpoint) -> None:
                self._store[checkpoint.backend_key] = checkpoint

        self.store = InMemoryStore()
        self.default_config: dict[str, Any] = {"url": "http://example.com"}

    def test_default_queue_maxsize(self):
        """Test default queue maxsize is 1000."""
        backend = ConcreteWatcherBackend(
            backend_key="test",
            config=self.default_config,
            checkpoint_store=self.store,
        )
        self.assertEqual(backend._event_queue.maxsize, 1000)

    def test_custom_queue_maxsize(self):
        """Test custom queue maxsize from config."""
        backend = ConcreteWatcherBackend(
            backend_key="test",
            config={"url": "http://example.com"},
            checkpoint_store=self.store,
            queue_maxsize=100,
        )
        self.assertEqual(backend._event_queue.maxsize, 100)


if __name__ == "__main__":
    unittest.main()
