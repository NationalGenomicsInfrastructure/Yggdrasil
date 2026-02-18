"""
Unit tests for lib.watchers.backends.couchdb module.

Tests the CouchDBBackend implementation with mocked CouchDB client.
"""

import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from lib.couchdb.changes_fetcher import ApiException
from lib.watchers.backends.base import Checkpoint
from lib.watchers.backends.checkpoint_store import InMemoryCheckpointStore
from lib.watchers.backends.couchdb import CouchDBBackend


class TestCouchDBBackendInit(unittest.TestCase):
    """Tests for CouchDBBackend initialization."""

    def setUp(self):
        """Set up test fixtures."""
        self.store = InMemoryCheckpointStore()
        self.base_config = {
            "url": "https://couch.example.org",
            "db": "projects",
            "user_env": "TEST_COUCH_USER",
            "pass_env": "TEST_COUCH_PASS",
        }

    def test_backend_key_format(self):
        """Test backend_key is stored correctly."""
        backend = CouchDBBackend(
            backend_key="couchdb:projects",
            config=self.base_config,
            checkpoint_store=self.store,
        )
        self.assertEqual(backend.backend_key, "couchdb:projects")

    def test_required_config_db(self):
        """Test that 'db' is required in config."""
        config = {
            "url": "https://couch.example.org",
            "user_env": "TEST_COUCH_USER",
            "pass_env": "TEST_COUCH_PASS",
        }  # Missing 'db'
        with self.assertRaises(KeyError) as ctx:
            CouchDBBackend(
                backend_key="couchdb:test",
                config=config,
                checkpoint_store=self.store,
            )
        self.assertIn("db", str(ctx.exception))

    def test_required_config_user_env(self):
        """Test that 'user_env' is required in config."""
        config = {
            "url": "https://couch.example.org",
            "db": "test_db",
            "pass_env": "TEST_COUCH_PASS",
        }  # Missing 'user_env'
        with self.assertRaises(KeyError) as ctx:
            CouchDBBackend(
                backend_key="couchdb:test",
                config=config,
                checkpoint_store=self.store,
            )
        self.assertIn("user_env", str(ctx.exception))

    def test_required_config_pass_env(self):
        """Test that 'pass_env' is required in config."""
        config = {
            "url": "https://couch.example.org",
            "db": "test_db",
            "user_env": "TEST_COUCH_USER",
        }  # Missing 'pass_env'
        with self.assertRaises(KeyError) as ctx:
            CouchDBBackend(
                backend_key="couchdb:test",
                config=config,
                checkpoint_store=self.store,
            )
        self.assertIn("pass_env", str(ctx.exception))

    def test_default_config_values(self):
        """Test default config values are applied."""
        backend = CouchDBBackend(
            backend_key="couchdb:projects",
            config=self.base_config,
            checkpoint_store=self.store,
        )
        self.assertEqual(backend._db_name, "projects")
        self.assertTrue(backend._include_docs)
        self.assertEqual(backend._poll_interval, 1.0)
        self.assertEqual(backend._start_seq, "0")  # DEFAULT_START_SEQ
        self.assertEqual(backend._limit, 100)
        self.assertEqual(backend._feed, "normal")
        self.assertEqual(backend._longpoll_timeout_ms, 5000)

    def test_custom_config_values(self):
        """Test custom config values are respected."""
        config = {
            "url": "https://couch.example.org",
            "db": "projects",
            "user_env": "TEST_COUCH_USER",
            "pass_env": "TEST_COUCH_PASS",
            "include_docs": False,
            "poll_interval": 10.0,
            "start_seq": "100-abc",
            "limit": 500,
        }
        backend = CouchDBBackend(
            backend_key="couchdb:projects",
            config=config,
            checkpoint_store=self.store,
        )
        self.assertFalse(backend._include_docs)
        self.assertEqual(backend._poll_interval, 10.0)
        self.assertEqual(backend._start_seq, "100-abc")
        self.assertEqual(backend._limit, 500)


class TestCouchDBBackendPolling(unittest.TestCase):
    """Tests for CouchDBBackend polling behavior."""

    def setUp(self):
        """Set up test fixtures."""
        self.store = InMemoryCheckpointStore()
        self.config = {
            "url": "https://couch.example.org",
            "db": "testdb",
            "user_env": "TEST_COUCH_USER",
            "pass_env": "TEST_COUCH_PASS",
        }

    @patch.object(CouchDBBackend, "_create_handler")
    def test_poll_yields_raw_watch_events(self, mock_create_handler):
        """Test that polling yields RawWatchEvent objects."""
        # Set up mock handler
        mock_handler = MagicMock()
        mock_create_handler.return_value = mock_handler

        responses = [
            (
                [
                    {
                        "id": "doc1",
                        "seq": "1-abc",
                        "doc": {"_id": "doc1", "type": "project"},
                    },
                    {"id": "doc2", "seq": "2-def", "deleted": True, "doc": None},
                ],
                "2-def",
            ),
            ([], "2-def"),
        ]

        def _batch(**kwargs):
            if responses:
                return responses.pop(0)
            return ([], "2-def")

        mock_handler.fetch_changes_batch.side_effect = _batch

        async def run_test():
            backend = CouchDBBackend(
                backend_key="couchdb:testdb",
                config=self.config,
                checkpoint_store=self.store,
            )

            await backend.start()

            events = []
            async for event in backend.events():
                events.append(event)
                if len(events) >= 2:
                    await backend.stop()
                    break

            self.assertEqual(len(events), 2)
            self.assertEqual(events[0].id, "doc1")
            self.assertEqual(events[0].seq, "1-abc")
            self.assertFalse(events[0].deleted)
            self.assertIsNotNone(events[0].doc)

            self.assertEqual(events[1].id, "doc2")
            self.assertTrue(events[1].deleted)

        asyncio.run(run_test())

    @patch.object(CouchDBBackend, "_create_handler")
    def test_loads_checkpoint_on_start(self, mock_create_handler):
        """Test that backend loads checkpoint on start."""
        mock_handler = MagicMock()
        mock_create_handler.return_value = mock_handler

        # Pre-save a checkpoint
        cp = Checkpoint(
            backend_key="couchdb:testdb",
            value="saved-seq-123",
            updated_at="2024-01-15T12:00:00Z",
        )
        self.store.save(cp)

        # Mock empty response (no new changes)
        mock_handler.fetch_changes_batch.return_value = ([], "saved-seq-123")

        async def run_test():
            backend = CouchDBBackend(
                backend_key="couchdb:testdb",
                config=self.config,
                checkpoint_store=self.store,
            )

            await backend.start()
            await asyncio.sleep(0.1)
            await backend.stop()

            # Verify batch fetch was called with saved checkpoint
            call_args = mock_handler.fetch_changes_batch.call_args
            self.assertEqual(call_args.kwargs.get("since"), "saved-seq-123")
            self.assertEqual(call_args.kwargs.get("feed"), "normal")

        asyncio.run(run_test())

    @patch.object(CouchDBBackend, "_create_handler")
    def test_saves_checkpoint_after_batch(self, mock_create_handler):
        """Test that backend saves checkpoint after processing batch."""
        mock_handler = MagicMock()
        mock_create_handler.return_value = mock_handler

        mock_handler.fetch_changes_batch.return_value = (
            [{"id": "doc1", "seq": "100-abc", "doc": {"_id": "doc1"}}],
            "100-abc",
        )

        async def run_test():
            backend = CouchDBBackend(
                backend_key="couchdb:testdb",
                config=self.config,
                checkpoint_store=self.store,
            )

            await backend.start()

            # Consume one event
            async for event in backend.events():
                break

            await backend.stop()

            # Verify checkpoint was saved with last_seq
            saved = self.store.load("couchdb:testdb")
            self.assertIsNotNone(saved)
            assert saved is not None  # Help mypy understand the guard
            self.assertEqual(saved.value, "100-abc")

        asyncio.run(run_test())

    @patch.object(CouchDBBackend, "_create_handler")
    @patch.object(CouchDBBackend, "save_checkpoint")
    def test_does_not_save_checkpoint_when_last_seq_unchanged(
        self, mock_save_checkpoint, mock_create_handler
    ):
        """Test that checkpoint is not persisted when last_seq has not advanced."""
        mock_handler = MagicMock()
        mock_create_handler.return_value = mock_handler

        # Pre-save checkpoint so backend starts from this seq.
        cp = Checkpoint(
            backend_key="couchdb:testdb",
            value="saved-seq-123",
            updated_at="2024-01-15T12:00:00Z",
        )
        self.store.save(cp)

        # No new changes and same last_seq should not trigger a checkpoint write.
        mock_handler.fetch_changes_batch.return_value = ([], "saved-seq-123")

        async def run_test():
            backend = CouchDBBackend(
                backend_key="couchdb:testdb",
                config=self.config,
                checkpoint_store=self.store,
            )

            await backend.start()
            await asyncio.sleep(0.05)
            await backend.stop()

            mock_save_checkpoint.assert_not_called()

        asyncio.run(run_test())


class TestCouchDBBackendRetry(unittest.TestCase):
    """Tests for CouchDBBackend retry behavior."""

    def setUp(self):
        """Set up test fixtures."""
        self.store = InMemoryCheckpointStore()
        self.config = {
            "url": "https://couch.example.org",
            "db": "testdb",
            "user_env": "TEST_COUCH_USER",
            "pass_env": "TEST_COUCH_PASS",
        }

    @patch.object(CouchDBBackend, "_create_handler")
    @patch("lib.couchdb.changes_fetcher.asyncio.sleep", new_callable=AsyncMock)
    def test_retries_on_connection_error(self, mock_sleep, mock_create_handler):
        """Test that backend retries on connection errors."""
        mock_handler = MagicMock()
        mock_handler.db_name = "testdb"  # Required by ChangesFetcher logging
        mock_create_handler.return_value = mock_handler

        # Use a call counter stored in a list to avoid threading issues with nonlocal
        call_tracker = {"count": 0}

        def mock_fetch_changes_batch(**kwargs):
            call_tracker["count"] += 1
            if call_tracker["count"] <= 2:
                raise ApiException(code=503, message="Service Unavailable")
            # Return success on 3rd+ call
            return ([{"id": "doc1", "seq": "1-abc"}], "1-abc")

        mock_handler.fetch_changes_batch.side_effect = mock_fetch_changes_batch

        async def run_test():
            backend = CouchDBBackend(
                backend_key="couchdb:testdb",
                config=self.config,
                checkpoint_store=self.store,
            )

            await backend.start()

            # Wait for retries and eventual success
            events = []
            try:
                async with asyncio.timeout(2.0):
                    async for event in backend.events():
                        events.append(event)
                        break  # Got the event, exit loop
            except TimeoutError:
                pass
            finally:
                await backend.stop()

            # We got an event, proving recovery worked
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0].id, "doc1")
            # Verify: 2 failures + at least 1 success = >= 3 calls
            # (stored after thread execution)
            self.assertGreaterEqual(call_tracker["count"], 3)

        asyncio.run(run_test())

    @patch.object(CouchDBBackend, "_create_handler")
    @patch("lib.couchdb.changes_fetcher.asyncio.sleep", new_callable=AsyncMock)
    def test_does_not_recreate_handler_on_transient_errors(
        self, mock_sleep, mock_create_handler
    ):
        """Retry policy is delegated to ChangesFetcher; backend handler is created once."""
        handler = MagicMock()
        handler.db_name = "testdb"  # Required by ChangesFetcher logging
        mock_create_handler.return_value = handler

        call_tracker = {"count": 0}

        def _fetch_changes_batch(**kwargs):
            call_tracker["count"] += 1
            if call_tracker["count"] <= 2:
                raise ApiException(code=503, message="Service Unavailable")
            return ([{"id": "doc-recovered", "seq": "1-abc"}], "1-abc")

        handler.fetch_changes_batch.side_effect = _fetch_changes_batch

        async def run_test():
            backend = CouchDBBackend(
                backend_key="couchdb:testdb",
                config=self.config,
                checkpoint_store=self.store,
            )

            await backend.start()

            events = []
            try:
                async with asyncio.timeout(2.0):
                    async for event in backend.events():
                        events.append(event)
                        break
            finally:
                await backend.stop()

            self.assertEqual(mock_create_handler.call_count, 1)
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0].id, "doc-recovered")
            # Verify retries occurred (3+ calls: 2 failures + success)
            self.assertGreaterEqual(call_tracker["count"], 3)

        asyncio.run(run_test())


class TestCouchDBBackendGracefulStop(unittest.TestCase):
    """Tests for CouchDBBackend graceful shutdown."""

    def setUp(self):
        """Set up test fixtures."""
        self.store = InMemoryCheckpointStore()
        self.config = {
            "url": "https://couch.example.org",
            "db": "testdb",
            "user_env": "TEST_COUCH_USER",
            "pass_env": "TEST_COUCH_PASS",
        }

    @patch.object(CouchDBBackend, "_create_handler")
    def test_stop_during_polling(self, mock_create_handler):
        """Test that stop() works during active polling."""
        mock_handler = MagicMock()
        mock_create_handler.return_value = mock_handler

        # Mock response that returns empty results
        mock_handler.fetch_changes_batch.return_value = ([], "0")

        async def run_test():
            backend = CouchDBBackend(
                backend_key="couchdb:testdb",
                config=self.config,
                checkpoint_store=self.store,
            )

            await backend.start()
            await asyncio.sleep(0.05)  # Let polling start

            # Stop should complete quickly
            import time

            start = time.time()
            await backend.stop()
            elapsed = time.time() - start

            self.assertLess(elapsed, 1.0)  # Should be fast

        asyncio.run(run_test())


if __name__ == "__main__":
    unittest.main()
