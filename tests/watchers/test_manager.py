"""
Unit tests for lib.watchers.manager module.

Tests the WatcherManager class including lifecycle management,
grouping/deduplication, and config resolution.
"""

import asyncio
import unittest
from collections.abc import AsyncIterator
from typing import Any

from lib.watchers.backends.base import CheckpointStore, RawWatchEvent, WatcherBackend
from lib.watchers.backends.checkpoint_store import InMemoryCheckpointStore
from lib.watchers.manager import WatcherBackendGroup, WatcherManager


class MockWatcherBackend(WatcherBackend):
    """Mock backend for testing WatcherManager."""

    def __init__(
        self,
        backend_key: str,
        config: dict[str, Any],
        checkpoint_store: CheckpointStore,
        queue_maxsize: int = 1000,
        logger: Any = None,
    ):
        super().__init__(backend_key, config, checkpoint_store, queue_maxsize, logger)
        self._started = False
        self._stopped = False
        self._events_to_emit: list[RawWatchEvent] = []

    async def stream(self) -> AsyncIterator[RawWatchEvent]:
        """Yield mock events."""
        for event in self._events_to_emit:
            if not self._running:
                break
            yield event

    async def start(self) -> None:
        await super().start()
        self._started = True

    async def stop(self) -> None:
        await super().stop()
        self._stopped = True


class TestWatcherBackendGroup(unittest.TestCase):
    """Tests for WatcherBackendGroup dataclass."""

    def test_creation(self):
        """Test group creation."""
        group = WatcherBackendGroup(
            backend_type="couchdb",
            connection="projects_db",
        )
        self.assertEqual(group.backend_type, "couchdb")
        self.assertEqual(group.connection, "projects_db")
        self.assertIsNone(group.backend_instance)

    def test_key_property(self):
        """Test key property returns (backend_type, connection)."""
        group = WatcherBackendGroup(
            backend_type="couchdb",
            connection="projects_db",
        )
        self.assertEqual(group.key, ("couchdb", "projects_db"))


class TestWatcherManagerRegistry(unittest.TestCase):
    """Tests for WatcherManager backend registry."""

    def setUp(self):
        """Clear registry before each test."""
        WatcherManager._backend_registry.clear()

    def test_register_backend(self):
        """Test registering a backend type."""
        WatcherManager.register_backend("mock", MockWatcherBackend)
        self.assertIn("mock", WatcherManager._backend_registry)
        self.assertEqual(WatcherManager._backend_registry["mock"], MockWatcherBackend)

    def test_register_multiple_backends(self):
        """Test registering multiple backend types."""
        WatcherManager.register_backend("mock1", MockWatcherBackend)
        WatcherManager.register_backend("mock2", MockWatcherBackend)
        self.assertEqual(len(WatcherManager._backend_registry), 2)


class TestWatcherManagerGrouping(unittest.TestCase):
    """Tests for WatcherManager grouping/deduplication."""

    def setUp(self):
        """Set up manager with config."""
        WatcherManager._backend_registry.clear()
        WatcherManager.register_backend("couchdb", MockWatcherBackend)

        self.config = {
            "endpoints": {
                "couchdb": {
                    "backend": "couchdb",
                    "url": "https://couch.example.org",
                    "auth": {"user_env": "COUCH_USER", "pass_env": "COUCH_PASS"},
                }
            },
            "connections": {
                "projects_db": {
                    "endpoint": "couchdb",
                    "resource": {"db": "projects"},
                },
                "yggdrasil_db": {
                    "endpoint": "couchdb",
                    "resource": {"db": "yggdrasil"},
                },
            },
        }
        self.store = InMemoryCheckpointStore()
        self.manager = WatcherManager(
            config=self.config,
            checkpoint_store=self.store,
        )

    def test_add_watcher_group_creates_group(self):
        """Test add_watcher_group creates a new group."""
        group = self.manager.add_watcher_group(
            backend_type="couchdb",
            connection="projects_db",
        )
        self.assertIsNotNone(group)
        self.assertEqual(group.backend_type, "couchdb")
        self.assertEqual(group.connection, "projects_db")

    def test_add_watcher_group_deduplicates(self):
        """Test add_watcher_group returns existing group on duplicate."""
        group1 = self.manager.add_watcher_group(
            backend_type="couchdb",
            connection="projects_db",
        )
        group2 = self.manager.add_watcher_group(
            backend_type="couchdb",
            connection="projects_db",
        )

        self.assertIs(group1, group2)
        self.assertEqual(len(self.manager._watcher_groups), 1)

    def test_different_connections_create_separate_groups(self):
        """Test different connections create separate groups."""
        group1 = self.manager.add_watcher_group(
            backend_type="couchdb",
            connection="projects_db",
        )
        group2 = self.manager.add_watcher_group(
            backend_type="couchdb",
            connection="yggdrasil_db",
        )

        self.assertIsNot(group1, group2)
        self.assertEqual(len(self.manager._watcher_groups), 2)


class TestWatcherManagerConfigResolution(unittest.TestCase):
    """Tests for WatcherManager config resolution."""

    def setUp(self):
        """Set up manager with config."""
        WatcherManager._backend_registry.clear()
        WatcherManager.register_backend("couchdb", MockWatcherBackend)

        self.config = {
            "endpoints": {
                "couchdb": {
                    "backend": "couchdb",
                    "url": "https://couch.example.org",
                    "auth": {"user_env": "COUCH_USER", "pass_env": "COUCH_PASS"},
                }
            },
            "connections": {
                "projects_db": {
                    "endpoint": "couchdb",
                    "resource": {"db": "projects"},
                    "watch": {
                        "poll_interval": 3,
                        "include_docs": True,
                        "limit": 100,
                        "start_seq": "now",
                    },
                },
            },
            "defaults": {
                "start_seq": "0",
                "poll_interval": 5,
                "include_docs": False,
            },
        }
        self.store = InMemoryCheckpointStore()

    def test_resolve_connection_config_merges_correctly(self):
        """Test _resolve_connection_config merges endpoint + resource."""
        # No need to patch env vars - resolution passes env var NAMES, not values
        manager = WatcherManager(
            config=self.config,
            checkpoint_store=self.store,
        )
        resolved = manager._resolve_connection_config("projects_db")

        self.assertEqual(resolved["backend"], "couchdb")
        self.assertEqual(resolved["url"], "https://couch.example.org")
        self.assertEqual(resolved["db"], "projects")
        self.assertEqual(resolved["poll_interval"], 3)
        self.assertTrue(resolved["include_docs"])
        self.assertEqual(resolved["limit"], 100)
        self.assertEqual(resolved["start_seq"], "now")
        # user_env and pass_env are env var NAMES (not resolved values)
        # Backend/factory resolves env vars at client creation time
        self.assertEqual(resolved["user_env"], "COUCH_USER")
        self.assertEqual(resolved["pass_env"], "COUCH_PASS")

    def test_resolve_connection_config_watch_overrides_defaults(self):
        """Test per-connection watch settings take precedence over defaults."""
        manager = WatcherManager(
            config=self.config,
            checkpoint_store=self.store,
        )

        resolved = manager._resolve_connection_config("projects_db")
        self.assertEqual(resolved["start_seq"], "now")
        self.assertEqual(resolved["poll_interval"], 3)
        self.assertTrue(resolved["include_docs"])

    def test_resolve_connection_config_raises_on_missing_url(self):
        """Test _resolve_connection_config raises KeyError on missing url."""
        # Endpoint missing required 'url' key
        bad_config = {
            "endpoints": {
                "couchdb": {
                    "backend": "couchdb",
                    # 'url' is missing
                    "auth": {"user_env": "COUCH_USER", "pass_env": "COUCH_PASS"},
                }
            },
            "connections": {
                "projects_db": {
                    "endpoint": "couchdb",
                    "resource": {"db": "projects"},
                },
            },
        }
        manager = WatcherManager(
            config=bad_config,
            checkpoint_store=self.store,
        )
        with self.assertRaises(KeyError) as ctx:
            manager._resolve_connection_config("projects_db")
        self.assertIn("url", str(ctx.exception))

    def test_resolve_connection_config_raises_on_missing_connection(self):
        """Test _resolve_connection_config raises on missing connection."""
        manager = WatcherManager(
            config=self.config,
            checkpoint_store=self.store,
        )
        with self.assertRaises(KeyError):
            manager._resolve_connection_config("unknown_db")

    def test_resolve_connection_config_raises_on_missing_endpoint(self):
        """Test _resolve_connection_config raises on missing endpoint."""
        bad_config = {
            "endpoints": {},
            "connections": {
                "projects_db": {
                    "endpoint": "nonexistent",
                    "resource": {"db": "projects"},
                }
            },
        }
        manager = WatcherManager(
            config=bad_config,
            checkpoint_store=self.store,
        )
        with self.assertRaises(KeyError):
            manager._resolve_connection_config("projects_db")


class TestWatcherManagerBackendTypeValidation(unittest.TestCase):
    """Tests for WatcherManager backend type validation."""

    def setUp(self):
        """Set up manager."""
        WatcherManager._backend_registry.clear()
        WatcherManager.register_backend("couchdb", MockWatcherBackend)
        self.store = InMemoryCheckpointStore()

    def test_validates_backend_type_consistency(self):
        """Test that _instantiate_watcher_backends validates backend type."""
        # Config with mismatched backend type
        config = {
            "endpoints": {
                "couchdb": {
                    "backend": "postgres",  # Different from group's backend_type
                    "url": "postgres://...",
                    "auth": {},
                }
            },
            "connections": {
                "test_conn": {
                    "endpoint": "couchdb",
                    "resource": {"db": "test"},
                },
            },
        }

        manager = WatcherManager(config=config, checkpoint_store=self.store)
        manager.add_watcher_group(backend_type="couchdb", connection="test_conn")

        # Should raise on instantiation due to backend type mismatch
        with self.assertRaises(ValueError) as ctx:
            manager._instantiate_watcher_backends()
        self.assertIn("mismatch", str(ctx.exception).lower())


class TestWatcherManagerLifecycle(unittest.TestCase):
    """Tests for WatcherManager start/stop lifecycle."""

    def setUp(self):
        """Set up manager with mock backend."""
        WatcherManager._backend_registry.clear()
        WatcherManager.register_backend("mock", MockWatcherBackend)

        self.config = {
            "endpoints": {
                "mock_endpoint": {
                    "backend": "mock",
                    "url": "mock://test",
                    "auth": {},
                }
            },
            "connections": {
                "conn1": {
                    "endpoint": "mock_endpoint",
                    "resource": {"db": "test1"},
                },
                "conn2": {
                    "endpoint": "mock_endpoint",
                    "resource": {"db": "test2"},
                },
            },
        }
        self.store = InMemoryCheckpointStore()

    def test_start_starts_all_backends(self):
        """Test start() starts all registered backends."""

        async def run_test():
            manager = WatcherManager(
                config=self.config,
                checkpoint_store=self.store,
            )
            manager.add_watcher_group("mock", "conn1")
            manager.add_watcher_group("mock", "conn2")

            await manager.start()

            # Verify all backends started
            for group in manager._watcher_groups.values():
                self.assertIsNotNone(group.backend_instance)
                assert isinstance(group.backend_instance, MockWatcherBackend)
                self.assertTrue(group.backend_instance._started)

            await manager.stop()

        asyncio.run(run_test())

    def test_start_is_idempotent(self):
        """Test that calling start() twice logs warning."""

        async def run_test():
            manager = WatcherManager(
                config=self.config,
                checkpoint_store=self.store,
            )
            manager.add_watcher_group("mock", "conn1")

            await manager.start()
            self.assertTrue(manager.is_running)

            # Second start should be safe
            await manager.start()
            self.assertTrue(manager.is_running)

            await manager.stop()

        asyncio.run(run_test())

    def test_stop_stops_all_backends(self):
        """Test stop() stops all backends."""

        async def run_test():
            manager = WatcherManager(
                config=self.config,
                checkpoint_store=self.store,
            )
            manager.add_watcher_group("mock", "conn1")
            manager.add_watcher_group("mock", "conn2")

            await manager.start()
            await manager.stop()

            # Verify all backends stopped
            for group in manager._watcher_groups.values():
                assert isinstance(group.backend_instance, MockWatcherBackend)
                self.assertTrue(group.backend_instance._stopped)

            self.assertFalse(manager.is_running)

        asyncio.run(run_test())

    def test_stop_is_idempotent(self):
        """Test that calling stop() when not running is safe."""

        async def run_test():
            manager = WatcherManager(
                config=self.config,
                checkpoint_store=self.store,
            )

            # Stop without start
            await manager.stop()
            self.assertFalse(manager.is_running)

        asyncio.run(run_test())

    def test_start_handles_backend_failure(self):
        """Test start() continues if one backend fails."""

        class FailingBackend(WatcherBackend):
            async def stream(self) -> AsyncIterator[RawWatchEvent]:
                if False:
                    yield RawWatchEvent(id="never")

            async def start(self) -> None:
                raise RuntimeError("Startup failed")

        WatcherManager.register_backend("failing", FailingBackend)

        config = {
            "endpoints": {
                "mock_endpoint": {"backend": "mock", "url": "mock://", "auth": {}},
                "failing_endpoint": {
                    "backend": "failing",
                    "url": "fail://",
                    "auth": {},
                },
            },
            "connections": {
                "good_conn": {"endpoint": "mock_endpoint", "resource": {"db": "good"}},
                "bad_conn": {"endpoint": "failing_endpoint", "resource": {"db": "bad"}},
            },
        }

        async def run_test():
            manager = WatcherManager(
                config=config,
                checkpoint_store=self.store,
            )
            manager.add_watcher_group("mock", "good_conn")
            manager.add_watcher_group("failing", "bad_conn")

            # Should not raise, but log error
            await manager.start()
            self.assertTrue(manager.is_running)

            # Good backend should be started
            good_group = manager._watcher_groups[("mock", "good_conn")]
            assert good_group.backend_instance is not None
            assert isinstance(good_group.backend_instance, MockWatcherBackend)
            self.assertTrue(good_group.backend_instance._started)

            await manager.stop()

        asyncio.run(run_test())


class TestWatcherManagerIsRunning(unittest.TestCase):
    """Tests for WatcherManager.is_running property."""

    def setUp(self):
        """Set up manager."""
        WatcherManager._backend_registry.clear()
        WatcherManager.register_backend("mock", MockWatcherBackend)

        self.config = {
            "endpoints": {
                "mock_endpoint": {"backend": "mock", "url": "mock://", "auth": {}}
            },
            "connections": {
                "conn1": {"endpoint": "mock_endpoint", "resource": {"db": "test"}}
            },
        }
        self.store = InMemoryCheckpointStore()

    def test_is_running_initially_false(self):
        """Test is_running is False initially."""
        manager = WatcherManager(
            config=self.config,
            checkpoint_store=self.store,
        )
        self.assertFalse(manager.is_running)

    def test_is_running_true_after_start(self):
        """Test is_running is True after start()."""

        async def run_test():
            manager = WatcherManager(
                config=self.config,
                checkpoint_store=self.store,
            )
            manager.add_watcher_group("mock", "conn1")

            await manager.start()
            self.assertTrue(manager.is_running)

            await manager.stop()

        asyncio.run(run_test())

    def test_is_running_false_after_stop(self):
        """Test is_running is False after stop()."""

        async def run_test():
            manager = WatcherManager(
                config=self.config,
                checkpoint_store=self.store,
            )
            manager.add_watcher_group("mock", "conn1")

            await manager.start()
            await manager.stop()
            self.assertFalse(manager.is_running)

        asyncio.run(run_test())


if __name__ == "__main__":
    unittest.main()
    unittest.main()
