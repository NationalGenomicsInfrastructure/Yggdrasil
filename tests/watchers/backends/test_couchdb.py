"""
Unit tests for lib.watchers.backends.couchdb module — init and config.

Polling, retry, and stream behavior is covered in test_couchdb_backend.py.
"""

import unittest

from lib.watchers.backends.checkpoint_store import InMemoryCheckpointStore
from lib.watchers.backends.couchdb import CouchDBBackend


class TestCouchDBBackendInit(unittest.TestCase):
    """Tests for CouchDBBackend initialization and config validation."""

    def setUp(self):
        self.store = InMemoryCheckpointStore()
        self.base_config = {
            "url": "https://couch.example.org",
            "db": "projects",
            "user_env": "TEST_COUCH_USER",
            "pass_env": "TEST_COUCH_PASS",
        }

    def test_backend_key_format(self):
        backend = CouchDBBackend(
            backend_key="couchdb:projects",
            config=self.base_config,
            checkpoint_store=self.store,
        )
        self.assertEqual(backend.backend_key, "couchdb:projects")

    def test_required_config_db(self):
        config = {
            "url": "https://couch.example.org",
            "user_env": "TEST_COUCH_USER",
            "pass_env": "TEST_COUCH_PASS",
        }
        with self.assertRaises(KeyError) as ctx:
            CouchDBBackend(
                backend_key="couchdb:test",
                config=config,
                checkpoint_store=self.store,
            )
        self.assertIn("db", str(ctx.exception))

    def test_required_config_user_env(self):
        config = {
            "url": "https://couch.example.org",
            "db": "test_db",
            "pass_env": "TEST_COUCH_PASS",
        }
        with self.assertRaises(KeyError) as ctx:
            CouchDBBackend(
                backend_key="couchdb:test",
                config=config,
                checkpoint_store=self.store,
            )
        self.assertIn("user_env", str(ctx.exception))

    def test_required_config_pass_env(self):
        config = {
            "url": "https://couch.example.org",
            "db": "test_db",
            "user_env": "TEST_COUCH_USER",
        }
        with self.assertRaises(KeyError) as ctx:
            CouchDBBackend(
                backend_key="couchdb:test",
                config=config,
                checkpoint_store=self.store,
            )
        self.assertIn("pass_env", str(ctx.exception))

    def test_default_config_values(self):
        backend = CouchDBBackend(
            backend_key="couchdb:projects",
            config=self.base_config,
            checkpoint_store=self.store,
        )
        self.assertEqual(backend._db_name, "projects")
        self.assertEqual(backend._poll_interval, CouchDBBackend.DEFAULT_POLL_INTERVAL)
        self.assertEqual(backend._start_seq, CouchDBBackend.DEFAULT_START_SEQ)
        self.assertIsNone(backend._limit)
        self.assertEqual(
            backend._longpoll_timeout_ms, CouchDBBackend.DEFAULT_LONGPOLL_TIMEOUT_MS
        )
        self.assertEqual(backend._max_retries, CouchDBBackend.DEFAULT_MAX_RETRIES)
        self.assertEqual(backend._retry_delay, CouchDBBackend.DEFAULT_RETRY_DELAY)

    def test_custom_config_values(self):
        config = {
            **self.base_config,
            "poll_interval": 10.0,
            "start_seq": "100-abc",
            "limit": 500,
            "longpoll_timeout_ms": 30_000,
            "max_observation_retries": 5,
            "observation_retry_delay_s": 2.0,
        }
        backend = CouchDBBackend(
            backend_key="couchdb:projects",
            config=config,
            checkpoint_store=self.store,
        )
        self.assertEqual(backend._poll_interval, 10.0)
        self.assertEqual(backend._start_seq, "100-abc")
        self.assertEqual(backend._limit, 500)
        self.assertEqual(backend._longpoll_timeout_ms, 30_000)
        self.assertEqual(backend._max_retries, 5)
        self.assertEqual(backend._retry_delay, 2.0)

    def test_empty_required_value_raises(self):
        config = {**self.base_config, "db": ""}
        with self.assertRaises(ValueError):
            CouchDBBackend(
                backend_key="couchdb:test",
                config=config,
                checkpoint_store=self.store,
            )


if __name__ == "__main__":
    unittest.main()
