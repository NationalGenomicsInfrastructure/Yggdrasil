"""Unit tests for internal-storage configuration resolution."""

import unittest
from pathlib import Path

from lib.core_utils.errors import InternalStorageConfigurationError
from lib.storage.config import (
    CouchInternalStorageConfig,
    SQLiteInternalStorageConfig,
    default_sqlite_path,
    resolve_internal_storage_config,
)


def _couch_config(role_map: dict | None = None) -> dict:
    """Build a full config with explicit CouchDB internal storage."""
    return {
        "internal_storage": {
            "backend": "couchdb",
            "couchdb": {
                "connections": role_map
                or {
                    "coordination": "yggdrasil_db",
                    "plans": "yggdrasil_plans_db",
                    "operations": "yggdrasil_ops_db",
                }
            },
        },
        "external_systems": {
            "endpoints": {
                "couchdb": {
                    "backend": "couchdb",
                    "url": "https://couch.example.org:5984",
                    "auth": {"user_env": "TEST_USER", "pass_env": "TEST_PASS"},
                },
                "pg": {
                    "backend": "postgres",
                    "url": "pg.example.org:5432",
                },
            },
            "connections": {
                "yggdrasil_db": {
                    "endpoint": "couchdb",
                    "resource": {"db": "yggdrasil"},
                },
                "yggdrasil_plans_db": {
                    "endpoint": "couchdb",
                    "resource": {"db": "yggdrasil_plans"},
                },
                "yggdrasil_ops_db": {
                    "endpoint": "couchdb",
                    "resource": {"db": "yggdrasil_ops"},
                },
                "pg_conn": {
                    "endpoint": "pg",
                    "resource": {"db": "whatever"},
                },
            },
        },
    }


class TestResolveInternalStorageConfig(unittest.TestCase):
    """Tests for resolve_internal_storage_config()."""

    def test_absent_block_returns_none(self):
        self.assertIsNone(resolve_internal_storage_config({}))

    def test_unknown_backend_rejected(self):
        with self.assertRaises(InternalStorageConfigurationError):
            resolve_internal_storage_config({"internal_storage": {"backend": "redis"}})

    def test_non_mapping_block_rejected(self):
        with self.assertRaises(InternalStorageConfigurationError):
            resolve_internal_storage_config({"internal_storage": "couchdb"})

    # ------------------------------------------------------------------
    # Explicit CouchDB
    # ------------------------------------------------------------------

    def test_explicit_couchdb_resolves_all_roles(self):
        resolved = resolve_internal_storage_config(_couch_config())
        self.assertIsInstance(resolved, CouchInternalStorageConfig)
        self.assertEqual(resolved.coordination.db_name, "yggdrasil")
        self.assertEqual(resolved.plans.db_name, "yggdrasil_plans")
        self.assertEqual(resolved.operations.db_name, "yggdrasil_ops")
        # Endpoint auth/url come from the referenced endpoint
        self.assertEqual(resolved.plans.endpoint.user_env, "TEST_USER")
        self.assertEqual(resolved.plans.endpoint.pass_env, "TEST_PASS")
        self.assertTrue(resolved.plans.endpoint.url.startswith("https://"))

    def test_missing_role_rejected(self):
        config = _couch_config(role_map={"plans": "yggdrasil_plans_db"})
        with self.assertRaises(InternalStorageConfigurationError) as ctx:
            resolve_internal_storage_config(config)
        self.assertIn("coordination", str(ctx.exception))

    def test_unknown_connection_rejected(self):
        config = _couch_config(
            role_map={
                "coordination": "nope_db",
                "plans": "yggdrasil_plans_db",
                "operations": "yggdrasil_ops_db",
            }
        )
        with self.assertRaises(InternalStorageConfigurationError) as ctx:
            resolve_internal_storage_config(config)
        self.assertIn("nope_db", str(ctx.exception))

    def test_non_couchdb_endpoint_rejected(self):
        config = _couch_config(
            role_map={
                "coordination": "pg_conn",
                "plans": "yggdrasil_plans_db",
                "operations": "yggdrasil_ops_db",
            }
        )
        with self.assertRaises(InternalStorageConfigurationError) as ctx:
            resolve_internal_storage_config(config)
        self.assertIn("postgres", str(ctx.exception))

    def test_missing_connections_map_rejected(self):
        config = {"internal_storage": {"backend": "couchdb", "couchdb": {}}}
        with self.assertRaises(InternalStorageConfigurationError):
            resolve_internal_storage_config(config)

    # ------------------------------------------------------------------
    # Explicit SQLite (dev-gated)
    # ------------------------------------------------------------------

    def test_sqlite_rejected_outside_dev_mode(self):
        config = {"internal_storage": {"backend": "sqlite", "sqlite": {"path": None}}}
        with self.assertRaises(InternalStorageConfigurationError) as ctx:
            resolve_internal_storage_config(config, dev_mode=False)
        self.assertIn("dev mode", str(ctx.exception))

    def test_sqlite_default_path_in_dev_mode(self):
        config = {"internal_storage": {"backend": "sqlite", "sqlite": {"path": None}}}
        resolved = resolve_internal_storage_config(config, dev_mode=True)
        self.assertIsInstance(resolved, SQLiteInternalStorageConfig)
        self.assertEqual(resolved.path, default_sqlite_path())
        self.assertTrue(str(resolved.path).endswith("yggdrasil.sqlite3"))

    def test_sqlite_missing_block_uses_default_path(self):
        config = {"internal_storage": {"backend": "sqlite"}}
        resolved = resolve_internal_storage_config(config, dev_mode=True)
        self.assertEqual(resolved.path, default_sqlite_path())

    def test_sqlite_explicit_absolute_path(self):
        config = {
            "internal_storage": {
                "backend": "sqlite",
                "sqlite": {"path": "/var/tmp/ygg_dev.sqlite3"},
            }
        }
        resolved = resolve_internal_storage_config(config, dev_mode=True)
        self.assertEqual(resolved.path, Path("/var/tmp/ygg_dev.sqlite3"))

    def test_sqlite_relative_path_rejected(self):
        config = {
            "internal_storage": {
                "backend": "sqlite",
                "sqlite": {"path": "relative/ygg.sqlite3"},
            }
        }
        with self.assertRaises(InternalStorageConfigurationError) as ctx:
            resolve_internal_storage_config(config, dev_mode=True)
        self.assertIn("absolute", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
