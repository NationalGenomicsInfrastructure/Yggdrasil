"""Tests for watcher/config wiring validation."""

import unittest
from pathlib import Path

from lib.core_utils.event_types import EventType
from lib.watchers.backends.base import WatcherBackend
from lib.watchers.config_validation import (
    WatcherConfigurationError,
    validate_watcher_config_wiring,
)
from lib.watchers.watchspec import BoundWatchSpec, WatchSpec


def _bound_spec(
    *,
    realm_id: str = "test_realm",
    backend: str = "couchdb",
    connection: str = "projects_db",
) -> BoundWatchSpec:
    return BoundWatchSpec(
        spec=WatchSpec(
            backend=backend,
            connection=connection,
            event_type=EventType.COUCHDB_DOC_CHANGED,
            build_scope=lambda event: {"kind": "doc", "id": event.id},
            build_payload=lambda event: {"doc": event.doc},
        ),
        realm_id=realm_id,
    )


def _external_systems(*, endpoint_backend: str = "couchdb") -> dict:
    return {
        "endpoints": {
            "main_endpoint": {
                "backend": endpoint_backend,
                "url": "https://couch.example.org",
                "auth": {},
            }
        },
        "connections": {
            "projects_db": {
                "endpoint": "main_endpoint",
                "resource": {"db": "projects"},
            }
        },
    }


class TestWatcherConfigValidation(unittest.TestCase):
    def test_missing_connection_reports_realm_available_connections_and_path(self):
        spec = _bound_spec(
            realm_id="dmx_realm",
            connection="demux_sample_info_db",
        )

        with self.assertRaises(WatcherConfigurationError) as ctx:
            validate_watcher_config_wiring(
                bound_specs=[spec],
                external_systems=_external_systems(),
                backend_registry={"couchdb": WatcherBackend},
                config_path=Path("/tmp/main.json"),
            )

        msg = str(ctx.exception)
        self.assertIn("/tmp/main.json", msg)
        self.assertIn("realm 'dmx_realm'", msg)
        self.assertIn("demux_sample_info_db", msg)
        self.assertIn("connection is not configured", msg)
        self.assertIn("Available connections: projects_db", msg)
        self.assertEqual(ctx.exception.issues[0].kind, "invalid_connection")

    def test_invalid_endpoint_is_reported_as_connection_resolution_issue(self):
        cfg = _external_systems()
        cfg["connections"]["projects_db"]["endpoint"] = "missing_endpoint"

        with self.assertRaises(WatcherConfigurationError) as ctx:
            validate_watcher_config_wiring(
                bound_specs=[_bound_spec()],
                external_systems=cfg,
                backend_registry={"couchdb": WatcherBackend},
            )

        msg = str(ctx.exception)
        self.assertIn("connection could not be resolved", msg)
        self.assertIn("missing_endpoint", msg)

    def test_missing_resource_db_is_reported_as_connection_resolution_issue(self):
        cfg = _external_systems()
        cfg["connections"]["projects_db"]["resource"] = {}

        with self.assertRaises(WatcherConfigurationError) as ctx:
            validate_watcher_config_wiring(
                bound_specs=[_bound_spec()],
                external_systems=cfg,
                backend_registry={"couchdb": WatcherBackend},
            )

        self.assertIn("missing required 'db' field", str(ctx.exception))

    def test_unknown_backend_reports_available_backends(self):
        cfg = _external_systems(endpoint_backend="fs")

        with self.assertRaises(WatcherConfigurationError) as ctx:
            validate_watcher_config_wiring(
                bound_specs=[_bound_spec(backend="fs")],
                external_systems=cfg,
                backend_registry={"couchdb": WatcherBackend},
            )

        msg = str(ctx.exception)
        self.assertIn("watcher backend is not registered", msg)
        self.assertIn("Available backends: couchdb", msg)
        self.assertEqual(ctx.exception.issues[0].kind, "unknown_backend")

    def test_backend_mismatch_reports_watchspec_and_endpoint_backends(self):
        with self.assertRaises(WatcherConfigurationError) as ctx:
            validate_watcher_config_wiring(
                bound_specs=[_bound_spec(backend="postgres")],
                external_systems=_external_systems(endpoint_backend="couchdb"),
                backend_registry={"postgres": WatcherBackend},
            )

        msg = str(ctx.exception)
        self.assertIn("WatchSpec backend is 'postgres'", msg)
        self.assertIn("configured endpoint backend is 'couchdb'", msg)
        self.assertEqual(ctx.exception.issues[0].kind, "backend_mismatch")

    def test_multiple_issues_are_aggregated(self):
        specs = [
            _bound_spec(realm_id="dmx_realm", connection="missing_db"),
            _bound_spec(realm_id="fs_realm", backend="fs"),
        ]
        cfg = _external_systems(endpoint_backend="fs")

        with self.assertRaises(WatcherConfigurationError) as ctx:
            validate_watcher_config_wiring(
                bound_specs=specs,
                external_systems=cfg,
                backend_registry={"couchdb": WatcherBackend},
            )

        msg = str(ctx.exception)
        self.assertIn("2 issue(s)", msg)
        self.assertIn("missing_db", msg)
        self.assertIn("watcher backend is not registered", msg)
        self.assertEqual(len(ctx.exception.issues), 2)


if __name__ == "__main__":
    unittest.main()
