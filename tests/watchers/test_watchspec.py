"""
Unit tests for lib.watchers.watchspec module.

Tests WatchSpec and BoundWatchSpec dataclasses.
"""

import unittest
from typing import Any

from lib.core_utils.event_types import EventType
from lib.watchers.backends.base import RawWatchEvent
from lib.watchers.watchspec import BoundWatchSpec, WatchSpec


def _build_scope(event: RawWatchEvent) -> dict[str, Any]:
    return {"kind": "project", "id": event.id}


def _build_payload(event: RawWatchEvent) -> dict[str, Any]:
    return {"doc": event.doc}


class TestWatchSpec(unittest.TestCase):
    """Tests for WatchSpec dataclass."""

    def _make_spec(self, **overrides: Any) -> WatchSpec:
        defaults: dict[str, Any] = {
            "backend": "couchdb",
            "connection": "projects_db",
            "event_type": EventType.COUCHDB_DOC_CHANGED,
            "build_scope": _build_scope,
            "build_payload": _build_payload,
        }
        defaults.update(overrides)
        return WatchSpec(**defaults)

    def test_basic_creation(self):
        spec = self._make_spec()
        self.assertEqual(spec.backend, "couchdb")
        self.assertEqual(spec.connection, "projects_db")
        self.assertEqual(spec.event_type, EventType.COUCHDB_DOC_CHANGED)
        self.assertIsNone(spec.target_handlers)
        self.assertIsNone(spec.filter_expr)

    def test_with_target_handlers(self):
        spec = self._make_spec(target_handlers=["handler_a", "handler_b"])
        self.assertEqual(spec.target_handlers, ["handler_a", "handler_b"])

    def test_with_filter_expr(self):
        filter_expr = {"==": [{"var": "doc.type"}, "project"]}
        spec = self._make_spec(filter_expr=filter_expr)
        self.assertEqual(spec.filter_expr, filter_expr)

    def test_frozen(self):
        spec = self._make_spec()
        with self.assertRaises(AttributeError):
            spec.backend = "fs"  # type: ignore[misc]

    def test_build_scope_callable(self):
        spec = self._make_spec()
        event = RawWatchEvent(id="P12345", doc={"_id": "P12345"})
        scope = spec.build_scope(event)
        self.assertEqual(scope, {"kind": "project", "id": "P12345"})

    def test_build_payload_callable(self):
        spec = self._make_spec()
        event = RawWatchEvent(id="P12345", doc={"_id": "P12345", "type": "project"})
        payload = spec.build_payload(event)
        self.assertEqual(payload, {"doc": {"_id": "P12345", "type": "project"}})


class TestBoundWatchSpec(unittest.TestCase):
    """Tests for BoundWatchSpec dataclass."""

    def _make_bound_spec(self, realm_id="my_realm", **spec_overrides) -> BoundWatchSpec:
        spec = WatchSpec(
            backend=spec_overrides.get("backend", "couchdb"),
            connection=spec_overrides.get("connection", "projects_db"),
            event_type=spec_overrides.get("event_type", EventType.COUCHDB_DOC_CHANGED),
            build_scope=_build_scope,
            build_payload=_build_payload,
            target_handlers=spec_overrides.get("target_handlers"),
            filter_expr=spec_overrides.get("filter_expr"),
        )
        return BoundWatchSpec(spec=spec, realm_id=realm_id)

    def test_basic_binding(self):
        bs = self._make_bound_spec(realm_id="tenx")
        self.assertEqual(bs.realm_id, "tenx")
        self.assertEqual(bs.spec.backend, "couchdb")

    def test_backend_group_key(self):
        bs = self._make_bound_spec(backend="couchdb", connection="projects_db")
        self.assertEqual(bs.backend_group_key, ("couchdb", "projects_db"))

    def test_different_realms_same_backend(self):
        """Two realms watching the same (backend, connection) produce same group key."""
        bs1 = self._make_bound_spec(realm_id="tenx")
        bs2 = self._make_bound_spec(realm_id="smartseq3")
        self.assertEqual(bs1.backend_group_key, bs2.backend_group_key)

    def test_different_connections(self):
        """Different connections produce different group keys."""
        bs1 = self._make_bound_spec(connection="projects_db")
        bs2 = self._make_bound_spec(connection="yggdrasil_db")
        self.assertNotEqual(bs1.backend_group_key, bs2.backend_group_key)


if __name__ == "__main__":
    unittest.main()
