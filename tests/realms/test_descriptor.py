"""
Unit tests for yggdrasil.core.realm.descriptor module.

Tests the RealmDescriptor dataclass including watchspec resolution
from both static lists and callables.
"""

import unittest
from typing import Any, ClassVar

from lib.core_utils.event_types import EventType
from lib.watchers.watchspec import WatchSpec
from yggdrasil.core.realm import RealmDescriptor
from yggdrasil.flow.base_handler import BaseHandler
from yggdrasil.flow.planner.api import PlanDraft


class _FakeHandler(BaseHandler):
    """Minimal handler for descriptor tests."""

    event_type: ClassVar[EventType] = EventType.PROJECT_CHANGE
    handler_id: ClassVar[str] = "fake_handler"

    def derive_scope(self, doc: dict[str, Any]) -> dict[str, Any]:
        return {"kind": "project", "id": doc.get("project_id", "unknown")}

    async def generate_plan_drafts(self, payload: dict[str, Any]) -> list[PlanDraft]:
        raise NotImplementedError

    def __call__(self, payload: dict[str, Any]) -> None:
        pass


class TestRealmDescriptor(unittest.TestCase):
    """Tests for RealmDescriptor dataclass."""

    def test_basic_creation(self):
        """RealmDescriptor stores realm_id and handler_classes."""
        desc = RealmDescriptor(
            realm_id="test_realm",
            handler_classes=[_FakeHandler],
        )
        self.assertEqual(desc.realm_id, "test_realm")
        self.assertEqual(desc.handler_classes, [_FakeHandler])

    def test_get_watchspecs_empty(self):
        """get_watchspecs returns empty list when no watchspecs provided."""
        desc = RealmDescriptor(
            realm_id="empty",
            handler_classes=[_FakeHandler],
        )
        self.assertEqual(desc.get_watchspecs(), [])

    def test_get_watchspecs_static_list(self):
        """get_watchspecs returns static list of WatchSpecs."""
        spec = WatchSpec(
            backend="couchdb",
            connection="projects_db",
            event_type=EventType.COUCHDB_DOC_CHANGED,
            build_scope=lambda e: {"kind": "project", "id": e.id},
            build_payload=lambda e: {"doc": e.doc},
        )
        desc = RealmDescriptor(
            realm_id="with_specs",
            handler_classes=[_FakeHandler],
            watchspecs=[spec],
        )
        result = desc.get_watchspecs()
        self.assertEqual(len(result), 1)
        self.assertIs(result[0], spec)

    def test_get_watchspecs_callable(self):
        """get_watchspecs calls callable to produce WatchSpecs."""
        spec = WatchSpec(
            backend="couchdb",
            connection="projects_db",
            event_type=EventType.COUCHDB_DOC_CHANGED,
            build_scope=lambda e: {"kind": "project", "id": e.id},
            build_payload=lambda e: {"doc": e.doc},
        )

        def spec_factory():
            return [spec]

        desc = RealmDescriptor(
            realm_id="callable_specs",
            handler_classes=[_FakeHandler],
            watchspecs=spec_factory,
        )
        result = desc.get_watchspecs()
        self.assertEqual(len(result), 1)
        self.assertIs(result[0], spec)

    def test_get_watchspecs_default_empty_list(self):
        """Default watchspecs is empty list."""
        desc = RealmDescriptor(
            realm_id="default",
            handler_classes=[_FakeHandler],
        )
        self.assertEqual(desc.get_watchspecs(), [])

    def test_descriptor_is_frozen(self):
        """RealmDescriptor is frozen (immutable)."""
        desc = RealmDescriptor(
            realm_id="frozen",
            handler_classes=[_FakeHandler],
        )
        with self.assertRaises(AttributeError):
            desc.realm_id = "modified"  # type: ignore[misc]

    def test_multiple_handlers(self):
        """RealmDescriptor can hold multiple handler classes."""

        class _SecondHandler(_FakeHandler):
            handler_id: ClassVar[str] = "second_handler"

        desc = RealmDescriptor(
            realm_id="multi",
            handler_classes=[_FakeHandler, _SecondHandler],
        )
        self.assertEqual(len(desc.handler_classes), 2)


if __name__ == "__main__":
    unittest.main()
