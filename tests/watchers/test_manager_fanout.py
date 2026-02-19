"""
Unit tests for WatcherManager fan-out (Phase 2).

Tests add_watchspec(), _consume_backend(), and _fan_out() methods.
"""

import asyncio
import logging
import unittest
from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import MagicMock, Mock

from lib.core_utils.event_types import EventType
from lib.watchers.abstract_watcher import YggdrasilEvent
from lib.watchers.backends.base import RawWatchEvent, WatcherBackend
from lib.watchers.manager import WatcherBackendGroup, WatcherManager
from lib.watchers.watchspec import BoundWatchSpec, WatchSpec


def _build_scope(event: RawWatchEvent) -> dict[str, Any]:
    return {"kind": "project", "id": event.id}


def _build_payload(event: RawWatchEvent) -> dict[str, Any]:
    return {"doc": event.doc or {}}


class MockBackend(WatcherBackend):
    """Mock backend that yields pre-loaded events."""

    def __init__(self, events_to_emit=None, **kwargs):
        super().__init__(**kwargs)
        self._events_to_emit = events_to_emit or []

    async def stream(self) -> AsyncIterator[RawWatchEvent]:
        for event in self._events_to_emit:
            if not self._running:
                break
            yield event

    async def start(self) -> None:
        await super().start()


class TestAddWatchSpec(unittest.TestCase):
    """Tests for WatcherManager.add_watchspec()."""

    def _make_manager(self, **kwargs):
        return WatcherManager(
            config={"endpoints": {}, "connections": {}},
            **kwargs,
        )

    def _make_bound_spec(
        self,
        realm_id="my_realm",
        backend="couchdb",
        connection="projects_db",
        event_type=EventType.COUCHDB_DOC_CHANGED,
        target_handlers=None,
        filter_expr=None,
    ):
        spec = WatchSpec(
            backend=backend,
            connection=connection,
            event_type=event_type,
            build_scope=_build_scope,
            build_payload=_build_payload,
            target_handlers=target_handlers,
            filter_expr=filter_expr,
        )
        return BoundWatchSpec(spec=spec, realm_id=realm_id)

    def test_add_watchspec_creates_backend_group(self):
        """add_watchspec creates a backend group for the spec's (backend, connection)."""
        mgr = self._make_manager()
        bs = self._make_bound_spec()

        mgr.add_watchspec(bs)

        groups = mgr.get_watcher_groups()
        self.assertIn(("couchdb", "projects_db"), groups)

    def test_add_watchspec_registers_bound_spec(self):
        """add_watchspec registers the BoundWatchSpec for fan-out."""
        mgr = self._make_manager()
        bs = self._make_bound_spec()

        mgr.add_watchspec(bs)

        bound = mgr.get_bound_specs()
        self.assertIn(("couchdb", "projects_db"), bound)
        self.assertEqual(len(bound[("couchdb", "projects_db")]), 1)

    def test_deduplication_shared_backend(self):
        """Multiple specs with same (backend, connection) share one backend group."""
        mgr = self._make_manager()
        bs1 = self._make_bound_spec(realm_id="tenx")
        bs2 = self._make_bound_spec(realm_id="smartseq3")

        mgr.add_watchspec(bs1)
        mgr.add_watchspec(bs2)

        groups = mgr.get_watcher_groups()
        self.assertEqual(len(groups), 1)

        bound = mgr.get_bound_specs()
        self.assertEqual(len(bound[("couchdb", "projects_db")]), 2)

    def test_different_connections_separate_groups(self):
        """Different connections create separate backend groups."""
        mgr = self._make_manager()
        bs1 = self._make_bound_spec(connection="projects_db")
        bs2 = self._make_bound_spec(connection="yggdrasil_db")

        mgr.add_watchspec(bs1)
        mgr.add_watchspec(bs2)

        groups = mgr.get_watcher_groups()
        self.assertEqual(len(groups), 2)


class TestFanOut(unittest.TestCase):
    """Tests for WatcherManager._fan_out()."""

    def _make_manager(self, on_event=None):
        return WatcherManager(
            config={"endpoints": {}, "connections": {}},
            on_event=on_event,
        )

    def _make_bound_spec(
        self,
        realm_id="my_realm",
        filter_expr=None,
        target_handlers=None,
        event_type=EventType.COUCHDB_DOC_CHANGED,
    ):
        spec = WatchSpec(
            backend="couchdb",
            connection="projects_db",
            event_type=event_type,
            build_scope=_build_scope,
            build_payload=_build_payload,
            target_handlers=target_handlers,
            filter_expr=filter_expr,
        )
        return BoundWatchSpec(spec=spec, realm_id=realm_id)

    def test_fan_out_no_filter_matches(self):
        """Without filter_expr, all specs match."""
        collected: list[YggdrasilEvent] = []
        mgr = self._make_manager(on_event=collected.append)

        bs = self._make_bound_spec(realm_id="tenx")
        raw = RawWatchEvent(id="P123", doc={"_id": "P123", "type": "project"})

        mgr._fan_out(raw, [bs], source="couchdb:projects_db")

        self.assertEqual(len(collected), 1)
        event = collected[0]
        self.assertEqual(event.event_type, EventType.COUCHDB_DOC_CHANGED)
        self.assertEqual(event.payload["realm_id"], "tenx")
        self.assertEqual(event.payload["scope"]["id"], "P123")
        self.assertEqual(event.source, "couchdb:projects_db")

    def test_fan_out_filter_match(self):
        """Filter matching produces event."""
        collected: list[YggdrasilEvent] = []
        mgr = self._make_manager(on_event=collected.append)

        filter_expr = {"==": [{"var": "doc.type"}, "project"]}
        bs = self._make_bound_spec(filter_expr=filter_expr)
        raw = RawWatchEvent(id="P123", doc={"type": "project"})

        mgr._fan_out(raw, [bs], source="test")

        self.assertEqual(len(collected), 1)

    def test_fan_out_filter_no_match(self):
        """Filter not matching produces no event."""
        collected: list[YggdrasilEvent] = []
        mgr = self._make_manager(on_event=collected.append)

        filter_expr = {"==": [{"var": "doc.type"}, "flowcell"]}
        bs = self._make_bound_spec(filter_expr=filter_expr)
        raw = RawWatchEvent(id="P123", doc={"type": "project"})

        mgr._fan_out(raw, [bs], source="test")

        self.assertEqual(len(collected), 0)

    def test_fan_out_multiple_specs(self):
        """Multiple specs evaluated independently per event."""
        collected: list[YggdrasilEvent] = []
        mgr = self._make_manager(on_event=collected.append)

        bs1 = self._make_bound_spec(realm_id="tenx")
        bs2 = self._make_bound_spec(
            realm_id="smartseq3",
            filter_expr={"==": [{"var": "doc.type"}, "flowcell"]},
        )
        raw = RawWatchEvent(id="P123", doc={"type": "project"})

        mgr._fan_out(raw, [bs1, bs2], source="test")

        # bs1 matches (no filter), bs2 does not (type != flowcell)
        self.assertEqual(len(collected), 1)
        self.assertEqual(collected[0].payload["realm_id"], "tenx")

    def test_fan_out_injects_target_handlers(self):
        """target_handlers from spec are injected into payload."""
        collected: list[YggdrasilEvent] = []
        mgr = self._make_manager(on_event=collected.append)

        bs = self._make_bound_spec(target_handlers=["handler_a"])
        raw = RawWatchEvent(id="P123", doc={"type": "project"})

        mgr._fan_out(raw, [bs], source="test")

        self.assertEqual(len(collected), 1)
        self.assertEqual(collected[0].payload["target_handlers"], ["handler_a"])

    def test_fan_out_no_on_event_logs_warning(self):
        """Without on_event callback, fan_out logs warning."""
        logger = Mock(spec=logging.Logger)
        mgr = self._make_manager(on_event=None)
        mgr._logger = logger

        bs = self._make_bound_spec()
        raw = RawWatchEvent(id="P123", doc={})

        mgr._fan_out(raw, [bs], source="test")

        warning_calls = [c for c in logger.warning.call_args_list]
        self.assertTrue(any("No on_event callback" in str(c) for c in warning_calls))

    def test_fan_out_build_scope_error(self):
        """Error in build_scope is caught and logged."""
        collected: list[YggdrasilEvent] = []
        logger = Mock(spec=logging.Logger)
        mgr = self._make_manager(on_event=collected.append)
        mgr._logger = logger

        def bad_scope(event):
            raise ValueError("scope error")

        spec = WatchSpec(
            backend="couchdb",
            connection="projects_db",
            event_type=EventType.COUCHDB_DOC_CHANGED,
            build_scope=bad_scope,
            build_payload=_build_payload,
        )
        bs = BoundWatchSpec(spec=spec, realm_id="broken")
        raw = RawWatchEvent(id="P123", doc={})

        mgr._fan_out(raw, [bs], source="test")

        self.assertEqual(len(collected), 0)
        error_calls = [c for c in logger.error.call_args_list]
        self.assertTrue(
            any("Scope/payload build failed" in str(c) for c in error_calls)
        )

    def test_fan_out_on_event_exception_caught(self):
        """Exception in on_event callback is caught and logged."""
        logger = Mock(spec=logging.Logger)

        def exploding_callback(event):
            raise RuntimeError("callback boom")

        mgr = self._make_manager(on_event=exploding_callback)
        mgr._logger = logger

        bs = self._make_bound_spec()
        raw = RawWatchEvent(id="P123", doc={})

        # Should not raise
        mgr._fan_out(raw, [bs], source="test")

        error_calls = [c for c in logger.error.call_args_list]
        self.assertTrue(any("on_event callback failed" in str(c) for c in error_calls))


class TestConsumeBackend(unittest.TestCase):
    """Tests for WatcherManager._consume_backend() async method."""

    def test_consume_backend_dispatches_events(self):
        """_consume_backend reads events from backend and calls _fan_out."""
        collected: list[YggdrasilEvent] = []
        mgr = WatcherManager(
            config={"endpoints": {}, "connections": {}},
            on_event=collected.append,
        )

        raw_event = RawWatchEvent(id="P123", doc={"type": "project"})

        # Create a mock group with a mock backend
        group = WatcherBackendGroup(
            backend_type="couchdb",
            connection="projects_db",
        )

        # Mock backend with async events() generator
        mock_backend = MagicMock()
        mock_backend.backend_key = "couchdb:projects_db"

        async def mock_events():
            yield raw_event

        mock_backend.events = mock_events
        group.backend_instance = mock_backend

        bs = BoundWatchSpec(
            spec=WatchSpec(
                backend="couchdb",
                connection="projects_db",
                event_type=EventType.COUCHDB_DOC_CHANGED,
                build_scope=_build_scope,
                build_payload=_build_payload,
            ),
            realm_id="tenx",
        )

        # Run _consume_backend
        asyncio.run(mgr._consume_backend(group, [bs]))

        self.assertEqual(len(collected), 1)
        self.assertEqual(collected[0].payload["realm_id"], "tenx")


if __name__ == "__main__":
    unittest.main()
