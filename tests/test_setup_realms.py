"""
Unit tests for Phase 2: setup_realms(), handler identity registry,
and handle_event() routing with realm_id / target_handlers filtering.

These tests focus on the NEW Phase 2 code paths in YggdrasilCore.
"""

import logging
import unittest
from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock, Mock, patch

from lib.core_utils.event_types import EventType
from lib.core_utils.singleton_decorator import SingletonMeta
from lib.core_utils.yggdrasil_core import YggdrasilCore
from lib.watchers.abstract_watcher import YggdrasilEvent
from lib.watchers.watchspec import WatchSpec
from yggdrasil.core.realm import RealmDescriptor
from yggdrasil.flow.base_handler import BaseHandler
from yggdrasil.flow.planner.api import PlanDraft

# ---------------------------------------------------------------------------
# Test handler classes
# ---------------------------------------------------------------------------


class _ProjectHandlerA(BaseHandler):
    event_type: ClassVar[EventType] = EventType.PROJECT_CHANGE
    handler_id: ClassVar[str] = "handler_a"

    def derive_scope(self, doc: dict[str, Any]) -> dict[str, Any]:
        return {"kind": "project", "id": doc.get("project_id", "?")}

    async def generate_plan_drafts(self, payload: dict[str, Any]) -> list[PlanDraft]:
        raise NotImplementedError

    def __call__(self, payload: dict[str, Any]) -> None:
        pass


class _ProjectHandlerB(BaseHandler):
    event_type: ClassVar[EventType] = EventType.PROJECT_CHANGE
    handler_id: ClassVar[str] = "handler_b"

    def derive_scope(self, doc: dict[str, Any]) -> dict[str, Any]:
        return {"kind": "project", "id": doc.get("project_id", "?")}

    async def generate_plan_drafts(self, payload: dict[str, Any]) -> list[PlanDraft]:
        raise NotImplementedError

    def __call__(self, payload: dict[str, Any]) -> None:
        pass


class _FlowcellHandler(BaseHandler):
    event_type: ClassVar[EventType] = EventType.FLOWCELL_READY
    handler_id: ClassVar[str] = "flowcell_handler"

    def derive_scope(self, doc: dict[str, Any]) -> dict[str, Any]:
        return {"kind": "flowcell", "id": doc.get("flowcell_id", "?")}

    async def generate_plan_drafts(self, payload: dict[str, Any]) -> list[PlanDraft]:
        raise NotImplementedError

    def __call__(self, payload: dict[str, Any]) -> None:
        pass


class _CouchDBDocHandler(BaseHandler):
    event_type: ClassVar[EventType] = EventType.COUCHDB_DOC_CHANGED
    handler_id: ClassVar[str] = "couchdb_handler"

    def derive_scope(self, doc: dict[str, Any]) -> dict[str, Any]:
        return {"kind": "doc", "id": doc.get("_id", "?")}

    async def generate_plan_drafts(self, payload: dict[str, Any]) -> list[PlanDraft]:
        raise NotImplementedError

    def __call__(self, payload: dict[str, Any]) -> None:
        pass


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------


class TestSetupRealms(unittest.TestCase):
    """Tests for YggdrasilCore.setup_realms() and related helpers."""

    @classmethod
    def setUpClass(cls):
        SingletonMeta._instances.clear()

    @classmethod
    def tearDownClass(cls):
        SingletonMeta._instances.clear()

    def setUp(self):
        SingletonMeta._instances.clear()

        # Patch heavy infrastructure
        self.ops_patcher = patch("lib.core_utils.yggdrasil_core.OpsConsumerService")
        self.engine_patcher = patch("lib.core_utils.yggdrasil_core.Engine")
        self.db_patcher = patch(
            "lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers"
        )
        self.mock_ops = self.ops_patcher.start()
        self.mock_engine = self.engine_patcher.start()
        self.mock_db = self.db_patcher.start()

        ops_inst = Mock()
        ops_inst.start = Mock()
        ops_inst.stop = AsyncMock()
        self.mock_ops.return_value = ops_inst

        self.config = {"couchdb": {"poll_interval": 5}}
        self.logger = Mock(spec=logging.Logger)

    def tearDown(self):
        self.ops_patcher.stop()
        self.engine_patcher.stop()
        self.db_patcher.stop()
        SingletonMeta._instances.clear()

    def _make_core(self) -> YggdrasilCore:
        return YggdrasilCore(self.config, self.logger)

    # --- setup_realms basic flow ---

    @patch("importlib.metadata.entry_points", return_value=[])
    @patch("lib.core_utils.yggdrasil_core.discover_realms")
    @patch("lib.realms.test_realm.is_test_realm_enabled", return_value=False)
    def test_setup_realms_single_realm(self, _test_enabled, mock_discover, _mock_eps):
        """setup_realms registers handlers from a single discovered realm."""
        desc = RealmDescriptor(
            realm_id="tenx",
            handler_classes=[_ProjectHandlerA],
        )
        mock_discover.return_value = [desc]

        core = self._make_core()
        core.setup_realms()

        # Handler registered in subscriptions
        self.assertIn(EventType.PROJECT_CHANGE, core.subscriptions)
        handlers = core.subscriptions[EventType.PROJECT_CHANGE]
        self.assertEqual(len(handlers), 1)
        self.assertIsInstance(handlers[0], _ProjectHandlerA)
        self.assertEqual(handlers[0].realm_id, "tenx")

        # Realm registry populated
        self.assertIn("tenx", core._realm_registry)

        # Handler identity registry populated
        self.assertIn(("tenx", "handler_a"), core._handler_identity_registry)

    @patch("importlib.metadata.entry_points", return_value=[])
    @patch("lib.core_utils.yggdrasil_core.discover_realms")
    @patch("lib.realms.test_realm.is_test_realm_enabled", return_value=False)
    def test_setup_realms_multiple_realms(
        self, _test_enabled, mock_discover, _mock_eps
    ):
        """setup_realms registers handlers from multiple realms."""
        desc1 = RealmDescriptor(
            realm_id="tenx",
            handler_classes=[_ProjectHandlerA],
        )
        desc2 = RealmDescriptor(
            realm_id="smartseq3",
            handler_classes=[_ProjectHandlerB],
        )
        mock_discover.return_value = [desc1, desc2]

        core = self._make_core()
        core.setup_realms()

        handlers = core.subscriptions.get(EventType.PROJECT_CHANGE, [])
        self.assertEqual(len(handlers), 2)

        realm_ids = {h.realm_id for h in handlers}
        self.assertEqual(realm_ids, {"tenx", "smartseq3"})

    @patch("importlib.metadata.entry_points", return_value=[])
    @patch("lib.core_utils.yggdrasil_core.discover_realms")
    @patch("lib.realms.test_realm.is_test_realm_enabled", return_value=False)
    def test_setup_realms_no_realms(self, _test_enabled, mock_discover, _mock_eps):
        """setup_realms handles no realms discovered gracefully."""
        mock_discover.return_value = []

        core = self._make_core()
        core.setup_realms()

        self.assertEqual(core.subscriptions, {})
        self.assertEqual(core._realm_registry, {})

    # --- Realm ID uniqueness ---

    @patch("importlib.metadata.entry_points", return_value=[])
    @patch("lib.core_utils.yggdrasil_core.discover_realms")
    @patch("lib.realms.test_realm.is_test_realm_enabled", return_value=False)
    def test_duplicate_realm_id_raises(self, _test_enabled, mock_discover, _mock_eps):
        """Duplicate realm_id raises RuntimeError."""
        desc1 = RealmDescriptor(
            realm_id="duplicate",
            handler_classes=[_ProjectHandlerA],
        )
        desc2 = RealmDescriptor(
            realm_id="duplicate",
            handler_classes=[_ProjectHandlerB],
        )
        mock_discover.return_value = [desc1, desc2]

        core = self._make_core()
        with self.assertRaises(RuntimeError) as ctx:
            core.setup_realms()
        self.assertIn("Duplicate realm_id", str(ctx.exception))

    # --- Handler identity ---

    @patch("importlib.metadata.entry_points", return_value=[])
    @patch("lib.core_utils.yggdrasil_core.discover_realms")
    @patch("lib.realms.test_realm.is_test_realm_enabled", return_value=False)
    def test_handler_id_set_on_instance(self, _test_enabled, mock_discover, _mock_eps):
        """Handler instances have realm_id set by core."""
        desc = RealmDescriptor(
            realm_id="my_realm",
            handler_classes=[_ProjectHandlerA],
        )
        mock_discover.return_value = [desc]

        core = self._make_core()
        core.setup_realms()

        handler = core._handler_identity_registry[("my_realm", "handler_a")]
        self.assertEqual(handler.realm_id, "my_realm")

    @patch("importlib.metadata.entry_points", return_value=[])
    @patch("lib.core_utils.yggdrasil_core.discover_realms")
    @patch("lib.realms.test_realm.is_test_realm_enabled", return_value=False)
    def test_missing_handler_id_raises(self, _test_enabled, mock_discover, _mock_eps):
        """Handler class missing handler_id raises RuntimeError."""

        class _NoIdHandler(BaseHandler):
            event_type: ClassVar[EventType] = EventType.PROJECT_CHANGE
            # handler_id deliberately missing

            def derive_scope(self, doc):
                return {}

            async def generate_plan_drafts(self, payload):
                raise NotImplementedError

            def __call__(self, payload):
                pass

        desc = RealmDescriptor(
            realm_id="broken",
            handler_classes=[_NoIdHandler],
        )
        mock_discover.return_value = [desc]

        core = self._make_core()
        with self.assertRaises(RuntimeError) as ctx:
            core.setup_realms()
        self.assertIn("missing required 'handler_id'", str(ctx.exception))

    @patch("importlib.metadata.entry_points", return_value=[])
    @patch("lib.core_utils.yggdrasil_core.discover_realms")
    @patch("lib.realms.test_realm.is_test_realm_enabled", return_value=False)
    def test_missing_event_type_raises(self, _test_enabled, mock_discover, _mock_eps):
        """Handler class with invalid event_type raises RuntimeError."""

        class _BadEventHandler(BaseHandler):
            event_type = "not_an_enum"  # type: ignore[assignment]
            handler_id: ClassVar[str] = "bad_event"

            def derive_scope(self, doc):
                return {}

            async def generate_plan_drafts(self, payload):
                raise NotImplementedError

            def __call__(self, payload):
                pass

        desc = RealmDescriptor(
            realm_id="broken_event",
            handler_classes=[_BadEventHandler],
        )
        mock_discover.return_value = [desc]

        core = self._make_core()
        with self.assertRaises(RuntimeError) as ctx:
            core.setup_realms()
        self.assertIn("invalid or missing 'event_type'", str(ctx.exception))

    # --- WatchSpec validation ---

    @patch("importlib.metadata.entry_points", return_value=[])
    @patch("lib.core_utils.yggdrasil_core.discover_realms")
    @patch("lib.realms.test_realm.is_test_realm_enabled", return_value=False)
    def test_watchspec_target_handler_unknown_raises(
        self, _test_enabled, mock_discover, _mock_eps
    ):
        """WatchSpec referencing unknown handler_id raises RuntimeError."""
        spec = WatchSpec(
            backend="couchdb",
            connection="projects_db",
            event_type=EventType.COUCHDB_DOC_CHANGED,
            build_scope=lambda e: {"kind": "doc", "id": e.id},
            build_payload=lambda e: {"doc": e.doc},
            target_handlers=["nonexistent_handler"],
        )
        desc = RealmDescriptor(
            realm_id="realm_with_bad_spec",
            handler_classes=[_CouchDBDocHandler],
            watchspecs=[spec],
        )
        mock_discover.return_value = [desc]

        core = self._make_core()
        with self.assertRaises(RuntimeError) as ctx:
            core.setup_realms()
        self.assertIn("unknown handler_id", str(ctx.exception))

    @patch("importlib.metadata.entry_points", return_value=[])
    @patch("lib.core_utils.yggdrasil_core.discover_realms")
    @patch("lib.realms.test_realm.is_test_realm_enabled", return_value=False)
    def test_watchspec_no_receiver_raises(
        self, _test_enabled, mock_discover, _mock_eps
    ):
        """WatchSpec with event_type that no handler subscribes to raises RuntimeError."""
        # Handler subscribes to PROJECT_CHANGE, but spec emits COUCHDB_DOC_CHANGED
        spec = WatchSpec(
            backend="couchdb",
            connection="projects_db",
            event_type=EventType.COUCHDB_DOC_CHANGED,
            build_scope=lambda e: {"kind": "doc", "id": e.id},
            build_payload=lambda e: {"doc": e.doc},
        )
        desc = RealmDescriptor(
            realm_id="mismatched",
            handler_classes=[_ProjectHandlerA],  # subscribes to PROJECT_CHANGE
            watchspecs=[spec],  # emits COUCHDB_DOC_CHANGED
        )
        mock_discover.return_value = [desc]

        core = self._make_core()
        with self.assertRaises(RuntimeError) as ctx:
            core.setup_realms()
        self.assertIn("no handler in this realm subscribes", str(ctx.exception))

    @patch("importlib.metadata.entry_points", return_value=[])
    @patch("lib.core_utils.yggdrasil_core.discover_realms")
    @patch("lib.realms.test_realm.is_test_realm_enabled", return_value=False)
    def test_watchspec_valid_target_handler_passes(
        self, _test_enabled, mock_discover, _mock_eps
    ):
        """WatchSpec with valid target_handlers passes validation."""
        spec = WatchSpec(
            backend="couchdb",
            connection="projects_db",
            event_type=EventType.COUCHDB_DOC_CHANGED,
            build_scope=lambda e: {"kind": "doc", "id": e.id},
            build_payload=lambda e: {"doc": e.doc},
            target_handlers=["couchdb_handler"],
        )
        desc = RealmDescriptor(
            realm_id="valid_target",
            handler_classes=[_CouchDBDocHandler],
            watchspecs=[spec],
        )
        mock_discover.return_value = [desc]

        core = self._make_core()
        # _setup_watcher_manager does a local import; patch that path
        with patch("lib.watchers.manager.WatcherManager") as mock_wm_cls:
            mock_wm = Mock()
            mock_wm_cls.return_value = mock_wm
            core.setup_realms()

        self.assertIn(
            ("valid_target", "couchdb_handler"), core._handler_identity_registry
        )

    # --- Legacy handler wrapping ---

    @patch("lib.core_utils.yggdrasil_core.discover_realms")
    @patch("lib.realms.test_realm.is_test_realm_enabled", return_value=False)
    @patch("importlib.metadata.entry_points")
    def test_legacy_handler_wrapped_as_realm(
        self, mock_eps, _test_enabled, mock_discover
    ):
        """Legacy ygg.handler entry points are wrapped as RealmDescriptors."""
        mock_discover.return_value = []

        # Create a mock legacy handler class
        legacy_cls = type(
            "LegacyHandler",
            (_ProjectHandlerA,),
            {
                "event_type": EventType.PROJECT_CHANGE,
                "handler_id": "legacy_proj",
                "realm_id": None,
                "__module__": "legacy_pkg.handler",
                "__qualname__": "LegacyHandler",
            },
        )

        ep = MagicMock()
        ep.name = "legacy_proj"
        ep.value = "legacy_pkg.handler:LegacyHandler"
        ep.load.return_value = legacy_cls
        ep.dist = MagicMock()
        ep.dist.name = "legacy_pkg"
        mock_eps.return_value = [ep]

        core = self._make_core()
        core.setup_realms()

        # Should be registered with derived realm_id
        self.assertIn("legacy_pkg", core._realm_registry)


class TestHandleEventRouting(unittest.TestCase):
    """Tests for handle_event() with realm_id and target_handlers routing."""

    @classmethod
    def setUpClass(cls):
        SingletonMeta._instances.clear()

    @classmethod
    def tearDownClass(cls):
        SingletonMeta._instances.clear()

    def setUp(self):
        SingletonMeta._instances.clear()

        self.ops_patcher = patch("lib.core_utils.yggdrasil_core.OpsConsumerService")
        self.engine_patcher = patch("lib.core_utils.yggdrasil_core.Engine")
        self.db_patcher = patch(
            "lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers"
        )
        self.mock_ops = self.ops_patcher.start()
        self.mock_engine = self.engine_patcher.start()
        self.mock_db = self.db_patcher.start()

        ops_inst = Mock()
        ops_inst.start = Mock()
        ops_inst.stop = AsyncMock()
        self.mock_ops.return_value = ops_inst

        self.config = {"couchdb": {"poll_interval": 5}}
        self.logger = Mock(spec=logging.Logger)

    def tearDown(self):
        self.ops_patcher.stop()
        self.engine_patcher.stop()
        self.db_patcher.stop()
        SingletonMeta._instances.clear()

    def _make_core_with_handlers(self):
        """Create core with two handlers in different realms for same event."""
        core = YggdrasilCore(self.config, self.logger)

        handler_a = _ProjectHandlerA()
        handler_a.realm_id = "tenx"
        handler_b = _ProjectHandlerB()
        handler_b.realm_id = "smartseq3"

        core.subscriptions[EventType.PROJECT_CHANGE] = [handler_a, handler_b]
        core._handler_identity_registry[("tenx", "handler_a")] = handler_a
        core._handler_identity_registry[("smartseq3", "handler_b")] = handler_b

        return core, handler_a, handler_b

    @patch("lib.core_utils.yggdrasil_core.asyncio.create_task")
    def test_broadcast_to_all_handlers(self, mock_create_task):
        """Without routing hints, event is broadcast to ALL handlers."""
        core, handler_a, handler_b = self._make_core_with_handlers()

        event = YggdrasilEvent(
            event_type=EventType.PROJECT_CHANGE,
            payload={
                "doc": {"project_id": "P123"},
                "scope": {"kind": "project", "id": "P123"},
            },
            source="test",
        )

        core.handle_event(event)

        # Both handlers should have tasks created
        self.assertEqual(mock_create_task.call_count, 2)

    @patch("lib.core_utils.yggdrasil_core.asyncio.create_task")
    def test_filter_by_realm_id(self, mock_create_task):
        """With realm_id in payload, only that realm's handlers are invoked."""
        core, handler_a, handler_b = self._make_core_with_handlers()

        event = YggdrasilEvent(
            event_type=EventType.PROJECT_CHANGE,
            payload={
                "doc": {"project_id": "P123"},
                "scope": {"kind": "project", "id": "P123"},
                "realm_id": "tenx",
            },
            source="test",
        )

        core.handle_event(event)

        # Only handler_a (tenx) should be invoked
        self.assertEqual(mock_create_task.call_count, 1)

    @patch("lib.core_utils.yggdrasil_core.asyncio.create_task")
    def test_filter_by_target_handlers(self, mock_create_task):
        """With target_handlers in payload, only matching handler_ids are invoked."""
        core, handler_a, handler_b = self._make_core_with_handlers()

        event = YggdrasilEvent(
            event_type=EventType.PROJECT_CHANGE,
            payload={
                "doc": {"project_id": "P123"},
                "scope": {"kind": "project", "id": "P123"},
                "target_handlers": ["handler_b"],
            },
            source="test",
        )

        core.handle_event(event)

        # Only handler_b (smartseq3) should be invoked
        self.assertEqual(mock_create_task.call_count, 1)

    @patch("lib.core_utils.yggdrasil_core.asyncio.create_task")
    def test_filter_by_realm_and_target(self, mock_create_task):
        """Both realm_id and target_handlers narrow to specific handler."""
        core, handler_a, handler_b = self._make_core_with_handlers()

        event = YggdrasilEvent(
            event_type=EventType.PROJECT_CHANGE,
            payload={
                "doc": {"project_id": "P123"},
                "scope": {"kind": "project", "id": "P123"},
                "realm_id": "tenx",
                "target_handlers": ["handler_a"],
            },
            source="test",
        )

        core.handle_event(event)
        self.assertEqual(mock_create_task.call_count, 1)

    @patch("lib.core_utils.yggdrasil_core.asyncio.create_task")
    def test_unknown_realm_id_no_dispatch(self, mock_create_task):
        """Unknown realm_id means no handlers are invoked."""
        core, _, _ = self._make_core_with_handlers()

        event = YggdrasilEvent(
            event_type=EventType.PROJECT_CHANGE,
            payload={
                "doc": {"project_id": "P123"},
                "scope": {"kind": "project", "id": "P123"},
                "realm_id": "nonexistent_realm",
            },
            source="test",
        )

        core.handle_event(event)
        mock_create_task.assert_not_called()

    @patch("lib.core_utils.yggdrasil_core.asyncio.create_task")
    def test_routing_hints_popped_from_payload(self, mock_create_task):
        """realm_id and target_handlers are popped and don't leak into handler payload."""
        core, handler_a, _ = self._make_core_with_handlers()

        event = YggdrasilEvent(
            event_type=EventType.PROJECT_CHANGE,
            payload={
                "doc": {"project_id": "P123"},
                "scope": {"kind": "project", "id": "P123"},
                "realm_id": "tenx",
                "target_handlers": ["handler_a"],
            },
            source="test",
        )

        core.handle_event(event)

        # The create_task call should have been invoked with a coroutine
        # that received a payload WITHOUT realm_id/target_handlers
        self.assertEqual(mock_create_task.call_count, 1)

    def test_no_subscribers_logs_warning(self):
        """Event with no subscribers logs warning."""
        core = YggdrasilCore(self.config, self.logger)

        event = YggdrasilEvent(
            event_type=EventType.DELIVERY_READY,
            payload={},
            source="test",
        )

        core.handle_event(event)
        # Logger should have warning about no subscribers
        warning_calls = [
            c
            for c in self.logger.method_calls
            if c[0] == "warning" and "No subscribers" in str(c)
        ]
        self.assertTrue(len(warning_calls) > 0)


class TestGetRealmHelpers(unittest.TestCase):
    """Tests for _get_realm_handler_ids() and _get_realm_event_types()."""

    @classmethod
    def setUpClass(cls):
        SingletonMeta._instances.clear()

    @classmethod
    def tearDownClass(cls):
        SingletonMeta._instances.clear()

    def setUp(self):
        SingletonMeta._instances.clear()
        self.ops_patcher = patch("lib.core_utils.yggdrasil_core.OpsConsumerService")
        self.engine_patcher = patch("lib.core_utils.yggdrasil_core.Engine")
        self.db_patcher = patch(
            "lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers"
        )
        self.mock_ops = self.ops_patcher.start()
        self.mock_engine = self.engine_patcher.start()
        self.mock_db = self.db_patcher.start()

        ops_inst = Mock()
        ops_inst.start = Mock()
        ops_inst.stop = AsyncMock()
        self.mock_ops.return_value = ops_inst

    def tearDown(self):
        self.ops_patcher.stop()
        self.engine_patcher.stop()
        self.db_patcher.stop()
        SingletonMeta._instances.clear()

    def test_get_realm_handler_ids(self):
        core = YggdrasilCore({})

        handler_a = _ProjectHandlerA()
        handler_a.realm_id = "tenx"
        handler_b = _ProjectHandlerB()
        handler_b.realm_id = "tenx"

        core._handler_identity_registry[("tenx", "handler_a")] = handler_a
        core._handler_identity_registry[("tenx", "handler_b")] = handler_b
        core._handler_identity_registry[("other", "handler_c")] = Mock()

        ids = core._get_realm_handler_ids("tenx")
        self.assertEqual(sorted(ids), ["handler_a", "handler_b"])

    def test_get_realm_event_types(self):
        core = YggdrasilCore({})

        handler_a = _ProjectHandlerA()
        handler_a.realm_id = "tenx"
        handler_fc = _FlowcellHandler()
        handler_fc.realm_id = "tenx"

        core._handler_identity_registry[("tenx", "handler_a")] = handler_a
        core._handler_identity_registry[("tenx", "flowcell_handler")] = handler_fc

        event_types = core._get_realm_event_types("tenx")
        self.assertEqual(
            event_types, {EventType.PROJECT_CHANGE, EventType.FLOWCELL_READY}
        )

    def test_get_realm_handler_ids_empty(self):
        core = YggdrasilCore({})
        ids = core._get_realm_handler_ids("nonexistent")
        self.assertEqual(ids, [])

    def test_get_realm_event_types_empty(self):
        core = YggdrasilCore({})
        event_types = core._get_realm_event_types("nonexistent")
        self.assertEqual(event_types, set())


if __name__ == "__main__":
    unittest.main()
