"""Unit tests for yggdrasil.flow.data_access.DataAccess.

Tests cover allowlist enforcement, caching, error conditions, and the
PlanningContext guard. All tests are filesystem-free (injected config +
mocked CouchDBHandler).
"""

import unittest
from unittest.mock import Mock, patch

from yggdrasil.flow.data_access import (
    DataAccess,
    DataAccessConfigError,
    DataAccessDeniedError,
    DataAccessError,
)
from yggdrasil.flow.data_access.couchdb_read import CouchDBReadClient

# ---------------------------------------------------------------------------
# Shared test config
# ---------------------------------------------------------------------------

SAMPLE_CFG = {
    "endpoints": {
        "couchdb": {
            "backend": "couchdb",
            "url": "http://couch.example.org:5984",
            "auth": {"user_env": "MY_USER", "pass_env": "MY_PASS"},
        }
    },
    "data_access_defaults": {"couchdb": {"max_limit": 150}},
    "connections": {
        "allowed_db": {
            "endpoint": "couchdb",
            "resource": {"db": "allowed"},
            "data_access": {
                "realm_allowlist": ["demux", "tenx"],
            },
        },
        "restricted_db": {
            "endpoint": "couchdb",
            "resource": {"db": "restricted"},
            "data_access": {
                "realm_allowlist": ["admin"],
            },
        },
        "no_policy_db": {
            "endpoint": "couchdb",
            "resource": {"db": "no_policy"},
            # No data_access block
        },
    },
}


# ---------------------------------------------------------------------------
# TestDataAccess
# ---------------------------------------------------------------------------


class TestDataAccess(unittest.TestCase):
    """Tests for DataAccess allowlist enforcement and caching."""

    def _make_da(self, realm_id: str) -> DataAccess:
        """Create a DataAccess with injected config (no disk access)."""
        return DataAccess(realm_id=realm_id, cfg=SAMPLE_CFG)

    # --- Allowlist enforcement ---

    @patch("lib.couchdb.couchdb_connection.CouchDBHandler")
    def test_allowed_realm_returns_client(self, MockHandler):
        """Allowed realm gets a CouchDBReadClient for the connection."""
        da = self._make_da("demux")
        client = da.couchdb("allowed_db")
        self.assertIsInstance(client, CouchDBReadClient)

    @patch("lib.couchdb.couchdb_connection.CouchDBHandler")
    def test_second_allowed_realm_returns_client(self, MockHandler):
        """Second realm in the allowlist also gets a client."""
        da = self._make_da("tenx")
        client = da.couchdb("allowed_db")
        self.assertIsInstance(client, CouchDBReadClient)

    def test_realm_not_in_allowlist_raises(self):
        """Realm not in allowlist raises DataAccessDeniedError."""
        da = self._make_da("smartseq3")
        with self.assertRaises(DataAccessDeniedError) as ctx:
            da.couchdb("restricted_db")
        self.assertIn("smartseq3", str(ctx.exception))
        self.assertIn("restricted_db", str(ctx.exception))

    def test_connection_with_no_policy_raises(self):
        """Connection without data_access block raises DataAccessDeniedError."""
        da = self._make_da("demux")
        with self.assertRaises(DataAccessDeniedError) as ctx:
            da.couchdb("no_policy_db")
        self.assertIn("no_policy_db", str(ctx.exception))

    def test_denied_error_is_subclass_of_data_access_error(self):
        """DataAccessDeniedError is-a DataAccessError."""
        da = self._make_da("demux")
        with self.assertRaises(DataAccessError):
            da.couchdb("no_policy_db")

    # --- Caching ---

    @patch("lib.couchdb.couchdb_connection.CouchDBHandler")
    def test_same_connection_called_twice_returns_same_client(self, MockHandler):
        """couchdb() returns cached client on second call (same instance)."""
        da = self._make_da("demux")
        client1 = da.couchdb("allowed_db")
        client2 = da.couchdb("allowed_db")
        self.assertIs(client1, client2)

    @patch("lib.couchdb.couchdb_connection.CouchDBHandler")
    def test_handler_created_only_once_for_same_connection(self, MockHandler):
        """CouchDBHandler is instantiated only once for repeated couchdb() calls."""
        da = self._make_da("demux")
        da.couchdb("allowed_db")
        da.couchdb("allowed_db")
        da.couchdb("allowed_db")
        # Handler constructor should be called exactly once
        self.assertEqual(MockHandler.call_count, 1)

    # --- Config loaded once ---

    @patch("yggdrasil.flow.data_access.data_access.load_external_systems_config")
    def test_config_loaded_once_in_init(self, mock_loader):
        """load_external_systems_config() called once at __init__, not per couchdb()."""
        mock_loader.return_value = SAMPLE_CFG
        DataAccess(realm_id="demux")
        self.assertEqual(mock_loader.call_count, 1)

    @patch("yggdrasil.flow.data_access.data_access.load_external_systems_config")
    @patch("lib.couchdb.couchdb_connection.CouchDBHandler")
    def test_config_not_reloaded_on_multiple_couchdb_calls(
        self, MockHandler, mock_loader
    ):
        """Repeated couchdb() calls do not trigger additional config loads."""
        mock_loader.return_value = SAMPLE_CFG
        da = DataAccess(realm_id="demux")
        da.couchdb("allowed_db")
        da.couchdb("allowed_db")
        # Still only one call at __init__
        self.assertEqual(mock_loader.call_count, 1)

    # --- Unknown connection ---

    def test_unknown_connection_raises_config_error(self):
        """Requesting a connection not in config raises DataAccessConfigError."""
        da = self._make_da("demux")
        with self.assertRaises(DataAccessConfigError) as ctx:
            da.couchdb("nonexistent_db")
        self.assertIn("nonexistent_db", str(ctx.exception))

    def test_unknown_connection_error_lists_available(self):
        """DataAccessConfigError message includes available connection names."""
        da = self._make_da("demux")
        with self.assertRaises(DataAccessConfigError) as ctx:
            da.couchdb("nonexistent_db")
        error_msg = str(ctx.exception)
        # At least one known connection should appear in the message
        self.assertTrue(
            any(
                conn in error_msg
                for conn in ("allowed_db", "restricted_db", "no_policy_db")
            ),
            f"Expected available connections in error message, got: {error_msg}",
        )

    def test_config_error_is_subclass_of_data_access_error(self):
        """DataAccessConfigError is-a DataAccessError."""
        da = self._make_da("demux")
        with self.assertRaises(DataAccessError):
            da.couchdb("nonexistent_db")

    def test_unknown_connection_does_not_leak_keyerror(self):
        """Raw KeyError must not propagate; only DataAccessConfigError should."""
        da = self._make_da("demux")
        try:
            da.couchdb("nonexistent_db")
        except DataAccessConfigError:
            pass  # expected
        except KeyError as exc:
            self.fail(f"Raw KeyError leaked instead of DataAccessConfigError: {exc}")


# ---------------------------------------------------------------------------
# TestPlanningContextGuard
# ---------------------------------------------------------------------------


class TestPlanningContextGuard(unittest.TestCase):
    """Tests for PlanningContext construction and build_planning_context() helper."""

    def setUp(self):
        from pathlib import Path
        from tempfile import TemporaryDirectory

        self._tmpdir = TemporaryDirectory()
        self.scope_dir = Path(self._tmpdir.name)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_construction_with_mock_data_passes(self):
        """PlanningContext() with mock data sets ctx.data correctly."""
        from yggdrasil.flow.planner.api import PlanningContext

        ctx = PlanningContext(
            realm="test",
            scope={},
            scope_dir=self.scope_dir,
            emitter=None,
            source_doc={},
            reason="test",
            data=Mock(),
        )
        self.assertIsNotNone(ctx.data)

    def test_build_planning_context_returns_populated_context(self):
        """build_planning_context() returns PlanningContext with data set."""
        from lib.core_utils.event_types import EventType
        from yggdrasil.flow.base_handler import BaseHandler
        from yggdrasil.flow.planner.api import PlanningContext

        class _Handler(BaseHandler):
            event_type = EventType.PROJECT_CHANGE
            handler_id = "test_handler"

            def derive_scope(self, doc):
                return {"kind": "project", "id": doc.get("id", "x")}

            async def generate_plan_draft(self, payload):  # noqa: D102
                ...

        handler = _Handler()
        handler.realm_id = "demux"

        # Patching DataAccess so it doesn't try to load config
        with patch(
            "yggdrasil.flow.data_access.data_access.load_external_systems_config",
            return_value=SAMPLE_CFG,
        ):
            ctx = handler.build_planning_context(
                scope={"kind": "project", "id": "P123"},
                scope_dir=self.scope_dir,
                emitter=None,
                source_doc={"id": "P123"},
                reason="test",
            )

        self.assertIsInstance(ctx, PlanningContext)
        self.assertEqual(ctx.realm, "demux")
        self.assertIsNotNone(ctx.data)

    def test_data_access_is_lazy_per_handler_instance(self):
        """_data_access lazy property returns same instance on repeated access."""
        from lib.core_utils.event_types import EventType
        from yggdrasil.flow.base_handler import BaseHandler

        class _Handler(BaseHandler):
            event_type = EventType.PROJECT_CHANGE
            handler_id = "lazy_test"

            def derive_scope(self, doc):
                return {}

            async def generate_plan_draft(self, payload): ...

        handler = _Handler()
        handler.realm_id = "demux"

        with patch(
            "yggdrasil.flow.data_access.data_access.load_external_systems_config",
            return_value=SAMPLE_CFG,
        ):
            da1 = handler._data_access
            da2 = handler._data_access

        self.assertIs(da1, da2)


if __name__ == "__main__":
    unittest.main()
