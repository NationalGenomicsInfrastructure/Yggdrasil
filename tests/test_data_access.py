"""Unit tests for yggdrasil.flow.data_access.DataAccess.

Tests cover permission enforcement, phase-aware client selection, caching,
error conditions, and the PlanningContext guard. All tests are filesystem-free
(injected config + mocked CouchDBHandler).
"""

import unittest
from unittest.mock import MagicMock, Mock, patch

from yggdrasil.flow.data_access import (
    DataAccess,
    DataAccessConfigError,
    DataAccessDeniedError,
    DataAccessError,
)
from yggdrasil.flow.data_access.couchdb_data import (
    CouchDBExecutionClient,
    CouchDBPlanningClient,
)

# ---------------------------------------------------------------------------
# Shared test config
# ---------------------------------------------------------------------------

SAMPLE_CFG = {
    "endpoints": {
        "couchdb": {
            "backend": "couchdb",
            "url": "http://couch.example.org:5984",
            "auth": {"user_env": "MY_USER", "pass_env": "MY_PASS"},
        },
        "postgres": {
            "backend": "postgres",
            "url": "http://pg.example.org:5432",
        },
    },
    "defaults": {"couchdb": {"max_limit": 150}},
    "connections": {
        "allowed_db": {
            "endpoint": "couchdb",
            "resource": {"db": "allowed"},
            "data_access": {
                "realms": {
                    "demux": {
                        "planning": {"permissions": ["read"]},
                        "execution": {"permissions": ["read", "write"]},
                    },
                    "tenx": {
                        "planning": {"permissions": ["read"]},
                        "execution": {"permissions": ["read"]},
                    },
                }
            },
        },
        "restricted_db": {
            "endpoint": "couchdb",
            "resource": {"db": "restricted"},
            "data_access": {
                "realms": {
                    "admin": {
                        "planning": {"permissions": ["read"]},
                        "execution": {"permissions": ["read", "write"]},
                    }
                }
            },
        },
        "write_only_db": {
            "endpoint": "couchdb",
            "resource": {"db": "write_only"},
            "data_access": {
                "realms": {
                    "dmx": {
                        "execution": {"permissions": ["write"]},
                    }
                }
            },
        },
        "no_policy_db": {
            "endpoint": "couchdb",
            "resource": {"db": "no_policy"},
            # No data_access block
        },
        "pg_db": {
            "endpoint": "postgres",
            "resource": {"db": "pgdb"},
            "data_access": {
                "realms": {"demux": {"execution": {"permissions": ["read"]}}}
            },
        },
    },
}

_PATCH_HANDLER = "lib.couchdb.couchdb_connection.CouchDBHandler"


def _mock_handler():
    m = MagicMock()
    m.fetch_document_by_id.return_value = None
    m.find_documents.return_value = []
    return m


# ---------------------------------------------------------------------------
# TestDataAccess — phase-aware client selection
# ---------------------------------------------------------------------------


class TestDataAccessConnection(unittest.TestCase):
    """Tests for DataAccess.connection() phase-aware client selection."""

    @patch(_PATCH_HANDLER)
    def test_planning_phase_returns_planning_client(self, MockHandler):
        MockHandler.return_value = _mock_handler()
        da = DataAccess("demux", phase="planning", cfg=SAMPLE_CFG)
        client = da.connection("allowed_db")
        self.assertIsInstance(client, CouchDBPlanningClient)

    @patch(_PATCH_HANDLER)
    def test_execution_phase_returns_execution_client(self, MockHandler):
        MockHandler.return_value = _mock_handler()
        da = DataAccess("demux", phase="execution", cfg=SAMPLE_CFG)
        client = da.connection("allowed_db")
        self.assertIsInstance(client, CouchDBExecutionClient)

    @patch(_PATCH_HANDLER)
    def test_connection_cached_second_call_returns_same_instance(self, MockHandler):
        MockHandler.return_value = _mock_handler()
        da = DataAccess("demux", phase="execution", cfg=SAMPLE_CFG)
        c1 = da.connection("allowed_db")
        c2 = da.connection("allowed_db")
        self.assertIs(c1, c2)

    @patch(_PATCH_HANDLER)
    def test_handler_created_only_once_for_same_connection(self, MockHandler):
        MockHandler.return_value = _mock_handler()
        da = DataAccess("demux", phase="execution", cfg=SAMPLE_CFG)
        da.connection("allowed_db")
        da.connection("allowed_db")
        da.connection("allowed_db")
        self.assertEqual(MockHandler.call_count, 1)

    # --- Permission denial ---

    def test_connection_denied_when_no_permissions_for_phase(self):
        """Realm has planning permissions=[] (empty) → denied."""
        cfg = {
            "endpoints": {"couchdb": {"backend": "couchdb", "url": "http://h:5984"}},
            "connections": {
                "db": {
                    "endpoint": "couchdb",
                    "resource": {"db": "x"},
                    "data_access": {
                        "realms": {"demux": {"planning": {"permissions": []}}}
                    },
                }
            },
        }
        da = DataAccess("demux", phase="planning", cfg=cfg)
        with self.assertRaises(DataAccessDeniedError) as ctx:
            da.connection("db")
        self.assertIn("demux", str(ctx.exception))

    def test_connection_denied_when_realm_not_in_policy(self):
        da = DataAccess("smartseq3", phase="execution", cfg=SAMPLE_CFG)
        with self.assertRaises(DataAccessDeniedError) as ctx:
            da.connection("restricted_db")
        self.assertIn("smartseq3", str(ctx.exception))

    def test_connection_denied_when_phase_not_in_policy(self):
        """write_only_db has only execution phase; planning → denied."""
        da = DataAccess("dmx", phase="planning", cfg=SAMPLE_CFG)
        with self.assertRaises(DataAccessDeniedError):
            da.connection("write_only_db")

    def test_connection_with_no_policy_raises_denied(self):
        da = DataAccess("demux", phase="execution", cfg=SAMPLE_CFG)
        with self.assertRaises(DataAccessDeniedError) as ctx:
            da.connection("no_policy_db")
        self.assertIn("no_policy_db", str(ctx.exception))

    def test_unknown_connection_raises_config_error(self):
        da = DataAccess("demux", phase="planning", cfg=SAMPLE_CFG)
        with self.assertRaises(DataAccessConfigError) as ctx:
            da.connection("nonexistent_db")
        self.assertIn("nonexistent_db", str(ctx.exception))

    def test_denied_error_is_subclass_of_data_access_error(self):
        da = DataAccess("demux", phase="execution", cfg=SAMPLE_CFG)
        with self.assertRaises(DataAccessError):
            da.connection("no_policy_db")

    def test_config_error_is_subclass_of_data_access_error(self):
        da = DataAccess("demux", phase="planning", cfg=SAMPLE_CFG)
        with self.assertRaises(DataAccessError):
            da.connection("nonexistent_db")

    # --- Read vs write permission on execution client ---

    @patch(_PATCH_HANDLER)
    def test_write_permission_does_not_grant_read(self, MockHandler):
        """write-only permission → get() raises DataAccessDeniedError."""
        MockHandler.return_value = _mock_handler()
        da = DataAccess("dmx", phase="execution", cfg=SAMPLE_CFG)
        client = da.connection("write_only_db")
        with self.assertRaises(DataAccessDeniedError):
            client.get("some_id")

    # --- Planning phase read requirement (Fix 1) ---

    def test_planning_with_only_write_permission_denied(self):
        """Planning phase with only 'write' permission → DataAccessDeniedError."""
        cfg = {
            "endpoints": {"couchdb": {"backend": "couchdb", "url": "http://h:5984"}},
            "connections": {
                "db": {
                    "endpoint": "couchdb",
                    "resource": {"db": "x"},
                    "data_access": {
                        "realms": {
                            "dmx": {
                                "planning": {"permissions": ["write"]},
                            }
                        }
                    },
                }
            },
        }
        da = DataAccess("dmx", phase="planning", cfg=cfg)
        with self.assertRaises(DataAccessDeniedError) as ctx:
            da.connection("db")
        self.assertIn("read", str(ctx.exception))

    # --- Backend type guard in connection() (Fix 2) ---

    def test_connection_on_non_couchdb_backend_raises_config_error(self):
        """connection() on a non-CouchDB backend raises DataAccessConfigError."""
        da = DataAccess("demux", phase="execution", cfg=SAMPLE_CFG)
        with self.assertRaises(DataAccessConfigError) as ctx:
            da.connection("pg_db")
        self.assertIn("postgres", str(ctx.exception))

    def test_connection_invalid_permission_config_raises_config_error(self):
        """connection() with a typo'd permission string raises DataAccessConfigError."""
        cfg = {
            "endpoints": {"couchdb": {"backend": "couchdb", "url": "http://h:5984"}},
            "connections": {
                "db": {
                    "endpoint": "couchdb",
                    "resource": {"db": "x"},
                    "data_access": {
                        "realms": {"demux": {"execution": {"permissions": ["reed"]}}}
                    },
                }
            },
        }
        da = DataAccess("demux", phase="execution", cfg=cfg)
        with self.assertRaises(DataAccessConfigError) as ctx:
            da.connection("db")
        self.assertIn("reed", str(ctx.exception))

    def test_connection_invalid_phase_config_raises_config_error(self):
        """connection() with a typo'd phase key raises DataAccessConfigError."""
        cfg = {
            "endpoints": {"couchdb": {"backend": "couchdb", "url": "http://h:5984"}},
            "connections": {
                "db": {
                    "endpoint": "couchdb",
                    "resource": {"db": "x"},
                    "data_access": {
                        "realms": {"demux": {"executin": {"permissions": ["read"]}}}
                    },
                }
            },
        }
        da = DataAccess("demux", phase="execution", cfg=cfg)
        with self.assertRaises(DataAccessConfigError) as ctx:
            da.connection("db")
        self.assertIn("executin", str(ctx.exception))

    # --- couchdb() helper ---

    @patch(_PATCH_HANDLER)
    def test_couchdb_helper_returns_planning_client(self, MockHandler):
        MockHandler.return_value = _mock_handler()
        da = DataAccess("demux", phase="planning", cfg=SAMPLE_CFG)
        client = da.couchdb("allowed_db")
        self.assertIsInstance(client, CouchDBPlanningClient)
        """couchdb() on a non-CouchDB endpoint raises DataAccessConfigError."""
        da = DataAccess("demux", phase="execution", cfg=SAMPLE_CFG)
        with self.assertRaises(DataAccessConfigError) as ctx:
            da.couchdb("pg_db")
        self.assertIn("postgres", str(ctx.exception))

    # --- Config loaded once ---

    @patch("yggdrasil.flow.data_access.data_access.load_external_systems_config")
    def test_config_loaded_once_in_init(self, mock_loader):
        mock_loader.return_value = SAMPLE_CFG
        DataAccess(realm_id="demux", phase="planning")
        self.assertEqual(mock_loader.call_count, 1)


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

            async def generate_plan_drafts(self, payload):  # noqa: D102
                ...

        handler = _Handler()
        handler.realm_id = "demux"

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
        # planning phase → DataAccess with phase="planning"
        self.assertEqual(ctx.data._phase, "planning")

    def test_data_access_is_lazy_per_handler_instance(self):
        """_data_access lazy property returns same instance on repeated access."""
        from lib.core_utils.event_types import EventType
        from yggdrasil.flow.base_handler import BaseHandler

        class _Handler(BaseHandler):
            event_type = EventType.PROJECT_CHANGE
            handler_id = "lazy_test"

            def derive_scope(self, doc):
                return {}

            async def generate_plan_drafts(self, payload): ...

        handler = _Handler()
        handler.realm_id = "demux"

        with patch(
            "yggdrasil.flow.data_access.data_access.load_external_systems_config",
            return_value=SAMPLE_CFG,
        ):
            da1 = handler._data_access
            da2 = handler._data_access

        self.assertIs(da1, da2)
        self.assertEqual(da1._phase, "planning")


if __name__ == "__main__":
    unittest.main()
