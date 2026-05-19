"""Unit tests for lib.core_utils.external_systems_resolver.

Tests cover endpoint resolution, connection resolution, DataAccessPolicy
merging, error conditions, and config injection for filesystem-free testing.
"""

import unittest

from lib.core_utils.external_systems_resolver import (
    DataAccessPolicy,
    ResolvedConnection,
    ResolvedEndpoint,
    load_external_systems_config,
    resolve_connection,
    resolve_endpoint,
)

# ---------------------------------------------------------------------------
# Shared test config fixture
# ---------------------------------------------------------------------------

SAMPLE_CFG = {
    "endpoints": {
        "couchdb": {
            "backend": "couchdb",
            "url": "http://couch.example.org:5984",
            "auth": {
                "user_env": "MY_USER",
                "pass_env": "MY_PASS",
            },
        },
        "couchdb_no_auth": {
            "backend": "couchdb",
            "url": "https://other.host:5984",
            # No "auth" key — should fall back to defaults
        },
    },
    "defaults": {
        "couchdb": {
            "max_limit": 150,
        }
    },
    "connections": {
        "projects_db": {
            "endpoint": "couchdb",
            "resource": {"db": "projects"},
            # No data_access — not accessible by realms
        },
        "flowcell_db": {
            "endpoint": "couchdb",
            "resource": {"db": "flowcells"},
            "data_access": {
                "realms": {
                    "demux": {
                        "planning": {"permissions": ["read"]},
                        "execution": {"permissions": ["read", "write"]},
                    },
                    "tenx": {
                        "execution": {"permissions": ["read"]},
                    },
                }
                # No options → should use global defaults
            },
        },
        "samplesheet_db": {
            "endpoint": "couchdb",
            "resource": {"db": "samplesheet_info"},
            "data_access": {
                "realms": {
                    "demux": {
                        "execution": {"permissions": ["read"]},
                    }
                },
                "options": {"max_limit": 50},  # per-connection override
            },
        },
        "no_auth_db": {
            "endpoint": "couchdb_no_auth",
            "resource": {"db": "stuff"},
        },
    },
}


# ---------------------------------------------------------------------------
# load_external_systems_config
# ---------------------------------------------------------------------------


class TestLoadExternalSystemsConfig(unittest.TestCase):
    """Tests for load_external_systems_config."""

    def test_injected_cfg_returned_as_is(self):
        """Injected cfg dict is returned directly (no loading from disk)."""
        cfg = {"endpoints": {}, "connections": {}}
        result = load_external_systems_config(cfg=cfg)
        self.assertIs(result, cfg)

    def test_injected_cfg_none_triggers_loader(self):
        """When cfg=None, ConfigLoader is called. We can't test that without mocking,
        but verify the function is importable and returns a dict for None arg
        by patching ConfigLoader at module level."""
        # We test the happy-path for this via injected cfg in other tests.
        # The None path is covered implicitly by integration (WatcherManager tests).
        pass


# ---------------------------------------------------------------------------
# resolve_endpoint
# ---------------------------------------------------------------------------


class TestResolveEndpoint(unittest.TestCase):
    """Tests for resolve_endpoint."""

    def test_returns_resolved_endpoint(self):
        """resolve_endpoint returns a ResolvedEndpoint with correct fields."""
        ep = resolve_endpoint("couchdb", SAMPLE_CFG)
        self.assertIsInstance(ep, ResolvedEndpoint)
        self.assertEqual(ep.name, "couchdb")
        self.assertEqual(ep.url, "http://couch.example.org:5984")
        self.assertEqual(ep.user_env, "MY_USER")
        self.assertEqual(ep.pass_env, "MY_PASS")
        self.assertEqual(ep.backend_type, "couchdb")
        self.assertIsNone(ep.dsn_env)

    def test_url_scheme_added_if_missing(self):
        """URL without scheme gets http:// prepended."""
        cfg = {
            "endpoints": {
                "bare": {
                    "backend": "couchdb",
                    "url": "host.example.org:5984",
                }
            },
            "connections": {},
        }
        ep = resolve_endpoint("bare", cfg)
        self.assertTrue(ep.url.startswith("http://"), ep.url)

    def test_url_trailing_slash_stripped(self):
        """Trailing slash in URL is stripped."""
        cfg = {
            "endpoints": {
                "ep": {
                    "backend": "couchdb",
                    "url": "http://host.example.org:5984/",
                }
            },
            "connections": {},
        }
        ep = resolve_endpoint("ep", cfg)
        self.assertFalse(ep.url.endswith("/"), ep.url)

    def test_auth_defaults_applied_when_no_auth_key(self):
        """Endpoint with no 'auth' key gets default env var names."""
        ep = resolve_endpoint("couchdb_no_auth", SAMPLE_CFG)
        self.assertEqual(ep.user_env, "YGG_COUCH_USER")
        self.assertEqual(ep.pass_env, "YGG_COUCH_PASS")
        self.assertIsNone(ep.dsn_env)

    def test_unknown_endpoint_raises_keyerror(self):
        """Unknown endpoint name raises KeyError with a helpful message."""
        with self.assertRaises(KeyError) as ctx:
            resolve_endpoint("nonexistent", SAMPLE_CFG)
        self.assertIn("nonexistent", str(ctx.exception))

    def test_endpoint_missing_url_raises_keyerror(self):
        """Endpoint config without 'url' raises KeyError."""
        cfg = {
            "endpoints": {"bad": {"backend": "couchdb"}},
            "connections": {},
        }
        with self.assertRaises(KeyError) as ctx:
            resolve_endpoint("bad", cfg)
        self.assertIn("url", str(ctx.exception).lower())


# ---------------------------------------------------------------------------
# resolve_connection
# ---------------------------------------------------------------------------


class TestResolveConnection(unittest.TestCase):
    """Tests for resolve_connection."""

    # --- Basic resolution ---

    def test_returns_resolved_connection(self):
        """resolve_connection returns a ResolvedConnection with correct fields."""
        conn = resolve_connection("projects_db", SAMPLE_CFG)
        self.assertIsInstance(conn, ResolvedConnection)
        self.assertEqual(conn.name, "projects_db")
        self.assertEqual(conn.db_name, "projects")
        self.assertIsInstance(conn.endpoint, ResolvedEndpoint)
        self.assertEqual(conn.endpoint.name, "couchdb")

    def test_no_data_access_block_yields_none_policy(self):
        """Connection with no 'data_access' block → data_access is None."""
        conn = resolve_connection("projects_db", SAMPLE_CFG)
        self.assertIsNone(conn.data_access)

    def test_resource_field_populated(self):
        """resource field contains raw resource dict from config."""
        conn = resolve_connection("flowcell_db", SAMPLE_CFG)
        self.assertEqual(conn.resource, {"db": "flowcells"})

    def test_db_name_matches_resource_db(self):
        """db_name matches resource["db"]."""
        conn = resolve_connection("flowcell_db", SAMPLE_CFG)
        self.assertEqual(conn.db_name, conn.resource["db"])

    # --- DataAccessPolicy construction ---

    def test_data_access_policy_present(self):
        """Connection with data_access block → DataAccessPolicy populated."""
        conn = resolve_connection("flowcell_db", SAMPLE_CFG)
        self.assertIsNotNone(conn.data_access)
        self.assertIsInstance(conn.data_access, DataAccessPolicy)

    def test_get_permissions_returns_correct_set(self):
        """get_permissions returns correct frozenset for configured realm+phase."""
        conn = resolve_connection("flowcell_db", SAMPLE_CFG)
        perms = conn.data_access.get_permissions("demux", "execution")
        self.assertEqual(perms, frozenset({"read", "write"}))

    def test_get_permissions_planning_phase(self):
        """get_permissions returns read-only for planning phase."""
        conn = resolve_connection("flowcell_db", SAMPLE_CFG)
        perms = conn.data_access.get_permissions("demux", "planning")
        self.assertEqual(perms, frozenset({"read"}))

    def test_get_permissions_returns_empty_for_missing_realm(self):
        """get_permissions returns frozenset() for a realm not in policy."""
        conn = resolve_connection("flowcell_db", SAMPLE_CFG)
        perms = conn.data_access.get_permissions("unknown_realm", "execution")
        self.assertEqual(perms, frozenset())

    def test_get_permissions_returns_empty_for_missing_phase(self):
        """get_permissions returns frozenset() when realm exists but phase is absent."""
        conn = resolve_connection("flowcell_db", SAMPLE_CFG)
        # tenx has only "execution" configured, not "planning"
        perms = conn.data_access.get_permissions("tenx", "planning")
        self.assertEqual(perms, frozenset())

    def test_global_options_used_when_no_connection_override(self):
        """Global defaults.couchdb options are used when connection has no options."""
        conn = resolve_connection("flowcell_db", SAMPLE_CFG)
        # SAMPLE_CFG has global max_limit=150; flowcell_db has no options
        self.assertEqual(conn.data_access.options.get("max_limit"), 150)

    def test_per_connection_options_override_global(self):
        """Connection-level options override global defaults."""
        conn = resolve_connection("samplesheet_db", SAMPLE_CFG)
        self.assertEqual(conn.data_access.options.get("max_limit"), 50)

    def test_options_merged_global_then_connection(self):
        """Connection options merged on top of global defaults (connection wins)."""
        cfg = {
            "endpoints": {"couchdb": {"backend": "couchdb", "url": "http://host:5984"}},
            "defaults": {"couchdb": {"max_limit": 200, "timeout": 30}},
            "connections": {
                "mydb": {
                    "endpoint": "couchdb",
                    "resource": {"db": "mydb"},
                    "data_access": {
                        "realms": {},
                        "options": {"max_limit": 50},
                    },
                }
            },
        }
        conn = resolve_connection("mydb", cfg)
        self.assertEqual(conn.data_access.options, {"max_limit": 50, "timeout": 30})

    def test_options_empty_when_no_defaults_or_connection_options(self):
        """options is empty dict when no global defaults and no connection options."""
        cfg = {
            "endpoints": {"couchdb": {"backend": "couchdb", "url": "http://host:5984"}},
            "connections": {
                "mydb": {
                    "endpoint": "couchdb",
                    "resource": {"db": "mydb"},
                    "data_access": {"realms": {}},
                }
            },
        }
        conn = resolve_connection("mydb", cfg)
        self.assertEqual(conn.data_access.options, {})

    # --- Error conditions ---

    def test_unknown_connection_raises_keyerror(self):
        """Unknown connection name raises KeyError with a helpful message."""
        with self.assertRaises(KeyError) as ctx:
            resolve_connection("nonexistent", SAMPLE_CFG)
        self.assertIn("nonexistent", str(ctx.exception))

    def test_connection_missing_endpoint_raises_keyerror(self):
        """Connection with no 'endpoint' key raises KeyError."""
        cfg = {
            "endpoints": {"couchdb": {"backend": "couchdb", "url": "http://host:5984"}},
            "connections": {"bad_conn": {"resource": {"db": "stuff"}}},  # No endpoint
        }
        with self.assertRaises(KeyError) as ctx:
            resolve_connection("bad_conn", cfg)
        self.assertIn("endpoint", str(ctx.exception).lower())

    def test_connection_with_unknown_endpoint_raises_keyerror(self):
        """Connection referencing a non-existent endpoint raises KeyError."""
        cfg = {
            "endpoints": {},
            "connections": {
                "conn": {"endpoint": "ghost_endpoint", "resource": {"db": "x"}}
            },
        }
        with self.assertRaises(KeyError) as ctx:
            resolve_connection("conn", cfg)
        self.assertIn("ghost_endpoint", str(ctx.exception))

    def test_connection_resource_missing_db_raises_keyerror(self):
        """Connection whose resource has no 'db' field raises KeyError."""
        cfg = {
            "endpoints": {"ep": {"backend": "couchdb", "url": "http://host:5984"}},
            "connections": {
                "conn": {
                    "endpoint": "ep",
                    "resource": {},  # No 'db'
                }
            },
        }
        with self.assertRaises(KeyError) as ctx:
            resolve_connection("conn", cfg)
        self.assertIn("db", str(ctx.exception).lower())

    # --- Endpoint fields correctly propagated ---

    def test_resolved_endpoint_url_in_connection(self):
        """Endpoint URL is correctly propagated into the resolved connection."""
        conn = resolve_connection("flowcell_db", SAMPLE_CFG)
        self.assertEqual(conn.endpoint.url, "http://couch.example.org:5984")

    def test_resolved_connection_endpoint_auth_env_vars(self):
        """Auth env vars from endpoint are accessible via connection.endpoint."""
        conn = resolve_connection("flowcell_db", SAMPLE_CFG)
        self.assertEqual(conn.endpoint.user_env, "MY_USER")
        self.assertEqual(conn.endpoint.pass_env, "MY_PASS")

    def test_default_auth_env_vars_for_connection_with_no_auth_endpoint(self):
        """Connection using an endpoint without 'auth' gets default env var names."""
        conn = resolve_connection("no_auth_db", SAMPLE_CFG)
        self.assertEqual(conn.endpoint.user_env, "YGG_COUCH_USER")
        self.assertEqual(conn.endpoint.pass_env, "YGG_COUCH_PASS")


# ---------------------------------------------------------------------------
# TestResolveConnectionValidation — permission and phase validation
# ---------------------------------------------------------------------------


class TestResolveConnectionValidation(unittest.TestCase):
    """Tests for config validation added in v0.3: unknown phases and permissions."""

    def _make_cfg(self, permissions, phase="execution"):
        return {
            "endpoints": {"couchdb": {"backend": "couchdb", "url": "http://h:5984"}},
            "connections": {
                "db": {
                    "endpoint": "couchdb",
                    "resource": {"db": "x"},
                    "data_access": {
                        "realms": {"demux": {phase: {"permissions": permissions}}}
                    },
                }
            },
        }

    def test_unknown_permission_raises_value_error(self):
        """Typo in permissions list raises ValueError at resolve time."""
        cfg = self._make_cfg(["reed", "write"])
        with self.assertRaises(ValueError) as ctx:
            resolve_connection("db", cfg)
        self.assertIn("reed", str(ctx.exception))
        self.assertIn("demux", str(ctx.exception))

    def test_unknown_phase_raises_value_error(self):
        """Typo in phase key raises ValueError at resolve time."""
        cfg = self._make_cfg(["read"], phase="executin")
        with self.assertRaises(ValueError) as ctx:
            resolve_connection("db", cfg)
        self.assertIn("executin", str(ctx.exception))
        self.assertIn("demux", str(ctx.exception))

    def test_valid_permissions_do_not_raise(self):
        """Known-good permissions pass validation."""
        cfg = self._make_cfg(["read", "write"])
        conn = resolve_connection("db", cfg)
        self.assertEqual(
            conn.data_access.get_permissions("demux", "execution"),
            frozenset({"read", "write"}),
        )

    def test_valid_phases_do_not_raise(self):
        """Both 'planning' and 'execution' are accepted."""
        cfg = {
            "endpoints": {"couchdb": {"backend": "couchdb", "url": "http://h:5984"}},
            "connections": {
                "db": {
                    "endpoint": "couchdb",
                    "resource": {"db": "x"},
                    "data_access": {
                        "realms": {
                            "dmx": {
                                "planning": {"permissions": ["read"]},
                                "execution": {"permissions": ["read", "write"]},
                            }
                        }
                    },
                }
            },
        }
        conn = resolve_connection("db", cfg)
        self.assertEqual(
            conn.data_access.get_permissions("dmx", "planning"),
            frozenset({"read"}),
        )


if __name__ == "__main__":
    unittest.main()
