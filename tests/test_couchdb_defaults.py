"""Tests for CouchDB defaults resolution helpers."""

import unittest
from unittest.mock import patch

from lib.couchdb.couchdb_defaults import (
    DEFAULT_ENDPOINT,
    DEFAULT_PASS_ENV,
    DEFAULT_USER_ENV,
    resolve_couchdb_params,
)


class TestCouchDBDefaults(unittest.TestCase):
    """Tests for resolve_couchdb_params."""

    def test_resolve_from_config(self):
        """Resolve parameters from endpoint config."""
        cfg = {
            "external_systems": {
                "endpoints": {
                    DEFAULT_ENDPOINT: {
                        "url": "http://couchdb.local:5984",
                        "auth": {
                            "user_env": "COUCH_USER",
                            "pass_env": "COUCH_PASS",
                        },
                    }
                }
            }
        }

        with patch(
            "lib.couchdb.couchdb_defaults.ConfigLoader.load_config"
        ) as mock_load:
            with patch("lib.couchdb.couchdb_defaults.Ygg.normalize_url") as mock_norm:
                mock_load.return_value = cfg
                mock_norm.return_value = "http://couchdb.local:5984"

                params = resolve_couchdb_params()

        self.assertEqual(params.url, "http://couchdb.local:5984")
        self.assertEqual(params.user_env, "COUCH_USER")
        self.assertEqual(params.pass_env, "COUCH_PASS")

    def test_resolve_with_overrides(self):
        """Explicit args override config values."""
        cfg = {
            "external_systems": {
                "endpoints": {
                    DEFAULT_ENDPOINT: {
                        "url": "http://ignored:5984",
                        "auth": {
                            "user_env": "IGNORED_USER",
                            "pass_env": "IGNORED_PASS",
                        },
                    }
                }
            }
        }

        with patch(
            "lib.couchdb.couchdb_defaults.ConfigLoader.load_config"
        ) as mock_load:
            with patch("lib.couchdb.couchdb_defaults.Ygg.normalize_url") as mock_norm:
                mock_load.return_value = cfg
                mock_norm.return_value = "https://override:6984"

                params = resolve_couchdb_params(
                    url="https://override:6984",
                    user_env="OVERRIDE_USER",
                    pass_env="OVERRIDE_PASS",
                )

        self.assertEqual(params.url, "https://override:6984")
        self.assertEqual(params.user_env, "OVERRIDE_USER")
        self.assertEqual(params.pass_env, "OVERRIDE_PASS")

    def test_missing_endpoint_config_raises(self):
        """Missing endpoint config raises when no overrides provided."""
        with patch(
            "lib.couchdb.couchdb_defaults.ConfigLoader.load_config"
        ) as mock_load:
            mock_load.return_value = {}
            with self.assertRaises(RuntimeError) as ctx:
                resolve_couchdb_params()

        self.assertIn("Missing CouchDB endpoint config", str(ctx.exception))

    def test_missing_url_raises(self):
        """Missing endpoint url raises even if auth is present."""
        cfg = {
            "external_systems": {
                "endpoints": {
                    DEFAULT_ENDPOINT: {
                        "auth": {
                            "user_env": DEFAULT_USER_ENV,
                            "pass_env": DEFAULT_PASS_ENV,
                        }
                    }
                }
            }
        }

        with patch(
            "lib.couchdb.couchdb_defaults.ConfigLoader.load_config"
        ) as mock_load:
            mock_load.return_value = cfg
            with self.assertRaises(RuntimeError) as ctx:
                resolve_couchdb_params()

        self.assertIn("Missing url for endpoint", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
