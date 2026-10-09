"""Importing the realm-facing packages leaves logging and the CouchDB client alone."""

import json
import subprocess
import sys
import unittest

# Runs in a fresh interpreter so modules imported by other tests cannot mask a regression.
_PROBE = """
import json
import logging
import sys

import yggdrasil.flow
import yggdrasil.watchers

print(json.dumps({
    "ibmcloudant": sorted(m for m in sys.modules if m.split(".")[0] == "ibmcloudant"),
    "logging_utils": "yggdrasil.logging_utils" in sys.modules,
    "debug_name": logging.getLevelName(logging.DEBUG),
    "pil_level": logging.getLogger("PIL").level,
    "cloudant_level": logging.getLogger("ibmcloudant.cloudant_v1").level,
}))
"""

_LOGGING_UTILS_PROBE = """
import json
import logging

import yggdrasil.logging_utils

names = [
    "matplotlib", "numba", "h5py", "PIL", "watchdog",
    "ibm-cloud-sdk-core", "ibmcloudant.cloudant_v1", "urllib3.connectionpool",
]
root = logging.getLogger()
print(json.dumps({
    "level_names": [
        logging.getLevelName(level)
        for level in (logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL)
    ],
    "logger_levels": {name: logging.getLogger(name).level for name in names},
    "root_level": root.level,
    "root_handlers": len(root.handlers),
}))
"""


def _run_probe(probe: str) -> dict:
    """Run ``probe`` in a fresh interpreter and return the JSON it prints."""
    result = subprocess.run(
        [sys.executable, "-c", probe],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


class TestImportLightness(unittest.TestCase):
    """``import yggdrasil.flow, yggdrasil.watchers`` has no process-wide side effects."""

    @classmethod
    def setUpClass(cls):
        cls.state = _run_probe(_PROBE)

    def test_couchdb_client_not_imported(self):
        self.assertEqual(self.state["ibmcloudant"], [])

    def test_logging_utils_not_imported(self):
        self.assertFalse(self.state["logging_utils"])

    def test_level_names_untouched(self):
        self.assertEqual(self.state["debug_name"], "DEBUG")

    def test_third_party_logger_levels_untouched(self):
        self.assertEqual(self.state["pil_level"], 0)  # logging.NOTSET
        self.assertEqual(self.state["cloudant_level"], 0)


class TestLoggingUtilsImport(unittest.TestCase):
    """Importing ``yggdrasil.logging_utils`` itself leaves logging untouched.

    Process-wide setup belongs in ``configure_logging()``; this catches it
    creeping back to module scope, which the guard above cannot see because
    ``logging_utils`` is outside the realm-facing import graph.
    """

    @classmethod
    def setUpClass(cls):
        cls.state = _run_probe(_LOGGING_UTILS_PROBE)

    def test_level_names_untouched(self):
        self.assertEqual(
            self.state["level_names"], ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        )

    def test_third_party_logger_levels_untouched(self):
        for name, level in self.state["logger_levels"].items():
            with self.subTest(logger=name):
                self.assertEqual(level, 0)  # logging.NOTSET

    def test_root_logger_untouched(self):
        self.assertEqual(self.state["root_level"], 30)  # logging.WARNING, the default
        self.assertEqual(self.state["root_handlers"], 0)


if __name__ == "__main__":
    unittest.main()
