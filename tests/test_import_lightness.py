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


class TestImportLightness(unittest.TestCase):
    """``import yggdrasil.flow, yggdrasil.watchers`` has no process-wide side effects."""

    @classmethod
    def setUpClass(cls):
        result = subprocess.run(
            [sys.executable, "-c", _PROBE],
            capture_output=True,
            text=True,
            check=True,
        )
        cls.state = json.loads(result.stdout)

    def test_couchdb_client_not_imported(self):
        self.assertEqual(self.state["ibmcloudant"], [])

    def test_logging_utils_not_imported(self):
        self.assertFalse(self.state["logging_utils"])

    def test_level_names_untouched(self):
        self.assertEqual(self.state["debug_name"], "DEBUG")

    def test_third_party_logger_levels_untouched(self):
        self.assertEqual(self.state["pil_level"], 0)  # logging.NOTSET
        self.assertEqual(self.state["cloudant_level"], 0)


if __name__ == "__main__":
    unittest.main()
