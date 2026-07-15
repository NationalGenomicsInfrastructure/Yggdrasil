"""Tests for cross-cutting exceptions in lib/core_utils/errors.py."""

import unittest

from lib.core_utils.errors import ExternalSystemUnavailableError


class TestExternalSystemUnavailableError(unittest.TestCase):
    """Tests for ExternalSystemUnavailableError."""

    def test_message_without_hint(self):
        """Message states system and endpoint only when no hint is given."""
        error = ExternalSystemUnavailableError("CouchDB", "http://localhost:5984")
        self.assertEqual(str(error), "Cannot reach CouchDB at http://localhost:5984.")

    def test_message_with_hint(self):
        """Hint is appended to the message."""
        error = ExternalSystemUnavailableError(
            "CouchDB",
            "http://localhost:5984",
            hint="Check VPN connectivity.",
        )
        self.assertEqual(
            str(error),
            "Cannot reach CouchDB at http://localhost:5984. Check VPN connectivity.",
        )

    def test_attributes_stored(self):
        """system, endpoint, and hint are available as attributes."""
        error = ExternalSystemUnavailableError(
            "HPC", "hpc.example.org:22", hint="Check SSH keys."
        )
        self.assertEqual(error.system, "HPC")
        self.assertEqual(error.endpoint, "hpc.example.org:22")
        self.assertEqual(error.hint, "Check SSH keys.")

    def test_hint_defaults_to_none(self):
        """hint is None when not provided."""
        error = ExternalSystemUnavailableError("CouchDB", "http://localhost:5984")
        self.assertIsNone(error.hint)

    def test_is_connection_error_subclass(self):
        """Existing `except ConnectionError` sites keep catching this error."""
        error = ExternalSystemUnavailableError("CouchDB", "http://localhost:5984")
        self.assertIsInstance(error, ConnectionError)
