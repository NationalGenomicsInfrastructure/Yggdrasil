"""
Unit tests for yggdrasil.core.realm.discovery module.

Tests discover_realms() with mocked entry points.
"""

import logging
import unittest
from typing import Any, ClassVar
from unittest.mock import MagicMock, patch

from lib.core_utils.event_types import EventType
from yggdrasil.core.realm import RealmDescriptor, discover_realms
from yggdrasil.flow.base_handler import BaseHandler
from yggdrasil.flow.planner.api import PlanDraft


class _FakeHandler(BaseHandler):
    """Minimal handler for discovery tests."""

    event_type: ClassVar[EventType] = EventType.PROJECT_CHANGE
    handler_id: ClassVar[str] = "fake_handler"

    def derive_scope(self, doc: dict[str, Any]) -> dict[str, Any]:
        return {"kind": "project", "id": doc.get("project_id", "unknown")}

    async def generate_plan_draft(self, payload: dict[str, Any]) -> PlanDraft:
        raise NotImplementedError

    def __call__(self, payload: dict[str, Any]) -> None:
        pass


def _make_entry_point(name: str, value: str, provider_fn):
    """Create a mock entry point."""
    ep = MagicMock()
    ep.name = name
    ep.value = value
    ep.load.return_value = provider_fn
    return ep


class TestDiscoverRealms(unittest.TestCase):
    """Tests for discover_realms()."""

    def _make_descriptor(self, realm_id="test_realm"):
        return RealmDescriptor(
            realm_id=realm_id,
            handler_classes=[_FakeHandler],
        )

    @patch("yggdrasil.core.realm.discovery.importlib.metadata.entry_points")
    def test_no_entry_points(self, mock_eps):
        """Returns empty list when no ygg.realm entry points exist."""
        mock_eps.return_value = []
        result = discover_realms()
        self.assertEqual(result, [])

    @patch("yggdrasil.core.realm.discovery.importlib.metadata.entry_points")
    def test_single_realm(self, mock_eps):
        """Discovers a single realm from entry point."""
        desc = self._make_descriptor("my_realm")

        def provider():
            return desc

        ep = _make_entry_point("my_realm", "my_pkg.realm:provide", provider)
        mock_eps.return_value = [ep]

        result = discover_realms()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].realm_id, "my_realm")

    @patch("yggdrasil.core.realm.discovery.importlib.metadata.entry_points")
    def test_multiple_realms(self, mock_eps):
        """Discovers multiple realms."""
        desc1 = self._make_descriptor("realm_a")
        desc2 = self._make_descriptor("realm_b")

        ep1 = _make_entry_point("realm_a", "a:provide", lambda: desc1)
        ep2 = _make_entry_point("realm_b", "b:provide", lambda: desc2)
        mock_eps.return_value = [ep1, ep2]

        result = discover_realms()
        self.assertEqual(len(result), 2)
        ids = {r.realm_id for r in result}
        self.assertEqual(ids, {"realm_a", "realm_b"})

    @patch("yggdrasil.core.realm.discovery.importlib.metadata.entry_points")
    def test_deduplicates_entry_points(self, mock_eps):
        """Deduplicates entry points with same (name, value)."""
        desc = self._make_descriptor("dedup_realm")
        ep1 = _make_entry_point("same", "same:provide", lambda: desc)
        ep2 = _make_entry_point("same", "same:provide", lambda: desc)
        mock_eps.return_value = [ep1, ep2]

        result = discover_realms()
        self.assertEqual(len(result), 1)

    @patch("yggdrasil.core.realm.discovery.importlib.metadata.entry_points")
    def test_load_failure_skipped(self, mock_eps):
        """Entry points that fail to load are skipped."""
        ep = MagicMock()
        ep.name = "broken"
        ep.value = "broken:provide"
        ep.load.side_effect = ImportError("no module")
        mock_eps.return_value = [ep]

        logger = logging.getLogger("test_discovery")
        result = discover_realms(logger=logger)
        self.assertEqual(result, [])

    @patch("yggdrasil.core.realm.discovery.importlib.metadata.entry_points")
    def test_provider_returns_none_skipped(self, mock_eps):
        """Provider returning None is skipped."""
        ep = _make_entry_point("skip", "skip:provide", lambda: None)
        mock_eps.return_value = [ep]

        result = discover_realms()
        self.assertEqual(result, [])

    @patch("yggdrasil.core.realm.discovery.importlib.metadata.entry_points")
    def test_empty_realm_id_skipped(self, mock_eps):
        """Provider returning descriptor with empty realm_id is skipped."""
        desc = RealmDescriptor(
            realm_id="",
            handler_classes=[_FakeHandler],
        )
        ep = _make_entry_point("empty", "empty:provide", lambda: desc)
        mock_eps.return_value = [ep]

        result = discover_realms()
        self.assertEqual(result, [])

    @patch("yggdrasil.core.realm.discovery.importlib.metadata.entry_points")
    def test_provider_exception_skipped(self, mock_eps):
        """Provider that raises exception is skipped."""

        def bad_provider():
            raise RuntimeError("boom")

        ep = _make_entry_point("bad", "bad:provide", bad_provider)
        mock_eps.return_value = [ep]

        result = discover_realms()
        self.assertEqual(result, [])


if __name__ == "__main__":
    unittest.main()
