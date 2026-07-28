"""Unit tests for mode-aware runtime path resolution."""

import os
import unittest
from pathlib import Path
from unittest.mock import patch

from lib.core_utils.runtime_paths import (
    default_event_spool,
    default_work_root,
    resolve_event_spool,
    resolve_work_root,
)
from lib.core_utils.ygg_session import YggSession


class RuntimePathsTestBase(unittest.TestCase):
    """Controls YggSession mode and the relevant environment variables."""

    def setUp(self):
        self._saved_session_state = (
            YggSession._YggSession__dev_mode,
            YggSession._YggSession__dev_already_set,
        )
        self._set_dev(False)
        self._env_patcher = patch.dict(os.environ, {}, clear=False)
        self._env_patcher.start()
        os.environ.pop("YGG_WORK_ROOT", None)
        os.environ.pop("YGG_EVENT_SPOOL", None)

    def tearDown(self):
        self._env_patcher.stop()
        (
            YggSession._YggSession__dev_mode,
            YggSession._YggSession__dev_already_set,
        ) = self._saved_session_state

    @staticmethod
    def _set_dev(dev: bool) -> None:
        setattr(YggSession, "_YggSession__dev_mode", dev)
        setattr(YggSession, "_YggSession__dev_already_set", dev)


class TestModeDefaults(RuntimePathsTestBase):
    def test_prod_defaults_unchanged(self):
        self.assertEqual(default_work_root(), Path("/tmp/ygg_work"))
        self.assertEqual(default_event_spool(), Path("/tmp/ygg_events"))

    def test_dev_defaults_are_suffixed(self):
        self._set_dev(True)
        self.assertEqual(default_work_root(), Path("/tmp/ygg_work_dev"))
        self.assertEqual(default_event_spool(), Path("/tmp/ygg_events_dev"))


class TestResolveWorkRoot(RuntimePathsTestBase):
    def test_config_beats_env(self):
        os.environ["YGG_WORK_ROOT"] = "/env/work"
        resolved = resolve_work_root({"work_root": "/config/work"})
        self.assertEqual(resolved, Path("/config/work"))

    def test_env_beats_mode_default(self):
        os.environ["YGG_WORK_ROOT"] = "/env/work"
        self._set_dev(True)
        self.assertEqual(resolve_work_root(), Path("/env/work"))

    def test_mode_default_when_unset(self):
        self.assertEqual(resolve_work_root({}), Path("/tmp/ygg_work"))
        self._set_dev(True)
        self.assertEqual(resolve_work_root({}), Path("/tmp/ygg_work_dev"))

    def test_falsy_config_value_ignored(self):
        os.environ["YGG_WORK_ROOT"] = "/env/work"
        self.assertEqual(resolve_work_root({"work_root": ""}), Path("/env/work"))


class TestResolveEventSpool(RuntimePathsTestBase):
    def test_env_beats_mode_default(self):
        os.environ["YGG_EVENT_SPOOL"] = "/env/spool"
        self._set_dev(True)
        self.assertEqual(resolve_event_spool(), Path("/env/spool"))

    def test_mode_default_when_unset(self):
        self.assertEqual(resolve_event_spool(), Path("/tmp/ygg_events"))
        self._set_dev(True)
        self.assertEqual(resolve_event_spool(), Path("/tmp/ygg_events_dev"))


if __name__ == "__main__":
    unittest.main()
