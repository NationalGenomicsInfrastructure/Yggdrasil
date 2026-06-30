import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from lib.core_utils.daemon_lock import DaemonLock, DaemonLockError


class TestDaemonLock(unittest.TestCase):
    def test_acquire_writes_metadata(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch(
                "lib.core_utils.daemon_lock.appdirs.user_runtime_dir",
                return_value=tmpdir,
                create=True,
            ):
                with DaemonLock.acquire(
                    dev_mode=True,
                    config_path="/tmp/dev_main.json",
                ) as lock:
                    metadata = json.loads(lock.lock_path.read_text())

                self.assertEqual(lock.lock_path, Path(tmpdir) / "daemon.lock")
                self.assertTrue(metadata["dev_mode"])
                self.assertEqual(metadata["config_path"], "/tmp/dev_main.json")
                self.assertIn("pid", metadata)
                self.assertIn("hostname", metadata)

    def test_second_acquire_fails_with_existing_metadata(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch(
                "lib.core_utils.daemon_lock.appdirs.user_runtime_dir",
                return_value=tmpdir,
                create=True,
            ):
                with DaemonLock.acquire(dev_mode=False, config_path=None):
                    with self.assertRaises(DaemonLockError) as context:
                        DaemonLock.acquire(dev_mode=False, config_path=None)

                self.assertEqual(
                    context.exception.lock_path, Path(tmpdir) / "daemon.lock"
                )
                self.assertIn("pid", context.exception.existing_metadata)

    def test_dev_and_normal_use_same_local_lock_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch(
                "lib.core_utils.daemon_lock.appdirs.user_runtime_dir",
                return_value=tmpdir,
                create=True,
            ):
                with DaemonLock.acquire(dev_mode=False, config_path=None) as normal:
                    normal_path = normal.lock_path

                with DaemonLock.acquire(dev_mode=True, config_path=None) as dev:
                    dev_path = dev.lock_path

                self.assertEqual(normal_path, dev_path)
