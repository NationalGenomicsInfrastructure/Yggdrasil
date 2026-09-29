import json
import os
import stat
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

                self.assertEqual(lock.lock_path, Path(tmpdir) / "daemon-dev.lock")
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
                self.assertIn("prod mode", str(context.exception))

    def test_second_dev_acquire_fails_with_dev_mode_in_message(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch(
                "lib.core_utils.daemon_lock.appdirs.user_runtime_dir",
                return_value=tmpdir,
                create=True,
            ):
                with DaemonLock.acquire(dev_mode=True, config_path=None):
                    with self.assertRaises(DaemonLockError) as context:
                        DaemonLock.acquire(dev_mode=True, config_path=None)

                self.assertEqual(
                    context.exception.lock_path, Path(tmpdir) / "daemon-dev.lock"
                )
                self.assertIn("dev mode", str(context.exception))

    def test_dev_and_prod_use_distinct_lock_paths(self):
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

                self.assertEqual(normal_path, Path(tmpdir) / "daemon.lock")
                self.assertEqual(dev_path, Path(tmpdir) / "daemon-dev.lock")
                self.assertNotEqual(normal_path, dev_path)

    def test_prod_and_dev_locks_held_concurrently(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch(
                "lib.core_utils.daemon_lock.appdirs.user_runtime_dir",
                return_value=tmpdir,
                create=True,
            ):
                # Both modes acquire and hold simultaneously — no error.
                with DaemonLock.acquire(dev_mode=False, config_path=None):
                    with DaemonLock.acquire(dev_mode=True, config_path=None) as dev:
                        self.assertEqual(
                            dev.lock_path, Path(tmpdir) / "daemon-dev.lock"
                        )

    def test_fallback_runtime_dir_is_created_with_restrictive_permissions(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fallback_dir = Path(tmpdir) / "yggdrasil-lock"
            with (
                patch(
                    "lib.core_utils.daemon_lock.appdirs.user_runtime_dir",
                    return_value=None,
                    create=True,
                ),
                patch.object(
                    DaemonLock,
                    "_fallback_runtime_dir",
                    return_value=fallback_dir,
                ),
            ):
                with DaemonLock.acquire(dev_mode=False, config_path=None):
                    pass

            mode = stat.S_IMODE(fallback_dir.stat().st_mode)
            self.assertEqual(mode, 0o700)

    def test_existing_fallback_runtime_dir_is_chmodded_to_restrictive_permissions(
        self,
    ):
        with tempfile.TemporaryDirectory() as tmpdir:
            fallback_dir = Path(tmpdir) / "yggdrasil-lock"
            fallback_dir.mkdir(mode=0o755)
            fallback_dir.chmod(0o755)

            with patch.object(
                DaemonLock,
                "_fallback_runtime_dir",
                return_value=fallback_dir,
            ):
                DaemonLock._prepare_runtime_dir(fallback_dir)

            mode = stat.S_IMODE(fallback_dir.stat().st_mode)
            self.assertEqual(mode, 0o700)

    def test_appdirs_runtime_dir_permissions_are_not_mutated(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_dir = Path(tmpdir)
            runtime_dir.chmod(0o755)
            with patch(
                "lib.core_utils.daemon_lock.appdirs.user_runtime_dir",
                return_value=runtime_dir,
                create=True,
            ):
                with DaemonLock.acquire(dev_mode=False, config_path=None):
                    pass

            mode = stat.S_IMODE(runtime_dir.stat().st_mode)
            self.assertEqual(mode, 0o755)

    def test_existing_fallback_runtime_dir_owned_by_other_user_raises(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fallback_dir = Path(tmpdir) / "yggdrasil-lock"
            fallback_dir.mkdir()
            other_uid = os.getuid() + 1

            with (
                patch.object(
                    DaemonLock,
                    "_fallback_runtime_dir",
                    return_value=fallback_dir,
                ),
                patch("lib.core_utils.daemon_lock.os.getuid", return_value=other_uid),
            ):
                with self.assertRaises(PermissionError):
                    DaemonLock._prepare_runtime_dir(fallback_dir)
