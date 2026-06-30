"""Local process lock for Yggdrasil daemon invocations.

The lock prevents two daemon processes from running in the same local runtime.
It is implemented with an advisory ``fcntl.flock`` held on an open lock file
for the lifetime of the daemon process.
"""

from __future__ import annotations

import errno
import fcntl
import getpass
import json
import os
import socket
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import appdirs


class DaemonLockError(RuntimeError):
    """
    Raised when the local daemon runtime lock is already held.

    Attributes:
        lock_path: Path to the lock file that could not be acquired.
        existing_metadata: Best-effort metadata read from the lock file. This
            is useful for logs because it can identify the process currently
            holding the lock.
    """

    def __init__(self, lock_path: Path, existing_metadata: dict[str, Any] | None):
        """
        Initialize the lock acquisition error.

        Args:
            lock_path: Path to the lock file that is already locked.
            existing_metadata: Metadata from the existing lock file, if it
                could be parsed as JSON.
        """
        self.lock_path = lock_path
        self.existing_metadata = existing_metadata or {}
        super().__init__(f"Yggdrasil daemon lock is already held: {lock_path}")


@dataclass
class DaemonLock:
    """
    Represents a held local daemon lock.

    A ``DaemonLock`` instance owns an open file handle. Keeping that handle open
    keeps the advisory lock active; calling :meth:`release` or leaving the
    context manager closes the handle and releases the lock.

    Attributes:
        lock_path: Path to the lock file.
        _file: Open file object that owns the advisory lock.
    """

    lock_path: Path
    _file: Any

    LOCK_FILENAME = "daemon.lock"

    @classmethod
    def acquire(
        cls,
        *,
        dev_mode: bool,
        config_path: str | os.PathLike[str] | None = None,
    ) -> DaemonLock:
        """
        Acquire the local daemon lock.

        Creates the runtime directory if needed, opens the lock file, and takes
        a non-blocking exclusive ``flock``. On success, writes metadata about
        the current process to the lock file and returns a held ``DaemonLock``.

        Args:
            dev_mode: Whether the daemon was started with ``--dev``. Stored in
                lock metadata for diagnostics only.
            config_path: Resolved configuration path, if known. Stored in lock
                metadata for diagnostics only.

        Returns:
            A held ``DaemonLock``. The caller must keep it alive for the daemon
            lifetime, usually with a ``with`` block.

        Raises:
            DaemonLockError: If another process already holds the local daemon
                lock.
            OSError: If the lock file cannot be opened, locked, written, or
                synced for reasons other than an already-held lock.
        """
        lock_dir = cls._runtime_dir()
        lock_dir.mkdir(parents=True, exist_ok=True)
        lock_path = lock_dir / cls.LOCK_FILENAME

        lock_file = lock_path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno not in (errno.EACCES, errno.EAGAIN):
                lock_file.close()
                raise
            existing_metadata = cls._read_metadata(lock_file)
            lock_file.close()
            raise DaemonLockError(lock_path, existing_metadata) from exc

        metadata = cls._metadata(dev_mode=dev_mode, config_path=config_path)
        lock_file.seek(0)
        lock_file.truncate()
        json.dump(metadata, lock_file, indent=2, sort_keys=True)
        lock_file.write("\n")
        lock_file.flush()
        os.fsync(lock_file.fileno())

        return cls(lock_path=lock_path, _file=lock_file)

    @staticmethod
    def _runtime_dir() -> Path:
        """
        Resolve the runtime directory used for daemon lock files.

        Uses ``appdirs.user_runtime_dir("yggdrasil")`` when available. Some
        supported ``appdirs`` versions do not expose that helper, so the method
        falls back to a per-user directory under ``/tmp``.

        Returns:
            Directory path where daemon lock files should be stored.
        """
        runtime_dir_func = getattr(appdirs, "user_runtime_dir", None)
        runtime_dir = cast(
            str | os.PathLike[str] | None,
            runtime_dir_func("yggdrasil") if callable(runtime_dir_func) else None,
        )
        if runtime_dir:
            return Path(runtime_dir)
        return Path("/tmp") / f"yggdrasil-{os.getuid()}"

    @staticmethod
    def _metadata(
        *,
        dev_mode: bool,
        config_path: str | os.PathLike[str] | None,
    ) -> dict[str, Any]:
        """
        Build diagnostic metadata for the process holding the lock.

        Args:
            dev_mode: Whether the daemon was started with ``--dev``.
            config_path: Resolved configuration path, if known.

        Returns:
            JSON-serializable metadata describing the current process and
            daemon invocation.
        """
        return {
            "pid": os.getpid(),
            "hostname": socket.gethostname(),
            "username": getpass.getuser(),
            "cwd": str(Path.cwd()),
            "dev_mode": dev_mode,
            "config_path": str(config_path) if config_path is not None else None,
            "started_at": datetime.now(UTC).isoformat(),
        }

    @staticmethod
    def _read_metadata(lock_file: Any) -> dict[str, Any] | None:
        """
        Read metadata from an already-open lock file.

        This is a best-effort diagnostic helper used after lock acquisition
        fails. Invalid, empty, or non-object JSON is treated as missing
        metadata.

        Args:
            lock_file: Open lock file positioned anywhere.

        Returns:
            Parsed metadata dict, or ``None`` if no valid metadata is present.
        """
        try:
            lock_file.seek(0)
            content = lock_file.read()
            if not content.strip():
                return None
            metadata = json.loads(content)
            return metadata if isinstance(metadata, dict) else None
        except Exception:
            return None

    def release(self) -> None:
        """
        Release the local daemon lock.

        Unlocks and closes the underlying file handle. The method is idempotent
        so cleanup paths can call it safely even if the lock was already
        released.
        """
        if self._file is None:
            return
        try:
            fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
        finally:
            self._file.close()
            self._file = None

    def __enter__(self) -> DaemonLock:
        """
        Enter the daemon-lock context manager.

        Returns:
            The held ``DaemonLock`` instance.
        """
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        """
        Exit the daemon-lock context manager and release the lock.

        Args:
            exc_type: Exception type from the ``with`` block, if any.
            exc: Exception instance from the ``with`` block, if any.
            tb: Traceback from the ``with`` block, if any.
        """
        self.release()
