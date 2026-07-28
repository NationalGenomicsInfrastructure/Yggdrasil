"""Cross-cutting exceptions shared across Yggdrasil subsystems.

Module-local exceptions (raised and handled within a single subsystem) stay
next to the code that raises them — e.g. ``DaemonLockError`` in
``lib/core_utils/daemon_lock.py`` or ``WatcherConfigurationError`` in
``lib/watchers/config_validation.py``. This module hosts only exceptions that
cut across subsystems: raised at one boundary (CouchDB layer, future SSH/HPC
clients, watcher backends) and handled centrally (the CLI).
"""

from __future__ import annotations


class InternalStorageConfigurationError(RuntimeError):
    """The ``internal_storage`` configuration block is invalid.

    Raised during startup configuration resolution (``lib/storage/config.py``)
    when an explicit ``internal_storage`` block references unknown connections,
    a non-CouchDB endpoint, requests SQLite outside dev mode, or supplies an
    invalid SQLite path. Explicit configuration errors are fatal by design —
    Yggdrasil never silently falls back to another storage backend. The CLI
    catches this at startup to print a concise operator message instead of a
    full traceback.
    """


class ExternalSystemUnavailableError(ConnectionError):
    """An external system required by Yggdrasil could not be reached.

    Raised at connection boundaries (e.g. ``CouchDBClientFactory``) when a
    required external system is unreachable or a required resource on it is
    missing. The CLI catches this at startup to print a concise operator
    message instead of a full traceback.

    Subclasses the builtin ``ConnectionError`` so existing callers that catch
    ``ConnectionError`` (e.g. the data-access wrappers) keep working.

    Attributes:
        system: Human-readable system name (e.g. "CouchDB").
        endpoint: The URL/address that could not be reached.
        hint: Optional operator-facing remediation hint.
    """

    def __init__(self, system: str, endpoint: str, *, hint: str | None = None) -> None:
        """Initialize the error with system identity and optional hint.

        Args:
            system: Human-readable system name (e.g. "CouchDB").
            endpoint: The URL/address that could not be reached.
            hint: Optional operator-facing remediation hint, appended to the
                message.
        """
        self.system = system
        self.endpoint = endpoint
        self.hint = hint
        msg = f"Cannot reach {system} at {endpoint}."
        if hint:
            msg += f" {hint}"
        super().__init__(msg)
