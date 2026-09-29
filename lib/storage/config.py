"""Configuration resolution for internal storage.

Reads the optional ``internal_storage`` block from the loaded main
configuration and resolves it into a typed config object:

- ``backend: "couchdb"`` — each logical database role is assigned one named
  ``external_systems`` connection; URL and auth env-var names come from the
  referenced endpoint. Credentials, URLs, and database names are never
  duplicated under ``internal_storage``.
- ``backend: "sqlite"`` — accepted only in effective dev mode (or explicit
  test injection). ``path: null`` selects the workspace default.
- Absent block — legacy implicit CouchDB construction (deprecated).

Invalid explicit configuration is fatal; Yggdrasil never falls back to a
different backend than the one configured.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar

from lib.core_utils.common import YggdrasilUtilities as Ygg
from lib.core_utils.errors import InternalStorageConfigurationError
from lib.core_utils.external_systems_resolver import (
    ResolvedConnection,
    resolve_connection,
)
from lib.core_utils.ygg_session import YggSession

# Logical database roles that must each map to a named connection.
_COUCH_ROLES = ("coordination", "plans", "operations")


@dataclass(frozen=True)
class CouchInternalStorageConfig:
    """Explicit CouchDB internal-storage configuration.

    Attributes:
        coordination: Connection for the coordination database (checkpoints).
        plans: Connection for the plan documents database.
        operations: Connection for the operations snapshots database.
    """

    coordination: ResolvedConnection
    plans: ResolvedConnection
    operations: ResolvedConnection

    backend: ClassVar[str] = "couchdb"


@dataclass(frozen=True)
class SQLiteInternalStorageConfig:
    """Explicit SQLite internal-storage configuration (dev/tests only).

    Attributes:
        path: Absolute path of the SQLite database file.
    """

    path: Path

    backend: ClassVar[str] = "sqlite"


def default_sqlite_path() -> Path:
    """Return the default dev SQLite database path inside the workspace."""
    return Ygg.workspace_path() / "internal_state" / "dev" / "yggdrasil.sqlite3"


def resolve_internal_storage_config(
    config: Mapping[str, Any],
    *,
    dev_mode: bool | None = None,
) -> CouchInternalStorageConfig | SQLiteInternalStorageConfig | None:
    """Resolve the ``internal_storage`` block of the main configuration.

    Args:
        config: The full loaded main configuration mapping.
        dev_mode: Effective dev mode override for tests. Defaults to
            ``YggSession.is_dev()``.

    Returns:
        A typed config object, or None when the block is absent (legacy
        implicit CouchDB construction applies).

    Raises:
        InternalStorageConfigurationError: On any invalid explicit
            configuration (unknown backend, missing/malformed connection
            references, non-CouchDB endpoints, SQLite outside dev mode, or a
            non-absolute SQLite path).
    """
    block = config.get("internal_storage")
    if block is None:
        return None

    if not isinstance(block, Mapping):
        raise InternalStorageConfigurationError(
            "internal_storage must be an object, got " f"{type(block).__name__}"
        )

    backend = block.get("backend")
    if backend == "couchdb":
        return _resolve_couchdb(block, config)
    if backend == "sqlite":
        return _resolve_sqlite(block, dev_mode=dev_mode)

    raise InternalStorageConfigurationError(
        f"internal_storage.backend must be 'couchdb' or 'sqlite', got {backend!r}"
    )


def _resolve_couchdb(
    block: Mapping[str, Any],
    config: Mapping[str, Any],
) -> CouchInternalStorageConfig:
    """Resolve the explicit CouchDB role→connection assignments."""
    couch_block = block.get("couchdb")
    if not isinstance(couch_block, Mapping):
        raise InternalStorageConfigurationError(
            "internal_storage.couchdb must be an object with a 'connections' map"
        )
    role_map = couch_block.get("connections")
    if not isinstance(role_map, Mapping):
        raise InternalStorageConfigurationError(
            "internal_storage.couchdb.connections must map roles "
            f"{list(_COUCH_ROLES)} to external_systems connection names"
        )

    missing = [role for role in _COUCH_ROLES if not role_map.get(role)]
    if missing:
        raise InternalStorageConfigurationError(
            f"internal_storage.couchdb.connections is missing roles: {missing}"
        )

    ext_cfg = dict(config.get("external_systems") or {})

    resolved: dict[str, ResolvedConnection] = {}
    for role in _COUCH_ROLES:
        connection_name = role_map[role]
        try:
            conn = resolve_connection(str(connection_name), ext_cfg)
        except (KeyError, ValueError) as e:
            raise InternalStorageConfigurationError(
                f"internal_storage role '{role}': {e}"
            ) from e
        if conn.endpoint.backend_type != "couchdb":
            raise InternalStorageConfigurationError(
                f"internal_storage role '{role}' resolves to endpoint "
                f"'{conn.endpoint.name}' with backend "
                f"'{conn.endpoint.backend_type}'; a CouchDB endpoint is required"
            )
        resolved[role] = conn

    return CouchInternalStorageConfig(
        coordination=resolved["coordination"],
        plans=resolved["plans"],
        operations=resolved["operations"],
    )


def _resolve_sqlite(
    block: Mapping[str, Any],
    *,
    dev_mode: bool | None,
) -> SQLiteInternalStorageConfig:
    """Resolve the explicit SQLite configuration (dev-mode gated)."""
    effective_dev = YggSession.is_dev() if dev_mode is None else dev_mode
    if not effective_dev:
        raise InternalStorageConfigurationError(
            "internal_storage.backend='sqlite' is only supported in dev mode "
            "(run with --dev) or tests; production uses CouchDB"
        )

    sqlite_block = block.get("sqlite") or {}
    if not isinstance(sqlite_block, Mapping):
        raise InternalStorageConfigurationError(
            "internal_storage.sqlite must be an object"
        )

    raw_path = sqlite_block.get("path")
    if raw_path is None:
        return SQLiteInternalStorageConfig(path=default_sqlite_path())

    path = Path(str(raw_path))
    if not path.is_absolute():
        raise InternalStorageConfigurationError(
            f"internal_storage.sqlite.path must be absolute, got {raw_path!r}"
        )
    return SQLiteInternalStorageConfig(path=path)
