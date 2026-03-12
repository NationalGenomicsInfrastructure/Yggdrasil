"""External systems resolver for Yggdrasil.

Provides a single source of truth for resolving endpoint and connection
configuration from external_systems in main.json. Used by both WatcherManager
(to build backend config) and DataAccess (to build read clients).

Usage
-----
    from lib.core_utils.external_systems_resolver import (
        load_external_systems_config,
        resolve_endpoint,
        resolve_connection,
    )

    cfg = load_external_systems_config()
    conn = resolve_connection("flowcell_db", cfg)
    # conn.db_name, conn.endpoint.url, conn.data_access.realm_allowlist, ...
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from lib.core_utils.common import YggdrasilUtilities as Ygg
from lib.core_utils.logging_utils import custom_logger

logger = custom_logger(__name__)

# Built-in defaults for CouchDB auth env var names (same as WatcherManager)
_DEFAULT_USER_ENV = "YGG_COUCH_USER"
_DEFAULT_PASS_ENV = "YGG_COUCH_PASS"

# Built-in default for max_limit when not specified in config
_DEFAULT_MAX_LIMIT = 200


# ---------------------------------------------------------------------------
# Resolved dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ResolvedEndpoint:
    """Resolved endpoint configuration.

    Attributes:
        name: Endpoint name from config (e.g. "couchdb").
        url: Normalized URL with scheme (http:// or https://).
        user_env: Env var name for the CouchDB username.
        pass_env: Env var name for the CouchDB password.
        backend_type: Backend type identifier (e.g. "couchdb").
        dsn_env: Optional env var name for DSN (Postgres etc.); None if absent.
    """

    name: str
    url: str
    user_env: str
    pass_env: str
    backend_type: str
    dsn_env: str | None = None


@dataclass(frozen=True)
class DataAccessPolicy:
    """Per-connection data access policy.

    Controls which realms may read a connection and the query limits applied.

    Attributes:
        realm_allowlist: Realm IDs permitted to read this connection.
            No wildcard support in v1 — explicit realm IDs only.
        max_limit: Maximum number of results for find() queries.
            Effective value: per-connection override wins over global default,
            which wins over built-in default (200).

    Note:
        default_timeout_s is intentionally excluded from v1. It will be added
        in a future PR when actually wired to Cloudant request options.
    """

    realm_allowlist: list[str]
    max_limit: int


@dataclass(frozen=True)
class ResolvedConnection:
    """Resolved connection configuration.

    Attributes:
        name: Connection name from config (e.g. "flowcell_db").
        endpoint: Resolved endpoint this connection belongs to.
        db_name: Database name from resource.db.
        data_access: Policy governing realm read access, or None if the
            connection has no data_access block (not readable by realms).
    """

    name: str
    endpoint: ResolvedEndpoint
    db_name: str
    data_access: DataAccessPolicy | None


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------


def load_external_systems_config(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    """Load the external_systems block from main.json.

    Args:
        cfg: If provided, use this dict directly (for testing / injection).
             Must be a plain dict (not MappingProxyType).
             If None, loads via ConfigLoader().load_config("main.json").

    Returns:
        A plain dict with "endpoints" and "connections" keys (and optionally
        "defaults", "data_access_defaults").  Always a plain dict, never a
        MappingProxyType, so callers can safely merge / copy values.

    Notes:
        ConfigLoader returns MappingProxyType. This function converts the
        external_systems block to a plain dict (shallow copy at the top level).
        Nested values may still be MappingProxyType; resolver functions read
        them without mutation, so this is safe.
    """
    if cfg is not None:
        return cfg

    from lib.core_utils.config_loader import ConfigLoader

    full_config = ConfigLoader().load_config("main.json")
    external_systems = full_config.get("external_systems")

    if external_systems is None:
        logger.warning(
            "No 'external_systems' key found in main.json; using empty config"
        )
        return {"endpoints": {}, "connections": {}}

    # Shallow copy to get a plain dict (top-level keys become mutable)
    return dict(external_systems)


# ---------------------------------------------------------------------------
# Resolvers
# ---------------------------------------------------------------------------


def resolve_endpoint(endpoint_name: str, cfg: dict[str, Any]) -> ResolvedEndpoint:
    """Resolve an endpoint by name from external_systems config.

    Args:
        endpoint_name: Key in config["endpoints"] (e.g. "couchdb").
        cfg: External systems config dict (from load_external_systems_config).

    Returns:
        ResolvedEndpoint with normalized URL and auth env var names.

    Raises:
        KeyError: If the endpoint name is not found or URL is missing.
    """
    endpoints = cfg.get("endpoints") or {}

    if endpoint_name not in endpoints:
        available = list(endpoints.keys())
        raise KeyError(
            f"Unknown endpoint '{endpoint_name}'. " f"Available endpoints: {available}"
        )

    endpoint = endpoints[endpoint_name]

    raw_url = endpoint.get("url")
    if not raw_url:
        raise KeyError(f"Endpoint '{endpoint_name}' is missing required 'url' field")

    normalized_url = Ygg.normalize_url(raw_url)
    auth = endpoint.get("auth") or {}

    dsn_env: str | None = auth.get("dsn_env")

    return ResolvedEndpoint(
        name=endpoint_name,
        url=normalized_url,
        user_env=auth.get("user_env", _DEFAULT_USER_ENV),
        pass_env=auth.get("pass_env", _DEFAULT_PASS_ENV),
        backend_type=endpoint.get("backend", "couchdb"),
        dsn_env=dsn_env,
    )


def resolve_connection(connection_name: str, cfg: dict[str, Any]) -> ResolvedConnection:
    """Resolve a connection by name from external_systems config.

    Resolves the referenced endpoint, extracts db_name from resource, and
    builds a DataAccessPolicy if a data_access block is present.

    DataAccessPolicy.max_limit effective value:
        per-connection data_access.max_limit
        OR global data_access_defaults.couchdb.max_limit
        OR built-in default (200)

    Args:
        connection_name: Key in config["connections"] (e.g. "flowcell_db").
        cfg: External systems config dict (from load_external_systems_config).

    Returns:
        ResolvedConnection with endpoint, db_name, and optional DataAccessPolicy.

    Raises:
        KeyError: If the connection, its endpoint, or required fields are missing.
    """
    connections = cfg.get("connections") or {}

    if connection_name not in connections:
        available = list(connections.keys())
        raise KeyError(
            f"Unknown connection '{connection_name}'. "
            f"Available connections: {available}"
        )

    conn = connections[connection_name]

    endpoint_name = conn.get("endpoint")
    if not endpoint_name:
        raise KeyError(
            f"Connection '{connection_name}' is missing required 'endpoint' field"
        )

    endpoint = resolve_endpoint(endpoint_name, cfg)

    resource = conn.get("resource") or {}
    db_name: str | None = resource.get("db")
    if not db_name:
        raise KeyError(
            f"Connection '{connection_name}' resource is missing required 'db' field"
        )

    # Build DataAccessPolicy if connection has a data_access block
    da_policy: DataAccessPolicy | None = None
    conn_da = conn.get("data_access")

    if conn_da is not None:
        # Merge: global defaults < per-connection override
        global_defaults = (cfg.get("data_access_defaults") or {}).get("couchdb") or {}
        global_max_limit: int = global_defaults.get("max_limit", _DEFAULT_MAX_LIMIT)
        per_conn_max_limit: int | None = conn_da.get("max_limit")
        effective_max_limit = (
            per_conn_max_limit if per_conn_max_limit is not None else global_max_limit
        )

        realm_allowlist = list(conn_da.get("realm_allowlist") or [])

        da_policy = DataAccessPolicy(
            realm_allowlist=realm_allowlist,
            max_limit=effective_max_limit,
        )

    return ResolvedConnection(
        name=connection_name,
        endpoint=endpoint,
        db_name=db_name,
        data_access=da_policy,
    )
