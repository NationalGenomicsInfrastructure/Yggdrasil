"""DataAccess: realm-scoped read-only access to external systems."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from lib.core_utils.external_systems_resolver import (
    load_external_systems_config,
    resolve_connection,
)
from yggdrasil.flow.data_access.couchdb_read import CouchDBReadClient
from yggdrasil.flow.data_access.errors import (
    DataAccessConfigError,
    DataAccessDeniedError,
)

if TYPE_CHECKING:
    pass


class DataAccess:
    """Realm-scoped read-only gateway to external system connections.

    Each :class:`DataAccess` instance is bound to a single realm.  It
    enforces the ``data_access.realm_allowlist`` policy for every
    connection request and provides typed read clients.

    Config is loaded exactly once at construction time and reused for
    the lifetime of the instance, so repeated ``couchdb()`` calls do
    not trigger additional disk reads.  Clients are also cached: calling
    ``couchdb("name")`` twice returns the same :class:`CouchDBReadClient`.

    Args:
        realm_id: The realm identifier (e.g. ``"demux"``).
        cfg: Optional pre-loaded external_systems config dict.  When
             ``None`` (the default), config is loaded from
             ``ConfigLoader().load_config("main.json")["external_systems"]``.
             Pass a plain dict in tests to avoid filesystem access.

    Example::

        da = DataAccess(realm_id="demux")
        client = da.couchdb("flowcell_db")
        doc = await client.get("some_doc_id")
    """

    def __init__(self, realm_id: str, cfg: dict[str, Any] | None = None) -> None:
        self._realm_id = realm_id
        # Load config ONCE; all resolver calls below use this stored dict.
        self._cfg: dict[str, Any] = (
            cfg if cfg is not None else load_external_systems_config()
        )
        self._couchdb_clients: dict[str, CouchDBReadClient] = {}

    def couchdb(self, connection_name: str) -> CouchDBReadClient:
        """Return a read-only CouchDB client for the named connection.

        The client is created on first call and cached for subsequent
        calls with the same ``connection_name``.

        Args:
            connection_name: Key in ``external_systems.connections``
                             (e.g. ``"flowcell_db"``).

        Returns:
            :class:`CouchDBReadClient` bound to the connection's database.

        Raises:
            DataAccessConfigError: If ``connection_name`` is not found in
                ``external_systems.connections``.
            DataAccessDeniedError: If the connection has no ``data_access``
                policy, or if this realm is not listed in
                ``data_access.realm_allowlist``.
        """
        if connection_name in self._couchdb_clients:
            return self._couchdb_clients[connection_name]

        try:
            conn = resolve_connection(connection_name, self._cfg)
        except KeyError:
            available = list((self._cfg.get("connections") or {}).keys())
            raise DataAccessConfigError(
                f"Unknown connection '{connection_name}'. "
                f"Available connections: {available}"
            ) from None

        if conn.data_access is None:
            raise DataAccessDeniedError(
                f"Connection '{connection_name}' has no data_access policy configured. "
                "Add a data_access block with realm_allowlist to main.json."
            )
        if self._realm_id not in conn.data_access.realm_allowlist:
            raise DataAccessDeniedError(
                f"Realm '{self._realm_id}' is not in the allowlist for "
                f"connection '{connection_name}'."
            )

        from lib.couchdb.couchdb_connection import CouchDBHandler

        handler = CouchDBHandler(
            db_name=conn.db_name,
            url=conn.endpoint.url,
            user_env=conn.endpoint.user_env,
            pass_env=conn.endpoint.pass_env,
        )
        client = CouchDBReadClient(handler, conn.data_access)
        self._couchdb_clients[connection_name] = client
        return client
