"""DataAccess: realm-scoped read/write gateway to external systems."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from lib.core_utils.external_systems_resolver import (
    load_external_systems_config,
    resolve_connection,
)
from yggdrasil.flow.data_access.errors import (
    DataAccessConfigError,
    DataAccessDeniedError,
)

if TYPE_CHECKING:
    from lib.core_utils.external_systems_resolver import ResolvedConnection
    from yggdrasil.flow.data_access.models import DataAccessTraceContext

DataAccessPhase = Literal["planning", "execution"]


class DataAccess:
    """Realm-scoped read/write gateway to external system connections.

    Each :class:`DataAccess` instance is bound to a single realm and phase.
    It enforces per-realm/per-phase permission policy for every connection
    request and returns the appropriate typed client.

    In the planning phase, ``connection()`` returns a :class:`CouchDBPlanningClient`
    with async read methods (no writes).  In the execution phase, it returns a
    :class:`CouchDBExecutionClient` with sync reads and ``put()`` for writes.

    Config is loaded exactly once at construction time. Clients are cached:
    calling ``connection("name")`` twice returns the same instance.

    Args:
        realm_id: The realm identifier (e.g. ``"demux"``).
        phase: ``"planning"`` or ``"execution"``.
        cfg: Optional pre-loaded external_systems config dict. When ``None``
             (the default), config is loaded from main.json.
        trace_context: Trace metadata injected by core for execution-phase
             DataAccess instances. Enables write event emission.
    """

    def __init__(
        self,
        realm_id: str,
        phase: DataAccessPhase,
        cfg: dict[str, Any] | None = None,
        trace_context: DataAccessTraceContext | None = None,
    ) -> None:
        self._realm_id = realm_id
        self._phase = phase
        self._trace_context = trace_context
        self._cfg: dict[str, Any] = (
            cfg if cfg is not None else load_external_systems_config()
        )
        self._clients: dict[str, object] = {}

    def connection(self, connection_name: str) -> Any:
        """Return the appropriate data client for this realm and phase.

        Authorization is checked eagerly here:
        - Connection must exist in config.
        - Connection must have a data_access policy.
        - Realm must have at least one permission for this phase.

        Returns:
            CouchDBPlanningClient  (phase="planning")
            CouchDBExecutionClient (phase="execution")

        Raises:
            DataAccessConfigError:  connection_name not found in config.
            DataAccessDeniedError:  connection has no data_access policy, or
                                    realm has no permissions for this phase.
        """
        if connection_name in self._clients:
            return self._clients[connection_name]

        try:
            conn = resolve_connection(connection_name, self._cfg)
        except KeyError:
            available = list((self._cfg.get("connections") or {}).keys())
            raise DataAccessConfigError(
                f"Unknown connection '{connection_name}'. "
                f"Available connections: {available}"
            ) from None
        except ValueError as exc:
            raise DataAccessConfigError(str(exc)) from exc

        if conn.data_access is None:
            raise DataAccessDeniedError(
                f"Connection '{connection_name}' has no data_access policy configured."
            )

        permissions = conn.data_access.get_permissions(self._realm_id, self._phase)
        if not permissions:
            raise DataAccessDeniedError(
                f"Realm '{self._realm_id}' has no permissions for connection "
                f"'{connection_name}' in phase '{self._phase}'."
            )

        if self._phase == "planning" and "read" not in permissions:
            raise DataAccessDeniedError(
                f"Realm '{self._realm_id}' does not have 'read' permission for connection "
                f"'{connection_name}' in phase '{self._phase}'. "
                "Planning phase requires 'read' permission."
            )

        if conn.endpoint.backend_type != "couchdb":
            raise DataAccessConfigError(
                f"Connection '{connection_name}' uses backend "
                f"'{conn.endpoint.backend_type}', which is not yet supported by DataAccess. "
                "Currently only 'couchdb' is supported."
            )

        client = self._build_client(conn, permissions)
        self._clients[connection_name] = client
        return client

    def couchdb(self, connection_name: str) -> Any:
        """Return a CouchDB-specific client for the named connection.

        Validates that the resolved endpoint is a CouchDB backend, then
        delegates to connection(). Use connection() for backend-neutral code.

        Raises:
            DataAccessConfigError: connection not found, or endpoint is not CouchDB.
            DataAccessDeniedError: no permissions for realm+phase.
        """
        try:
            conn = resolve_connection(connection_name, self._cfg)
        except KeyError:
            available = list((self._cfg.get("connections") or {}).keys())
            raise DataAccessConfigError(
                f"Unknown connection '{connection_name}'. "
                f"Available connections: {available}"
            ) from None
        except ValueError as exc:
            raise DataAccessConfigError(str(exc)) from exc

        if conn.endpoint.backend_type != "couchdb":
            raise DataAccessConfigError(
                f"Connection '{connection_name}' uses backend "
                f"'{conn.endpoint.backend_type}', not 'couchdb'. "
                "Use connection() for backend-neutral access."
            )

        return self.connection(connection_name)

    def _build_client(
        self,
        conn: ResolvedConnection,
        permissions: frozenset[str],
    ) -> Any:
        from lib.couchdb.couchdb_connection import CouchDBHandler
        from yggdrasil.flow.data_access.couchdb_data import (
            CouchDBExecutionClient,
            CouchDBPlanningClient,
            _CouchDBSyncOps,
            _CouchDBWriteOps,
        )

        handler = CouchDBHandler(
            db_name=conn.db_name,
            url=conn.endpoint.url,
            user_env=conn.endpoint.user_env,
            pass_env=conn.endpoint.pass_env,
        )
        ops = _CouchDBSyncOps(handler, conn.data_access.options)  # type: ignore[union-attr]

        if self._phase == "planning":
            return CouchDBPlanningClient(ops)
        else:
            return CouchDBExecutionClient(
                ops=ops,
                write_ops=_CouchDBWriteOps(handler),
                permissions=permissions,
                realm_id=self._realm_id,
                connection_name=conn.name,
                resource=conn.db_name,
                trace_context=self._trace_context,
            )
