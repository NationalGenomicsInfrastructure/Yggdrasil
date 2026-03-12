"""Read-only data access for realms.

Provides :class:`DataAccess`, the realm-scoped gateway for reading
from external system connections (CouchDB v1).

Usage::

    from yggdrasil.flow.data_access import DataAccess, DataAccessDeniedError

    da = DataAccess(realm_id="demux")
    client = da.couchdb("flowcell_db")
    doc = await client.get("run_123")
"""

from yggdrasil.flow.data_access.data_access import DataAccess
from yggdrasil.flow.data_access.errors import (
    DataAccessConfigError,
    DataAccessDeniedError,
    DataAccessError,
    DataAccessNotFoundError,
    DataAccessQueryError,
)

__all__ = [
    "DataAccess",
    "DataAccessError",
    "DataAccessConfigError",
    "DataAccessDeniedError",
    "DataAccessNotFoundError",
    "DataAccessQueryError",
]
