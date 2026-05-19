"""Read/write data access for realms.

Provides :class:`DataAccess`, the realm-scoped gateway for reading from and
writing to external system connections (CouchDB v1).

Usage::

    from yggdrasil.flow.data_access import DataAccess, DataAccessDeniedError

    da = DataAccess(realm_id="demux", phase="execution")
    client = da.connection("flowcell_db")

    # Reads
    doc = client.get("run_123")

    # Writes — pass a clean body dict (no _id or _rev)
    body = {"status": "complete", "updated_by": "yggdrasil"}
    result = client.put("run_123", body, mode="upsert")
"""

from yggdrasil.flow.data_access.data_access import DataAccess
from yggdrasil.flow.data_access.errors import (
    DataAccessConfigError,
    DataAccessDeniedError,
    DataAccessError,
    DataAccessNotFoundError,
    DataAccessQueryError,
    DataAccessWriteError,
)
from yggdrasil.flow.data_access.models import (
    DataAccessTraceContext,
    DataAccessWriteResult,
)

__all__ = [
    "DataAccess",
    "DataAccessError",
    "DataAccessConfigError",
    "DataAccessDeniedError",
    "DataAccessNotFoundError",
    "DataAccessQueryError",
    "DataAccessWriteError",
    "DataAccessTraceContext",
    "DataAccessWriteResult",
]
