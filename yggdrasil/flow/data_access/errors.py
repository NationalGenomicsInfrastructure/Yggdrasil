"""Exceptions for the data_access package."""


class DataAccessError(Exception):
    """Base exception for all data access failures."""


class DataAccessDeniedError(DataAccessError):
    """Raised when a realm requests a connection it is not allowed to read.

    This is raised when:
    - The connection has no data_access policy configured, or
    - The realm_id is not listed in data_access.realm_allowlist.
    """


class DataAccessConfigError(DataAccessError):
    """Raised when a requested connection name does not exist in config.

    Distinct from DataAccessDeniedError (which means the connection exists
    but the realm is not permitted).  DataAccessConfigError means the
    connection key itself is absent from external_systems.connections,
    which is a configuration problem rather than a permission problem.
    """


class DataAccessNotFoundError(DataAccessError):
    """Raised by require() / require_one() when no document is found."""


class DataAccessQueryError(DataAccessError):
    """Raised when a CouchDB query fails due to an API or transport error.

    Distinct from DataAccessNotFoundError (query succeeded, no results).
    DataAccessQueryError means the query itself failed — e.g. a permissions
    error (403), server error (500), network failure, or malformed selector.
    """
