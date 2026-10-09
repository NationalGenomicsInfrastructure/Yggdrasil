import logging

from lib.core_utils.logging_utils import custom_logger
from lib.couchdb.couchdb_connection import CouchDBHandler
from lib.couchdb.couchdb_defaults import DEFAULT_ENDPOINT, resolve_couchdb_params


class ProjectDBManager(CouchDBHandler):
    """CouchDB handler bound to the external ``projects`` database.

    Used only by the run-doc paths, which read single project documents
    through the inherited ``CouchDBHandler`` members.
    """

    def __init__(
        self,
        *,
        endpoint: str = DEFAULT_ENDPOINT,
        url: str | None = None,
        user_env: str | None = None,
        pass_env: str | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        """Resolve connection parameters and connect to the database.

        Args:
            endpoint: Name of the CouchDB endpoint in ``external_systems``.
            url: Explicit server URL; overrides the endpoint's URL.
            user_env: Name of the environment variable holding the user name.
            pass_env: Name of the environment variable holding the password.
            logger: Logger to use; a module-scoped one is created if omitted.
        """
        self._logger = logger or custom_logger(f"{__name__}.{type(self).__name__}")
        params = resolve_couchdb_params(
            endpoint=endpoint,
            url=url,
            user_env=user_env,
            pass_env=pass_env,
        )

        super().__init__(
            "projects",
            url=params.url,
            user_env=params.user_env,
            pass_env=params.pass_env,
            logger=self._logger,
        )
