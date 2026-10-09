import logging

from lib.core_utils.logging_utils import custom_logger
from lib.couchdb.couchdb_connection import CouchDBHandler
from lib.couchdb.couchdb_defaults import DEFAULT_ENDPOINT, resolve_couchdb_params


class YggdrasilDBManager(CouchDBHandler):
    """CouchDB handler bound to the ``yggdrasil`` coordination database.

    Defines no operations of its own: its users (CouchDB checkpoint
    persistence, the CouchDB internal-storage bundle and PlanWatcher's
    bare-construction default) need only the ``CouchDBHandler`` members.
    """

    def __init__(
        self,
        *,
        endpoint: str = DEFAULT_ENDPOINT,
        url: str | None = None,
        user_env: str | None = None,
        pass_env: str | None = None,
        db_name: str = "yggdrasil",
        logger: logging.Logger | None = None,
    ) -> None:
        """Resolve connection parameters and connect to the database.

        Args:
            endpoint: Name of the CouchDB endpoint in ``external_systems``.
            url: Explicit server URL; overrides the endpoint's URL.
            user_env: Name of the environment variable holding the user name.
            pass_env: Name of the environment variable holding the password.
            db_name: Database name. Defaults to ``"yggdrasil"``.
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
            db_name,
            url=params.url,
            user_env=params.user_env,
            pass_env=params.pass_env,
            logger=self._logger,
        )
