import hashlib
import os
import threading
from dataclasses import dataclass
from typing import Any

from ibm_cloud_sdk_core.api_exception import ApiException
from ibmcloudant import CouchDbSessionAuthenticator, cloudant_v1

from lib.core_utils.common import YggdrasilUtilities as Ygg
from lib.core_utils.config_loader import ConfigLoader
from lib.core_utils.logging_utils import custom_logger
from lib.core_utils.singleton_decorator import singleton

logging = custom_logger(__name__.split(".")[-1])

# Sentinel for cache key when using default credentials
_DEFAULT_PASS_ENV_SENTINEL = "__default_credentials__"


@dataclass
class _CachedClient:
    """Internal: cached CloudantV1 client with password hash for rotation detection."""

    client: cloudant_v1.CloudantV1
    password_hash: str


@singleton
class CouchDBConnectionManager:
    """
    Singleton that manages CouchDB/CloudantV1 connections.

    Features:
      - Default client for legacy code (connect_server)
      - Per-endpoint client cache with password rotation detection (get_server)
      - Thread-safe cache access
      - Unified entry point for all CouchDB connections

    Cache Key Design:
        (url, user, pass_env_name) - no raw passwords in keys.
        Password changes are detected via sha256 hash stored with cached client.

    Usage:
        # Legacy (default credentials from env/config):
        mgr = CouchDBConnectionManager()
        client = mgr.server

        # Endpoint-specific:
        client = mgr.get_server(url="...", user="...", pass_env_name="MY_PASS_VAR")
    """

    def __init__(
        self,
        db_url: str | None = None,
        db_user: str | None = None,
        db_password: str | None = None,
    ) -> None:
        # Load defaults from configuration file or environment
        self.db_config = ConfigLoader().load_config("main.json").get("couchdb", {})
        self.db_url = Ygg.normalize_url(db_url or self.db_config.get("url"))
        self.db_user = db_user or os.getenv(
            "COUCH_USER", self.db_config.get("default_user")
        )
        self.db_password = db_password or os.getenv(
            "COUCH_PASS", self.db_config.get("default_password")
        )

        # Default client (legacy)
        self.server: cloudant_v1.CloudantV1 | None = None

        # Per-endpoint client cache: (url, user, pass_env_name) -> _CachedClient
        self._client_cache: dict[tuple[str, str | None, str], _CachedClient] = {}
        self._cache_lock = threading.Lock()

        self.connect_server()

    def connect_server(self) -> None:
        """Establishes the default connection to the CouchDB server."""
        if self.server is None:
            try:
                self.server = cloudant_v1.CloudantV1(
                    authenticator=CouchDbSessionAuthenticator(
                        self.db_user, self.db_password
                    )
                )
                self.server.set_service_url(self.db_url)

                info = self.server.get_server_information().get_result() or {}
                if isinstance(info, dict):
                    version = str(info.get("version", "unknown"))
                else:
                    version = "unknown"

                logging.info(f"Connected to CouchDB server. Version: {version}")
            except Exception as e:
                logging.error(
                    f"An error occurred while connecting to the CouchDB server: {e}"
                )
                raise ConnectionError("Failed to connect to CouchDB server")
        else:
            logging.info("Already connected to CouchDB server.")

    def get_server(
        self,
        *,
        url: str | None = None,
        user: str | None = None,
        pass_env_name: str | None = None,
    ) -> cloudant_v1.CloudantV1:
        """
        Get a CloudantV1 client, using cache for deduplication.

        If url/user/pass_env_name are all None, returns the default client.
        Otherwise, returns a cached client for the specified endpoint,
        creating one if necessary.

        Password rotation detection:
            If the password (resolved from pass_env_name) has changed since
            the client was cached, a new client is created.

        Args:
            url: CouchDB server URL (None = use default)
            user: Username (None = use default)
            pass_env_name: Environment variable name containing password
                          (None = use default credentials)

        Returns:
            CloudantV1 client instance (shared, do not close)

        Raises:
            RuntimeError: If pass_env_name is specified but env var is missing
            ConnectionError: If connection fails
        """
        # All None = use default client
        if url is None and user is None and pass_env_name is None:
            if self.server is None:
                raise ConnectionError("Default server not connected")
            return self.server

        # Resolve actual values (falling back to defaults where None)
        actual_url = url if url is not None else self.db_url
        actual_user = user if user is not None else self.db_user

        # Resolve password from env var or default
        if pass_env_name is not None:
            if pass_env_name not in os.environ:
                raise RuntimeError(
                    f"Missing required env var '{pass_env_name}' for CouchDB connection"
                )
            actual_password = os.environ[pass_env_name]
        else:
            if self.db_password is None:
                raise ConnectionError(
                    "No password available: pass_env_name not specified and "
                    "no default password configured"
                )
            actual_password = self.db_password

        # Cache key uses pass_env_name (not password) to avoid secrets in keys
        cache_key = (
            actual_url,
            actual_user,
            pass_env_name or _DEFAULT_PASS_ENV_SENTINEL,
        )
        password_hash = hashlib.sha256(actual_password.encode()).hexdigest()

        with self._cache_lock:
            cached = self._client_cache.get(cache_key)

            if cached is not None:
                # Check for password rotation
                if cached.password_hash == password_hash:
                    return cached.client
                else:
                    logging.info(
                        "Password rotation detected for %s@%s; creating new client",
                        actual_user,
                        actual_url,
                    )
                    # Fall through to create new client

            # Create new client
            try:
                client = cloudant_v1.CloudantV1(
                    authenticator=CouchDbSessionAuthenticator(
                        actual_user, actual_password
                    )
                )
                client.set_service_url(actual_url)

                # Verify connection
                info = client.get_server_information().get_result() or {}
                if isinstance(info, dict):
                    version = str(info.get("version", "unknown"))
                else:
                    version = "unknown"

                logging.info(
                    "Connected to CouchDB at %s (user=%s, version=%s)",
                    actual_url,
                    actual_user,
                    version,
                )

                # Cache the client
                self._client_cache[cache_key] = _CachedClient(
                    client=client, password_hash=password_hash
                )
                return client

            except Exception as e:
                logging.error("Failed to connect to CouchDB at %s: %s", actual_url, e)
                raise ConnectionError(
                    f"Failed to connect to CouchDB at {actual_url}"
                ) from e

    def ensure_db(self, db_name: str) -> str:
        """
        Verify the database exists on the DEFAULT server. Return `db_name` if it does.

        LEGACY: This method only checks the default client (self.server).
        For endpoint-specific connections, CouchDBHandler.__init__ already
        verifies database existence on the appropriate server.

        Deprecated: Will be removed in a future version. New code should
        use CouchDBHandler directly, which handles DB verification internally.
        """
        if not self.server:
            raise ConnectionError("Server not connected")
        try:
            self.server.get_database_information(db=db_name)
            return db_name
        except ApiException as e:
            if e.code == 404:
                logging.error(f"Database {db_name} does not exist on the server.")
            else:
                logging.error(
                    f"An error occurred while accessing database {db_name}: {e}"
                )
            raise ConnectionError(f"Database {db_name} does not exist") from e


class CouchDBHandler:
    """
    Base class for CouchDB operations on a specific database.

    Inheriting classes specify a database name and leverage the
    CouchDBConnectionManager to:

      - Obtain a server connection (default or endpoint-specific).
      - Connect to the desired database.
      - Store a `server` attribute for CRUD operations.

    All connections go through CouchDBConnectionManager.get_server(),
    ensuring client deduplication and proper caching.

    Credential override support:
        When url/user/pass_env_name are provided (e.g., from watcher
        endpoint config), get_server() returns a cached client for
        that endpoint. Otherwise, falls back to the default client.
    """

    def __init__(
        self,
        db_name: str,
        *,
        url: str | None = None,
        user: str | None = None,
        pass_env_name: str | None = None,
    ) -> None:
        """
        Initialize CouchDB handler for a specific database.

        Args:
            db_name: Database name to operate on
            url: Optional CouchDB server URL (None = use config default)
            user: Optional username (None = use env/config default)
            pass_env_name: Environment variable name containing password
                          (None = use default credentials)

        All connections go through CouchDBConnectionManager.get_server()
        for proper client caching and deduplication.

        Raises:
            ConnectionError: If database doesn't exist or connection fails
            RuntimeError: If pass_env_name is specified but env var is missing
        """
        self.db_name = db_name

        # Get connection manager singleton
        self.connection_manager = CouchDBConnectionManager()

        # Get server via unified get_server() method (handles caching)
        self.server = self.connection_manager.get_server(
            url=url,
            user=user,
            pass_env_name=pass_env_name,
        )

        # Verify database exists (fail fast)
        try:
            self.server.get_database_information(db=db_name)
        except ApiException as e:
            if e.code == 404:
                raise ConnectionError(f"Database {db_name} does not exist") from e
            raise

    def fetch_document_by_id(self, doc_id) -> dict[str, Any] | None:
        """Fetches a document from the database by its ID.

        Args:
            doc_id (str): The ID of the document to fetch.

        Returns:
            Optional[dict[str, Any]]: The retrieved document, or None if not found.
        """
        try:
            document = self.server.get_document(
                db=self.db_name, doc_id=doc_id
            ).get_result()
            if isinstance(document, dict):
                return document
            logging.warning("Unexpected non-dict response when fetching %s", doc_id)
            return None
        except ApiException as e:
            if e.code == 404:
                logging.debug(
                    "Document '%s' not found in database '%s'",
                    doc_id,
                    self.db_name,
                )
                return None
            logging.error(
                f"Cloudant API error fetching '{doc_id}' from {self.db_name}: {e.code} {e.message}"
            )
            return None
        except Exception as e:
            logging.error(f"Error while accessing database {self.db_name}: {e}")
            return None

    def post_changes(
        self,
        *,
        since: str | int | None = None,
        include_docs: bool = True,
        limit: int = 100,
    ) -> dict[str, Any]:
        """
        Fetch changes from the database's _changes feed.

        This is a wrapper around the CloudantV1 post_changes API,
        providing a simpler interface for watcher backends.

        Args:
            since: Sequence token to start from (default: "0" for all changes)
                   Accepts int for convenience; normalized to str for SDK.
            include_docs: Include full documents in response (default: True)
            limit: Maximum number of changes to return (default: 100)

        Returns:
            Dict with 'results' (list of changes) and 'last_seq' (checkpoint)

        Raises:
            ApiException: If the CouchDB API call fails
            TypeError: If response is not a dict (unexpected SDK behavior)
        """
        # Normalize since to str (SDK expects str | None)
        if since is None:
            since_str: str | None = None  # Let SDK use default
        else:
            since_str = str(since)

        response = self.server.post_changes(
            db=self.db_name,
            since=since_str,
            include_docs=include_docs,
            limit=limit,
        )
        result = response.get_result()
        if isinstance(result, dict):
            return result
        # Unexpected SDK response - fail explicitly
        raise TypeError(
            f"post_changes returned non-dict result: {type(result).__name__}"
        )
