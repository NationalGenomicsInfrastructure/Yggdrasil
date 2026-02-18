"""
CouchDB connection utilities.

This module provides:
- CouchDBClientFactory: Stateless factory for creating CloudantV1 clients
- CouchDBHandler: Base class for database operations

Design principles:
- No global shared clients (each backend gets its own client)
- No singleton pattern (stateless factory)
- No config file reading (config resolution happens in WatcherManager)
- Env var resolution happens at client creation time
- Clear errors on misconfiguration (no silent defaults)
"""

import os
from typing import Any

from ibm_cloud_sdk_core.api_exception import ApiException
from ibmcloudant import CouchDbSessionAuthenticator, cloudant_v1

from lib.core_utils.logging_utils import custom_logger

logger = custom_logger(__name__.split(".")[-1])


class CouchDBClientFactory:
    """
    Stateless factory for creating CouchDB/CloudantV1 clients.

    This factory:
    - Creates a NEW client on each call (no caching)
    - Validates URL has correct scheme
    - Resolves credentials from environment variables
    - Optionally verifies connection via ping

    Each backend instance should create its own client.
    No shared state, no locks, no singleton.
    """

    _logged_connections: set[tuple[str, str]] = set()

    @staticmethod
    def create_client(
        url: str,
        user_env: str,
        pass_env: str,
        *,
        verify_connection: bool = True,
    ) -> cloudant_v1.CloudantV1:
        """
        Create a new CloudantV1 client.

        Args:
            url: CouchDB server URL (must include http:// or https://)
            user_env: Environment variable name containing username
            pass_env: Environment variable name containing password
            verify_connection: If True, ping server to fail fast (default True)

        Returns:
            CloudantV1 client instance (caller owns this client)

        Raises:
            ValueError: If URL is missing scheme (http/https)
            RuntimeError: If required env var is missing
            ConnectionError: If connection verification fails
        """
        # Validate URL has scheme
        if not url.startswith(("http://", "https://")):
            raise ValueError(
                f"CouchDB URL must include scheme (http:// or https://). "
                f"Got: {url!r}"
            )

        # Resolve credentials from env vars
        user = os.environ.get(user_env)
        if not user:
            raise RuntimeError(
                f"Missing required env var '{user_env}' for CouchDB username"
            )

        password = os.environ.get(pass_env)
        if not password:
            raise RuntimeError(
                f"Missing required env var '{pass_env}' for CouchDB password"
            )

        # Create client
        try:
            client = cloudant_v1.CloudantV1(
                authenticator=CouchDbSessionAuthenticator(user, password)
            )
            client.set_service_url(url)

            # Verify connection (fail fast)
            if verify_connection:
                info = client.get_server_information().get_result() or {}
                if isinstance(info, dict):
                    version = str(info.get("version", "unknown"))
                else:
                    version = "unknown"

                conn_key = (url, user)
                if conn_key not in CouchDBClientFactory._logged_connections:
                    logger.info(
                        "Connected to CouchDB at %s (user=%s, version=%s)",
                        url,
                        user,
                        version,
                    )
                    CouchDBClientFactory._logged_connections.add(conn_key)
                else:
                    logger.debug(
                        "Reconnected to CouchDB at %s (user=%s)",
                        url,
                        user,
                    )

            return client

        except Exception as e:
            logger.error("Failed to connect to CouchDB at %s: %s", url, e)
            raise ConnectionError(f"Failed to connect to CouchDB at {url}") from e


class CouchDBHandler:
    """
    Base class for CouchDB operations on a specific database.

    Each handler instance owns its own CloudantV1 client created via
    CouchDBClientFactory. No shared clients, no connection pooling.

    Usage:
        handler = CouchDBHandler(
            db_name="projects",
            url="https://couch.example.org:5984",
            user_env="COUCH_USER",
            pass_env="COUCH_PASS",
        )
        doc = handler.fetch_document_by_id("doc123")
    """

    def __init__(
        self,
        db_name: str,
        *,
        url: str,
        user_env: str,
        pass_env: str,
    ) -> None:
        """
        Initialize CouchDB handler for a specific database.

        Args:
            db_name: Database name to operate on
            url: CouchDB server URL (must include http:// or https://)
            user_env: Environment variable name containing username
            pass_env: Environment variable name containing password

        Raises:
            ValueError: If URL is missing scheme
            RuntimeError: If required env var is missing
            ConnectionError: If database doesn't exist or connection fails
        """
        self.db_name = db_name

        # Create client via factory (each handler owns its client)
        self.server = CouchDBClientFactory.create_client(
            url=url,
            user_env=user_env,
            pass_env=pass_env,
        )

        # Verify database exists (fail fast)
        try:
            self.server.get_database_information(db=db_name)
        except ApiException as e:
            if e.code == 404:
                raise ConnectionError(f"Database {db_name} does not exist") from e
            raise

    def fetch_document_by_id(self, doc_id: str) -> dict[str, Any] | None:
        """
        Fetch a document from the database by its ID.

        Args:
            doc_id: The ID of the document to fetch.

        Returns:
            The retrieved document, or None if not found.
        """
        try:
            document = self.server.get_document(
                db=self.db_name, doc_id=doc_id
            ).get_result()
            if isinstance(document, dict):
                return document
            logger.warning("Unexpected non-dict response when fetching %s", doc_id)
            return None
        except ApiException as e:
            if e.code == 404:
                logger.debug(
                    "Document '%s' not found in database '%s'",
                    doc_id,
                    self.db_name,
                )
                return None
            logger.error(
                "Cloudant API error fetching '%s' from %s: %s %s",
                doc_id,
                self.db_name,
                e.code,
                e.message,
            )
            return None
        except Exception as e:
            logger.error("Error while accessing database %s: %s", self.db_name, e)
            return None

    def post_changes(
        self,
        *,
        since: str | int | None = None,
        include_docs: bool = True,
        limit: int = 100,
        feed: str | None = None,
        timeout_ms: int | None = None,
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
            feed: CouchDB feed mode (e.g. "normal", "longpoll", "continuous")
            timeout_ms: Optional request timeout in milliseconds

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

        request: dict[str, Any] = {
            "db": self.db_name,
            "since": since_str,
            "include_docs": include_docs,
            "limit": limit,
        }
        if feed is not None:
            request["feed"] = feed
        if timeout_ms is not None:
            request["timeout"] = timeout_ms

        response = self.server.post_changes(**request)
        result = response.get_result()
        if isinstance(result, dict):
            return result
        # Unexpected SDK response - fail explicitly
        raise TypeError(
            f"post_changes returned non-dict result: {type(result).__name__}"
        )

    def fetch_changes_batch(
        self,
        *,
        since: str | None = None,
        include_docs: bool = True,
        limit: int = 100,
        feed: str = "normal",
        timeout_ms: int | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """Fetch one _changes batch and return ``(results, last_seq)``.

        This is intentionally a single-request helper. It performs no polling,
        retries, sleeping, or checkpointing.
        """
        result = self.post_changes(
            since=since,
            include_docs=include_docs,
            limit=limit,
            feed=feed,
            timeout_ms=timeout_ms,
        )

        raw_results = result.get("results", []) if isinstance(result, dict) else []
        if isinstance(raw_results, list):
            results: list[dict[str, Any]] = [
                item for item in raw_results if isinstance(item, dict)
            ]
        else:
            results = []

        last_seq = result.get("last_seq") if isinstance(result, dict) else None
        last_seq_str = str(last_seq) if last_seq is not None else None
        return results, last_seq_str
