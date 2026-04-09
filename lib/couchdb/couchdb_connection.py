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

import logging
import os
import threading
from typing import Any

import requests
from ibm_cloud_sdk_core.api_exception import ApiException
from ibmcloudant import CouchDbSessionAuthenticator, cloudant_v1

from lib.core_utils.logging_utils import custom_logger
from lib.couchdb.couchdb_models import ChangesBatch, ChangesRow

logger = custom_logger(__name__)


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
    _logged_lock = threading.Lock()

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
            client.enable_retries(max_retries=3, retry_interval=5.0)

            # Verify connection (fail fast)
            if verify_connection:
                info = client.get_server_information().get_result() or {}
                if isinstance(info, dict):
                    version = str(info.get("version", "unknown"))
                else:
                    version = "unknown"

                conn_key = (url, user)

                with CouchDBClientFactory._logged_lock:
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
        logger: logging.Logger | None = None,
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
        self._logger = logger or custom_logger(f"{__name__}.{type(self).__name__}")
        self.db_name = db_name

        # Create client via factory (each handler owns its client)
        self.server = CouchDBClientFactory.create_client(
            url=url,
            user_env=user_env,
            pass_env=pass_env,
        )

        # Store URL and credentials for raw HTTP requests (validated by factory above)
        self._url: str = url.rstrip("/")
        self._auth: tuple[str, str] = (
            os.environ[user_env],
            os.environ[pass_env],
        )

        # Verify database exists (fail fast)
        try:
            self.server.get_database_information(db=db_name)
        except ApiException as e:
            if e.status_code == 404:
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
            self._logger.warning(
                "Unexpected non-dict response when fetching %s", doc_id
            )
            return None
        except ApiException as e:
            if e.status_code == 404:
                self._logger.debug(
                    "Document '%s' not found in database '%s'",
                    doc_id,
                    self.db_name,
                )
                return None
            self._logger.error(
                "Cloudant API error fetching '%s' from %s: %s %s",
                doc_id,
                self.db_name,
                e.status_code,
                e.message,
            )
            raise
        except Exception as e:
            self._logger.error("Error while accessing database %s: %s", self.db_name, e)
            raise

    def find_documents(
        self,
        selector: dict[str, Any],
        *,
        fields: list[str] | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        """
        Run a Mango selector query against this database.

        Uses the cloudant SDK post_find() endpoint.

        Args:
            selector: Mango query selector dict (e.g. {"status": {"$eq": "ready"}})
            fields: Optional list of field names to include in results.
                    If None or empty, all fields are returned.
            limit: Maximum number of documents to return (default: 200)

        Returns:
            List of matching documents. Empty list if no matches.
        """
        try:
            response = self.server.post_find(
                db=self.db_name,
                selector=selector,
                fields=fields or [],
                limit=limit,
            )
            result = response.get_result()
            if not isinstance(result, dict):
                self._logger.warning(
                    "Unexpected non-dict response from post_find on '%s'", self.db_name
                )
                return []
            docs = result.get("docs", [])
            return docs if isinstance(docs, list) else []
        except ApiException as e:
            self._logger.error(
                "Cloudant API error in find_documents on '%s': %s %s",
                self.db_name,
                e.status_code,
                e.message,
            )
            raise
        except Exception as e:
            self._logger.error(
                "Error in find_documents on database '%s': %s", self.db_name, e
            )
            raise

    def fetch_changes_raw(
        self,
        *,
        since: str | int | None = None,
        feed: str = "normal",
        limit: int | None = None,
        timeout_ms: int = 30_000,
    ) -> ChangesBatch:
        """Fetch one ``_changes`` batch via a raw HTTP GET request.

        Uses ``requests`` directly (not the IBM SDK) so that exception types are
        predictable and classifiable by :func:`is_transient_poll_error`.

        Args:
            since:      Resume token.  ``None`` is sent as ``"0"`` (from the start).
            feed:       CouchDB feed mode — ``"normal"`` or ``"longpoll"``.
            limit:      Maximum rows to return.  ``None`` omits the parameter.
            timeout_ms: CouchDB-internal timeout for longpoll.  A socket-level timeout
                        of ``timeout_ms / 1000 + 5`` is applied to the HTTP request,
                        giving CouchDB time to respond before the socket closes.

        Returns:
            Parsed :class:`ChangesBatch`.

        Raises:
            requests.exceptions.Timeout:         Socket/read timeout.
            requests.exceptions.ConnectionError: Network-level failure.
            requests.exceptions.HTTPError:       Non-2xx HTTP response (after
                                                 ``raise_for_status()``).
            ValueError / KeyError:               Malformed JSON response.
        """
        url = f"{self._url}/{self.db_name}/_changes"
        params: dict[str, Any] = {
            "feed": feed,
            "since": since if since is not None else "0",
            "include_docs": "false",
        }
        if limit is not None:
            params["limit"] = limit
        if feed == "longpoll":
            # Pass CouchDB's own timeout so it returns before the socket closes
            params["timeout"] = timeout_ms

        socket_timeout = timeout_ms / 1000 + 5  # 5 s margin beyond CouchDB timeout
        response = requests.get(
            url,
            params=params,
            auth=self._auth,
            timeout=socket_timeout,
        )
        response.raise_for_status()

        data: dict[str, Any] = response.json()
        rows: list[ChangesRow] = []
        for r in data.get("results", []):
            changes_list = r.get("changes")
            rev = changes_list[0]["rev"] if changes_list else None
            rows.append(
                ChangesRow(
                    id=r["id"],
                    seq=r["seq"],
                    deleted=r.get("deleted", False),
                    rev=rev,
                )
            )
        return ChangesBatch(
            rows=rows,
            last_seq=data.get("last_seq"),
            pending=int(data.get("pending", 0)),
        )


def is_transient_poll_error(exc: Exception) -> bool:
    """Return True for transient ``_changes`` polling failures (requests-based).

    Used **only** for errors raised by :meth:`CouchDBHandler.fetch_changes_raw`,
    which uses the ``requests`` library directly.

    Classification:
    - ``requests.Timeout`` / ``ConnectionError`` → transient (network)
    - HTTP 5xx → transient (server-side)
    - HTTP 4xx (other), parse errors → permanent
    """
    if isinstance(
        exc, requests.exceptions.Timeout | requests.exceptions.ConnectionError
    ):
        return True
    if isinstance(exc, requests.exceptions.HTTPError):
        status = exc.response.status_code if exc.response is not None else None
        return status is not None and status >= 500
    return False


def is_transient_doc_fetch_error(exc: Exception) -> bool:
    """Return True for transient document fetch failures (IBM SDK-based).

    Used **only** for errors raised by :meth:`CouchDBHandler.fetch_document_by_id`,
    which uses the IBM CloudantV1 SDK.

    Note: 404 is *not* raised by ``fetch_document_by_id`` — it returns ``None`` instead.
    So this function will never be called for a 404.

    Classification:
    - ``ApiException`` with 5xx or 429 → transient
    - ``ApiException`` with 4xx other → permanent
    - ``requests.Timeout`` / ``ConnectionError`` → transient (defensive; SDK may surface these)
    - Other exceptions → permanent
    """
    # Duck-type check: any exception with an integer status_code is treated as
    # an API-style error (covers real ApiException and test mocks alike).
    status = getattr(exc, "status_code", None)
    if isinstance(status, int):
        return status >= 500 or status == 429
    # Defensive: IBM SDK may surface network failures as requests exceptions in some versions
    if isinstance(
        exc, requests.exceptions.Timeout | requests.exceptions.ConnectionError
    ):
        return True
    return False
