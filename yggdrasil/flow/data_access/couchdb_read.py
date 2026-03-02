"""CouchDB read-only client for realm data access."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from ibm_cloud_sdk_core.api_exception import ApiException
from requests.exceptions import RequestException
from urllib3.exceptions import HTTPError as Urllib3HTTPError

from yggdrasil.flow.data_access.errors import (
    DataAccessNotFoundError,
    DataAccessQueryError,
)

if TYPE_CHECKING:
    from lib.core_utils.external_systems_resolver import DataAccessPolicy
    from lib.couchdb.couchdb_connection import CouchDBHandler

# Exceptions that represent CouchDB / network infrastructure failures.
# Only these are wrapped as DataAccessQueryError; all others (TypeError,
# AttributeError, etc.) bubble up so programmer mistakes stay visible.
# NOTE: Add OSError / ConnectionResetError / TimeoutError if we see them in practice.
_QUERY_EXCEPTIONS = (ApiException, RequestException, ConnectionError, Urllib3HTTPError)


class CouchDBReadClient:
    """Read-only async CouchDB client for a single database.

    Wraps :class:`CouchDBHandler` (synchronous cloudant SDK) using
    ``asyncio.to_thread()`` so that callers in the async event loop
    are not blocked.

    All query limits are clamped to ``policy.max_limit`` to enforce
    the data access policy configured per connection.

    Args:
        handler: Synchronous CouchDB handler for one database.
        policy: Data access policy governing query limits.
    """

    def __init__(self, handler: CouchDBHandler, policy: DataAccessPolicy) -> None:
        self._handler = handler
        self._policy = policy

    async def get(self, doc_id: str) -> dict[str, Any] | None:
        """Fetch a single document by ID.

        Args:
            doc_id: CouchDB document ID.

        Returns:
            The document dict, or None if not found.

        Raises:
            DataAccessQueryError: If the CouchDB request fails (non-404 error).
        """
        try:
            return await asyncio.to_thread(self._handler.fetch_document_by_id, doc_id)
        except _QUERY_EXCEPTIONS as exc:
            raise DataAccessQueryError(
                f"Failed to fetch document '{doc_id}': {exc}"
            ) from exc

    async def find(
        self,
        selector: dict[str, Any],
        *,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Run a Mango selector query.

        Args:
            selector: Mango query selector dict.
            limit: Maximum results to return. Clamped to ``policy.max_limit``.
                   If None, ``policy.max_limit`` is used directly.

        Returns:
            List of matching documents. Empty list if no matches.

        Raises:
            DataAccessQueryError: If the CouchDB request fails.
        """
        effective_limit = (
            min(limit, self._policy.max_limit)
            if limit is not None
            else self._policy.max_limit
        )
        try:
            return await asyncio.to_thread(
                self._handler.find_documents, selector, limit=effective_limit
            )
        except _QUERY_EXCEPTIONS as exc:
            raise DataAccessQueryError(f"Query failed: {exc}") from exc

    async def find_one(self, selector: dict[str, Any]) -> dict[str, Any] | None:
        """Return the first matching document or None.

        Always executes with ``limit=1`` internally.

        Args:
            selector: Mango query selector dict.

        Returns:
            First matching document, or None if no match.
        """
        results = await self.find(selector, limit=1)
        return results[0] if results else None

    async def fetch_by_field(
        self,
        field: str,
        value: Any,
        *,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Find documents where ``field == value``.

        Convenience wrapper that builds a simple equality selector.

        Args:
            field: Field name to match.
            value: Value the field must equal.
            limit: Optional limit (clamped to policy.max_limit).

        Returns:
            List of matching documents.
        """
        return await self.find({field: {"$eq": value}}, limit=limit)

    async def require(self, doc_id: str) -> dict[str, Any]:
        """Fetch a document by ID; raise if not found.

        Args:
            doc_id: CouchDB document ID.

        Returns:
            The document dict.

        Raises:
            DataAccessNotFoundError: If the document does not exist.
        """
        doc = await self.get(doc_id)
        if doc is None:
            raise DataAccessNotFoundError(f"Document '{doc_id}' not found")
        return doc

    async def require_one(self, selector: dict[str, Any]) -> dict[str, Any]:
        """Run a Mango query and raise if no document matches.

        Args:
            selector: Mango query selector dict.

        Returns:
            First matching document.

        Raises:
            DataAccessNotFoundError: If no document matches the selector.
        """
        doc = await self.find_one(selector)
        if doc is None:
            raise DataAccessNotFoundError(f"No document matched selector {selector!r}")
        return doc

    # ------------------------------------------------------------------
    # Blocking (synchronous) equivalents — for use in sync step functions
    # ------------------------------------------------------------------

    def get_blocking(self, doc_id: str) -> dict[str, Any] | None:
        """Fetch a single document by ID synchronously.

        Calls the handler directly without going through the event loop.
        Use this inside plain ``def`` step functions.

        Args:
            doc_id: CouchDB document ID.

        Returns:
            The document dict, or None if not found.

        Raises:
            DataAccessQueryError: If the CouchDB request fails (non-404 error).
        """
        try:
            return self._handler.fetch_document_by_id(doc_id)
        except _QUERY_EXCEPTIONS as exc:
            raise DataAccessQueryError(
                f"Failed to fetch document '{doc_id}': {exc}"
            ) from exc

    def find_blocking(
        self,
        selector: dict[str, Any],
        *,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Run a Mango selector query synchronously.

        Enforces ``policy.max_limit`` the same way as the async ``find``.

        Args:
            selector: Mango query selector dict.
            limit: Maximum results to return. Clamped to ``policy.max_limit``.
                   If None, ``policy.max_limit`` is used directly.

        Returns:
            List of matching documents. Empty list if no matches.

        Raises:
            DataAccessQueryError: If the CouchDB request fails.
        """
        effective_limit = (
            min(limit, self._policy.max_limit)
            if limit is not None
            else self._policy.max_limit
        )
        try:
            return self._handler.find_documents(selector, limit=effective_limit)
        except _QUERY_EXCEPTIONS as exc:
            raise DataAccessQueryError(f"Query failed: {exc}") from exc

    def find_one_blocking(self, selector: dict[str, Any]) -> dict[str, Any] | None:
        """Return the first matching document or None (synchronous).

        Always executes with ``limit=1`` internally.

        Args:
            selector: Mango query selector dict.

        Returns:
            First matching document, or None if no match.
        """
        results = self.find_blocking(selector, limit=1)
        return results[0] if results else None

    def fetch_by_field_blocking(
        self,
        field: str,
        value: Any,
        *,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Find documents where ``field == value`` (synchronous).

        Args:
            field: Field name to match.
            value: Value the field must equal.
            limit: Optional limit (clamped to policy.max_limit).

        Returns:
            List of matching documents.
        """
        return self.find_blocking({field: {"$eq": value}}, limit=limit)

    def require_blocking(self, doc_id: str) -> dict[str, Any]:
        """Fetch a document by ID synchronously; raise if not found.

        Args:
            doc_id: CouchDB document ID.

        Returns:
            The document dict.

        Raises:
            DataAccessNotFoundError: If the document does not exist.
        """
        doc = self.get_blocking(doc_id)
        if doc is None:
            raise DataAccessNotFoundError(f"Document '{doc_id}' not found")
        return doc

    def require_one_blocking(self, selector: dict[str, Any]) -> dict[str, Any]:
        """Run a Mango query synchronously and raise if no document matches.

        Args:
            selector: Mango query selector dict.

        Returns:
            First matching document.

        Raises:
            DataAccessNotFoundError: If no document matches the selector.
        """
        doc = self.find_one_blocking(selector)
        if doc is None:
            raise DataAccessNotFoundError(f"No document matched selector {selector!r}")
        return doc
