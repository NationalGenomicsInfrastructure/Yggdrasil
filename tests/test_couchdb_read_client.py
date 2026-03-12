"""Unit tests for yggdrasil.flow.data_access.CouchDBReadClient.

Tests cover limit clamping, delegation to CouchDBHandler.find_documents,
convenience methods (find_one, fetch_by_field, require, require_one),
DataAccessNotFoundError semantics, and DataAccessQueryError propagation.
All async tests use IsolatedAsyncioTestCase.
"""

import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from lib.core_utils.external_systems_resolver import DataAccessPolicy
from yggdrasil.flow.data_access.couchdb_read import CouchDBReadClient
from yggdrasil.flow.data_access.errors import (
    DataAccessNotFoundError,
    DataAccessQueryError,
)


def _make_client(
    mock_handler: MagicMock,
    *,
    max_limit: int = 200,
) -> CouchDBReadClient:
    """Build a CouchDBReadClient with a mock handler and given max_limit."""
    policy = DataAccessPolicy(realm_allowlist=["demux"], max_limit=max_limit)
    return CouchDBReadClient(handler=mock_handler, policy=policy)


class TestCouchDBReadClientFind(unittest.IsolatedAsyncioTestCase):
    """Tests for CouchDBReadClient.find() limit clamping and delegation."""

    async def test_find_uses_policy_max_limit_when_no_limit_given(self):
        """find() without limit uses policy.max_limit."""
        handler = MagicMock()
        handler.find_documents.return_value = []
        client = _make_client(handler, max_limit=50)

        with patch("asyncio.to_thread", new=AsyncMock(return_value=[])) as mock_thread:
            await client.find({"status": "active"})

        mock_thread.assert_called_once()
        _, call_kwargs = mock_thread.call_args
        # limit keyword arg
        self.assertEqual(call_kwargs.get("limit"), 50)

    async def test_find_clamps_limit_to_max_limit(self):
        """find(limit=1000) is clamped to policy.max_limit."""
        handler = MagicMock()
        client = _make_client(handler, max_limit=50)

        with patch("asyncio.to_thread", new=AsyncMock(return_value=[])) as mock_thread:
            await client.find({"status": "active"}, limit=1000)

        _, call_kwargs = mock_thread.call_args
        self.assertEqual(call_kwargs.get("limit"), 50)

    async def test_find_preserves_limit_below_max(self):
        """find(limit=10) keeps limit=10 when max_limit=200."""
        handler = MagicMock()
        client = _make_client(handler, max_limit=200)

        with patch("asyncio.to_thread", new=AsyncMock(return_value=[])) as mock_thread:
            await client.find({"status": "active"}, limit=10)

        _, call_kwargs = mock_thread.call_args
        self.assertEqual(call_kwargs.get("limit"), 10)

    async def test_find_returns_list_from_handler(self):
        """find() returns the list returned by CouchDBHandler.find_documents."""
        docs = [{"_id": "doc1"}, {"_id": "doc2"}]
        handler = MagicMock()
        client = _make_client(handler, max_limit=200)

        with patch("asyncio.to_thread", new=AsyncMock(return_value=docs)):
            result = await client.find({"status": "active"})

        self.assertEqual(result, docs)

    async def test_find_passes_selector_to_handler(self):
        """find() passes the selector to CouchDBHandler.find_documents."""
        handler = MagicMock()
        client = _make_client(handler, max_limit=200)
        selector = {"status": {"$eq": "ready"}}

        with patch("asyncio.to_thread", new=AsyncMock(return_value=[])) as mock_thread:
            await client.find(selector)

        mock_thread.assert_called_once()
        pos_args, _ = mock_thread.call_args
        # First positional arg is the callable, second is selector
        self.assertIn(selector, pos_args)


class TestCouchDBReadClientFindOne(unittest.IsolatedAsyncioTestCase):
    """Tests for CouchDBReadClient.find_one()."""

    async def test_find_one_calls_find_with_limit_1(self):
        """find_one() always calls find() with limit=1."""
        handler = MagicMock()
        client = _make_client(handler, max_limit=200)

        with patch.object(client, "find", new=AsyncMock(return_value=[])) as mock_find:
            await client.find_one({"status": "active"})

        mock_find.assert_called_once_with({"status": "active"}, limit=1)

    async def test_find_one_returns_first_doc(self):
        """find_one() returns first element of find() result."""
        handler = MagicMock()
        client = _make_client(handler, max_limit=200)
        docs = [{"_id": "doc1"}, {"_id": "doc2"}]

        with patch.object(client, "find", new=AsyncMock(return_value=docs)):
            result = await client.find_one({})

        self.assertEqual(result, {"_id": "doc1"})

    async def test_find_one_returns_none_when_empty(self):
        """find_one() returns None when find() returns empty list."""
        handler = MagicMock()
        client = _make_client(handler, max_limit=200)

        with patch.object(client, "find", new=AsyncMock(return_value=[])):
            result = await client.find_one({})

        self.assertIsNone(result)


class TestCouchDBReadClientFetchByField(unittest.IsolatedAsyncioTestCase):
    """Tests for CouchDBReadClient.fetch_by_field()."""

    async def test_fetch_by_field_builds_equality_selector(self):
        """fetch_by_field builds {field: {"$eq": value}} selector."""
        handler = MagicMock()
        client = _make_client(handler, max_limit=200)

        with patch.object(client, "find", new=AsyncMock(return_value=[])) as mock_find:
            await client.fetch_by_field("flowcell_id", "X")

        expected_selector = {"flowcell_id": {"$eq": "X"}}
        mock_find.assert_called_once_with(expected_selector, limit=None)

    async def test_fetch_by_field_passes_limit(self):
        """fetch_by_field passes limit kwarg to find()."""
        handler = MagicMock()
        client = _make_client(handler, max_limit=200)

        with patch.object(client, "find", new=AsyncMock(return_value=[])) as mock_find:
            await client.fetch_by_field("status", "active", limit=5)

        _, call_kwargs = mock_find.call_args
        self.assertEqual(call_kwargs.get("limit"), 5)

    async def test_fetch_by_field_returns_result(self):
        """fetch_by_field returns the list from find()."""
        handler = MagicMock()
        client = _make_client(handler, max_limit=200)
        docs = [{"_id": "x", "flowcell_id": "X"}]

        with patch.object(client, "find", new=AsyncMock(return_value=docs)):
            result = await client.fetch_by_field("flowcell_id", "X")

        self.assertEqual(result, docs)


class TestCouchDBReadClientGet(unittest.IsolatedAsyncioTestCase):
    """Tests for CouchDBReadClient.get()."""

    async def test_get_fetches_document(self):
        """get() returns the document from fetch_document_by_id."""
        handler = MagicMock()
        doc = {"_id": "doc1", "status": "ready"}
        client = _make_client(handler, max_limit=200)

        with patch("asyncio.to_thread", new=AsyncMock(return_value=doc)):
            result = await client.get("doc1")

        self.assertEqual(result, doc)

    async def test_get_returns_none_when_not_found(self):
        """get() returns None when handler returns None."""
        handler = MagicMock()
        client = _make_client(handler, max_limit=200)

        with patch("asyncio.to_thread", new=AsyncMock(return_value=None)):
            result = await client.get("missing_doc")

        self.assertIsNone(result)


class TestCouchDBReadClientRequire(unittest.IsolatedAsyncioTestCase):
    """Tests for CouchDBReadClient.require() and require_one()."""

    async def test_require_returns_doc_when_found(self):
        """require() returns document when get() succeeds."""
        handler = MagicMock()
        doc = {"_id": "doc1"}
        client = _make_client(handler, max_limit=200)

        with patch.object(client, "get", new=AsyncMock(return_value=doc)):
            result = await client.require("doc1")

        self.assertEqual(result, doc)

    async def test_require_raises_when_not_found(self):
        """require() raises DataAccessNotFoundError when get() returns None."""
        handler = MagicMock()
        client = _make_client(handler, max_limit=200)

        with patch.object(client, "get", new=AsyncMock(return_value=None)):
            with self.assertRaises(DataAccessNotFoundError) as ctx:
                await client.require("missing")

        self.assertIn("missing", str(ctx.exception))

    async def test_require_one_returns_doc_when_found(self):
        """require_one() returns document when find_one() succeeds."""
        handler = MagicMock()
        doc = {"_id": "doc1"}
        client = _make_client(handler, max_limit=200)
        selector = {"status": "active"}

        with patch.object(client, "find_one", new=AsyncMock(return_value=doc)):
            result = await client.require_one(selector)

        self.assertEqual(result, doc)

    async def test_require_one_raises_when_no_match(self):
        """require_one() raises DataAccessNotFoundError when find_one() returns None."""
        handler = MagicMock()
        client = _make_client(handler, max_limit=200)
        selector = {"status": "nonexistent"}

        with patch.object(client, "find_one", new=AsyncMock(return_value=None)):
            with self.assertRaises(DataAccessNotFoundError) as ctx:
                await client.require_one(selector)

        self.assertIn("nonexistent", str(ctx.exception))


# ---------------------------------------------------------------------------
# TestCouchDBReadClientBlocking — sync _blocking methods
# ---------------------------------------------------------------------------


class TestCouchDBReadClientBlocking(unittest.TestCase):
    """Tests for CouchDBReadClient blocking (sync) methods."""

    def test_get_blocking_returns_doc(self):
        """get_blocking() returns the document from handler.fetch_document_by_id."""
        handler = MagicMock()
        doc = {"_id": "doc1", "status": "ready"}
        handler.fetch_document_by_id.return_value = doc
        client = _make_client(handler, max_limit=200)

        result = client.get_blocking("doc1")

        self.assertEqual(result, doc)
        handler.fetch_document_by_id.assert_called_once_with("doc1")

    def test_get_blocking_returns_none_when_not_found(self):
        """get_blocking() returns None when handler returns None."""
        handler = MagicMock()
        handler.fetch_document_by_id.return_value = None
        client = _make_client(handler, max_limit=200)

        result = client.get_blocking("missing")

        self.assertIsNone(result)

    def test_find_blocking_clamps_limit_to_max_limit(self):
        """find_blocking(limit=1000) is clamped to policy.max_limit."""
        handler = MagicMock()
        handler.find_documents.return_value = []
        client = _make_client(handler, max_limit=50)

        client.find_blocking({"type": "x"}, limit=1000)

        _, call_kwargs = handler.find_documents.call_args
        self.assertEqual(call_kwargs["limit"], 50)

    def test_find_blocking_uses_max_limit_when_no_limit(self):
        """find_blocking() without limit uses policy.max_limit."""
        handler = MagicMock()
        handler.find_documents.return_value = []
        client = _make_client(handler, max_limit=75)

        client.find_blocking({"type": "x"})

        _, call_kwargs = handler.find_documents.call_args
        self.assertEqual(call_kwargs["limit"], 75)

    def test_find_blocking_preserves_limit_below_max(self):
        """find_blocking(limit=5) keeps limit=5 when max_limit=200."""
        handler = MagicMock()
        handler.find_documents.return_value = []
        client = _make_client(handler, max_limit=200)

        client.find_blocking({"type": "x"}, limit=5)

        _, call_kwargs = handler.find_documents.call_args
        self.assertEqual(call_kwargs["limit"], 5)

    def test_find_blocking_returns_docs(self):
        """find_blocking() returns the list from handler.find_documents."""
        docs = [{"_id": "a"}, {"_id": "b"}]
        handler = MagicMock()
        handler.find_documents.return_value = docs
        client = _make_client(handler, max_limit=200)

        result = client.find_blocking({"type": "x"})

        self.assertEqual(result, docs)

    def test_find_one_blocking_calls_find_with_limit_1(self):
        """find_one_blocking() calls find_blocking with limit=1."""
        handler = MagicMock()
        handler.find_documents.return_value = []
        client = _make_client(handler, max_limit=200)

        client.find_one_blocking({"type": "x"})

        _, call_kwargs = handler.find_documents.call_args
        self.assertEqual(call_kwargs["limit"], 1)

    def test_find_one_blocking_returns_first_doc(self):
        """find_one_blocking() returns first element when results exist."""
        docs = [{"_id": "first"}, {"_id": "second"}]
        handler = MagicMock()
        handler.find_documents.return_value = docs
        client = _make_client(handler, max_limit=200)

        result = client.find_one_blocking({"type": "x"})

        self.assertEqual(result, {"_id": "first"})

    def test_find_one_blocking_returns_none_when_empty(self):
        """find_one_blocking() returns None when find_blocking returns empty list."""
        handler = MagicMock()
        handler.find_documents.return_value = []
        client = _make_client(handler, max_limit=200)

        result = client.find_one_blocking({"type": "x"})

        self.assertIsNone(result)

    def test_fetch_by_field_blocking_builds_equality_selector(self):
        """fetch_by_field_blocking builds {field: {"$eq": value}} selector."""
        handler = MagicMock()
        handler.find_documents.return_value = []
        client = _make_client(handler, max_limit=200)

        client.fetch_by_field_blocking("flowcell_id", "X")

        pos_args, _ = handler.find_documents.call_args
        self.assertIn({"flowcell_id": {"$eq": "X"}}, pos_args)

    def test_fetch_by_field_blocking_passes_limit(self):
        """fetch_by_field_blocking passes limit through to find_blocking."""
        handler = MagicMock()
        handler.find_documents.return_value = []
        client = _make_client(handler, max_limit=200)

        client.fetch_by_field_blocking("status", "active", limit=3)

        _, call_kwargs = handler.find_documents.call_args
        self.assertEqual(call_kwargs["limit"], 3)

    def test_require_blocking_returns_doc_when_found(self):
        """require_blocking() returns doc when handler finds it."""
        doc = {"_id": "doc1"}
        handler = MagicMock()
        handler.fetch_document_by_id.return_value = doc
        client = _make_client(handler, max_limit=200)

        result = client.require_blocking("doc1")

        self.assertEqual(result, doc)

    def test_require_blocking_raises_when_not_found(self):
        """require_blocking() raises DataAccessNotFoundError when doc is None."""
        handler = MagicMock()
        handler.fetch_document_by_id.return_value = None
        client = _make_client(handler, max_limit=200)

        with self.assertRaises(DataAccessNotFoundError) as ctx:
            client.require_blocking("missing")

        self.assertIn("missing", str(ctx.exception))

    def test_require_one_blocking_returns_doc_when_found(self):
        """require_one_blocking() returns doc when a match exists."""
        doc = {"_id": "doc1"}
        handler = MagicMock()
        handler.find_documents.return_value = [doc]
        client = _make_client(handler, max_limit=200)

        result = client.require_one_blocking({"status": "active"})

        self.assertEqual(result, doc)

    def test_require_one_blocking_raises_when_no_match(self):
        """require_one_blocking() raises DataAccessNotFoundError when no match."""
        handler = MagicMock()
        handler.find_documents.return_value = []
        client = _make_client(handler, max_limit=200)
        selector = {"status": "nonexistent"}

        with self.assertRaises(DataAccessNotFoundError) as ctx:
            client.require_one_blocking(selector)

        self.assertIn("nonexistent", str(ctx.exception))


# ---------------------------------------------------------------------------
# TestCouchDBReadClientQueryErrors — async error propagation
# ---------------------------------------------------------------------------


class TestCouchDBReadClientQueryErrors(unittest.IsolatedAsyncioTestCase):
    """Tests that DataAccessQueryError is raised when the handler raises."""

    async def test_get_raises_query_error_on_handler_failure(self):
        """get() wraps known CouchDB/network exceptions as DataAccessQueryError."""
        handler = MagicMock()
        client = _make_client(handler, max_limit=200)

        with patch(
            "asyncio.to_thread",
            new=AsyncMock(side_effect=ConnectionError("conn refused")),
        ):
            with self.assertRaises(DataAccessQueryError) as ctx:
                await client.get("doc1")

        self.assertIn("doc1", str(ctx.exception))

    async def test_find_raises_query_error_on_handler_failure(self):
        """find() wraps known CouchDB/network exceptions as DataAccessQueryError."""
        handler = MagicMock()
        client = _make_client(handler, max_limit=200)

        with patch(
            "asyncio.to_thread",
            new=AsyncMock(side_effect=ConnectionError("server 500")),
        ):
            with self.assertRaises(DataAccessQueryError):
                await client.find({"status": "active"})

    async def test_get_type_error_bubbles_through(self):
        """get() does NOT wrap TypeError; programmer mistakes stay visible."""
        handler = MagicMock()
        client = _make_client(handler, max_limit=200)

        with patch(
            "asyncio.to_thread", new=AsyncMock(side_effect=TypeError("wrong type"))
        ):
            with self.assertRaises(TypeError):
                await client.get("doc1")

    async def test_find_type_error_bubbles_through(self):
        """find() does NOT wrap TypeError; programmer mistakes stay visible."""
        handler = MagicMock()
        client = _make_client(handler, max_limit=200)

        with patch(
            "asyncio.to_thread", new=AsyncMock(side_effect=TypeError("bad selector"))
        ):
            with self.assertRaises(TypeError):
                await client.find({"status": "active"})

    async def test_find_one_propagates_query_error(self):
        """find_one() propagates DataAccessQueryError from find()."""
        handler = MagicMock()
        client = _make_client(handler, max_limit=200)

        with patch.object(
            client, "find", new=AsyncMock(side_effect=DataAccessQueryError("failed"))
        ):
            with self.assertRaises(DataAccessQueryError):
                await client.find_one({})

    async def test_require_propagates_query_error_from_get(self):
        """require() propagates DataAccessQueryError from get(); does not raise NotFoundError."""
        handler = MagicMock()
        client = _make_client(handler, max_limit=200)

        with patch.object(
            client, "get", new=AsyncMock(side_effect=DataAccessQueryError("failed"))
        ):
            with self.assertRaises(DataAccessQueryError):
                await client.require("doc1")

    async def test_require_one_propagates_query_error_from_find_one(self):
        """require_one() propagates DataAccessQueryError from find_one()."""
        handler = MagicMock()
        client = _make_client(handler, max_limit=200)

        with patch.object(
            client,
            "find_one",
            new=AsyncMock(side_effect=DataAccessQueryError("failed")),
        ):
            with self.assertRaises(DataAccessQueryError):
                await client.require_one({"status": "active"})

    async def test_query_error_and_not_found_error_are_distinct(self):
        """DataAccessQueryError and DataAccessNotFoundError are separate exception types."""
        self.assertFalse(issubclass(DataAccessQueryError, DataAccessNotFoundError))
        self.assertFalse(issubclass(DataAccessNotFoundError, DataAccessQueryError))


# ---------------------------------------------------------------------------
# TestCouchDBReadClientBlockingQueryErrors — sync error propagation
# ---------------------------------------------------------------------------


class TestCouchDBReadClientBlockingQueryErrors(unittest.TestCase):
    """Tests that DataAccessQueryError is raised by _blocking methods when handler raises."""

    def test_get_blocking_raises_query_error_on_handler_failure(self):
        """get_blocking() wraps known CouchDB/network exceptions as DataAccessQueryError."""
        handler = MagicMock()
        handler.fetch_document_by_id.side_effect = ConnectionError("conn refused")
        client = _make_client(handler, max_limit=200)

        with self.assertRaises(DataAccessQueryError) as ctx:
            client.get_blocking("doc1")

        self.assertIn("doc1", str(ctx.exception))

    def test_find_blocking_raises_query_error_on_handler_failure(self):
        """find_blocking() wraps known CouchDB/network exceptions as DataAccessQueryError."""
        handler = MagicMock()
        handler.find_documents.side_effect = ConnectionError("server 500")
        client = _make_client(handler, max_limit=200)

        with self.assertRaises(DataAccessQueryError):
            client.find_blocking({"type": "x"})

    def test_find_one_blocking_propagates_query_error(self):
        """find_one_blocking() propagates DataAccessQueryError from find_blocking()."""
        handler = MagicMock()
        handler.find_documents.side_effect = ConnectionError("server 500")
        client = _make_client(handler, max_limit=200)

        with self.assertRaises(DataAccessQueryError):
            client.find_one_blocking({"type": "x"})

    def test_fetch_by_field_blocking_propagates_query_error(self):
        """fetch_by_field_blocking() propagates DataAccessQueryError from find_blocking()."""
        handler = MagicMock()
        handler.find_documents.side_effect = ConnectionError("server 500")
        client = _make_client(handler, max_limit=200)

        with self.assertRaises(DataAccessQueryError):
            client.fetch_by_field_blocking("status", "active")

    def test_require_blocking_propagates_query_error_from_get_blocking(self):
        """require_blocking() propagates DataAccessQueryError; not DataAccessNotFoundError."""
        handler = MagicMock()
        handler.fetch_document_by_id.side_effect = ConnectionError("server 500")
        client = _make_client(handler, max_limit=200)

        with self.assertRaises(DataAccessQueryError):
            client.require_blocking("doc1")

    def test_require_one_blocking_propagates_query_error(self):
        """require_one_blocking() propagates DataAccessQueryError from find_one_blocking()."""
        handler = MagicMock()
        handler.find_documents.side_effect = ConnectionError("server 500")
        client = _make_client(handler, max_limit=200)

        with self.assertRaises(DataAccessQueryError):
            client.require_one_blocking({"status": "active"})

    def test_get_blocking_type_error_bubbles_through(self):
        """get_blocking() does NOT wrap TypeError; programmer mistakes stay visible."""
        handler = MagicMock()
        handler.fetch_document_by_id.side_effect = TypeError("wrong type")
        client = _make_client(handler, max_limit=200)

        with self.assertRaises(TypeError):
            client.get_blocking("doc1")

    def test_find_blocking_type_error_bubbles_through(self):
        """find_blocking() does NOT wrap TypeError; programmer mistakes stay visible."""
        handler = MagicMock()
        handler.find_documents.side_effect = TypeError("bad selector")
        client = _make_client(handler, max_limit=200)

        with self.assertRaises(TypeError):
            client.find_blocking({"type": "x"})


if __name__ == "__main__":
    unittest.main()
