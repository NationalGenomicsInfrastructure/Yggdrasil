"""Unit tests for CouchDB data clients (planning + execution).

All tests use a mock CouchDBHandler — no real CouchDB required.
"""

import unittest
from unittest.mock import MagicMock

from ibm_cloud_sdk_core.api_exception import ApiException

from yggdrasil.flow.data_access.couchdb_data import (
    CouchDBExecutionClient,
    CouchDBPlanningClient,
    _CouchDBSyncOps,
    _CouchDBWriteOps,
)
from yggdrasil.flow.data_access.errors import (
    DataAccessDeniedError,
    DataAccessNotFoundError,
    DataAccessWriteError,
)
from yggdrasil.flow.data_access.models import (
    DataAccessTraceContext,
    DataAccessWriteResult,
)

# ---------------------------------------------------------------------------
# Setup helpers
# ---------------------------------------------------------------------------


def make_handler_mock(*, get_result=None, find_result=None, put_result=None):
    handler = MagicMock()
    handler.fetch_document_by_id.return_value = get_result
    handler.find_documents.return_value = find_result or []
    handler.put_document.return_value = put_result or {
        "id": "x",
        "rev": "1-abc",
        "ok": True,
    }
    return handler


def make_planning_client(handler, options=None):
    ops = _CouchDBSyncOps(handler, options or {})
    return CouchDBPlanningClient(ops)


def make_execution_client(
    handler,
    *,
    permissions=frozenset({"read", "write"}),
    options=None,
    trace_context=None,
):
    ops = _CouchDBSyncOps(handler, options or {})
    write_ops = _CouchDBWriteOps(handler)
    return CouchDBExecutionClient(
        ops=ops,
        write_ops=write_ops,
        permissions=permissions,
        realm_id="test_realm",
        connection_name="test_db",
        resource="test_db",
        trace_context=trace_context,
    )


def make_api_exception(status_code: int) -> ApiException:
    exc = ApiException(status_code, message=f"HTTP {status_code}")
    return exc


# ---------------------------------------------------------------------------
# CouchDBPlanningClient
# ---------------------------------------------------------------------------


class TestCouchDBPlanningClient(unittest.IsolatedAsyncioTestCase):
    """Async tests for CouchDBPlanningClient."""

    async def test_get_returns_doc(self):
        handler = make_handler_mock(get_result={"_id": "x"})
        client = make_planning_client(handler)
        result = await client.get("x")
        self.assertEqual(result, {"_id": "x"})

    async def test_get_returns_none_if_not_found(self):
        handler = make_handler_mock(get_result=None)
        client = make_planning_client(handler)
        result = await client.get("x")
        self.assertIsNone(result)

    async def test_find_clamps_limit_to_max_limit(self):
        handler = make_handler_mock(find_result=[])
        client = make_planning_client(handler, options={"max_limit": 10})
        await client.find({"type": "run"}, limit=100)
        handler.find_documents.assert_called_once_with({"type": "run"}, limit=10)

    async def test_find_uses_max_limit_when_no_limit_given(self):
        handler = make_handler_mock(find_result=[])
        client = make_planning_client(handler, options={"max_limit": 5})
        await client.find({"type": "run"})
        handler.find_documents.assert_called_once_with({"type": "run"}, limit=5)

    async def test_require_raises_not_found(self):
        handler = make_handler_mock(get_result=None)
        client = make_planning_client(handler)
        with self.assertRaises(DataAccessNotFoundError):
            await client.require("x")

    async def test_require_returns_doc_when_found(self):
        handler = make_handler_mock(get_result={"_id": "x", "val": 1})
        client = make_planning_client(handler)
        doc = await client.require("x")
        self.assertEqual(doc["val"], 1)

    def test_planning_client_has_no_put_method(self):
        handler = make_handler_mock()
        client = make_planning_client(handler)
        self.assertFalse(hasattr(client, "put"))


# ---------------------------------------------------------------------------
# CouchDBExecutionClient — reads
# ---------------------------------------------------------------------------


class TestCouchDBExecutionClientReads(unittest.TestCase):
    """Tests for CouchDBExecutionClient sync read methods."""

    def test_get_is_synchronous(self):
        """get() returns a plain dict, not a coroutine."""
        import inspect

        handler = make_handler_mock(get_result={"_id": "x"})
        client = make_execution_client(handler, permissions=frozenset({"read"}))
        result = client.get("x")
        self.assertFalse(inspect.iscoroutine(result))
        self.assertEqual(result, {"_id": "x"})

    def test_get_denied_without_read_permission(self):
        handler = make_handler_mock()
        client = make_execution_client(handler, permissions=frozenset({"write"}))
        with self.assertRaises(DataAccessDeniedError):
            client.get("x")

    def test_find_denied_without_read_permission(self):
        handler = make_handler_mock()
        client = make_execution_client(handler, permissions=frozenset({"write"}))
        with self.assertRaises(DataAccessDeniedError):
            client.find({})

    def test_get_succeeds_with_read_permission(self):
        handler = make_handler_mock(get_result={"_id": "x"})
        client = make_execution_client(handler, permissions=frozenset({"read"}))
        result = client.get("x")
        self.assertEqual(result, {"_id": "x"})

    def test_find_clamps_limit(self):
        handler = make_handler_mock(find_result=[])
        client = make_execution_client(
            handler, permissions=frozenset({"read"}), options={"max_limit": 5}
        )
        client.find({"status": "new"}, limit=100)
        handler.find_documents.assert_called_once_with({"status": "new"}, limit=5)

    def test_require_raises_not_found_when_absent(self):
        handler = make_handler_mock(get_result=None)
        client = make_execution_client(handler)
        with self.assertRaises(DataAccessNotFoundError):
            client.require("missing")


# ---------------------------------------------------------------------------
# CouchDBExecutionClient.put() — permission tests
# ---------------------------------------------------------------------------


class TestCouchDBExecutionClientPutPermissions(unittest.TestCase):
    def test_put_denied_without_write_permission(self):
        handler = make_handler_mock()
        client = make_execution_client(handler, permissions=frozenset({"read"}))
        with self.assertRaises(DataAccessDeniedError):
            client.put("x", {})

    def test_put_succeeds_with_only_write_permission_no_read(self):
        handler = make_handler_mock()
        client = make_execution_client(handler, permissions=frozenset({"write"}))
        # create mode: no pre-fetch needed
        result = client.put("x", {}, mode="create")
        self.assertIsInstance(result, DataAccessWriteResult)

    def test_upsert_succeeds_with_only_write_permission(self):
        """Internal _rev fetch must NOT require realm-level 'read' permission."""
        handler = make_handler_mock(get_result=None)  # absent → create path
        client = make_execution_client(handler, permissions=frozenset({"write"}))
        result = client.put("x", {}, mode="upsert")
        self.assertEqual(result.status, "created")


# ---------------------------------------------------------------------------
# CouchDBExecutionClient.put() — input validation
# ---------------------------------------------------------------------------


class TestCouchDBExecutionClientPutValidation(unittest.TestCase):
    def test_invalid_mode_raises_value_error(self):
        handler = make_handler_mock()
        client = make_execution_client(handler)
        with self.assertRaises(ValueError) as ctx:
            client.put("x", {}, mode="replace")
        self.assertIn("replace", str(ctx.exception))

    def test_doc_with_id_raises_value_error(self):
        handler = make_handler_mock()
        client = make_execution_client(handler)
        with self.assertRaises(ValueError) as ctx:
            client.put("x", {"_id": "x"})
        self.assertIn("_id", str(ctx.exception))

    def test_doc_with_rev_raises_value_error(self):
        handler = make_handler_mock()
        client = make_execution_client(handler)
        with self.assertRaises(ValueError) as ctx:
            client.put("x", {"_rev": "1-abc"})
        self.assertIn("_rev", str(ctx.exception))


# ---------------------------------------------------------------------------
# CouchDBExecutionClient.put() — mode tests
# ---------------------------------------------------------------------------


class TestCouchDBExecutionClientPutModes(unittest.TestCase):
    def test_put_create_returns_created_status(self):
        handler = make_handler_mock(put_result={"rev": "1-abc", "ok": True})
        client = make_execution_client(handler)
        result = client.put("x", {"val": 1}, mode="create")
        handler.fetch_document_by_id.assert_not_called()
        self.assertEqual(result.status, "created")
        self.assertIsNone(result.old_rev)
        self.assertEqual(result.new_rev, "1-abc")

    def test_put_create_fails_on_409(self):
        handler = make_handler_mock()
        handler.put_document.side_effect = make_api_exception(409)
        client = make_execution_client(handler)
        with self.assertRaises(DataAccessWriteError) as ctx:
            client.put("x", {}, mode="create")
        self.assertIn("already exists", str(ctx.exception))

    def test_put_update_returns_updated_status(self):
        handler = make_handler_mock(
            get_result={"_id": "x", "_rev": "1-old"},
            put_result={"rev": "2-new", "ok": True},
        )
        client = make_execution_client(handler)
        result = client.put("x", {"val": 2}, mode="update")
        self.assertEqual(result.status, "updated")
        self.assertEqual(result.old_rev, "1-old")
        self.assertEqual(result.new_rev, "2-new")

    def test_put_update_fails_when_doc_absent(self):
        handler = make_handler_mock(get_result=None)
        client = make_execution_client(handler)
        with self.assertRaises(DataAccessWriteError) as ctx:
            client.put("x", {}, mode="update")
        self.assertIn("does not exist", str(ctx.exception))

    def test_put_update_fetch_failure_raises_write_error(self):
        """ApiException during pre-fetch is wrapped as DataAccessWriteError."""
        handler = make_handler_mock()
        handler.fetch_document_by_id.side_effect = make_api_exception(500)
        client = make_execution_client(handler)
        with self.assertRaises(DataAccessWriteError) as ctx:
            client.put("x", {}, mode="update")
        self.assertIn("pre-check", str(ctx.exception))

    def test_put_upsert_creates_when_absent(self):
        handler = make_handler_mock(
            get_result=None,
            put_result={"rev": "1-abc", "ok": True},
        )
        client = make_execution_client(handler)
        result = client.put("x", {}, mode="upsert")
        self.assertEqual(result.status, "created")

    def test_put_upsert_updates_when_present(self):
        handler = make_handler_mock(
            get_result={"_id": "x", "_rev": "1-old"},
            put_result={"rev": "2-new", "ok": True},
        )
        client = make_execution_client(handler)
        result = client.put("x", {"val": 2}, mode="upsert")
        self.assertEqual(result.status, "updated")
        self.assertEqual(result.old_rev, "1-old")

    def test_put_upsert_retries_once_on_409(self):
        handler = make_handler_mock()
        # First fetch returns present doc
        handler.fetch_document_by_id.side_effect = [
            {"_id": "x", "_rev": "1-old"},  # first fetch
            {"_id": "x", "_rev": "2-new"},  # retry fetch
        ]
        # First put raises 409, second succeeds
        handler.put_document.side_effect = [
            make_api_exception(409),
            {"rev": "3-xyz", "ok": True},
        ]
        client = make_execution_client(handler)
        result = client.put("x", {"val": 1}, mode="upsert")
        self.assertEqual(result.status, "updated")
        self.assertEqual(result.new_rev, "3-xyz")

    def test_put_upsert_raises_after_two_409s(self):
        handler = make_handler_mock()
        handler.fetch_document_by_id.return_value = {"_id": "x", "_rev": "1-old"}
        handler.put_document.side_effect = make_api_exception(409)
        client = make_execution_client(handler)
        with self.assertRaises(DataAccessWriteError) as ctx:
            client.put("x", {}, mode="upsert")
        self.assertIn("consecutive", str(ctx.exception))

    def test_put_upsert_fetch_failure_raises_write_error(self):
        from requests.exceptions import RequestException

        handler = make_handler_mock()
        handler.fetch_document_by_id.side_effect = RequestException("timeout")
        client = make_execution_client(handler)
        with self.assertRaises(DataAccessWriteError) as ctx:
            client.put("x", {}, mode="upsert")
        self.assertIn("pre-check", str(ctx.exception))

    def test_put_upsert_retry_fetch_failure_raises_write_error(self):
        """Fetch failure during retry raises DataAccessWriteError."""
        from requests.exceptions import RequestException

        handler = make_handler_mock()
        # First fetch succeeds, first put raises 409, retry fetch fails
        handler.fetch_document_by_id.side_effect = [
            {"_id": "x", "_rev": "1-old"},
            RequestException("timeout on retry"),
        ]
        handler.put_document.side_effect = make_api_exception(409)
        client = make_execution_client(handler)
        with self.assertRaises(DataAccessWriteError):
            client.put("x", {}, mode="upsert")

    # --- Fix 3: create-path 409 retry ---

    def test_upsert_create_path_409_retries_and_updates(self):
        """Document absent at fetch, create returns 409, retry updates successfully."""
        handler = make_handler_mock()
        handler.fetch_document_by_id.side_effect = [
            None,  # initial fetch: absent
            {"_id": "x", "_rev": "1-abc"},  # retry fetch after create-path 409
        ]
        handler.put_document.side_effect = [
            make_api_exception(409),  # first create attempt: 409
            {"rev": "2-xyz", "ok": True},  # retry update: success
        ]
        client = make_execution_client(handler)
        result = client.put("x", {}, mode="upsert")
        self.assertEqual(result.status, "updated")
        self.assertEqual(result.new_rev, "2-xyz")

    def test_upsert_create_path_two_consecutive_409s_raise_with_conflict_message(self):
        """Create-path 409, refetch present, retry update 409 → 'two consecutive' message."""
        handler = make_handler_mock()
        handler.fetch_document_by_id.side_effect = [
            None,  # initial fetch: absent
            {"_id": "x", "_rev": "1-abc"},  # retry fetch after create-path 409
        ]
        handler.put_document.side_effect = [
            make_api_exception(409),  # first create: 409
            make_api_exception(409),  # retry update: 409 again
        ]
        client = make_execution_client(handler)
        with self.assertRaises(DataAccessWriteError) as ctx:
            client.put("x", {}, mode="upsert")
        self.assertIn("consecutive", str(ctx.exception))

    def test_upsert_create_path_409_refetch_absent_retries_create(self):
        """Create-path 409, refetch returns None, second create succeeds."""
        handler = make_handler_mock()
        handler.fetch_document_by_id.side_effect = [
            None,  # initial fetch: absent
            None,  # retry fetch: still absent
        ]
        handler.put_document.side_effect = [
            make_api_exception(409),  # first create: 409
            {"rev": "1-abc", "ok": True},  # second create: success
        ]
        client = make_execution_client(handler)
        result = client.put("x", {}, mode="upsert")
        self.assertEqual(result.status, "created")

    def test_upsert_create_path_non_409_error_raises(self):
        """Non-409 ApiException on create path propagates as DataAccessWriteError."""
        handler = make_handler_mock()
        handler.fetch_document_by_id.return_value = None
        handler.put_document.side_effect = make_api_exception(500)
        client = make_execution_client(handler)
        with self.assertRaises(DataAccessWriteError) as ctx:
            client.put("x", {}, mode="upsert")
        self.assertIn("500", str(ctx.exception))

    # --- Fix 4: retry transport errors wrapped ---

    def test_upsert_update_path_retry_transport_error_wrapped(self):
        """RequestException on update-path retry is wrapped as DataAccessWriteError."""
        from requests.exceptions import RequestException

        handler = make_handler_mock()
        handler.fetch_document_by_id.side_effect = [
            {"_id": "x", "_rev": "1-old"},  # initial fetch: present
            {"_id": "x", "_rev": "2-new"},  # retry fetch after 409
        ]
        handler.put_document.side_effect = [
            make_api_exception(409),  # first write: 409
            RequestException("connection reset"),  # retry write: transport error
        ]
        client = make_execution_client(handler)
        with self.assertRaises(DataAccessWriteError) as ctx:
            client.put("x", {}, mode="upsert")
        self.assertIn("connection reset", str(ctx.exception))

    # --- Fix 5: _rev guard ---

    def test_update_raises_when_existing_doc_has_no_rev(self):
        """Existing document without '_rev' raises DataAccessWriteError."""
        handler = make_handler_mock(get_result={"_id": "x", "value": 1})  # no _rev
        client = make_execution_client(handler)
        with self.assertRaises(DataAccessWriteError) as ctx:
            client.put("x", {}, mode="update")
        self.assertIn("_rev", str(ctx.exception))

    def test_upsert_raises_when_existing_doc_has_no_rev(self):
        """Existing document without '_rev' raises DataAccessWriteError in upsert."""
        handler = make_handler_mock(get_result={"_id": "x", "value": 1})  # no _rev
        client = make_execution_client(handler)
        with self.assertRaises(DataAccessWriteError) as ctx:
            client.put("x", {}, mode="upsert")
        self.assertIn("_rev", str(ctx.exception))


# ---------------------------------------------------------------------------
# CouchDBExecutionClient.put() — result shape
# ---------------------------------------------------------------------------


class TestCouchDBExecutionClientWriteResult(unittest.TestCase):
    def _make_result(self):
        handler = make_handler_mock(put_result={"rev": "1-abc", "ok": True})
        client = make_execution_client(handler)
        return client.put("x", {"val": 1}, mode="upsert")

    def test_write_result_fields(self):
        result = self._make_result()
        self.assertEqual(result.backend, "couchdb")
        self.assertEqual(result.connection_name, "test_db")
        self.assertEqual(result.resource, "test_db")
        self.assertEqual(result.operation, "upsert")
        self.assertEqual(result.doc_id, "x")
        self.assertIn(result.status, ("created", "updated"))

    def test_write_result_status_is_str_not_bool(self):
        result = self._make_result()
        self.assertIsInstance(result.status, str)
        self.assertIn(result.status, ("created", "updated"))


# ---------------------------------------------------------------------------
# CouchDBExecutionClient.put() — event emission
# ---------------------------------------------------------------------------


class TestCouchDBExecutionClientEvents(unittest.TestCase):
    def _make_trace(self, emitter):
        return DataAccessTraceContext(
            realm="r",
            phase="execution",
            plan_id="p1",
            run_id="r1",
            step_id="s1",
            step_name="my_step",
            emitter=emitter,
        )

    def test_succeeded_event_emitted_on_success(self):
        mock_emitter = MagicMock()
        handler = make_handler_mock(put_result={"rev": "1-abc"})
        client = make_execution_client(
            handler, trace_context=self._make_trace(mock_emitter)
        )
        client.put("x", {}, mode="create")
        mock_emitter.emit.assert_called_once()
        event = mock_emitter.emit.call_args[0][0]
        self.assertEqual(event["type"], "data_access.write.succeeded")
        self.assertEqual(event["status"], "created")
        self.assertEqual(event["realm"], "r")
        self.assertEqual(event["plan_id"], "p1")
        self.assertEqual(event["doc_id"], "x")

    def test_denied_event_emitted_before_denied_error(self):
        mock_emitter = MagicMock()
        trace = self._make_trace(mock_emitter)
        handler = make_handler_mock()
        client = make_execution_client(
            handler, permissions=frozenset({"read"}), trace_context=trace
        )
        with self.assertRaises(DataAccessDeniedError):
            client.put("x", {})
        mock_emitter.emit.assert_called_once()
        event = mock_emitter.emit.call_args[0][0]
        self.assertEqual(event["type"], "data_access.write.denied")

    def test_failed_event_emitted_on_backend_error(self):
        mock_emitter = MagicMock()
        handler = make_handler_mock()
        handler.put_document.side_effect = make_api_exception(409)
        client = make_execution_client(
            handler, trace_context=self._make_trace(mock_emitter)
        )
        with self.assertRaises(DataAccessWriteError):
            client.put("x", {}, mode="create")
        mock_emitter.emit.assert_called_once()
        event = mock_emitter.emit.call_args[0][0]
        self.assertEqual(event["type"], "data_access.write.failed")

    def test_emission_failure_does_not_suppress_write_result(self):
        mock_emitter = MagicMock()
        mock_emitter.emit.side_effect = RuntimeError("spool full")
        handler = make_handler_mock(put_result={"rev": "1-abc"})
        client = make_execution_client(
            handler, trace_context=self._make_trace(mock_emitter)
        )
        result = client.put("x", {}, mode="create")
        self.assertEqual(result.status, "created")

    def test_no_emission_when_trace_context_is_none(self):
        handler = make_handler_mock(put_result={"rev": "1-abc"})
        client = make_execution_client(handler, trace_context=None)
        result = client.put("x", {}, mode="create")
        self.assertIsNotNone(result)

    def test_spool_path_hints_in_event(self):
        mock_emitter = MagicMock()
        trace = self._make_trace(mock_emitter)
        handler = make_handler_mock(put_result={"rev": "1-abc"})
        client = make_execution_client(handler, trace_context=trace)
        client.put("x", {}, mode="create")
        event = mock_emitter.emit.call_args[0][0]
        sp = event["_spool_path"]
        self.assertEqual(sp["realm"], trace.realm)
        self.assertEqual(sp["plan_id"], trace.plan_id)
        self.assertEqual(sp["step_id"], trace.step_id)

    def test_write_events_have_unique_filenames(self):
        """Two put() calls in the same step produce unique _spool_path filenames."""
        mock_emitter = MagicMock()
        trace = self._make_trace(mock_emitter)
        handler = make_handler_mock(put_result={"rev": "1-abc"})
        client = make_execution_client(handler, trace_context=trace)
        client.put("doc_a", {}, mode="create")
        client.put("doc_b", {}, mode="create")
        filenames = [
            c[0][0]["_spool_path"]["filename"] for c in mock_emitter.emit.call_args_list
        ]
        self.assertEqual(len(filenames), 2)
        self.assertNotEqual(filenames[0], filenames[1])
        for fn in filenames:
            self.assertTrue(fn.startswith("data_access_write_"))
            self.assertTrue(fn.endswith(".json"))


if __name__ == "__main__":
    unittest.main()
