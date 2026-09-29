"""
Unit tests for ChangesFetcher.

Tests the generic CouchDB _changes fetcher without requiring a live database.
All CouchDB operations are mocked.
"""

import asyncio
import unittest
from unittest.mock import Mock, patch

from requests.exceptions import ConnectionError as RequestsConnectionError

from lib.couchdb.changes_fetcher import ChangesFetcher
from lib.couchdb.couchdb_models import ChangesBatch, ChangesRow


class MockApiException(Exception):
    """Stand-in for ibm_cloud_sdk_core.api_exception.ApiException in tests."""

    def __init__(self, message="", status_code=None):
        super().__init__(message)
        self.status_code = status_code
        self.message = message


def make_batch(rows=None, last_seq="1-abc", pending=0):
    return ChangesBatch(rows=rows or [], last_seq=last_seq, pending=pending)


def make_row(doc_id, seq="1-abc", deleted=False):
    return ChangesRow(id=doc_id, seq=seq, deleted=deleted)


class TestChangesFetcher(unittest.TestCase):
    """Unit tests for ChangesFetcher."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_db_handler = Mock()
        self.mock_db_handler.db_name = "test_db"
        self.mock_db_handler.fetch_changes_raw = Mock(return_value=make_batch())
        self.mock_db_handler.fetch_document_by_id = Mock(return_value=None)

    def test_checkpoint_doc_id(self):
        """Test canonical checkpoint doc ID construction."""
        fetcher = ChangesFetcher(self.mock_db_handler)
        self.assertEqual(fetcher.db_handler.db_name, "test_db")

    def test_init_defaults(self):
        """Test fetcher initialization with default parameters."""
        fetcher = ChangesFetcher(self.mock_db_handler)
        self.assertEqual(fetcher.db_handler, self.mock_db_handler)
        self.assertTrue(fetcher.include_docs)
        self.assertEqual(fetcher.retry_delay_sec, 2.0)
        self.assertEqual(fetcher.max_retries, 3)
        self.assertEqual(fetcher.longpoll_timeout_ms, 60_000)

    def test_init_custom_params(self):
        """Test fetcher initialization with custom parameters."""
        fetcher = ChangesFetcher(
            self.mock_db_handler,
            include_docs=False,
            retry_delay_sec=5.0,
            max_retries=5,
            longpoll_timeout_ms=30_000,
        )
        self.assertFalse(fetcher.include_docs)
        self.assertEqual(fetcher.retry_delay_sec, 5.0)
        self.assertEqual(fetcher.max_retries, 5)
        self.assertEqual(fetcher.longpoll_timeout_ms, 30_000)

    @patch("asyncio.sleep")
    def test_fetch_changes_basic(self, mock_sleep):
        """Test basic _changes fetch: rows yielded, docs fetched, change shape correct."""
        doc1 = {"_id": "doc1", "data": "x"}
        doc2 = {"_id": "doc2", "data": "y"}
        self.mock_db_handler.fetch_changes_raw.return_value = make_batch(
            rows=[make_row("doc1", "1-abc"), make_row("doc2", "2-def")],
            last_seq="2-def",
        )
        self.mock_db_handler.fetch_document_by_id.side_effect = [doc1, doc2]

        fetcher = ChangesFetcher(self.mock_db_handler, include_docs=True)

        loop = asyncio.new_event_loop()
        try:
            changes = []

            async def collect():
                async for change in fetcher.fetch_changes(since="0"):
                    changes.append(change)

            loop.run_until_complete(collect())

            self.assertEqual(len(changes), 2)
            self.assertEqual(
                changes[0],
                {"id": "doc1", "seq": "1-abc", "deleted": False, "doc": doc1},
            )
            self.assertEqual(
                changes[1],
                {"id": "doc2", "seq": "2-def", "deleted": False, "doc": doc2},
            )
            self.mock_db_handler.fetch_changes_raw.assert_called_once_with(
                since="0", feed="normal"
            )
            self.assertEqual(self.mock_db_handler.fetch_document_by_id.call_count, 2)
        finally:
            loop.close()

    @patch("asyncio.sleep")
    def test_fetch_changes_empty(self, mock_sleep):
        """Test _changes fetch with no changes."""
        self.mock_db_handler.fetch_changes_raw.return_value = make_batch(
            rows=[], last_seq="100"
        )

        fetcher = ChangesFetcher(self.mock_db_handler)

        loop = asyncio.new_event_loop()
        try:
            changes = []

            async def collect():
                async for change in fetcher.fetch_changes(since="100"):
                    changes.append(change)

            loop.run_until_complete(collect())
            self.assertEqual(len(changes), 0)
            self.mock_db_handler.fetch_document_by_id.assert_not_called()
        finally:
            loop.close()

    @patch("asyncio.sleep")
    def test_fetch_changes_filters_internal_docs(self, mock_sleep):
        """Test that _design/ and _local/ rows are filtered before yielding."""
        self.mock_db_handler.fetch_changes_raw.return_value = make_batch(
            rows=[
                make_row("_design/views", "1-abc"),
                make_row("_local/checkpoint", "2-def"),
                make_row("real-doc", "3-ghi"),
            ],
            last_seq="3-ghi",
        )
        self.mock_db_handler.fetch_document_by_id.return_value = {"_id": "real-doc"}

        fetcher = ChangesFetcher(self.mock_db_handler, include_docs=True)

        loop = asyncio.new_event_loop()
        try:
            changes = []

            async def collect():
                async for change in fetcher.fetch_changes():
                    changes.append(change)

            loop.run_until_complete(collect())
            self.assertEqual(len(changes), 1)
            self.assertEqual(changes[0]["id"], "real-doc")
            self.mock_db_handler.fetch_document_by_id.assert_called_once_with(
                "real-doc"
            )
        finally:
            loop.close()

    @patch("asyncio.sleep")
    def test_fetch_changes_deleted_skips_doc_fetch(self, mock_sleep):
        """Test that deleted rows are yielded without fetching the document."""
        self.mock_db_handler.fetch_changes_raw.return_value = make_batch(
            rows=[
                make_row("doc-alive", "1-abc", deleted=False),
                make_row("doc-dead", "2-def", deleted=True),
            ],
            last_seq="2-def",
        )
        self.mock_db_handler.fetch_document_by_id.return_value = {"_id": "doc-alive"}

        fetcher = ChangesFetcher(self.mock_db_handler, include_docs=True)

        loop = asyncio.new_event_loop()
        try:
            changes = []

            async def collect():
                async for change in fetcher.fetch_changes():
                    changes.append(change)

            loop.run_until_complete(collect())
            self.assertEqual(len(changes), 2)
            self.assertTrue(changes[1]["deleted"])
            self.assertNotIn("doc", changes[1])
            # fetch_document_by_id called only for the non-deleted row
            self.mock_db_handler.fetch_document_by_id.assert_called_once_with(
                "doc-alive"
            )
        finally:
            loop.close()

    @patch("asyncio.sleep")
    def test_fetch_changes_connection_error_propagates(self, mock_sleep):
        """Test that connection errors from fetch_changes_raw propagate to caller."""
        self.mock_db_handler.fetch_changes_raw.side_effect = RequestsConnectionError(
            "Connection refused"
        )

        fetcher = ChangesFetcher(self.mock_db_handler)

        loop = asyncio.new_event_loop()
        try:

            async def collect():
                async for _ in fetcher.fetch_changes():
                    pass

            with self.assertRaises(RequestsConnectionError):
                loop.run_until_complete(collect())
        finally:
            loop.close()

    @patch("asyncio.sleep")
    def test_stream_changes_continuously_single_batch(self, mock_sleep):
        """Test continuous streaming with single batch of changes."""
        self.mock_db_handler.fetch_changes_raw.return_value = make_batch(
            rows=[make_row("doc1", "1-abc"), make_row("doc2", "2-def")],
            last_seq="2-def",
        )
        self.mock_db_handler.fetch_document_by_id.return_value = {"_id": "doc"}

        fetcher = ChangesFetcher(self.mock_db_handler, include_docs=False)

        loop = asyncio.new_event_loop()
        try:
            changes = []
            call_count = [0]

            async def collect_limited():
                async for change in fetcher.stream_changes_continuously(
                    poll_interval_sec=0.1
                ):
                    changes.append(change)
                    call_count[0] += 1
                    if call_count[0] >= 2:
                        break

            loop.run_until_complete(asyncio.wait_for(collect_limited(), timeout=2.0))
            self.assertEqual(len(changes), 2)
        finally:
            loop.close()

    @patch("asyncio.sleep")
    def test_stream_continuous_connection_error_retries(self, mock_sleep):
        """Test that connection errors are retried with backoff before succeeding."""
        call_count = [0]

        def raw_side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] < 3:
                raise RequestsConnectionError("Connection reset")
            return make_batch(rows=[make_row("doc1", "1-abc")], last_seq="1-abc")

        self.mock_db_handler.fetch_changes_raw.side_effect = raw_side_effect

        fetcher = ChangesFetcher(
            self.mock_db_handler, max_retries=3, include_docs=False
        )

        loop = asyncio.new_event_loop()
        try:
            changes = []

            async def collect():
                async for change in fetcher.stream_changes_continuously(
                    poll_interval_sec=0.0
                ):
                    changes.append(change)
                    break

            loop.run_until_complete(asyncio.wait_for(collect(), timeout=2.0))
            self.assertEqual(len(changes), 1)
            self.assertGreaterEqual(call_count[0], 3)
        finally:
            loop.close()

    @patch("asyncio.sleep")
    def test_stream_continuous_connection_reset_no_permanent_abort(self, mock_sleep):
        """Test that ConnectionResetError backs off and resets instead of aborting."""
        call_count = [0]

        def raw_side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] <= 5:
                raise ConnectionResetError(104, "Connection reset by peer")
            return make_batch(rows=[make_row("doc1", "1-abc")], last_seq="1-abc")

        self.mock_db_handler.fetch_changes_raw.side_effect = raw_side_effect

        fetcher = ChangesFetcher(
            self.mock_db_handler, max_retries=2, include_docs=False
        )

        loop = asyncio.new_event_loop()
        try:
            changes = []

            async def collect():
                async for change in fetcher.stream_changes_continuously(
                    poll_interval_sec=0.0
                ):
                    changes.append(change)
                    break

            # Must NOT raise — backs off 60s (mocked), resets, and eventually succeeds
            loop.run_until_complete(asyncio.wait_for(collect(), timeout=2.0))
            self.assertEqual(len(changes), 1)
            self.assertGreater(call_count[0], 3)
        finally:
            loop.close()

    # --- last_seq property ---

    def test_last_seq_initially_none(self):
        """last_seq is None before any fetch."""
        fetcher = ChangesFetcher(self.mock_db_handler)
        self.assertIsNone(fetcher.last_seq)

    @patch("asyncio.sleep")
    def test_last_seq_updated_after_fetch(self, mock_sleep):
        """last_seq reflects the batch last_seq after fetch_changes completes."""
        self.mock_db_handler.fetch_changes_raw.return_value = make_batch(
            rows=[], last_seq="42-xyz"
        )
        fetcher = ChangesFetcher(self.mock_db_handler, include_docs=False)

        loop = asyncio.new_event_loop()
        try:

            async def run():
                async for _ in fetcher.fetch_changes(since="0"):
                    pass

            loop.run_until_complete(run())
        finally:
            loop.close()

        self.assertEqual(fetcher.last_seq, "42-xyz")

    # --- cursor advancement on empty batch ---

    @patch("asyncio.sleep")
    def test_stream_advances_cursor_on_empty_batch(self, mock_sleep):
        """Empty batch with advanced last_seq updates current_seq for next poll."""
        call_count = [0]

        def raw_side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                # Empty batch but CouchDB advanced the sequence
                return make_batch(rows=[], last_seq="50-advanced", pending=0)
            # Second call: yield a row so we can break
            return make_batch(
                rows=[make_row("doc1", "51-abc")], last_seq="51-abc", pending=0
            )

        self.mock_db_handler.fetch_changes_raw.side_effect = raw_side_effect
        fetcher = ChangesFetcher(self.mock_db_handler, include_docs=False)

        loop = asyncio.new_event_loop()
        try:
            changes = []

            async def collect():
                async for change in fetcher.stream_changes_continuously(
                    since="0", poll_interval_sec=0.0
                ):
                    changes.append(change)
                    break

            loop.run_until_complete(asyncio.wait_for(collect(), timeout=2.0))
        finally:
            loop.close()

        # Second call must use the advanced sequence, not "0"
        second_call_kwargs = self.mock_db_handler.fetch_changes_raw.call_args_list[1][1]
        self.assertEqual(second_call_kwargs["since"], "50-advanced")

    # --- feed mode switching ---

    @patch("asyncio.sleep")
    def test_stream_switches_to_longpoll_when_caught_up(self, mock_sleep):
        """pending=0 causes next poll to use feed='longpoll' with timeout."""
        call_count = [0]

        def raw_side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                # Caught up — no pending changes
                return make_batch(rows=[], last_seq="10-abc", pending=0)
            # Second poll: yield a row so test can break
            return make_batch(
                rows=[make_row("doc1", "11-abc")], last_seq="11-abc", pending=0
            )

        self.mock_db_handler.fetch_changes_raw.side_effect = raw_side_effect
        fetcher = ChangesFetcher(
            self.mock_db_handler, include_docs=False, longpoll_timeout_ms=45_000
        )

        loop = asyncio.new_event_loop()
        try:
            changes = []

            async def collect():
                async for change in fetcher.stream_changes_continuously(
                    since="0", poll_interval_sec=0.0
                ):
                    changes.append(change)
                    break

            loop.run_until_complete(asyncio.wait_for(collect(), timeout=2.0))
        finally:
            loop.close()

        second_call_kwargs = self.mock_db_handler.fetch_changes_raw.call_args_list[1][1]
        self.assertEqual(second_call_kwargs["feed"], "longpoll")
        self.assertEqual(second_call_kwargs["timeout_ms"], 45_000)

    @patch("asyncio.sleep")
    def test_stream_reverts_to_normal_on_pending(self, mock_sleep):
        """pending>0 after longpoll causes revert to feed='normal'."""
        call_count = [0]

        def raw_side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                # First: caught up → switch to longpoll
                return make_batch(rows=[], last_seq="10-abc", pending=0)
            if call_count[0] == 2:
                # Second (longpoll): changes arrive, pending>0 → revert to normal
                return make_batch(
                    rows=[make_row("doc1", "11-abc")], last_seq="11-abc", pending=5
                )
            # Third: break out
            return make_batch(
                rows=[make_row("doc2", "12-abc")], last_seq="12-abc", pending=0
            )

        self.mock_db_handler.fetch_changes_raw.side_effect = raw_side_effect
        fetcher = ChangesFetcher(self.mock_db_handler, include_docs=False)

        loop = asyncio.new_event_loop()
        try:
            seen = []

            async def collect():
                async for change in fetcher.stream_changes_continuously(
                    since="0", poll_interval_sec=0.0
                ):
                    seen.append(change)
                    if len(seen) >= 2:
                        break

            loop.run_until_complete(asyncio.wait_for(collect(), timeout=2.0))
        finally:
            loop.close()

        # Third call should be back in normal mode
        third_call_kwargs = self.mock_db_handler.fetch_changes_raw.call_args_list[2][1]
        self.assertEqual(third_call_kwargs["feed"], "normal")

    # --- recovery log after retries ---

    @patch("asyncio.sleep")
    def test_stream_recovery_log_after_retries(self, mock_sleep):
        """A successful poll after a retry emits the 'Recovered after retries' debug log."""
        call_count = [0]

        def raw_side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise ConnectionResetError(104, "reset")
            if call_count[0] == 2:
                # Empty batch: inner async-for completes → recovery log fires
                return make_batch(rows=[], last_seq="1-abc", pending=0)
            # Third call: yield a row so we can break
            return make_batch(rows=[make_row("doc1", "2-abc")], last_seq="2-abc")

        self.mock_db_handler.fetch_changes_raw.side_effect = raw_side_effect
        fetcher = ChangesFetcher(
            self.mock_db_handler, max_retries=3, include_docs=False
        )

        loop = asyncio.new_event_loop()
        try:

            async def collect():
                async for _ in fetcher.stream_changes_continuously(
                    poll_interval_sec=0.0
                ):
                    break

            with self.assertLogs(level="DEBUG") as cm:
                loop.run_until_complete(asyncio.wait_for(collect(), timeout=2.0))
        finally:
            loop.close()

        messages = " ".join(cm.output)
        self.assertIn("Recovered after retries", messages)

    # --- ApiException handling ---

    @patch("asyncio.sleep")
    @patch("lib.couchdb.changes_fetcher.ApiException", MockApiException)
    def test_stream_api_exception_transient_retries(self, mock_sleep):
        """ApiException with status 500 is retried with exponential backoff."""
        call_count = [0]
        self.mock_db_handler.fetch_changes_raw.return_value = make_batch(
            rows=[make_row("doc1", "1-abc")], last_seq="1-abc"
        )

        def doc_side_effect(doc_id):
            call_count[0] += 1
            if call_count[0] < 3:
                raise MockApiException("server error", status_code=500)
            return {"_id": doc_id}

        self.mock_db_handler.fetch_document_by_id.side_effect = doc_side_effect
        fetcher = ChangesFetcher(self.mock_db_handler, max_retries=5, include_docs=True)

        loop = asyncio.new_event_loop()
        try:
            changes = []

            async def collect():
                async for change in fetcher.stream_changes_continuously(
                    poll_interval_sec=0.0
                ):
                    changes.append(change)
                    break

            loop.run_until_complete(asyncio.wait_for(collect(), timeout=2.0))
        finally:
            loop.close()

        self.assertEqual(len(changes), 1)
        self.assertGreaterEqual(call_count[0], 3)
        # asyncio.sleep must have been called for each backoff
        self.assertGreater(mock_sleep.call_count, 0)

    @patch("asyncio.sleep")
    @patch("lib.couchdb.changes_fetcher.ApiException", MockApiException)
    def test_stream_api_exception_max_retries_exceeded_no_abort(self, mock_sleep):
        """ApiException max retries exceeded → 60s sleep + reset, stream continues."""
        call_count = [0]
        self.mock_db_handler.fetch_changes_raw.return_value = make_batch(
            rows=[make_row("doc1", "1-abc")], last_seq="1-abc"
        )

        def doc_side_effect(doc_id):
            call_count[0] += 1
            # Fail 3 times (exceeds max_retries=1 on 2nd failure), then succeed
            if call_count[0] <= 3:
                raise MockApiException("server error", status_code=500)
            return {"_id": doc_id}

        self.mock_db_handler.fetch_document_by_id.side_effect = doc_side_effect
        fetcher = ChangesFetcher(self.mock_db_handler, max_retries=1, include_docs=True)

        loop = asyncio.new_event_loop()
        try:
            changes = []

            async def collect():
                async for change in fetcher.stream_changes_continuously(
                    poll_interval_sec=0.0
                ):
                    changes.append(change)
                    break

            # Must NOT raise; should eventually yield after 60s sleep (mocked)
            loop.run_until_complete(asyncio.wait_for(collect(), timeout=2.0))
        finally:
            loop.close()

        self.assertEqual(len(changes), 1)
        # 60s sleep must have been called at least once (when max retries exceeded)
        sleep_calls = [c[0][0] for c in mock_sleep.call_args_list]
        self.assertIn(60.0, sleep_calls)

    @patch("asyncio.sleep")
    @patch("lib.couchdb.changes_fetcher.ApiException", MockApiException)
    def test_stream_api_exception_non_transient_raises(self, mock_sleep):
        """ApiException with non-transient status (e.g. 400) is immediately re-raised."""
        self.mock_db_handler.fetch_changes_raw.return_value = make_batch(
            rows=[make_row("doc1", "1-abc")], last_seq="1-abc"
        )
        self.mock_db_handler.fetch_document_by_id.side_effect = MockApiException(
            "bad request", status_code=400
        )
        fetcher = ChangesFetcher(self.mock_db_handler, include_docs=True)

        loop = asyncio.new_event_loop()
        try:

            async def collect():
                async for _ in fetcher.stream_changes_continuously(
                    poll_interval_sec=0.0
                ):
                    break

            with self.assertRaises(MockApiException):
                loop.run_until_complete(asyncio.wait_for(collect(), timeout=2.0))
        finally:
            loop.close()

    # --- generic exception catch-all ---

    @patch("asyncio.sleep")
    def test_stream_generic_exception_raises(self, mock_sleep):
        """An unexpected exception (not ApiException, not network) is logged and re-raised."""
        self.mock_db_handler.fetch_changes_raw.side_effect = ValueError(
            "malformed JSON"
        )
        fetcher = ChangesFetcher(self.mock_db_handler, include_docs=False)

        loop = asyncio.new_event_loop()
        try:

            async def collect():
                async for _ in fetcher.stream_changes_continuously(
                    poll_interval_sec=0.0
                ):
                    break

            with self.assertRaises(ValueError):
                loop.run_until_complete(asyncio.wait_for(collect(), timeout=2.0))
        finally:
            loop.close()


if __name__ == "__main__":
    unittest.main()
