"""
Unit tests for ChangesFetcher.

Tests the generic CouchDB _changes fetcher without requiring a live database.
All CouchDB operations are mocked.
"""

import asyncio
import json
import unittest
from unittest.mock import Mock, patch

from lib.couchdb.changes_fetcher import ChangesFetcher


class MockApiException(Exception):
    """Mock ApiException for testing."""

    def __init__(self, code, message="Test error"):
        super().__init__(message)
        self.code = code
        self.message = message


class TestChangesFetcher(unittest.TestCase):
    """Unit tests for ChangesFetcher."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_db_handler = Mock()
        self.mock_db_handler.db_name = "test_db"
        self.mock_db_handler.server = Mock()

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

    def test_init_custom_params(self):
        """Test fetcher initialization with custom parameters."""
        fetcher = ChangesFetcher(
            self.mock_db_handler,
            include_docs=False,
            retry_delay_sec=5.0,
            max_retries=5,
        )
        self.assertFalse(fetcher.include_docs)
        self.assertEqual(fetcher.retry_delay_sec, 5.0)
        self.assertEqual(fetcher.max_retries, 5)

    @patch("asyncio.sleep")
    def test_fetch_changes_basic(self, mock_sleep):
        """Test basic _changes fetch without errors."""
        # Mock response
        mock_response = Mock()
        mock_lines = [
            json.dumps({"id": "doc1", "seq": "1-abc", "doc": {"_id": "doc1"}}),
            json.dumps({"id": "doc2", "seq": "2-def", "doc": {"_id": "doc2"}}),
        ]
        mock_response.iter_lines.return_value = mock_lines

        mock_result = Mock()
        mock_result.get_result.return_value = mock_response
        self.mock_db_handler.server.post_changes_as_stream.return_value = mock_result

        fetcher = ChangesFetcher(self.mock_db_handler)

        # Run async test
        loop = asyncio.new_event_loop()
        try:
            changes = []

            async def collect():
                async for change in fetcher.fetch_changes(since="0"):
                    changes.append(change)

            loop.run_until_complete(collect())

            # Verify results
            self.assertEqual(len(changes), 2)
            self.assertEqual(changes[0]["id"], "doc1")
            self.assertEqual(changes[1]["id"], "doc2")

            # Verify API was called correctly
            self.mock_db_handler.server.post_changes_as_stream.assert_called_once()
            call_kwargs = self.mock_db_handler.server.post_changes_as_stream.call_args[
                1
            ]
            self.assertEqual(call_kwargs["feed"], "normal")
            self.assertEqual(call_kwargs["since"], "0")
            self.assertTrue(call_kwargs["include_docs"])
        finally:
            loop.close()

    @patch("asyncio.sleep")
    def test_fetch_changes_empty(self, mock_sleep):
        """Test _changes fetch with no changes."""
        mock_response = Mock()
        mock_response.iter_lines.return_value = []
        mock_result = Mock()
        mock_result.get_result.return_value = mock_response
        self.mock_db_handler.server.post_changes_as_stream.return_value = mock_result

        fetcher = ChangesFetcher(self.mock_db_handler)

        loop = asyncio.new_event_loop()
        try:
            changes = []

            async def collect():
                async for change in fetcher.fetch_changes(since="100"):
                    changes.append(change)

            loop.run_until_complete(collect())
            self.assertEqual(len(changes), 0)
        finally:
            loop.close()

    @patch("asyncio.sleep")
    def test_fetch_changes_skips_invalid_json(self, mock_sleep):
        """Test that invalid JSON lines are skipped."""
        mock_response = Mock()
        mock_lines = [
            json.dumps({"id": "doc1", "seq": "1-abc", "doc": {}}),
            "invalid json {{{",
            json.dumps({"id": "doc2", "seq": "2-def", "doc": {}}),
        ]
        mock_response.iter_lines.return_value = mock_lines
        mock_result = Mock()
        mock_result.get_result.return_value = mock_response
        self.mock_db_handler.server.post_changes_as_stream.return_value = mock_result

        fetcher = ChangesFetcher(self.mock_db_handler)

        loop = asyncio.new_event_loop()
        try:
            changes = []

            async def collect():
                async for change in fetcher.fetch_changes():
                    changes.append(change)

            loop.run_until_complete(collect())
            # Should only get valid changes (invalid JSON skipped)
            self.assertEqual(len(changes), 2)
        finally:
            loop.close()

    @patch("asyncio.sleep")
    def test_fetch_changes_skips_empty_lines(self, mock_sleep):
        """Test that empty lines are skipped."""
        mock_response = Mock()
        mock_lines = [
            json.dumps({"id": "doc1", "seq": "1-abc"}),
            "",
            None,
            json.dumps({"id": "doc2", "seq": "2-def"}),
        ]
        mock_response.iter_lines.return_value = mock_lines
        mock_result = Mock()
        mock_result.get_result.return_value = mock_response
        self.mock_db_handler.server.post_changes_as_stream.return_value = mock_result

        fetcher = ChangesFetcher(self.mock_db_handler)

        loop = asyncio.new_event_loop()
        try:
            changes = []

            async def collect():
                async for change in fetcher.fetch_changes():
                    changes.append(change)

            loop.run_until_complete(collect())
            self.assertEqual(len(changes), 2)
        finally:
            loop.close()

    @patch("lib.couchdb.changes_fetcher.ApiException", MockApiException)
    @patch("asyncio.sleep")
    def test_fetch_changes_api_exception(self, mock_sleep):
        """Test that API exceptions are re-raised."""
        mock_result = Mock()
        mock_result.get_result.side_effect = MockApiException(500, "Server error")
        self.mock_db_handler.server.post_changes_as_stream.return_value = mock_result

        fetcher = ChangesFetcher(self.mock_db_handler)

        loop = asyncio.new_event_loop()
        try:

            async def collect():
                async for _ in fetcher.fetch_changes():
                    pass

            with self.assertRaises(MockApiException):
                loop.run_until_complete(collect())
        finally:
            loop.close()

    @patch("asyncio.sleep")
    def test_stream_changes_continuously_single_batch(self, mock_sleep):
        """Test continuous streaming with single batch of changes."""
        mock_response = Mock()
        mock_lines = [
            json.dumps({"id": "doc1", "seq": "1-abc"}),
            json.dumps({"id": "doc2", "seq": "2-def"}),
        ]
        mock_response.iter_lines.return_value = mock_lines

        # After first batch, raise to stop iteration
        mock_result1 = Mock()
        mock_result1.get_result.return_value = mock_response
        self.mock_db_handler.server.post_changes_as_stream.return_value = mock_result1

        fetcher = ChangesFetcher(self.mock_db_handler)

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

    @patch("lib.couchdb.changes_fetcher.ApiException", MockApiException)
    @patch("asyncio.sleep")
    def test_stream_continuous_transient_error_retry(self, mock_sleep):
        """Test retry logic on transient 500 error."""
        # First call fails with 500
        mock_result_fail = Mock()
        mock_result_fail.get_result.side_effect = MockApiException(500, "Server error")

        # Second call succeeds
        mock_response = Mock()
        mock_lines = [json.dumps({"id": "doc1", "seq": "1-abc"})]
        mock_response.iter_lines.return_value = mock_lines

        mock_result_success = Mock()
        mock_result_success.get_result.return_value = mock_response

        # Alternate between fail and success
        self.mock_db_handler.server.post_changes_as_stream.side_effect = [
            mock_result_fail,
            mock_result_success,
        ]

        fetcher = ChangesFetcher(self.mock_db_handler, max_retries=3)

        loop = asyncio.new_event_loop()
        try:
            changes = []
            call_count = [0]

            async def collect():
                async for change in fetcher.stream_changes_continuously(
                    poll_interval_sec=0.01
                ):
                    changes.append(change)
                    call_count[0] += 1
                    if call_count[0] >= 1:
                        break

            loop.run_until_complete(asyncio.wait_for(collect(), timeout=2.0))
            # Should get 1 change after retry succeeds
            self.assertEqual(len(changes), 1)
            # Should have called post_changes_as_stream twice (fail + retry)
            self.assertEqual(
                self.mock_db_handler.server.post_changes_as_stream.call_count, 2
            )
        finally:
            loop.close()

    @patch("lib.couchdb.changes_fetcher.ApiException", MockApiException)
    @patch("asyncio.sleep")
    def test_stream_continuous_max_retries_exceeded(self, mock_sleep):
        """Test that stream stops after max retries exceeded."""
        mock_result = Mock()
        mock_result.get_result.side_effect = MockApiException(500, "Server error")
        self.mock_db_handler.server.post_changes_as_stream.return_value = mock_result

        fetcher = ChangesFetcher(self.mock_db_handler, max_retries=2)

        loop = asyncio.new_event_loop()
        try:

            async def collect():
                async for _ in fetcher.stream_changes_continuously(
                    poll_interval_sec=0.01
                ):
                    pass

            with self.assertRaises(MockApiException):
                loop.run_until_complete(asyncio.wait_for(collect(), timeout=2.0))
        finally:
            loop.close()

    @patch("lib.couchdb.changes_fetcher.ApiException", MockApiException)
    @patch("asyncio.sleep")
    def test_stream_continuous_404_error_not_retried(self, mock_sleep):
        """Test that 404 (DB not found) is not retried."""
        mock_result = Mock()
        mock_result.get_result.side_effect = MockApiException(404, "Not found")
        self.mock_db_handler.server.post_changes_as_stream.return_value = mock_result

        fetcher = ChangesFetcher(self.mock_db_handler, max_retries=3)

        loop = asyncio.new_event_loop()
        try:

            async def collect():
                async for _ in fetcher.stream_changes_continuously(
                    poll_interval_sec=0.01
                ):
                    pass

            with self.assertRaises(MockApiException) as ctx:
                loop.run_until_complete(asyncio.wait_for(collect(), timeout=2.0))

            self.assertEqual(ctx.exception.code, 404)
            # Should only be called once (no retries)
            self.assertEqual(
                self.mock_db_handler.server.post_changes_as_stream.call_count, 1
            )
        finally:
            loop.close()


if __name__ == "__main__":
    unittest.main()
