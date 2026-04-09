"""
CouchDB backend stream tests — groups A through G.

Tests A–F drive ``CouchDBBackend.stream()`` directly via ``collect_stream()``.
Tests G use the public ``start()`` / ``stop()`` / ``events()`` interface.
"""

from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from ibm_cloud_sdk_core.api_exception import ApiException

from lib.couchdb.couchdb_models import ChangesBatch, ChangesRow, FeedMode
from lib.watchers.backends.checkpoint_store import InMemoryCheckpointStore
from lib.watchers.backends.couchdb import CouchDBBackend, _is_internal_doc

# ── Utilities ─────────────────────────────────────────────────────────────────

_BASE_CONFIG = {
    "url": "http://localhost:5984",
    "db": "testdb",
    "user_env": "COUCH_USER",
    "pass_env": "COUCH_PASS",
    "poll_interval": 0,
    "max_observation_retries": 2,
    "observation_retry_delay_s": 0,
}


def make_backend(extra_config=None, store=None):
    config = {**_BASE_CONFIG}
    if extra_config:
        config.update(extra_config)
    return CouchDBBackend(
        backend_key="couchdb:testdb",
        config=config,
        checkpoint_store=store or InMemoryCheckpointStore(),
    )


def make_batch(rows=None, last_seq="1-abc", pending=0):
    return ChangesBatch(rows=rows or [], last_seq=last_seq, pending=pending)


def make_row(doc_id, seq="1-abc", deleted=False):
    return ChangesRow(id=doc_id, seq=seq, deleted=deleted)


def setup_handler(backend, batches, *, doc_return=None, doc_side_effect=None):
    """
    Return a MagicMock handler whose fetch_changes_raw cycles through ``batches``.

    When batches are exhausted, sets ``backend._running = False`` so ``stream()``
    exits cleanly on the next loop check.

    ``doc_return`` sets a fixed return value for ``fetch_document_by_id``.
    ``doc_side_effect`` sets a side_effect list for ``fetch_document_by_id``.
    """
    handler = MagicMock()
    batch_it = iter(batches)

    def fetch_raw(**kwargs):
        try:
            return next(batch_it)
        except StopIteration:
            backend._running = False
            return ChangesBatch(rows=[], last_seq=None, pending=0)

    handler.fetch_changes_raw.side_effect = fetch_raw
    if doc_side_effect is not None:
        handler.fetch_document_by_id.side_effect = doc_side_effect
    else:
        handler.fetch_document_by_id.return_value = doc_return
    return handler


async def collect_stream(backend, *, max_events=20):
    """Iterate ``backend.stream()`` until stopped or ``max_events`` events collected."""
    events = []
    async for event in backend.stream():
        events.append(event)
        if len(events) >= max_events:
            break
    return events


# ── A: Feed mode ──────────────────────────────────────────────────────────────


class TestFeedMode(unittest.IsolatedAsyncioTestCase):

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_a1_first_call_uses_normal(self, _sleep):
        """First poll uses feed=normal regardless of pending."""
        backend = make_backend()
        backend._running = True
        handler = setup_handler(backend, [make_batch(pending=0)])
        with patch.object(backend, "_create_handler", return_value=handler):
            await collect_stream(backend)
        first_call = handler.fetch_changes_raw.call_args_list[0]
        self.assertEqual(first_call.kwargs["feed"], FeedMode.NORMAL.value)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_a2_pending_zero_switches_to_longpoll(self, _sleep):
        """After pending==0 batch, next poll uses feed=longpoll."""
        backend = make_backend()
        backend._running = True
        handler = setup_handler(
            backend,
            [
                make_batch(pending=0, last_seq="1"),
                make_batch(pending=0, last_seq="2"),
            ],
        )
        with patch.object(backend, "_create_handler", return_value=handler):
            await collect_stream(backend)
        second_call = handler.fetch_changes_raw.call_args_list[1]
        self.assertEqual(second_call.kwargs["feed"], FeedMode.LONGPOLL.value)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_a3_pending_nonzero_stays_normal(self, _sleep):
        """While pending>0, subsequent polls remain feed=normal."""
        backend = make_backend()
        backend._running = True
        handler = setup_handler(
            backend,
            [
                make_batch(pending=3, last_seq="1"),
                make_batch(pending=3, last_seq="2"),
            ],
        )
        with patch.object(backend, "_create_handler", return_value=handler):
            await collect_stream(backend)
        second_call = handler.fetch_changes_raw.call_args_list[1]
        self.assertEqual(second_call.kwargs["feed"], FeedMode.NORMAL.value)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_a4_remains_longpoll_when_pending_stays_zero(self, _sleep):
        """Once in longpoll mode, stays longpoll while pending==0."""
        backend = make_backend()
        backend._running = True
        handler = setup_handler(
            backend,
            [
                make_batch(pending=0, last_seq="1"),
                make_batch(pending=0, last_seq="2"),
                make_batch(pending=0, last_seq="3"),
            ],
        )
        with patch.object(backend, "_create_handler", return_value=handler):
            await collect_stream(backend)
        # calls 2 and 3 (index 1, 2) should use longpoll
        calls = handler.fetch_changes_raw.call_args_list
        self.assertEqual(calls[1].kwargs["feed"], FeedMode.LONGPOLL.value)
        self.assertEqual(calls[2].kwargs["feed"], FeedMode.LONGPOLL.value)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_a5_longpoll_returns_to_normal_when_pending_nonzero(self, _sleep):
        """Longpoll mode returns to normal when a batch has pending>0."""
        backend = make_backend()
        backend._running = True
        handler = setup_handler(
            backend,
            [
                make_batch(pending=0, last_seq="1"),  # → switches to longpoll
                make_batch(pending=2, last_seq="2"),  # → back to normal
                make_batch(pending=0, last_seq="3"),  # uses normal
            ],
        )
        with patch.object(backend, "_create_handler", return_value=handler):
            await collect_stream(backend)
        calls = handler.fetch_changes_raw.call_args_list
        self.assertEqual(calls[2].kwargs["feed"], FeedMode.NORMAL.value)


# ── B: Document retrieval ──────────────────────────────────────────────────────


class TestDocRetrieval(unittest.IsolatedAsyncioTestCase):

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_b1_deleted_row_skips_fetch(self, _sleep):
        """Deleted rows emit event without calling fetch_document_by_id."""
        backend = make_backend()
        backend._running = True
        row = make_row("doc-del", seq="1", deleted=True)
        handler = setup_handler(backend, [make_batch(rows=[row])])
        with patch.object(backend, "_create_handler", return_value=handler):
            events = await collect_stream(backend)

        self.assertEqual(len(events), 1)
        self.assertTrue(events[0].deleted)
        self.assertIsNone(events[0].doc)
        handler.fetch_document_by_id.assert_not_called()

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_b2_non_deleted_row_fetches_doc(self, _sleep):
        """Non-deleted rows call fetch_document_by_id and include doc in event."""
        backend = make_backend()
        backend._running = True
        row = make_row("doc-1", seq="1", deleted=False)
        doc = {"_id": "doc-1", "type": "project"}
        handler = setup_handler(backend, [make_batch(rows=[row])], doc_return=doc)
        with patch.object(backend, "_create_handler", return_value=handler):
            events = await collect_stream(backend)

        self.assertEqual(len(events), 1)
        self.assertFalse(events[0].deleted)
        self.assertEqual(events[0].doc, doc)
        handler.fetch_document_by_id.assert_called_once_with("doc-1")


# ── C: Internal doc filtering ──────────────────────────────────────────────────


class TestInternalDocFiltering(unittest.TestCase):

    def test_c1_design_doc_is_internal(self):
        self.assertTrue(_is_internal_doc("_design/views"))

    def test_c2_local_doc_is_internal(self):
        self.assertTrue(_is_internal_doc("_local/mrview"))

    def test_c3_normal_doc_is_not_internal(self):
        self.assertFalse(_is_internal_doc("NGIS-ABC-123"))


class TestInternalDocStream(unittest.IsolatedAsyncioTestCase):

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_c1_design_doc_yields_no_event(self, _sleep):
        """_design/* rows produce no event."""
        backend = make_backend()
        backend._running = True
        row = make_row("_design/views", seq="1")
        handler = setup_handler(backend, [make_batch(rows=[row])])
        with patch.object(backend, "_create_handler", return_value=handler):
            events = await collect_stream(backend)
        self.assertEqual(events, [])

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_c2_local_doc_yields_no_event(self, _sleep):
        """_local/* rows produce no event."""
        backend = make_backend()
        backend._running = True
        row = make_row("_local/mrview", seq="2")
        handler = setup_handler(backend, [make_batch(rows=[row])])
        with patch.object(backend, "_create_handler", return_value=handler):
            events = await collect_stream(backend)
        self.assertEqual(events, [])

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_c3_normal_doc_yields_event(self, _sleep):
        """Normal doc rows produce an event."""
        backend = make_backend()
        backend._running = True
        row = make_row("NGIS-ABC-123", seq="3")
        doc = {"_id": "NGIS-ABC-123"}
        handler = setup_handler(backend, [make_batch(rows=[row])], doc_return=doc)
        with patch.object(backend, "_create_handler", return_value=handler):
            events = await collect_stream(backend)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, "NGIS-ABC-123")

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_c4_internal_doc_advances_checkpoint(self, _sleep):
        """Internal doc rows still advance the checkpoint."""
        store = InMemoryCheckpointStore()
        backend = make_backend(store=store)
        backend._running = True
        row = make_row("_design/views", seq="42-abc")
        handler = setup_handler(backend, [make_batch(rows=[row])])
        with patch.object(backend, "_create_handler", return_value=handler):
            await collect_stream(backend)
        saved = store.load("couchdb:testdb")
        self.assertIsNotNone(saved)
        self.assertEqual(str(saved.value), "42-abc")


# ── D: _changes poll failures ──────────────────────────────────────────────────


class TestPollFailures(unittest.IsolatedAsyncioTestCase):

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_d1_poll_failure_retried(self, _sleep):
        """A poll failure is retried; events from recovery batch are emitted."""
        import requests

        backend = make_backend()
        backend._running = True

        call_count = {"n": 0}
        row = make_row("doc-1", seq="1")
        doc = {"_id": "doc-1"}

        def fetch_raw(**kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise requests.exceptions.Timeout("timed out")
            if call_count["n"] == 2:
                return make_batch(rows=[row])
            backend._running = False
            return ChangesBatch(rows=[], last_seq=None, pending=0)

        handler = MagicMock()
        handler.fetch_changes_raw.side_effect = fetch_raw
        handler.fetch_document_by_id.return_value = doc

        with patch.object(backend, "_create_handler", return_value=handler):
            events = await collect_stream(backend)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, "doc-1")
        self.assertGreaterEqual(call_count["n"], 2)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_d2_transient_poll_failure_logs_warning(self, _sleep):
        """A transient poll failure logs at WARNING level."""
        import requests

        backend = make_backend()
        backend._running = True

        call_count = {"n": 0}

        def fetch_raw(**kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise requests.exceptions.ConnectionError("refused")
            backend._running = False
            return ChangesBatch(rows=[], last_seq=None, pending=0)

        handler = MagicMock()
        handler.fetch_changes_raw.side_effect = fetch_raw

        with patch.object(backend, "_create_handler", return_value=handler):
            with patch.object(backend._logger, "warning") as mock_warn:
                await collect_stream(backend)

        mock_warn.assert_called()
        warn_msg = mock_warn.call_args_list[0][0][0]
        self.assertIn("poll failed", warn_msg)
        self.assertIn("transient", warn_msg)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_d3_since_not_advanced_after_poll_failure(self, _sleep):
        """After a poll failure, the next call uses the same since value."""
        import requests

        backend = make_backend()
        backend._running = True

        call_args_recorded = []
        call_count = {"n": 0}

        def fetch_raw(**kwargs):
            call_args_recorded.append(kwargs.get("since"))
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise requests.exceptions.Timeout("timeout")
            backend._running = False
            return ChangesBatch(rows=[], last_seq="5-new", pending=0)

        handler = MagicMock()
        handler.fetch_changes_raw.side_effect = fetch_raw

        with patch.object(backend, "_create_handler", return_value=handler):
            await collect_stream(backend)

        # Both calls should use the same since (initial "0" or checkpoint)
        self.assertEqual(call_args_recorded[0], call_args_recorded[1])


# ── E: Document fetch failures and 404 ────────────────────────────────────────


class TestDocFetchFailures(unittest.IsolatedAsyncioTestCase):

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_e1_transient_failure_then_success(self, _sleep):
        """Transient fetch error is retried; successful retry emits event."""
        backend = make_backend(extra_config={"max_observation_retries": 2})
        backend._running = True
        row = make_row("doc-1", seq="1")
        doc = {"_id": "doc-1"}

        handler = setup_handler(
            backend,
            [make_batch(rows=[row])],
            doc_side_effect=[ApiException(code=503), doc],
        )
        with patch.object(backend, "_create_handler", return_value=handler):
            events = await collect_stream(backend)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].doc, doc)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_e2_transient_failure_exhausted_skips(self, _sleep):
        """Exhausting retries on transient error: no event, checkpoint advances."""
        store = InMemoryCheckpointStore()
        backend = make_backend(
            extra_config={"max_observation_retries": 2},
            store=store,
        )
        backend._running = True
        row = make_row("doc-bad", seq="5-xyz")

        handler = setup_handler(
            backend,
            [make_batch(rows=[row])],
            doc_side_effect=[
                ApiException(code=500),
                ApiException(code=500),
                ApiException(code=500),
            ],
        )
        with patch.object(backend, "_create_handler", return_value=handler):
            events = await collect_stream(backend)

        self.assertEqual(events, [])
        saved = store.load("couchdb:testdb")
        self.assertIsNotNone(saved)
        self.assertEqual(str(saved.value), "5-xyz")

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_e3_404_skips_and_warns(self, _sleep):
        """fetch_document_by_id returning None (404) skips and logs warning."""
        store = InMemoryCheckpointStore()
        backend = make_backend(store=store)
        backend._running = True
        row = make_row("missing-doc", seq="7-abc")

        handler = setup_handler(
            backend,
            [make_batch(rows=[row])],
            doc_return=None,  # simulates 404
        )
        with patch.object(backend, "_create_handler", return_value=handler):
            with patch.object(backend._logger, "warning") as mock_warn:
                events = await collect_stream(backend)

        self.assertEqual(events, [])
        mock_warn.assert_called()
        # Checkpoint still advances
        saved = store.load("couchdb:testdb")
        self.assertIsNotNone(saved)
        self.assertEqual(str(saved.value), "7-abc")

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_e4_non_transient_error_skips_immediately(self, _sleep):
        """Non-transient ApiException (e.g. 400) skips row without retrying."""
        store = InMemoryCheckpointStore()
        backend = make_backend(
            extra_config={"max_observation_retries": 3},
            store=store,
        )
        backend._running = True
        row = make_row("bad-doc", seq="9-abc")

        handler = setup_handler(
            backend,
            [make_batch(rows=[row])],
            doc_side_effect=[ApiException(code=400)],
        )
        with patch.object(backend, "_create_handler", return_value=handler):
            with patch.object(backend._logger, "error") as mock_error:
                events = await collect_stream(backend)

        self.assertEqual(events, [])
        # Only 1 attempt (no retry on 400)
        self.assertEqual(handler.fetch_document_by_id.call_count, 1)
        mock_error.assert_called()
        saved = store.load("couchdb:testdb")
        self.assertIsNotNone(saved)
        self.assertEqual(str(saved.value), "9-abc")

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_e5_retry_count_respected(self, _sleep):
        """max_observation_retries=2 means exactly 3 fetch attempts (initial + 2)."""
        backend = make_backend(extra_config={"max_observation_retries": 2})
        backend._running = True
        row = make_row("retry-doc", seq="11-abc")

        handler = setup_handler(
            backend,
            [make_batch(rows=[row])],
            doc_side_effect=[
                ApiException(code=503),
                ApiException(code=503),
                ApiException(code=503),
            ],
        )
        with patch.object(backend, "_create_handler", return_value=handler):
            events = await collect_stream(backend)

        self.assertEqual(events, [])
        # 3 attempts: initial + 2 retries (max_observation_retries=2)
        self.assertEqual(handler.fetch_document_by_id.call_count, 3)


# ── F: Checkpoint semantics ────────────────────────────────────────────────────


class TestCheckpointSemantics(unittest.IsolatedAsyncioTestCase):

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_f1_checkpoint_after_emitted_event(self, _sleep):
        """Checkpoint saved with row seq after successfully emitting event."""
        store = InMemoryCheckpointStore()
        backend = make_backend(store=store)
        backend._running = True
        row = make_row("doc-1", seq="10-abc")
        doc = {"_id": "doc-1"}

        handler = setup_handler(backend, [make_batch(rows=[row])], doc_return=doc)
        with patch.object(backend, "_create_handler", return_value=handler):
            await collect_stream(backend)

        saved = store.load("couchdb:testdb")
        self.assertIsNotNone(saved)
        self.assertEqual(str(saved.value), "10-abc")

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_f2_checkpoint_after_deleted_row(self, _sleep):
        """Checkpoint saved after deleted row."""
        store = InMemoryCheckpointStore()
        backend = make_backend(store=store)
        backend._running = True
        row = make_row("doc-del", seq="20-xyz", deleted=True)

        handler = setup_handler(backend, [make_batch(rows=[row])])
        with patch.object(backend, "_create_handler", return_value=handler):
            await collect_stream(backend)

        saved = store.load("couchdb:testdb")
        self.assertIsNotNone(saved)
        self.assertEqual(str(saved.value), "20-xyz")

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_f3_checkpoint_after_internal_doc_skip(self, _sleep):
        """Checkpoint saved when internal doc is skipped."""
        store = InMemoryCheckpointStore()
        backend = make_backend(store=store)
        backend._running = True
        row = make_row("_design/main", seq="30-def")

        handler = setup_handler(backend, [make_batch(rows=[row])])
        with patch.object(backend, "_create_handler", return_value=handler):
            await collect_stream(backend)

        saved = store.load("couchdb:testdb")
        self.assertIsNotNone(saved)
        self.assertEqual(str(saved.value), "30-def")

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_f4_checkpoint_after_404_skip(self, _sleep):
        """Checkpoint saved when row is skipped due to 404."""
        store = InMemoryCheckpointStore()
        backend = make_backend(store=store)
        backend._running = True
        row = make_row("missing", seq="40-ghi")

        handler = setup_handler(backend, [make_batch(rows=[row])], doc_return=None)
        with patch.object(backend, "_create_handler", return_value=handler):
            await collect_stream(backend)

        saved = store.load("couchdb:testdb")
        self.assertIsNotNone(saved)
        self.assertEqual(str(saved.value), "40-ghi")

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_f5_checkpoint_saved_per_row(self, _sleep):
        """In a 3-row batch, checkpoint is saved 3 times (once per row)."""
        store = InMemoryCheckpointStore()
        backend = make_backend(store=store)
        backend._running = True

        rows = [
            make_row("doc-a", seq="1-a", deleted=True),
            make_row("doc-b", seq="2-b", deleted=True),
            make_row("doc-c", seq="3-c", deleted=True),
        ]
        handler = setup_handler(backend, [make_batch(rows=rows)])
        with patch.object(backend, "_create_handler", return_value=handler):
            with patch.object(
                backend, "save_checkpoint", wraps=backend.save_checkpoint
            ) as mock_save:
                await collect_stream(backend)

        self.assertEqual(mock_save.call_count, 3)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_f6_no_checkpoint_on_poll_failure(self, _sleep):
        """Poll failure does not advance checkpoint."""
        import requests

        store = InMemoryCheckpointStore()
        backend = make_backend(store=store)
        backend._running = True

        call_count = {"n": 0}

        def fetch_raw(**kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise requests.exceptions.Timeout("timeout")
            backend._running = False
            return ChangesBatch(rows=[], last_seq="failed-seq", pending=0)

        handler = MagicMock()
        handler.fetch_changes_raw.side_effect = fetch_raw

        with patch.object(backend, "_create_handler", return_value=handler):
            await collect_stream(backend)

        # No checkpoint should have been saved (no rows were processed)
        saved = store.load("couchdb:testdb")
        self.assertIsNone(saved)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_f7_since_advances_to_last_seq(self, _sleep):
        """After a batch, ``since`` advances to ``batch.last_seq`` for next poll."""
        backend = make_backend()
        backend._running = True

        call_args = []
        call_count = {"n": 0}

        def fetch_raw(**kwargs):
            call_args.append(kwargs.get("since"))
            call_count["n"] += 1
            if call_count["n"] == 1:
                return make_batch(rows=[], last_seq="99-abc", pending=0)
            backend._running = False
            return ChangesBatch(rows=[], last_seq=None, pending=0)

        handler = MagicMock()
        handler.fetch_changes_raw.side_effect = fetch_raw

        with patch.object(backend, "_create_handler", return_value=handler):
            await collect_stream(backend)

        # Second call should use the last_seq from the first batch
        self.assertEqual(call_args[1], "99-abc")


# ── G: Public interface contract ───────────────────────────────────────────────


class TestPublicInterface(unittest.IsolatedAsyncioTestCase):

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_g1_start_stop_roundtrip(self, _sleep):
        """start() / stop() round-trip does not raise."""
        backend = make_backend()

        # Infinite empty batches — stop() will cancel the producer task
        handler = MagicMock()
        handler.fetch_changes_raw.return_value = ChangesBatch(
            rows=[], last_seq="0", pending=0
        )

        with patch.object(backend, "_create_handler", return_value=handler):
            await backend.start()
            self.assertTrue(backend.is_running)
            await backend.stop()
            self.assertFalse(backend.is_running)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_g2_events_yields_through_queue(self, _sleep):
        """Events are received via the public events() iterator (not stream() directly)."""
        backend = make_backend()

        row = make_row("queue-doc", seq="1-abc", deleted=True)
        # After the batch with one row, return empty forever so the producer loops
        # but we stop after consuming the event.
        call_count = {"n": 0}

        def fetch_raw(**kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return make_batch(rows=[row], pending=0)
            return ChangesBatch(rows=[], last_seq="1-abc", pending=0)

        handler = MagicMock()
        handler.fetch_changes_raw.side_effect = fetch_raw

        with patch.object(backend, "_create_handler", return_value=handler):
            await backend.start()

            received = []
            async for event in backend.events():
                received.append(event)
                break  # get one event then stop consuming

            await backend.stop()

        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].id, "queue-doc")
        self.assertTrue(received[0].deleted)


if __name__ == "__main__":
    unittest.main()
