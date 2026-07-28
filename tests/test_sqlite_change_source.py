"""Unit tests for the SQLite plan change source (cursor semantics)."""

import asyncio
import tempfile
import unittest
from pathlib import Path

from lib.storage.sqlite import (
    SQLiteInternalStore,
    SQLitePlanChangeSource,
    SQLitePlanStore,
)
from yggdrasil.flow.model import Plan, StepSpec


def _make_plan(plan_id: str) -> Plan:
    return Plan(
        plan_id=plan_id,
        realm="test_realm",
        scope={"kind": "project", "id": "P1"},
        steps=[
            StepSpec(
                step_id="s1",
                name="echo",
                fn_ref="tests.integration.mock_steps:echo_step",
                params={},
            )
        ],
    )


def _collect(source: SQLitePlanChangeSource, since, count: int, timeout: float = 5.0):
    """Collect ``count`` events from the (infinite) stream."""

    async def _run():
        events = []

        async def consume():
            async for event in source.stream_changes_continuously(
                since=since, poll_interval_sec=0.01
            ):
                events.append(event)
                if len(events) >= count:
                    return

        await asyncio.wait_for(consume(), timeout)
        return events

    return asyncio.run(_run())


class TestSQLitePlanChangeSource(unittest.TestCase):
    """Cursor resolution, resume, coalescing, and tombstones."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.store = SQLiteInternalStore(Path(self._tmp.name) / "ygg.sqlite3")
        self.plans = SQLitePlanStore(self.store)
        self.source = SQLitePlanChangeSource(self.store)

    def tearDown(self):
        self._tmp.cleanup()

    def _save(self, plan_id: str, *, auto_run: bool = True) -> None:
        self.plans.save_plan(
            _make_plan(plan_id),
            "test_realm",
            {"kind": "project", "id": "P1"},
            auto_run=auto_run,
        )

    def test_since_zero_replays_from_start(self):
        self._save("pln_a")
        self._save("pln_b")
        events = _collect(self.source, 0, 2)
        self.assertEqual([e.id for e in events], ["pln_a", "pln_b"])
        self.assertEqual(events[0].doc["_id"], "pln_a")
        self.assertFalse(events[0].deleted)

    def test_since_now_skips_existing_changes(self):
        self._save("pln_before")
        head = self.store.current_plan_seq()

        async def _run():
            events = []

            async def consume():
                async for event in self.source.stream_changes_continuously(
                    since="now", poll_interval_sec=0.01
                ):
                    events.append(event)
                    return

            # Save a new plan while the stream is idle at the head
            async def mutate():
                await asyncio.sleep(0.05)
                await asyncio.to_thread(self._save, "pln_after")

            await asyncio.wait_for(asyncio.gather(consume(), mutate()), timeout=30.0)
            return events

        events = asyncio.run(_run())
        self.assertEqual([e.id for e in events], ["pln_after"])
        self.assertGreater(events[0].seq, head)

    def test_opaque_cursor_resume(self):
        self._save("pln_a")
        self._save("pln_b")
        first = _collect(self.source, 0, 1)[0]
        resumed = _collect(self.source, first.seq, 1)
        self.assertEqual([e.id for e in resumed], ["pln_b"])

    def test_mutations_coalesce_to_latest_state(self):
        self._save("pln_a", auto_run=False)  # draft
        self._save("pln_a", auto_run=True)  # regenerated as approved
        events = _collect(self.source, 0, 1)
        # One event, carrying the latest document state
        self.assertEqual(events[0].id, "pln_a")
        self.assertEqual(events[0].doc["status"], "approved")
        self.assertEqual(events[0].seq, self.store.current_plan_seq())

    def test_tombstone_emits_deleted_event(self):
        self._save("pln_a")
        self.plans.delete_plan("pln_a")
        events = _collect(self.source, 0, 1)
        self.assertEqual(events[0].id, "pln_a")
        self.assertTrue(events[0].deleted)
        self.assertIsNone(events[0].doc)

    def test_unparseable_cursor_starts_at_head(self):
        self._save("pln_before")

        # A CouchDB-style cursor cannot be parsed; stream starts at head
        # and only observes new changes.
        async def _run():
            events = []

            async def consume():
                async for event in self.source.stream_changes_continuously(
                    since="123-abcdef", poll_interval_sec=0.01
                ):
                    events.append(event)
                    return

            async def mutate():
                await asyncio.sleep(0.05)
                await asyncio.to_thread(self._save, "pln_after")

            await asyncio.wait_for(asyncio.gather(consume(), mutate()), timeout=30.0)
            return events

        events = asyncio.run(_run())
        self.assertEqual([e.id for e in events], ["pln_after"])

    def test_restart_preserves_document_and_cursor_state(self):
        self._save("pln_a")
        head = self.store.current_plan_seq()
        # Reopen the same file (new store instance = daemon restart)
        store2 = SQLiteInternalStore(self.store.path)
        source2 = SQLitePlanChangeSource(store2)
        self.assertEqual(store2.current_plan_seq(), head)
        events = _collect(source2, 0, 1)
        self.assertEqual(events[0].id, "pln_a")


if __name__ == "__main__":
    unittest.main()
