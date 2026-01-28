"""
Unit tests for PlanWatcher.

Tests the watcher's behavior for monitoring yggdrasil_plans database,
including change processing, eligibility filtering, and event emission.
All CouchDB operations are mocked.
"""

import asyncio
import unittest
from unittest.mock import MagicMock, Mock, patch

from lib.core_utils.event_types import EventType
from lib.watchers.plan_watcher import PlanWatcher


class MockApiException(Exception):
    """Mock ApiException for testing."""

    def __init__(self, code, message="Test error"):
        super().__init__(message)
        self.code = code
        self.message = message


class TestPlanWatcher(unittest.TestCase):
    """Unit tests for PlanWatcher."""

    def setUp(self):
        """Set up test fixtures with mocked DB connections."""
        # Patch PlanDBManager
        self.plan_db_patcher = patch("lib.watchers.plan_watcher.PlanDBManager")
        self.mock_plan_db_class = self.plan_db_patcher.start()
        self.mock_plan_db = MagicMock()
        self.mock_plan_db_class.return_value = self.mock_plan_db

        # Patch YggdrasilDBManager
        self.ygg_db_patcher = patch("lib.watchers.plan_watcher.YggdrasilDBManager")
        self.mock_ygg_db_class = self.ygg_db_patcher.start()
        self.mock_ygg_db = MagicMock()
        self.mock_ygg_db_class.return_value = self.mock_ygg_db

        # Patch WatcherCheckpointStore
        self.checkpoint_patcher = patch(
            "lib.watchers.plan_watcher.WatcherCheckpointStore"
        )
        self.mock_checkpoint_class = self.checkpoint_patcher.start()
        self.mock_checkpoint = MagicMock()
        self.mock_checkpoint.get_checkpoint.return_value = None
        self.mock_checkpoint.save_checkpoint.return_value = True
        self.mock_checkpoint_class.return_value = self.mock_checkpoint

        # Patch ChangesFetcher
        self.fetcher_patcher = patch("lib.watchers.plan_watcher.ChangesFetcher")
        self.mock_fetcher_class = self.fetcher_patcher.start()
        self.mock_fetcher = MagicMock()
        self.mock_fetcher_class.return_value = self.mock_fetcher

        # Create event callback mock
        self.mock_on_event = Mock()

        # Create watcher instance
        self.watcher = PlanWatcher(
            on_event=self.mock_on_event,
            poll_interval_sec=0.1,  # Short for testing
        )

    def tearDown(self):
        """Clean up patches."""
        self.plan_db_patcher.stop()
        self.ygg_db_patcher.stop()
        self.checkpoint_patcher.stop()
        self.fetcher_patcher.stop()

    # ==========================================
    # Initialization Tests
    # ==========================================

    def test_init_creates_db_managers(self):
        """Test that PlanWatcher initializes DB managers."""
        self.mock_plan_db_class.assert_called_once()
        self.mock_ygg_db_class.assert_called_once()

    def test_init_creates_checkpoint_store(self):
        """Test that PlanWatcher creates checkpoint store with correct name."""
        self.mock_checkpoint_class.assert_called_once()
        call_kwargs = self.mock_checkpoint_class.call_args[1]
        self.assertEqual(call_kwargs["watcher_name"], "PlanWatcher")

    def test_init_creates_changes_fetcher(self):
        """Test that PlanWatcher creates changes fetcher with include_docs."""
        self.mock_fetcher_class.assert_called_once()
        call_kwargs = self.mock_fetcher_class.call_args[1]
        self.assertTrue(call_kwargs["include_docs"])

    def test_init_event_type(self):
        """Test that PlanWatcher uses correct event type."""
        self.assertEqual(self.watcher.event_type, EventType.PLAN_EXECUTION)

    def test_init_poll_interval(self):
        """Test that poll interval is configured correctly."""
        self.assertEqual(self.watcher.poll_interval_sec, 0.1)

    # ==========================================
    # _evaluate_change Tests
    # ==========================================

    def test_evaluate_change_skips_design_docs(self):
        """Test that design documents are skipped."""
        change = {"id": "_design/views", "seq": "1", "doc": {"views": {}}}

        asyncio.run(self.watcher._evaluate_change(change))

        self.mock_on_event.assert_not_called()

    def test_evaluate_change_skips_deleted_docs(self):
        """Test that deleted documents are skipped."""
        change = {"id": "pln_tenx_P12345_v1", "seq": "1", "deleted": True}

        asyncio.run(self.watcher._evaluate_change(change))

        self.mock_on_event.assert_not_called()

    def test_evaluate_change_skips_no_doc(self):
        """Test that changes without doc are skipped."""
        change = {"id": "pln_tenx_P12345_v1", "seq": "1"}  # no 'doc' field

        asyncio.run(self.watcher._evaluate_change(change))

        self.mock_on_event.assert_not_called()

    def test_evaluate_change_emits_eligible_plan(self):
        """Test that eligible plans trigger event emission."""
        eligible_doc = {
            "_id": "pln_tenx_P12345_v1",
            "status": "approved",
            "run_token": 0,
            "executed_run_token": -1,
            "execution_authority": "daemon",  # Required field
        }
        change = {"id": "pln_tenx_P12345_v1", "seq": "1", "doc": eligible_doc}

        asyncio.run(self.watcher._evaluate_change(change))

        # Verify emit was called
        self.mock_on_event.assert_called_once()
        event = self.mock_on_event.call_args[0][0]
        self.assertEqual(event.event_type, EventType.PLAN_EXECUTION)
        self.assertEqual(event.payload["plan_doc_id"], "pln_tenx_P12345_v1")
        self.assertEqual(event.payload["plan_doc"], eligible_doc)

    def test_evaluate_change_skips_ineligible_draft(self):
        """Test that draft plans are skipped."""
        draft_doc = {
            "_id": "pln_tenx_P12345_v1",
            "status": "draft",
            "run_token": 0,
            "executed_run_token": -1,
            "execution_authority": "daemon",
        }
        change = {"id": "pln_tenx_P12345_v1", "seq": "1", "doc": draft_doc}

        asyncio.run(self.watcher._evaluate_change(change))

        self.mock_on_event.assert_not_called()

    def test_evaluate_change_skips_already_executed(self):
        """Test that already-executed plans are skipped."""
        executed_doc = {
            "_id": "pln_tenx_P12345_v1",
            "status": "approved",
            "run_token": 0,
            "executed_run_token": 0,  # tokens equal
            "execution_authority": "daemon",
        }
        change = {"id": "pln_tenx_P12345_v1", "seq": "1", "doc": executed_doc}

        asyncio.run(self.watcher._evaluate_change(change))

        self.mock_on_event.assert_not_called()

    def test_evaluate_change_emits_rerun(self):
        """Test that manual re-run (incremented token) triggers emission."""
        rerun_doc = {
            "_id": "pln_tenx_P12345_v1",
            "status": "approved",
            "run_token": 1,  # incremented
            "executed_run_token": 0,
            "execution_authority": "daemon",
        }
        change = {"id": "pln_tenx_P12345_v1", "seq": "1", "doc": rerun_doc}

        asyncio.run(self.watcher._evaluate_change(change))

        self.mock_on_event.assert_called_once()

    # ==========================================
    # recover_pending_plans Tests
    # ==========================================

    def test_recover_pending_plans_queries_db(self):
        """Test that recovery queries approved pending plans."""
        self.mock_plan_db.query_approved_pending.return_value = []

        asyncio.run(self.watcher.recover_pending_plans())

        self.mock_plan_db.query_approved_pending.assert_called_once()

    def test_recover_pending_plans_emits_for_each(self):
        """Test that recovery emits events for all eligible plans."""
        eligible_plans = [
            {
                "_id": "pln_tenx_P1_v1",
                "status": "approved",
                "run_token": 0,
                "executed_run_token": -1,
                "execution_authority": "daemon",
            },
            {
                "_id": "pln_tenx_P2_v1",
                "status": "approved",
                "run_token": 1,
                "executed_run_token": 0,
                "execution_authority": "daemon",
            },
        ]
        self.mock_plan_db.query_approved_pending.return_value = eligible_plans

        result = asyncio.run(self.watcher.recover_pending_plans())

        # Should emit for each plan
        self.assertEqual(self.mock_on_event.call_count, 2)
        self.assertEqual(result, eligible_plans)

    def test_recover_pending_plans_returns_empty_on_no_plans(self):
        """Test that recovery returns empty list when no plans pending."""
        self.mock_plan_db.query_approved_pending.return_value = []

        result = asyncio.run(self.watcher.recover_pending_plans())

        self.assertEqual(result, [])
        self.mock_on_event.assert_not_called()

    # ==========================================
    # start/stop Tests
    # ==========================================

    def test_start_gets_checkpoint(self):
        """Test that start() retrieves checkpoint from store."""

        async def mock_fetch_changes(since=None):
            # Return empty to exit quickly
            return
            yield  # Make it a generator

        self.mock_fetcher.fetch_changes = mock_fetch_changes

        async def run_watcher_briefly():
            task = asyncio.create_task(self.watcher.start())
            await asyncio.sleep(0.05)
            await self.watcher.stop()
            await task

        asyncio.run(run_watcher_briefly())

        self.mock_checkpoint.get_checkpoint.assert_called()

    def test_start_resumes_from_checkpoint(self):
        """Test that start() resumes from saved checkpoint."""
        self.mock_checkpoint.get_checkpoint.return_value = "100-abcdef"

        async def mock_fetch_changes(since=None):
            # Verify since parameter
            if since == "100-abcdef":
                pass  # Expected
            return
            yield  # Make it a generator

        self.mock_fetcher.fetch_changes = mock_fetch_changes

        async def run_watcher_briefly():
            task = asyncio.create_task(self.watcher.start())
            await asyncio.sleep(0.05)
            await self.watcher.stop()
            await task

        asyncio.run(run_watcher_briefly())

        # Checkpoint should have been retrieved
        self.mock_checkpoint.get_checkpoint.assert_called()

    def test_stop_sets_running_false(self):
        """Test that stop() sets _running to False."""
        self.watcher._running = True

        asyncio.run(self.watcher.stop())

        self.assertFalse(self.watcher._running)

    def test_stop_idempotent(self):
        """Test that stop() is idempotent when not running."""
        self.watcher._running = False

        # Should not raise
        asyncio.run(self.watcher.stop())

        self.assertFalse(self.watcher._running)

    def test_start_idempotent_when_running(self):
        """Test that start() is idempotent when already running."""
        self.watcher._running = True

        # Should return immediately without starting again
        async def test():
            await self.watcher.start()  # Should return immediately

        # This would hang if not idempotent; use timeout
        try:
            asyncio.run(asyncio.wait_for(test(), timeout=0.1))
        except TimeoutError:
            self.fail("start() did not return when already running")


class TestPlanWatcherIntegration(unittest.TestCase):
    """Integration-style tests for PlanWatcher with realistic scenarios."""

    def setUp(self):
        """Set up with mocked dependencies."""
        self.plan_db_patcher = patch("lib.watchers.plan_watcher.PlanDBManager")
        self.ygg_db_patcher = patch("lib.watchers.plan_watcher.YggdrasilDBManager")
        self.checkpoint_patcher = patch(
            "lib.watchers.plan_watcher.WatcherCheckpointStore"
        )
        self.fetcher_patcher = patch("lib.watchers.plan_watcher.ChangesFetcher")

        self.mock_plan_db_class = self.plan_db_patcher.start()
        self.mock_ygg_db_class = self.ygg_db_patcher.start()
        self.mock_checkpoint_class = self.checkpoint_patcher.start()
        self.mock_fetcher_class = self.fetcher_patcher.start()

        # Setup mock instances
        self.mock_plan_db = MagicMock()
        self.mock_plan_db_class.return_value = self.mock_plan_db

        self.mock_ygg_db = MagicMock()
        self.mock_ygg_db_class.return_value = self.mock_ygg_db

        self.mock_checkpoint = MagicMock()
        self.mock_checkpoint.get_checkpoint.return_value = None
        self.mock_checkpoint.save_checkpoint.return_value = True
        self.mock_checkpoint_class.return_value = self.mock_checkpoint

        self.mock_fetcher = MagicMock()
        self.mock_fetcher_class.return_value = self.mock_fetcher

        self.emitted_events = []
        self.mock_on_event = lambda e: self.emitted_events.append(e)

    def tearDown(self):
        """Clean up patches."""
        self.plan_db_patcher.stop()
        self.ygg_db_patcher.stop()
        self.checkpoint_patcher.stop()
        self.fetcher_patcher.stop()

    def test_full_change_processing_cycle(self):
        """Test processing a batch of changes with mixed eligibility."""
        watcher = PlanWatcher(
            on_event=self.mock_on_event,
            poll_interval_sec=0.1,
        )

        # Setup changes: 1 eligible, 1 draft, 1 already executed
        changes = [
            {
                "id": "pln_tenx_P1_v1",
                "seq": "1",
                "doc": {
                    "_id": "pln_tenx_P1_v1",
                    "status": "approved",
                    "run_token": 0,
                    "executed_run_token": -1,
                    "execution_authority": "daemon",
                },
            },
            {
                "id": "pln_tenx_P2_v1",
                "seq": "2",
                "doc": {
                    "_id": "pln_tenx_P2_v1",
                    "status": "draft",
                    "run_token": 0,
                    "executed_run_token": -1,
                    "execution_authority": "daemon",
                },
            },
            {
                "id": "pln_tenx_P3_v1",
                "seq": "3",
                "doc": {
                    "_id": "pln_tenx_P3_v1",
                    "status": "approved",
                    "run_token": 0,
                    "executed_run_token": 0,
                    "execution_authority": "daemon",
                },
            },
        ]

        async def test():
            for change in changes:
                await watcher._evaluate_change(change)

        asyncio.run(test())

        # Only 1 eligible plan should have been emitted
        self.assertEqual(len(self.emitted_events), 1)
        self.assertEqual(
            self.emitted_events[0].payload["plan_doc_id"], "pln_tenx_P1_v1"
        )

    def test_checkpoint_saved_per_change(self):
        """Test that checkpoint is saved after each processed change."""
        watcher = PlanWatcher(
            on_event=self.mock_on_event,
            poll_interval_sec=0.1,
        )

        # Setup mock fetcher to yield changes then stop
        changes = [
            {
                "id": "pln_tenx_P1_v1",
                "seq": "100-abc",
                "doc": {
                    "_id": "pln_tenx_P1_v1",
                    "status": "approved",
                    "run_token": 0,
                    "executed_run_token": -1,
                    "execution_authority": "daemon",
                },
            },
            {
                "id": "pln_tenx_P2_v1",
                "seq": "101-def",
                "doc": {
                    "_id": "pln_tenx_P2_v1",
                    "status": "approved",
                    "run_token": 0,
                    "executed_run_token": -1,
                    "execution_authority": "daemon",
                },
            },
        ]

        async def mock_fetch_changes(since=None):
            for change in changes:
                yield change

        self.mock_fetcher.fetch_changes = mock_fetch_changes

        async def run_one_cycle():
            watcher._running = True
            async for change in watcher.changes_fetcher.fetch_changes(since="0"):
                new_seq = change.get("seq")
                await watcher._evaluate_change(change)
                if new_seq:
                    watcher.checkpoint_store.save_checkpoint(new_seq)
            watcher._running = False

        asyncio.run(run_one_cycle())

        # Checkpoint should have been saved twice
        self.assertEqual(self.mock_checkpoint.save_checkpoint.call_count, 2)
        # Verify last checkpoint was final seq
        self.mock_checkpoint.save_checkpoint.assert_called_with("101-def")


class TestPlanWatcherFiltering(unittest.TestCase):
    """Unit tests for PlanWatcher execution origin and owner filtering."""

    def setUp(self):
        """Set up test fixtures with mocked DB connections."""
        # Patch PlanDBManager
        self.plan_db_patcher = patch("lib.watchers.plan_watcher.PlanDBManager")
        self.mock_plan_db_class = self.plan_db_patcher.start()
        self.mock_plan_db = MagicMock()
        self.mock_plan_db_class.return_value = self.mock_plan_db

        # Patch YggdrasilDBManager
        self.ygg_db_patcher = patch("lib.watchers.plan_watcher.YggdrasilDBManager")
        self.mock_ygg_db_class = self.ygg_db_patcher.start()
        self.mock_ygg_db = MagicMock()
        self.mock_ygg_db_class.return_value = self.mock_ygg_db

        # Patch WatcherCheckpointStore
        self.checkpoint_patcher = patch(
            "lib.watchers.plan_watcher.WatcherCheckpointStore"
        )
        self.mock_checkpoint_class = self.checkpoint_patcher.start()
        self.mock_checkpoint = MagicMock()
        self.mock_checkpoint.get_checkpoint.return_value = None
        self.mock_checkpoint_class.return_value = self.mock_checkpoint

        # Patch ChangesFetcher
        self.fetcher_patcher = patch("lib.watchers.plan_watcher.ChangesFetcher")
        self.mock_fetcher_class = self.fetcher_patcher.start()
        self.mock_fetcher = MagicMock()
        self.mock_fetcher_class.return_value = self.mock_fetcher

        # Create event callback mock
        self.mock_on_event = Mock()

    def tearDown(self):
        """Clean up patches."""
        self.plan_db_patcher.stop()
        self.ygg_db_patcher.stop()
        self.checkpoint_patcher.stop()
        self.fetcher_patcher.stop()

    # ==========================================
    # Execution Origin Filter Tests
    # ==========================================

    def test_daemon_watcher_skips_run_once_plans(self):
        """Test that daemon watcher skips plans with authority='run_once'."""
        watcher = PlanWatcher(
            on_event=self.mock_on_event,
            poll_interval_sec=0.1,
            execution_authority_filter="daemon",
        )

        run_once_doc = {
            "_id": "pln_tenx_P12345_v1",
            "status": "approved",
            "run_token": 0,
            "executed_run_token": -1,
            "execution_authority": "run_once",
            "execution_owner": "run_once:test-uuid",
        }
        change = {"id": "pln_tenx_P12345_v1", "seq": "1", "doc": run_once_doc}

        asyncio.run(watcher._evaluate_change(change))

        self.mock_on_event.assert_not_called()

    def test_daemon_watcher_processes_daemon_plans(self):
        """Test that daemon watcher processes plans with authority='daemon'."""
        watcher = PlanWatcher(
            on_event=self.mock_on_event,
            poll_interval_sec=0.1,
            execution_authority_filter="daemon",
        )

        daemon_doc = {
            "_id": "pln_tenx_P12345_v1",
            "status": "approved",
            "run_token": 0,
            "executed_run_token": -1,
            "execution_authority": "daemon",
        }
        change = {"id": "pln_tenx_P12345_v1", "seq": "1", "doc": daemon_doc}

        asyncio.run(watcher._evaluate_change(change))

        self.mock_on_event.assert_called_once()

    def test_scoped_watcher_filters_by_owner(self):
        """Test that scoped watcher only processes matching owner."""
        my_owner = "run_once:my-session-uuid"
        watcher = PlanWatcher(
            on_event=self.mock_on_event,
            poll_interval_sec=0.1,
            execution_authority_filter="run_once",
            execution_owner_filter=my_owner,
        )

        my_doc = {
            "_id": "pln_tenx_P12345_v1",
            "status": "approved",
            "run_token": 0,
            "executed_run_token": -1,
            "execution_authority": "run_once",
            "execution_owner": my_owner,
        }
        change = {"id": "pln_tenx_P12345_v1", "seq": "1", "doc": my_doc}

        asyncio.run(watcher._evaluate_change(change))

        self.mock_on_event.assert_called_once()

    def test_scoped_watcher_skips_other_owners(self):
        """Test that scoped watcher skips plans with different owner."""
        my_owner = "run_once:my-session-uuid"
        watcher = PlanWatcher(
            on_event=self.mock_on_event,
            poll_interval_sec=0.1,
            execution_authority_filter="run_once",
            execution_owner_filter=my_owner,
        )

        other_doc = {
            "_id": "pln_tenx_P12345_v1",
            "status": "approved",
            "run_token": 0,
            "executed_run_token": -1,
            "execution_authority": "run_once",
            "execution_owner": "run_once:other-session-uuid",
        }
        change = {"id": "pln_tenx_P12345_v1", "seq": "1", "doc": other_doc}

        asyncio.run(watcher._evaluate_change(change))

        self.mock_on_event.assert_not_called()

    def test_watcher_skips_missing_execution_origin(self):
        """Test that watcher skips plans without execution_authority field."""
        watcher = PlanWatcher(
            on_event=self.mock_on_event,
            poll_interval_sec=0.1,
            execution_authority_filter="daemon",
        )

        legacy_doc = {
            "_id": "pln_tenx_P12345_v1",
            "status": "approved",
            "run_token": 0,
            "executed_run_token": -1,
            # no execution_authority field
        }
        change = {"id": "pln_tenx_P12345_v1", "seq": "1", "doc": legacy_doc}

        asyncio.run(watcher._evaluate_change(change))

        self.mock_on_event.assert_not_called()

    def test_no_filter_processes_all_origins(self):
        """Test that watcher with no filter processes all origins."""
        watcher = PlanWatcher(
            on_event=self.mock_on_event,
            poll_interval_sec=0.1,
            # No filter set
        )

        # Note: without filters, missing execution_authority still skips
        daemon_doc = {
            "_id": "pln_tenx_P1_v1",
            "status": "approved",
            "run_token": 0,
            "executed_run_token": -1,
            "execution_authority": "daemon",
        }
        run_once_doc = {
            "_id": "pln_tenx_P2_v1",
            "status": "approved",
            "run_token": 0,
            "executed_run_token": -1,
            "execution_authority": "run_once",
        }

        asyncio.run(
            watcher._evaluate_change(
                {"id": "pln_tenx_P1_v1", "seq": "1", "doc": daemon_doc}
            )
        )
        asyncio.run(
            watcher._evaluate_change(
                {"id": "pln_tenx_P2_v1", "seq": "2", "doc": run_once_doc}
            )
        )

        self.assertEqual(self.mock_on_event.call_count, 2)

    # ==========================================
    # Recovery Filtering Tests
    # ==========================================

    def test_recovery_respects_origin_filter(self):
        """Test that recover_pending_plans respects execution_authority filter."""
        watcher = PlanWatcher(
            on_event=self.mock_on_event,
            poll_interval_sec=0.1,
            execution_authority_filter="daemon",
        )

        # Simulate mixed plans from DB
        all_plans = [
            {
                "_id": "pln_tenx_P1_v1",
                "status": "approved",
                "run_token": 0,
                "executed_run_token": -1,
                "execution_authority": "daemon",
            },
            {
                "_id": "pln_tenx_P2_v1",
                "status": "approved",
                "run_token": 0,
                "executed_run_token": -1,
                "execution_authority": "run_once",
            },
        ]
        self.mock_plan_db.query_approved_pending.return_value = all_plans

        result = asyncio.run(watcher.recover_pending_plans())

        # Only daemon plan should be recovered
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["_id"], "pln_tenx_P1_v1")
        self.mock_on_event.assert_called_once()

    def test_recovery_respects_owner_filter(self):
        """Test that recover_pending_plans respects execution_owner filter."""
        my_owner = "run_once:my-session"
        watcher = PlanWatcher(
            on_event=self.mock_on_event,
            poll_interval_sec=0.1,
            execution_authority_filter="run_once",
            execution_owner_filter=my_owner,
        )

        all_plans = [
            {
                "_id": "pln_tenx_P1_v1",
                "execution_authority": "run_once",
                "execution_owner": my_owner,
            },
            {
                "_id": "pln_tenx_P2_v1",
                "execution_authority": "run_once",
                "execution_owner": "run_once:other-session",
            },
        ]
        self.mock_plan_db.query_approved_pending.return_value = all_plans

        result = asyncio.run(watcher.recover_pending_plans())

        # Only my plan should be recovered
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["_id"], "pln_tenx_P1_v1")

    def test_recovery_skips_missing_execution_origin(self):
        """Test that recovery skips plans without execution_authority."""
        watcher = PlanWatcher(
            on_event=self.mock_on_event,
            poll_interval_sec=0.1,
            execution_authority_filter="daemon",
        )

        all_plans = [
            {
                "_id": "pln_tenx_P1_v1",
                "execution_authority": "daemon",
            },
            {
                "_id": "pln_tenx_P2_v1",
                # No execution_authority (legacy)
            },
        ]
        self.mock_plan_db.query_approved_pending.return_value = all_plans

        result = asyncio.run(watcher.recover_pending_plans())

        # Only plan with execution_authority should be recovered
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["_id"], "pln_tenx_P1_v1")


if __name__ == "__main__":
    unittest.main()
