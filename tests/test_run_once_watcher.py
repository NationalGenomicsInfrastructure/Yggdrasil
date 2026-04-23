"""
Unit tests for run_once_with_watcher functionality in YggdrasilCore.

Tests the CLI's --run-once execution mode which:
1. Generates execution owner tokens
2. Creates plans with execution_authority='run_once' and execution_owner
3. Uses a scoped PlanWatcher to execute plans
4. Handles ownership transfer, timeout, and interrupts
"""

import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from lib.core_utils.yggdrasil_core import (  # type: ignore[attr-defined]
    YggdrasilCore,
    _generate_run_once_owner,
)


class TestGenerateRunOnceOwner(unittest.TestCase):
    """Tests for _generate_run_once_owner() helper function."""

    def test_generates_prefixed_uuid(self):
        """Test that owner token has 'run_once:' prefix."""
        owner = _generate_run_once_owner()
        self.assertTrue(owner.startswith("run_once:"))

    def test_generates_unique_tokens(self):
        """Test that each call generates a unique token."""
        tokens = {_generate_run_once_owner() for _ in range(100)}
        self.assertEqual(len(tokens), 100)

    def test_uuid_format(self):
        """Test that the UUID portion is valid."""
        owner = _generate_run_once_owner()
        uuid_part = owner.replace("run_once:", "")
        # UUID4 format: 8-4-4-4-12 hex chars
        parts = uuid_part.split("-")
        self.assertEqual(len(parts), 5)
        self.assertEqual([len(p) for p in parts], [8, 4, 4, 4, 12])


class TestCheckPlanOverwrite(unittest.TestCase):
    """Tests for _check_plan_overwrite() method."""

    def setUp(self):
        """Set up test fixtures."""
        YggdrasilCore._instance = None
        self.mock_config = {"work_root": "/tmp/ygg_test"}
        self.mock_plan_dbm = MagicMock()

    def tearDown(self):
        """Clean up singleton."""
        YggdrasilCore._instance = None

    @patch("lib.core_utils.yggdrasil_core.OpsConsumerService")
    @patch("lib.core_utils.yggdrasil_core.FileSpoolEmitter")
    @patch("lib.core_utils.yggdrasil_core.Engine")
    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_returns_true_when_no_existing_plan(
        self, mock_init_db, mock_engine, mock_emitter, mock_ops
    ):
        """Test that method returns (plan_id, True) when no existing plan."""
        core = YggdrasilCore(self.mock_config)
        core.plan_dbm = (
            self.mock_plan_dbm
        )  # Assign mock since _init_db_managers is patched
        self.mock_plan_dbm.get_plan_summary.return_value = None

        plan_doc_id, should_continue = core._check_plan_overwrite(
            "pln_test_123", force=False
        )

        self.assertEqual(plan_doc_id, "pln_test_123")
        self.assertTrue(should_continue)

    @patch("lib.core_utils.yggdrasil_core.OpsConsumerService")
    @patch("lib.core_utils.yggdrasil_core.FileSpoolEmitter")
    @patch("lib.core_utils.yggdrasil_core.Engine")
    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_returns_false_when_existing_plan_without_force(
        self, mock_init_db, mock_engine, mock_emitter, mock_ops
    ):
        """Test that method returns (plan_id, False) when plan exists and no force."""
        core = YggdrasilCore(self.mock_config)
        core.plan_dbm = (
            self.mock_plan_dbm
        )  # Assign mock since _init_db_managers is patched
        self.mock_plan_dbm.get_plan_summary.return_value = {
            "status": "draft",
            "execution_authority": "daemon",
            "updated_at": "2024-01-01T00:00:00Z",
            "run_token": 1,
            "executed_run_token": 0,
        }

        plan_doc_id, should_continue = core._check_plan_overwrite(
            "pln_test_123", force=False
        )

        self.assertEqual(plan_doc_id, "pln_test_123")
        self.assertFalse(should_continue)

    @patch("lib.core_utils.yggdrasil_core.OpsConsumerService")
    @patch("lib.core_utils.yggdrasil_core.FileSpoolEmitter")
    @patch("lib.core_utils.yggdrasil_core.Engine")
    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_returns_true_when_existing_plan_with_force(
        self, mock_init_db, mock_engine, mock_emitter, mock_ops
    ):
        """Test that method returns (plan_id, True) when plan exists and force=True."""
        core = YggdrasilCore(self.mock_config)
        core.plan_dbm = (
            self.mock_plan_dbm
        )  # Assign mock since _init_db_managers is patched
        self.mock_plan_dbm.get_plan_summary.return_value = {
            "status": "draft",
            "execution_authority": "daemon",
            "updated_at": "2024-01-01T00:00:00Z",
            "run_token": 1,
            "executed_run_token": 0,
        }

        plan_doc_id, should_continue = core._check_plan_overwrite(
            "pln_test_123", force=True
        )

        self.assertEqual(plan_doc_id, "pln_test_123")
        self.assertTrue(should_continue)


class TestRunOnceWithWatcher(unittest.TestCase):
    """Tests for run_once_with_watcher() method."""

    def setUp(self):
        """Set up test fixtures."""
        YggdrasilCore._instance = None
        self.mock_config = {"work_root": "/tmp/ygg_test"}

    def tearDown(self):
        """Clean up singleton."""
        YggdrasilCore._instance = None

    @patch("lib.ops.sinks.couch.OpsWriter")
    @patch("lib.ops.consumer.FileSpoolConsumer")
    @patch("lib.couchdb.plan_db_manager.PlanDBManager")
    @patch("lib.couchdb.project_db_manager.ProjectDBManager")
    @patch("lib.core_utils.yggdrasil_core.OpsConsumerService")
    @patch("lib.core_utils.yggdrasil_core.FileSpoolEmitter")
    @patch("lib.core_utils.yggdrasil_core.Engine")
    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_returns_error_when_doc_not_found(
        self,
        mock_init_db,
        mock_engine,
        mock_emitter,
        mock_ops,
        mock_pdm_class,
        mock_plan_dbm_class,
        mock_consumer,
        mock_writer,
    ):
        """Test that run_once_with_watcher returns 1 when doc doesn't exist."""
        # Setup
        mock_pdm = MagicMock()
        mock_pdm.fetch_document_by_id.return_value = None
        mock_pdm_class.return_value = mock_pdm

        core = YggdrasilCore(self.mock_config)
        core.pdm = mock_pdm  # Assign mock since _init_db_managers is patched

        # Act
        result = core.run_once_with_watcher("P12345")

        # Assert
        self.assertEqual(result, 1)

    @patch("lib.ops.sinks.couch.OpsWriter")
    @patch("lib.ops.consumer.FileSpoolConsumer")
    @patch("lib.couchdb.plan_db_manager.PlanDBManager")
    @patch("lib.couchdb.project_db_manager.ProjectDBManager")
    @patch("lib.core_utils.yggdrasil_core.OpsConsumerService")
    @patch("lib.core_utils.yggdrasil_core.FileSpoolEmitter")
    @patch("lib.core_utils.yggdrasil_core.Engine")
    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_returns_error_when_no_handlers_registered(
        self,
        mock_init_db,
        mock_engine,
        mock_emitter,
        mock_ops,
        mock_pdm_class,
        mock_plan_dbm_class,
        mock_consumer,
        mock_writer,
    ):
        """Test that run_once_with_watcher returns 1 when no handlers exist."""
        # Setup
        mock_pdm = MagicMock()
        mock_pdm.fetch_document_by_id.return_value = {"_id": "P12345"}
        mock_pdm_class.return_value = mock_pdm

        core = YggdrasilCore(self.mock_config)
        core.pdm = mock_pdm  # Assign mock since _init_db_managers is patched
        # Clear any registered handlers
        from lib.core_utils.event_types import EventType

        core.subscriptions[EventType.PROJECT_CHANGE] = []

        # Act
        result = core.run_once_with_watcher("P12345")

        # Assert
        self.assertEqual(result, 1)

    @patch("lib.ops.sinks.couch.OpsWriter")
    @patch("lib.ops.consumer.FileSpoolConsumer")
    @patch("lib.couchdb.plan_db_manager.PlanDBManager")
    @patch("lib.couchdb.project_db_manager.ProjectDBManager")
    @patch("lib.core_utils.yggdrasil_core.OpsConsumerService")
    @patch("lib.core_utils.yggdrasil_core.FileSpoolEmitter")
    @patch("lib.core_utils.yggdrasil_core.Engine")
    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_creates_plan_with_run_once_origin(
        self,
        mock_init_db,
        mock_engine,
        mock_emitter,
        mock_ops,
        mock_pdm_class,
        mock_plan_dbm_class,
        mock_consumer,
        mock_writer,
    ):
        """Test that plan is created with execution_authority='run_once'."""
        # Setup
        mock_pdm = MagicMock()
        mock_pdm.fetch_document_by_id.return_value = {"_id": "P12345"}
        mock_pdm_class.return_value = mock_pdm

        mock_plan_dbm = MagicMock()
        mock_plan_dbm.get_plan_summary.return_value = None
        mock_plan_dbm.save_plan.return_value = "pln_test_12345"
        mock_plan_dbm_class.return_value = mock_plan_dbm

        core = YggdrasilCore(self.mock_config)
        core.pdm = mock_pdm  # Assign mock since _init_db_managers is patched
        core.plan_dbm = mock_plan_dbm  # Assign mock since _init_db_managers is patched

        # Mock handler
        mock_handler = MagicMock()
        mock_handler.realm_id = "test_realm"
        mock_handler.derive_scope.return_value = {"kind": "project", "id": "P12345"}
        mock_handler.class_qualified_name.return_value = "test.TestHandler"
        mock_plan = MagicMock()
        mock_plan.plan_id = "pln_test_12345"
        mock_draft = MagicMock()
        mock_draft.plan = mock_plan
        mock_draft.auto_run = True
        mock_handler.run_now.return_value = [mock_draft]

        from lib.core_utils.event_types import EventType

        core.subscriptions[EventType.PROJECT_CHANGE] = [mock_handler]

        # Mock _run_once_watcher_loop to return success
        with patch.object(
            core, "_run_once_watcher_loop", new=AsyncMock(return_value=0)
        ):
            core.run_once_with_watcher("P12345")

        # Verify save_plan was called with execution_authority='run_once'
        call_args = mock_plan_dbm.save_plan.call_args
        self.assertEqual(call_args.kwargs.get("execution_authority"), "run_once")

    @patch("lib.ops.sinks.couch.OpsWriter")
    @patch("lib.ops.consumer.FileSpoolConsumer")
    @patch("lib.couchdb.plan_db_manager.PlanDBManager")
    @patch("lib.couchdb.project_db_manager.ProjectDBManager")
    @patch("lib.core_utils.yggdrasil_core.OpsConsumerService")
    @patch("lib.core_utils.yggdrasil_core.FileSpoolEmitter")
    @patch("lib.core_utils.yggdrasil_core.Engine")
    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_creates_plan_with_execution_owner(
        self,
        mock_init_db,
        mock_engine,
        mock_emitter,
        mock_ops,
        mock_pdm_class,
        mock_plan_dbm_class,
        mock_consumer,
        mock_writer,
    ):
        """Test that plan is created with unique execution_owner token."""
        # Setup
        mock_pdm = MagicMock()
        mock_pdm.fetch_document_by_id.return_value = {"_id": "P12345"}
        mock_pdm_class.return_value = mock_pdm

        mock_plan_dbm = MagicMock()
        mock_plan_dbm.get_plan_summary.return_value = None
        mock_plan_dbm.save_plan.return_value = "pln_test_12345"
        mock_plan_dbm_class.return_value = mock_plan_dbm

        core = YggdrasilCore(self.mock_config)
        core.pdm = mock_pdm  # Assign mock since _init_db_managers is patched
        core.plan_dbm = mock_plan_dbm  # Assign mock since _init_db_managers is patched

        # Mock handler
        mock_handler = MagicMock()
        mock_handler.realm_id = "test_realm"
        mock_handler.derive_scope.return_value = {"kind": "project", "id": "P12345"}
        mock_handler.class_qualified_name.return_value = "test.TestHandler"
        mock_plan = MagicMock()
        mock_plan.plan_id = "pln_test_12345"
        mock_draft = MagicMock()
        mock_draft.plan = mock_plan
        mock_draft.auto_run = True
        mock_handler.run_now.return_value = [mock_draft]

        from lib.core_utils.event_types import EventType

        core.subscriptions[EventType.PROJECT_CHANGE] = [mock_handler]

        # Mock _run_once_watcher_loop to return success
        with patch.object(
            core, "_run_once_watcher_loop", new=AsyncMock(return_value=0)
        ):
            core.run_once_with_watcher("P12345")

        # Verify save_plan was called with execution_owner
        call_args = mock_plan_dbm.save_plan.call_args
        owner = call_args.kwargs.get("execution_owner")
        self.assertIsNotNone(owner)
        self.assertTrue(owner.startswith("run_once:"))

    @patch("lib.ops.sinks.couch.OpsWriter")
    @patch("lib.ops.consumer.FileSpoolConsumer")
    @patch("lib.couchdb.plan_db_manager.PlanDBManager")
    @patch("lib.couchdb.project_db_manager.ProjectDBManager")
    @patch("lib.core_utils.yggdrasil_core.OpsConsumerService")
    @patch("lib.core_utils.yggdrasil_core.FileSpoolEmitter")
    @patch("lib.core_utils.yggdrasil_core.Engine")
    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_force_overwrites_existing_plan(
        self,
        mock_init_db,
        mock_engine,
        mock_emitter,
        mock_ops,
        mock_pdm_class,
        mock_plan_dbm_class,
        mock_consumer,
        mock_writer,
    ):
        """Test that --force overwrites existing plan."""
        # Setup
        mock_pdm = MagicMock()
        mock_pdm.fetch_document_by_id.return_value = {"_id": "P12345"}
        mock_pdm_class.return_value = mock_pdm

        mock_plan_dbm = MagicMock()
        mock_plan_dbm.get_plan_summary.return_value = {
            "status": "draft",
            "execution_authority": "daemon",
            "updated_at": "2024-01-01T00:00:00Z",
            "run_token": 1,
            "executed_run_token": 0,
        }
        mock_plan_dbm.save_plan.return_value = "pln_test_12345"
        mock_plan_dbm_class.return_value = mock_plan_dbm

        core = YggdrasilCore(self.mock_config)
        core.pdm = mock_pdm  # Assign mock since _init_db_managers is patched
        core.plan_dbm = mock_plan_dbm  # Assign mock since _init_db_managers is patched

        # Mock handler
        mock_handler = MagicMock()
        mock_handler.realm_id = "test_realm"
        mock_handler.derive_scope.return_value = {"kind": "project", "id": "P12345"}
        mock_handler.class_qualified_name.return_value = "test.TestHandler"
        mock_plan = MagicMock()
        mock_plan.plan_id = "pln_test_12345"
        mock_draft = MagicMock()
        mock_draft.plan = mock_plan
        mock_draft.auto_run = True
        mock_handler.run_now.return_value = [mock_draft]

        from lib.core_utils.event_types import EventType

        core.subscriptions[EventType.PROJECT_CHANGE] = [mock_handler]

        # Mock _run_once_watcher_loop to return success
        with patch.object(
            core, "_run_once_watcher_loop", new=AsyncMock(return_value=0)
        ):
            result = core.run_once_with_watcher("P12345", force_overwrite=True)

        # Should succeed despite existing plan
        self.assertEqual(result, 0)
        mock_plan_dbm.save_plan.assert_called()


class TestRunOnceWatcherLoop(unittest.TestCase):
    """Tests for _run_once_watcher_loop() async method."""

    def setUp(self):
        """Set up test fixtures."""
        YggdrasilCore._instance = None
        self.mock_config = {"work_root": "/tmp/ygg_test"}
        self.mock_plan_dbm = MagicMock()

    def tearDown(self):
        """Clean up singleton."""
        YggdrasilCore._instance = None

    @patch("lib.core_utils.yggdrasil_core.PlanWatcher")
    @patch("lib.core_utils.yggdrasil_core.is_plan_eligible")
    @patch("lib.core_utils.yggdrasil_core.OpsConsumerService")
    @patch("lib.core_utils.yggdrasil_core.FileSpoolEmitter")
    @patch("lib.core_utils.yggdrasil_core.Engine")
    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_returns_one_on_timeout(
        self,
        mock_init_db,
        mock_engine_class,
        mock_emitter,
        mock_ops,
        mock_eligible,
        mock_watcher_cls,
    ):
        """Test that loop returns 1 when timeout occurs."""
        # Setup
        mock_watcher = MagicMock()
        mock_watcher.start = AsyncMock()  # Never emits events
        mock_watcher.stop = AsyncMock()
        mock_watcher_cls.return_value = mock_watcher

        core = YggdrasilCore(self.mock_config)
        core.plan_dbm = (
            self.mock_plan_dbm
        )  # Assign mock since _init_db_managers is patched

        pending_plan_ids = ["pln_1"]
        execution_owner = "run_once:test-uuid"

        # Act
        result = asyncio.run(
            core._run_once_watcher_loop(
                pending_plan_ids=pending_plan_ids,
                execution_owner=execution_owner,
                timeout_seconds=0.1,  # Very short timeout
            )
        )

        # Assert
        self.assertEqual(result, 1)


class TestCreateRunOncePlanForHandler(unittest.TestCase):
    """Tests for _create_run_once_plan_for_handler() helper method."""

    def setUp(self):
        """Set up test fixtures."""
        YggdrasilCore._instance = None
        self.mock_config = {"work_root": "/tmp/ygg_test"}
        self.mock_plan_dbm = MagicMock()

    def tearDown(self):
        """Clean up singleton."""
        YggdrasilCore._instance = None

    @patch("lib.core_utils.yggdrasil_core.OpsConsumerService")
    @patch("lib.core_utils.yggdrasil_core.FileSpoolEmitter")
    @patch("lib.core_utils.yggdrasil_core.Engine")
    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_returns_none_when_handler_lacks_derive_scope(
        self, mock_init_db, mock_engine, mock_emitter, mock_ops
    ):
        """Test that method returns empty list if handler doesn't have derive_scope."""
        core = YggdrasilCore(self.mock_config)
        core.plan_dbm = (
            self.mock_plan_dbm
        )  # Assign mock since _init_db_managers is patched

        # Mock handler without derive_scope (but with class_qualified_name method)
        mock_handler = MagicMock()
        mock_handler.class_qualified_name = MagicMock(return_value="test.TestHandler")
        delattr(mock_handler, "derive_scope")  # Remove derive_scope

        result = core._create_run_once_plan_for_handler(
            handler=mock_handler,
            doc={"_id": "P12345"},
            doc_id="P12345",
            execution_owner="run_once:test",
            force_overwrite=False,
        )

        self.assertEqual(result, [])

    @patch("lib.core_utils.yggdrasil_core.PlanDBManager")
    @patch("lib.core_utils.yggdrasil_core.OpsConsumerService")
    @patch("lib.core_utils.yggdrasil_core.FileSpoolEmitter")
    @patch("lib.core_utils.yggdrasil_core.Engine")
    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_returns_plan_doc_id_on_success(
        self, mock_init_db, mock_engine, mock_emitter, mock_ops, mock_plan_dbm_cls
    ):
        """Test that method returns plan_doc_id on successful creation."""
        core = YggdrasilCore(self.mock_config)

        # Mock the PlanDBManager class to return our mock instance
        mock_plan_dbm_cls.return_value = self.mock_plan_dbm
        core.plan_dbm = (
            self.mock_plan_dbm
        )  # Assign mock since _init_db_managers is patched

        # Mock handler
        mock_handler = MagicMock()
        mock_handler.realm_id = "test_realm"
        mock_handler.derive_scope.return_value = {"kind": "project", "id": "P12345"}
        mock_handler.class_qualified_name = MagicMock(return_value="test.TestHandler")
        mock_plan = MagicMock()
        mock_plan.plan_id = "pln_test_12345"
        mock_draft = MagicMock()
        mock_draft.plan = mock_plan
        mock_draft.auto_run = True
        mock_draft.preview = {}
        mock_draft.notes = None
        mock_handler.run_now.return_value = [mock_draft]

        self.mock_plan_dbm.get_plan_summary.return_value = None
        self.mock_plan_dbm.save_plan.return_value = "pln_test_12345"

        result = core._create_run_once_plan_for_handler(
            handler=mock_handler,
            doc={"_id": "P12345"},
            doc_id="P12345",
            execution_owner="run_once:test",
            force_overwrite=False,
        )

        self.assertEqual(result, ["pln_test_12345"])

    @patch("lib.core_utils.yggdrasil_core.OpsConsumerService")
    @patch("lib.core_utils.yggdrasil_core.FileSpoolEmitter")
    @patch("lib.core_utils.yggdrasil_core.Engine")
    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_uses_plan_id_from_draft_for_overwrite_check(
        self, mock_init_db, mock_engine, mock_emitter, mock_ops
    ):
        """Test that overwrite check uses plan_id from draft, not derived ID."""
        core = YggdrasilCore(self.mock_config)
        core.plan_dbm = (
            self.mock_plan_dbm
        )  # Assign mock since _init_db_managers is patched

        # Mock handler
        mock_handler = MagicMock()
        mock_handler.realm_id = "test_realm"
        mock_handler.derive_scope.return_value = {"kind": "project", "id": "P12345"}
        mock_handler.class_qualified_name.return_value = "test.TestHandler"
        mock_plan = MagicMock()
        mock_plan.plan_id = "pln_test_12345"
        mock_draft = MagicMock()
        mock_draft.plan = mock_plan
        mock_draft.auto_run = True
        mock_handler.run_now.return_value = [mock_draft]

        self.mock_plan_dbm.get_plan_summary.return_value = None
        self.mock_plan_dbm.save_plan.return_value = "pln_test_12345"

        core._create_run_once_plan_for_handler(
            handler=mock_handler,
            doc={"_id": "P12345"},
            doc_id="P12345",
            execution_owner="run_once:test",
            force_overwrite=False,
        )

        # get_plan_summary should be called with the plan_id from draft
        self.mock_plan_dbm.get_plan_summary.assert_called_once_with("pln_test_12345")


if __name__ == "__main__":
    unittest.main()
