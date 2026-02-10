import asyncio
import logging
import os
import unittest
from unittest.mock import AsyncMock, Mock, call, patch

from lib.core_utils.event_types import EventType
from lib.core_utils.singleton_decorator import SingletonMeta
from lib.core_utils.yggdrasil_core import YggdrasilCore
from lib.watchers.abstract_watcher import YggdrasilEvent


class TestYggdrasilCore(unittest.TestCase):
    """
    Comprehensive tests for YggdrasilCore - the central orchestrator.

    Tests initialization, singleton behavior, watcher/handler management,
    async lifecycle, event processing, and error handling scenarios.

    Note: All tests use setUp patches to avoid CouchDB connections from
    OpsConsumerService and Engine during YggdrasilCore initialization.
    """

    @classmethod
    def setUpClass(cls):
        """Set up class-level resources."""
        # Store original event loop policy
        cls.original_event_loop_policy = asyncio.get_event_loop_policy()
        # CRITICAL: Clear ALL singleton instances before this test class runs.
        # This ensures tests in this file aren't affected by singletons created
        # by tests that ran earlier in the full test suite.
        SingletonMeta._instances.clear()

    @classmethod
    def tearDownClass(cls):
        """Clean up class-level resources and reset event loop state."""
        # Reset to default event loop policy for subsequent tests
        asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
        # Clear singletons to avoid polluting tests that run after this class
        SingletonMeta._instances.clear()

    def setUp(self):
        """Set up test fixtures and clear singleton state."""
        # Clear singleton instance before each test to ensure isolation
        SingletonMeta._instances.clear()

        # Patch OpsConsumerService and Engine to avoid CouchDB connections
        self.ops_patcher = patch("lib.core_utils.yggdrasil_core.OpsConsumerService")
        self.engine_patcher = patch("lib.core_utils.yggdrasil_core.Engine")
        self.mock_ops_service_class = self.ops_patcher.start()
        self.mock_engine = self.engine_patcher.start()

        # Configure the OpsConsumerService mock instance with async methods
        self.mock_ops_service_instance = Mock()
        self.mock_ops_service_instance.start = Mock()
        self.mock_ops_service_instance.stop = AsyncMock()
        self.mock_ops_service_class.return_value = self.mock_ops_service_instance

        # Sample configuration for testing
        self.test_config = {
            "couchdb": {
                "poll_interval": 10,
            },
            "some_other_setting": "value",
        }

        # Mock logger for testing
        self.mock_logger = Mock(spec=logging.Logger)

        # Sample event for testing
        self.test_event = YggdrasilEvent(
            event_type=EventType.PROJECT_CHANGE,
            payload={
                "doc": {"id": "test_doc", "project_id": "test_project"},
                "scope": {"kind": "project", "id": "test_project"},
            },
            source="TestSource",
        )

    def tearDown(self):
        """Clean up after each test."""
        # Stop patchers
        self.ops_patcher.stop()
        self.engine_patcher.stop()
        # Clear singleton state after each test
        SingletonMeta._instances.clear()

    # =====================================================
    # INITIALIZATION AND SINGLETON TESTS
    # =====================================================

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_initialization_with_config_and_logger(self, mock_init_db):
        """Test basic initialization with config and custom logger."""
        # Act
        core = YggdrasilCore(self.test_config, self.mock_logger)

        # Assert
        self.assertEqual(core.config, self.test_config)
        self.assertEqual(core._logger, self.mock_logger)
        self.assertFalse(core._running)
        self.assertEqual(core.watchers, [])
        self.assertEqual(core.subscriptions, {})

        mock_init_db.assert_called_once()
        self.mock_logger.info.assert_called_with("YggdrasilCore initialized.")

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_initialization_with_default_logger(self, mock_init_db):
        """Test initialization with default logger when none provided."""
        with patch("logging.getLogger") as mock_get_logger:
            mock_default_logger = Mock(spec=logging.Logger)
            mock_get_logger.return_value = mock_default_logger

            # Act
            core = YggdrasilCore(self.test_config)

            # Assert
            self.assertEqual(core._logger, mock_default_logger)
            mock_get_logger.assert_called_once_with("YggdrasilCore")
            mock_default_logger.info.assert_called_with("YggdrasilCore initialized.")

    def test_singleton_behavior(self):
        """Test that YggdrasilCore properly implements singleton pattern."""
        with patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers"):
            # Act - create multiple instances
            core1 = YggdrasilCore(self.test_config, self.mock_logger)
            core2 = YggdrasilCore({"different": "config"}, Mock())

            # Assert - should be the same instance
            self.assertIs(core1, core2)
            # Config should be from first instantiation
            self.assertEqual(core2.config, self.test_config)
            self.assertEqual(core2._logger, self.mock_logger)

    @patch("lib.couchdb.yggdrasil_db_manager.YggdrasilDBManager")
    @patch("lib.core_utils.yggdrasil_core.ProjectDBManager")
    @patch("lib.core_utils.yggdrasil_core.PlanDBManager")
    def test_init_db_managers_success(
        self, mock_plan_dbm_class, mock_pdm_class, mock_ydm_class
    ):
        """Test successful database manager initialization."""
        # Arrange
        mock_pdm_instance = Mock()
        mock_ydm_instance = Mock()
        mock_plan_dbm_instance = Mock()
        mock_pdm_class.return_value = mock_pdm_instance
        mock_ydm_class.return_value = mock_ydm_instance
        mock_plan_dbm_class.return_value = mock_plan_dbm_instance

        # Act
        core = YggdrasilCore(self.test_config, self.mock_logger)

        # Assert
        self.assertEqual(core.pdm, mock_pdm_instance)
        self.assertEqual(core.ydm, mock_ydm_instance)
        self.assertEqual(core.plan_dbm, mock_plan_dbm_instance)
        mock_pdm_class.assert_called_once()
        mock_ydm_class.assert_called_once()
        mock_plan_dbm_class.assert_called_once()

        expected_calls = [
            call("Initializing DB managers..."),
            call("DB managers initialized."),
            call("YggdrasilCore initialized."),
        ]
        self.mock_logger.info.assert_has_calls(expected_calls)

    @patch("lib.core_utils.yggdrasil_core.PlanDBManager")
    @patch("lib.couchdb.yggdrasil_db_manager.YggdrasilDBManager")
    @patch("lib.core_utils.yggdrasil_core.ProjectDBManager")
    def test_init_db_managers_exception(
        self, mock_pdm_class, mock_ydm_class, mock_plan_dbm_class
    ):
        """Test database manager initialization with exception."""
        # Arrange - make ProjectDBManager raise exception
        mock_pdm_class.side_effect = Exception("DB connection failed")

        # Act & Assert
        with self.assertRaises(Exception) as context:
            YggdrasilCore(self.test_config, self.mock_logger)

        self.assertIn("DB connection failed", str(context.exception))

    # =====================================================
    # WATCHER REGISTRATION AND SETUP TESTS
    # =====================================================

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_register_watcher(self, mock_init_db):
        """Test watcher registration functionality."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)
        mock_watcher = Mock()

        # Act
        core.register_watcher(mock_watcher)

        # Assert
        self.assertIn(mock_watcher, core.watchers)
        self.mock_logger.debug.assert_called_with(
            f"Registering watcher: {mock_watcher}"
        )

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_setup_cdb_watchers(self, mock_init_db):
        """Test CouchDB watcher setup."""
        from lib.watchers.couchdb_watcher import CouchDBWatcher

        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)
        core.pdm = Mock()  # Mock the ProjectDBManager
        core.pdm.fetch_changes = Mock()
        initial_watchers_count = len(core.watchers)

        # Act
        core._setup_cdb_watchers()

        # Assert - Check that a watcher was added
        self.assertEqual(len(core.watchers), initial_watchers_count + 1)

        # Verify the watcher is a CouchDBWatcher
        added_watcher = core.watchers[-1]
        self.assertIsInstance(added_watcher, CouchDBWatcher)

        # Verify watcher properties
        self.assertEqual(added_watcher.name, "ProjectDBWatcher")
        self.assertEqual(added_watcher.event_type, EventType.PROJECT_CHANGE)
        self.assertEqual(added_watcher.poll_interval, 10)  # From test config

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._setup_plan_watcher")
    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_setup_watchers(self, mock_init_db, mock_setup_plan):
        """Test main setup_watchers method."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)

        # Act
        core.setup_watchers()

        # Assert
        mock_setup_plan.assert_called_once()
        expected_calls = [call("Setting up watchers..."), call("Watchers setup done.")]
        self.mock_logger.info.assert_has_calls(expected_calls, any_order=True)

    # =====================================================
    # HANDLER REGISTRATION AND SETUP TESTS
    # =====================================================

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_register_handler(self, mock_init_db):
        """Test handler registration functionality."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)
        mock_handler = Mock()

        # Act
        core.register_handler(EventType.PROJECT_CHANGE, mock_handler)

        # Assert
        self.assertIn(EventType.PROJECT_CHANGE, core.subscriptions)
        self.assertEqual(core.subscriptions[EventType.PROJECT_CHANGE], [mock_handler])
        self.mock_logger.debug.assert_called()

    @patch("importlib.metadata.entry_points")
    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_auto_register_external_handlers_success(
        self, mock_init_db, mock_entry_points
    ):
        """Test successful auto-registration of external handlers."""
        # Arrange
        mock_handler_class = Mock()
        mock_handler_class.event_type = EventType.DELIVERY_READY
        mock_handler_instance = Mock()
        mock_handler_class.return_value = mock_handler_instance

        mock_entry_point = Mock()
        mock_entry_point.name = "test_handler"
        mock_entry_point.load.return_value = mock_handler_class

        mock_entry_points.return_value = [mock_entry_point]

        core = YggdrasilCore(self.test_config, self.mock_logger)

        # Act
        core.auto_register_external_handlers()

        # Assert
        mock_entry_points.assert_called_once_with(group="ygg.handler")
        mock_entry_point.load.assert_called_once()

        # Verify that getattr was called on the handler class to get event_type
        # and that the handler class was instantiated
        self.assertIn(EventType.DELIVERY_READY, core.subscriptions)
        self.assertEqual(
            core.subscriptions[EventType.DELIVERY_READY], [mock_handler_instance]
        )

        # Check that the log message was written
        self.mock_logger.info.assert_called()

    @patch("importlib.metadata.entry_points")
    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_auto_register_external_handlers_invalid_event_type(
        self, mock_init_db, mock_entry_points
    ):
        """Test auto-registration with invalid event type."""
        # Arrange
        mock_handler_class = Mock()
        mock_handler_class.event_type = "invalid_event_type"  # Not an EventType

        mock_entry_point = Mock()
        mock_entry_point.name = "bad_handler"
        mock_entry_point.load.return_value = mock_handler_class

        mock_entry_points.return_value = [mock_entry_point]

        core = YggdrasilCore(self.test_config, self.mock_logger)

        # Act
        core.auto_register_external_handlers()

        # Assert
        self.assertEqual(len(core.subscriptions), 0)  # No handlers should be registered
        self.mock_logger.error.assert_called()

    @patch("importlib.metadata.entry_points")
    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_auto_register_external_handlers_no_event_type(
        self, mock_init_db, mock_entry_points
    ):
        """Test auto-registration with missing event_type attribute."""
        # Arrange
        mock_handler_class = Mock()
        del mock_handler_class.event_type  # No event_type attribute

        mock_entry_point = Mock()
        mock_entry_point.name = "no_event_type_handler"
        mock_entry_point.load.return_value = mock_handler_class

        mock_entry_points.return_value = [mock_entry_point]

        core = YggdrasilCore(self.test_config, self.mock_logger)

        # Act
        core.auto_register_external_handlers()

        # Assert
        self.assertEqual(len(core.subscriptions), 0)
        self.mock_logger.error.assert_called()

    @patch("lib.handlers.bp_analysis_handler.BestPracticeAnalysisHandler")
    @patch(
        "lib.core_utils.yggdrasil_core.YggdrasilCore.auto_register_external_handlers"
    )
    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_setup_handlers(
        self, mock_init_db, mock_auto_register, mock_bp_handler_class
    ):
        """Test complete handler setup process."""
        # Arrange
        mock_bp_handler_instance = Mock()
        mock_bp_handler_class.return_value = mock_bp_handler_instance

        core = YggdrasilCore(self.test_config, self.mock_logger)

        # Act
        core.setup_handlers()

        # Assert
        mock_auto_register.assert_called_once()
        # Removed bp_handler assertions - setup_handlers no longer registers built-in handlers
        self.mock_logger.info.assert_called()

    # =====================================================
    # ASYNC LIFECYCLE TESTS
    # =====================================================

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_start_not_running(self, mock_init_db):
        """Test starting watchers when not already running."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)

        mock_watcher1 = Mock()
        mock_watcher1.start = AsyncMock()
        mock_watcher2 = Mock()
        mock_watcher2.start = AsyncMock()

        core.watchers = [mock_watcher1, mock_watcher2]

        async def test_start():
            # Act
            await core.start()

            # Assert
            self.assertTrue(core._running)
            mock_watcher1.start.assert_called_once()
            mock_watcher2.start.assert_called_once()

            expected_calls = [
                call("Starting all watchers..."),
                call("Running 2 watchers in parallel."),
                call("All watchers have exited or been stopped."),
            ]
            self.mock_logger.info.assert_has_calls(expected_calls, any_order=True)

        # Run the async test
        asyncio.run(test_start())

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_start_already_running(self, mock_init_db):
        """Test starting watchers when already running."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)
        core._running = True

        async def test_start():
            # Act
            await core.start()

            # Assert
            self.mock_logger.warning.assert_called_with(
                "YggdrasilCore is already running."
            )

        asyncio.run(test_start())

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_stop_when_running(self, mock_init_db):
        """Test stopping watchers when running."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)
        core._running = True

        mock_watcher1 = Mock()
        mock_watcher1.stop = AsyncMock()
        mock_watcher2 = Mock()
        mock_watcher2.stop = AsyncMock()

        core.watchers = [mock_watcher1, mock_watcher2]

        async def test_stop():
            # Act
            await core.stop()

            # Assert
            self.assertFalse(core._running)
            mock_watcher1.stop.assert_called_once()
            mock_watcher2.stop.assert_called_once()

            expected_calls = [
                call("Stopping all watchers..."),
                call("All watchers stopped."),
            ]
            self.mock_logger.info.assert_has_calls(expected_calls)

        asyncio.run(test_stop())

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_stop_when_not_running(self, mock_init_db):
        """Test stopping watchers when not running."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)
        core._running = False

        async def test_stop():
            # Act
            await core.stop()

            # Assert
            self.mock_logger.debug.assert_called_with(
                "YggdrasilCore stop called, but not running."
            )

        asyncio.run(test_stop())

    # =====================================================
    # EVENT HANDLING TESTS
    # =====================================================

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._generate_and_persist_plan")
    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_handle_event_with_registered_handler(
        self, mock_init_db, mock_generate_plan
    ):
        """Test event handling with registered handler."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)
        mock_handler = Mock()
        mock_handler.class_qualified_name = Mock(return_value="MockHandler")
        mock_handler.class_key = Mock(return_value=("test", "MockHandler"))
        mock_handler.realm_id = "test_realm"
        mock_handler.derive_scope = Mock(
            return_value={"kind": "project", "id": "test_project"}
        )
        core.register_handler(EventType.PROJECT_CHANGE, mock_handler)

        # Act - handle_event schedules async task
        core.handle_event(self.test_event)

        # Assert - _generate_and_persist_plan should have been scheduled
        # Note: asyncio.create_task is called, so we just check it was attempted
        # The event has 'scope' in payload, so derive_scope is NOT called
        self.mock_logger.info.assert_called()
        self.mock_logger.debug.assert_called()  # Logs scheduling message

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_handle_event_no_handler(self, mock_init_db):
        """Test event handling when no handler is registered."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)

        # Act
        core.handle_event(self.test_event)

        # Assert
        self.mock_logger.info.assert_called()
        self.mock_logger.warning.assert_called()

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._generate_and_persist_plan")
    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_handle_event_handler_exception(self, mock_init_db, mock_generate_plan):
        """Test event handling when _make_planning_ctx raises exception."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)
        mock_handler = Mock()
        mock_handler.class_qualified_name = Mock(return_value="MockHandler")
        mock_handler.class_key = Mock(return_value=("test", "MockHandler"))
        mock_handler.realm_id = "test_realm"
        core.register_handler(EventType.PROJECT_CHANGE, mock_handler)

        # Create an event without scope to trigger derive_scope
        test_event_no_scope = YggdrasilEvent(
            event_type=EventType.PROJECT_CHANGE,
            payload={"doc": {"id": "test_doc", "project_id": "test_project"}},
            source="TestSource",
        )

        # Make derive_scope raise an exception
        mock_handler.derive_scope.side_effect = Exception("derive_scope failed")

        # Act - exception in derive_scope should be caught and logged
        core.handle_event(test_event_no_scope)

        # Assert - derive_scope should have been called and exception logged
        mock_handler.derive_scope.assert_called_once()
        self.mock_logger.error.assert_called()

    # =====================================================
    # CLI AND RUN_ONCE TESTS
    # =====================================================

    @patch("lib.ops.sinks.couch.OpsWriter")
    @patch("lib.ops.consumer.FileSpoolConsumer")
    @patch("lib.couchdb.project_db_manager.ProjectDBManager")
    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_run_once_success(
        self, mock_init_db, mock_pdm_class, mock_consumer_class, mock_ops_writer
    ):
        """Test successful run_once execution."""
        # Arrange
        mock_pdm_instance = Mock()
        mock_doc = {"id": "test_doc", "project_id": "test_project", "data": "test"}
        mock_pdm_instance.fetch_document_by_id.return_value = mock_doc
        mock_pdm_class.return_value = mock_pdm_instance

        mock_consumer_instance = Mock()
        mock_consumer_class.return_value = mock_consumer_instance

        mock_handler = Mock()
        mock_handler.run_now = Mock()
        mock_handler.class_qualified_name = Mock(return_value="MockHandler")
        mock_handler.class_key = Mock(return_value=("test", "MockHandler"))
        mock_handler.realm_id = "test_realm"
        mock_handler.derive_scope = Mock(
            return_value={"kind": "project", "id": "test_project"}
        )

        core = YggdrasilCore(self.test_config, self.mock_logger)
        core.pdm = mock_pdm_instance  # Assign since _init_db_managers is patched
        core.register_handler(EventType.PROJECT_CHANGE, mock_handler)

        # Act
        core.run_once("test_doc_id")

        # Assert
        mock_pdm_instance.fetch_document_by_id.assert_called_once_with("test_doc_id")
        mock_handler.derive_scope.assert_called_once_with(mock_doc)
        mock_handler.run_now.assert_called_once()
        # Verify consumer was called
        mock_consumer_instance.consume.assert_called_once()

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_run_once_document_not_found(self, mock_init_db):
        """Test run_once when document is not found."""
        # Arrange
        mock_pdm = Mock()
        mock_pdm.fetch_document_by_id.return_value = None

        core = YggdrasilCore(self.test_config, self.mock_logger)
        core.pdm = mock_pdm  # Assign since _init_db_managers is patched

        # Act
        core.run_once("nonexistent_doc")

        # Assert
        self.mock_logger.error.assert_called()

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_run_once_no_handler(self, mock_init_db):
        """Test run_once when no handler is registered."""
        # Arrange
        mock_pdm = Mock()
        mock_doc = {"id": "test_doc", "project_id": "test_project"}
        mock_pdm.fetch_document_by_id.return_value = mock_doc

        core = YggdrasilCore(self.test_config, self.mock_logger)
        core.pdm = mock_pdm  # Assign since _init_db_managers is patched
        # No handler registered

        # Act
        core.run_once("test_doc_id")

        # Assert
        self.mock_logger.error.assert_called()

    @patch("lib.ops.sinks.couch.OpsWriter")
    @patch("lib.ops.consumer.FileSpoolConsumer")
    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_run_once_handler_no_run_now_method(
        self, mock_init_db, mock_consumer_class, mock_ops_writer
    ):
        """Test run_once when handler doesn't have run_now method."""
        # Arrange
        mock_pdm = Mock()
        mock_doc = {"id": "test_doc", "project_id": "test_project"}
        mock_pdm.fetch_document_by_id.return_value = mock_doc

        mock_handler = Mock()
        del mock_handler.run_now  # Remove run_now method
        mock_handler.class_qualified_name = Mock(return_value="MockHandler")
        mock_handler.class_key = Mock(return_value=("test", "MockHandler"))
        mock_handler.realm_id = "test_realm"
        mock_handler.derive_scope = Mock(
            return_value={"kind": "project", "id": "test_project"}
        )

        core = YggdrasilCore(self.test_config, self.mock_logger)
        core.pdm = mock_pdm  # Assign since _init_db_managers is patched
        core.register_handler(EventType.PROJECT_CHANGE, mock_handler)

        # Act
        core.run_once("test_doc_id")

        # Assert - should handle the exception gracefully
        self.mock_logger.exception.assert_called()

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_process_cli_command(self, mock_init_db):
        """Test CLI command processing."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)

        # Act
        core.process_cli_command("test_command", arg1="value1", arg2="value2")

        # Assert
        expected_kwargs = {"arg1": "value1", "arg2": "value2"}
        self.mock_logger.info.assert_called_with(
            f"Processing CLI command 'test_command' with args={expected_kwargs}"
        )

    # =====================================================
    # EDGE CASES AND ERROR SCENARIOS
    # =====================================================

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_empty_config(self, mock_init_db):
        """Test initialization with empty configuration."""
        # Act
        core = YggdrasilCore({}, self.mock_logger)

        # Assert
        self.assertEqual(core.config, {})
        # Should still initialize successfully
        self.assertIsInstance(core.watchers, list)
        self.assertIsInstance(core.subscriptions, dict)

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_cdb_watcher_setup_uses_default_poll_interval(self, mock_init_db):
        """Test CouchDB watcher setup uses default poll interval when not in config."""

        # Arrange
        config_no_poll = {}
        core = YggdrasilCore(config_no_poll, self.mock_logger)
        core.pdm = Mock()
        core.pdm.fetch_changes = Mock()
        initial_watchers_count = len(core.watchers)

        # Act
        core._setup_cdb_watchers()

        # Assert - Check that a watcher was added
        self.assertEqual(len(core.watchers), initial_watchers_count + 1)

        # Verify the watcher uses default poll interval
        added_watcher = core.watchers[-1]
        self.assertEqual(added_watcher.poll_interval, 5)  # Default value

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_multiple_watchers_and_handlers(self, mock_init_db):
        """Test registering multiple watchers and handlers."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)

        mock_watcher1 = Mock()
        mock_watcher2 = Mock()
        mock_handler1 = Mock()
        mock_handler2 = Mock()

        # Act
        core.register_watcher(mock_watcher1)
        core.register_watcher(mock_watcher2)
        core.register_handler(EventType.PROJECT_CHANGE, mock_handler1)
        core.register_handler(EventType.FLOWCELL_READY, mock_handler2)

        # Assert
        self.assertEqual(len(core.watchers), 2)
        self.assertIn(mock_watcher1, core.watchers)
        self.assertIn(mock_watcher2, core.watchers)

        self.assertEqual(len(core.subscriptions), 2)
        self.assertIn(EventType.PROJECT_CHANGE, core.subscriptions)
        self.assertIn(EventType.FLOWCELL_READY, core.subscriptions)

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_start_with_watcher_exception(self, mock_init_db):
        """Test starting watchers when one raises an exception."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)

        mock_watcher1 = Mock()
        mock_watcher1.start = AsyncMock()
        mock_watcher2 = Mock()
        mock_watcher2.start = AsyncMock(side_effect=Exception("Watcher failed"))

        core.watchers = [mock_watcher1, mock_watcher2]

        async def test_start():
            # Act
            await core.start()

            # Assert - should still complete despite exception
            self.assertTrue(core._running)
            mock_watcher1.start.assert_called_once()
            mock_watcher2.start.assert_called_once()

        asyncio.run(test_start())

    # =====================================================
    # PLAN PERSISTENCE AND GENERATION TESTS
    # =====================================================

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_persist_plan_draft_daemon_mode(self, mock_init_db):
        """Test persisting a plan draft with daemon execution authority."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)
        core.plan_dbm = Mock()
        core.plan_dbm.save_plan.return_value = "pln_test_123"

        mock_plan = Mock()
        mock_plan.plan_id = "pln_test_123"
        mock_plan.scope = {"kind": "project", "id": "P12345"}

        mock_draft = Mock()
        mock_draft.plan = mock_plan
        mock_draft.auto_run = False
        mock_draft.preview = "Test plan"
        mock_draft.notes = "Test notes"

        # Act
        result = core._persist_plan_draft(mock_draft, "test_realm")

        # Assert
        self.assertEqual(result, "pln_test_123")
        core.plan_dbm.save_plan.assert_called_once_with(
            plan=mock_plan,
            realm="test_realm",
            scope={"kind": "project", "id": "P12345"},
            auto_run=False,
            execution_authority="daemon",
            execution_owner=None,
            preview="Test plan",
            notes="Test notes",
        )

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_persist_plan_draft_run_once_mode(self, mock_init_db):
        """Test persisting a plan draft with run_once execution authority."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)
        core.plan_dbm = Mock()
        core.plan_dbm.save_plan.return_value = "pln_test_456"

        mock_plan = Mock()
        mock_plan.plan_id = "pln_test_456"
        mock_plan.scope = {"kind": "project", "id": "P12345"}

        mock_draft = Mock()
        mock_draft.plan = mock_plan
        mock_draft.auto_run = True
        mock_draft.preview = None
        mock_draft.notes = None

        # Act
        result = core._persist_plan_draft(
            mock_draft,
            "test_realm",
            execution_authority="run_once",
            execution_owner="run_once:abc123",
        )

        # Assert
        self.assertEqual(result, "pln_test_456")
        core.plan_dbm.save_plan.assert_called_once()
        call_kwargs = core.plan_dbm.save_plan.call_args.kwargs
        self.assertEqual(call_kwargs["execution_authority"], "run_once")
        self.assertEqual(call_kwargs["execution_owner"], "run_once:abc123")
        self.assertTrue(call_kwargs["auto_run"])

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_derive_realm_id_from_handler_attribute(self, mock_init_db):
        """Test deriving realm_id when handler has explicit realm_id attribute."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)
        mock_handler = Mock()
        mock_handler.realm_id = "explicit_realm"
        mock_handler.__class__ = Mock
        mock_handler.__class__.__name__ = "TestHandler"

        # Act
        core._derive_realm_id(mock_handler)

        # Assert
        self.assertEqual(mock_handler.realm_id, "explicit_realm")
        self.assertIn("explicit_realm", core._legacy_realm_class_registry)

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_derive_realm_id_from_entry_point(self, mock_init_db):
        """Test deriving realm_id from entry point distribution name."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)
        mock_handler = Mock()
        del mock_handler.realm_id  # No explicit realm_id
        mock_handler.__module__ = "test_module.handlers"
        mock_handler.__class__ = Mock

        mock_ep = Mock()
        mock_ep.dist = Mock()
        mock_ep.dist.name = "test-realm-package"

        # Act
        core._derive_realm_id(mock_handler, mock_ep)

        # Assert
        self.assertEqual(mock_handler.realm_id, "test_realm_package")
        self.assertIn("test_realm_package", core._legacy_realm_class_registry)

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_derive_realm_id_from_module_name(self, mock_init_db):
        """Test deriving realm_id from module name when no entry point."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)
        mock_handler = Mock()
        del mock_handler.realm_id  # No explicit realm_id
        mock_handler.__module__ = "my_realm.handlers.project"
        mock_handler.__class__ = Mock

        # Act
        core._derive_realm_id(mock_handler, None)

        # Assert
        self.assertEqual(mock_handler.realm_id, "my_realm")
        self.assertIn("my_realm", core._legacy_realm_class_registry)

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_derive_realm_id_duplicate_raises_error(self, mock_init_db):
        """Test that duplicate realm_ids from different classes raise an error."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)

        # First handler
        mock_handler1 = Mock()
        mock_handler1.realm_id = "duplicate_realm"
        mock_handler1.__class__ = type("Handler1", (), {})

        # Second handler with same realm_id but different class
        mock_handler2 = Mock()
        mock_handler2.realm_id = "duplicate_realm"
        mock_handler2.__class__ = type("Handler2", (), {})
        mock_handler2.__module__ = "test.module"
        mock_handler2.__qualname__ = "Handler2"

        # Act & Assert
        core._derive_realm_id(mock_handler1)
        with self.assertRaises(RuntimeError) as context:
            core._derive_realm_id(mock_handler2)

        self.assertIn("Duplicate realm_id", str(context.exception))

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_make_planning_ctx(self, mock_init_db):
        """Test creating a PlanningContext from handler and scope."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)
        mock_handler = Mock()
        mock_handler.realm_id = "test_realm"

        scope = {"kind": "project", "id": "P12345"}
        doc = {"_id": "P12345", "project_name": "Test Project"}
        reason = "test_reason"

        # Act
        with patch.dict(
            os.environ,
            {"YGG_WORK_ROOT": "/tmp/ygg_work", "YGG_EVENT_SPOOL": "/tmp/ygg_events"},
        ):
            ctx = core._make_planning_ctx(mock_handler, scope, doc=doc, reason=reason)

        # Assert
        self.assertEqual(ctx.realm, "test_realm")
        self.assertEqual(ctx.scope, scope)
        self.assertEqual(ctx.source_doc, doc)
        self.assertEqual(ctx.reason, reason)
        self.assertIn("test_realm", str(ctx.scope_dir))
        self.assertIn("P12345", str(ctx.scope_dir))

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_as_event_type_with_event_type_enum(self, mock_init_db):
        """Test _as_event_type with proper EventType enum."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)

        # Act
        result = core._as_event_type(EventType.PROJECT_CHANGE)

        # Assert
        self.assertEqual(result, EventType.PROJECT_CHANGE)

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_as_event_type_with_matching_value(self, mock_init_db):
        """Test _as_event_type with enum having matching value."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)

        from enum import Enum

        class OtherEventType(str, Enum):
            PROJECT_CHANGE = "project_change"

        # Act
        result = core._as_event_type(OtherEventType.PROJECT_CHANGE)

        # Assert
        self.assertEqual(result, EventType.PROJECT_CHANGE)

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_as_event_type_with_string(self, mock_init_db):
        """Test _as_event_type with raw string value."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)

        # Act
        result = core._as_event_type("flowcell_ready")

        # Assert
        self.assertEqual(result, EventType.FLOWCELL_READY)

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_as_event_type_with_invalid_value(self, mock_init_db):
        """Test _as_event_type with invalid value returns None."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)

        # Act
        result = core._as_event_type("invalid_event_type")

        # Assert
        self.assertIsNone(result)

    # =====================================================
    # PLAN EXECUTION TESTS
    # =====================================================

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_handle_plan_execution_event_success(self, mock_init_db):
        """Test handling PLAN_EXECUTION event from PlanWatcher."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)
        core._execute_approved_plan = AsyncMock()

        plan_event = YggdrasilEvent(
            event_type=EventType.PLAN_EXECUTION,
            payload={
                "plan_doc_id": "pln_test_123",
                "plan_doc": {"_id": "pln_test_123"},
            },
            source="PlanWatcher",
        )

        # Act
        with patch("asyncio.create_task") as mock_create_task:
            core._handle_plan_execution_event(plan_event)

            # Assert
            mock_create_task.assert_called_once()

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_handle_plan_execution_event_wrong_type(self, mock_init_db):
        """Test handling event with wrong type."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)

        wrong_event = YggdrasilEvent(
            event_type=EventType.PROJECT_CHANGE,
            payload={},
            source="Test",
        )

        # Act
        core._handle_plan_execution_event(wrong_event)

        # Assert
        self.mock_logger.warning.assert_called()

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_handle_plan_execution_event_missing_plan_id(self, mock_init_db):
        """Test handling PLAN_EXECUTION event without plan_doc_id."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)

        plan_event = YggdrasilEvent(
            event_type=EventType.PLAN_EXECUTION, payload={}, source="PlanWatcher"
        )

        # Act
        core._handle_plan_execution_event(plan_event)

        # Assert
        self.mock_logger.error.assert_called()

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_execute_approved_plan_success(self, mock_init_db):
        """Test successfully executing an approved plan."""

        async def test_execute():
            # Arrange
            core = YggdrasilCore(self.test_config, self.mock_logger)
            core.plan_dbm = Mock()
            core.plan_dbm.fetch_plan.return_value = {
                "_id": "pln_test_123",
                "status": "approved",
                "run_token": 1,
                "executed_run_token": 0,
                "realm": "test_realm",
            }

            mock_plan_model = Mock()
            core.plan_dbm.fetch_plan_as_model.return_value = mock_plan_model
            core.plan_dbm.update_executed_token.return_value = True

            core.engine = Mock()
            core.engine.run = Mock()

            with patch(
                "lib.core_utils.yggdrasil_core.is_plan_eligible", return_value=True
            ):
                # Act
                await core._execute_approved_plan("pln_test_123")

                # Assert
                core.plan_dbm.fetch_plan.assert_called_once_with("pln_test_123")
                core.engine.run.assert_called_once_with(mock_plan_model)
                core.plan_dbm.update_executed_token.assert_called_once_with(
                    "pln_test_123", 1
                )

        asyncio.run(test_execute())

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_execute_approved_plan_not_eligible(self, mock_init_db):
        """Test skipping execution when plan is not eligible."""

        async def test_execute():
            # Arrange
            core = YggdrasilCore(self.test_config, self.mock_logger)
            core.plan_dbm = Mock()
            core.plan_dbm.fetch_plan.return_value = {
                "_id": "pln_test_123",
                "status": "draft",  # Not approved
            }

            with patch(
                "lib.core_utils.yggdrasil_core.is_plan_eligible", return_value=False
            ):
                # Act
                await core._execute_approved_plan("pln_test_123")

                # Assert
                core.plan_dbm.fetch_plan_as_model.assert_not_called()

        asyncio.run(test_execute())

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_execute_approved_plan_not_found(self, mock_init_db):
        """Test executing plan that doesn't exist in database."""

        async def test_execute():
            # Arrange
            core = YggdrasilCore(self.test_config, self.mock_logger)
            core.plan_dbm = Mock()
            core.plan_dbm.fetch_plan.return_value = None

            # Act
            await core._execute_approved_plan("pln_nonexistent")

            # Assert
            self.mock_logger.error.assert_called()
            core.plan_dbm.fetch_plan_as_model.assert_not_called()

        asyncio.run(test_execute())

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_execute_approved_plan_engine_failure(self, mock_init_db):
        """Test handling engine execution failure."""

        async def test_execute():
            # Arrange
            core = YggdrasilCore(self.test_config, self.mock_logger)
            core.plan_dbm = Mock()
            core.plan_dbm.fetch_plan.return_value = {
                "_id": "pln_test_123",
                "status": "approved",
                "run_token": 1,
                "executed_run_token": 0,
            }
            core.plan_dbm.fetch_plan_as_model.return_value = Mock()
            core.engine = Mock()
            core.engine.run = Mock(side_effect=Exception("Engine failed"))

            with patch(
                "lib.core_utils.yggdrasil_core.is_plan_eligible", return_value=True
            ):
                # Act
                await core._execute_approved_plan("pln_test_123")

                # Assert - should log exception but not crash
                self.mock_logger.exception.assert_called()
                # Token should NOT be updated on failure
                core.plan_dbm.update_executed_token.assert_not_called()

        asyncio.run(test_execute())

    # =====================================================
    # CREATE_PLAN_FROM_DOC TESTS (--plan-only mode)
    # =====================================================

    @patch("lib.ops.sinks.couch.OpsWriter")
    @patch("lib.ops.consumer.FileSpoolConsumer")
    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_create_plan_from_doc_success(
        self, mock_init_db, mock_consumer, mock_writer
    ):
        """Test creating a plan without execution (--plan-only mode)."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)
        core.pdm = Mock()
        core.pdm.fetch_document_by_id.return_value = {"_id": "P12345"}

        core.plan_dbm = Mock()
        core.plan_dbm.get_plan_summary.return_value = None  # No existing plan
        core.plan_dbm.save_plan.return_value = "pln_test_123"

        # Mock handler
        mock_handler = Mock()
        mock_handler.realm_id = "test_realm"
        mock_handler.derive_scope.return_value = {"kind": "project", "id": "P12345"}
        mock_handler.class_qualified_name.return_value = "test.TestHandler"

        mock_plan = Mock()
        mock_plan.plan_id = "pln_test_123"
        mock_plan.scope = {"kind": "project", "id": "P12345"}

        mock_draft = Mock()
        mock_draft.plan = mock_plan
        mock_draft.auto_run = True  # Will be forced to False
        mock_draft.preview = "Test plan"
        mock_draft.notes = "Test notes"

        mock_handler.run_now.return_value = mock_draft

        core.subscriptions[EventType.PROJECT_CHANGE] = [mock_handler]

        # Act
        result = core.create_plan_from_doc("P12345", force_overwrite=False)

        # Assert
        self.assertEqual(result, "pln_test_123")
        # Verify plan was created with execution_authority='daemon'
        call_kwargs = core.plan_dbm.save_plan.call_args.kwargs
        self.assertEqual(call_kwargs["execution_authority"], "daemon")
        self.assertIsNone(call_kwargs["execution_owner"])
        self.assertFalse(call_kwargs["auto_run"])  # Forced to False for plan-only

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_create_plan_from_doc_not_found(self, mock_init_db):
        """Test create_plan_from_doc when document doesn't exist."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)
        core.pdm = Mock()
        core.pdm.fetch_document_by_id.return_value = None

        # Act
        result = core.create_plan_from_doc("P12345")

        # Assert
        self.assertIsNone(result)
        self.mock_logger.error.assert_called()

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_create_plan_from_doc_no_handlers(self, mock_init_db):
        """Test create_plan_from_doc when no handlers registered."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)
        core.pdm = Mock()
        core.pdm.fetch_document_by_id.return_value = {"_id": "P12345"}
        core.subscriptions[EventType.PROJECT_CHANGE] = []

        # Act
        result = core.create_plan_from_doc("P12345")

        # Assert
        self.assertIsNone(result)
        self.mock_logger.error.assert_called()

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_create_plan_from_doc_existing_plan_without_force(self, mock_init_db):
        """Test create_plan_from_doc refuses to overwrite without --force."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)
        core.pdm = Mock()
        core.pdm.fetch_document_by_id.return_value = {"_id": "P12345"}

        core.plan_dbm = Mock()
        core.plan_dbm.get_plan_summary.return_value = {
            "status": "approved",
            "execution_authority": "daemon",
            "updated_at": "2024-01-01T00:00:00Z",
            "run_token": 1,
            "executed_run_token": 1,
        }

        mock_handler = Mock()
        mock_handler.realm_id = "test_realm"
        mock_handler.derive_scope.return_value = {"kind": "project", "id": "P12345"}
        mock_handler.class_qualified_name.return_value = "test.TestHandler"

        mock_plan = Mock()
        mock_plan.plan_id = "pln_test_123"
        mock_draft = Mock()
        mock_draft.plan = mock_plan
        mock_handler.run_now.return_value = mock_draft

        core.subscriptions[EventType.PROJECT_CHANGE] = [mock_handler]

        # Act
        result = core.create_plan_from_doc("P12345", force_overwrite=False)

        # Assert
        self.assertIsNone(result)  # Should abort
        self.mock_logger.error.assert_called()
        # Plan should NOT be saved
        core.plan_dbm.save_plan.assert_not_called()

    # =====================================================
    # GENERATE_AND_PERSIST_PLAN TESTS
    # =====================================================

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_generate_and_persist_plan_auto_run_true(self, mock_init_db):
        """Test generating and persisting plan when auto_run=True."""

        async def test_generate():
            # Arrange
            core = YggdrasilCore(self.test_config, self.mock_logger)
            core.plan_dbm = Mock()
            core.plan_dbm.save_plan.return_value = "pln_test_123"
            core.engine = Mock()
            core.engine.run = Mock()

            mock_handler = Mock()
            mock_handler.realm_id = "test_realm"
            mock_handler.class_qualified_name.return_value = "test.TestHandler"

            mock_plan = Mock()
            mock_plan.plan_id = "pln_test_123"
            mock_plan.scope = {"kind": "project", "id": "P12345"}

            mock_draft = Mock()
            mock_draft.plan = mock_plan
            mock_draft.auto_run = True
            mock_draft.approvals_required = False
            mock_draft.preview = None
            mock_draft.notes = None

            mock_handler.generate_plan_draft = AsyncMock(return_value=mock_draft)

            payload = {"planning_ctx": Mock()}

            # Act
            await core._generate_and_persist_plan(mock_handler, payload)

            # Assert
            core.plan_dbm.save_plan.assert_called_once()
            core.engine.run.assert_not_called()  # Daemon mode: no inline execution

        asyncio.run(test_generate())

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_generate_and_persist_plan_auto_run_false(self, mock_init_db):
        """Test generating plan when auto_run=False (awaiting approval)."""

        async def test_generate():
            # Arrange
            core = YggdrasilCore(self.test_config, self.mock_logger)
            core.plan_dbm = Mock()
            core.plan_dbm.save_plan.return_value = "pln_test_123"
            core.engine = Mock()

            mock_handler = Mock()
            mock_handler.realm_id = "test_realm"
            mock_handler.class_qualified_name.return_value = "test.TestHandler"

            mock_plan = Mock()
            mock_plan.plan_id = "pln_test_123"
            mock_plan.scope = {"kind": "project", "id": "P12345"}

            mock_draft = Mock()
            mock_draft.plan = mock_plan
            mock_draft.auto_run = False
            mock_draft.approvals_required = True
            mock_draft.preview = None
            mock_draft.notes = None

            mock_handler.generate_plan_draft = AsyncMock(return_value=mock_draft)

            payload = {"planning_ctx": Mock()}

            # Act
            await core._generate_and_persist_plan(mock_handler, payload)

            # Assert
            core.plan_dbm.save_plan.assert_called_once()
            core.engine.run.assert_not_called()  # Should NOT execute

        asyncio.run(test_generate())

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_generate_and_persist_plan_handler_exception(self, mock_init_db):
        """Test handling exception during plan generation."""

        async def test_generate():
            # Arrange
            core = YggdrasilCore(self.test_config, self.mock_logger)

            mock_handler = Mock()
            mock_handler.class_qualified_name.return_value = "test.TestHandler"
            mock_handler.generate_plan_draft = AsyncMock(
                side_effect=Exception("Handler failed")
            )

            payload = {"planning_ctx": Mock()}

            # Act
            await core._generate_and_persist_plan(mock_handler, payload)

            # Assert - should log exception
            self.mock_logger.exception.assert_called()

        asyncio.run(test_generate())


if __name__ == "__main__":
    unittest.main()
