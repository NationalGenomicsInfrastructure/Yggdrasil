import asyncio
import logging
import unittest
from unittest.mock import AsyncMock, Mock, call, patch

from lib.core_utils.event_types import EventType
from lib.core_utils.singleton_decorator import SingletonMeta
from lib.core_utils.yggdrasil_core import YggdrasilCore
from lib.watchers.couchdb_watcher import CouchDBWatcher
from lib.watchers.seq_data_watcher import SeqDataWatcher, YggdrasilEvent


class TestYggdrasilCore(unittest.TestCase):
    """
    Comprehensive tests for YggdrasilCore - the central orchestrator.

    Tests initialization, singleton behavior, watcher/handler management,
    async lifecycle, event processing, and error handling scenarios.
    """

    @classmethod
    def setUpClass(cls):
        """Set up class-level resources."""
        # Store original event loop policy
        cls.original_event_loop_policy = asyncio.get_event_loop_policy()

    @classmethod
    def tearDownClass(cls):
        """Clean up class-level resources and reset event loop state."""
        # Reset to default event loop policy for subsequent tests
        asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())

    def setUp(self):
        """Set up test fixtures and clear singleton state."""
        # Clear singleton instance before each test
        SingletonMeta._instances.clear()

        # Sample configuration for testing
        self.test_config = {
            "instrument_watch": [
                {
                    "name": "TestInstrument",
                    "directory": "/test/path",
                    "marker_files": ["test.txt"],
                }
            ],
            "couchdb_poll_interval": 10,
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
    @patch("lib.couchdb.project_db_manager.ProjectDBManager")
    def test_init_db_managers_success(self, mock_pdm_class, mock_ydm_class):
        """Test successful database manager initialization."""
        # Arrange
        mock_pdm_instance = Mock()
        mock_ydm_instance = Mock()
        mock_pdm_class.return_value = mock_pdm_instance
        mock_ydm_class.return_value = mock_ydm_instance

        # Act
        core = YggdrasilCore(self.test_config, self.mock_logger)

        # Assert
        self.assertEqual(core.pdm, mock_pdm_instance)
        self.assertEqual(core.ydm, mock_ydm_instance)
        mock_pdm_class.assert_called_once()
        mock_ydm_class.assert_called_once()

        expected_calls = [
            call("Initializing DB managers..."),
            call("DB managers initialized."),
            call("YggdrasilCore initialized."),
        ]
        self.mock_logger.info.assert_has_calls(expected_calls)

    @patch("lib.couchdb.project_db_manager.ProjectDBManager")
    def test_init_db_managers_exception(self, mock_pdm_class):
        """Test database manager initialization with exception."""
        # Arrange
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
    def test_setup_fs_watchers(self, mock_init_db):
        """Test file system watcher setup with instrument configuration."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)
        initial_watchers_count = len(core.watchers)

        # Act
        core._setup_fs_watchers()

        # Assert - Check that a watcher was added
        self.assertEqual(len(core.watchers), initial_watchers_count + 1)

        # Verify the watcher is a SeqDataWatcher
        added_watcher = core.watchers[-1]
        self.assertIsInstance(added_watcher, SeqDataWatcher)

        # Verify watcher properties
        self.assertEqual(
            added_watcher.name, "SeqDataWatcher-MiSeq"
        )  # From hardcoded config
        self.assertEqual(added_watcher.event_type, EventType.FLOWCELL_READY)

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_setup_cdb_watchers(self, mock_init_db):
        """Test CouchDB watcher setup."""
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

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._setup_fs_watchers")
    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_setup_watchers(self, mock_init_db, mock_setup_fs):
        """Test main setup_watchers method."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)

        # Act
        core.setup_watchers()

        # Assert
        mock_setup_fs.assert_called_once()
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

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_handle_event_with_registered_handler(self, mock_init_db):
        """Test event handling with registered handler."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)
        mock_handler = Mock()
        mock_handler.class_qualified_name = Mock(return_value="MockHandler")
        mock_handler.class_key = Mock(return_value=("test", "MockHandler"))
        mock_handler.realm_id = "test_realm"
        core.register_handler(EventType.PROJECT_CHANGE, mock_handler)

        # Act
        core.handle_event(self.test_event)

        # Assert
        mock_handler.assert_called_once()
        self.mock_logger.info.assert_called()

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

    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_handle_event_handler_exception(self, mock_init_db):
        """Test event handling when handler raises exception."""
        # Arrange
        core = YggdrasilCore(self.test_config, self.mock_logger)
        mock_handler = Mock()
        mock_handler.side_effect = Exception("Handler failed")
        mock_handler.class_qualified_name = Mock(return_value="MockHandler")
        mock_handler.class_key = Mock(return_value=("test", "MockHandler"))
        mock_handler.realm_id = "test_realm"
        core.register_handler(EventType.PROJECT_CHANGE, mock_handler)

        # Act
        core.handle_event(self.test_event)

        # Assert
        mock_handler.assert_called_once()
        self.mock_logger.error.assert_called()

    # =====================================================
    # CLI AND RUN_ONCE TESTS
    # =====================================================

    @patch("lib.ops.consumer.FileSpoolConsumer")
    @patch("lib.couchdb.project_db_manager.ProjectDBManager")
    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_run_once_success(self, mock_init_db, mock_pdm_class, mock_consumer_class):
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
        core.register_handler(EventType.PROJECT_CHANGE, mock_handler)

        # Act
        core.run_once("test_doc_id")

        # Assert
        mock_pdm_instance.fetch_document_by_id.assert_called_once_with("test_doc_id")
        mock_handler.derive_scope.assert_called_once_with(mock_doc)
        mock_handler.run_now.assert_called_once()
        # Verify consumer was called
        mock_consumer_instance.consume.assert_called_once()

    @patch("lib.couchdb.project_db_manager.ProjectDBManager")
    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_run_once_document_not_found(self, mock_init_db, mock_pdm_class):
        """Test run_once when document is not found."""
        # Arrange
        mock_pdm_instance = Mock()
        mock_pdm_instance.fetch_document_by_id.return_value = None
        mock_pdm_class.return_value = mock_pdm_instance

        core = YggdrasilCore(self.test_config, self.mock_logger)

        # Act
        core.run_once("nonexistent_doc")

        # Assert
        self.mock_logger.error.assert_called()

    @patch("lib.couchdb.project_db_manager.ProjectDBManager")
    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_run_once_no_handler(self, mock_init_db, mock_pdm_class):
        """Test run_once when no handler is registered."""
        # Arrange
        mock_pdm_instance = Mock()
        mock_doc = {"id": "test_doc", "project_id": "test_project"}
        mock_pdm_instance.fetch_document_by_id.return_value = mock_doc
        mock_pdm_class.return_value = mock_pdm_instance

        core = YggdrasilCore(self.test_config, self.mock_logger)
        # No handler registered

        # Act
        core.run_once("test_doc_id")

        # Assert
        self.mock_logger.error.assert_called()

    @patch("lib.ops.consumer.FileSpoolConsumer")
    @patch("lib.couchdb.project_db_manager.ProjectDBManager")
    @patch("lib.core_utils.yggdrasil_core.YggdrasilCore._init_db_managers")
    def test_run_once_handler_no_run_now_method(
        self, mock_init_db, mock_pdm_class, mock_consumer_class
    ):
        """Test run_once when handler doesn't have run_now method."""
        # Arrange
        mock_pdm_instance = Mock()
        mock_doc = {"id": "test_doc", "project_id": "test_project"}
        mock_pdm_instance.fetch_document_by_id.return_value = mock_doc
        mock_pdm_class.return_value = mock_pdm_instance

        mock_handler = Mock()
        del mock_handler.run_now  # Remove run_now method
        mock_handler.class_qualified_name = Mock(return_value="MockHandler")
        mock_handler.class_key = Mock(return_value=("test", "MockHandler"))
        mock_handler.realm_id = "test_realm"
        mock_handler.derive_scope = Mock(
            return_value={"kind": "project", "id": "test_project"}
        )

        core = YggdrasilCore(self.test_config, self.mock_logger)
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
    def test_setup_fs_watchers_no_instruments(self, mock_init_db):
        """Test file system watcher setup with no instruments configured."""
        # Arrange
        config_no_instruments = {}
        core = YggdrasilCore(config_no_instruments, self.mock_logger)

        # Act
        core._setup_fs_watchers()

        # Assert
        # Should create one watcher from hardcoded config
        self.assertEqual(len(core.watchers), 1)

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


if __name__ == "__main__":
    unittest.main()
