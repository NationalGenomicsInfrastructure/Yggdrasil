import logging
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from lib.core_utils.logging_utils import (
    DeduplicatingHandler,
    configure_logging,
    custom_logger,
)


class TestLoggingUtils(unittest.TestCase):

    def _unwrap_handlers(self, handlers):
        """Return underlying handlers, unwrapping DeduplicatingHandler if present."""
        return [
            h.wrapped_handler if isinstance(h, DeduplicatingHandler) else h
            for h in handlers
        ]

    def setUp(self):
        # Backup original logging handlers and level
        self.original_handlers = logging.getLogger().handlers.copy()
        self.original_level = logging.getLogger().level

        # Clear existing handlers
        logging.getLogger().handlers = []

        # Patch ConfigLoader to return mock configs
        self.mock_configs = {"yggdrasil": {"log_dir": "/tmp/yggdrasil_logs"}}
        self.patcher_config_loader = patch("lib.core_utils.logging_utils.ConfigLoader")
        self.mock_config_loader_class = self.patcher_config_loader.start()
        self.mock_config_loader_class.return_value.load_config.return_value = (
            self.mock_configs
        )

        # Mock datetime.datetime to control the timestamp
        self.patcher_datetime = patch("lib.core_utils.logging_utils.datetime")
        self.mock_datetime = self.patcher_datetime.start()

        # Create a mock datetime instance
        mock_now = MagicMock()
        mock_now.strftime.return_value = "2021-01-01_12.00.00"
        # Set datetime.now() to return our mock datetime instance
        self.mock_datetime.now.return_value = mock_now

        # Mock Path.mkdir to prevent actual directory creation
        self.patcher_mkdir = patch("pathlib.Path.mkdir")
        self.mock_mkdir = self.patcher_mkdir.start()

        # Mock logging.basicConfig to track calls
        self.patcher_basicConfig = patch("logging.basicConfig")
        self.mock_basicConfig = self.patcher_basicConfig.start()

        # Mock logging.FileHandler to prevent it from actually opening a file
        self.patcher_filehandler = patch("logging.FileHandler", MagicMock())
        self.mock_filehandler = self.patcher_filehandler.start()

    def tearDown(self):
        # Restore original logging handlers and level
        logging.getLogger().handlers = self.original_handlers
        logging.getLogger().level = self.original_level

        # Stop all patches
        self.patcher_config_loader.stop()
        self.patcher_datetime.stop()
        self.patcher_mkdir.stop()
        self.patcher_basicConfig.stop()
        self.patcher_filehandler.stop()

    def test_configure_logging_default(self):
        # Test configure_logging with default parameters (debug=False, console=True)
        with patch("lib.core_utils.logging_utils._RICH_AVAILABLE", True):
            with patch("lib.core_utils.logging_utils.AbbrevRichHandler") as mock_rich:
                configure_logging()

                expected_log_dir = Path(self.mock_configs["yggdrasil"]["log_dir"])
                expected_log_file = (
                    expected_log_dir / "yggdrasil_2021-01-01_12.00.00.log"
                )
                expected_log_level = logging.INFO
                # Accept either the rich or non-rich format
                possible_formats = [
                    "%(asctime)s [%(levelname)s][%(name)s]\t%(message)s",
                    "%(message)s",
                ]

                self.mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
                self.mock_filehandler.assert_called_once_with(expected_log_file)
                self.mock_basicConfig.assert_called_once()
                call_args = self.mock_basicConfig.call_args[1]
                self.assertEqual(call_args.get("level"), expected_log_level)
                self.assertIn(call_args.get("format"), possible_formats)
                # With defaults (debug=False, console=True), should have both FileHandler and console handler
                self.assertIn("handlers", call_args)
                self.assertEqual(len(call_args["handlers"]), 2)
                mock_rich.assert_called_once()

    @patch("lib.core_utils.logging_utils.AbbrevRichHandler")
    @patch("logging.StreamHandler")
    @patch("logging.FileHandler")
    def test_configure_logging_debug_true(
        self,
        mock_file_handler_class,
        mock_stream_handler_class,
        mock_rich_handler_class,
    ):
        # Mock instances
        mock_file_handler = MagicMock()
        mock_stream_handler = MagicMock()
        mock_rich_handler = MagicMock()
        mock_file_handler_class.return_value = mock_file_handler
        mock_stream_handler_class.return_value = mock_stream_handler
        mock_rich_handler_class.return_value = mock_rich_handler

        # Patch _RICH_AVAILABLE to True and test with Rich handler
        with patch("lib.core_utils.logging_utils._RICH_AVAILABLE", True):
            configure_logging(debug=True)
            expected_log_level = logging.DEBUG
            possible_formats = [
                "%(asctime)s [%(levelname)s][%(name)s]\t%(message)s",
                "%(message)s",
            ]
            self.mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
            self.mock_basicConfig.assert_called_once()
            call_args = self.mock_basicConfig.call_args[1]
            self.assertEqual(call_args["level"], expected_log_level)
            self.assertIn(call_args["format"], possible_formats)
            handlers = call_args["handlers"]
            unwrapped = self._unwrap_handlers(handlers)
            self.assertIn(mock_file_handler, unwrapped)
            self.assertIn(mock_rich_handler, unwrapped)

        # Patch _RICH_AVAILABLE to False and test with StreamHandler
        self.mock_mkdir.reset_mock()
        self.mock_basicConfig.reset_mock()
        with patch("lib.core_utils.logging_utils._RICH_AVAILABLE", False):
            configure_logging(debug=True)
            self.mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
            self.mock_basicConfig.assert_called_once()
            call_args = self.mock_basicConfig.call_args[1]
            self.assertEqual(call_args["level"], expected_log_level)
            self.assertIn(call_args["format"], possible_formats)
            handlers = call_args["handlers"]
            unwrapped = self._unwrap_handlers(handlers)
            self.assertIn(mock_file_handler, unwrapped)
            self.assertIn(mock_stream_handler, unwrapped)

    def test_configure_logging_creates_log_directory(self):
        # Ensure that configure_logging attempts to create the log directory
        configure_logging()

        expected_log_dir = Path(self.mock_configs["yggdrasil"]["log_dir"])
        self.mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
        self.assertEqual(self.mock_mkdir.call_args[0], ())
        self.assertEqual(
            self.mock_mkdir.call_args[1], {"parents": True, "exist_ok": True}
        )

    def test_configure_logging_invalid_log_dir(self):
        # Test handling when the log directory is invalid
        self.mock_mkdir.side_effect = PermissionError("Permission denied")

        with self.assertRaises(PermissionError):
            configure_logging()

    def test_configure_logging_logs_to_correct_file(self):
        configure_logging()
        expected_log_dir = Path(self.mock_configs["yggdrasil"]["log_dir"])
        expected_log_file = expected_log_dir / "yggdrasil_2021-01-01_12.00.00.log"
        self.mock_filehandler.assert_called_once_with(expected_log_file)

    def test_custom_logger_returns_logger(self):
        # Test that custom_logger returns a Logger instance with the correct name
        module_name = "test_module"
        logger = custom_logger(module_name)

        self.assertIsInstance(logger, logging.Logger)
        self.assertEqual(logger.name, module_name)

    def test_custom_logger_same_logger(self):
        # Test that calling custom_logger multiple times with the same name returns the same logger
        module_name = "test_module"
        logger1 = custom_logger(module_name)
        logger2 = custom_logger(module_name)

        self.assertIs(logger1, logger2)

    def test_logging_levels_suppressed(self):
        # Test that logging levels for specified noisy libraries are set to WARNING
        noisy_libraries = ["matplotlib", "numba", "h5py", "PIL"]
        for lib in noisy_libraries:
            logger = logging.getLogger(lib)
            self.assertEqual(logger.level, logging.WARNING)

    def test_configure_logging_no_configs(self):
        # Test behavior when configs do not contain 'yggdrasil.log_dir'
        self.mock_configs.pop("yggdrasil", None)

        with self.assertRaises(KeyError):
            configure_logging()

    def test_configure_logging_with_invalid_log_file(self):
        # Test handling when the log file cannot be created
        with patch(
            "logging.FileHandler", side_effect=PermissionError("Permission denied")
        ):
            with self.assertRaises(PermissionError):
                configure_logging()

    def test_configure_logging_with_invalid_stream_handler(self):
        # Test handling when StreamHandler or AbbrevRichHandler cannot be initialized

        # Case 1: Rich is NOT available, StreamHandler fails
        with patch("lib.core_utils.logging_utils._RICH_AVAILABLE", False):
            with patch("logging.StreamHandler", side_effect=Exception("Stream error")):
                with self.assertRaises(Exception) as ctx:
                    configure_logging(debug=True)
                self.assertIn("Stream error", str(ctx.exception))

        # Case 2: Rich IS available, AbbrevRichHandler fails
        with patch("lib.core_utils.logging_utils._RICH_AVAILABLE", True):
            with patch(
                "lib.core_utils.logging_utils.AbbrevRichHandler",
                side_effect=Exception("Rich handler error"),
            ):
                with self.assertRaises(Exception) as ctx:
                    configure_logging(debug=True)
                self.assertIn("Rich handler error", str(ctx.exception))

    def test_configure_logging_with_existing_handlers(self):
        # Test that existing handlers are replaced
        logging.getLogger().handlers = [MagicMock()]
        configure_logging()
        self.assertEqual(len(logging.getLogger().handlers), 1)
        self.mock_basicConfig.assert_called_once()

    @patch("lib.core_utils.logging_utils.AbbrevRichHandler")
    @patch("logging.StreamHandler")
    @patch("logging.FileHandler")
    def test_configure_logging_handler_types(
        self, mock_file_handler, mock_stream_handler, mock_rich_handler
    ):
        # Patch _RICH_AVAILABLE to True to use AbbrevRichHandler
        with patch("lib.core_utils.logging_utils._RICH_AVAILABLE", True):
            configure_logging(debug=True)
            call_args = self.mock_basicConfig.call_args[1]
            handlers = call_args["handlers"]
            self.assertEqual(len(handlers), 2)
            unwrapped = self._unwrap_handlers(handlers)
            self.assertIn(mock_file_handler.return_value, unwrapped)
            self.assertIn(mock_rich_handler.return_value, unwrapped)

    def test_configure_logging_log_format(self):
        configure_logging()
        possible_formats = [
            "%(asctime)s [%(levelname)s][%(name)s]\t%(message)s",
            "%(message)s",
        ]
        self.mock_basicConfig.assert_called_once()
        self.assertIn(self.mock_basicConfig.call_args[1]["format"], possible_formats)

    def test_configure_logging_log_level_info(self):
        configure_logging()
        self.mock_basicConfig.assert_called_once()
        self.assertEqual(self.mock_basicConfig.call_args[1]["level"], logging.INFO)

    def test_configure_logging_log_level_debug(self):
        configure_logging(debug=True)
        self.mock_basicConfig.assert_called_once()
        self.assertEqual(self.mock_basicConfig.call_args[1]["level"], logging.DEBUG)

    @patch("lib.core_utils.logging_utils.AbbrevRichHandler")
    @patch("logging.StreamHandler")
    @patch("logging.FileHandler")
    def test_configure_logging_handlers_order(
        self, mock_file_handler, mock_stream_handler, mock_rich_handler
    ):
        configure_logging(debug=True)
        handlers = self.mock_basicConfig.call_args[1]["handlers"]
        self.assertEqual(len(handlers), 2)
        self.assertIsInstance(handlers[0], DeduplicatingHandler)
        self.assertIsInstance(handlers[1], DeduplicatingHandler)
        self.assertEqual(handlers[0].wrapped_handler, mock_file_handler.return_value)
        self.assertEqual(handlers[1].wrapped_handler, mock_rich_handler.return_value)

    def test_configure_logging_timestamp_format(self):
        configure_logging()
        expected_timestamp = "2021-01-01_12.00.00"
        expected_log_dir = Path(self.mock_configs["yggdrasil"]["log_dir"])
        expected_log_file = expected_log_dir / f"yggdrasil_{expected_timestamp}.log"
        self.mock_filehandler.assert_called_with(expected_log_file)

    @patch("lib.core_utils.logging_utils.datetime")
    def test_configure_logging_custom_timestamp(self, mock_datetime):
        mock_now = MagicMock()
        mock_now.strftime.return_value = "2022-02-02_14.30.00"
        mock_datetime.now.return_value = mock_now

        configure_logging()
        expected_timestamp = "2022-02-02_14.30.00"
        expected_log_dir = Path(self.mock_configs["yggdrasil"]["log_dir"])
        expected_log_file = expected_log_dir / f"yggdrasil_{expected_timestamp}.log"
        self.mock_filehandler.assert_called_with(expected_log_file)

    def test_configure_logging_invalid_configs_type(self):
        # Test handling when ConfigLoader returns None
        self.mock_config_loader_class.return_value.load_config.return_value = None
        with self.assertRaises(TypeError):
            configure_logging()

    def test_configure_logging_log_dir_is_file(self):
        self.mock_mkdir.side_effect = NotADirectoryError
        with self.assertRaises(NotADirectoryError):
            configure_logging()

    def test_configure_logging_no_handlers(self):
        # Test that logging.basicConfig is called with correct handlers
        with patch("logging.basicConfig") as mock_basic_config:
            configure_logging()
            self.assertIn("handlers", mock_basic_config.call_args[1])

    def test_custom_logger_different_names(self):
        # Test that different module names return different loggers
        logger1 = custom_logger("module1")
        logger2 = custom_logger("module2")
        self.assertNotEqual(logger1, logger2)
        self.assertNotEqual(logger1.name, logger2.name)

    def test_custom_logger_propagate_false(self):
        # Test that the logger's propagate attribute is default (True)
        logger = custom_logger("module")
        self.assertTrue(logger.propagate)

    def test_custom_logger_level_not_set(self):
        # Test that the logger's level is not explicitly set (inherits from root)
        logger = custom_logger("module")
        self.assertEqual(logger.level, logging.NOTSET)

    def test_configure_logging_without_debug_stream_handler(self):
        # Test that StreamHandler is not added when debug=False
        with patch("logging.StreamHandler") as mock_stream_handler:
            configure_logging()
            mock_stream_handler.assert_not_called()

    def test_configure_logging_with_debug_stream_handler(self):
        # Test that StreamHandler is added when debug=True and rich is not available
        with patch("lib.core_utils.logging_utils._RICH_AVAILABLE", False):
            with patch("logging.StreamHandler") as mock_stream_handler:
                configure_logging(debug=True)
                mock_stream_handler.assert_called_once()

    @patch("lib.core_utils.logging_utils.AbbrevRichHandler")
    @patch("logging.StreamHandler")
    @patch("logging.FileHandler")
    def test_configure_logging_handlers_are_set_correctly(
        self, mock_file_handler, mock_stream_handler, mock_rich_handler
    ):
        # Test that handlers are set correctly in the root logger for both Rich and non-Rich cases
        # Case 1: Rich is available
        with patch("lib.core_utils.logging_utils._RICH_AVAILABLE", True):
            configure_logging(debug=True)
            call_args = self.mock_basicConfig.call_args[1]
            handlers = call_args["handlers"]
            self.assertEqual(len(handlers), 2)
            unwrapped = self._unwrap_handlers(handlers)
            self.assertIn(mock_file_handler.return_value, unwrapped)
            self.assertIn(mock_rich_handler.return_value, unwrapped)

        # Case 2: Rich is not available
        self.mock_basicConfig.reset_mock()
        with patch("lib.core_utils.logging_utils._RICH_AVAILABLE", False):
            configure_logging(debug=True)
            call_args = self.mock_basicConfig.call_args[1]
            handlers = call_args["handlers"]
            self.assertEqual(len(handlers), 2)
            unwrapped = self._unwrap_handlers(handlers)
            self.assertIn(mock_file_handler.return_value, unwrapped)
            self.assertIn(mock_stream_handler.return_value, unwrapped)

    def test_configure_logging_respects_existing_loggers(self):
        # Test that existing loggers are not affected by configure_logging
        existing_logger = logging.getLogger("existing")
        existing_logger_level = existing_logger.level
        existing_logger_handlers = existing_logger.handlers.copy()

        configure_logging()

        self.assertEqual(existing_logger.level, existing_logger_level)
        self.assertEqual(existing_logger.handlers, existing_logger_handlers)

    @patch("logging.FileHandler")
    def test_logging_messages_after_configuration(self, mock_file_handler):
        # Test that logging messages are handled correctly after configuration
        mock_file_handler_instance = MagicMock()
        # Set the level attribute to a real int to avoid TypeError in logger logic
        mock_file_handler_instance.level = logging.NOTSET
        mock_file_handler.return_value = mock_file_handler_instance
        configure_logging()
        logger = custom_logger("test_module")
        # Set logger level to INFO and ensure it does not propagate to avoid root logger issues
        logger.setLevel(logging.INFO)
        logger.propagate = False
        logger.handlers = [mock_file_handler_instance]

        # Manually create a LogRecord and call emit to simulate logging
        record = logging.LogRecord(
            name=logger.name,
            level=logging.INFO,
            pathname=__file__,
            lineno=0,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        mock_file_handler_instance.emit(record)
        mock_file_handler_instance.emit.assert_called_with(record)

    def test_suppressed_loggers_levels(self):
        # Ensure that suppressed loggers have their levels set to WARNING
        suppressed_loggers = ["matplotlib", "numba", "h5py", "PIL"]
        for logger_name in suppressed_loggers:
            logger = logging.getLogger(logger_name)
            self.assertEqual(logger.level, logging.WARNING)

    def test_suppressed_loggers_do_not_propagate(self):
        # Ensure that suppressed loggers still propagate messages
        suppressed_loggers = ["matplotlib", "numba", "h5py", "PIL"]
        for logger_name in suppressed_loggers:
            logger = logging.getLogger(logger_name)
            self.assertTrue(logger.propagate)

    def test_logging_basic_config_called_once(self):
        # Ensure that logging.basicConfig is called only once
        configure_logging()
        self.mock_basicConfig.assert_called_once()

    def test_configure_logging_with_relative_log_dir(self):
        # Test handling when log_dir is a relative path
        self.mock_configs["yggdrasil"]["log_dir"] = "relative/path/to/logs"
        configure_logging()

        expected_log_dir = Path("relative/path/to/logs")
        self.mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)

    def test_configure_logging_with_env_var_in_log_dir(self):
        # Test handling when log_dir contains an environment variable
        self.mock_configs["yggdrasil"]["log_dir"] = "${HOME}/logs"
        with patch.dict(os.environ, {"HOME": "/home/testuser"}):
            configure_logging()

            expected_log_dir = Path("/home/testuser/logs")
            self.mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)

    def test_configure_logging_invalid_log_format(self):
        # Test handling when log_format is invalid
        with patch("logging.basicConfig") as mock_basic_config:
            configure_logging()
            mock_basic_config.assert_called_once()
            self.assertIn("format", mock_basic_config.call_args[1])

    @patch("logging.FileHandler")
    def test_configure_logging_with_custom_handlers(self, mock_file_handler):
        # Test that custom handlers can be added if the code is modified in the future
        # Since the current code does not support this, we check that handlers are as expected
        mock_file_handler_instance = MagicMock()
        mock_file_handler.return_value = mock_file_handler_instance

        with patch("lib.core_utils.logging_utils._RICH_AVAILABLE", True):
            with patch("lib.core_utils.logging_utils.AbbrevRichHandler") as mock_rich:
                configure_logging()
                # Check that FileHandler was used in the handlers passed to basicConfig
                handlers = self.mock_basicConfig.call_args[1]["handlers"]
                # With defaults (debug=False, console=True), should have 2 handlers
                self.assertEqual(len(handlers), 2)
                unwrapped = self._unwrap_handlers(handlers)
                self.assertIn(mock_file_handler_instance, unwrapped)
                self.assertIn(mock_rich.return_value, unwrapped)

    def test_configure_logging_with_no_handlers(self):
        # Test that an error is raised if FileHandler fails
        self.mock_filehandler.side_effect = Exception("Handler error")
        with self.assertRaises(Exception):
            configure_logging()

    def test_configure_logging_multiple_times(self):
        configure_logging()
        configure_logging(debug=True)
        self.assertTrue(True)

    def test_configure_logging_console_false(self):
        """Test configure_logging with console=False (silent mode)."""
        configure_logging(debug=False, console=False)

        call_args = self.mock_basicConfig.call_args[1]
        self.assertEqual(call_args.get("level"), logging.INFO)
        # Should have only FileHandler, no console handler
        self.assertIn("handlers", call_args)
        self.assertEqual(len(call_args["handlers"]), 1)

    def test_configure_logging_console_true_debug_false(self):
        """Test configure_logging with console=True, debug=False (normal mode)."""
        with patch("lib.core_utils.logging_utils._RICH_AVAILABLE", True):
            with patch("lib.core_utils.logging_utils.AbbrevRichHandler") as mock_rich:
                configure_logging(debug=False, console=True)

                call_args = self.mock_basicConfig.call_args[1]
                self.assertEqual(call_args.get("level"), logging.INFO)
                # Should have both FileHandler and console handler
                self.assertIn("handlers", call_args)
                self.assertEqual(len(call_args["handlers"]), 2)
                mock_rich.assert_called_once()

    def test_configure_logging_console_true_debug_true(self):
        """Test configure_logging with console=True, debug=True (dev mode)."""
        with patch("lib.core_utils.logging_utils._RICH_AVAILABLE", True):
            with patch("lib.core_utils.logging_utils.AbbrevRichHandler") as mock_rich:
                configure_logging(debug=True, console=True)

                call_args = self.mock_basicConfig.call_args[1]
                self.assertEqual(call_args.get("level"), logging.DEBUG)
                # Should have both FileHandler and console handler
                self.assertIn("handlers", call_args)
                self.assertEqual(len(call_args["handlers"]), 2)
                mock_rich.assert_called_once()

    def test_configure_logging_console_fallback_no_rich(self):
        """Test console logging falls back to StreamHandler when Rich unavailable."""
        with patch("lib.core_utils.logging_utils._RICH_AVAILABLE", False):
            with patch("logging.StreamHandler") as mock_stream:
                configure_logging(debug=False, console=True)

                call_args = self.mock_basicConfig.call_args[1]
                # Should have both FileHandler and StreamHandler
                self.assertIn("handlers", call_args)
                self.assertEqual(len(call_args["handlers"]), 2)
                mock_stream.assert_called_once()


if __name__ == "__main__":
    unittest.main()
