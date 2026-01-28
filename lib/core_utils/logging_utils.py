import logging
import re
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

# from lib.core_utils.config_loader import configs
from lib.core_utils.config_loader import ConfigLoader

try:
    from rich.logging import RichHandler
    from rich.style import Style
    from rich.text import Text

    _RICH_AVAILABLE = True
except ImportError:
    _RICH_AVAILABLE = False

# Suppress logging for specific noisy libraries
for noisy in ("matplotlib", "numba", "h5py", "PIL", "watchdog"):
    logging.getLogger(noisy).setLevel(logging.WARNING)

logging.getLogger("ibm-cloud-sdk-core").setLevel(logging.ERROR)  # Only errors
logging.getLogger("ibmcloudant.cloudant_v1").setLevel(
    logging.WARNING
)  # Warnings and above
logging.getLogger("urllib3.connectionpool").setLevel(
    logging.WARNING
)  # Suppress debug logs

# Helper to abbreviate level names
for _name, _abbr in {
    "DEBUG": "D",
    "INFO": "I",
    "WARNING": "W",
    "ERROR": "E",
    "CRITICAL": "C",
}.items():
    logging.addLevelName(getattr(logging, _name), _abbr)


def _truncate_long_sequences(message: str, max_len: int = 20) -> str:
    """
    Truncate long sequence values in log messages.

    Shortens `since='...'` and `seq='...'` parameters to avoid log spam,
    keeping original values in persisted event files.

    Args:
        message: The log message
        max_len: Maximum characters to keep before truncation (default 20)

    Returns:
        Message with truncated sequences
    """
    # Truncate 'since=' parameter
    message = re.sub(
        r"since='([^']{" + str(max_len) + r",})'",
        lambda m: f"since='{m.group(1)[:max_len]}...'",
        message,
    )

    # Truncate 'seq=' parameter
    message = re.sub(
        r"seq='([^']{" + str(max_len) + r",})'",
        lambda m: f"seq='{m.group(1)[:max_len]}...'",
        message,
    )

    # Truncate long base64/JSON strings in quotes (general case)
    message = re.sub(
        r"='([a-zA-Z0-9_\-+/]{" + str(max_len) + r",})'",
        lambda m: f"='{m.group(1)[:max_len]}...'",
        message,
    )

    return message


class DeduplicatingHandler(logging.Handler):
    """
    Logging handler wrapper that suppresses consecutive duplicate messages on console.

    Behavior:
    - First occurrence: emitted immediately
    - Duplicates: suppressed (only for RichHandler/console)
    - On new message: emits summary "↑ Previous message repeated N times" if N > 1
    - File handlers: always emit all messages (no suppression)
    """

    def __init__(
        self, wrapped_handler: logging.Handler, suppress_duplicates: bool = True
    ):
        """
        Args:
            wrapped_handler: The actual handler to emit deduplicated records to
            suppress_duplicates: If True, suppress duplicate console output (False for file handlers)
        """
        super().__init__()
        self.wrapped_handler = wrapped_handler
        self.suppress_duplicates = suppress_duplicates  # False for file handlers
        self.last_message: str | None = None
        self.repeat_count: int = 0

    def emit(self, record: logging.LogRecord) -> None:
        """
        Emit a record, suppressing consecutive duplicates on console.
        """
        # Get the formatted message (handles format args)
        original_msg = record.getMessage()

        # Truncate long sequences in the message
        truncated_msg = _truncate_long_sequences(original_msg)

        # Create a new record with the truncated, formatted message
        # Important: use msg (not getMessage) and set args=() to avoid re-formatting
        formatted_record = logging.LogRecord(
            name=record.name,
            level=record.levelno,
            pathname=record.pathname,
            lineno=record.lineno,
            msg=truncated_msg,  # Already formatted, no args needed
            args=(),
            exc_info=record.exc_info,
        )
        formatted_record.levelname = record.levelname

        # Different message - emit summary if previous message repeated
        if truncated_msg != self.last_message:
            if self.suppress_duplicates and self.repeat_count > 1:
                # Emit summary for previous message
                summary_record = logging.LogRecord(
                    name=record.name,
                    level=record.levelno,
                    pathname=record.pathname,
                    lineno=record.lineno,
                    msg=f"↑ message repeated {self.repeat_count} times",
                    args=(),
                    exc_info=None,
                )
                summary_record.levelname = record.levelname
                self.wrapped_handler.emit(summary_record)

            # Emit new message
            self.wrapped_handler.emit(formatted_record)

            # Reset tracking
            self.last_message = truncated_msg
            self.repeat_count = 1
        else:
            # Same message - increment counter
            self.repeat_count += 1

            # If not suppressing (file handler), emit every occurrence
            if not self.suppress_duplicates:
                self.wrapped_handler.emit(formatted_record)

    def flush(self) -> None:
        """Ensure any pending data is flushed."""
        if hasattr(self.wrapped_handler, "flush"):
            self.wrapped_handler.flush()


class AbbrevRichHandler(RichHandler):
    _level_style = {
        "D": Style(color="cyan"),
        "I": Style(color="green"),
        "W": Style(color="yellow"),
        "E": Style(color="red"),
        "C": Style(color="red", bold=True),
    }

    def render_message(self, record, message):  # called by RichHandler
        # Use first char of levelname
        abbrev = record.levelname[0]
        style = self._level_style.get(abbrev, "")
        lvl_txt = Text(abbrev, style=style)
        mod_txt = Text(f"[{record.name}]", style=style)  # same colour as level
        return Text.assemble("[", lvl_txt, "]", mod_txt, "\t", Text(message))


def configure_logging(debug: bool = False, console: bool = True) -> None:
    """Set up logging for the Yggdrasil application.

    Configures the logging environment by creating a log directory if it doesn't exist,
    setting the log file's path with a timestamp, and defining the log format and log level.

    Deduplicates repeated log messages to reduce spam from polling loops.

    Args:
        debug (bool, optional): If True, sets DEBUG level. If False, sets INFO level.
            Defaults to False.
        console (bool, optional): If True, log messages will also be printed to the console.
            Defaults to True.

    Returns:
        None
    """
    configs: Mapping[str, Any] = ConfigLoader().load_config("config.json")
    log_dir = Path(configs["yggdrasil_log_dir"])
    log_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    log_file = log_dir / f"yggdrasil_{timestamp}.log"

    log_level = logging.DEBUG if debug else logging.INFO
    log_format = "%(asctime)s [%(levelname)s][%(name)s]\t%(message)s"
    format = (
        "%(message)s" if _RICH_AVAILABLE else log_format
    )  # Use simple format if Rich not available

    # Configure logging with a file handler and optionally a console handler
    handlers: list[logging.Handler] = []

    # File handler (NO deduplication - log everything)
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter(log_format))
    # Wrap with deduplication but disable suppression for file logs
    dedup_file_handler = DeduplicatingHandler(file_handler, suppress_duplicates=False)
    handlers.append(dedup_file_handler)

    # Console handler (with deduplication enabled)
    if console:
        if _RICH_AVAILABLE:
            rich_handler = AbbrevRichHandler(
                rich_tracebacks=True,
                markup=False,
                show_level=False,
                show_time=True,
                show_path=False,
                omit_repeated_times=True,
                level=log_level,
                log_time_format="%Y-%m-%d %H:%M:%S",
            )
            # Suppress duplicates on console
            dedup_console_handler = DeduplicatingHandler(
                rich_handler, suppress_duplicates=True
            )
            handlers.append(dedup_console_handler)
        else:
            # fallback to plain StreamHandler if Rich not installed
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(logging.Formatter(log_format))
            dedup_stream_handler = DeduplicatingHandler(
                stream_handler, suppress_duplicates=True
            )
            handlers.append(dedup_stream_handler)

    logging.basicConfig(level=log_level, format=format, handlers=handlers, force=True)


def custom_logger(module_name: str) -> logging.Logger:
    """Create a custom logger for the specified module.

    Args:
        module_name (str): The name of the module for which the logger is created.

    Returns:
        logging.Logger: A custom logger for the specified module.
    """
    return logging.getLogger(module_name)
