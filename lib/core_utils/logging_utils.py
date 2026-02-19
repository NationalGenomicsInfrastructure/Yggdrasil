import logging
import re
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

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
logging.getLogger("ibmcloudant.cloudant_v1").setLevel(logging.WARNING)  # Warnings+
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)  # Suppress debug

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

    Shortens `since='...'` and `seq='...'` parameters to avoid log spam.

    Args:
        message: The log message
        max_len: Maximum characters to keep before truncation (default 20)

    Returns:
        Message with truncated sequences
    """
    message = re.sub(
        r"since='([^']{" + str(max_len) + r",})'",
        lambda m: f"since='{m.group(1)[:max_len]}...'",
        message,
    )

    message = re.sub(
        r"seq='([^']{" + str(max_len) + r",})'",
        lambda m: f"seq='{m.group(1)[:max_len]}...'",
        message,
    )

    message = re.sub(
        r"='([a-zA-Z0-9_\-+/]{" + str(max_len) + r",})'",
        lambda m: f"='{m.group(1)[:max_len]}...'",
        message,
    )

    return message


class ShortNameFilter(logging.Filter):
    """
    Adds record.shortname = last component of record.name (after final dot).

    Used for console output only (short + readable), while file logs
    keep full %(name)s.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        record.shortname = record.name.rsplit(".", 1)[-1]
        return True


class DeduplicatingHandler(logging.Handler):
    """
    Logging handler wrapper that suppresses consecutive duplicate messages on console.

    Behavior:
    - First occurrence: emitted immediately
    - Duplicates: suppressed (only for console handlers)
    - On new message: emits summary "↑ message repeated N times" if N > 1
    - File handlers: always emit all messages (no suppression)

    Important: this wrapper MUST NOT bypass the wrapped handler's formatter.
    We forward via wrapped_handler.handle(record), not .emit(record).
    """

    def __init__(
        self, wrapped_handler: logging.Handler, suppress_duplicates: bool = True
    ) -> None:
        super().__init__()
        self.wrapped_handler = wrapped_handler
        self.suppress_duplicates = suppress_duplicates
        self.last_message: str | None = None
        self.repeat_count: int = 0

    def _clone_record_with_message(
        self, record: logging.LogRecord, message: str
    ) -> logging.LogRecord:
        """
        Clone LogRecord preserving metadata, but replacing message.
        We set args=() because message is already fully formatted.
        """
        d = record.__dict__.copy()
        d["msg"] = message
        d["args"] = ()
        return logging.makeLogRecord(d)

    def emit(self, record: logging.LogRecord) -> None:
        # Expand %-formatting safely, then truncate.
        original_msg = record.getMessage()
        truncated_msg = _truncate_long_sequences(original_msg)

        out_record = self._clone_record_with_message(record, truncated_msg)

        # New message: optionally flush summary, then emit new record
        if truncated_msg != self.last_message:
            if self.suppress_duplicates and self.repeat_count > 1:
                summary_msg = f"↑ message repeated {self.repeat_count} times"
                summary_record = self._clone_record_with_message(record, summary_msg)
                self.wrapped_handler.handle(summary_record)

            self.wrapped_handler.handle(out_record)
            self.last_message = truncated_msg
            self.repeat_count = 1
            return

        # Same message as last time
        self.repeat_count += 1
        if not self.suppress_duplicates:
            # File handler mode: emit every occurrence
            self.wrapped_handler.handle(out_record)

    def flush(self) -> None:
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
        abbrev = record.levelname[0]
        style = self._level_style.get(abbrev, "")
        lvl_txt = Text(abbrev, style=style)
        name = getattr(record, "shortname", record.name)
        mod_txt = Text(f"[{name}]", style=style)
        return Text.assemble("[", lvl_txt, "]", mod_txt, "\t", Text(message))


def configure_logging(debug: bool = False, console: bool = True) -> None:
    """
    Set up logging for the Yggdrasil application.

    - File log: full logger name (%(name)s)
    - Console log: short logger name (%(shortname)s) via ShortNameFilter
    - Console dedup enabled; file dedup disabled
    """
    configs: Mapping[str, Any] = ConfigLoader().load_config("main.json")
    log_dir = Path(configs["yggdrasil"]["log_dir"])
    log_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    log_file = log_dir / f"yggdrasil_{timestamp}.log"

    log_level = logging.DEBUG if debug else logging.INFO

    # File format keeps full name
    file_format = "%(asctime)s [%(levelname)s][%(name)s]\t%(message)s"

    # Console fallback formatter uses shortname (only used when Rich is not available)
    console_format = "%(asctime)s [%(levelname)s][%(shortname)s]\t%(message)s"

    # Rich uses its own render_message; basicConfig format is only used if Rich not available
    basic_format = "%(message)s" if _RICH_AVAILABLE else file_format

    handlers: list[logging.Handler] = []

    # --- File handler (no duplicate suppression) ---
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter(file_format))
    dedup_file_handler = DeduplicatingHandler(file_handler, suppress_duplicates=False)
    handlers.append(dedup_file_handler)

    # --- Console handler (duplicate suppression enabled) ---
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
            # Attach ShortNameFilter so AbbrevRichHandler can use record.shortname
            rich_handler.addFilter(ShortNameFilter())

            dedup_console_handler = DeduplicatingHandler(
                rich_handler, suppress_duplicates=True
            )
            handlers.append(dedup_console_handler)
        else:
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(logging.Formatter(console_format))
            # Attach ShortNameFilter so %(shortname)s exists for console formatter
            stream_handler.addFilter(ShortNameFilter())

            dedup_stream_handler = DeduplicatingHandler(
                stream_handler, suppress_duplicates=True
            )
            handlers.append(dedup_stream_handler)

    logging.basicConfig(
        level=log_level, format=basic_format, handlers=handlers, force=True
    )


def custom_logger(module_name: str) -> logging.Logger:
    """
    Create a logger for the specified module name.

    (This is intentionally just getLogger(). The behavior is driven by handlers
    configured in configure_logging().)
    """
    return logging.getLogger(module_name)
