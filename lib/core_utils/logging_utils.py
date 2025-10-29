import logging
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
    handlers = [logging.FileHandler(log_file)]
    if console:
        if _RICH_AVAILABLE:
            handlers.append(
                AbbrevRichHandler(
                    rich_tracebacks=True,
                    markup=False,
                    show_level=False,
                    show_time=True,
                    show_path=False,
                    omit_repeated_times=True,
                    level=log_level,
                    log_time_format="%Y-%m-%d %H:%M:%S",
                )
            )
        else:
            # fallback to plain StreamHandler if Rich not installed
            handlers.append(logging.StreamHandler())

    logging.basicConfig(level=log_level, format=format, handlers=handlers, force=True)


def custom_logger(module_name: str) -> logging.Logger:
    """Create a custom logger for the specified module.

    Args:
        module_name (str): The name of the module for which the logger is created.

    Returns:
        logging.Logger: A custom logger for the specified module.
    """
    return logging.getLogger(module_name)
