"""Session-wide pytest setup for the Yggdrasil test suite.

Points ``YGG_HOME`` at a throwaway workspace holding a minimal ``main.json``,
so tests never read the developer's gitignored ``yggdrasil_workspace/`` (which
does not exist in CI checkouts) and never write log files into its ``log_dir``.
"""

import json
import os
import shutil
import tempfile
from pathlib import Path

import pytest

_workspace: Path | None = None
_previous_ygg_home: str | None = None


def _minimal_main_config(workspace: Path) -> dict:
    """Build the smallest ``main.json`` the suite needs.

    Args:
        workspace: Root of the throwaway test workspace.

    Returns:
        A config with a log directory inside the workspace and a local CouchDB
        endpoint whose credentials come from the default env-var names.
    """
    return {
        "yggdrasil": {"log_dir": str(workspace / "logs")},
        "external_systems": {
            "endpoints": {
                "couchdb": {
                    "url": "http://localhost:5984",
                    "auth": {
                        "user_env": "YGG_COUCH_USER",
                        "pass_env": "YGG_COUCH_PASS",
                    },
                }
            }
        },
    }


def pytest_configure(config: pytest.Config) -> None:
    """Create the test workspace and point ``YGG_HOME`` at it.

    Runs before collection, so modules that load ``main.json`` at import time
    also see the test config. Any ``YGG_HOME`` already set is overridden and
    restored in ``pytest_unconfigure``.

    Args:
        config: The pytest config object (unused).
    """
    global _workspace, _previous_ygg_home

    _workspace = Path(tempfile.mkdtemp(prefix="ygg_test_workspace_"))
    config_dir = _workspace / "common" / "configurations"
    config_dir.mkdir(parents=True)
    (config_dir / "main.json").write_text(
        json.dumps(_minimal_main_config(_workspace), indent=2)
    )

    _previous_ygg_home = os.environ.get("YGG_HOME")
    os.environ["YGG_HOME"] = str(_workspace)


def pytest_unconfigure(config: pytest.Config) -> None:
    """Restore the original ``YGG_HOME`` and delete the test workspace.

    Args:
        config: The pytest config object (unused).
    """
    if _previous_ygg_home is None:
        os.environ.pop("YGG_HOME", None)
    else:
        os.environ["YGG_HOME"] = _previous_ygg_home

    if _workspace is not None:
        shutil.rmtree(_workspace, ignore_errors=True)
