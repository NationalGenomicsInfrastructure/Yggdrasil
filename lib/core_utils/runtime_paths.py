"""Mode-aware defaults for machine-local runtime paths.

A production daemon and a dev daemon (``--dev``) may run on the same
machine. Their machine-local state — the engine work root and the event
spool — must not collide: shared ``success.fingerprint`` caches would
cross-skip steps, and each ops consumer would ingest the other
environment's events. These helpers keep production defaults exactly as
they were and give dev mode ``_dev``-suffixed defaults.

Explicit values (config key or environment variable) are exact overrides
and are never rewritten. Resolution precedence:

- work root: ``config["work_root"]`` → ``$YGG_WORK_ROOT`` → mode default
- event spool: ``$YGG_EVENT_SPOOL`` → mode default
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from lib.core_utils.ygg_session import YggSession


def default_work_root() -> Path:
    """Return the mode default for the engine work root."""
    return Path("/tmp/ygg_work_dev" if YggSession.is_dev() else "/tmp/ygg_work")


def default_event_spool() -> Path:
    """Return the mode default for the event spool."""
    return Path("/tmp/ygg_events_dev" if YggSession.is_dev() else "/tmp/ygg_events")


def resolve_work_root(config: Mapping[str, Any] | None = None) -> Path:
    """Resolve the engine work root.

    Args:
        config: Optional main configuration; its ``work_root`` key (when
            truthy) takes precedence over the environment.

    Returns:
        The resolved work root path.
    """
    if config:
        configured = config.get("work_root")
        if configured:
            return Path(configured)
    env_value = os.environ.get("YGG_WORK_ROOT")
    if env_value:
        return Path(env_value)
    return default_work_root()


def resolve_event_spool() -> Path:
    """Resolve the event spool root (``$YGG_EVENT_SPOOL`` → mode default)."""
    env_value = os.environ.get("YGG_EVENT_SPOOL")
    if env_value:
        return Path(env_value)
    return default_event_spool()
