"""Composition root: build the internal-storage bundle from configuration."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from yggdrasil.logging_utils import custom_logger
from yggdrasil.storage.config import (
    CouchInternalStorageConfig,
    resolve_internal_storage_config,
)
from yggdrasil.storage.protocols import InternalStorageBundle

logger = custom_logger(__name__)

#: Default CouchDB hold time of an idle plan ``_changes`` longpoll.
DEFAULT_PLAN_LONGPOLL_TIMEOUT_MS = 60_000


def _plan_longpoll_timeout_ms(config: Mapping[str, Any]) -> int:
    """Read ``yggdrasil.plan_watcher_longpoll_timeout_ms`` (positive int)."""
    value = config.get("yggdrasil", {}).get(
        "plan_watcher_longpoll_timeout_ms", DEFAULT_PLAN_LONGPOLL_TIMEOUT_MS
    )
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(
            "yggdrasil.plan_watcher_longpoll_timeout_ms must be a positive "
            f"integer (milliseconds), got {value!r}"
        )
    return value


def build_internal_storage(
    config: Mapping[str, Any],
    *,
    dev_mode: bool | None = None,
) -> InternalStorageBundle:
    """Resolve configuration and build the internal-storage bundle.

    Args:
        config: The full loaded main configuration mapping.
        dev_mode: Effective dev mode override for tests. Defaults to
            ``YggSession.is_dev()``.

    Returns:
        InternalStorageBundle for the configured backend (CouchDB when the
        ``internal_storage`` block is absent — the deprecated implicit path).

    Raises:
        InternalStorageConfigurationError: On invalid explicit configuration.
    """
    resolved = resolve_internal_storage_config(config, dev_mode=dev_mode)

    if resolved is None or isinstance(resolved, CouchInternalStorageConfig):
        from yggdrasil.storage.couchdb.bundle import build_couchdb_bundle

        bundle = build_couchdb_bundle(
            resolved, longpoll_timeout_ms=_plan_longpoll_timeout_ms(config)
        )
    else:
        from yggdrasil.storage.sqlite import build_sqlite_bundle

        bundle = build_sqlite_bundle(resolved)

    logger.info("Internal storage backend: %s", bundle.backend)
    return bundle
