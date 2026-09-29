"""Composition root: build the internal-storage bundle from configuration."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from lib.core_utils.logging_utils import custom_logger
from lib.storage.config import (
    CouchInternalStorageConfig,
    resolve_internal_storage_config,
)
from lib.storage.protocols import InternalStorageBundle

logger = custom_logger(__name__)


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
        from lib.storage.couch import build_couchdb_bundle

        bundle = build_couchdb_bundle(resolved)
    else:
        from lib.storage.sqlite import build_sqlite_bundle

        bundle = build_sqlite_bundle(resolved)

    logger.info("Internal storage backend: %s", bundle.backend)
    return bundle
