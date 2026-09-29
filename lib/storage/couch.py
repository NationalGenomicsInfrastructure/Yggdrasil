"""CouchDB internal-storage bundle.

Wraps the existing CouchDB implementations (``PlanDBManager``,
``ChangesFetcher``, ``CouchDBCheckpointStore``, ``OpsWriter``) behind the
internal-storage protocols. Database and document IDs, plan JSON and
``_rev`` handling, ``_changes`` semantics, checkpoint documents, and
snapshot upserts are all preserved — the adapters only translate shapes.
"""

from __future__ import annotations

import logging
import os
from collections.abc import AsyncIterator

from lib.core_utils.logging_utils import custom_logger
from lib.couchdb.changes_fetcher import ChangesFetcher
from lib.couchdb.plan_db_manager import PlanDBManager
from lib.couchdb.yggdrasil_db_manager import YggdrasilDBManager
from lib.ops.sinks.couch import OpsWriter
from lib.storage.config import CouchInternalStorageConfig
from lib.storage.protocols import InternalStorageBundle
from lib.watchers.backends.base import RawWatchEvent
from lib.watchers.backends.checkpoint_store import CouchDBCheckpointStore

logger = custom_logger(__name__)

_LEGACY_OPS_DB = "yggdrasil_ops"


class CouchPlanChangeSource:
    """Adapts ``ChangesFetcher`` dict changes to ``RawWatchEvent`` objects.

    Attributes:
        _fetcher: The underlying ChangesFetcher (bound to the plans DB,
            include_docs=True).
    """

    def __init__(self, changes_fetcher: ChangesFetcher) -> None:
        """Initialize with a ChangesFetcher bound to the plans database."""
        self._fetcher = changes_fetcher

    async def stream_changes_continuously(
        self,
        *,
        since: str | int,
        poll_interval_sec: float,
    ) -> AsyncIterator[RawWatchEvent]:
        """Stream plan changes as backend-agnostic events.

        Args:
            since: CouchDB sequence cursor, or "now" for the current head.
            poll_interval_sec: Seconds between _changes polls.

        Yields:
            RawWatchEvent per change entry; ``meta`` carries the remaining
            CouchDB change-entry fields for diagnostics only.
        """
        async for change in self._fetcher.stream_changes_continuously(
            since=str(since),
            poll_interval_sec=poll_interval_sec,
        ):
            yield RawWatchEvent(
                id=str(change.get("id", "")),
                doc=change.get("doc"),
                seq=change.get("seq"),
                deleted=bool(change.get("deleted", False)),
                meta={
                    k: v
                    for k, v in change.items()
                    if k not in ("id", "doc", "seq", "deleted")
                },
            )


def build_couchdb_bundle(
    cfg: CouchInternalStorageConfig | None,
    *,
    log: logging.Logger | None = None,
) -> InternalStorageBundle:
    """Build the CouchDB internal-storage bundle.

    Args:
        cfg: Explicit configuration, or None for the legacy implicit path
            (deprecated): fixed database names resolved through the default
            ``couchdb`` endpoint, with ``OPS_DB`` honored for the operations
            database name.
        log: Optional logger override.

    Returns:
        InternalStorageBundle backed by CouchDB.
    """
    _log = log or logger

    if cfg is None:
        _log.warning(
            "No 'internal_storage' block in configuration; using implicit "
            "CouchDB internal storage (deprecated). Explicit configuration "
            "will be required in a future release."
        )
        ops_db_env = os.environ.get("OPS_DB")
        if ops_db_env:
            _log.warning(
                "OPS_DB=%s is honored only by the implicit legacy "
                "configuration and is deprecated; set "
                "internal_storage.couchdb.connections.operations instead.",
                ops_db_env,
            )
        plans = PlanDBManager()
        # Eagerly construct the coordination manager (matching pre-bundle Core
        # behavior). A lazy CouchDBCheckpointStore() would defer the 'yggdrasil'
        # connection until first checkpoint load, where failures are swallowed
        # (returning None) and PlanWatcher silently starts at "now" — failing
        # open instead of failing fast at startup.
        checkpoints = CouchDBCheckpointStore(db_manager=YggdrasilDBManager())
        ops_snapshots = OpsWriter(db_name=ops_db_env or _LEGACY_OPS_DB)
    else:
        plans = PlanDBManager(
            url=cfg.plans.endpoint.url,
            user_env=cfg.plans.endpoint.user_env,
            pass_env=cfg.plans.endpoint.pass_env,
            db_name=cfg.plans.db_name,
        )
        coordination_dbm = YggdrasilDBManager(
            url=cfg.coordination.endpoint.url,
            user_env=cfg.coordination.endpoint.user_env,
            pass_env=cfg.coordination.endpoint.pass_env,
            db_name=cfg.coordination.db_name,
        )
        checkpoints = CouchDBCheckpointStore(db_manager=coordination_dbm)
        # Explicit configuration is authoritative: OPS_DB is ignored here.
        ops_snapshots = OpsWriter(
            db_name=cfg.operations.db_name,
            url=cfg.operations.endpoint.url,
            user_env=cfg.operations.endpoint.user_env,
            pass_env=cfg.operations.endpoint.pass_env,
        )

    plan_changes = CouchPlanChangeSource(
        ChangesFetcher(db_handler=plans, include_docs=True)
    )

    return InternalStorageBundle(
        backend="couchdb",
        plans=plans,
        plan_changes=plan_changes,
        checkpoints=checkpoints,
        ops_snapshots=ops_snapshots,
    )
