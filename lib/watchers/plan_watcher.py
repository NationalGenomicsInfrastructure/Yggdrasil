"""
PlanWatcher: Monitors yggdrasil_plans database for approved plans.

This watcher implements the Plan Approval & Execution Workflow by:
1. Watching the _changes feed on yggdrasil_plans
2. Filtering for eligible plans (status='approved', run_token > executed_run_token)
3. Emitting events for execution by YggdrasilCore
4. Managing checkpoint persistence for restart safety

Key design principles:
- Uses CouchDBCheckpointStore for DB-backed checkpoint persistence
- Uses ChangesFetcher for generic _changes streaming
- Uses is_plan_eligible() pure function for eligibility logic
- Graceful error handling (log + continue, no crash)
"""

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, cast

from lib.core_utils.event_types import EventType
from lib.core_utils.logging_utils import custom_logger
from lib.core_utils.plan_eligibility import get_eligibility_reason, is_plan_eligible
from lib.couchdb.changes_fetcher import ChangesFetcher
from lib.couchdb.couchdb_connection import CouchDBHandler
from lib.couchdb.plan_db_manager import PlanDBManager
from lib.couchdb.yggdrasil_db_manager import YggdrasilDBManager
from lib.storage.couch import CouchPlanChangeSource
from lib.storage.protocols import PlanChangeSource, PlanStore
from lib.watchers.abstract_watcher import AbstractWatcher, YggdrasilEvent
from lib.watchers.backends.base import Checkpoint, CheckpointStore, RawWatchEvent
from lib.watchers.backends.checkpoint_store import CouchDBCheckpointStore


class PlanWatcher(AbstractWatcher):
    """
    Watches yggdrasil_plans database for approved plans ready for execution.

    This watcher:
    - Polls the _changes feed on yggdrasil_plans
    - Filters changes to only process eligible plans
    - Emits PLAN_EXECUTION_EVENT for each eligible plan
    - Persists checkpoint after each successful emit
    - Supports targeted run-once and future daemon-startup recovery

    The watcher does NOT execute plans directly. It emits events that
    YggdrasilCore handles via execute_approved_plan().

    Storage is backend-neutral: YggdrasilCore injects the plan store, change
    source, and checkpoint store from its InternalStorageBundle. Bare
    construction (no injection) falls back to the legacy CouchDB wiring.

    Attributes:
        plan_db: PlanStore for plan documents
        checkpoint_store: CheckpointStore for checkpoint persistence
        change_source: PlanChangeSource streaming RawWatchEvents
        poll_interval_sec: Seconds between poll cycles
    """

    CHECKPOINT_KEY = "watcher:PlanWatcher"

    def __init__(
        self,
        on_event: Callable[[YggdrasilEvent], None],
        poll_interval_sec: float = 5.0,
        *,
        plan_store: PlanStore | None = None,
        change_source: PlanChangeSource | None = None,
        checkpoint_store: CheckpointStore | None = None,
        execution_authority_filter: str | None = None,
        execution_owner_filter: str | None = None,
        logger: logging.Logger | None = None,
    ):
        """
        Initialize PlanWatcher.

        Args:
            on_event: Callback to invoke when eligible plan is detected
            poll_interval_sec: Seconds between change poll cycles (default 5.0)
            plan_store: Injected plan store; legacy default is PlanDBManager.
            change_source: Injected plan change source; legacy default wraps a
                ChangesFetcher on the plan store (requires a CouchDB store).
            checkpoint_store: Injected checkpoint store; legacy default is
                CouchDBCheckpointStore on the yggdrasil database.
            execution_authority_filter: If set, only process plans with this origin.
                - "daemon": Normal daemon operation (skips run_once plans)
                - "run_once": Scoped run-once operation
                - None: Process all plans (legacy behavior, NOT recommended)
            execution_owner_filter: If set, only process plans with this owner.
                Used in run-once mode to isolate concurrent CLI invocations.
            logger: Optional logger; uses module logger if None
        """
        self._logger = logger or custom_logger(f"{__name__}.{type(self).__name__}")
        super().__init__(
            on_event=on_event,
            event_type=EventType.PLAN_EXECUTION,
            name="PlanWatcher",
            logger=self._logger,
        )

        self.poll_interval_sec = poll_interval_sec
        self.execution_authority_filter = execution_authority_filter
        self.execution_owner_filter = execution_owner_filter

        # Storage wiring: injected by YggdrasilCore, or legacy CouchDB
        # defaults for bare construction. Partial injection that pairs a
        # non-CouchDB plan store with a default change source is unsupported.
        if plan_store is None:
            plan_store = PlanDBManager()
        self.plan_db = plan_store

        if checkpoint_store is None:
            self._yggdrasil_db = YggdrasilDBManager()
            checkpoint_store = CouchDBCheckpointStore(
                db_manager=self._yggdrasil_db,
            )
        self.checkpoint_store = checkpoint_store

        if change_source is None:
            self.changes_fetcher = ChangesFetcher(
                db_handler=cast(CouchDBHandler, self.plan_db), include_docs=True
            )
            change_source = CouchPlanChangeSource(self.changes_fetcher)
        self.change_source = change_source

        self._logger.debug(
            "PlanWatcher initialized (poll_interval=%.1fs, authority_filter=%r, owner_filter=%r)",
            poll_interval_sec,
            execution_authority_filter,
            execution_owner_filter,
        )

    async def start(self) -> None:
        """
        Start watching for plan changes.

        Resumes from checkpoint if available; otherwise starts from current head.
        Runs until stop() is called.

        Uses the injected PlanChangeSource's stream_changes_continuously()
        as the canonical polling/backoff path.
        """
        if self._running:
            self._logger.warning("PlanWatcher already running; ignoring start()")
            return

        self._running = True
        self._logger.info("Starting PlanWatcher...")

        # Get starting checkpoint. If missing, start from "now" to avoid
        # replaying full history by default for plans.
        checkpoint = self.checkpoint_store.load(self.CHECKPOINT_KEY)
        if checkpoint and checkpoint.value is not None:
            current_seq = str(checkpoint.value)
            self._logger.info("Resuming from checkpoint: seq='%s'", current_seq)
        else:
            self._logger.info("No checkpoint found; starting from 'now'")
            current_seq = "now"

        try:
            async for change in self.change_source.stream_changes_continuously(
                since=current_seq,
                poll_interval_sec=self.poll_interval_sec,
            ):
                if not self._running:
                    break

                # Evaluate the change (filter + eligibility check + emit)
                await self._evaluate_change(change)

                # Update current_seq and checkpoint after each change
                new_seq = change.seq
                if new_seq is not None:
                    current_seq = str(new_seq)
                    try:
                        self.checkpoint_store.save(
                            Checkpoint(
                                backend_key=self.CHECKPOINT_KEY,
                                value=current_seq,
                                updated_at=datetime.now(UTC).isoformat(),
                            )
                        )
                    except Exception as e:
                        self._logger.error(
                            "Failed to save PlanWatcher checkpoint for change id=%r seq=%r; continuing: %s",
                            change.id,
                            current_seq,
                            e,
                            exc_info=True,
                        )
        except Exception as e:
            # stream_changes_continuously handles transient retry/backoff internally.
            # Unexpected errors are logged and terminate start().
            self._logger.error("PlanWatcher stream terminated with error: %s", e)

        self._logger.info("PlanWatcher stopped.")

    async def stop(self) -> None:
        """
        Stop watching for plan changes.

        Sets _running=False; the start() loop will exit after current cycle.
        """
        if not self._running:
            self._logger.debug("PlanWatcher not running; ignoring stop()")
            return

        self._logger.info("Stopping PlanWatcher...")
        self._running = False

    async def _evaluate_change(self, change: RawWatchEvent) -> None:
        """
        Evaluate a single plan change event.

        Applies filtering criteria (origin, owner), checks eligibility,
        and emits event if plan is ready for execution.

        Args:
            change: Backend-agnostic plan change event.
        """
        doc_id = change.id
        doc = change.doc

        # Skip deleted documents (tombstones)
        if change.deleted:
            self._logger.debug("Skipping deleted document: %s", doc_id)
            return

        # Skip if doc not included (shouldn't happen; sources include docs)
        if not doc:
            self._logger.warning("Change has no 'doc' field: %s", doc_id)
            return

        # --- Execution origin filtering ---
        doc_authority = doc.get("execution_authority")

        # Schema validation: skip plans missing execution_authority
        if doc_authority is None:
            self._logger.warning(
                "Skipping plan with missing execution_authority: %s", doc_id
            )
            return

        # Origin filter: skip plans that don't match our filter
        if (
            self.execution_authority_filter
            and doc_authority != self.execution_authority_filter
        ):
            self._logger.debug(
                "Skipping plan %s: authority=%r doesn't match filter=%r",
                doc_id,
                doc_authority,
                self.execution_authority_filter,
            )
            return

        # --- Execution owner filtering (for run-once scoping) ---
        if self.execution_owner_filter:
            doc_owner = doc.get("execution_owner")
            if doc_owner != self.execution_owner_filter:
                self._logger.debug(
                    "Skipping plan %s: owner=%r doesn't match filter=%r",
                    doc_id,
                    doc_owner,
                    self.execution_owner_filter,
                )
                return

        # Check eligibility (status + token logic)
        if is_plan_eligible(doc):
            self._logger.info(
                "Eligible plan detected: %s (authority=%s, run_token=%s, executed_run_token=%s)",
                doc_id,
                doc_authority,
                doc.get("run_token", 0),
                doc.get("executed_run_token", -1),
            )
            # Emit event for execution
            payload = {
                "plan_doc_id": doc_id,
                "plan_doc": doc,
            }
            await self.emit(payload, source="PlanWatcher")
        else:
            reason = get_eligibility_reason(doc)
            self._logger.debug("Skipping ineligible plan: %s (%s)", doc_id, reason)

    async def recover_pending_plans(
        self, plan_ids: list[str] | None = None
    ) -> list[dict[str, Any]]:
        """Query, filter, and emit currently eligible plans.

        Scoped run-once recovery supplies ``plan_ids`` so only plans created
        by that invocation are fetched. Passing ``None`` retains the full
        eligible-plan scan needed by a future daemon-startup recovery path.
        Both paths apply the configured authority and owner filters.

        This method does not load or save checkpoints, and it does not
        coordinate a recovery scan with the live change-feed cursor.

        Returns:
            Plan documents that were emitted for execution.
        """
        if plan_ids is None:
            self._logger.info(
                "Running eligible-plan recovery: querying all approved "
                "pending plans..."
            )
            # Preserve the full-scan branch for future daemon-startup recovery.
            all_eligible = self.plan_db.query_approved_pending()
        else:
            self._logger.info(
                "Running scoped eligible-plan recovery for %d plan(s)...",
                len(plan_ids),
            )
            all_eligible = []
            # A full scan can return each document only once. Preserve that
            # behavior while retaining the caller's order.
            for plan_id in dict.fromkeys(plan_ids):
                try:
                    plan_doc = self.plan_db.fetch_plan(plan_id)
                except Exception as exc:
                    self._logger.error(
                        "Recovery: failed to fetch requested plan %s: %s",
                        plan_id,
                        exc,
                        exc_info=True,
                    )
                    continue
                if plan_doc is None:
                    self._logger.warning(
                        "Recovery: requested plan %s was not found", plan_id
                    )
                    continue
                if not is_plan_eligible(plan_doc):
                    self._logger.debug(
                        "Recovery: requested plan %s is not eligible (%s)",
                        plan_id,
                        get_eligibility_reason(plan_doc),
                    )
                    continue
                all_eligible.append(plan_doc)

        # Apply origin and owner filters
        filtered_plans: list[dict[str, Any]] = []
        for plan_doc in all_eligible:
            doc_id = plan_doc.get("_id", "unknown")
            doc_authority = plan_doc.get("execution_authority")
            doc_owner = plan_doc.get("execution_owner")

            # Skip plans missing execution_authority
            if doc_authority is None:
                self._logger.warning(
                    "Recovery: skipping %s (missing execution_authority)", doc_id
                )
                continue

            # Apply execution_authority filter
            if (
                self.execution_authority_filter
                and doc_authority != self.execution_authority_filter
            ):
                self._logger.debug(
                    "Recovery: skipping %s (authority=%s, filter=%s)",
                    doc_id,
                    doc_authority,
                    self.execution_authority_filter,
                )
                continue

            # Skip if owner doesn't match filter
            if self.execution_owner_filter and doc_owner != self.execution_owner_filter:
                self._logger.debug(
                    "Recovery: skipping %s (owner=%r != filter=%r)",
                    doc_id,
                    doc_owner,
                    self.execution_owner_filter,
                )
                continue

            filtered_plans.append(plan_doc)

        self._logger.info(
            "Found %d eligible plans for recovery (of %d total eligible)",
            len(filtered_plans),
            len(all_eligible),
        )

        for plan_doc in filtered_plans:
            doc_id = plan_doc.get("_id", "unknown")
            self._logger.info("Recovery: emitting eligible plan %s", doc_id)
            payload = {
                "plan_doc_id": doc_id,
                "plan_doc": plan_doc,
            }
            await self.emit(payload, source="PlanWatcher:recovery")

        return filtered_plans
