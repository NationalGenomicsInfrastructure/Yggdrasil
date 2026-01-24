"""
PlanWatcher: Monitors yggdrasil_plans database for approved plans.

This watcher implements the Plan Approval & Execution Workflow by:
1. Watching the _changes feed on yggdrasil_plans
2. Filtering for eligible plans (status='approved', run_token > executed_run_token)
3. Emitting events for execution by YggdrasilCore
4. Managing checkpoint persistence for restart safety

Key design principles:
- Uses WatcherCheckpointStore for DB-backed checkpoint persistence
- Uses ChangesFetcher for generic _changes streaming
- Uses is_plan_eligible() pure function for eligibility logic
- Graceful error handling (log + continue, no crash)
"""

import asyncio
import logging
from collections.abc import Callable
from typing import Any

from lib.core_utils.event_types import EventType
from lib.core_utils.plan_eligibility import get_eligibility_reason, is_plan_eligible
from lib.couchdb.changes_fetcher import ChangesFetcher
from lib.couchdb.plan_db_manager import PlanDBManager
from lib.couchdb.watcher_checkpoint_store import WatcherCheckpointStore
from lib.couchdb.yggdrasil_db_manager import YggdrasilDBManager
from lib.watchers.abstract_watcher import AbstractWatcher, YggdrasilEvent


class PlanWatcher(AbstractWatcher):
    """
    Watches yggdrasil_plans database for approved plans ready for execution.

    This watcher:
    - Polls the _changes feed on yggdrasil_plans
    - Filters changes to only process eligible plans
    - Emits PLAN_EXECUTION_EVENT for each eligible plan
    - Persists checkpoint after each successful emit
    - Supports startup recovery via query_approved_pending()

    The watcher does NOT execute plans directly. It emits events that
    YggdrasilCore handles via execute_approved_plan().

    Attributes:
        plan_db: PlanDBManager for accessing yggdrasil_plans
        checkpoint_store: WatcherCheckpointStore for checkpoint persistence
        changes_fetcher: ChangesFetcher for streaming _changes
        poll_interval_sec: Seconds between poll cycles
    """

    def __init__(
        self,
        on_event: Callable[[YggdrasilEvent], None],
        poll_interval_sec: float = 5.0,
        *,
        execution_authority_filter: str | None = None,
        execution_owner_filter: str | None = None,
        logger: logging.Logger | None = None,
    ):
        """
        Initialize PlanWatcher.

        Args:
            on_event: Callback to invoke when eligible plan is detected
            poll_interval_sec: Seconds between _changes poll cycles (default 5.0)
            execution_authority_filter: If set, only process plans with this origin.
                - "daemon": Normal daemon operation (skips run_once plans)
                - "run_once": Scoped run-once operation
                - None: Process all plans (legacy behavior, NOT recommended)
            execution_owner_filter: If set, only process plans with this owner.
                Used in run-once mode to isolate concurrent CLI invocations.
            logger: Optional logger; uses module logger if None
        """
        super().__init__(
            on_event=on_event,
            event_type=EventType.PLAN_EXECUTION,
            name="PlanWatcher",
            logger=logger,
        )

        self.poll_interval_sec = poll_interval_sec
        self.execution_authority_filter = execution_authority_filter
        self.execution_owner_filter = execution_owner_filter

        # Initialize DB managers
        self.plan_db = PlanDBManager()
        self._yggdrasil_db = YggdrasilDBManager()

        # Initialize checkpoint store (persists to yggdrasil DB)
        self.checkpoint_store = WatcherCheckpointStore(
            watcher_name="PlanWatcher",
            db_handler=self._yggdrasil_db,
            logger=self._logger,
        )

        # Initialize changes fetcher (reads from yggdrasil_plans DB)
        self.changes_fetcher = ChangesFetcher(
            db_handler=self.plan_db,
            include_docs=True,
            logger=self._logger,
        )

        self._logger.debug(
            "PlanWatcher initialized (poll_interval=%.1fs, authority_filter=%r, owner_filter=%r)",
            poll_interval_sec,
            execution_authority_filter,
            execution_owner_filter,
        )

    async def start(self) -> None:
        """
        Start watching for plan changes.

        Resumes from checkpoint if available; otherwise starts from beginning.
        Runs until stop() is called.

        The watcher loop:
        1. Fetch changes since last checkpoint
        2. For each change, check eligibility
        3. Emit event for eligible plans
        4. Update checkpoint
        5. Sleep and repeat
        """
        if self._running:
            self._logger.warning("PlanWatcher already running; ignoring start()")
            return

        self._running = True
        self._logger.info("Starting PlanWatcher...")

        # Get starting checkpoint (None = start from "0")
        current_seq = self.checkpoint_store.get_checkpoint()
        if current_seq:
            self._logger.info("Resuming from checkpoint: seq='%s'", current_seq)
        else:
            self._logger.info("No checkpoint found; starting from beginning")
            current_seq = "0"

        while self._running:
            try:
                # Fetch changes since last checkpoint
                async for change in self.changes_fetcher.fetch_changes(
                    since=current_seq
                ):
                    if not self._running:
                        break

                    # Update current_seq for checkpoint
                    new_seq = change.get("seq")
                    if new_seq:
                        current_seq = new_seq

                    # Evaluate the change (filter + eligibility check + emit)
                    await self._evaluate_change(change)

                    # Save checkpoint after each change (durability)
                    if new_seq:
                        self.checkpoint_store.save_checkpoint(new_seq)

            except Exception as e:
                self._logger.error("Error in PlanWatcher loop: %s", e, exc_info=True)
                # Continue polling despite errors (resilience)

            # Sleep between poll cycles
            if self._running:
                await asyncio.sleep(self.poll_interval_sec)

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

    async def _evaluate_change(self, change: dict[str, Any]) -> None:
        """
        Evaluate a single change entry from _changes feed.

        Applies filtering criteria (origin, owner), checks eligibility,
        and emits event if plan is ready for execution.

        Args:
            change: Change entry with 'id', 'seq', 'doc' fields
        """
        doc_id = change.get("id", "")
        doc = change.get("doc")

        # Skip design documents
        if doc_id.startswith("_design/"):
            self._logger.debug("Skipping design document: %s", doc_id)
            return

        # Skip deleted documents
        if change.get("deleted"):
            self._logger.debug("Skipping deleted document: %s", doc_id)
            return

        # Skip if doc not included (shouldn't happen with include_docs=True)
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

    async def recover_pending_plans(self) -> list[dict[str, Any]]:
        """
        Query and emit events for all approved pending plans.

        This is the startup recovery fallback when checkpoint is missing.
        It queries all plans where status='approved' and run_token > executed_run_token,
        then applies execution_authority and execution_owner filters.

        Returns:
            list: Plan documents that were emitted for execution

        Note:
            This method is called by YggdrasilCore during startup recovery,
            NOT during normal watcher operation. It does NOT update checkpoints;
            the caller is responsible for checkpoint management after recovery.
        """
        self._logger.info(
            "Running startup recovery: querying approved pending plans..."
        )

        # Get all eligible plans (approval + token logic)
        all_eligible = self.plan_db.query_approved_pending()

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
