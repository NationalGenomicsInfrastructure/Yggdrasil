# lib/handlers/project_handler.py

import asyncio

# import contextvars
import logging
from typing import Any

from lib.core_utils.common import YggdrasilUtilities as Ygg
from lib.couchdb.yggdrasil_db_manager import YggdrasilDBManager
from lib.handlers.base_handler import BaseHandler

# A ContextVar to carry a per‐event trace identifier through logs
# event_trace_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("event_trace_id", default=None)


class BestPracticeAnalysisHandler(BaseHandler):
    """
    Handles ProjectDB 'document_change' events by loading the appropriate
    best-practice realm and running it—either in the background (__call__)
    or synchronously (run_now).
    """

    def __init__(self):
        self.ydm = YggdrasilDBManager()
        self.logger = logging.getLogger("BPA-Handler")

    async def handle_task(self, payload: dict[str, Any]) -> None:
        """
        Core coroutine invoked by both __call__ (via asyncio.create_task) and run_now.
        """
        doc = payload.get("document")
        module_loc = payload.get("module_location")

        if not isinstance(doc, dict):
            self.logger.warning(
                "handle_async: missing or invalid 'document' in payload"
            )
            return

        if not isinstance(module_loc, str):
            self.logger.warning(
                "handle_async: missing or invalid 'module_location' in payload"
            )
            return

        project_id = doc.get("project_id", "<unknown>")

        self.logger.info(
            "Processing ProjectDB change for project %s → module %s",
            project_id,
            module_loc,
        )

        realm_cls = Ygg.load_realm_class(module_loc)
        if realm_cls is None:
            self.logger.error(
                "Cannot load realm class '%s' for project %s", module_loc, project_id
            )
            return

        realm = realm_cls(doc, self.ydm)
        if not getattr(realm, "proceed", False):
            self.logger.info("Realm skipped (proceed=False) for project %s", project_id)
            return

        self.logger.info("Launching realm for project %s", project_id)
        try:
            await realm.launch_template()
            self.logger.info("Realm completed for project %s", project_id)
        except Exception:
            self.logger.error(
                "Error running realm for project %s", project_id, exc_info=True
            )

    def __call__(self, payload: dict[str, Any]) -> None:
        """
        Schedule the async handler under the running loop.
        """

        # Schedule the async work without blocking the caller
        try:
            asyncio.create_task(self.handle_task(payload))
        except RuntimeError:
            # No running loop? fallback to ensure_future if appropriate
            # TODO: might be needed for standalone calls, remove if not
            loop = asyncio.get_event_loop()
            loop.create_task(self.handle_task(payload))

    # def run_now(self, payload: Dict[str, Any]) -> None:
    #     """
    #     Blocking, one-off invocation for CLI mode.
    #     Validates payload, loads the realm, and runs it to completion.
    #     """
    #     # 1) Validate exactly as in __call__():
    #     doc = payload.get("document")
    #     module_loc = payload.get("module_location")
    #     if not isinstance(doc, dict):
    #         self.logger.error("run_now: missing or invalid 'document'")
    #         return
    #     if not isinstance(module_loc, str):
    #         self.logger.error("run_now: missing or invalid 'module_location'")
    #         return

    #     # 2) Resolve the realm class
    #     realm_cls = Ygg.load_realm_class(module_loc)
    #     if realm_cls is None:
    #         self.logger.error("run_now: cannot load realm '%s'", module_loc)
    #         return

    #     # 3) Run the async logic to completion
    #     try:
    #         asyncio.run(self._run_realm(realm_cls, doc))
    #     except Exception as exc:
    #         self.logger.error("run_now: execution failed: %s", exc, exc_info=True)
