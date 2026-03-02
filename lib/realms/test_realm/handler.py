"""
Test realm handler for processing test scenario documents.

TestRealmHandler responds to COUCHDB_DOC_CHANGED events for scenario documents,
generating execution plans from scenario documents stored in the yggdrasil database.
"""

import logging
from typing import Any, ClassVar, cast

from lib.core_utils.event_types import EventType
from lib.core_utils.logging_utils import custom_logger
from lib.handlers.base_handler import BaseHandler
from lib.realms.test_realm.templates import (
    TEMPLATES,
    data_fetch_plan_steps,
    get_template,
)
from yggdrasil.flow.model import Plan
from yggdrasil.flow.planner.api import PlanDraft, PlanningContext


class TestRealmHandler(BaseHandler):
    """
    Handler for test scenario documents.

    Processes documents with type="ygg_test_scenario" from the yggdrasil database,
    generating plans based on the specified template.

    Document schema:
        {
            "_id": "test_scenario:<unique_id>",
            "type": "ygg_test_scenario",
            "template": "happy_path",  # One of TEMPLATES keys
            "auto_run": true,          # Optional, default True
            "overrides": {             # Optional step param overrides
                "step_id": {"param": "value"}
            }
        }

    Note: Uses generic COUCHDB_DOC_CHANGED event type. Document filtering
    is handled by the WatchSpec's filter_expr in the realm descriptor.
    """

    event_type: ClassVar[EventType] = EventType.COUCHDB_DOC_CHANGED
    handler_id: ClassVar[str] = "test_scenario_handler"

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._logger = logger or custom_logger(f"{__name__}.{type(self).__name__}")

    def derive_scope(self, doc: dict[str, Any]) -> dict[str, Any]:
        """
        Extract scope from test scenario document.

        Args:
            doc: Scenario document from yggdrasil DB

        Returns:
            Scope dict with kind='test_scenario' and id from document
        """
        # Use _id or fall back to a generated ID
        doc_id = doc.get("_id", doc.get("scenario_id", "unknown"))
        return {"kind": "test_scenario", "id": doc_id}

    def _parse_custom_steps(self, steps_config: list[dict[str, Any]]) -> list:
        """
        Parse custom steps from scenario document.

        Args:
            steps_config: List of step definitions from document

        Returns:
            List of StepSpec objects

        Example step config:
            {
                "step_id": "my_step",
                "name": "My Step",
                "fn_name": "step_echo",
                "params": {"message": "Hello"},
                "deps": ["previous_step"]
            }
        """
        from yggdrasil.flow.model import StepSpec

        _FN_REF_PREFIX = "lib.realms.test_realm.steps"
        steps = []

        for step_cfg in steps_config:
            if not isinstance(step_cfg, dict):
                raise ValueError(f"Step config must be a dict, got: {type(step_cfg)}")

            # Required fields
            step_id = step_cfg.get("step_id")
            fn_name = step_cfg.get("fn_name")

            if not step_id or not fn_name:
                raise ValueError(
                    f"Step config must have 'step_id' and 'fn_name': {step_cfg}"
                )

            # Optional fields - use cast for type safety
            name = cast(str, step_cfg.get("name", step_id))
            params = cast(dict[str, Any], step_cfg.get("params", {}))
            deps = cast(list[str], step_cfg.get("deps", []))

            step = StepSpec(
                step_id=step_id,
                name=name,
                fn_ref=f"{_FN_REF_PREFIX}.{fn_name}",
                params=params,
                deps=deps,
            )
            steps.append(step)

        return steps

    async def _do_plan_time_fetch(self, ctx: PlanningContext) -> str:
        """
        Fetch the data-access reference document from CouchDB during planning.

        Returns a human-readable string that gets baked into the generated
        plan's step params, making the fetch observable in the plan record.
        On any error the method returns a descriptive error string rather than
        raising, so the plan is still created (with an error message visible
        in the step params).
        """
        from yggdrasil.flow.data_access import DataAccessError

        try:
            client = ctx.data.couchdb("yggdrasil_db")
            doc = await client.get("data_access_test:reference_doc")
            if doc is None:
                return "reference doc not found in yggdrasil_db"
            msg = doc.get("message", "<no message field>")
            value = doc.get("value", "<no value field>")
            return f"Fetched at plan time: {msg!r} (value={value})"
        except DataAccessError as exc:
            return f"DataAccess error during planning: {exc}"
        except Exception as exc:
            return f"Unexpected error during planning fetch: {exc}"

    async def generate_plan_draft(self, payload: dict[str, Any]) -> PlanDraft:
        """
        Generate a PlanDraft from the scenario document.

        Supports three modes:
        1. data_fetch_plan: Fetches a CouchDB document *during planning* and
           bakes the result into the step params — proving the fetch happened
           at plan-generation time by making it visible in the persisted plan.
        2. Template-based: Provide 'template' field (e.g., 'happy_path').
        3. Custom steps: Provide 'steps' array directly.

        Args:
            payload: Event payload containing:
                - doc: The scenario document
                - planning_ctx: PlanningContext from YggdrasilCore

        Returns:
            PlanDraft with plan, auto_run flag, and notes

        Raises:
            ValueError: If document type is not 'ygg_test_scenario'
        """

        doc = payload.get("doc", {})
        ctx: PlanningContext = payload["planning_ctx"]

        # Guard: Verify document type (defense-in-depth; WatchSpec filters too)
        doc_type = doc.get("type")
        if doc_type != "ygg_test_scenario":
            raise ValueError(
                f"Expected doc.type='ygg_test_scenario', got '{doc_type}'. "
                f"Document _id: {doc.get('_id')}"
            )

        template_name = doc.get("template")
        custom_steps = doc.get("steps")

        # --- Mode 1: Planning-time data fetch ---
        if template_name == "data_fetch_plan":
            self._logger.info(
                "Generating data_fetch_plan for scenario '%s': "
                "fetching reference doc from CouchDB during planning",
                doc.get("_id"),
            )
            fetched_message = await self._do_plan_time_fetch(ctx)
            steps = data_fetch_plan_steps(fetched_message=fetched_message)
            notes = (
                "Planning-time data fetch: reference doc content baked into step params"
            )
            preview = {
                "template": "data_fetch_plan",
                "step_count": len(steps),
                "step_names": [s.name for s in steps],
                "planning_time_fetch": True,
                "fetched_message": fetched_message,
            }

        # --- Mode 2: Standard template-based ---
        elif template_name:
            if template_name not in TEMPLATES:
                raise ValueError(
                    f"Unknown template '{template_name}'. "
                    f"Available: {list(TEMPLATES.keys())}"
                )

            overrides = doc.get("overrides", {})

            self._logger.info(
                "Generating plan from template '%s' for scenario '%s'",
                template_name,
                doc.get("_id"),
            )
            template_fn = get_template(template_name)
            steps = template_fn(overrides=overrides)
            notes = f"Test scenario using template '{template_name}'"
            preview = {
                "template": template_name,
                "step_count": len(steps),
                "step_names": [s.name for s in steps],
            }

        # --- Mode 3: Custom steps ---
        elif custom_steps:
            self._logger.info(
                "Generating plan from custom steps for scenario '%s'",
                doc.get("_id"),
            )
            steps = self._parse_custom_steps(custom_steps)
            notes = f"Test scenario with {len(steps)} custom step(s)"
            preview = {
                "template": None,
                "step_count": len(steps),
                "step_names": [s.name for s in steps],
            }

        else:
            raise ValueError(
                f"Scenario document must have either 'template' or 'steps' field: {doc.get('_id')}"
            )

        # Build Plan
        plan = Plan(
            plan_id=f"test_realm:{ctx.scope['id']}",
            realm=self.realm_id or "test_realm",
            scope=ctx.scope,
            steps=steps,
        )

        auto_run = doc.get("auto_run", True)

        return PlanDraft(
            plan=plan,
            auto_run=auto_run,
            approvals_required=[],
            notes=notes,
            preview=preview,
        )
