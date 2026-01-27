"""
Test realm handler for processing test scenario documents.

TestRealmHandler responds to TEST_SCENARIO_CHANGE events, generating
execution plans from scenario documents stored in the yggdrasil database.
"""

import asyncio
from typing import Any, ClassVar

from lib.core_utils.logging_utils import custom_logger
from lib.handlers.base_handler import BaseHandler
from lib.realms.test_realm.templates import TEMPLATES, get_template
from yggdrasil.core_utils.event_types import EventType  # type: ignore
from yggdrasil.flow.model import Plan
from yggdrasil.flow.planner.api import PlanDraft, PlanningContext

logging = custom_logger(__name__.split(".")[-1])


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
    """

    event_type: ClassVar[EventType] = EventType.TEST_SCENARIO_CHANGE
    realm_id: ClassVar[str] = "test_realm"

    def derive_scope(self, doc: dict[str, Any]) -> dict[str, Any]:
        """
        Extract scope from test scenario document.

        Args:
            doc: Scenario document from yggdrasil DB

        Returns:
            Scope dict with kind='scenario' and id from document
        """
        # Use _id or fall back to a generated ID
        doc_id = doc.get("_id", doc.get("scenario_id", "unknown"))
        return {"kind": "scenario", "id": doc_id}

    async def generate_plan_draft(self, payload: dict[str, Any]) -> PlanDraft:
        """
        Generate a PlanDraft from the scenario document.

        Args:
            payload: Event payload containing:
                - doc: The scenario document
                - planning_ctx: PlanningContext from YggdrasilCore

        Returns:
            PlanDraft with plan, auto_run flag, and notes
        """
        doc = payload.get("doc", {})
        ctx: PlanningContext = payload["planning_ctx"]

        # Extract template name (required)
        template_name = doc.get("template")
        if not template_name:
            raise ValueError(
                f"Scenario document missing 'template' field: {doc.get('_id')}"
            )

        if template_name not in TEMPLATES:
            raise ValueError(
                f"Unknown template '{template_name}'. "
                f"Available: {list(TEMPLATES.keys())}"
            )

        # Get optional overrides
        overrides = doc.get("overrides", {})

        # Generate steps from template
        logging.info(
            "Generating plan from template '%s' for scenario '%s'",
            template_name,
            doc.get("_id"),
        )
        template_fn = get_template(template_name)
        steps = template_fn(overrides=overrides)

        # Build Plan
        plan = Plan(
            plan_id=f"test_realm:{ctx.scope['id']}",
            realm=self.realm_id,
            scope=ctx.scope,
            steps=steps,
        )

        # Determine auto_run (default True for test scenarios)
        auto_run = doc.get("auto_run", True)

        # Build preview from template info
        preview = {
            "template": template_name,
            "step_count": len(steps),
            "step_names": [s.name for s in steps],
        }

        notes = f"Test scenario using template '{template_name}'"

        return PlanDraft(
            plan=plan,
            auto_run=auto_run,
            approvals_required=[],  # Test realm never requires approval
            notes=notes,
            preview=preview,
        )

    def __call__(self, payload: dict[str, Any]) -> None:
        """
        Schedule async plan generation.

        Called by YggdrasilCore when a TEST_SCENARIO_CHANGE event is received.
        """
        asyncio.create_task(self.generate_plan_draft(payload))

    def run_now(self, payload: dict[str, Any]) -> PlanDraft:
        """
        Blocking entrypoint for CLI mode.

        Args:
            payload: Event payload with doc and planning_ctx

        Returns:
            PlanDraft for immediate execution
        """
        return asyncio.run(self.generate_plan_draft(payload))
