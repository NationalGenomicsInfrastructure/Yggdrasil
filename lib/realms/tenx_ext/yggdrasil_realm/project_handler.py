from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any

from lib.core_utils.config_loader import ConfigLoader
from lib.core_utils.event_types import EventType
from lib.handlers.base_handler import BaseHandler
from lib.ops.sinks.couch import OpsWriter
from lib.realms.tenx_ext.planner import TenxPlanner
from yggdrasil.flow.engine import Engine
from yggdrasil.flow.events.emitter import FileSpoolEmitter
from yggdrasil.flow.planner import PlanDraft, PlanningContext


def _looks_like_10x(doc: dict[str, Any]) -> bool:
    details = doc.get("details") or {}
    method = (details.get("library_construction_method") or "").lower()
    return "10x" in method or "chromium" in method


class TenxProjectHandler(BaseHandler):
    """
    External realm handler for 10x projects.
    Triggered on PROJECT_CHANGE.
    """

    event_type = EventType.PROJECT_CHANGE

    def __init__(self) -> None:
        super().__init__()
        self.work_root = Path(os.getenv("YGG_WORK_ROOT", "/tmp/ygg_work"))
        self.consumer_spool = Path(os.getenv("YGG_EVENT_SPOOL", "/tmp/ygg_events"))
        self.ops_db = os.getenv("OPS_DB", "yggdrasil_ops")
        self.cfg = ConfigLoader().load_config("10x_config.json")

    def __call__(self, payload: dict[str, Any]) -> None:
        asyncio.create_task(self.handle_task(payload))

    async def handle_task(self, payload: dict[str, Any]) -> None:
        # Accept both "doc" and "document" to be tolerant
        doc = payload.get("doc") or payload.get("document")
        if not isinstance(doc, dict):
            return

        if not _looks_like_10x(doc):
            return  # not our realm's concern

        project_id = doc.get("project_id")
        if not project_id:
            # TODO: log somewhere central
            # self._logger.error("No project_id in doc; refusing to plan.")
            return
        scope = {"kind": "project", "id": project_id}

        ctx = PlanningContext(
            realm="tenx",
            scope=scope,
            scope_dir=self.work_root / "tenx" / project_id,
            emitter=FileSpoolEmitter(str(self.consumer_spool)),
            source_doc=doc,
            reason=payload.get("reason", "project.updated"),
            realm_config=self.cfg,  # handy place to pass cfg into planner/model
        )

        # ---- generate draft (planner is the ONE place that builds steps) ----
        planner = TenxPlanner()
        draft: PlanDraft = planner.generate(ctx)

        # # -------- facts > rules > plan --------
        # model = TenxProjectModel.from_doc(doc, self.cfg)
        # facts = model.to_facts()

        # builder = PlanBuilder(
        #     plan_id=f"pln_tenx_{project_id}_v1",
        #     realm=ctx.realm,
        #     scope=ctx.scope,
        #     base_dir=ctx.work_root / ctx.realm / project_id,
        # )
        # REGISTRY.apply(builder=builder, facts=facts)
        # plan = builder.to_plan()

        # # Draft + policy
        # require_approval = bool(model.notes)  # e.g. unknown organism etc.
        # draft = PlanDraft(
        #     plan=plan,
        #     auto_run=not require_approval,
        #     approvals_required=["tenx_lead"] if require_approval else [],
        #     notes="; ".join(model.notes) or f"{len(model.run_samples)} run-samples",
        #     preview={"run_samples": [rs.run_sample_id for rs in model.run_samples]},
        # )

        print("The following 10x plan draft was generated:", draft)

        # -------- persist draft to ops --------
        ops = OpsWriter(db_name=self.ops_db)
        ops.upsert_plan_draft(draft)

        # -------- execute if allowed (offload so we don’t block loop) --------
        if draft.auto_run and draft.plan.steps:
            await asyncio.to_thread(
                Engine(work_root=ctx.scope_dir, emitter=ctx.emitter).run, draft.plan
            )
