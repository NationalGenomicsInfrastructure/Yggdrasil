from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

from lib.ops.sinks.couch import OpsWriter
from yggdrasil.flow.engine import Engine
from yggdrasil.flow.events.emitter import FileSpoolEmitter
from yggdrasil.flow.model import Plan, StepSpec
from yggdrasil.flow.planner import PlanningContext

from .planner_old import TenxPlanner  # realm-local


def handle_project_change(payload: dict[str, Any]) -> None:
    doc = payload["doc"]  # full project doc
    if (
        doc.get("details").get("library_construction_method") or ""
    ).lower() != "10x chromium":  # realm decides
        logging.info("Not a 10x project, skipping tenx planning")
        return

    scope = {"kind": "project", "id": doc["project_id"]}
    ctx = PlanningContext(
        realm="tenx",
        scope=scope,
        scope_dir=Path(os.environ.get("YGG_WORK_ROOT", "/tmp/ygg_work")),
        emitter=FileSpoolEmitter(),
        source_doc=doc,
        reason=payload.get("reason", "project.updated"),
    )

    draft = TenxPlanner().generate(ctx)

    # Persist draft for visibility/approval
    ops = OpsWriter(db_name=os.environ.get("OPS_DB", "yggdrasil_ops"))
    ops.upsert_plan_draft(draft)

    # Auto-run if allowed
    if draft.auto_run:
        Engine(work_root=ctx.scope_dir, emitter=ctx.emitter).run(draft.plan)


def handle_plan_draft_change(payload: dict[str, Any]) -> None:
    draft_doc = payload["doc"]
    if draft_doc.get("type") != "plan_draft" or draft_doc.get("realm") != "tenx":
        return
    if not draft_doc.get("approved"):
        return

    # Run exactly the embedded plan (auditable); or re-plan if policy demands
    plan_json = draft_doc["plan"]  # already a flow.Plan JSON shape
    plan = Plan(
        plan_id=plan_json["plan_id"],
        realm=plan_json["realm"],
        scope=plan_json["scope"],
        steps=[StepSpec(**s) for s in plan_json["steps"]],
    )
    Engine(work_root=Path(os.environ["YGG_WORK_ROOT"]), emitter=FileSpoolEmitter()).run(
        plan
    )
