from lib.realms.tenx_ext.project_model import TenxProjectModel
from lib.realms.tenx_ext.rules import REGISTRY
from yggdrasil.flow.planner import PlanDraft, Planner, PlanningContext
from yggdrasil.flow.planner.builder import PlanBuilder


class TenxPlanner(Planner):
    def generate(self, ctx: PlanningContext) -> PlanDraft:
        cfg = ctx.realm_config or {}
        model = TenxProjectModel.from_doc(ctx.source_doc, cfg)
        facts = model.to_facts()

        fs = sorted(
            {f for rs in facts["samples"]["by_run_sample"] for f in rs["features"]}
        )
        actives = REGISTRY.active_for(facts)
        print(
            f"facts summary: method={facts['project']['method']}, method_family={facts['project']['method_family']} features={fs}"
        )
        print(
            f"rules: total={len(REGISTRY._items)} active={len(actives)} -> {[r.name for r in actives]}"
        )

        proj = facts["project"]["id"] or "unknown"
        builder = PlanBuilder(
            plan_id=f"pln_tenx_{proj}_v1",
            realm=ctx.realm,
            scope=ctx.scope,
            base=ctx.scope_dir,  # your builder’s current signature
        )

        # for rule in REGISTRY.active_for(facts):
        #     rule.build(builder, facts)

        REGISTRY.apply(builder=builder, facts=facts)

        plan = builder.to_plan()
        require_approval = bool(model.notes)  # e.g., unresolved organism, etc.

        return PlanDraft(
            plan=plan,
            auto_run=not require_approval,
            approvals_required=["tenx_lead"] if require_approval else [],
            notes="; ".join(model.notes) or f"{len(model.run_samples)} run-samples",
            preview={"steps": [s.name for s in plan.steps]},
        )
