# lib/realms/tenx/planner.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from yggdrasil.flow.model import Plan, StepSpec
from yggdrasil.flow.planner import PlanDraft, Planner, PlanningContext

# Dotted paths to realm steps (wrap old tenx functions with @step later)
INTAKE_COLLECT = "lib.realms.tenx.steps.collect_metadata"  # TODO: implement/wrap
INTAKE_DETECT = "lib.realms.tenx.steps.detect_assay"  # TODO: implement/wrap
STEP_LIBCSV = "lib.realms.tenx.steps.build_libraries_csv"  # TODO: implement/wrap
STEP_MULTI = "lib.realms.tenx.steps.cellranger_multi"  # TODO: implement/wrap


@dataclass
class TenxPlanner(Planner):
    """
    Super-minimal planning:
      - (optional) intake steps that normalize doc → intake/{metadata,assay}.json
      - decide small plan
      - return a PlanDraft (auto_run can be flipped by doc)
    """

    def generate(self, ctx: PlanningContext) -> PlanDraft:
        realm, scope = ctx.realm, ctx.scope
        proj_id = scope["id"]
        base = Path(ctx.scope_dir) / realm / proj_id

        # --- OPTIONAL: run intake (tracked) so we have stable artifacts to wire ---
        # Comment out until you have the @step wrappers ready.
        # eng = Engine(work_root=ctx.base_dir, emitter=ctx.emitter or FileSpoolEmitter())
        # intake = Plan(
        #     plan_id=f"intake_{proj_id}",
        #     realm=realm,
        #     scope=scope,
        #     steps=[
        #         StepSpec(
        #             step_id=f"collect_metadata__{proj_id}__v1",
        #             name="collect_metadata",
        #             fn_ref=INTAKE_COLLECT,
        #             params={"doc": ctx.source_doc, "out_dir": str(base / "intake")},
        #         ),
        #         StepSpec(
        #             step_id=f"detect_assay__{proj_id}__v1",
        #             name="detect_assay",
        #             fn_ref=INTAKE_DETECT,
        #             deps=[f"collect_metadata__{proj_id}__v1"],
        #             params={"in_dir": str(base / "intake"), "out_dir": str(base / "intake")},
        #         ),
        #     ],
        # )
        # eng.run(intake)

        # --- Tiny decision for now; later read intake/assay.json ---
        tenx = ctx.source_doc.get("tenx") or {}
        assay = tenx.get("assay", "GEX")  # e.g., "GEX", "GEX_CITE", "VDJ", etc.

        # --- Materialize execution plan ---
        plid = f"pln_tenx_{proj_id}_v1"

        lib_dir = base / "libcsv"
        lib_csv = lib_dir / "library.csv"

        steps: list[StepSpec] = [
            StepSpec(
                step_id=f"build_library_csv__{proj_id}__v1",
                name="build_library_csv",
                fn_ref=STEP_LIBCSV,
                params={
                    "out_dir": str(lib_dir),
                    "features": ctx.source_doc.get("features", {}),
                },
            )
        ]
        steps.append(
            StepSpec(
                step_id=f"cellranger_multi__{proj_id}__v1",
                name="cellranger_multi",
                fn_ref=STEP_MULTI,
                deps=[f"build_library_csv__{proj_id}__v1"],
                params={
                    "out_dir": str(base / "cr_outs"),
                    "sample_id": proj_id,
                    "library_csv": str(lib_csv),  # <-- add this
                },
                inputs={"library_csv": str(lib_csv)},
            )
        )

        plan = Plan(plan_id=plid, realm=realm, scope=scope, steps=steps)

        # Approval gate (document-driven; change in ops db will re-trigger handler)
        require_approval = bool(tenx.get("require_approval", False))

        return PlanDraft(
            plan=plan,
            auto_run=not require_approval,
            approvals_required=["tenx_lead"] if require_approval else [],
            notes=f"Assay={assay}; steps={len(steps)}",
            preview={"assay": assay, "steps": [s.name for s in steps]},
        )
