from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from yggdrasil.flow.step import StepContext, StepResult, step

# ---------- optional “intake” steps (safe stubs) ----------


@step("collect_metadata")
def collect_metadata(ctx: StepContext, doc: dict[str, Any], out_dir: str) -> StepResult:
    outdir = Path(out_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    meta_path = outdir / "metadata.json"

    # keep it tiny; just persist a normalized view for the next step
    payload = {
        "project_id": doc.get("project_id"),
        "tenx": doc.get("tenx") or {},
    }
    meta_path.write_text(json.dumps(payload, indent=2))
    ctx.add_artifact("intake_metadata", str(meta_path))
    return StepResult(artifacts=[], metrics={"saved": True})


@step("detect_assay", input_keys=("in_dir",))  # hash intake dir contents if provided
def detect_assay(ctx: StepContext, in_dir: str, out_dir: str) -> StepResult:
    indir = Path(in_dir)
    outdir = Path(out_dir)
    outdir.mkdir(parents=True, exist_ok=True)

    meta_path = indir / "metadata.json"
    assay = "GEX"
    if meta_path.exists():
        d = json.loads(meta_path.read_text())
        assay = (d.get("tenx") or {}).get("assay", assay)

    assay_path = outdir / "assay.json"
    assay_path.write_text(json.dumps({"assay": assay}, indent=2))
    ctx.add_artifact("intake_assay", str(assay_path))
    return StepResult(metrics={"assay": assay})


# ---------- execution steps (safe stubs) ----------


@step("build_libraries_csv")
def build_libraries_csv(
    ctx: StepContext, features: dict[str, str], out_dir: str
) -> StepResult:
    outdir = Path(out_dir)

    # GOOD TO HAVE: Enhanced safety for not writing outside workdir
    if not outdir.is_absolute():
        outdir = (ctx.workdir / outdir).resolve()

    outdir.mkdir(parents=True, exist_ok=True)
    csv_path = outdir / "libraries.csv"

    with csv_path.open("w") as fh:
        fh.write("feature_type,feature_id\n")
        for ft, fid in (features or {}).items():
            fh.write(f"{ft},{fid}\n")

    art = ctx.add_artifact("libraries_csv", str(csv_path))
    return StepResult(artifacts=[art], metrics={"rows": len(features or {})})


@step("cellranger_multi", input_keys=("libraries_csv",))
def cellranger_multi(
    ctx: StepContext,
    libraries_csv: (
        str | None
    ),  # <-- we will also pass this in params so the step can use it
    out_dir: str,
    sample_id: str,
) -> StepResult:
    """
    Stub: don't actually run CR. Just show wiring works and produce a “submit script”.
    """
    if not libraries_csv:
        raise ValueError("cellranger_multi requires 'libraries_csv' in params")

    outdir = Path(out_dir)

    # GOOD TO HAVE: Enhanced safety for not writing outside workdir
    if not outdir.is_absolute():
        outdir = (ctx.workdir / outdir).resolve()

    outdir.mkdir(parents=True, exist_ok=True)

    # Emit a submit script artifact (glass-box) so a human could run it if needed.
    submit = outdir / "submit_cellranger_multi.sh"
    submit.write_text(
        f"""#!/usr/bin/env bash
set -euo pipefail
# STUB submit for {sample_id}
# would run: cellranger multi --config {libraries_csv} --id {sample_id} --output-dir {out_dir}
echo "Simulating cellranger multi for {sample_id} using {libraries_csv}" > "{outdir}/_SIMULATED_OUT"
"""
    )
    submit.chmod(0o755)

    ctx.add_artifact("submit_script", str(submit))
    # pretend we produced some outs
    simulated = outdir / "_SIMULATED_OUT"
    if simulated.exists():
        ctx.add_artifact("cr_outs", str(outdir))

    return StepResult(metrics={"simulated": True})


@step("cellranger_count", input_keys=("libraries_csv",))
def cellranger_count(
    ctx: StepContext,
    libraries_csv: (
        str | None
    ),  # <-- we will also pass this in params so the step can use it
    out_dir: str,
    project_id: str,
) -> StepResult:
    """
    Stub: don't actually run CR. Just show wiring works and produce a “submit script”.
    """
    if not libraries_csv:
        raise ValueError("cellranger_count requires 'libraries_csv' in params")

    outdir = Path(out_dir)

    # GOOD TO HAVE: Enhanced safety for not writing outside workdir
    if not outdir.is_absolute():
        outdir = (ctx.workdir / outdir).resolve()

    outdir.mkdir(parents=True, exist_ok=True)

    # Emit a submit script artifact (glass-box) so a human could run it if needed.
    submit = outdir / "submit_cellranger_count.sh"
    submit.write_text(
        f"""#!/usr/bin/env bash
set -euo pipefail
# STUB submit for {project_id}
# would run: cellranger count --libraries {libraries_csv} --id {project_id} --output-dir {out_dir}
echo "Simulating cellranger count for {project_id} using {libraries_csv}" > "{outdir}/_SIMULATED_OUT"
"""
    )
    submit.chmod(0o755)
    ctx.add_artifact("submit_script", str(submit))
    # pretend we produced some outs
    simulated = outdir / "_SIMULATED_OUT"
    if simulated.exists():
        ctx.add_artifact("cr_outs", str(outdir))
    return StepResult(metrics={"simulated": True})
