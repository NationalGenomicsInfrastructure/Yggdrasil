from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from lib.realms.tenx_ext.constants import DirName, FileName, Role
from lib.realms.tenx_ext.steps import (
    build_libraries_csv,
    cellranger_count,
    cellranger_multi,
)
from yggdrasil.flow.planner import StepRuleRegistry, when_eq
from yggdrasil.flow.planner.builder import PlanBuilder

REGISTRY = StepRuleRegistry()


def _features(facts) -> set[str]:
    return set(facts.get("samples", {}).get("tallies", {}).get("features", []))


def features_exact(names: Iterable[str]):
    want = set(names)
    return lambda facts: _features(facts) == want


def method_is(*families: str):
    fam = set(families)
    return lambda facts: facts.get("project", {}).get("method_family") in fam


@REGISTRY.rule(name="libcsv_for_10x", when=when_eq("is_10x", True), order=20)
def rule_libcsv_for_10x(builder: PlanBuilder, facts: dict[str, Any]) -> None:
    """Materialize libraries.csv (provides role 'libraries_csv')."""
    proj = facts["project"]["id"]
    libs_dir = builder.dir_for(DirName.LIBS)
    libs_csv = builder.file_for(DirName.LIBS, FileName.LIBRARIES)

    builder.add_step_fn(
        fn=build_libraries_csv,
        step_id=f"build_libraries_csv__{proj}__v1",
        params={
            "out_dir": libs_dir,
            "features": {"GEX": "FT001"},
        },  # placeholder until wired to model
        provides={Role.LIBRARIES_CSV: str(libs_csv)},
    )


@REGISTRY.rule(name="cellranger_multi_gex", when=when_eq("assay", "GEX"), order=40)
def rule_cellranger_multi_gex(builder: PlanBuilder, facts: dict[str, Any]) -> None:
    """Run cellranger multi, consuming libraries.csv."""
    proj = facts["project"]["id"]
    lib_csv = builder.require(Role.LIBRARIES_CSV)
    out_dir = builder.dir_for(DirName.CR_OUTS)

    builder.add_step_fn(
        fn=cellranger_multi,
        step_id=f"cellranger_multi__{proj}__v1",
        params={"out_dir": str(out_dir), "sample_id": proj, "libraries_csv": lib_csv},
        inputs={Role.LIBRARIES_CSV: lib_csv},
        requires_roles=(Role.LIBRARIES_CSV,),
    )


@REGISTRY.when(
    predicate=lambda f: method_is("3GEX", "5GEX", "FLEX")(f)
    and features_exact({"gex"})(f),
    name="gex_count",
    order=20,
)
def r_gex_count(builder: PlanBuilder, facts: dict[str, Any]):
    proj = facts["project"]["id"]
    libs_dir = builder.dir_for(DirName.LIBS)
    out_dir = builder.dir_for(DirName.CR_OUTS)
    libs_csv = libs_dir / FileName.LIBRARIES

    builder.add_step_fn(
        fn=build_libraries_csv,
        step_id=f"build_libraries_csv__{proj}__v1",
        params={"out_dir": str(libs_dir), "features": {"GEX": "FT002"}},
        provides={Role.LIBRARIES_CSV: str(libs_csv)},
    )

    builder.add_step_fn(
        fn=cellranger_count,
        step_id=f"cellranger_count__{proj}__v1",
        params={
            "out_dir": str(out_dir),
            "project_id": proj,
            "libraries_csv": str(libs_csv),
        },
        inputs={Role.LIBRARIES_CSV: str(libs_csv)},
        requires_roles=(Role.LIBRARIES_CSV,),
    )
