from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class TenXLabSample:
    """
    Pure representation of a 10x lab-subsample (e.g., GEX, VDJ, HTO split).
    No filesystem, no DB, no Slurm.
    """

    lab_sample_id: str  # e.g. "P35802_1015"
    feature: str  # "gex" | "vdj" | "hto" | "crispr" | ...
    sample_data: dict[str, Any]  # raw sub-sample dict from doc
    project_info: dict[str, Any]  # at least {project_id, project_name, organism?}

    organism: str = ""
    flowcell_ids: list[str] = field(default_factory=list)
    reference_key: str | None = (
        None  # e.g. "gex", "vdj", "atac" (maps to config.reference_mapping[<key>])
    )
    reference_path: str | None = (
        None  # resolved purely from config; not checked on disk
    )

    @staticmethod
    def discover_flowcells(sample_data: dict[str, Any]) -> list[str]:
        """
        Pure extraction of flowcell ids from the doc structure:
        sample_data["library_prep"][<A/B/...>]["sequenced_fc"] -> list[str]
        """
        fcs: list[str] = []
        libprep = sample_data.get("library_prep") or {}
        for prep_info in libprep.values():
            fcs.extend(prep_info.get("sequenced_fc", []) or [])
        # Keep unique flowcells (while preserving order)
        uniq = list(dict.fromkeys(fcs))
        return uniq

    @classmethod
    def from_doc(
        cls,
        lab_sample_id: str,
        feature: str,
        sample_data: dict[str, Any],
        project_info: dict[str, Any],
        *,
        config: Mapping[str, Any],
    ) -> TenXLabSample:
        """
        Build a pure lab-sample. Resolves reference path from config but does no I/O.
        """
        organism = (project_info.get("organism") or "").lower()
        flowcells = cls.discover_flowcells(sample_data)

        # resolve reference path purely from config
        feature_to_ref_key = config.get("feature_to_ref_key") or {}
        reference_mapping = config.get("reference_mapping") or {}
        ref_key = feature_to_ref_key.get(feature)  # e.g. "gex" | "vdj" | "atac"
        ref_path = (
            (reference_mapping.get(ref_key) or {}).get(organism) if ref_key else None
        )

        return cls(
            lab_sample_id=lab_sample_id,
            feature=feature,
            sample_data=sample_data,
            project_info=project_info,
            organism=organism,
            flowcell_ids=flowcells,
            reference_key=ref_key,
            reference_path=ref_path,
        )

    def to_fact(self) -> dict[str, Any]:
        # Strictly planner-facing fields
        return {
            "lab_sample_id": self.lab_sample_id,
            "feature": self.feature,  # "gex", "vdj", "hto", ...
            "organism": self.organism,  # normalized lower-cased
            "flowcell_ids": list(self.flowcell_ids),
            "reference_key": self.reference_key,  # e.g. "gex"
            "has_reference": bool(self.reference_path),
        }
