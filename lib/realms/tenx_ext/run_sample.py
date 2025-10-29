from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from lib.realms.tenx_ext.lab_sample import TenXLabSample


@dataclass
class TenXRunSample:
    """
    Pure grouping of lab sub-samples that together form a run-sample
    (typically grouped by original sample id).
    """

    run_sample_id: str
    lab_samples: list[TenXLabSample]
    project_info: dict[str, Any]
    config: Mapping[str, Any]

    # derived
    features: list[str] = field(default_factory=list)  # e.g. ["gex","hto"]
    reference_genomes: dict[str, str] = field(
        default_factory=dict
    )  # {"gex": "/ref/...", ...}
    pipeline_info: dict[str, Any] = field(default_factory=dict)
    issues: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.features = sorted({ls.feature for ls in self.lab_samples})
        self.reference_genomes = self._collect_reference_genomes()
        self.pipeline_info = self._resolve_pipeline_info()

        if not self.reference_genomes:
            self.issues.append("Missing or conflicting reference genomes.")
        if not self.pipeline_info:
            self.issues.append("No pipeline_info resolved from features/method.")

        self.status = "ready" if not self.issues else "blocked"

    @classmethod
    def from_group(
        cls,
        *,
        run_sample_id: str,
        lab_samples: list[TenXLabSample],
        project_info: dict[str, Any],
        config: Mapping[str, Any],
    ) -> TenXRunSample:
        """
        Alternative constructor from grouped lab-samples.
        Not needed but for symmetry with TenXLabSample.from_doc(...).
        """
        return cls(
            run_sample_id=run_sample_id,
            lab_samples=lab_samples,
            project_info=project_info,
            config=config,
        )

    def _collect_reference_genomes(self) -> dict[str, str]:
        """
        Purely collect and validate reference paths coming from lab-samples.
        Ensures there is at most one path per reference_key.
        """
        out: dict[str, str] = {}
        for ls in self.lab_samples:
            if not ls.reference_key:
                self.issues.append(
                    f"[{ls.lab_sample_id}] Unknown reference key for feature '{ls.feature}'."
                )
                continue
            if not ls.reference_path:
                self.issues.append(
                    f"[{ls.lab_sample_id}] No reference path for {ls.reference_key} / organism '{ls.organism}'."
                )
                continue
            existing = out.get(ls.reference_key)
            if existing and existing != ls.reference_path:
                self.issues.append(
                    f"Conflicting reference for key '{ls.reference_key}': "
                    f"'{existing}' vs '{ls.reference_path}'"
                )
                return {}  # conflict => caller sees empty map -> blocked
            out[ls.reference_key] = ls.reference_path
        return out

    def _resolve_pipeline_info(self) -> dict[str, Any]:
        """
        Pure resolution of pipeline info from config + features + method.
        Avoids I/O; assumes config already loaded.
        """
        # TODO: Existing TenXUtils.get_pipeline_info(...) is OK *if* pure (CHECK)
        # If it does file I/O, move the data it needs into config and reimplement here.
        library_prep_method = self.project_info.get("library_prep_method", "")
        feature_tuple = tuple(sorted(self.features))  # cache key if needed

        dt = self.config.get("decision_table") or {}  # inject via config
        # TODO: Naive selection logic; adapt to decision table shape (CHECK)
        # dt could be { "<method>": { "<feature_tuple>": {pipeline_info...} } }
        entry = (dt.get(library_prep_method) or {}).get(",".join(feature_tuple)) or {}
        return entry

    # ---------- Pure data for steps (no side-effects) ----------

    def libraries_rows(self, fastq_index: dict[str, list[str]]) -> list[dict[str, str]]:
        """
        Build rows for libraries.csv based on *discovered* fastq directories.
        - fastq_index: { lab_sample_id -> [fastq_dir1, fastq_dir2, ...] }
        Returns rows: {fastqs, sample, library_type}
        """
        rows: list[dict[str, str]] = []
        feature_to_library_type = self.config.get("feature_to_library_type") or {}
        for ls in self.lab_samples:
            lib_type = feature_to_library_type.get(ls.feature)
            if not lib_type:
                self.issues.append(
                    f"No library_type for feature '{ls.feature}' in config."
                )
                continue
            dirs = fastq_index.get(ls.lab_sample_id) or []
            for d in dirs:
                rows.append(
                    {
                        "fastqs": d,
                        "sample": ls.lab_sample_id,
                        "library_type": lib_type,
                    }
                )
        return rows

    def multi_csv_structure(self) -> dict[str, Any]:
        """
        Return a structured description used by a step to render 'multi.csv'.
        """
        mi = self.pipeline_info or {}
        sections = mi.get("multi_csv_sections", [])
        args = mi.get("multi_csv_arguments", {})
        return {
            "sections": sections,
            "section_args": args,
            "reference_by_section": self.reference_genomes,  # e.g. {"gex": "/ref/..."}
        }

    def command_spec(self) -> dict[str, Any]:
        """
        Pure command pieces a step can convert into a shell command (no strings joined here).
        """
        if not self.pipeline_info:
            return {}
        return {
            "pipeline_exec": self.pipeline_info.get("pipeline_exec"),
            "pipeline": self.pipeline_info.get("pipeline"),
            "required_args": self.pipeline_info.get("required_arguments", []),
            "additional_args": self.pipeline_info.get("command_arguments", []),
            # Step will add --output-dir from its workdir
        }

    def to_fact(self) -> dict[str, Any]:
        # Derive a compact, reference summary from lab-samples
        refs: dict[str, dict[str, Any]] = {}
        for ls in self.lab_samples:
            if ls.reference_key:  # "gex" | "vdj" | "atac" ...
                # Organism per run is usually uniform; keep the lab’s organism to be safe
                refs.setdefault(
                    ls.reference_key, {"present": True, "organism": ls.organism}
                )
        return {
            "run_sample_id": self.run_sample_id,
            "features": list(self.features),  # e.g. ["gex","hto"]
            "references": refs,  # e.g. {"gex": {"present": True, "organism": "human"}, ...}
            "pipeline_key": self.pipeline_info.get("pipeline"),
            "issues": list(self.issues),
            "lab_samples": [ls.to_fact() for ls in self.lab_samples],
        }
