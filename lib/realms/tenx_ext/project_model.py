from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from lib.realms.tenx_ext.lab_sample import TenXLabSample
from lib.realms.tenx_ext.run_sample import TenXRunSample

_ASSAY_DIGIT_MAP = {
    "1": "gex",
    "2": "gex",
    "3": "vdj-b",
    "4": "vdj-t",
    "5": "hto",
    "6": "crispr",
}


@dataclass
class TenxProjectModel:
    doc: Mapping[str, Any]
    config: Mapping[str, Any]
    project_id: str = ""
    project_name: str = ""

    # --- declared (UDFs) ---
    method: str = ""
    hashing: str = "None"
    cite: str = "None"
    vdj: str = "None"
    feature: str = "None"
    organism: str | None = None
    notes: list[str] = field(default_factory=list)

    # --- samples (pure) ---
    lab_samples: dict[str, tuple["TenXLabSample", str]] = field(default_factory=dict)
    run_samples: list["TenXRunSample"] = field(default_factory=list)

    @classmethod
    def from_doc(
        cls, doc: dict[str, Any], config: Mapping[str, Any]
    ) -> "TenxProjectModel":
        m = cls(doc=doc, config=config)
        m._parse_project_identity()
        m._parse_declared_udfs()
        m._parse_organism()
        m._build_samples()
        return m

    def _parse_project_identity(self) -> None:
        self.project_id = self.doc.get("project_id", "Unknown_Project")
        self.project_name = self.doc.get("project_name", "")

    def _parse_declared_udfs(self) -> None:
        details = self.doc.get("details", {}) or {}
        self.method = details.get("library_construction_method", "") or ""
        self.hashing = (
            details.get("library_prep_option_single_cell_(hashing)", "None") or "None"
        )
        self.cite = (
            details.get("library_prep_option_single_cell_(cite)", "None") or "None"
        )
        self.vdj = (
            details.get("library_prep_option_single_cell_(vdj)", "None") or "None"
        )
        self.feature = (
            details.get("library_prep_option_single_cell_(feature)", "None") or "None"
        )

    def _parse_organism(self) -> None:
        # Pure rewrite of original determine_organism
        ref = (self.doc.get("reference_genome") or "").strip()
        if ref and ref.lower() != "other (-, -)":
            org = ref.split("(")[0].strip().lower()
        else:
            org = (self.doc.get("details", {}).get("organism") or "").strip().lower()
        gex_orgs = set(self.config.get("reference_mapping", {}).get("gex", {}).keys())
        if org in gex_orgs:
            self.organism = org
        else:
            self.organism = None
            self.notes.append(f"Unsupported or missing organism: '{org or 'unknown'}'.")

    def _build_samples(self) -> None:
        # reuse existing functions (below we rely on refactoring lab/run classes to be pure)
        sample_data = self.doc.get("samples", {}) or {}
        sample_data = self.filter_aborted_samples(sample_data)

        project_info = self._project_info()
        labs = self.create_lab_samples(sample_data, project_info=project_info)
        grouped = self.group_lab_samples(labs)
        self.lab_samples = labs
        self.run_samples = self.create_run_samples(
            grouped, project_info=project_info, config=self.config
        )

    def _project_info(self) -> dict[str, Any]:
        d = self.doc
        details = d.get("details", {}) or {}
        return {
            "project_name": self.project_name,
            "project_id": self.project_id,
            "library_prep_method": details.get("library_construction_method", ""),
            "organism": self.organism or "",
            # add whatever fields the existing lab/run sample constructors expect
        }

    def filter_aborted_samples(self, sample_data: dict[str, Any]) -> dict[str, Any]:
        """Filter out aborted samples from the sample data.

        Args:
            sample_data (Dict[str, Any]): The original sample data.

        Returns:
            Dict[str, Any]: Sample data excluding aborted samples.
        """
        return {
            sample_id: sample_info
            for sample_id, sample_info in sample_data.items()
            if str(sample_info.get("details", {}).get("status_(manual)", ""))
            .strip()
            .lower()
            != "aborted"
        }

    def _classify_new_format(self, sample_id: str) -> tuple[str, str]:
        """
        Return (feature, original_sample_id) using the NEW format:
        - 3-digit suffix (e.g. P12345_105): base GEX --> feature="gex", original=sample_id
        - 4-digit suffix (e.g. P12345_1051): last digit encodes sub-assay
            --> feature = map(last), original = sample_id with the last digit removed
        """
        if "_" not in sample_id:
            # Fallback: treat as unknown base
            return "unknown", sample_id

        prefix, suffix = sample_id.rsplit("_", 1)
        if not suffix.isdigit():
            return "unknown", sample_id

        # Base 3-digit case --> GEX
        if len(suffix) == 3:
            return "gex", sample_id

        # Sub-assay case --> use last digit
        last = suffix[-1]
        feature = _ASSAY_DIGIT_MAP.get(last, "unknown")
        original = f"{prefix}_{suffix[:-1]}" if len(suffix) > 1 else sample_id
        return feature, original

    def _map_method_family(self, s: str) -> str:
        t = (s or "").upper()
        if "MULTIOME" in t:
            return "MULTIOME"
        if "ATAC" in t:
            return "ATAC"
        if "FLEX" in t:
            return "FLEX"
        if "5GEX" in t:
            return "5GEX"
        if "3GEX" in t:
            return "3GEX"
        return "UNKNOWN"

    def create_lab_samples(
        self,
        sample_data: dict[str, Any],
        *,
        project_info: dict[str, Any],
    ) -> dict[str, tuple[TenXLabSample, str]]:
        """
        Build {lab_sample_id -> (TenXLabSample, original_sample_id)} for *new format* only.
        Uses TenXLabSample.from_doc(...) exactly as defined in your class.
        """
        labs: dict[str, tuple[TenXLabSample, str]] = {}

        for sample_id, sinfo in (sample_data or {}).items():
            feature, original_id = self._classify_new_format(sample_id)

            lab = TenXLabSample.from_doc(
                lab_sample_id=sample_id,
                feature=feature,
                sample_data=sinfo,
                project_info=project_info,
                config=self.config,  # resolves reference_key/path; no I/O
            )
            labs[sample_id] = (lab, original_id)

        return labs

    def create_run_samples(
        self,
        grouped_lab_samples: dict[str, list[TenXLabSample]],
        *,
        project_info: dict[str, Any],
        config: Mapping[str, Any],
    ) -> list[TenXRunSample]:
        """
        Create TenXRunSample objects (pure). __post_init__ will:
        - derive features (sorted unique),
        - collect/validate reference_genomes,
        - resolve pipeline_info,
        - populate issues + status.
        """
        runs: list[TenXRunSample] = []
        for original_id, labs in grouped_lab_samples.items():
            rs = TenXRunSample(
                run_sample_id=original_id,
                lab_samples=labs,
                project_info=project_info,
                config=config,
            )
            runs.append(rs)
        return runs

    def group_lab_samples(
        self, lab_samples: dict[str, tuple[TenXLabSample, str]]
    ) -> dict[str, list[TenXLabSample]]:
        """Group lab samples by original sample ID.

        Args:
            lab_samples (Dict[str, Tuple[TenXLabSample, str]]): The lab samples.

        Returns:
            Dict[str, List[TenXLabSample]]: A dictionary grouping lab samples by original sample ID.
        """
        groups: dict[str, list[TenXLabSample]] = {}
        for lab_sample, original_sample_id in lab_samples.values():
            groups.setdefault(original_sample_id, []).append(lab_sample)
        return groups

    def to_facts(self) -> dict[str, Any]:
        """
        Compact, planner-facing shape. No file paths, no DB IDs, no Slurm.
        """

        self.method_family = self._map_method_family(self.method)

        return {
            "project": {
                "id": self.project_id,
                "name": self.project_name,
                "organism": self.organism,  # normalized as you already do
                "method": self.method,
                "method_family": self.method_family,  # normalized family (3GEX/5GEX/FLEX/…)
            },
            "samples": {
                "by_run_sample": [rs.to_fact() for rs in self.run_samples],
                # optional summaries for quick guards:
                "tallies": {
                    "n_run_samples": len(self.run_samples),
                    "features": sorted(
                        {f for rs in self.run_samples for f in rs.features}
                    ),
                },
            },
        }

    # def original_id_for(self, sample_id: str) -> str | None:
    #     pass

    # def feature_for(self, sample_id: str, feature_map: dict[str, str]) -> str:
    #     pass
