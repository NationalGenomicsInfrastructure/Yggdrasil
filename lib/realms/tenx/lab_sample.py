import glob
import logging
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from lib.core_utils.config_loader import ConfigLoader
from lib.core_utils.logging_utils import custom_logger


class TenXLabSample:
    """Class representing a TenX lab sample."""

    config: Mapping[str, Any] = ConfigLoader().load_config("10x_config.json")

    def __init__(
        self,
        lab_sample_id: str,
        feature: str,
        sample_data: dict[str, Any],
        project_info: dict[str, Any],
        logger: logging.Logger | None = None,
    ) -> None:
        """
        Initialize a LabSample instance.

        Args:
            lab_sample_id (str): ID of the lab sample (e.g., 'X3_24_025_GE').
            feature (str): Feature associated with the sample (e.g., 'gex', 'vdj').
            sample_data (Dict[str, Any]): Dictionary containing sample-specific data.
            project_info (Dict[str, Any]): Dictionary containing project-specific information.
        """
        self._logger = logger or custom_logger(f"{__name__}.{type(self).__name__}")
        self.lab_sample_id: str = lab_sample_id
        self.feature: str = feature
        self.sample_data: dict[str, Any] = sample_data
        self.project_info: dict[str, Any] = project_info

        self.organism: str = self.project_info.get("organism", "")
        self.lims_id: str = sample_data.get("sample_id", "")
        self.flowcell_ids: list[str] = self._get_all_flowcells()
        self.fastq_dirs: dict[str, list[str]] | None = self.locate_fastq_dirs()
        self.reference_genome: str | None = self.get_reference_genome()

        # self._logger.debug(f"Reference genome for sample {self.lab_sample_id}: {self.reference_genome}")

    def _get_all_flowcells(self) -> list[str]:
        """
        Collect all flowcell IDs associated with the sample.

        Returns:
            List[str]: A list of flowcell IDs for the sample.
        """
        flowcell_ids: list[str] = []
        try:
            library_prep = self.sample_data.get("library_prep", {})
            # if 'library_prep' in self.sample_data:
            for prep_info in library_prep.values():
                flowcell_ids.extend(prep_info.get("sequenced_fc", []))

            if not flowcell_ids:
                self._logger.warning(
                    f"No valid flowcells found for sample {self.lab_sample_id}."
                )
            return flowcell_ids
        except Exception as e:
            self._logger.error(
                f"Error while fetching flowcell IDs for sample '{self.lab_sample_id}': {e}",
                exc_info=True,
            )
            # return None
            return []

    def locate_fastq_dirs(self) -> dict[str, list[str]] | None:
        """Locate the parent directories of the FASTQ files for each flowcell.

        Returns:
            Optional[Dict[str, List[str]]]: A dictionary mapping flowcell IDs to their
                respective parent directories.
        """
        fastq_dirs: dict[str, list[str]] = {}
        for flowcell_id in self.flowcell_ids:
            pattern = Path(
                self.config["seq_root_dir"],
                self.project_info.get("project_id", ""),
                self.lab_sample_id,
                "*",
                flowcell_id,
            )
            matched_dirs = glob.glob(str(pattern))

            if matched_dirs:
                fastq_dirs[flowcell_id] = matched_dirs
            else:
                self._logger.warning(
                    f"No FASTQ directory found for flowcell {flowcell_id} in sample {self.lab_sample_id}"
                )

        if not fastq_dirs:
            self._logger.warning(
                f"No FASTQ directories found for sample {self.lab_sample_id}."
            )
            return None

        return fastq_dirs

    def get_reference_genome(self) -> str | None:
        """Get the reference genome path for the sample based on the feature and organism.

        Returns:
            Optional[str]: The path to the reference genome, or None if not found.
        """
        reference_mapping = self.config.get("reference_mapping", {})
        feature_to_ref_key = self.config.get("feature_to_ref_key", {})

        ref_key = feature_to_ref_key.get(self.feature)

        if not ref_key:
            self._logger.error(
                f"Feature '{self.feature}' is not recognized for reference genome mapping."
            )
            return None

        refs_map = reference_mapping.get(ref_key)
        if not refs_map:
            self._logger.error(
                f"No reference genomes found for reference key '{ref_key}'"
            )
            return None

        ref_genome = refs_map.get(self.organism)
        if not ref_genome:
            self._logger.error(
                f"No reference genome found for feature '{self.feature}' "
                f"(mapped to '{ref_key}') and organism '{self.organism}'"
            )
            return None

        return ref_genome
        return ref_genome
