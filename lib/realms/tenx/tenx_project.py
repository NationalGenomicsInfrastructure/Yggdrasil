import asyncio
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

from lib.base.abstract_project import AbstractProject
from lib.core_utils.config_loader import ConfigLoader
from lib.core_utils.logging_utils import custom_logger
from lib.realms.tenx.lab_sample import TenXLabSample
from lib.realms.tenx.run_sample import TenXRunSample

logging = custom_logger(__name__.split(".")[-1])


class TenXProject(AbstractProject):
    """
    Class representing a TenX project.
    """

    config: Mapping[str, Any] = ConfigLoader().load_config("10x_config.json")

    def __init__(self, doc: Dict[str, Any], yggdrasil_db_manager: Any) -> None:
        """
        Initialize a TenXProject instance.

        Args:
            doc (Dict[str, Any]): Document containing project metadata.
            yggdrasil_db_manager (Any): Yggdrasil database manager instance.
        """
        super().__init__(doc, yggdrasil_db_manager)
        self.doc: Dict[str, Any] = doc
        self.ydm: Any = yggdrasil_db_manager

        # TODO: Might need to check required fields for each different method, if they differ
        self.proceed: bool = self._check_required_fields()

        if self.proceed:
            # Extract metadata from project document
            self.project_info: Dict[str, Any] = self._extract_project_info()

            if not self.determine_organism():
                # TODO: Send this message as a notification (e.g. on Slack)
                logging.error(
                    "Project organism could not be determined. Handle manually!"
                )
                self.proceed = False
                return

            self.project_dir: Optional[Path] = self.ensure_project_directory()
            self.samples: List[TenXRunSample] = []
            self.case_type: str = self.project_info.get("case_type", "unknown")
            logging.info(f"Case type: {self.case_type}")

            self.status: str = "initialized"

    def _extract_project_info(self) -> Dict[str, Any]:
        """
        Extracts relevant project information from the document.

        Returns:
            Dict[str, Any]: A dictionary containing extracted project info.
        """
        try:
            details = self.doc.get("details", {})
            project_info: Dict[str, Any] = {
                "project_name": self.doc.get("project_name", "").replace(".", "__"),
                "project_id": self.doc.get("project_id", "Unknown_Project"),
                "customer_reference": self.doc.get("customer_project_reference", ""),
                "library_prep_method": details.get("library_construction_method", ""),
                "library_prep_option": details.get("library_prep_option", ""),
                "reference_genome": self.doc.get("reference_genome", ""),
                "organism": details.get("organism", ""),
                "contact": self.doc.get("contact", ""),
            }

            # Determine case type based on library_prep_option
            if project_info["library_prep_option"]:
                # Old case, because library_prep_option is populated
                project_info["case_type"] = "old_format"
            else:
                # New case, because library_prep_option is empty or missing
                project_info["case_type"] = "new_format"

                # Add new UDFs for the new case
                # TODO: Examine this is still needed. Probably not anymore!
                project_info.update(
                    {
                        "hashing": details.get(
                            "library_prep_option_single_cell_hashing", "None"
                        ),
                        "cite": details.get(
                            "library_prep_option_single_cell_cite", "None"
                        ),
                        "vdj": details.get(
                            "library_prep_option_single_cell_vdj", "None"
                        ),
                        "feature": details.get(
                            "library_prep_option_single_cell_feature", "None"
                        ),
                    }
                )

            return project_info
        except Exception as e:
            logging.error(f"Error occurred while extracting project information: {e}")
            return {}

    def determine_organism(self) -> bool:
        """Determine the organism for the project.

        Tries to parse the 'reference_genome' field first, then falls back to the 'organism' field.
        Updates 'project_info' with the organism.

        Returns:
            bool: True if organism is determined successfully, False otherwise.
        """
        # Try to extract organism from 'reference_genome' field
        reference_genome = self.project_info.get("reference_genome", "").strip()
        if reference_genome and reference_genome.lower() != "other (-, -)":
            # Split at '(' and take the first part
            organism = reference_genome.split("(")[0].strip().lower()
            # Validate organism
            if self.is_supported_organism(organism):
                self.project_info["organism"] = organism
                return True
        # If 'reference_genome' is not usable, try 'organism' field
        organism = self.project_info.get("organism", "").strip().lower()
        if organism and self.is_supported_organism(organism):
            self.project_info["organism"] = organism
            return True
        # If neither field is usable, log an error
        logging.error(
            f"Organism '{organism}' not specified or unsupported for project "
            f"'{self.project_info.get('project_name', 'Unknown_Project')}'."
        )
        self.status = "failed"
        return False

    def is_supported_organism(self, organism: str) -> bool:
        """
        Checks if the given organism is supported.

        Currently based only on the 'gex' references in the configuration.

        Args:
            organism (str): The organism to check.

        Returns:
            bool: True if organism is supported, False otherwise.
        """
        reference_mapping = self.config.get("reference_mapping", {})
        gex_organisms = reference_mapping.get("gex", {}).keys()
        return organism in gex_organisms

    def _check_required_fields(self) -> bool:
        """Check if the document contains all required fields.

        Returns:
            bool: True if all required fields are present, False otherwise.
        """
        required_fields = self.config.get("required_fields", [])

        missing_keys = [
            field for field in required_fields if not self._is_field(field, self.doc)
        ]

        if missing_keys:
            logging.warning(f"Missing required project information: {missing_keys}.")
            return False

        return True

    def _is_field(self, field_path: str, data: Dict[str, Any]) -> bool:
        """Checks if the document contains all required fields.

        Args:
            field_path (str): The path to the required field.
            data (Dict[str, Any]): The dictionary to check.

        Returns:
            bool: True if the field exists, False otherwise.
        """
        keys = field_path.split(".")
        for key in keys:
            if isinstance(data, dict) and key in data:
                data = data[key]
            else:
                return False
        return True

    def ensure_project_directory(self) -> Optional[Path]:
        """Ensures that the project directory exists. Creates it if necessary.

        Returns:
            Optional[Path]: The Path object of the project directory, or None if creation fails.
        """
        try:
            project_base_dir = Path(self.config["10x_dir"])
            project_dir = project_base_dir / self.project_info["project_name"]
            project_dir.mkdir(parents=True, exist_ok=True)
            return project_dir
        except Exception as e:
            logging.error(f"Failed to create project directory: {e}")
            return None

    def get_default_feature(self, library_prep_id: str) -> str:
        """Get a default feature based on the library preparation ID.

        Args:
            library_prep_id (str): The library preparation ID.

        Returns:
            str: The default feature ('gex', 'atac', or 'unknown').
        """
        patterns = ["3' GEX", "5' GEX", "3GEX", "5GEX", "VDJ"]
        if any(pattern in library_prep_id for pattern in patterns):
            return "gex"
        elif "ATAC" in library_prep_id:
            return "atac"
        else:
            return "unknown"

    def identify_feature_old_case(self, sample_info: Dict[str, Any]) -> Tuple[str, str]:
        """Identify feature and original sample ID for old format cases.

        Args:
            sample_info (Dict[str, Any]): The sample information.

        Returns:
            Tuple[str, str]: A tuple containing the feature and original sample ID.
        """
        feature_map = self.config["feature_map"]["old_format"]
        customer_name = sample_info.get("customer_name", "")
        for assay_suffix, feature in feature_map.items():
            suffix_with_underscore = f"_{assay_suffix}"
            if suffix_with_underscore in customer_name:
                original_sample_id = customer_name.split(suffix_with_underscore)[0]
                return feature, original_sample_id
        # Assign default values if not found
        default_original_sample_id = customer_name or "unknown_sample_id"
        return "unknown", default_original_sample_id

    def identify_feature_new_case(self, sample_id: str) -> Tuple[str, str]:
        """Identify feature and original sample ID for new format cases.

        Args:
            sample_id (str): The sample ID.

        Returns:
            Tuple[str, str]: A tuple containing the feature and original sample ID.
        """
        feature_map = self.config["feature_map"]["new_format"]
        assay_digit = sample_id[-1]
        feature = feature_map.get(assay_digit, "unknown")
        default_original_sample_id = sample_id[:-1] or "unknown_sample_id"
        return feature, default_original_sample_id

    def filter_aborted_samples(self, sample_data: Dict[str, Any]) -> Dict[str, Any]:
        """Filter out aborted samples from the sample data.

        Args:
            sample_data (Dict[str, Any]): The original sample data.

        Returns:
            Dict[str, Any]: Sample data excluding aborted samples.
        """
        return {
            sample_id: sample_info
            for sample_id, sample_info in sample_data.items()
            if sample_info.get("details", {}).get("status_(manual)", "").lower()
            != "aborted"
        }

    def identify_feature_and_original_id_old(
        self, sample_id: str, sample_info: Dict[str, Any]
    ) -> Tuple[str, str]:
        """Identify feature and original sample ID for old format samples.

        Args:
            sample_id (str): The sample ID.
            sample_info (Dict[str, Any]): The sample information.

        Returns:
            Tuple[str, str]: A tuple containing the feature and original sample ID.
        """
        feature, original_sample_id = self.identify_feature_old_case(sample_info)
        if feature != "unknown":
            return feature, original_sample_id
        else:
            # Handle original samples without features
            library_prep_option = self.project_info.get("library_prep_option", "")
            feature = self.get_default_feature(library_prep_option)
            original_sample_id = sample_id
            return feature, original_sample_id

    def identify_feature_and_original_id_new(self, sample_id: str) -> Tuple[str, str]:
        """Identify feature and original sample ID for new format samples.

        Args:
            sample_id (str): The sample ID.

        Returns:
            Tuple[str, str]: A tuple containing the feature and
                original sample ID.
        """
        feature, original_sample_id = self.identify_feature_new_case(sample_id)
        if feature != "unknown":
            return feature, original_sample_id
        else:
            library_prep_method = self.project_info.get("library_prep_method", "")
            feature = self.get_default_feature(library_prep_method)
            original_sample_id = sample_id
            return feature, original_sample_id

    def create_lab_samples(
        self, sample_data: Dict[str, Any]
    ) -> Dict[str, Tuple[TenXLabSample, str]]:
        """Create lab samples from the sample data.

        Args:
            sample_data (Dict[str, Any]): The sample data.

        Returns:
            Dict[str, Tuple[TenXLabSample, str]]: A dictionary mapping sample IDs
                to lab sample instances and original IDs.
        """
        lab_samples = {}
        for sample_id, sample_info in sample_data.items():
            if self.case_type == "old_format":
                feature, original_sample_id = self.identify_feature_and_original_id_old(
                    sample_id, sample_info
                )
            else:
                feature, original_sample_id = self.identify_feature_and_original_id_new(
                    sample_id
                )

            lab_sample = TenXLabSample(
                sample_id, feature, sample_info, self.project_info
            )
            lab_samples[sample_id] = (lab_sample, original_sample_id)
        return lab_samples

    def group_lab_samples(
        self, lab_samples: Dict[str, Tuple[TenXLabSample, str]]
    ) -> Dict[str, List[TenXLabSample]]:
        """Group lab samples by original sample ID.

        Args:
            lab_samples (Dict[str, Tuple[TenXLabSample, str]]): The lab samples.

        Returns:
            Dict[str, List[TenXLabSample]]: A dictionary grouping lab samples by original sample ID.
        """
        groups: Dict[str, list[TenXLabSample]] = {}
        for lab_sample, original_sample_id in lab_samples.values():
            groups.setdefault(original_sample_id, []).append(lab_sample)
        return groups

    def create_run_samples(
        self, grouped_lab_samples: Dict[str, List[TenXLabSample]]
    ) -> List[TenXRunSample]:
        """Create run samples from grouped lab samples.

        Args:
            grouped_lab_samples (Dict[str, List[TenXLabSample]]): Grouped lab samples.

        Returns:
            List[TenXRunSample]: A list of run sample instances.
        """
        run_samples = []
        for original_sample_id, lab_samples in grouped_lab_samples.items():
            run_sample = TenXRunSample(
                original_sample_id,
                lab_samples,
                self.project_info,
                self.config,
                self.ydm,
            )
            run_samples.append(run_sample)
        return run_samples

    def extract_samples(self) -> List[TenXRunSample]:
        """Extract and prepare samples for processing.

        Returns:
            List[TenXRunSample]: A list of run sample instances ready for processing.
        """
        sample_data = self.doc.get("samples", {})
        # Step 1: Filter aborted samples
        sample_data = self.filter_aborted_samples(sample_data)
        # Step 2: Create lab samples
        lab_samples = self.create_lab_samples(sample_data)
        # Step 3: Group lab samples by original sample ID
        grouped_lab_samples = self.group_lab_samples(lab_samples)
        # Step 4: Create run samples
        run_samples = self.create_run_samples(grouped_lab_samples)
        return run_samples

    def pre_process(self) -> None:
        """Perform any pre-processing steps required before processing the project."""
        pass

    async def process(self):
        """Process the TenX project by handling its samples."""
        logging.info(f"Processing TenX project {self.project_info['project_name']}")
        self.status = "processing"

        self.samples = self.extract_samples()

        logging.info(
            f"Samples to be processed: {[sample.run_sample_id for sample in self.samples]}"
        )
        logging.info(f"Sample features: {[sample.features for sample in self.samples]}")

        if not self.samples:
            logging.warning("No samples found for processing. Returning...")
            return

        # Process each sample asynchronously
        tasks = [sample.process() for sample in self.samples]
        await asyncio.gather(*tasks)

        logging.info(
            f"All samples processed for project {self.project_info['project_name']}"
        )
        self.finalize_project()

    def create_slurm_job(self, data: Any) -> str:
        return ""

    def post_process(self, result: Any) -> None:
        pass

    def finalize_project(self) -> None:
        """
        Finalize the project by handling post-processing steps (e.g., report generation).
        """
        logging.info(f"Finalizing project {self.project_info['project_name']}")
        # Placeholder for any project-level finalization steps, like report generation, cleanup, etc.
        self.status = "completed"
        logging.info(
            f"Project {self.project_info['project_name']} has been successfully finalized."
        )
