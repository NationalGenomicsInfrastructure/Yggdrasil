import asyncio
import logging
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from lib.base.abstract_project import AbstractProject
from lib.core_utils.config_loader import ConfigLoader
from lib.core_utils.logging_utils import custom_logger
from lib.realms.tenx.lab_sample import TenXLabSample
from lib.realms.tenx.run_sample import TenXRunSample


class TenXProject(AbstractProject):
    """
    Class representing a TenX project.
    """

    config: Mapping[str, Any] = ConfigLoader().load_config("10x_config.json")

    def __init__(
        self,
        doc: dict[str, Any],
        yggdrasil_db_manager: Any,
        logger: logging.Logger | None = None,
    ) -> None:
        """
        Initialize a TenXProject instance.

        Args:
            doc (Dict[str, Any]): Document containing project metadata.
            yggdrasil_db_manager (Any): Yggdrasil database manager instance.
        """
        self._logger = logger or custom_logger(f"{__name__}.{type(self).__name__}")
        super().__init__(doc, yggdrasil_db_manager, logger=self._logger)
        self.submission_policy.realm_supports_auto = False  # Not as easy as SS3. Decide based on all(sample.auto_submit for sample in self.samples), when ready.

        # Already defined in the parent class
        # self.doc: Dict[str, Any] = doc
        # self.ydm: Any = yggdrasil_db_manager

        # TODO: Might need to check required fields for each different method, if they differ
        self.proceed: bool = self.check_required_fields()

        if self.proceed:
            self.initialize_project_in_db()
            # Extract metadata from project document
            self.project_info: dict[str, Any] = self._extract_project_info()

            if not self.determine_organism():
                # TODO: Send this message as a notification (e.g. on Slack)
                self._logger.error(
                    "Project organism could not be determined. Handle manually!"
                )
                self.proceed = False
                return

            self.project_dir: Path | None = self.ensure_project_directory()
            self.project_info["project_dir"] = self.project_dir
            self.samples: list[TenXRunSample] = []
            self.case_type: str = self.project_info.get("case_type", "unknown")
            self._logger.info(f"Case type: {self.case_type}")

            self.status: str = "initialized"

    # Experimental #
    async def launch_template(self) -> None:
        if not self.doc:
            self._logger.warning("TenxRealm: no project document bound; skipping.")
            return

        # Build the same payload your handler expects
        payload = {
            "doc": self.doc,  # or "document" if that’s what you use—be consistent
            "reason": f"project.status={getattr(self, 'project_status', '') or 'unknown'}",
        }
        from lib.realms.tenx.handler import handle_project_change

        # Offload the sync handler so we don’t block the event loop
        await asyncio.to_thread(handle_project_change, payload)

    def _extract_project_info(self) -> dict[str, Any]:
        """
        Extracts relevant project information from the document.

        Returns:
            Dict[str, Any]: A dictionary containing extracted project info.
        """
        try:
            details = self.doc.get("details", {})
            project_info: dict[str, Any] = {
                "project_name": self.doc.get("project_name", ""),
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
            self._logger.error(
                f"Error occurred while extracting project information: {e}"
            )
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
        self._logger.error(
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

    def check_required_fields(self) -> bool:
        """Check if the document contains all required fields.

        Returns:
            bool: True if all required fields are present, False otherwise.
        """
        required_fields = self.config.get("required_fields", [])

        missing_keys = [
            field for field in required_fields if not self._is_field(field, self.doc)
        ]

        if missing_keys:
            self._logger.warning(
                f"Missing required project information: {missing_keys}."
            )
            return False

        return True

    def _is_field(self, field_path: str, data: dict[str, Any]) -> bool:
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

    def ensure_project_directory(self) -> Path | None:
        """Ensures that the project directory exists. Creates it if necessary.

        Returns:
            Optional[Path]: The Path object of the project directory, or None if creation fails.
        """
        try:
            project_dir = (
                Path(self.config["10x_dir"])
                / "projects"
                / self.project_info["project_name"]
            )
            project_dir.mkdir(parents=True, exist_ok=True)
            return project_dir
        except Exception as e:
            self._logger.error(f"Failed to create project directory: {e}")
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

    def identify_feature_old_case(self, sample_info: dict[str, Any]) -> tuple[str, str]:
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

    def identify_feature_new_case(self, sample_id: str) -> tuple[str, str]:
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
            if sample_info.get("details", {}).get("status_(manual)", "").lower()
            != "aborted"
        }

    def identify_feature_and_original_id_old(
        self, sample_id: str, sample_info: dict[str, Any]
    ) -> tuple[str, str]:
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
            # NOTE: If you want to name samples with the customer name, comment out the line below
            original_sample_id = sample_id
            return feature, original_sample_id

    def identify_feature_and_original_id_new(self, sample_id: str) -> tuple[str, str]:
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
        self, sample_data: dict[str, Any]
    ) -> dict[str, tuple[TenXLabSample, str]]:
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

    def create_run_samples(
        self, grouped_lab_samples: dict[str, list[TenXLabSample]]
    ) -> list[TenXRunSample]:
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
                self.sjob_manager,
            )
            run_samples.append(run_sample)
        return run_samples

    def extract_samples(self) -> list[TenXRunSample]:
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

    async def launch(self):
        pass
        # """Launch the TenX Realm to handle its samples."""
        # self._logger.info(f"Processing TenX project {self.project_info['project_name']}")
        # self.status = "processing"

        # self.samples = self.extract_samples()
        # if not self.samples:
        #     self._logger.warning("No samples found for processing. Returning...")
        #     return

        # self.add_samples_to_project_in_db()

        # self._logger.info(f"Considered samples: {[sample.id for sample in self.samples]}")
        # self._logger.info(f"Sample features: {[sample.features for sample in self.samples]}")

        # # Pre-process each sample asynchronously
        # pre_tasks = [sample.pre_process() for sample in self.samples]
        # await asyncio.gather(*pre_tasks)

        # # Filter samples that passed pre-processing
        # pre_processed_samples = [
        #     sample for sample in self.samples if sample.status == "pre_processed"
        # ]

        # if not pre_processed_samples:
        #     self._logger.warning("No samples passed pre-processing. Exiting...")
        #     return

        # self._logger.info("\n")
        # self._logger.info(
        #     f"Samples that passed pre-processing:"
        #     f"{[sample.id for sample in pre_processed_samples]}"
        # )

        # # Process each sample asynchronously
        # tasks = [sample.process() for sample in pre_processed_samples]
        # await asyncio.gather(*tasks)

        # # Log samples that passed processing
        # processed_samples = [
        #     sample for sample in pre_processed_samples if sample.status == "completed"
        # ]
        # self._logger.info("\n")
        # self._logger.info(
        #     f"Samples that finished successfully: "
        #     f"{[sample.id for sample in processed_samples]}\n"
        # )

        # # self._logger.info(
        # #     f"All samples processed for project {self.project_info['project_name']}"
        # # )
        # self.finalize_project()

    def create_slurm_job(self, data: Any) -> str:
        return ""

    def post_process(self, result: Any) -> None:
        pass

    def finalize_project(self) -> None:
        """
        Finalize the project by handling post-processing steps (e.g., report generation).
        """
        self._logger.info(f"Finalizing project {self.project_info['project_name']}")
        # Placeholder for any project-level finalization steps, like report generation, cleanup, etc.
        self.status = "completed"
        self._logger.info(
            f"Project {self.project_info['project_name']} has been successfully finalized."
        )

    ####################################################################################################
    ######################## New methods for the templating transition #################################
    ####################################################################################################

    def do_extract_samples(self):
        # Reuse existing 10x logic (temporary for the templating transition).
        # Return a list of TenXSample objects
        self.samples = self.extract_samples()

        # Update the submission policy based on the samples
        self.submission_policy.realm_supports_auto = all(
            sample.auto_submit for sample in self.samples
        )

    async def do_pre_process_samples(self):
        self._logger.info("TenX realm: Pre-processing samples in parallel.")

        self._logger.info(
            f"Considered samples: {[sample.id for sample in self.samples]}"
        )
        self._logger.info(
            f"Sample features: {[sample.features for sample in self.samples]}"
        )

        tasks = [sample.pre_process() for sample in self.samples]
        await asyncio.gather(*tasks)

        # Filter out any that didn't become 'pre_processed'
        self.samples = [
            sample for sample in self.samples if sample.status == "pre_processed"
        ]
        if not self.samples:
            self._logger.warning("No samples passed pre-processing.")
        else:
            self._logger.info(
                f"Samples that passed pre-processing: {[sample.id for sample in self.samples]}"
            )

    async def do_finalize_project(self):
        """ """
        self.finalize_project()
        self.finalize_project()
