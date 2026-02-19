import csv
import logging
from collections.abc import Mapping
from typing import Any

from lib.base.abstract_sample import AbstractSample
from lib.core_utils.logging_utils import custom_logger
from lib.module_utils.report_transfer import transfer_report
from lib.module_utils.slurm_utils import generate_slurm_script
from lib.realms.tenx.utils.sample_file_handler import SampleFileHandler
from lib.realms.tenx.utils.tenx_utils import TenXUtils

DEBUG: bool = True  # Set to False in production


class TenXRunSample(AbstractSample):
    """Class representing a TenX run sample."""

    def __init__(
        self,
        sample_id: str,
        lab_samples: list[Any],
        project_info: dict[str, Any],
        config: Mapping[str, Any],
        yggdrasil_db_manager: Any,
        hpc_manager: Any,
        logger: logging.Logger | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize a TenXRunSample instance.

        Args:
            sample_id (str): The run sample ID.
            lab_samples (List[Any]): A list of lab sample instances.
            project_info (Dict[str, Any]): Project-specific information.
            config (Mapping[str, Any]): Configuration data.
            yggdrasil_db_manager (Any): Yggdrasil database manager instance.
            **kwargs (Any): Additional keyword arguments.
        """
        self._logger = logger or custom_logger(f"{__name__}.{type(self).__name__}")
        self.run_sample_id: str = sample_id
        self.lab_samples: list[Any] = lab_samples
        self.project_info: dict[str, Any] = project_info or {}
        # TODO: ensure project_id is always available
        self.project_id = self.project_info.get("project_id", "")
        self.config: Mapping[str, Any] = config or {}
        self.ydm: Any = yggdrasil_db_manager
        self.sjob_manager: Any = hpc_manager

        self.job_id: str | None = None

        # self.decision_table = TenXUtils.load_decision_table("10x_decision_table.json")
        self.feature_to_library_type: dict[str, Any] = self.config.get(
            "feature_to_library_type", {}
        )
        # self._status: str = "initialized"

        self.features: list[str] = self._collect_features()
        self.pipeline_info: dict[str, Any] | None = self._get_pipeline_info() or {}
        self.auto_submit: bool = self.pipeline_info.get("submit", False)
        self.reference_genomes: dict[str, str] = (
            self.collect_reference_genomes()
        ) or {}

        self.file_handler: SampleFileHandler = SampleFileHandler(self)

        self._status: str = "initialized"

    @property
    def id(self) -> str:
        """Get the run sample ID.

        Returns:
            str: The run sample ID.
        """
        return self.run_sample_id

    @property
    def status(self) -> str:
        """Get the current status of the sample.

        Returns:
            str: The current status.
        """
        return self._status

    @status.setter
    def status(self, value: str) -> None:
        """Set the current status of the sample.
        # (FUTURE) The status will be updated in the Yggdrasil database.

        Args:
            value (str): The new status value.
        """
        self._status = value
        # Update the status in the database
        self.ydm.update_sample_status(self.project_id, self.id, value)

    def collect_reference_genomes(self) -> dict[str, str] | None:
        """Collect reference genomes from lab samples and ensure consistency.

        Returns:
            Optional[Dict[str, str]]: A dictionary mapping reference keys to genome paths,
                or None if an error occurs.
        """
        ref_genomes: dict[str, str] = {}
        feature_to_ref_key = (
            self.config.get("feature_to_ref_key", {}) if self.config else {}
        )

        for lab_sample in self.lab_samples:
            if lab_sample.reference_genome:
                ref_key = feature_to_ref_key.get(lab_sample.feature)
                if not ref_key:
                    self._logger.error(
                        f"Feature '{lab_sample.feature}' is not recognized for reference genome mapping."
                    )
                    continue

                # TODO: test this logic - if existing ref same as another ref in lab sample, keep one e.g. take the set. Why fail this?
                # Ensure no conflicting reference genomes for the same ref_key
                existing_ref = ref_genomes.get(ref_key)
                if existing_ref and existing_ref != lab_sample.reference_genome:
                    self._logger.debug(
                        f"Existing reference genome: {existing_ref} != {lab_sample.reference_genome}"
                    )
                    self._logger.error(
                        f"Conflicting reference genomes found for reference key '{ref_key}' "
                        f"in sample '{self.id}'"
                    )
                    self.status = "failed"
                    return None
                else:
                    ref_genomes[ref_key] = lab_sample.reference_genome
            else:
                self._logger.error(
                    f"Lab sample {lab_sample.lab_sample_id} is missing a reference genome."
                )
                self.status = "failed"
                return None
        return ref_genomes

    def _get_pipeline_info(self) -> dict[str, Any] | None:
        """Get the pipeline information for the sample.

        Returns:
            Optional[Dict[str, Any]]: A dictionary containing pipeline information,
                or None if not found.
        """
        library_prep_method = self.project_info.get("library_prep_method", "")
        return TenXUtils.get_pipeline_info(library_prep_method, self.features)

    def _collect_features(self) -> list[str]:
        """Collect features from lab samples.

        Returns:
            List[str]: A list of unique features.
        """
        features = [lab_sample.feature for lab_sample in self.lab_samples]
        return list(set(features))

    async def pre_process(self):
        """Perform pre-processing steps before starting the processing."""
        self._logger.info(f"[{self.id}] Pre-processing...")

        # Step 1: Verify that all subsamples have FASTQ files
        # TODO: Also check any other requirements
        missing_fq_labsamples = [
            lab_sample.lab_sample_id
            for lab_sample in self.lab_samples
            if not lab_sample.fastq_dirs
        ]
        if missing_fq_labsamples:
            self._logger.error(
                f"[{self.id}] Missing FASTQ files for lab-samples: "
                f"{missing_fq_labsamples}. Skipping..."
            )
            self.status = "pre_processing_failed"
            return

        # Step 2: Determine the pipeline and additional files required
        if not self.pipeline_info:
            self._logger.error(
                f"[{self.id}] No pipeline information found. Skipping..."
            )
            self.status = "pre_processing_failed"
            return

        self._logger.info(f"[{self.id}] Generating required files...")

        # Step 3: Generate required files based on configuration
        # TODO: Register generated files in the file handler
        files_to_generate = self.pipeline_info.get("files_to_generate", [])
        for file_type in files_to_generate:
            if file_type == "libraries_csv":
                self.generate_libraries_csv()
            elif file_type == "feature_ref_csv":
                self.generate_feature_reference_csv()
            elif file_type == "multi_csv":
                self.generate_multi_sample_csv()

        # Step 4: Prepare SLURM script
        cellranger_command = self.assemble_cellranger_command()

        slurm_metadata = {
            "sample_id": self.id,
            "project_name": self.project_info.get("project_name", ""),
            "project_dir": str(self.file_handler.project_dir),
            "output_log": str(self.file_handler.slurm_output_path),
            "error_log": str(self.file_handler.slurm_error_path),
            "cellranger_command": cellranger_command,
        }

        slurm_template_path = self.config.get("slurm_template", "")
        if not generate_slurm_script(
            slurm_metadata, slurm_template_path, self.file_handler.slurm_script_path
        ):
            self._logger.error(f"[{self.id}] Failed to generate SLURM script.")
            self.status = "pre_processing_failed"
            return

        # If all pre-processing steps succeeded
        self.status = "pre_processed"
        self._logger.info(f"[{self.id}] Pre-processing completed successfully.")

    async def process(self):
        """Process the sample."""
        self._logger.info("\n")
        self._logger.info(f"[{self.id}] Processing...")

        if self.pipeline_info is None:
            self._logger.error(
                f"[{self.id}] Pipeline information is missing. Skipping..."
            )
            self.status = "processing_failed"
            return

        # Check if SLURM script should be submitted
        if not self.pipeline_info.get("submit", False):
            self._logger.info(
                f"[{self.id}] According to decision table, we should not submit. "
                f"Handle manually!"
            )
            self.status = "requires_manual_submission"
            return

        self._logger.debug(f"[{self.id}] Slurm script created. Submitting job...")
        self.status = "processing"
        self.job_id = await self.sjob_manager.submit_job(
            self.file_handler.slurm_script_path
        )

        if self.job_id:
            self._logger.debug(f"[{self.id}] Job submitted with ID: {self.job_id}")
            # Wait for the job to complete and monitor its status
            await self.sjob_manager.monitor_job(self.job_id, self)
            self._logger.debug(f"[{self.id}] Job {self.job_id} monitoring complete.")

            # NOTE: The sample's status will be updated by SlurmJobManager's check_status method
        else:
            self._logger.error(f"[{self.id}] Failed to submit job.")
            self.status = "processing_failed"
            return

    def assemble_cellranger_command(self) -> str:
        """Assemble the Cell Ranger command based on the pipeline information.

        Returns:
            str: The assembled command string ready to be executed.
        """
        if self.pipeline_info is None:
            raise ValueError("Pipeline information is missing.")

        if self.reference_genomes is None:
            raise ValueError("Reference genomes information is missing.")

        pipeline = self.pipeline_info.get("pipeline", "")
        pipeline_exec = self.pipeline_info.get("pipeline_exec", "")
        required_args = self.pipeline_info.get("required_arguments", [])
        additional_args = self.pipeline_info.get("command_arguments", [])

        command_parts = [f"{pipeline_exec} {pipeline}"]

        self._logger.debug(f"[{self.id}] Pipeline: {pipeline}")
        self._logger.debug(f"[{self.id}] Pipeline executable: {pipeline_exec}")

        # Mapping of argument names to their values
        arg_values: dict[str, Any] = {
            "--id": self.id,
            "--csv": str(self.file_handler.get_multi_csv_path()),
            "--transcriptome": self.reference_genomes["gex"],
            "--fastqs": ",".join(
                [",".join(paths) for paths in self.lab_samples[0].fastq_dirs.values()]
            ),
            "--sample": self.lab_samples[0].lab_sample_id,
            "--libraries": str(self.file_handler.get_libraries_csv_path()),
            "--feature-ref": str(self.file_handler.get_feature_reference_csv_path()),
        }

        # Add references based on the pipeline
        # if self.pipeline_info.get("pipeline") == "count":
        #     if "gex" in self.reference_genomes:
        #         arg_values["--transcriptome"] = self.reference_genomes["gex"]
        # elif self.pipeline_info.get("pipeline") == "vdj":
        #     if "vdj" in self.reference_genomes:
        #         arg_values["--reference"] = self.reference_genomes["vdj"]
        # elif self.pipeline_info.get("pipeline") == "atac":
        #     if "atac" in self.reference_genomes:
        #         arg_values["--reference"] = self.reference_genomes["atac"]
        # elif self.pipeline_info.get("pipeline") == "multi":
        #     # references are specified in the multi-sample CSV file
        #     pass

        for arg in required_args:
            value = arg_values.get(arg)
            if value:
                command_parts.append(f"{arg}={value}")
            else:
                self._logger.error(
                    f"[{self.id}] Missing value for required argument {arg}"
                )

        # Include additional arguments
        command_parts.extend(additional_args)

        # Add output directory argument
        command_parts.append(f"--output-dir={str(self.file_handler.sample_dir)}")

        # Join all parts into a single command string
        command = " \\\n    ".join(command_parts)
        return command

    def collect_libraries_data(self) -> list[dict[str, str]]:
        """Generate the data for the libraries."""
        libraries_data = []
        for lab_sample in self.lab_samples:
            feature_type = self.feature_to_library_type.get(lab_sample.feature)
            if not feature_type:
                self._logger.error(
                    f"[{self.id}] Feature type not found for feature "
                    f"'{lab_sample.feature}' in sample '{lab_sample.sample_id}'"
                )
                continue
            # Collect FASTQ paths
            for paths in lab_sample.fastq_dirs.values():
                for path in paths:
                    libraries_data.append(
                        {
                            "fastqs": str(path),
                            "sample": lab_sample.lab_sample_id,
                            "library_type": feature_type,
                        }
                    )
        return libraries_data

    def generate_libraries_csv(self) -> None:
        """Generate the libraries CSV file required for processing."""
        self._logger.info(f"[{self.id}] Generating library CSV")
        library_csv_path = self.file_handler.get_libraries_csv_path()

        # Ensure the directory exists
        library_csv_path.parent.mkdir(parents=True, exist_ok=True)

        libraries_data = self.collect_libraries_data()

        with open(library_csv_path, "w", newline="") as csvfile:
            writer = csv.DictWriter(
                csvfile, fieldnames=["fastqs", "sample", "library_type"]
            )
            writer.writeheader()
            for lib in libraries_data:
                writer.writerow(lib)

        self._logger.info(f"[{self.id}] Libraries CSV generated at {library_csv_path}")

    def generate_feature_reference_csv(self) -> None:
        """Generate the feature reference CSV file required for processing."""
        self._logger.info(f"[{self.id}] Generating feature reference CSV")
        # feature_ref_csv_path = self.file_handler.get_feature_reference_csv_path()
        pass

    def generate_multi_sample_csv(self) -> None:
        """Generate the multi-sample CSV file required for processing."""
        self._logger.info(f"[{self.id}] Generating multi-sample CSV")
        multi_csv_path = self.file_handler.get_multi_csv_path()

        # Ensure the directory exists
        multi_csv_path.parent.mkdir(parents=True, exist_ok=True)

        with open(multi_csv_path, "w") as multi_file:
            # Get multi CSV sections and arguments from the configuration
            if self.pipeline_info:
                multi_sections = self.pipeline_info.get("multi_csv_sections", [])
                multi_arguments = self.pipeline_info.get("multi_csv_arguments", {})

            # Write sections based on multi_arguments
            for section in multi_sections:
                multi_file.write(f"[{section}]\n")
                # Add reference path if available
                ref_key = self.config.get("feature_to_ref_key", {}).get(section)
                if ref_key and ref_key in self.reference_genomes:
                    ref_path = self.reference_genomes.get(ref_key, "")
                    multi_file.write(f"reference,{ref_path}\n")
                else:
                    self._logger.warning(
                        f"No reference genome found for section '{section}'"
                    )
                # Add additional arguments
                for arg in multi_arguments.get(section, []):
                    multi_file.write(f"{arg}\n")
                multi_file.write("\n")

            # Write the [libraries] section
            multi_file.write("[libraries]\n")
            multi_file.write("fastq_id,fastqs,feature_types\n")
            libraries_data = self.collect_libraries_data()
            for lib in libraries_data:
                multi_file.write(
                    f"{lib['sample']},{lib['fastqs']},{lib['library_type']}\n"
                )

        self._logger.info(f"[{self.id}] Multi-sample CSV generated at {multi_csv_path}")

    def post_process(self) -> None:
        """Perform post-processing steps after job completion."""
        self._logger.info("\n")
        self._logger.info(f"[{self.id}] Post-processing...")
        self.status = "post_processing"

        # Check if the run was successful
        if not self.file_handler.check_run_success():
            self._logger.error(f"[{self.id}] CellRanger run was not successful.")
            self.status = "post_processing_failed"
            return

        # Extract the report path
        if not self.file_handler.extract_report_path():
            self._logger.error(f"[{self.id}] Failed to extract report path.")
            self.status = "post_processing_failed"
            return

        # Transfer the report
        if self.file_handler.report_path and transfer_report(
            report_path=self.file_handler.report_path,
            project_id=self.project_info.get("project_id", ""),
            sample_id=self.id,
            destination_filename=self.file_handler.dest_report_name,
        ):
            self._logger.info(f"[{self.id}] Report transferred successfully.")
        else:
            self._logger.error(f"[{self.id}] Failed to transfer report.")
            self.status = "post_processing_failed"
            return

        # If all post-processing steps succeeded
        self.status = "completed"
        self._logger.info(f"[{self.id}] Post-processing completed successfully.")

    ####################################################################################################
    ######################## New methods for the templating transition #################################
    ####################################################################################################

    async def submit_job(self) -> None:
        """
        Submits a Slurm job for this sample, stores the job_id in the DB.
        """
        self._logger.info(f"[{self.id}] Submitting HPC job...")
        self.job_id = await self.sjob_manager.submit_job(
            self.file_handler.slurm_script_path
        )

        if self.job_id:
            self._logger.debug(f"[{self.id}] Job submitted with ID: {self.job_id}")
            # Store in DB
            self.ydm.update_sample_slurm_job_id(self.project_id, self.id, self.job_id)
            self._logger.info(f"[{self.id}] Job ID [{self.job_id}] stored in DB.")
            self.status = "auto-submitted"

            # Possibly set status, e.g. self.status = "processing"
        else:
            self._logger.error(f"[{self.id}] Failed to submit job.")
            self.job_id = None
            self.status = "job_submission_failed"
