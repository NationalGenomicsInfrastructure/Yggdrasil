import logging

from lib.base.abstract_sample import AbstractSample
from lib.core_utils.logging_utils import custom_logger
from lib.module_utils.report_transfer import transfer_report
from lib.module_utils.slurm_utils import generate_slurm_script
from lib.realms.smartseq3.report.report_generator import Smartseq3ReportGenerator
from lib.realms.smartseq3.utils.sample_file_handler import SampleFileHandler
from lib.realms.smartseq3.utils.ss3_utils import SS3Utils
from lib.realms.smartseq3.utils.yaml_utils import write_yaml


class SS3Sample(AbstractSample):
    """
    Class representing a sample in a SmartSeq3 project.

    Attributes:
        id (str): Sample ID.
        sample_data (dict): Data related to the sample.
        project_info (dict): Information about the parent project.
        barcode (str): Barcode of the sample.
        flowcell_id (str): ID of the latest flowcell.
        config (dict): Configuration settings.
        status (str): Current status of the sample.
        metadata (dict): Metadata for the sample.
        sjob_manager (Union[SlurmJobManager, MockSlurmJobManager]): Manager for submitting and monitoring Slurm jobs.
        file_handler (SampleFileHandler): Handler for sample files.
    """

    def __init__(
        self,
        sample_id,
        sample_data,
        project_info,
        config,
        yggdrasil_db_manager,
        hpc_manager,
        logger: logging.Logger | None = None,
    ):
        """
        Initialize a SmartSeq3 sample instance.

        Args:
            sample_id (str): ID of the sample.
            sample_data (dict): Data related to the sample.
            project_info (dict): Information about the parent project.
            config (dict): Configuration settings.
        """
        self._logger = logger or custom_logger(f"{__name__}.{type(self).__name__}")
        # TODO: self.id must be demanded by a template class
        self._id = sample_id
        self.sample_data = sample_data
        self.project_info = project_info
        # TODO: ensure project_id is always available
        self.project_id = self.project_info.get("project_id", "")
        self.config = config
        self.ydm = yggdrasil_db_manager
        self.sjob_manager = hpc_manager

        self.job_id: str | None = None

        # Initialize barcode
        self.barcode = self.get_barcode()

        # Collect flowcell ID
        self.flowcell_id = self._get_latest_flowcell()

        self.metadata = None  # YAML metadata
        self.slurm_meta = None  # Slurm metadata

        # Initialize SampleFileHandler
        self.file_handler = SampleFileHandler(self)

        if self.flowcell_id:
            self._status = "initialized"
        else:
            self._status = "unsequenced"

    @property
    def id(self):
        return self._id

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        self._status = value
        # Update the status in the database
        self.ydm.update_sample_status(self.project_id, self.id, value)

    async def pre_process(self):
        """Pre-process the sample by collecting metadata and creating YAML files."""
        self._logger.info("\n")
        self._logger.info(f"[{self.id}] Pre-processing...")
        # TODO: Maybe add decorators to log the start (and end?) of each method and update the status
        self.status = "pre_processing"

        yaml_metadata = self._collect_yaml_metadata()
        if not yaml_metadata:
            self._logger.error(f"[{self.id}] Metadata missing. Pre-processing failed.")
            self.status = "pre_processing_failed"
            return

        self._logger.info(f"[{self.id}] Metadata collected. Creating YAML file")
        if not self.create_yaml_file(yaml_metadata):
            self._logger.error(f"[{self.id}] Failed to create YAML file.")
            self.status = "pre_processing_failed"
            return
        self._logger.debug(f"[{self.id}] YAML file created.")

        self._logger.debug(f"[{self.id}] Collecting Slurm metadata...")
        slurm_metadata = self._collect_slurm_metadata()
        self.slurm_meta = slurm_metadata
        if not slurm_metadata:
            self._logger.error(
                f"[{self.id}] Slurm metadata missing. Pre-processing failed."
            )
            self.status = "pre_processing_failed"
            return

        # Create Slurm script and submit job
        # TODO: Move slurm_template_path to SampleFileHandler
        slurm_template_path = self.config.get("slurm_template", "")
        if not generate_slurm_script(
            slurm_metadata, slurm_template_path, self.file_handler.slurm_script_path
        ):
            self._logger.error(f"[{self.id}] Failed to create Slurm script.")
            self.status = "pre_processing_failed"
            return
        else:
            self._logger.debug(f"[{self.id}] Slurm script created.")

        # If all pre-processing steps succeeded
        self.status = "pre_processed"

    def get_barcode(self):
        """
        Retrieve and validate the barcode from sample data.

        Returns:
            str: The barcode of the sample.
        """
        try:
            library_prep = self.sample_data.get("library_prep", {})
            prep_A = library_prep.get("A", {})
            barcode = prep_A.get("barcode", None)
            if barcode:
                return barcode.split("-")[-1]
            else:
                self._logger.warning(f"No barcode found for sample {self.id}.")
                return None  # Or handle more appropriately based on your application's requirements
        except Exception as e:
            self._logger.error(f"Error extracting barcode for sample {self.id}: {e}")
            return None

    def _collect_yaml_metadata(self):
        """
        Collect metadata necessary for creating a YAML file for the sample.

        Returns:
            dict: The collected metadata or None if necessary data is missing.
        """
        # NOTE: zUMIs does not support multiple flowcells per sample
        # Potential solutions:
        #   1. SmartSeq3 sample libraries should not be sequenced across multiple flowcells
        #       SmartSeq3 libraries should not be re-sequenced in the same project
        #   2. Merge fastq files from multiple flowcells

        # Select the latest flowcell for analysis
        if self.flowcell_id:
            fastqs = self.file_handler.locate_fastq_files()
            if fastqs is None:
                self._logger.warning(
                    f"No FASTQ files found for sample '{self.id}' in flowcell '{self.flowcell_id}'. Ensure files are correctly named and located."
                )
                return None
        else:
            self._logger.warning(f"No flowcell found for sample {self.id}")
            return None

        # if not all(fastqs.values()):
        #     self._logger.warning(f"Not all fastq files found at {fastq_path}")
        #     return None

        seq_setup = self.project_info.get("sequencing_setup", "")
        if seq_setup:
            read_setup = SS3Utils.transform_seq_setup(seq_setup)

        # ref_gen = self.project_info.get('ref_genome', '')

        # NOTE: Might break if the reference genome naming format is odd.
        # TODO: Might need to make more robust or even map the ref genomes to their paths
        ref_paths = self.file_handler.locate_ref_paths()
        if not ref_paths:
            self._logger.warning(
                f"Reference paths not found for sample {self.id}. Skipping..."
            )
            return None

        if self.barcode is None:
            self._logger.warning(f"Barcode not available for sample {self.id}")
            return None

        if not self.file_handler.ensure_barcode_file():
            self._logger.error(
                f"Failed to create barcode file for sample {self.id}. Skipping..."
            )
            return None

        try:
            metadata = {
                "plate": self.id,  # NOTE: Temporarily not used, but might be used when we name everything after ngi
                # 'plate': self.sample_data.get('customer_name', ''),
                "barcode": self.barcode,
                "bc_file": self.file_handler.barcode_fpath,
                "fastqs": {k: str(v) for k, v in fastqs.items() if v},
                "read_setup": read_setup,
                "ref": ref_paths,
                "outdir": str(self.file_handler.sample_dir),
                "out_yaml": self.file_handler.project_dir / f"{self.id}.yaml",
            }
        except Exception as e:
            self._logger.error(f"Error constructing metadata for sample {self.id}: {e}")
            return None

        self.metadata = metadata

        return metadata

    def _get_latest_flowcell(self):
        """
        Selects the latest flowcell for the current sample.

        Returns:
            The latest flowcell ID or None if no valid flowcells are found.
        """
        try:
            latest_fc = None
            latest_date = None
            if "library_prep" in self.sample_data:
                for prep_info in self.sample_data["library_prep"].values():
                    for fc_id in prep_info.get("sequenced_fc", []):
                        fc_date = SS3Utils.parse_fc_date(fc_id)
                        if fc_date and (not latest_date or fc_date > latest_date):
                            latest_date = fc_date
                            latest_fc = fc_id

            if not latest_fc:
                self._logger.warning(f"No valid flowcells found for sample {self.id}.")
            return latest_fc

        except Exception as e:
            self._logger.error(
                f"Error extracting latest flowcell info for sample '{self.id}': {e}",
                exc_info=True,
            )
            return None

    def _collect_slurm_metadata(self):
        """
        Collect metadata necessary for creating a Slurm job script.

        Returns:
            dict: The collected metadata or None if necessary data is missing.
        """
        try:
            metadata = {
                "project_name": self.project_info["project_name"],
                "project_dir": self.file_handler.project_dir,
                # 'sample_id': self.id, # Temporarily not used, but might be used when we name everything after ngi
                "plate_id": self.id,  # self.sample_data.get('customer_name', ''),
                "yaml_settings_path": self.file_handler.project_dir / f"{self.id}.yaml",
                "zumis_path": self.config["zumis_path"],
            }
        except Exception as e:
            self._logger.error(f"Error constructing metadata for sample {self.id}: {e}")
            return None

        return metadata

    def _transform_seq_setup(self, seq_setup_str):
        """
        Transforms a sequencing setup string into a detailed format for each read type.

        Args:
            seq_setup_str (str): Sequencing setup string in the format "R1-I1-I2-R2".

        Returns:
            dict: A dictionary with formatted strings for each read type.
        """
        r1, i1, i2, r2 = seq_setup_str.split("-")

        return {
            "R1": (f"cDNA(23-{r1})", "UMI(12-19)"),
            "R2": f"cDNA(1-{r2})",
            "I1": f"BC(1-{i1})",
            "I2": f"BC(1-{i2})",
        }

    def _get_ref_paths(self, ref_gen, config):
        """
        Maps a reference genome to its STAR index and GTF file paths.

        Args:
            ref_gen (str): Reference genome string, e.g., "Zebrafish (Danio rerio, GRCz10)".
            config (dict): Configuration object containing the mapping.

        Returns:
            tuple: A tuple containing the STAR index path and GTF file path, or None if not found.
        """
        try:
            # Extract species name before the first comma
            species_key = ref_gen.split(",")[0].split("(")[1].strip().lower()
            idx_path = config["gen_refs"][species_key]["idx_path"]
            gtf_path = config["gen_refs"][species_key]["gtf_path"]
            return idx_path, gtf_path
        except KeyError as e:
            self._logger.warning(
                f"Reference for {e} species not found in config. Handle {self.id} manually."
            )
            return None, None

    def create_yaml_file(self, metadata) -> bool:
        """
        Create a YAML file with the provided metadata.

        Args:
            metadata (dict): Metadata to write to the YAML file.

        Returns:
            bool: True if the YAML file was created successfully, False otherwise.
        """
        return write_yaml(self.config, metadata)

    async def process(self):
        """Process the sample by submitting its job."""
        self._logger.info("\n")
        self._logger.info(f"[{self.id}] Processing...")
        self._logger.debug(f"[{self.id}] Submitting job...")
        self.status = "processing"
        self.job_id = await self.sjob_manager.submit_job(
            self.file_handler.slurm_script_path
        )

        if self.job_id:
            self._logger.debug(f"[{self.id}] Job submitted with ID: {self.job_id}")
            # Wait here for the monitoring to complete before exiting the process method
            await self.sjob_manager.monitor_job(self.job_id, self)
            self._logger.debug(f"[{self.id}] Job {self.job_id} monitoring complete.")
        else:
            self._logger.error(f"[{self.id}] Failed to submit job.")
            self.status = "processing_failed"

    def post_process(self):
        """
        Post-process the sample after job completion.
        """
        self._logger.info("\n")
        self._logger.info(f"[{self.id}] Post-processing...")
        self.status = "post_processing"

        if not self.metadata:
            self.metadata = self._collect_yaml_metadata()
            if not self.metadata:
                self._logger.error(
                    f"[{self.id}] Metadata missing. Post-processing failed."
                )
                self.status = "post_processing_failed"
                return

        # Check if sample output is valid
        if not self.file_handler.is_output_valid():
            # TODO: Send a notification (Slack?) for manual intervention
            self._logger.error(
                f"[{self.id}] Pipeline output is invalid. Skipping post-processing."
            )
            self.status = "post_processing_failed"
            return

        self.file_handler.create_directories()

        # Create symlinks for the fastq files
        if not self.file_handler.symlink_fastq_files():
            self._logger.error(
                f"[{self.id}] Failed to manage symlinks and auxiliary files."
            )
            self.status = "post_processing_failed"
            return
        else:
            self._logger.info(
                f"[{self.id}] Successfully managed symlinks and auxiliary files."
            )

        # Instantiate report generator
        report_generator = Smartseq3ReportGenerator(self)

        # Collect stats
        if not report_generator.collect_stats():
            self._logger.error(
                f"[{self.id}] Error collecting stats. Skipping report generation."
            )
            self.status = "post_processing_failed"
            return

        # Create Plots
        if not report_generator.create_graphs():
            self._logger.error(
                f"[{self.id}] Error creating plots. Skipping report generation."
            )
            self.status = "post_processing_failed"
            return

        # Generate Report
        report_generator.render(format="PDF")

        # Transfer the Report
        if not self.file_handler.report_fpath.exists():
            self._logger.error(
                f"[{self.id}] Report not found at {self.file_handler.report_fpath}"
            )
            self.status = "post_processing_failed"
            return

        if transfer_report(
            report_path=self.file_handler.report_fpath,
            project_id=self.file_handler.project_id,
            sample_id=self.id,
        ):
            self._logger.info(f"[{self.id}] Report transferred successfully.")
        else:
            self._logger.error(f"[{self.id}] Failed to transfer report.")
            self.status = "post_processing_failed"
            return

        # If all post-processing steps succeeded
        self.status = "completed"

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
            self.job_id = None
            self.status = "job_submission_failed"
