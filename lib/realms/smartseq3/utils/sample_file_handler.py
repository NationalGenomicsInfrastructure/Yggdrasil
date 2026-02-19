import glob
import logging
import shutil
from pathlib import Path

from lib.core_utils.logging_utils import custom_logger
from lib.realms.smartseq3.utils.ss3_utils import SS3Utils

logger = custom_logger(__name__)


class SampleFileHandler:
    """
    Handles file operations for a SmartSeq3 sample, including managing file paths, creating necessary directories,
    ensuring barcode files exist, locating reference and FASTQ files, creating symlinks, validating output files, etc.

    Attributes:
        sample_id (str): Identifier for the sample.
        plate (str): Identifier for the plate.
        flowcell_id (str): Identifier for the flowcell.
        barcode (str): Barcode of the sample.
        project_id (str): Identifier for the project.
        project_name (str): Name of the project.
        sample_ref (str): Reference genome for the sample.
        config (dict): Configuration settings.
        base_dir (Path): Base directory path for the project.
        sample_dir (Path): Directory path for the sample.
        zumis_output_dir (Path): Directory path for zUMIs output.
        stats_dir (Path): Directory path for zUMIs stats.
        expression_dir (Path): Directory path for zUMIs expression output.
        fastq_files_dir (Path): Directory path for FASTQ files.
        plots_dir (Path): Directory path for plots.
        fastq_files (dict): Dictionary of FASTQ file paths.
    """

    # TODO: Only pass the project_name if project_info is not used anywhere else
    def __init__(self, sample, logger: logging.Logger | None = None):
        """
        Initialize the SampleFileHandler with sample information and configuration settings.

        Args:
            sample (object): The sample object containing sample data and project information.
        """
        self._logger = logger or custom_logger(f"{__name__}.{type(self).__name__}")
        self.sample_id = sample.id

        # NOTE: Temporary solution to keep the plate id for the transition period
        self.plate = sample.id  # sample.sample_data.get('customer_name', sample.id)

        self.flowcell_id = sample.flowcell_id
        self.barcode = sample.barcode
        self.project_id = sample.project_info.get("project_id", None)
        self.project_name = sample.project_info.get(
            "project_name", None
        )  # TODO: Remove this if not used anywhere else / see todo above
        self.sample_ref = sample.project_info.get("ref_genome", None)
        self.organism = sample.project_info.get("organism", None)
        self.config = sample.config

        # Define sample folder structure
        self.project_dir = sample.project_info.get("project_dir", "")
        self.sample_dir = self.project_dir / self.sample_id
        self.zumis_output_dir = self.sample_dir / "zUMIs_output"
        self.stats_dir = self.sample_dir / "zUMIs_output" / "stats"
        self.expression_dir = self.sample_dir / "zUMIs_output" / "expression"
        self.fastq_files_dir = self.sample_dir / "fastq_files"
        self.plots_dir = self.sample_dir / "plots"

        # Initialize fastq files
        self.fastq_files = {"R1": None, "R2": None, "I1": None, "I2": None}

        # Define critical file paths
        self.init_file_paths()

    def init_file_paths(self):
        """
        Define critical file paths used in the sample processing pipeline.
        """
        # zUMIs output files
        self.gene_counts_fpath = self.stats_dir / f"{self.plate}.genecounts.txt"
        self.reads_per_cell_fpath = self.stats_dir / f"{self.plate}.readspercell.txt"
        self.umicount_inex_loom_fpath = (
            self.expression_dir / f"{self.plate}.umicount.inex.all.loom"
        )
        self.bc_umi_stats_fpath = (
            self.zumis_output_dir
            / f"{self.plate}kept_barcodes_binned.txt.BCUMIstats.txt"
        )
        self.zumis_log_fpath = self.sample_dir / f"{self.plate}.zUMIs_runlog.txt"
        self.features_plot_fpath = self.stats_dir / f"{self.plate}.features.pdf"

        # Files needed for processing
        self.slurm_script_path = self.project_dir / f"{self.sample_id}_slurm_script.sh"
        self.barcode_fpath = (
            Path(self.config["smartseq3_dir"]) / "barcodes" / f"{self.barcode}.txt"
        )
        self.barcode_lookup_fpath = Path(self.config["barcode_lookup_path"])

        # Report output files
        self.umi_stats_fpath = self.stats_dir / f"{self.plate}.umi_stats.txt"
        self.well_barcodes_fpath = self.stats_dir / f"{self.plate}.well_barcodes.txt"
        # TODO: whether PDF or HTML should be decided by the report generator
        self.report_fpath = self.zumis_output_dir / f"{self.plate}_SmartSeq3_report.pdf"

    def ensure_barcode_file(self):
        """
        Ensure that the barcode file exists, creating it if necessary.

        Returns:
            bool: True if the barcode file exists or is created successfully, False otherwise.
        """
        if not self.barcode_fpath.exists():
            self._logger.info(
                f"Barcode file for '{self.barcode}' does not exist. Creating..."
            )
            self._logger.debug(f"In: {self.barcode_fpath}")
            if not SS3Utils.create_barcode_file(
                self.barcode, self.barcode_lookup_fpath, self.barcode_fpath
            ):
                self._logger.error(
                    f"Failed to create barcode file at {self.barcode_fpath}."
                )
                return False
        return True

    def locate_ref_paths(self):
        """
        Maps a reference genome to its STAR index and GTF file paths and validate their existence.

        Returns:
            dict or None: Dictionary containing paths to the index and GTF files, or None if files are missing.
        """
        try:
            species_key = None

            # Check if sample_ref is provided and valid
            if self.sample_ref and self.sample_ref != "Other (-, -)":
                species_key = (
                    self.sample_ref.split(",")[0].split("(")[1].strip().lower()
                )

            # If sample_ref is None or indicates an unspecified reference, use self.organism
            if not species_key or species_key == "-" or species_key == "other":
                # If so, use self.organism as the species key, ensuring it's not None
                if self.organism:
                    species_key = self.organism.strip().lower()
                else:
                    self._logger.warning(
                        f"Organism is None for {self.sample_id}. Handle manually."
                    )
                    return None

            # Validate species_key before proceeding
            if not species_key:
                self._logger.warning(
                    f"Invalid species_key for {self.sample_id}. Handle manually."
                )
                return None

            # Attempt to retrieve paths, handle KeyError if species_key is not in config
            try:
                idx_path = Path(self.config["gen_refs"][species_key]["idx_path"])
                gtf_path = Path(self.config["gen_refs"][species_key]["gtf_path"])
            except KeyError:
                self._logger.warning(
                    f"Reference for {species_key} species not found in config. Handle {self.sample_id} manually."
                )
                return None

            # Check the existence of reference files
            if not idx_path.exists() or not gtf_path.exists():
                missing_files = "\n\t".join(
                    [str(p) for p in [idx_path, gtf_path] if not p.exists()]
                )
                self._logger.warning(
                    f"Missing reference genome files: \n[\n\t{ missing_files }\n]"
                )
                return None

            return {"gen_path": idx_path, "gtf_path": gtf_path}
        except (IndexError, AttributeError) as e:
            self._logger.error(
                f"Error parsing sample_ref or accessing organism: {str(e)}. Handle {self.sample_id} manually."
            )
            return None

    def locate_fastq_files(self):
        """
        Initialize and validate FASTQ file paths from the source directory.

        Returns:
            dict or None: Dictionary of FASTQ file paths if all files are found, None otherwise.
        """
        pattern = Path(
            self.config["seq_root_dir"],
            self.project_id,
            self.sample_id,
            "*",
            self.flowcell_id,
            f"{self.sample_id}_S*_*_*.f*q.gz",
        )
        file_paths = glob.glob(str(pattern))

        for file_path in file_paths:
            file = Path(file_path)
            if file.name.endswith((".fastq.gz", ".fq.gz")):
                if "_R1_" in file.stem:
                    self.fastq_files["R1"] = file
                elif "_R2_" in file.stem:
                    self.fastq_files["R2"] = file
                elif "_I1_" in file.stem:
                    self.fastq_files["I1"] = file
                elif "_I2_" in file.stem:
                    self.fastq_files["I2"] = file

        if not all(self.fastq_files.values()):
            missing = [key for key, value in self.fastq_files.items() if value is None]
            self._logger.warning(
                f"Missing FASTQ files for {missing} in {Path(pattern).parent}"
            )
            return None

        return self.fastq_files

    def symlink_fastq_files(self):
        """
        Create symlinks for the directory containing the FASTQ files and copy auxiliary files.

        Returns:
            bool: True if symlinks and auxiliary files are created/copied successfully, False otherwise.
        """
        try:
            # Construct the path directly from the known structure
            fastq_parent_dir = Path(
                self.config["seq_root_dir"], self.project_id, self.sample_id
            )

            # Create symlink for the fastq_parent_dir
            symlink_target = self.fastq_files_dir / fastq_parent_dir.name
            if not symlink_target.exists():
                symlink_target.symlink_to(fastq_parent_dir)
                self._logger.info(
                    f"Symlink created for directory {fastq_parent_dir} in {self.fastq_files_dir}"
                )
            else:
                self._logger.debug(
                    f"Symlink for directory {fastq_parent_dir} already exists."
                )

            # Handle .md5 and .lst files
            for file_extension in [".md5", ".lst"]:
                source_file = fastq_parent_dir.with_suffix(file_extension)
                if source_file.exists():
                    shutil.copy(source_file, self.fastq_files_dir)
                    self._logger.info(f"Copied {source_file} to {self.fastq_files_dir}")
                else:
                    self._logger.warning(
                        f"File {source_file} does not exist and was not copied."
                    )

        except Exception as e:
            self._logger.error(f"Failed to create symlink and copy files: {e}")
            return False

        return True

    # TODO: Add checks to ensure that the paths exist
    def get_stat_files(self):
        """
        Retrieve paths to critical statistics files generated by zUMIs.

        Returns:
            dict: Dictionary containing paths to stat files (gene counts, reads per cell, and barcode UMI stat files).
        """
        stats_files = {
            "genecounts": self.gene_counts_fpath,
            "readspercell": self.reads_per_cell_fpath,
            "bc_umi_stats": self.bc_umi_stats_fpath,
        }
        return stats_files

    # TODO: Add checks to ensure that the paths exist
    def get_counts_loom_file(self):
        """
        Retrieve paths to loom files containing UMI counts.

        Returns:
            dict: Dictionary containing paths to UMI count loom files.
        """
        loom_file = {"umicount_inex": self.umicount_inex_loom_fpath}
        return loom_file

    def create_directories(self):
        """
        Create sample directories for storing fastq files and plots.
        """
        if self.sample_dir.exists():
            self.fastq_files_dir.mkdir(exist_ok=True)
            self.plots_dir.mkdir(exist_ok=True)
        else:
            self._logger.error(
                f"Sample {self.sample_id} directory does not exist (yet?): {self.sample_dir}"
            )

    # def create_fastq_folder(self):
    #     """Create fastq_files folder and manage soft links."""
    #     self.fastq_files_dir.mkdir(exist_ok=True)
    #     # Logic to create soft links to fastq files

    # def create_plots_folder(self):
    #     """Create 'plots' folder for storing generated plots."""
    #     self.plots_dir.mkdir(exist_ok=True)

    # def get_gene_counts_file_path(self):
    #     """Get path to the gene counts file."""
    #     return self.zumis_output_dir / 'stats' / f"{self.sample_id}.genecounts.txt"

    # def get_reads_per_cell_file_path(self):
    #     """Get path to the reads per cell file."""
    #     return self.zumis_output_dir / 'stats' / f"{self.sample_id}.readspercell.txt"

    def is_output_valid(self):
        """
        Checks if the sample root directory and all expected zUMIs output files are present.

        This method verifies the presence of the sample root directory and all critical files generated by the zUMIs pipeline.

        Returns:
            bool: True if the root directory and all expected files are found, False otherwise.
        """
        if not (self.sample_dir.exists() and self.sample_dir.is_dir()):
            # TODO: In this case it might not make sense to continue, probably skip and report the issue (through Slack?)
            self._logger.error(
                f"Sample {self.sample_id} directory does not exist: {self.sample_dir}"
            )
            return

        expected_files = [
            self.gene_counts_fpath,
            self.reads_per_cell_fpath,
            self.umicount_inex_loom_fpath,
            self.bc_umi_stats_fpath,
            self.zumis_log_fpath,
            self.features_plot_fpath,
        ]

        missing_files = [
            file.name for file in expected_files if not file.exists()
        ]  # or file.stat().st_size == 0]

        if missing_files:
            missing_files_str = "\n\t".join(missing_files)
            self._logger.warning(
                f"Missing or empty crucial zUMIs output files for sample {self.sample_id} in {self.sample_dir}:\n[\n\t{missing_files_str}\n]"
            )
            return False
        else:
            self._logger.info(
                f"All expected zUMIs output files are present for sample {self.sample_id}."
            )
            return True
