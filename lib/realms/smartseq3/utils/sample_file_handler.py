import glob

from pathlib import Path

from lib.realms.smartseq3.utils.ss3_utils import SS3Utils

from lib.utils.logging_utils import custom_logger

logging = custom_logger(__name__.split('.')[-1])

class SampleFileHandler:
    # TODO: Only pass the project_name if project_info is not used anywhere else
    def __init__(self, sample):
        self.sample_id = sample.id
        self.flowcell_id = sample.flowcell_id
        self.barcode = sample.barcode
        self.project_id = sample.project_info.get('project_id', None)
        self.project_name = sample.project_info.get('project_name', None) # TODO: Remove this if not used anywhere else / see todo above
        self.sample_ref = sample.project_info.get('ref_genome', None)
        self.config = sample.config

        # Define sample folder structure
        self.base_dir = Path(self.config['smartseq3_dir']) / 'projects' / self.project_name
        self.sample_dir = self.base_dir / self.sample_id
        self.zumis_output_dir = self.sample_dir / 'zUMIs_output'
        self.stats_dir = self.sample_dir / 'zUMIs_output' / 'stats'
        self.expression_dir = self.sample_dir / 'zUMIs_output' / 'expression'
        self.fastq_files_dir = self.sample_dir / 'fastq_files'
        self.plots_dir = self.sample_dir / 'plots'

        # Define critical file paths
        self.init_file_paths()


    def init_file_paths(self):
        # Define critical file paths
        self.gene_counts_fpath = self.stats_dir / f"{self.sample_id}.genecounts.txt"
        self.reads_per_cell_fpath = self.stats_dir / f"{self.sample_id}.readspercell.txt"
        self.umicount_inex_loom_fpath = self.expression_dir / f"{self.sample_id}.umicount.inex.all.loom"
        self.bc_umi_stats_fpath = self.zumis_output_dir / f"{self.sample_id}kept_barcodes_binned.txt.BCUMIstats.txt"
        self.zumis_log_fpath = self.sample_dir / f"{self.sample_id}.zUMIs_runlog.txt"
        self.features_plot_fpath = self.stats_dir / f"{self.sample_id}.features.pdf"
        self.barcode_fpath = Path(self.config['smartseq3_dir']) / "barcodes" / f"{self.barcode}.txt"
        self.barcode_lookup_fpath = Path(self.config['barcode_lookup_path'])

        # NOTE: This is a future file that will be created by the report generator
        # TODO: whether PDF or HTML should be decided by the report generator
        self.report_fpath = self.zumis_output_dir / f"{self.sample_id}_report.pdf"


    def ensure_barcode_file(self):
        """Ensure that the barcode file exists, creating it if necessary."""
        if not self.barcode_fpath.exists():
            logging.info(f"Barcode file for '{self.barcode}' does not exist. Creating...")
            logging.debug(f"In: {self.barcode_fpath}")
            if not SS3Utils.create_barcode_file(self.barcode, self.barcode_lookup_fpath, self.barcode_fpath):
                logging.error(f"Failed to create barcode file at {self.barcode_fpath}.")
                return False
        return True

    def locate_ref_paths(self, ref_gen):
        """
        Maps a reference genome to its STAR index and GTF file paths and validate their existence.

        Args:
            ref_gen (str): Reference genome identifier, e.g. "Zebrafish (Danio rerio, GRCz10)".

        Returns:
            dict or None: Dictionary containing paths to the index and GTF files, or None if files are missing.
        """
        try:
            species_key = ref_gen.split(',')[0].split('(')[1].strip().lower()
            idx_path = Path(self.config['gen_refs'][species_key]['idx_path'])
            gtf_path = Path(self.config['gen_refs'][species_key]['gtf_path'])

            # Check the existence of reference files
            if not idx_path.exists() or not gtf_path.exists():
                missing_files = '\n\t'.join([str(p) for p in [idx_path, gtf_path] if not p.exists()])
                logging.warning(f"Missing reference genome files: \n[\n\t{ missing_files }\n]")
                return None

            return {
                'gen_path': idx_path,
                'gtf_path': gtf_path
            }
        except KeyError:
            logging.warning(f"Reference for {species_key} species not found in config. Handle {self.sample_id} manually.")
            return None


    def locate_fastq_files(self):
        """ Initialize and validate fastq file paths from the source directory. """
        pattern = Path(self.config['seq_root_dir'], self.project_id, self.sample_id, '*', self.flowcell_id, f"{self.sample_id}_S*_*_*.f*q.gz")
        file_paths = glob.glob(str(pattern))
        fastq_files = {'R1': None, 'R2': None, 'I1': None, 'I2': None}

        for file_path in file_paths:
            file = Path(file_path)
            if file.name.endswith(('.fastq.gz', '.fq.gz')):
                if '_R1_' in file.stem:
                    fastq_files['R1'] = file
                elif '_R2_' in file.stem:
                    fastq_files['R2'] = file
                elif '_I1_' in file.stem:
                    fastq_files['I1'] = file
                elif '_I2_' in file.stem:
                    fastq_files['I2'] = file

        if not all(fastq_files.values()):
            missing = [key for key, value in fastq_files.items() if value is None]
            logging.warning(f"Missing FASTQ files for {missing} in {Path(pattern).parent}")
            # logging.warning(f"Missing or incorrect FASTQ files for sample {self.sample_id} in {pattern.parent}")
            return None

        return fastq_files


    # TODO: Add checks to ensure that the paths exist
    def get_stat_files(self):
        """Retrieve paths to critical statistics files generated by zUMIs."""
        stats_files = {
            'genecounts': self.gene_counts_fpath,
            'readspercell': self.reads_per_cell_fpath,
            'bc_umi_stats': self.bc_umi_stats_fpath
        }
        return stats_files

    # TODO: Add checks to ensure that the paths exist
    def get_counts_loom_file(self):
        """Retrieve paths to loom files containing UMI counts."""
        loom_file = {
            'umicount_inex': self.umicount_inex_loom_fpath
        }
        return loom_file

    def create_directories(self):
        """Create sample directories for storing fastq files and plots."""
        if self.sample_dir.exists():
            self.fastq_files_dir.mkdir(exist_ok=True)
            self.plots_dir.mkdir(exist_ok=True)
        else:
            logging.error(f"Sample {self.sample_id} directory does not exist (yet?): {self.sample_dir}")

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
        # Check if sample output dir exists
        # self.sample_dir = Path(self.sample_dir)
        if not (self.sample_dir.exists() and self.sample_dir.is_dir()):
            # TODO: In this case it might not make sense to continue, probably skip and report the issue (through Slack?)
            logging.error(f"Sample {self.sample_id} directory does not exist: {self.sample_dir}")
            return

        expected_files = [
            self.gene_counts_fpath,
            self.reads_per_cell_fpath,
            self.umicount_inex_loom_fpath,
            self.bc_umi_stats_fpath,
            self.zumis_log_fpath,
            self.features_plot_fpath
            # Add more paths to expected files here
        ]

        missing_files = [file.name for file in expected_files if not file.exists()] # or file.stat().st_size == 0]

        if missing_files:
            missing_files_str = "\n\t".join(missing_files)
            logging.warning(f"Missing or empty crucial zUMIs output files for sample {self.sample_id} in {self.sample_dir}:\n[\n\t{missing_files_str}\n]")
            return False
        else:
            logging.info(f"All expected zUMIs output files are present for sample {self.sample_id}.")
            return True