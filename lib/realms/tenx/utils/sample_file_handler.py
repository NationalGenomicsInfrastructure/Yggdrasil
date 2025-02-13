import re
from pathlib import Path
from typing import Any, Dict, Optional

from lib.core_utils.logging_utils import custom_logger

logging = custom_logger(__name__.split(".")[-1])


class SampleFileHandler:
    """
    Handles file operations for a 10x sample, including managing file paths,
    creating necessary directories, locating reference and FASTQ files,
    creating symlinks, validating output files, etc.

    Attributes:
        sample_id (str): Identifier for the sample.]
        project_id (str): Identifier for the project.
        project_name (str): Name of the project.
        sample_ref (str): Reference genome for the sample.
        organism (str): Organism associated with the sample.
        config (Dict[str, Any]): Configuration settings.
        project_dir (Path): Base directory path for the project.
        sample_dir (Path): Directory path for the sample.
        fastq_files_dir (Path): Directory path for FASTQ files.
        fastq_files (Dict[str, Any]): Dictionary of FASTQ file paths.
        slurm_script_path (Path): Path to the SLURM script file.
        summary_fpath (Path): Path to the summary output file.
    """

    def __init__(self, sample: Any) -> None:
        """Initialize the SampleFileHandler with sample information and configuration settings.

        Args:
            sample (Any): The sample object containing sample data and project information.
        """
        self.sample_id: str = sample.run_sample_id
        self.project_id: str = sample.project_info.get("project_id", "")
        self.project_name: str = sample.project_info.get("project_name", "")
        self.sample_ref: str = sample.project_info.get("ref_genome", "")
        self.organism: str = sample.project_info.get("organism", "")
        self.config: Dict[str, Any] = sample.config
        self.pipeline_info: Dict[str, Any] = sample.pipeline_info

        # Define sample folder structure
        self.project_dir: Path = sample.project_info.get("project_dir", "")
        self.sample_dir: Path = self.project_dir / self.sample_id
        self.fastq_files_dir: Path = self.project_dir / "fastq_files"

        self.fastq_files: Dict[str, Any] = {}

        # Define the name the report should have when transferred to ngi-interal
        self.dest_report_name: str = f"{self.sample_id}_10x_report.html"

        # Define critical file paths
        self.init_file_paths()

    def init_file_paths(self) -> None:
        """Initialize critical file paths."""
        # Files needed for processing
        self.slurm_script_path: Path = (
            self.project_dir / f"{self.sample_id}_slurm_script.sh"
        )

        self.slurm_output_path: Path = self.project_dir / f"{self.sample_id}.out"
        self.slurm_error_path: Path = self.project_dir / f"{self.sample_id}.err"

        # Report file path / Will be set after parsing the output file
        self._report_path: Optional[Path] = None

    @property
    def report_path(self):
        if self._report_path is None:
            if not self.extract_report_path():
                return None
        return self._report_path

    def check_run_success(self) -> bool:
        """Check if the CellRanger run completed successfully."""

        if not self.slurm_output_path.exists():
            logging.error(f"CellRanger output file not found: {self.slurm_output_path}")
            return False

        with open(self.slurm_output_path) as f:
            content = f.read()

        if "Pipestance completed successfully!" in content:
            logging.info(
                f"CellRanger run completed successfully for sample {self.sample_id}"
            )
            return True
        else:
            logging.error(
                f"CellRanger did not complete successfully for sample {self.sample_id}"
            )
            return False

    def extract_report_path(self) -> bool:
        """Extract the report path from the Cell Ranger output file."""
        if not self.slurm_output_path.exists():
            logging.error(f"CellRanger output file not found: {self.slurm_output_path}")
            return False

        with open(self.slurm_output_path) as f:
            content = f.read()

        report_path = None
        # Patterns to match different pipelines
        patterns = [r"Run summary HTML:\s+(\S+)", r"web_summary:\s+(\S+)"]

        for pattern in patterns:
            match = re.search(pattern, content)
            if match:
                report_path = Path(match.group(1))
                break

        if report_path and report_path.exists():
            self._report_path = report_path
            logging.info(f"Report path found: {self.report_path}")
            return True
        else:
            logging.error(
                f"Report path not found in CellRanger output for sample {self.sample_id}"
            )
            return False

    def get_libraries_csv_path(self) -> Path:
        return self.project_dir / f"{self.sample_id}_libraries.csv"

    def get_multi_csv_path(self) -> Path:
        return self.project_dir / f"{self.sample_id}_multi.csv"

    def get_feature_reference_csv_path(self) -> Path:
        return self.project_dir / f"{self.sample_id}_feature_reference.csv"
