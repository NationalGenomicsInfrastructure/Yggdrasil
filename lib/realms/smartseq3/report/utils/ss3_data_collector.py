import logging

import loompy as lp
import numpy as np
import pandas as pd
from pandas.errors import EmptyDataError

from lib.core_utils.logging_utils import custom_logger
from lib.realms.smartseq3.utils.ss3_utils import SS3Utils

logger = custom_logger(__name__)


class SS3DataCollector:
    """
    Collects and aggregates data for SmartSeq3 samples.

    This class is responsible for collecting, aggregating, and processing statistical
    and metadata information for SmartSeq3 samples. It interacts with file handlers
    and utilizes various utilities to extract and compile necessary data.

    Attributes:
        sample (SS3Sample): Instance of SS3Sample containing sample information and metadata.
        file_handler (SampleFileHandler): Instance of SampleFileHandler for file operations.
        meta (dict): Metadata of the sample.
        config (dict): Configuration settings for the sample processing.

    Methods:
        collect_stats(): Collects and aggregates statistical data.
        _aggr_stats(stat_files, barcode_wells): Aggregates statistical data from multiple files.
        _merge_data_by_type(input_data, target_cols): Prepares and merges data based on unique 'type' values.
        _aggr_umis_from_loom(loom_file_path): Reads a loom file to aggregate UMI and read counts.
        collect_meta(stats): Collects metadata for the sample.
        get_zumis_version(zumis_log_fpath): Retrieves the zUMIs version from the log file.
        save_data(data, path): Saves the DataFrame to a CSV file.
    """

    def __init__(
        self,
        file_handler,
        sample,
        logger: logging.Logger | None = None,
    ):
        """
        Initialize the data collector with references to the file handler and sample instance.

        Args:
            file_handler (SampleFileHandler): Instance of SampleFileHandler for file operations.
            sample (SS3Sample): Instance of SS3Sample containing sample information and metadata.
        """
        self._logger = logger or custom_logger(f"{__name__}.{type(self).__name__}")
        self.sample = sample
        self.file_handler = file_handler
        self.meta = self.sample.metadata
        self.config = self.sample.config

    # def collect_stats(self):
    #     barcode_set = self.meta.get('barcode_set')
    #     if not barcode_set:
    #         self._logger.error("Missing barcode_set in metadata. Unable to extract well IDs.")
    #         # TODO: (FUTURE) Develope an exception handling strategy and raise an exception here (FUTURE)
    #         return None  # Or raise an exception, depending on your error handling strategy

    #     # get well_ids : barcodes dict for the given barcode set
    #     barcode_well_ids = self.extract_well_ids(barcode_set, self.config['barcode_lookup_path'])
    #     stat_files = self.file_handler.get_stat_files()

    #     # TODO: This is the new way. Compare to the old above. Also check speed of execution
    #     start_time_new = time.time()
    #     stats = self._aggr_stats(stat_files, barcode_well_ids)
    #     # After executing the new method
    #     end_time_new = time.time()
    #     execution_time_new = end_time_new - start_time_new
    #     # print(f"STATS: New Method Execution Time: {execution_time_new} seconds")

    #     counts_loom_file = self.file_handler.get_counts_loom_file()

    #     # TODO: This is the new way. Compare to the old below. Also check speed of execution
    #     start_time_new = time.time()
    #     counts = self._process_loom_file(counts_loom_file['umicount_inex'])
    #     # After executing the new method
    #     end_time_new = time.time()
    #     execution_time_new = end_time_new - start_time_new

    #     stats = pd.concat([stats, counts], axis=1)

    #     # Save new stats to files
    #     self.save_data(stats.loc[:, ('Loom', ['UMI_genes_detected', 'UMI_read_counts'])], self.file_handler.stats_dir / f"{self.file_handler.sample_id}.umi_stats.txt")
    #     self.save_data(stats.loc[:, ('bc_set', 'WellID')], self.file_handler.stats_dir / f"{self.file_handler.sample_id}.well_barcodes.txt")

    #     # TODO: Remember to change the new_stats to stats once the new methods are confirmed to work correctly
    #     return stats

    def collect_stats(self):
        """
        Collects and aggregates statistical data from various sources.

        Returns:
            pd.DataFrame or None: Aggregated statistics DataFrame if successful, None otherwise.
        """
        barcode_set = self.meta.get("barcode")
        if not barcode_set:
            self._logger.error(
                "Missing barcode in metadata. Unable to extract well IDs."
            )
            return None

        barcode_lookup = self.config.get("barcode_lookup_path")
        if not barcode_lookup:
            self._logger.error("Missing barcode lookup path in config.")
            return None

        barcode_well_ids = SS3Utils.extract_well_ids(barcode_set, barcode_lookup)
        if barcode_well_ids is None:
            self._logger.error("Well IDs extraction failed.")
            return None

        stat_files = self.file_handler.get_stat_files()
        stats = self._aggr_stats(stat_files, barcode_well_ids)
        if stats is None:
            self._logger.error("Aggregating stats failed.")
            return None

        counts_loom_file = self.file_handler.get_counts_loom_file()
        counts = self._aggr_umis_from_loom(counts_loom_file["umicount_inex"])
        if counts is None:
            self._logger.error("Aggregating UMI counts from loom failed.")
            return None

        if (
            not stats.empty and not counts.empty
        ):  # and stats.index.equals(counts.index):
            stats = pd.concat([stats, counts], axis=1)
            if stats.empty:
                self._logger.error("Concatenated stats are empty.")
                return None
        else:
            self._logger.error(
                "Stats and counts have incompatible indices or are empty."
            )
            self._logger.debug(f"Stats Index:\n{stats.index}")
            self._logger.debug(f"Counts Index:\n{counts.index}")
            return None

        # Save new stats to files
        self.save_data(
            stats.loc[:, ("Loom", ["UMI_genes_detected", "UMI_read_counts"])],
            self.file_handler.umi_stats_fpath,
        )
        self.save_data(
            stats.loc[:, ("bc_set", "WellID")], self.file_handler.well_barcodes_fpath
        )

        return stats

    ###########################################################################################################################
    ### NOTE: Below methods used for statistical data collection                                                              #
    ###########################################################################################################################

    def _aggr_stats(self, stat_files, barcode_wells):
        """
        Aggregate statistical data from multiple files for a given set of barcodes.

        Args:
            stat_files (dict): Dictionary with paths to statistical files.
            barcode_wells (pd.Series): Series mapping well IDs to barcodes.

        Returns:
            pd.DataFrame: DataFrame with aggregated statistical data or None on failure.
        """
        # Convert barcode_wells to DataFrame and set a MultiIndex
        data = pd.DataFrame(barcode_wells)

        # NOTE: The ', names=["source", "description"]' part is not necessary, but removing it will make the output
        # different to the old method's remnants. Discuss with the team as it might cause problems to users.
        data.columns = pd.MultiIndex.from_tuples(
            [("bc_set", "WellID")], names=["source", "description"]
        )

        for file_type, file_path in stat_files.items():
            try:
                stats = pd.read_table(file_path)
            except EmptyDataError:
                self._logger.error(f"File is empty or unreadable: {file_path}")
                return None
            except ValueError as e:
                self._logger.error(f"Error processing file {file_path}: {e}")
                return None
            except Exception as e:
                self._logger.error(
                    f"Unexpected error while reading file {file_path}: {e}"
                )
                return None

            if file_type == "readspercell":
                # Filter out 'bad' rows and perform necessary transformations
                stats = stats[stats["RG"] != "bad"]
                stats = self._merge_data_by_type(stats, ["N", "RG"])
                stats.set_index("RG", inplace=True)

                # NOTE: The ', names=["source", "description"]' part is not necessary, but removing it will make the output
                # different to the old method's remnants. Discuss with the team as it might cause problems to users.
                index = pd.MultiIndex.from_tuples(
                    [("readspercell", "TotalReads")], names=["source", "description"]
                )
                stats = pd.DataFrame(stats.sum(axis=1), columns=index)

            elif file_type == "bc_umi_stats":
                # Process Barcode-UMI statistics and set MultiIndex
                stats.set_index("XC", inplace=True)

                # NOTE: The ', names=["source", "description"]' part is not necessary, but removing it will make the output
                # different to the old method's remnants. Discuss with the team as it might cause problems to users.
                stats.columns = pd.MultiIndex.from_tuples(
                    [("BC_UMI_stats", col) for col in stats.columns],
                    names=["source", "description"],
                )

            elif file_type == "genecounts":
                # Process gene counts data and set MultiIndex
                stats = self._merge_data_by_type(stats, ["Count", "SampleID"])
                stats.set_index("SampleID", inplace=True)

                # NOTE: The ', names=["source", "description"]' part is not necessary, but removing it will make the output
                # different to the old method's remnants. Discuss with the team as it might cause problems to users.
                stats.columns = pd.MultiIndex.from_tuples(
                    [("genecounts", col) for col in stats.columns],
                    names=["source", "description"],
                )

            else:
                continue

            try:
                # Merge data
                data = pd.merge(
                    data, stats, how="left", left_index=True, right_index=True
                )
            except Exception as e:
                self._logger.error(f"Error occurred during merge operation: {e}")
                return None

        return data

    @staticmethod
    def _merge_data_by_type(
        input_data: pd.DataFrame, target_cols=["N", "RG"]
    ) -> pd.DataFrame:
        """
        Prepares and merges data based on unique 'type' values and target columns.

        Args:
            input_data (pd.DataFrame): The input data to process.
            target_cols (list): Target columns for processing, default ['N', 'RG'].

        Returns:
            pd.DataFrame: The processed and merged data.
        """

        # NOTE: Data validation checks can be added here

        data = pd.DataFrame(
            input_data[target_cols[1]].unique(), columns=[target_cols[1]]
        )

        # Process and merge data based on unique types
        for t in input_data["type"].unique():
            type_ext = input_data[input_data["type"] == t]
            type_ext = type_ext.rename(columns={target_cols[0]: t}).drop(
                columns=["type"]
            )
            data = pd.merge(data, type_ext, on=[target_cols[1]], how="left")

        data.fillna(0, inplace=True)

        return data

    # TODO: Find better name
    def _aggr_umis_from_loom(self, loom_file_path):
        """
        Processes a single loom file to aggregate UMI & read counts.

        Args:
            loom_file_path (str): Path to the loom file.

        Returns:
            pd.DataFrame: DataFrame containing UMI read counts and UMI genes detected.
        """
        try:
            with lp.connect(loom_file_path, validate=False) as umi_loom:
                # if not hasattr(umi_loom.ca, 'cell_names') or len(umi_loom.ca.cell_names) == 0:
                #     self._logger.warning(f"Loom file missing expected data: {loom_file_path}")
                #     return pd.DataFrame()  # or return None

                # Get cell barcodes
                cell_barcodes = umi_loom.ca.cell_names

                # Initialize DataFrame to store results
                umi_data = pd.DataFrame(index=cell_barcodes)

                # Accumulate data for each cell
                for cell in cell_barcodes:
                    cell_idx = (cell_barcodes == cell).nonzero()[0]
                    umi_data.loc[cell, "UMI_read_counts"] = umi_loom[:, cell_idx].sum()
                    umi_data.loc[cell, "UMI_genes_detected"] = np.count_nonzero(
                        np.asarray(umi_loom[:, cell_idx])
                    )
        except Exception as e:
            self._logger.error(f"Error processing loom file {loom_file_path}: {e}")
            return None

        # NOTE: With only casting "UMI_genes_detected" to int the data is identical to the old method
        umi_data["UMI_genes_detected"] = umi_data["UMI_genes_detected"].astype(int)
        # NOTE: Casting both (UMI_read_counts and UMI_genes_detected) to int makes sense,
        # but the data is different from the old method. Discuss with the team as it might cause problems to users.
        # umi_data = umi_data.astype(int)

        # Create MultiIndex for columns
        umi_data.columns = pd.MultiIndex.from_product(
            [["Loom"], umi_data.columns], names=["source", "description"]
        )

        return umi_data

    ###########################################################################################################################
    ### NOTE: Below methods used for metadata collection                                                                      #
    ###########################################################################################################################

    def collect_meta(self, stats):
        """
        Collect metadata for the sample, including zUMIs version, total reads, and filtered reads.

        Args:
            stats (pd.DataFrame): DataFrame containing aggregated statistics.

        Returns:
            dict: Dictionary containing metadata.
        """
        version = self.get_zumis_version(self.file_handler.zumis_log_fpath)

        total_reads = self.meta.get("total_reads", None)
        total_reads = None
        if total_reads is None:
            total_reads = "Find in project summary report"
        else:
            total_reads = f"{int(total_reads / 1000000)} M"

        filtered_reads = int(
            stats.loc[:, ("readspercell", "TotalReads")].sum(axis=0) / 1000000
        )  # in MReads
        avg_readspercell = round(filtered_reads / 384 * 1000, 2)  # in KReads

        metadata = {
            "zUMIs_version": version,
            "total_reads": total_reads,
            "filtered_reads": f"{filtered_reads} M",  # Placeholder in MReads
            "avg_readspercell": f"{avg_readspercell} K",  # Placeholder in KReads
        }
        return metadata

    @staticmethod
    def get_zumis_version(zumis_log_fpath):
        """
        Retrieves the zUMIs version from the log file of a specific sample run.

        Args:
            zumis_log_fpath (str): Path to the zUMIs log file.

        Returns:
            str: The extracted zUMIs version number, or '--' if not found or an error occurs.
        """
        try:
            with open(zumis_log_fpath) as log_file:
                for line in reversed(log_file.readlines()):
                    if "zUMIs version" in line:
                        return line.split()[-1].strip()
        except FileNotFoundError:
            self._logger.error(f"zUMIs log file not found: {zumis_log_fpath}")
        except Exception as e:
            self._logger.error(f"Error reading zUMIs log file: {e}")

        return "--"

    ###########################################################################################################################
    ### NOTE: Below methods are helping utilities                                                                             #
    ###########################################################################################################################

    @staticmethod
    def save_data(data, path):
        """
        Save the DataFrame to a CSV file.

        Args:
            data (pd.DataFrame): DataFrame to be saved.
            path (str or Path): Path to the output file.
        """
        data.to_csv(path, index=True, sep="\t")
