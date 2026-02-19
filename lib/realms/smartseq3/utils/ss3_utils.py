# import logging


# def collect_flowcell_ids(metadata):
#     """
#     Extracts and returns a list of flowcell IDs from the sample metadata.

#     Args:
#         metadata (dict): The sample metadata containing library_prep information.

#     Returns:
#         list: A list of flowcell IDs.
#     """
#     try:
#         flowcell_ids = []
#         library_prep = metadata.get('library_prep', {})
#         if not library_prep:
#             logger.warning("No library_prep information found in metadata.")
#         for prep_info in library_prep.values():
#             if 'sequenced_fc' in prep_info:
#                 flowcell_ids.extend(prep_info['sequenced_fc'])
#             else:
#                 logger.warning("sequenced_fc not found in library_prep.")
#         return flowcell_ids
#     except Exception as e:
#         logger.error(f"Error while collecting flowcell IDs: {e}")
#         return []
from datetime import datetime

import pandas as pd

from lib.core_utils.logging_utils import custom_logger

logger = custom_logger(__name__)


class SS3Utils:
    """
    Utility class for SmartSeq3-related operations, including sequence setup transformations,
    flowcell date parsing, barcode file creation, and well ID extraction.
    """

    @staticmethod
    def transform_seq_setup(seq_setup_str):
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

    @staticmethod
    def parse_fc_date(flowcell_id):
        """
        Parses the date from a flowcell ID, considering different date formats.

        Args:
            flowcell_id (str): The flowcell ID to parse.

        Returns:
            datetime.date: A date object representing the date of the flowcell or None if parsing fails.
        """
        date_formats = ["%Y%m%d", "%y%m%d"]
        date_str = flowcell_id.split("_")[0]
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        logger.error(f"Could not parse date for flowcell {flowcell_id}.")
        return None

    @staticmethod
    def create_barcode_file(bc_set, bc_lookup_fpath, save_as):
        """
        Creates a barcode file from specified well IDs.

        This function extracts well IDs for a given barcode set, formats them,
        and saves them to a specified file. It handles errors gracefully and logs the outcome.

        Args:
            bc_set (str): The barcode set identifier.
            bc_lookup_fpath (Path or str): The file path to the barcode lookup file.
            save_as (Path or str): The path where the barcode file should be saved.

        Returns:
            bool: True if the file was created successfully, False otherwise.
        """
        try:
            # Attempt to extract and reset index for barcode data
            bc = SS3Utils.extract_well_ids(bc_set, bc_lookup_fpath)
            if bc is None:
                logger.error("Failed to extract well IDs.")
                return False
            bc = bc.reset_index()

            # Attempt to save the barcode data to file
            bc["XC"].to_csv(save_as, index=False, header=False)
            logger.info("Barcode file created successfully.")
            return True
        except Exception as e:
            logger.error(f"Failed to create barcode file: {e}")
            return False

    @staticmethod
    def extract_well_ids(barcode_set, barcode_lookup_fpath, reagent="1.5"):
        """
        Extracts well IDs corresponding to a given barcode set.

        Args:
            barcode_set (str): The barcode set to use for extracting well IDs (e.g., 1A).
            barcode_lookup_fpath (str): Path to the CSV file containing barcode and well ID mappings.
            reagent (str, optional): The reagent version used, defaults to '1.5'.

        Returns:
            pandas.Series: A Series where the index is barcodes and the values are corresponding well IDs.

        The function reads the barcode lookup CSV file and filters the data to get well IDs for the specified barcode set.
        If an unsupported reagent version is provided, it logs a warning and defaults to using '1.5'.
        """
        # Determine the target column based on reagent version
        if reagent == "1.5":
            target_col = "XC"
        elif reagent == "1.0":
            target_col = "XC_NovaSeq"
        else:
            logger.warning(
                f"Unsupported reagent version '{reagent}'. Using default '1.5'."
            )
            target_col = "XC"

        try:
            # Read the CSV file
            bc_data = pd.read_csv(barcode_lookup_fpath, sep=",")
        except Exception as e:
            logger.error(f"Error reading barcode lookup file: {e}")
            return None

        # Filter data to get well IDs for the specified barcode set
        # Set the target column as index for efficient lookup
        bc_data.set_index(target_col, inplace=True)
        well_ids = bc_data.loc[bc_data.loc[:, "BCset"] == barcode_set, "WellID"]

        return well_ids
