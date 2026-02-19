from io import BytesIO
from pathlib import Path

import pandas as pd
from pdf2image import convert_from_path
from PIL import Image as PILImage
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import cm
from reportlab.lib.utils import ImageReader
from reportlab.platypus import Image, PageBreak, Paragraph, Spacer

from lib.core_utils.logging_utils import custom_logger

logger = custom_logger(__name__)


def get_image(path, width=1 * cm, **kwargs):
    """
    Create a ReportLab Image object with the correct aspect ratio.

    Args:
        path (str): Path to the image file.
        width (float): Width of the image in cm.

    Returns:
        Image: ReportLab Image object.
    """
    img = ImageReader(path)
    iw, ih = img.getSize()
    aspect = ih / float(iw)
    return Image(path, width=width, height=(width * aspect), **kwargs)


def get_image_from_bytes(buf, width=1 * cm, **kwargs):
    """
    Create a ReportLab Image object from a BytesIO buffer with the correct aspect ratio.

    Args:
        buf (BytesIO): BytesIO buffer containing the image data.
        width (float): Width of the image in cm.

    Returns:
        Image: ReportLab Image object.
    """
    # Load the image from BytesIO object
    pil_image = PILImage.open(buf)

    # Get image size and calculate aspect ratio
    iw, ih = pil_image.size
    aspect = ih / float(iw)

    # Calculate the height based on the aspect ratio
    height = width * aspect

    # Reset buffer position
    buf.seek(0)

    # Create and return the ReportLab Image
    return Image(buf, width=width, height=height, **kwargs)


def add_figures(report, style, info):
    """
    Add figures to the report.

    Args:
        report (list): List to which report elements are appended.
        style (StyleSheet1): The stylesheet used for formatting the report.
        info (dict): Dictionary containing figure information.

    Returns:
        list: Updated report list with figures added.
    """
    for key, info in info.items():
        # Add figure title
        title = f'<font style = "font-family:Lato" size=12><b>{info["title"]}</b><br/></font>'
        report.append(
            Paragraph(title, ParagraphStyle(name="centered", alignment=TA_CENTER))
        )

        # Add some space
        report.append(Spacer(1, 5))

        # Check if the source is a BytesIO object or a file path
        if isinstance(info["source"], BytesIO):
            # Load image from BytesIO
            buf = info["source"]
        elif isinstance(info["source"], Path):
            # Convert PDF page to image
            pil_image = convert_from_path(info["source"], info["size"][0])[0]
            buf = BytesIO()
            pil_image.save(buf, format="PNG")
            buf.seek(0)
        else:
            logger.error(f"Unrecognized source type: {type(info['source'])}")
            logger.debug(info["source"])
            continue

        # Add image to report using aspect ratio
        report.append(
            get_image_from_bytes(buf, width=info["size"][1] * cm, hAlign="CENTER")
        )

        # Add space and legend
        report.append(Spacer(1, 10))
        parts = info["legend"].split("\n")
        for part in parts:
            text = f'<font style = "font-family:Lora" size=8>{part}</font>'
            report.append(Paragraph(text, style["Justify"]))
            report.append(Spacer(1, 5))

        # Add a page break after each figure
        report.append(PageBreak())

    return report


# NOTE: Replaced by the get_zumis_version in the ss3_data_collector.py
# TODO: Fix and use this to automatically get zUMIs' version
# def get_zumis_version(project_dir, sample_plate):
#     """
#     Returns the zUMIs version that the given plate was run on.

#     Args:
#         project_dir (Path): The project directory.
#         sample_plate (str): The sample plate identifier.

#     Returns:
#         str: The zUMIs version.
#     """
#     # TODO: Try-Except in case the folder or file does not exist

#     sample_zumis_log_path = Path(project_dir / sample_plate / f"{sample_plate}.zUMIs_runlog.txt")

#     log_file = open(sample_zumis_log_path, 'r')
#     lines = log_file.readlines()
#     version = "--"

#     for line in reversed(lines):
#         if "zUMIs version" in line:
#             version = line.split()[-1]

#     log_file.close()

#     return version


def get_bc_wells(bc_set, bc_file, reagent="1.5"):
    """
    Extract well IDs corresponding to a given barcode set.

    Args:
        bc_set (str): The barcode set to use for extracting well IDs (e.g., 1A).
        bc_file (str): Path to the CSV file containing barcode and well ID mappings.
        reagent (str, optional): The reagent version used, defaults to '1.5'.

    Returns:
        pd.Series: Series where the index is barcodes and the values are corresponding well IDs.
    """
    if reagent == "1.5":
        target_col = "XC"
    elif reagent == "1.0":
        target_col = "XC_NovaSeq"
    else:
        logger.error(
            f"No recognized reagent version: {reagent}. Currently supported versions: 1.0, 1.5"
        )
        # TODO: Handle this error properly and possibly raise an exception

    bc = pd.read_csv(bc_file, sep=",")
    bc = bc.set_index(target_col)
    return bc.loc[bc.loc[:, "BCset"] == bc_set, "WellID"]


def prepare_data(input_data, bc_wells, target_cols=["N", "RG"]) -> pd.DataFrame:
    """
    Prepare data for analysis by merging based on target columns.

    Args:
        input_data (pd.DataFrame): Input data to be prepared.
        bc_wells (pd.Series): Barcode wells information.
        target_cols (list): List of target columns for merging.

    Returns:
        pd.DataFrame: Prepared data.
    """
    data = pd.DataFrame()

    # TODO: the loop is not correct. merge or join by using the barcode as indices
    # Could use the bc_wells barcodes, just to prevent missing barcodes when taken from `input_data`?
    for t in input_data["type"].unique():
        type_ext = input_data[input_data["type"] == t]

        type_ext = type_ext.rename(columns={target_cols[0]: t}).drop(columns=["type"])

        if data.empty:
            data[target_cols[1]] = input_data[target_cols[1]].unique()
        data = pd.merge(data, type_ext, on=[target_cols[1]], how="left")

    data = data.fillna(0)

    # # Add a column with the WellID in respect to barcodes
    # data['WellID'] = data[target_cols[1]].map(bc_wells)
    # # Create a categorical order in the WellID col, using natsort
    # data['WellID'] = pd.Categorical(data['WellID'], ordered=True, categories= ns.natsorted(data['WellID'].unique()))
    # # Sort (natural sort) data on WellID. [A1,A2,A3,...,A10,A11,...,A24,B1,...,P24]
    # data = data.sort_values('WellID', ignore_index=True)

    return data


def check_high_nan_percentage(data, threshold_percent):
    """
    Checks if the percentage of NaN values in a DataFrame exceeds a specified threshold.

    Args:
        data (pd.DataFrame): DataFrame to be checked for NaN values.
        threshold_percent (float): Threshold percentage of NaN values to check against.

    Returns:
        float or bool: The percentage of NaN values if it exceeds the threshold, otherwise False.
    """
    nan_count = data.isna().sum().sum()  # Total number of NaNs
    total_elements = data.size
    nan_percentage = (nan_count / total_elements) * 100

    if nan_percentage > threshold_percent:
        return nan_percentage

    return False
