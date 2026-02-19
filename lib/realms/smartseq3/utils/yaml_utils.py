from pathlib import Path

from ruamel.yaml import YAML

from lib.core_utils.logging_utils import custom_logger

logger = custom_logger(__name__)

yaml = YAML()
yaml.width = 200
yaml.preserve_quotes = True


def parse_yaml(file_path):
    """
    Parse a YAML file.

    Args:
        file_path (str): The path to the YAML file to be parsed.

    Returns:
        dict: The parsed YAML content as a dictionary.
    """
    return yaml.load(Path(file_path))


def write_yaml(config, args) -> bool:
    """
    Write data to a YAML file based on a template and provided arguments.

    This function reads a YAML template, fills it with values from `args`,
    and writes the output to a specified file. It handles sequence files,
    reference paths, output directories, and barcodes.

    Args:
        config (dict): Configuration dictionary containing the path to the YAML template.
        args (dict): Arguments to be filled into the template, including:
            - plate: Project name or identifier.
            - fastqs: Dictionary with paths to FASTQ files (R1, R2, I1, I2).
            - read_setup: Dictionary with base definitions for each read type.
            - ref: Dictionary with paths to reference genome files (gen_path, gtf_path).
            - outdir: Output directory path.
            - bc_file: Path to the barcode file.
            - out_yaml: Path to the output YAML file.
    """
    template = parse_yaml(config["yaml_template"])

    template["project"] = args["plate"]
    template["sequence_files"]["file1"]["name"] = str(args["fastqs"]["R1"])
    template["sequence_files"]["file2"]["name"] = str(args["fastqs"]["R2"])
    template["sequence_files"]["file3"]["name"] = str(args["fastqs"]["I1"])
    template["sequence_files"]["file4"]["name"] = str(args["fastqs"]["I2"])

    # Define the base definition according to read setup
    template["sequence_files"]["file1"]["base_definition"][0] = args["read_setup"][
        "R1"
    ][0]
    template["sequence_files"]["file1"]["base_definition"][1] = args["read_setup"][
        "R1"
    ][1]
    template["sequence_files"]["file2"]["base_definition"][0] = args["read_setup"]["R2"]
    template["sequence_files"]["file3"]["base_definition"] = args["read_setup"]["I1"]
    template["sequence_files"]["file4"]["base_definition"] = args["read_setup"]["I2"]

    template["reference"]["STAR_index"] = str(args["ref"]["gen_path"])
    template["reference"]["GTF_file"] = str(args["ref"]["gtf_path"])
    template["out_dir"] = str(args["outdir"])
    template["barcodes"]["barcode_file"] = str(args["bc_file"])

    # TODO: Do not exit. Notify user (through Slack?).
    # TODO: Make a backup of the existing file (e.g. by appending a timestamp, or suffixing _old#)
    if args["out_yaml"].is_file():
        logger.warning(f"YAML file `{args['out_yaml'].name}` already exists.")
        logger.debug(f"Path: {args['out_yaml']}")
        logger.warning("Continuing to overwrite the file...")

    try:
        with open(args["out_yaml"], "w") as outfile:
            yaml.dump(template, outfile)
        logger.debug(f"YAML file written successfully at {args['out_yaml']}")
        return True
    except Exception as e:
        logger.error(f"Failed to write YAML file {args['out_yaml']}: {e}")
        return False
