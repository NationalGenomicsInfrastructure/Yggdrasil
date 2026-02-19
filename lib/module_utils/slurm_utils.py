from pathlib import Path

from lib.core_utils.logging_utils import custom_logger

logger = custom_logger(__name__)


def generate_slurm_script(
    args_dict: dict[str, str],
    template_fpath: str | Path,
    output_fpath: str | Path,
) -> bool:
    """Generate a Slurm batch script by filling in placeholders in a template.

    This function reads a Slurm script template, replaces placeholders with
    values provided in `args_dict`, and writes the resulting script to `output_fpath`.

    Args:
        args_dict (Dict[str, str]): A dictionary of arguments and their values to be
            inserted into the template.
        template_fpath (Union[str, Path]): The filepath to the Slurm script template.
        output_fpath (Union[str, Path]): The filepath where the generated Slurm script will be saved.

    Returns:
        bool: True if the script is successfully written, False otherwise.
    """
    if not isinstance(template_fpath, (str, Path)):
        raise TypeError("template_fpath must be a string or a Path object")
    if not isinstance(output_fpath, (str, Path)):
        raise TypeError("output_fpath must be a string or a Path object")

    try:
        template_path = Path(template_fpath)
        output_path = Path(output_fpath)

        with template_path.open("r") as template_file:
            template = template_file.read()

        script_content = template.format(**args_dict)

        with output_path.open("w") as script_file:
            script_file.write(script_content)

        logger.debug(f"Slurm script generated successfully at {output_fpath}")
        return True
    except FileNotFoundError as e:
        logger.error(f"Template file not found: {e}")
    except KeyError as e:
        logger.error(f"Missing placeholder in args_dict: {e}")
    except Exception as e:
        logger.error(f"Failed to generate Slurm script: {e}")
    return False
