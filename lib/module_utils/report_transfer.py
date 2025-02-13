import subprocess
from pathlib import Path
from typing import Optional

from lib.core_utils.config_loader import configs
from lib.core_utils.logging_utils import custom_logger

logging = custom_logger(__name__.split(".")[-1])


def transfer_report(
    report_path: Path,
    project_id: str,
    sample_id: Optional[str] = None,
    destination_filename: Optional[str] = None,
) -> bool:
    try:
        report_transfer_config = configs["report_transfer"]
        if not isinstance(report_transfer_config, dict):
            logging.error(
                "Invalid configuration type for 'report_transfer'. Expected a dictionary."
            )
            return False

        server = report_transfer_config["server"]
        user = report_transfer_config["user"]
        destination_path = report_transfer_config["destination"]
        ssh_key = report_transfer_config.get("ssh_key")
    except KeyError as e:
        missing_key = e.args[0]
        logging.error(f"Missing configuration for report transfer: '{missing_key}'")
        logging.warning("Report transfer will not be attempted. Handle manually...")
        return False

    # Build the remote directory path
    remote_dir = f"{destination_path}/{project_id}"
    if sample_id:
        remote_dir = f"{remote_dir}/{sample_id}"

    if destination_filename:
        remote_path = f"{user}@{server}:{remote_dir}/{destination_filename}"
    else:
        remote_path = f"{user}@{server}:{remote_dir}/"

    rsync_command = [
        "rsync",
        "-avz",
        "--rsync-path",
        f"mkdir -p '{remote_dir}' && rsync",
        "-e",
        f"ssh -i {ssh_key}" if ssh_key else "ssh",
        str(report_path),
        remote_path,
    ]

    # logging.debug(f"RSYNC command: {' '.join(rsync_command)}")
    result = None
    try:
        # Execute the rsync command
        result = subprocess.run(
            rsync_command,
            check=True,
            text=True,
            capture_output=True,
        )

        logging.info(f"Report transferred successfully to:\n> {server} : {remote_dir}")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to transfer report:\n{e.stderr.strip()}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error during report transfer: {e}")
        # Check if result is not None before accessing its attributes
        if result is not None:
            logging.error(f"RSYNC output: {result.stdout}")
        else:
            logging.error("RSYNC output: No output available due to early error.")
        return False
