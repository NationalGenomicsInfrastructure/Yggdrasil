import logging
import os
from pathlib import Path


class YggdrasilUtilities:
    """
    Utility class for common functions in the Yggdrasil project.

    Provides workspace and configuration-path resolution and URL
    normalization.

    Attributes:
        DEFAULT_WORKSPACE_PATH (Path): Workspace bundled beside the source tree.
        CONFIG_DIR (Path): Default directory containing configuration files.
    """

    DEFAULT_WORKSPACE_PATH: Path = (
        Path(__file__).parent.parent.parent / "yggdrasil_workspace"
    )
    CONFIG_DIR: Path = DEFAULT_WORKSPACE_PATH / "common/configurations"

    @staticmethod
    def workspace_path() -> Path:
        """Return the root path for the Yggdrasil workspace.

        If YGG_HOME is set, it is treated as the workspace root. Otherwise,
        return the local development workspace bundled beside the source tree.
        """
        ygg_home = os.environ.get("YGG_HOME")
        if ygg_home:
            return Path(ygg_home).expanduser()

        return YggdrasilUtilities.DEFAULT_WORKSPACE_PATH

    @staticmethod
    def config_dir() -> Path:
        """Return the directory containing Yggdrasil configuration files."""
        if os.environ.get("YGG_HOME"):
            return YggdrasilUtilities.workspace_path() / "common/configurations"

        return YggdrasilUtilities.CONFIG_DIR

    @staticmethod
    def get_path(file_name: str) -> Path | None:
        """Get the full path to a specific configuration file.

        Args:
            file_name (str): The name of the configuration file.

        Returns:
            Optional[Path]: A Path object representing the full path to the specified
                configuration file, or None if the file is not found or is invalid.
        """
        # Convert to Path object
        requested_path = Path(file_name)

        # If file_name is absolute or tries to go outside config_dir, return None immediately
        if requested_path.is_absolute():
            logging.error(f"Absolute paths are not allowed: '{file_name}'")
            return None

        config_dir = YggdrasilUtilities.config_dir()

        # Construct the path within config_dir
        config_file = config_dir / requested_path

        # Check if the constructed path is still within config_dir (no directory traversal)
        try:
            # Resolve both paths to their absolute forms and ensure config_dir is a parent of config_file
            config_file_resolved = config_file.resolve()
            config_dir_resolved = config_dir.resolve()

            if config_dir_resolved not in config_file_resolved.parents:
                logging.error(
                    f"Attempted directory traversal outside config dir: '{file_name}'"
                )
                return None

            if config_file_resolved.exists():
                return config_file_resolved
            else:
                logging.error(f"Configuration file '{file_name}' not found.")
                return None
        except Exception as e:
            logging.error(f"Error resolving config file path '{file_name}': {e}")
            return None

    @staticmethod
    def normalize_url(url: str) -> str:
        if not url.startswith(("http://", "https://")):
            url = "http://" + url
        return url.rstrip("/")
