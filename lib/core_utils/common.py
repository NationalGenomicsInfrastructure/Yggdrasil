import importlib
import logging
import os
from pathlib import Path
from typing import Any


class YggdrasilUtilities:
    """
    Utility class for common functions in the Yggdrasil project.

    Provides utility functions used across various modules, including
    configuration handling, module loading, and environment variable access.

    Attributes:
        module_cache (Dict[str, Any]): Cache for loaded modules and classes.
        CONFIG_DIR (Path): Directory containing configuration files.
    """

    module_cache: dict[str, Any] = {}
    CONFIG_DIR: Path = (
        Path(__file__).parent.parent.parent
        / "yggdrasil_workspace/common/configurations"
    )

    @staticmethod
    def load_realm_class(module_path: str) -> type | None:
        """
        Load a realm class from a module path and cache it for reuse.

        The module path should include the full path to the class in the format
        'module.submodule.ClassName'.

        Args:
            module_path (str): The full path of the realm class to load (including the class name).

        Returns:
            Optional[Type]: The loaded realm class, or None if loading fails.
        """
        if module_path in YggdrasilUtilities.module_cache:
            return YggdrasilUtilities.module_cache[module_path]

        try:
            module_name, class_name = module_path.rsplit(".", 1)
            module = importlib.import_module(module_name)
            realm_class = getattr(module, class_name)
            YggdrasilUtilities.module_cache[module_path] = realm_class
            return realm_class
        except (ImportError, AttributeError) as e:
            logging.error(f"Failed to load realm class from '{module_path}': {e}")
            return None

    @staticmethod
    def load_module(module_path: str) -> Any | None:
        """Load a module and cache it for reuse.

        Args:
            module_path (str): The path of the module to load.

        Returns:
            Optional[Any]: The loaded module, or None if loading fails.
        """
        if module_path in YggdrasilUtilities.module_cache:
            return YggdrasilUtilities.module_cache[module_path]

        try:
            task_module = importlib.import_module(module_path)
            YggdrasilUtilities.module_cache[module_path] = task_module
            return task_module
        except ImportError as e:
            logging.error(f"Failed to load module '{module_path}': {e}")
            return None

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

        # If file_name is absolute or tries to go outside CONFIG_DIR, return None immediately
        if requested_path.is_absolute():
            logging.error(f"Absolute paths are not allowed: '{file_name}'")
            return None

        # Construct the path within CONFIG_DIR
        config_file = YggdrasilUtilities.CONFIG_DIR / requested_path

        # Check if the constructed path is still within CONFIG_DIR (no directory traversal)
        try:
            # Resolve both paths to their absolute forms and ensure CONFIG_DIR is a parent of config_file
            config_file_resolved = config_file.resolve()
            config_dir_resolved = YggdrasilUtilities.CONFIG_DIR.resolve()

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
    def env_variable(variable_name, default=None):
        """
        Get the value of an environment variable.

        Args:
            variable_name (str): The name of the environment variable.
            default (Optional[str], optional): Default value if the environment variable
                is not set. Defaults to None.

        Returns:
            Optional[str]: The value of the environment variable or the default value.
        """
        return os.environ.get(variable_name, default)

    @staticmethod
    def get_last_processed_seq() -> str:
        """Retrieve the last processed sequence number from a file.

        Returns:
            str: The last processed sequence number.
        """
        seq_file = YggdrasilUtilities.get_path(".last_processed_seq")

        if seq_file and seq_file.is_file():
            with open(seq_file) as file:
                content = file.read().strip()
                # If the file is empty, return "0"
                return content if content else "0"
        else:
            # Otherwise return a default sequence value of your choice.
            # NOTE: Zero (0) means start from the beginning. Note ideal!
            # TODO: Read default sequence value from configuration file.
            default_since = "0"
            return default_since

    @staticmethod
    def save_last_processed_seq(last_processed_seq: str) -> None:
        """Save the last processed sequence number to a file.

        Args:
            last_processed_seq (str): The last processed sequence number to save.
        """
        seq_file = YggdrasilUtilities.get_path(".last_processed_seq")

        if seq_file:
            try:
                with open(seq_file, "w") as file:
                    file.write(last_processed_seq)
            except Exception as e:
                logging.error(f"Failed to save last processed seq: {e}")
                # Don't re-raise, just log and exit the method gracefully
        else:
            logging.warning(
                "Failed to save last processed seq:"
                "'.last_processed_seq' File not found in the configurations."
            )
            pass

    @staticmethod
    def normalize_url(url: str) -> str:
        if not url.startswith(("http://", "https://")):
            url = "http://" + url
        return url.rstrip("/")
