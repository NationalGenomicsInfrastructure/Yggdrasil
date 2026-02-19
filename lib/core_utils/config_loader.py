import json
import logging
import types
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from lib.core_utils.common import YggdrasilUtilities as Ygg
from lib.core_utils.ygg_session import YggSession

# NOTE: To use custom_logger resolve circular import issue


class ConfigLoader:
    """
    Configuration loader for Yggdrasil.

    This class loads and manages configuration settings from a JSON file, providing an
    immutable view of the configuration data using MappingProxyType. It allows modules
    to access configuration settings without the ability to modify them.

    Attributes:
        _config (Optional[Mapping[str, Any]]): The loaded configuration data.
    """

    _cache: dict[Path, Mapping[str, Any]] = {}

    def __init__(self) -> None:
        self._config: Mapping[str, Any] | None = None

    def __getitem__(self, key: str) -> Any:
        """
        Allow accessing configuration values using subscript notation.

        Args:
            key (str): The configuration key to retrieve the value for.

        Returns:
            The value associated with the specified key, or None if the key does not exist.
        """
        return self._config.get(key) if self._config else None

    def load_config_path(self, path: str) -> Mapping[str, Any]:
        """
        Load and validate the configuration from a JSON file given its full path.

        Args:
            path (str): The full path to the configuration JSON file.

        Returns:
            Mapping[str, Any]: The loaded configuration data.
        """
        return self._load_config(path, is_path=True)

    def load_config(self, file_name: str) -> Mapping[str, Any]:
        """
        Load and validate the configuration from a JSON file.

        Args:
            file_name (str): The name of the configuration JSON file.

        Returns:
            Mapping[str, Any]: The loaded configuration data.
        """
        return self._load_config(file_name, is_path=False)

    def _load_config(self, file_name: str, is_path: bool = False) -> Mapping[str, Any]:
        """
        Unified method that respects dev mode for both is_path=True/False.
        """
        # 1) Build the 'base_file' (path to config.json)
        base_file = Path(file_name) if is_path else Ygg.get_path(file_name)

        if base_file is None:
            self._config = types.MappingProxyType({})
            return self._config

        # 2) If dev mode is on, try the dev counterpart (dev_myfile.suffix) in the same directory
        if YggSession.is_dev():
            dev_file = base_file.with_name(f"dev_{base_file.name}")
            if dev_file.is_file():
                logging.debug(
                    f"Dev mode ON. Using dev config '{dev_file.name}' instead of '{base_file.name}'."
                )
                base_file = dev_file
            else:
                logging.debug(
                    f"Dev mode ON but no dev config found for '{base_file.name}'. "
                    f"Using the original file."
                )

        # 3) **Cache lookup**
        key = base_file.resolve()
        if key in ConfigLoader._cache:
            logging.debug(
                "Config file '%s' already loaded. Using cached version.",
                base_file.name,
            )
            self._config = ConfigLoader._cache[key]
            return self._config

        # 4) Now actually load from disk
        try:
            with open(base_file) as f:
                raw = json.load(f)
        except json.JSONDecodeError as e:
            # Set config to empty immutable mapping before raising
            self._config = types.MappingProxyType({})
            raise json.JSONDecodeError(
                f"Error parsing config file '{base_file}': {e}", e.doc, e.pos
            )
        except TypeError as e:
            # Set config to empty immutable mapping before raising
            self._config = types.MappingProxyType({})
            raise TypeError(f"Error parsing config file '{base_file}': {e}")
        except Exception as e:
            logging.error(f"Unexpected error loading config file '{base_file}': {e}")
            # Set config to empty immutable mapping before raising
            self._config = types.MappingProxyType({})
            raise

        # 5) wrap and cache
        mp = types.MappingProxyType(raw)
        ConfigLoader._cache[key] = mp
        self._config = mp

        return self._config
