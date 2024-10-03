import json
from typing import Any, Dict, List, Optional

from lib.core_utils.common import YggdrasilUtilities as Ygg

from lib.core_utils.logging_utils import custom_logger

logging = custom_logger(__name__.split(".")[-1])


class TenXUtils:
    """Utility class for TenX processing."""

    @staticmethod
    def load_decision_table(file_name: str) -> List[Dict[str, Any]]:
        """
        Load the decision table JSON file.

        Args:
            file_name (str): The name of the decision table JSON file.

        Returns:
            List[Dict[str, Any]]: The loaded decision table as a list of dictionaries.
                Empty list if the file is not found or an error occurs.
        """
        config_file = Ygg.get_path(file_name)
        if config_file is None:
            logging.error(f"Decision table file '{file_name}' not found.")
            return []

        try:
            with open(config_file, "r") as f:
                decision_table = json.load(f)
                if not isinstance(decision_table, list):
                    logging.error(f"Decision table '{file_name}' is not a list.")
                    return []
                return decision_table
        except json.JSONDecodeError as e:
            logging.error(f"Error parsing decision table '{file_name}': {e}")
            return []
        except Exception as e:
            logging.error(f"Unexpected error loading decision table '{file_name}': {e}")
            return []
        
    @staticmethod
    def get_pipeline_info(
        library_prep_method: str,
        features: List[str]
    ) -> Optional[Dict[str, Any]]:
        """Get pipeline information based on library prep method and features.

        Args:
            library_prep_method (str): The library prep method.
            features (List[str]): List of features associated with the sample.

        Returns:
            Optional[Dict[str, Any]]: A dictionary containing pipeline information if found,
                None otherwise.
        """
        for entry in TenXUtils.load_decision_table("10x_decision_table.json"):
            if (
                entry.get("library_prep_method") == library_prep_method
                and
                set(entry.get("features", [])) == set(features)
            ):
                return entry
        logging.warning(
            f"No pipeline information found for library_prep_method '{library_prep_method}' "
            f"and features '{features}'."
        )
        return None