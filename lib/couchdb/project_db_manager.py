import json
from collections.abc import AsyncGenerator
from typing import Any, cast

from requests import Response

from lib.core_utils.common import YggdrasilUtilities as Ygg
from lib.core_utils.config_loader import ConfigLoader
from lib.core_utils.logging_utils import custom_logger
from lib.couchdb.couchdb_connection import CouchDBHandler

logging = custom_logger(__name__.split(".")[-1])

# Default environment variable names for credentials
DEFAULT_USER_ENV = "YGG_COUCH_USER"
DEFAULT_PASS_ENV = "YGG_COUCH_PASS"


def _get_couchdb_endpoint_config() -> dict[str, Any]:
    """Load CouchDB endpoint config from main.json."""
    full_config = ConfigLoader().load_config("main.json")
    external_systems = full_config.get("external_systems", {})
    endpoints = external_systems.get("endpoints", {})
    return endpoints.get("couchdb", {})


class ProjectDBManager(CouchDBHandler):
    """
    Manages interactions with the 'projects' database, such as:

      - Asynchronously fetching document changes (`fetch_changes` / `get_changes`).

    Inherits from `CouchDBHandler` to reuse the CouchDB connection.
    It is specialized for Yggdrasil needs (e.g., module registry lookups).
    """

    def __init__(
        self,
        *,
        url: str | None = None,
        user_env: str | None = None,
        pass_env: str | None = None,
    ) -> None:
        # Load config for defaults if not provided
        if url is None or user_env is None or pass_env is None:
            ep = _get_couchdb_endpoint_config()
            auth = ep.get("auth", {})
            if url is None:
                raw_url = ep.get("url")
                if not raw_url:
                    raise RuntimeError(
                        "CouchDB URL not configured. Set external_systems.endpoints.couchdb.url "
                        "in main.json or pass url= explicitly."
                    )
                url = Ygg.normalize_url(raw_url)
            if user_env is None:
                user_env = auth.get("user_env", DEFAULT_USER_ENV)
            if pass_env is None:
                pass_env = auth.get("pass_env", DEFAULT_PASS_ENV)

        assert url is not None
        assert user_env is not None
        assert pass_env is not None

        super().__init__("projects", url=url, user_env=user_env, pass_env=pass_env)
        self.module_registry = ConfigLoader().load_config("module_registry.json")

    async def fetch_changes(self) -> AsyncGenerator[tuple[dict[str, Any], str], None]:
        """Fetches document changes from the database asynchronously.

        Yields:
            Tuple[Dict[str, Any], str]: A tuple containing the document and module location.
        """
        last_processed_seq: str | None = None

        while True:
            async for change in self.get_changes(last_processed_seq=last_processed_seq):
                try:
                    method = change["details"]["library_construction_method"]
                    module_config = self.module_registry.get(method)

                    if module_config:
                        module_loc = module_config["module"]
                        yield (change, module_loc)
                    else:
                        # Check for prefix matches
                        for registered_method, config in self.module_registry.items():
                            if config.get("prefix") and method.startswith(
                                registered_method
                            ):
                                module_loc = config["module"]
                                yield (change, module_loc)
                                break
                        else:
                            # The majority of the tasks will not have a module configured.
                            # If you log this, expect to see many messages!
                            # logging.warning(f"No module configured for task type '{method}'.")
                            pass
                except Exception as e:  # noqa: F841
                    # logging.error(f"Error processing change: {e}")
                    pass

    async def get_changes(
        self, last_processed_seq: str | None = None
    ) -> AsyncGenerator[dict[str, Any], None]:
        """
        Fetch and yield document changes from a CouchDB database.

        Args:
            last_processed_seq (Optional[str]): The sequence number from which to start
                monitoring changes.

        Yields:
            Dict[str, Any]: A document representing a change.
        """
        if last_processed_seq is None:
            last_processed_seq = Ygg.get_last_processed_seq()

        response = self.server.post_changes_as_stream(
            db=self.db_name,
            feed="continuous",
            since=last_processed_seq,
            include_docs=False,
        ).get_result()

        # Type assertion: we expect a Response object for streaming
        changes = cast(Response, response)  # Makes Pylance happy

        for line in changes.iter_lines():
            # Reduce nesting / skip empty lines
            if not line:
                continue

            change = json.loads(line)

            # Only process real change entries
            if "id" not in change or "seq" not in change:
                continue

            try:
                doc = self.fetch_document_by_id(change["id"])
                last_processed_seq = change["seq"]
                if last_processed_seq is not None:
                    Ygg.save_last_processed_seq(last_processed_seq)
                else:
                    logging.warning(
                        "Received `None` for last_processed_seq. Skipping save."
                    )

                if doc is not None:
                    yield doc
                else:
                    logging.warning(f"Document with ID {change['id']} is None.")
            except Exception as e:
                logging.warning(f"Error processing change: {e}")
                logging.debug(f"Data causing the error: {change}")
