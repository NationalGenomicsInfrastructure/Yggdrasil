"""Shared CouchDB endpoint/default resolution helpers.

Centralizes endpoint config parsing from ``main.json`` and default credential
environment variable names so DB managers can reuse one consistent path.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from lib.core_utils.common import YggdrasilUtilities as Ygg
from lib.core_utils.config_loader import ConfigLoader

DEFAULT_USER_ENV = "YGG_COUCH_USER"
DEFAULT_PASS_ENV = "YGG_COUCH_PASS"
DEFAULT_ENDPOINT = "couchdb"


@dataclass(frozen=True)
class CouchDBParams:
    """Resolved CouchDB connection parameters for a manager/handler."""

    url: str
    user_env: str
    pass_env: str


def resolve_couchdb_params(
    *,
    endpoint: str = DEFAULT_ENDPOINT,
    url: str | None = None,
    user_env: str | None = None,
    pass_env: str | None = None,
    cfg: dict[str, Any] | None = None,
) -> CouchDBParams:
    """Resolve CouchDB URL and credential env-var names.

    Resolution order:
    1) Explicit function args
    2) ``external_systems.endpoints.<endpoint>`` from ``main.json``
    3) Built-in env var defaults for username/password names

    URL is normalized via ``Ygg.normalize_url`` and credential env-var names
    are intentionally not validated here; validation remains fail-fast in
    ``CouchDBClientFactory`` at client-creation time.
    """

    config = cfg or ConfigLoader().load_config("main.json")
    endpoint_cfg = (
        config.get("external_systems", {}).get("endpoints", {}).get(endpoint, {})
    )

    if not endpoint_cfg and (url is None or user_env is None or pass_env is None):
        raise RuntimeError(
            "Missing CouchDB endpoint config: " f"external_systems.endpoints.{endpoint}"
        )

    auth = endpoint_cfg.get("auth", {})

    resolved_url = url or endpoint_cfg.get("url")
    if not resolved_url:
        raise RuntimeError(
            "Missing url for endpoint "
            f"'{endpoint}' (external_systems.endpoints.{endpoint}.url)"
        )

    return CouchDBParams(
        url=Ygg.normalize_url(str(resolved_url)),
        user_env=(user_env or auth.get("user_env") or DEFAULT_USER_ENV),
        pass_env=(pass_env or auth.get("pass_env") or DEFAULT_PASS_ENV),
    )
