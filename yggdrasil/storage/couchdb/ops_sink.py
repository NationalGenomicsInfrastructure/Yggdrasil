# yggdrasil/storage/couchdb/ops_sink.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from ibm_cloud_sdk_core.api_exception import ApiException
from ibmcloudant.cloudant_v1 import Document

from yggdrasil.config.external_systems import (
    DEFAULT_PASS_ENV,
    DEFAULT_USER_ENV,
    normalize_url,
)
from yggdrasil.config.loader import ConfigLoader
from yggdrasil.couchdb.connection import CouchDBHandler
from yggdrasil.storage.partitions import partition_key


def _get_couchdb_endpoint_config() -> dict[str, Any]:
    """Load CouchDB endpoint config from main.json."""
    full_config = ConfigLoader().load_config("main.json")
    external_systems = full_config.get("external_systems", {})
    endpoints = external_systems.get("endpoints", {})
    return endpoints.get("couchdb", {})


@dataclass
class OpsWriter(CouchDBHandler):
    """
    - write(plan_dir, snapshot): used by the FileSpoolConsumer to upsert plan_status.
    """

    def __init__(
        self,
        db_name: str = "yggdrasil_ops_dev",
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
                url = normalize_url(raw_url)
            if user_env is None:
                user_env = auth.get("user_env", DEFAULT_USER_ENV)
            if pass_env is None:
                pass_env = auth.get("pass_env", DEFAULT_PASS_ENV)

        assert url is not None
        assert user_env is not None
        assert pass_env is not None

        super().__init__(db_name, url=url, user_env=user_env, pass_env=pass_env)

    # ---------- plan_status (existing path) ----------
    def write(self, plan_dir: Path, snapshot: dict[str, Any]) -> None:
        """Upsert a plan_status snapshot into Couch. `_plan_dir` unused (kept for interface parity)."""
        doc_id = self._doc_id_status(snapshot)
        payload = dict(snapshot)
        payload["_id"] = doc_id
        self._upsert(doc_id, payload)

    def _doc_id_status(self, snapshot: dict[str, Any]) -> str:
        part = partition_key(snapshot.get("scope") or {})
        realm = snapshot["realm"]
        plan_id = snapshot["plan_id"]
        return f"{part}:plan_status:{realm}:{plan_id}"

    # ---------- shared upsert ----------
    def _upsert(self, doc_id: str, payload: dict[str, Any]) -> None:
        # get current _rev if any
        rev: str | None = None
        try:
            current = self.server.get_document(
                db=self.db_name, doc_id=doc_id
            ).get_result()
            if isinstance(current, dict):
                rev = cast(str | None, current.get("_rev"))
        except ApiException as e:
            if e.code != 404:
                raise
        if rev:
            payload["_rev"] = rev

        doc = Document.from_dict(payload)
        try:
            self.server.put_document(db=self.db_name, doc_id=doc_id, document=doc)
        except ApiException as e:
            if e.code != 409:
                raise
            # simple conflict retry
            current = self.server.get_document(
                db=self.db_name, doc_id=doc_id
            ).get_result()
            new_rev = cast(
                str | None, current.get("_rev") if isinstance(current, dict) else None
            )
            if not new_rev:
                raise
            payload["_rev"] = new_rev
            doc = Document.from_dict(payload)
            self.server.put_document(db=self.db_name, doc_id=doc_id, document=doc)
