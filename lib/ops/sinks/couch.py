# lib/ops/sinks/couch.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from ibm_cloud_sdk_core.api_exception import ApiException
from ibmcloudant.cloudant_v1 import Document

from lib.couchdb.couchdb_connection import CouchDBHandler
from lib.couchdb.partitions import partition_key
from yggdrasil.flow.planner import PlanDraft
from yggdrasil.flow.utils.ygg_time import utcnow_iso


@dataclass
class OpsWriter(CouchDBHandler):
    """
    - write(plan_dir, snapshot): used by the FileSpoolConsumer to upsert plan_status.
    - upsert_plan_draft(draft): used by realm handlers/planners to publish a plan draft.
    """

    def __init__(self, db_name: str = "yggdrasil_ops_dev") -> None:
        super().__init__(db_name=db_name)

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

    # ---------- plan_draft (new, used by planners) ----------
    def upsert_plan_draft(self, draft: PlanDraft) -> None:
        plan = draft.plan
        part = partition_key(plan.scope or {})
        doc_id = f"{part}:plan_draft:{plan.realm}:{plan.plan_id}"

        payload: dict[str, Any] = {
            "_id": doc_id,
            "type": "plan_draft",
            "realm": plan.realm,
            "plan_id": plan.plan_id,
            "scope": plan.scope,
            "notes": draft.notes,
            "preview": draft.preview,
            "auto_run": draft.auto_run,
            "approved": False,  # flipped by a human in Genstat / ops UI
            "approvals_required": draft.approvals_required,
            "plan": {
                "plan_id": plan.plan_id,
                "realm": plan.realm,
                "scope": plan.scope,
                "steps": [s.__dict__ for s in plan.steps],
            },
            "updated_at": utcnow_iso(),
        }
        self._upsert(doc_id, payload)

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
