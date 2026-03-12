from __future__ import annotations

import json
import os
import uuid
from pathlib import Path
from typing import Any, Protocol

from yggdrasil.flow.utils.jsonify import to_jsonable
from yggdrasil.flow.utils.ygg_time import utcnow_iso


class EventEmitter(Protocol):
    def emit(self, event: dict[str, Any]) -> None: ...


class FileSpoolEmitter:
    def __init__(self, spool_dir: str | None = None):
        # TODO: Decide between env var `YGG_EVENT_SPOOL` or reading from config
        self.root = Path(
            spool_dir or os.environ.get("YGG_EVENT_SPOOL") or "/tmp/ygg_events"
        )
        self.root.mkdir(parents=True, exist_ok=True)

    def emit(self, event: dict[str, Any]) -> None:
        event = dict(event)  # shallow copy
        event.setdefault("eid", str(uuid.uuid4()))
        event.setdefault("ts", utcnow_iso())
        hints = event.pop("_spool_path", {})
        # Build relative spool path.
        # For plan-level events (e.g. type startswith 'plan.'), omit step directory if not provided.
        realm = hints.get("realm", "unknown")
        plan_id = hints.get("plan_id", "unknown_plan")
        step_id = hints.get("step_id")
        ev_type = str(event.get("type", ""))
        if not step_id and ev_type.startswith("plan."):
            rel = Path(realm, plan_id)
        else:
            rel = Path(realm, plan_id, step_id or "unknown_step")
        run_id = hints.get("run_id")
        if run_id:
            rel = rel / run_id
        d = self.root / rel
        d.mkdir(parents=True, exist_ok=True)
        fn = hints.get("filename", f"{event['eid']}.json")
        tmp = (d / fn).with_suffix(".json.tmp")
        tmp.write_text(json.dumps(to_jsonable(event), sort_keys=True), encoding="utf-8")
        tmp.replace(d / fn)


class TeeEmitter:
    """Fan out to multiple emitters (e.g., spool + couch)."""

    def __init__(self, *emitters: EventEmitter):
        self.emitters = emitters

    def emit(self, event: dict[str, Any]) -> None:
        for em in self.emitters:
            em.emit(event)


class CouchEmitter:
    """Inline write per-event to Couch using lib/couchdb helpers."""

    def __init__(self, couch_client):  # inject your existing client/helper
        self.couch = couch_client

    def emit(self, event: dict[str, Any]) -> None:
        # TODO: Normalize as needed; then upsert a per-plan/step doc (or append to a log doc).
        # NOTE: Keep this minimal: avoid heavy transforms; projections belong to the Consumer.
        self.couch.upsert_event(event)  # call into lib/couchdb code we already have
