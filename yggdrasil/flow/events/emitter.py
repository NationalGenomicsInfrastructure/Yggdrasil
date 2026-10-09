from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any, Protocol

from yggdrasil.config.runtime_paths import resolve_event_spool
from yggdrasil.flow.events.attempt_records import attempt_dir, step_events_dir
from yggdrasil.flow.utils.jsonify import to_jsonable
from yggdrasil.flow.utils.ygg_time import utcnow_iso


class EventEmitter(Protocol):
    def emit(self, event: dict[str, Any]) -> None: ...


class FileSpoolEmitter:
    def __init__(self, spool_dir: str | Path | None = None):
        # Default resolution ($YGG_EVENT_SPOOL → mode default) is centralized
        # in yggdrasil.config.runtime_paths.
        self.root = Path(spool_dir) if spool_dir else resolve_event_spool()
        self.root.mkdir(parents=True, exist_ok=True)

    def emit(self, event: dict[str, Any]) -> None:
        """Write one event as a JSON file under the spool root.

        The event's ``_spool_path`` hints decide where. With an
        ``execution_id``, the event belongs to that attempt and goes to its
        directory (see :mod:`yggdrasil.flow.events.attempt_records`): a
        plan-level record directly in it, a step's event in that step's
        directory. Without one, the older layout applies:
        ``<realm>/<plan_id>/<step_id>/<run_id>/``, or the plan's directory for
        a plan-level event without a step.

        Args:
            event: The event; not modified.

        Raises:
            ValueError: If the ``execution_id`` hint cannot name a directory.
            OSError: If the file cannot be written.
        """
        event = dict(event)  # shallow copy
        event.setdefault("eid", str(uuid.uuid4()))
        event.setdefault("ts", utcnow_iso())
        hints = event.pop("_spool_path", {})
        realm = hints.get("realm", "unknown")
        plan_id = hints.get("plan_id", "unknown_plan")
        step_id = hints.get("step_id")
        execution_id = hints.get("execution_id")
        if execution_id is not None:
            d = attempt_dir(self.root, realm, plan_id, execution_id)
            if step_id:
                d = step_events_dir(d, step_id)
        else:
            # For plan-level events (e.g. type startswith 'plan.'), omit the
            # step directory if not provided.
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
    """Inline write per-event to Couch using the yggdrasil/couchdb helpers."""

    def __init__(self, couch_client):  # inject your existing client/helper
        self.couch = couch_client

    def emit(self, event: dict[str, Any]) -> None:
        # TODO: Normalize as needed; then upsert a per-plan/step doc (or append to a log doc).
        # NOTE: Keep this minimal: avoid heavy transforms; projections belong to the Consumer.
        self.couch.upsert_event(
            event
        )  # call into yggdrasil/couchdb code we already have
