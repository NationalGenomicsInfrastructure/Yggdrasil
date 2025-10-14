from __future__ import annotations

import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from lib.core_utils.logging_utils import custom_logger
from yggdrasil.flow.utils.ygg_time import utcnow_iso

logging = custom_logger(__name__.split(".")[-1])

PlanFilter = Callable[[str, str], bool] | None


@dataclass
class FileSpoolConsumer:
    spool_root: Path
    writer: Any  # SnapshotWriter, CouchWriter, etc.
    filt: PlanFilter = None

    def consume(self) -> None:
        root = self.spool_root
        if not root.exists():
            return
        for realm_dir in (p for p in root.iterdir() if p.is_dir()):
            realm = realm_dir.name
            for plan_dir in (p for p in realm_dir.iterdir() if p.is_dir()):
                plan_id = plan_dir.name
                if self.filt and not self.filt(realm, plan_id):
                    continue
                snapshot = build_plan_snapshot(plan_dir, realm, plan_id)
                if not snapshot.get("scope"):
                    logging.warning(
                        f"Missing scope in plan.json for {realm}/{plan_id}; skipping..."
                    )
                    continue
                self.writer.write(plan_dir, snapshot)

    # NOTE: For dev purposes, it keeps on consuming
    def follow(self, interval_sec: float = 2.0) -> None:
        while True:
            self.consume()
            time.sleep(interval_sec)


def _safe_load(p: Path) -> dict[str, Any]:
    try:
        return json.loads(p.read_text())
    except Exception:
        return {}


def _find_any_event(plan_spool_dir: Path) -> dict[str, Any] | None:
    # Walk: <spool>/<realm>/<plan_id>/<step_id>/<run_id>/*.json
    for step_dir in (p for p in plan_spool_dir.iterdir() if p.is_dir()):
        for run_dir in (r for r in step_dir.iterdir() if r.is_dir()):
            # prefer early event if present
            evs = sorted(
                e for e in run_dir.glob("*.json") if not e.name.endswith(".tmp")
            )
            if evs:
                try:
                    return json.loads(evs[0].read_text())
                except Exception:
                    continue
    return None


def _read_scope_from_spool(plan_spool_dir: Path) -> dict[str, Any]:
    ev = _find_any_event(plan_spool_dir)
    return (ev.get("scope") if isinstance(ev, dict) else {}) or {}


def build_plan_snapshot(plan_dir: Path, realm: str, plan_id: str) -> dict[str, Any]:
    scope = _read_scope_from_spool(plan_dir)  # <-- use events, not plan.json
    steps: dict[str, Any] = {}
    for step_dir in (p for p in plan_dir.iterdir() if p.is_dir()):
        run_dirs = [d for d in step_dir.iterdir() if d.is_dir()]
        if not run_dirs:
            continue
        run_dir = sorted(run_dirs)[-1]
        events = sorted(
            e for e in run_dir.glob("*.json") if not e.name.endswith(".tmp")
        )
        if not events:
            continue
        last = _safe_load(events[-1])
        steps[step_dir.name] = {
            "step_name": last.get("step_name", ""),
            "state": last.get("type", "unknown"),
            "run_id": run_dir.name,
            "fingerprint": last.get("fingerprint"),
            "progress": last.get(
                "progress",
                100 if str(last.get("type", "")).endswith("succeeded") else 0,
            ),
            "artifacts": last.get("artifacts", []),
            "metrics": last.get("metrics", {}),
            "job": last.get("job"),
            "ts": last.get("ts"),
        }
    # updated_at via your common util if you have it
    try:
        now = utcnow_iso()
    except Exception:
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    return {
        "type": "plan_status",
        "realm": realm,
        "plan_id": plan_id,
        "scope": scope,
        "steps": steps,
        "updated_at": now,
    }
