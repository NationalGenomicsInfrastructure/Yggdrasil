"""Backend-neutral plan-document construction and interpretation.

Single source of truth for the plan document shape so the CouchDB and SQLite
plan stores cannot drift. Extracted from ``PlanDBManager`` (which now
delegates here) — the document schema is unchanged.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from yggdrasil.flow.model import Plan

# Valid values for the execution_authority field
VALID_EXECUTION_AUTHORITIES = frozenset({"daemon", "run_once"})


def json_safe(value: Any) -> Any:
    """Recursively coerce plan documents into JSON-serializable structures.

    Realm planners can return pathlib.Path (or other non-JSON types) inside
    params or preview payloads. This helper walks the structure and converts:
    - Path -> str
    - set/tuple -> list
    - dict/list elements recursively
    """
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {k: json_safe(v) for k, v in value.items()}
    if isinstance(value, list | tuple | set):
        return [json_safe(v) for v in value]
    return value


def validate_execution_authority(authority: str) -> None:
    """Raise ValueError if execution_authority is invalid."""
    if authority not in VALID_EXECUTION_AUTHORITIES:
        raise ValueError(
            f"Invalid execution_authority: {authority!r}. "
            f"Must be one of: {sorted(VALID_EXECUTION_AUTHORITIES)}"
        )


def utc_now_iso() -> str:
    """Return current UTC timestamp in ISO-8601 format."""
    return datetime.now(UTC).isoformat(timespec="seconds")


def build_plan_document(
    plan: Plan,
    realm: str,
    scope: dict[str, Any],
    *,
    auto_run: bool = False,
    execution_authority: str = "daemon",
    execution_owner: str | None = None,
    preview: dict[str, Any] | None = None,
    source_doc_id: str | None = None,
    source_doc_rev: str | None = None,
    notes: str | None = None,
    existing: dict[str, Any] | None = None,
    now: str | None = None,
) -> dict[str, Any]:
    """Build the canonical plan document for persistence.

    On regeneration (``existing`` provided), execution tokens are reset so the
    new plan is eligible for execution, while ``created_at`` is preserved.
    Backend-specific fields (``_rev``) are the caller's responsibility.

    Args:
        plan: The Plan object to persist (plan.plan_id is used as _id).
        realm: Realm identifier.
        scope: Scope dict with 'kind' and 'id' keys.
        auto_run: If True, set status='approved'; else status='draft'.
        execution_authority: "daemon" (default) or "run_once".
        execution_owner: Unique token for run_once isolation.
        preview: Optional preview data for UI display.
        source_doc_id: Optional source document ID that triggered this plan.
        source_doc_rev: Optional source document revision.
        notes: Optional notes about the plan.
        existing: Previously persisted document, if any (for created_at).
        now: ISO-8601 timestamp override; defaults to current UTC time.

    Returns:
        dict: JSON-safe plan document including ``_id``.

    Raises:
        ValueError: If execution_authority is invalid or plan.plan_id missing.
    """
    validate_execution_authority(execution_authority)
    doc_id = plan.plan_id
    if not doc_id:
        raise ValueError("plan.plan_id is required for persistence")

    timestamp = now or utc_now_iso()

    plan_doc: dict[str, Any] = {
        "_id": doc_id,
        "realm": realm,
        "scope": scope,
        "status": "approved" if auto_run else "draft",
        "plan": plan.to_dict(),
        "preview": preview or {},
        "run_token": 0,
        "executed_run_token": -1,
        "execution_authority": execution_authority,
        "execution_owner": execution_owner,
        "created_at": (
            existing.get("created_at", timestamp) if existing else timestamp
        ),
        "updated_at": timestamp,
    }

    if source_doc_id:
        plan_doc["source_doc_id"] = source_doc_id
    if source_doc_rev:
        plan_doc["source_doc_rev"] = source_doc_rev
    if notes:
        plan_doc["notes"] = notes

    return json_safe(plan_doc)


def plan_model_from_document(
    doc: dict[str, Any],
    logger: logging.Logger,
) -> Plan | None:
    """Deserialize the ``plan`` field of a plan document into a Plan model.

    Args:
        doc: The persisted plan document.
        logger: Logger for warnings/errors.

    Returns:
        Plan or None if the document has no plan field or fails to parse.
    """
    doc_id = doc.get("_id", "<unknown>")
    plan_data = doc.get("plan")
    if not plan_data:
        logger.warning("Plan document '%s' has no 'plan' field", doc_id)
        return None

    try:
        # Path reconstruction is handled by Engine via
        # coerce_params_to_signature_types() based on step type hints;
        # params remain strings in the persisted document.
        return Plan.from_dict(plan_data)
    except (KeyError, TypeError) as e:
        logger.error("Failed to deserialize plan '%s': %s", doc_id, e)
        return None


def plan_summary_from_document(doc: dict[str, Any]) -> dict[str, Any]:
    """Extract the minimal display summary from a plan document."""
    return {
        "status": doc.get("status", "unknown"),
        "execution_authority": doc.get("execution_authority", "daemon"),
        "execution_owner": doc.get("execution_owner"),
        "updated_at": doc.get("updated_at", "unknown"),
        "realm": doc.get("realm", "unknown"),
        "run_token": doc.get("run_token", 0),
        "executed_run_token": doc.get("executed_run_token", -1),
    }
