"""
Plan database manager for yggdrasil_plans DB.

This module provides CRUD operations for plan documents, which store
intent and approval state for workflow execution.

Plan documents are stored in the dedicated `yggdrasil_plans` database,
separate from operational data (yggdrasil_ops) and project data (projects).
"""

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from ibm_cloud_sdk_core.api_exception import ApiException
from ibmcloudant.cloudant_v1 import Document

from lib.core_utils.logging_utils import custom_logger
from lib.couchdb.couchdb_connection import CouchDBHandler
from yggdrasil.flow.model import Plan

logging = custom_logger(__name__.split(".")[-1])

# Valid values for execution_authority field
VALID_EXECUTION_AUTHORITIES = frozenset({"daemon", "run_once"})


def _json_safe(value: Any) -> Any:
    """Recursively coerce plan documents into JSON-serializable structures.

    CouchDB client calls ``json.dumps`` under the hood. Realm planners can
    sometimes return pathlib.Path (or other non-JSON types) inside params or
    preview payloads. This helper walks the structure and converts:
    - Path -> str
    - set/tuple -> list
    - dict/list elements recursively
    """

    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    return value


def _validate_execution_authority(authority: str) -> None:
    """Raise ValueError if execution_authority is invalid."""
    if authority not in VALID_EXECUTION_AUTHORITIES:
        raise ValueError(
            f"Invalid execution_authority: {authority!r}. "
            f"Must be one of: {sorted(VALID_EXECUTION_AUTHORITIES)}"
        )


def _utc_now_iso() -> str:
    """Return current UTC timestamp in ISO-8601 format."""
    return datetime.now(UTC).isoformat(timespec="seconds")


class PlanDBManager(CouchDBHandler):
    """
    Manages interactions with the 'yggdrasil_plans' database.

    Provides methods for:
    - Saving plan documents (with status, tokens, metadata)
    - Fetching plan documents by ID
    - Querying approved pending plans (for startup recovery)
    - Updating execution tokens after successful runs

    Plan Document Schema:
    {
        "_id": "pln_<realm>_<scope_id>_v<version>",
        "realm": "tenx",
        "scope": {"kind": "project", "id": "P36805"},
        "status": "draft" | "approved",
        "plan": { ... serialized Plan ... },
        "preview": { ... optional preview data ... },
        "run_token": 0,
        "executed_run_token": -1,
        "created_at": "ISO-8601",
        "updated_at": "ISO-8601",
        ...
    }
    """

    def __init__(self) -> None:
        """Initialize connection to yggdrasil_plans database."""
        super().__init__("yggdrasil_plans")

    def save_plan(
        self,
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
    ) -> str:
        """
        Persist a plan document to the database.

        Creates or overwrites the plan document. On regeneration, this resets
        execution tokens to ensure the new plan is eligible for execution.

        The document ID is taken from ``plan.plan_id``, which is owned by the
        realm/planner. PlanDBManager is a generic CRUD layer and never derives
        the document ID from (realm, scope).

        Args:
            plan: The Plan object to persist (plan.plan_id is used as _id)
            realm: Realm identifier
            scope: Scope dict with 'kind' and 'id' keys
            auto_run: If True, set status='approved'; else status='draft'
            execution_authority: Who has authority to execute - "daemon" (default) or "run_once"
            execution_owner: Unique token for run_once isolation (e.g., "run_once:<uuid>")
            preview: Optional preview data for UI display
            source_doc_id: Optional source document ID that triggered this plan
            source_doc_rev: Optional source document revision
            notes: Optional notes about the plan

        Returns:
            str: The document ID of the persisted plan (same as plan.plan_id)

        Raises:
            ValueError: If execution_authority is invalid or plan.plan_id is missing
            ApiException: On database errors
        """
        # Validate inputs
        _validate_execution_authority(execution_authority)
        doc_id = plan.plan_id
        if not doc_id:
            raise ValueError("plan.plan_id is required for persistence")

        # Check for existing document to get _rev
        existing = self.fetch_document_by_id(doc_id)
        rev = existing.get("_rev") if existing else None

        now = _utc_now_iso()

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
            "created_at": existing.get("created_at", now) if existing else now,
            "updated_at": now,
        }

        # Include _rev for conflict-safe update
        if rev:
            plan_doc["_rev"] = rev

        # Optional fields
        if source_doc_id:
            plan_doc["source_doc_id"] = source_doc_id
        if source_doc_rev:
            plan_doc["source_doc_rev"] = source_doc_rev
        if notes:
            plan_doc["notes"] = notes

        # Persist to database (after ensuring JSON-serializable payload)
        try:
            serializable_doc = cast(Document, _json_safe(plan_doc))
            self.server.put_document(
                db=self.db_name,
                doc_id=doc_id,
                document=serializable_doc,
            ).get_result()

            logging.info(
                "Saved plan '%s' (realm=%s, status=%s)",
                doc_id,
                realm,
                plan_doc["status"],
            )
            return doc_id

        except ApiException as e:
            logging.error("Failed to save plan '%s': %s", doc_id, e)
            raise

    def fetch_plan(self, doc_id: str) -> dict[str, Any] | None:
        """
        Fetch a plan document by ID.

        Args:
            doc_id: The plan document ID

        Returns:
            dict or None: The plan document, or None if not found
        """
        return self.fetch_document_by_id(doc_id)

    def fetch_plan_as_model(self, doc_id: str) -> Plan | None:
        """
        Fetch a plan document and deserialize to Plan model.

        Args:
            doc_id: The plan document ID

        Returns:
            Plan or None: The deserialized Plan, or None if not found
        """
        doc = self.fetch_document_by_id(doc_id)
        if not doc:
            return None

        plan_data = doc.get("plan")
        if not plan_data:
            logging.warning("Plan document '%s' has no 'plan' field", doc_id)
            return None

        try:
            # Note: Path reconstruction is handled by Engine via coerce_params_to_signature_types()
            # (from yggdrasil.flow.utils.typing_coerce) based on step function type hints.
            # Params remain as strings in the persisted document.
            return Plan.from_dict(plan_data)
        except (KeyError, TypeError) as e:
            logging.error("Failed to deserialize plan '%s': %s", doc_id, e)
            return None

    def update_executed_token(
        self,
        doc_id: str,
        run_token: int,
        *,
        max_retries: int = 3,
    ) -> bool:
        """
        Update executed_run_token after successful plan execution.

        Uses optimistic locking (_rev) to prevent race conditions.
        Retries on conflict (409) up to max_retries times.

        Args:
            doc_id: The plan document ID
            run_token: The run_token value that was just executed
            max_retries: Maximum retry attempts on conflict

        Returns:
            bool: True if update succeeded, False otherwise
        """
        for attempt in range(1, max_retries + 1):
            doc = self.fetch_document_by_id(doc_id)
            if not doc:
                logging.error("Cannot update token: plan '%s' not found", doc_id)
                return False

            # Update fields
            doc["executed_run_token"] = run_token
            doc["last_executed_at"] = _utc_now_iso()
            doc["updated_at"] = _utc_now_iso()

            try:
                self.server.put_document(
                    db=self.db_name,
                    doc_id=doc_id,
                    document=cast(Document, doc),
                ).get_result()

                logging.info(
                    "Updated executed_run_token=%d for plan '%s'",
                    run_token,
                    doc_id,
                )
                return True

            except ApiException as e:
                if e.code == 409:
                    logging.warning(
                        "Conflict updating plan '%s'; retry %d/%d",
                        doc_id,
                        attempt,
                        max_retries,
                    )
                    continue
                logging.error("Failed to update plan '%s': %s", doc_id, e)
                return False

        logging.error(
            "Failed to update plan '%s' after %d retries",
            doc_id,
            max_retries,
        )
        return False

    def query_approved_pending(self) -> list[dict[str, Any]]:
        """
        Query all approved plans that are pending execution.

        Used for startup recovery when checkpoint is missing.
        Returns plans where: status='approved' AND run_token > executed_run_token

        Note: This is a full scan (O(n)). For large databases, consider
        adding a CouchDB view for indexed queries.

        Returns:
            list: Plan documents eligible for execution
        """
        from lib.core_utils.plan_eligibility import is_plan_eligible

        eligible_plans: list[dict[str, Any]] = []

        try:
            # Fetch all documents (with full content)
            response = self.server.post_all_docs(
                db=self.db_name,
                include_docs=True,
            ).get_result()

            result = cast(dict[str, Any], response) if response else {}
            rows = result.get("rows", [])
            for row in rows:
                doc = row.get("doc", {})

                # Skip design documents
                if doc.get("_id", "").startswith("_design/"):
                    continue

                # Check eligibility
                if is_plan_eligible(doc):
                    eligible_plans.append(doc)

            logging.info(
                "Found %d eligible plans (of %d total) for recovery",
                len(eligible_plans),
                len(rows),
            )

        except ApiException as e:
            logging.error("Failed to query approved pending plans: %s", e)
            # Return empty list on error (caller handles recovery)

        return eligible_plans

    def delete_plan(self, doc_id: str) -> bool:
        """
        Delete a plan document (for testing/cleanup).

        Args:
            doc_id: The plan document ID

        Returns:
            bool: True if deleted, False otherwise
        """
        doc = self.fetch_document_by_id(doc_id)
        if not doc:
            logging.warning("Cannot delete: plan '%s' not found", doc_id)
            return False

        rev = doc.get("_rev")
        if not rev:
            logging.error("Cannot delete: plan '%s' has no _rev", doc_id)
            return False

        try:
            self.server.delete_document(
                db=self.db_name,
                doc_id=doc_id,
                rev=rev,
            ).get_result()
            logging.info("Deleted plan '%s'", doc_id)
            return True

        except ApiException as e:
            logging.error("Failed to delete plan '%s': %s", doc_id, e)
            return False

    def plan_exists(self, doc_id: str) -> bool:
        """
        Check if a plan document exists (without fetching full content).

        Used for overwrite detection in CLI mode.

        Args:
            doc_id: The plan document ID

        Returns:
            bool: True if document exists, False otherwise
        """
        return self.fetch_document_by_id(doc_id) is not None

    def get_plan_summary(self, doc_id: str) -> dict[str, Any] | None:
        """
        Fetch minimal plan summary for display.

        Returns key metadata without the full plan content.
        Useful for displaying existing plan info during overwrite warnings.

        Args:
            doc_id: The plan document ID

        Returns:
            dict with keys: status, execution_authority, execution_owner,
                updated_at, realm, run_token, executed_run_token
            or None if not found
        """
        doc = self.fetch_document_by_id(doc_id)
        if not doc:
            return None
        return {
            "status": doc.get("status", "unknown"),
            "execution_authority": doc.get("execution_authority", "daemon"),
            "execution_owner": doc.get("execution_owner"),
            "updated_at": doc.get("updated_at", "unknown"),
            "realm": doc.get("realm", "unknown"),
            "run_token": doc.get("run_token", 0),
            "executed_run_token": doc.get("executed_run_token", -1),
        }
