"""Backend-neutral plan-document construction and interpretation.

Single source of truth for the plan document shape so the CouchDB and SQLite
plan stores cannot drift. Extracted from ``PlanDBManager`` (which now
delegates here). The document's field names and words are spelled in
``yggdrasil.contract.fields``, and the display summary is a projection of
the shared reader's ``yggdrasil.contract.read.read_plan``.

Plan generation:
    Every document written by ``save_plan`` carries a fresh opaque
    ``plan_generation`` ID. It identifies one planned version of the plan:
    approval changes and run-token increments keep it, while regenerating the
    plan replaces it. It is never a stable default, so a plan that is deleted
    and later recreated under the same ID cannot be mistaken for its earlier
    version. A document of a supported family always carries a generation,
    and one without a usable generation is refused for execution: nothing
    adds a generation to an existing document.

Replacing a plan:
    Planning replaces the whole document in a plan's slot. Whether it may,
    and which ``created_at`` the replacement keeps, is decided by
    ``check_replaceable`` alone. ``build_plan_document`` applies it whenever
    it is given the slot's document, so every store that builds before
    writing refuses the same slots without writing.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from yggdrasil.contract import CURRENT_FAMILIES, PLAN_DOCUMENT, SUPPORTED_FAMILIES, read
from yggdrasil.contract.diagnostics import describe_recorded
from yggdrasil.contract.fields import (
    APPROVED,
    CREATED_AT_FIELD,
    DAEMON,
    DRAFT,
    EXECUTED_RUN_TOKEN_FIELD,
    EXECUTION_AUTHORITY_FIELD,
    EXECUTION_OWNER_FIELD,
    ID_FIELD,
    NOTES_FIELD,
    PLAN_FIELD,
    PLAN_GENERATION_FIELD,
    PREVIEW_FIELD,
    REALM_FIELD,
    RUN_ONCE,
    RUN_TOKEN_FIELD,
    SCHEMA_VERSION_FIELD,
    SCOPE_FIELD,
    SOURCE_DOC_ID_FIELD,
    SOURCE_DOC_REV_FIELD,
    STATUS_FIELD,
    UPDATED_AT_FIELD,
)
from yggdrasil.contract.fields import (
    LAST_FINALIZED_EXECUTION_FIELD as LAST_FINALIZED_EXECUTION_FIELD,
)
from yggdrasil.contract.timestamps import (
    canonical_from_legacy,
    is_canonical,
    is_date_time,
)
from yggdrasil.contract.versions import (
    INVALID,
    UNSUPPORTED,
    UNVERSIONED,
    version_of,
    version_sentence,
)
from yggdrasil.flow.model import Plan
from yggdrasil.flow.utils.ygg_time import utcnow_iso
from yggdrasil.storage.errors import UnsupportedDocumentError

# CREATED_AT_FIELD, LAST_FINALIZED_EXECUTION_FIELD and PLAN_GENERATION_FIELD
# stay importable from this module, as the yggdrasil.contract.fields spellings.

# Valid values for the execution_authority field, built from the contract's
# words for them.
VALID_EXECUTION_AUTHORITIES = frozenset({DAEMON, RUN_ONCE})


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


def new_plan_generation() -> str:
    """Return a fresh, globally unique plan generation ID.

    Returns:
        str: An opaque random ID. Generations are compared only for equality;
        they carry no order.
    """
    return uuid.uuid4().hex


def check_replaceable(doc_id: str, existing: Mapping[str, Any]) -> str:
    """Return the ``created_at`` that a replacement of a plan's document keeps.

    Planning may replace an unversioned (pre-contract) document or a document
    of a supported family. It refuses a document whose ``schema_version`` is
    invalid or names an unsupported family, such as a newer family that the
    replacement would downgrade. The marker is judged first, so such a
    document is refused for its family whatever its ``created_at`` holds.

    The replacement keeps the document's creation instant, spelled
    canonically. A supported document's value must already be canonical,
    whole seconds (``2026-10-22T10:00:00Z``) or a fraction
    (``2026-10-22T10:00:00.25Z``), and is kept verbatim. An unversioned
    document's value may be either spelling that earlier releases wrote, a
    whole-second UTC time ending in ``Z`` or ``+00:00``, and is re-spelled
    with ``Z``; earlier releases never wrote a fraction, so an unversioned
    document with one is refused. Any other value is refused: the build time
    is never substituted and precision is never truncated.

    Args:
        doc_id: ID of the plan whose slot is being replaced.
        existing: The document in the slot, as read.

    Returns:
        str: The ``created_at`` the replacement carries.

    Raises:
        UnsupportedDocumentError: If planning may not replace existing.
    """
    supported = SUPPORTED_FAMILIES[PLAN_DOCUMENT]
    version = version_of(PLAN_DOCUMENT, existing)
    if version.state in (INVALID, UNSUPPORTED):
        raise UnsupportedDocumentError(
            doc_id=doc_id,
            field=SCHEMA_VERSION_FIELD,
            recorded=version.recorded,
            reason=version.state,
            supported=supported,
        )

    if CREATED_AT_FIELD not in existing:
        raise UnsupportedDocumentError(
            doc_id=doc_id,
            field=CREATED_AT_FIELD,
            recorded=None,
            reason="missing",
            supported=supported,
        )
    recorded = existing[CREATED_AT_FIELD]
    if version.state == UNVERSIONED:
        kept = canonical_from_legacy(recorded)
    else:
        kept = recorded if is_canonical(recorded) else None
    if kept is None:
        raise UnsupportedDocumentError(
            doc_id=doc_id,
            field=CREATED_AT_FIELD,
            recorded=recorded,
            reason="unsupported" if is_date_time(recorded) else "malformed",
            supported=supported,
        )
    return kept


def build_plan_document(
    plan: Plan,
    realm: str,
    scope: dict[str, Any],
    *,
    auto_run: bool = False,
    execution_authority: str = DAEMON,
    execution_owner: str | None = None,
    preview: dict[str, Any] | None = None,
    source_doc_id: str | None = None,
    source_doc_rev: str | None = None,
    notes: str | None = None,
    existing: dict[str, Any] | None = None,
    now: str | None = None,
    plan_generation: str | None = None,
) -> dict[str, Any]:
    """Build the canonical plan document for persistence.

    Every call builds a new plan version: the document gets a fresh
    ``plan_generation``, and ``schema_version`` names the family of the
    release that built it (``CURRENT_FAMILIES`` in
    ``yggdrasil/contract/__init__.py``). Backend-specific fields (``_rev``)
    are the caller's responsibility.

    On regeneration (``existing`` provided), only the creation instant
    carries over, as ``check_replaceable`` decides; everything else is
    rebuilt. The execution tokens are reset so the new plan is eligible for
    execution, and approval and request stamps, any recorded execution
    result and fields written by others are dropped. A slot that planning may
    not replace is refused before anything is built.

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
        existing: The document currently in the plan's slot, as read, if
            there is one.
        now: Timestamp override for ``updated_at``, and for ``created_at``
            when there is no existing document, written as given; defaults
            to utcnow_iso().
        plan_generation: Generation ID override; defaults to a fresh one from
            new_plan_generation().

    Returns:
        dict: JSON-safe plan document including ``_id``.

    Raises:
        ValueError: If execution_authority is invalid or plan.plan_id missing.
        UnsupportedDocumentError: If planning may not replace existing (see
            check_replaceable).
    """
    validate_execution_authority(execution_authority)
    doc_id = plan.plan_id
    if not doc_id:
        raise ValueError("plan.plan_id is required for persistence")

    timestamp = now or utcnow_iso()
    created_at = timestamp if existing is None else check_replaceable(doc_id, existing)

    plan_doc: dict[str, Any] = {
        ID_FIELD: doc_id,
        SCHEMA_VERSION_FIELD: CURRENT_FAMILIES[PLAN_DOCUMENT],
        REALM_FIELD: realm,
        SCOPE_FIELD: scope,
        STATUS_FIELD: APPROVED if auto_run else DRAFT,
        PLAN_FIELD: plan.to_dict(),
        PREVIEW_FIELD: preview or {},
        PLAN_GENERATION_FIELD: plan_generation or new_plan_generation(),
        RUN_TOKEN_FIELD: 0,
        EXECUTED_RUN_TOKEN_FIELD: -1,
        EXECUTION_AUTHORITY_FIELD: execution_authority,
        EXECUTION_OWNER_FIELD: execution_owner,
        CREATED_AT_FIELD: created_at,
        UPDATED_AT_FIELD: timestamp,
    }

    if source_doc_id:
        plan_doc[SOURCE_DOC_ID_FIELD] = source_doc_id
    if source_doc_rev:
        plan_doc[SOURCE_DOC_REV_FIELD] = source_doc_rev
    if notes:
        plan_doc[NOTES_FIELD] = notes

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
    doc_id = doc.get(ID_FIELD, "<unknown>")
    plan_data = doc.get(PLAN_FIELD)
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


def warn_unsupported_omission(
    logger: logging.Logger, plan_id: object, document: Mapping[str, Any]
) -> None:
    """Report that a recovery listing leaves out a plan of no supported family.

    Eligibility checks the version first, so a listing omits a plan whose
    stored ``schema_version`` is absent, invalid or unsupported, whatever its
    status and tokens say: the plan cannot run while its stored version is
    any of these. The line names the plan, what its ``schema_version``
    records and the supported families, in the words every refusal of a
    family uses.

    Args:
        logger: The listing store's logger.
        plan_id: The plan's ID as the store holds it, shown bounded.
        document: The omitted plan document.
    """
    logger.warning(
        "Plan %s is not executed: %s; it is left out of the recovery listing",
        describe_recorded(plan_id),
        version_sentence(PLAN_DOCUMENT, version_of(PLAN_DOCUMENT, document)),
    )


# The summary's values, every one None when the reader can use nothing.
_SUMMARY_VALUES = (
    "status",
    "execution_authority",
    "execution_owner",
    "updated_at",
    "realm",
    "run_token",
    "executed_run_token",
    "plan_generation",
    "last_finalized_outcome",
)


def plan_summary_from_document(document: object, *, plan_id: str) -> dict[str, Any]:
    """Summarise a stored plan document for display, as the shared reader reads it.

    The summary is a projection of
    :func:`yggdrasil.contract.read.read_plan`, which reads the document under
    the ID it was requested by, so a body stored under another ``_id`` is
    reported rather than shown as the requested plan. Nothing is filled in: a
    value the reader does not recognise is None, never a display default.
    When the reader can use nothing, because the document is not an object,
    is of no supported family or is stored under another ID, every value is
    None and ``problems`` says why. Every stored document has a summary.

    Approval ``status`` and the outcome of the last finalized execution are
    separate values: an approved plan whose tokens are equal has finished its
    last request, not necessarily succeeded at it.

    Args:
        document: The stored body, of any shape.
        plan_id: The ID the document was requested under.

    Returns:
        dict[str, Any]: The summary, with these keys:

        - ``status``: ``draft`` or ``approved``; None for any other value.
        - ``execution_authority``: ``daemon`` or ``run_once``; None for any
          other value.
        - ``execution_owner``: the run-once owner, when it is non-empty text.
        - ``updated_at``: the recorded text, when it names an instant.
        - ``realm``: the realm, when it is non-empty text.
        - ``run_token``, ``executed_run_token``: each token when it is valid
          on its own, even if the pair is not.
        - ``plan_generation``: the generation, when it is non-empty text.
        - ``last_finalized_outcome``: the outcome the last finalized
          execution records, when the record agrees with its report and the
          outcome is a known one. It may be an earlier request's.
        - ``problems``: every problem the reader found, a tuple of
          ``ContractProblem``; empty for a sound document.

    Raises:
        TypeError: If plan_id is not a string.
        ValueError: If plan_id is empty.
    """
    plan = read.read_plan(document, plan_id=plan_id)
    recorded, tokens = plan.recorded, plan.tokens
    if recorded is None or tokens is None:
        # Nothing is derived from a document the reader cannot use.
        values: dict[str, Any] = dict.fromkeys(_SUMMARY_VALUES)
    else:
        finalized = plan.finalized
        updated_at = recorded.updated_at
        values = {
            "status": recorded.status.value,
            "execution_authority": recorded.execution_authority.value,
            "execution_owner": recorded.execution_owner.value,
            "updated_at": (
                updated_at.recorded if updated_at.value is not None else None
            ),
            "realm": recorded.realm.value,
            "run_token": tokens.run,
            "executed_run_token": tokens.executed,
            "plan_generation": plan.generation,
            "last_finalized_outcome": (
                finalized.outcome.value
                if finalized is not None and finalized.usable
                else None
            ),
        }
    return {**values, "problems": plan.problems}
