"""
Plan eligibility logic for determining execution readiness.

This module provides a pure function for evaluating whether a plan
document is eligible for execution. A plan is eligible when:

1. Its status is 'approved'
2. Its execution tokens are a valid pair, read under the strict token rule
   of :func:`yggdrasil.contract.controls.token_pair`
3. Its run_token is greater than its executed_run_token

No field is defaulted: an absent or unknown status is not approved, and an
absent, null or non-integer token makes the pair invalid, so the plan is not
eligible. Eligibility judges status and tokens only; whether the rest of the
document describes an executable plan is decided at admission.

This separation allows:
- Easy unit testing (pure function, no I/O)
- Clear eligibility semantics
- Reuse across PlanWatcher and startup recovery
"""

from typing import Any

from yggdrasil.contract.controls import token_pair
from yggdrasil.contract.diagnostics import describe_recorded

APPROVED_STATUS = "approved"


def is_plan_eligible(plan_doc: dict[str, Any]) -> bool:
    """
    Determine if a plan document is eligible for execution.

    A plan is eligible iff:
    - status == "approved"
    - run_token and executed_run_token are a valid token pair
    - run_token > executed_run_token

    This prevents:
    - Re-execution on restart (tokens equal)
    - Execution of unapproved plans (status != approved)
    - Duplicate execution of the same run
    - Execution under tokens that are missing, null or not JSON integers

    Manual re-runs work by incrementing run_token.

    Note:
        This function does NOT check `execution_authority` or `execution_owner`.
        Those fields are used for ownership filtering in PlanWatcher, not
        eligibility. A daemon watcher filters by authority="daemon"; a run-once
        CLI filters by authority="run_once" and its specific owner token.
        This separation keeps eligibility logic pure and reusable.

    Args:
        plan_doc: Plan document from yggdrasil_plans DB

    Returns:
        bool: True if plan should be executed, False otherwise

    Examples:
        An initial run:

        >>> is_plan_eligible({"status": "approved", "run_token": 0, "executed_run_token": -1})
        True

        A manual re-run (token incremented):

        >>> is_plan_eligible({"status": "approved", "run_token": 1, "executed_run_token": 0})
        True

        Already executed (tokens equal):

        >>> is_plan_eligible({"status": "approved", "run_token": 0, "executed_run_token": 0})
        False

        Awaiting approval:

        >>> is_plan_eligible({"status": "draft", "run_token": 0, "executed_run_token": -1})
        False

        A token that is not a JSON integer, or that is missing, is never read
        as a number:

        >>> is_plan_eligible({"status": "approved", "run_token": "1", "executed_run_token": 0})
        False
        >>> is_plan_eligible({"status": "approved", "run_token": 0})
        False
    """
    eligible, _ = _judge(plan_doc)
    return eligible


def get_eligibility_reason(plan_doc: dict[str, Any]) -> str:
    """
    Get a human-readable explanation of why a plan is or isn't eligible.

    Derived from the same decision as :func:`is_plan_eligible`. Invalid tokens
    are reported first, with every problem :func:`token_pair` found; then a
    status other than 'approved'; then a request that was already executed.

    Useful for debugging and logging.

    Args:
        plan_doc: Plan document from yggdrasil_plans DB

    Returns:
        str: Explanation of eligibility status
    """
    _, reason = _judge(plan_doc)
    return reason


def _judge(plan_doc: dict[str, Any]) -> tuple[bool, str]:
    """Decide a plan document's eligibility, and explain the decision."""
    tokens = token_pair(plan_doc)
    if not tokens.valid:
        return False, f"Not eligible: invalid tokens ({'; '.join(tokens.problems)})"
    if "status" not in plan_doc:
        return False, f"Not eligible: status is missing (must be '{APPROVED_STATUS}')"
    status = plan_doc["status"]
    if status != APPROVED_STATUS:
        return False, (
            f"Not eligible: status {describe_recorded(status)} "
            f"(must be '{APPROVED_STATUS}')"
        )
    # A valid pair never has executed above run: equal tokens are the only
    # pair whose request was already served.
    if tokens.run == tokens.executed:
        return False, (
            f"Not eligible: run_token={tokens.run} was already executed "
            f"(executed_run_token={tokens.executed})"
        )
    return True, (
        f"Eligible: status='{APPROVED_STATUS}', run_token={tokens.run} > "
        f"executed_run_token={tokens.executed}"
    )
