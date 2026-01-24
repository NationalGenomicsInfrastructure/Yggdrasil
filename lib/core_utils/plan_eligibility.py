"""
Plan eligibility logic for determining execution readiness.

This module provides a pure function for evaluating whether a plan
document is eligible for execution. The logic is based on:

1. Status must be 'approved'
2. run_token must be greater than executed_run_token

This separation allows:
- Easy unit testing (pure function, no I/O)
- Clear eligibility semantics
- Reuse across PlanWatcher and startup recovery
"""

from typing import Any


def is_plan_eligible(plan_doc: dict[str, Any]) -> bool:
    """
    Determine if a plan document is eligible for execution.

    A plan is eligible iff:
    - status == "approved"
    - run_token > executed_run_token

    This prevents:
    - Re-execution on restart (tokens equal)
    - Execution of unapproved plans (status != approved)
    - Duplicate execution of the same run

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
        >>> is_plan_eligible({"status": "approved", "run_token": 0, "executed_run_token": -1})
        True  # Initial run (default case)

        >>> is_plan_eligible({"status": "approved", "run_token": 1, "executed_run_token": 0})
        True  # Manual re-run (token incremented)

        >>> is_plan_eligible({"status": "approved", "run_token": 0, "executed_run_token": 0})
        False  # Already executed (tokens equal)

        >>> is_plan_eligible({"status": "draft", "run_token": 0, "executed_run_token": -1})
        False  # Awaiting approval
    """
    # Rule 1: Status must be 'approved'
    status = plan_doc.get("status", "draft")
    if status != "approved":
        return False

    # Rule 2: run_token > executed_run_token
    # Missing run_token defaults to 0 (initial run)
    # Missing executed_run_token defaults to -1 (never executed)
    run_token = plan_doc.get("run_token", 0)
    executed_run_token = plan_doc.get("executed_run_token", -1)

    # Type safety: ensure numeric comparison
    try:
        run_token = int(run_token)
        executed_run_token = int(executed_run_token)
    except (TypeError, ValueError):
        # Invalid token values → not eligible
        return False

    return run_token > executed_run_token


def get_eligibility_reason(plan_doc: dict[str, Any]) -> str:
    """
    Get a human-readable explanation of why a plan is or isn't eligible.

    Useful for debugging and logging.

    Args:
        plan_doc: Plan document from yggdrasil_plans DB

    Returns:
        str: Explanation of eligibility status
    """
    status = plan_doc.get("status", "draft")
    run_token = plan_doc.get("run_token", 0)
    executed_run_token = plan_doc.get("executed_run_token", -1)

    # Validate types
    try:
        run_token = int(run_token)
        executed_run_token = int(executed_run_token)
    except (TypeError, ValueError):
        return f"Invalid token values: run_token={run_token!r}, executed_run_token={executed_run_token!r}"

    if status != "approved":
        return f"Not eligible: status='{status}' (must be 'approved')"

    if run_token <= executed_run_token:
        return (
            f"Not eligible: run_token={run_token} <= executed_run_token={executed_run_token} "
            f"(already executed)"
        )

    return (
        f"Eligible: status='approved', run_token={run_token} > "
        f"executed_run_token={executed_run_token}"
    )
