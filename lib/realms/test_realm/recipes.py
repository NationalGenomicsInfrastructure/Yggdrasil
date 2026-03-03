"""
Test realm recipes for generating test plans.

Recipes are factory functions that return a list of StepSpec objects.
Each recipe represents a different test scenario:

- happy_path: All steps succeed
- fail_fast: First step fails
- fail_mid_plan: Fails in the middle of execution
- long_running: Extended sleep for timeout testing
- artifact_write: Tests artifact registration
"""

from typing import Any

from yggdrasil.flow.model import StepSpec

# Module path for fn_ref resolution by Engine
_FN_REF_PREFIX = "lib.realms.test_realm.steps"


def _make_step(
    step_id: str,
    name: str,
    fn_name: str,
    params: dict[str, Any] | None = None,
    deps: list[str] | None = None,
) -> StepSpec:
    """Helper to create StepSpec with consistent fn_ref format."""
    return StepSpec(
        step_id=step_id,
        name=name,
        fn_ref=f"{_FN_REF_PREFIX}.{fn_name}",
        params=params or {},
        deps=deps or [],
    )


# ---------------------------------------------------------------------------
# Recipe: happy_path
# ---------------------------------------------------------------------------


def happy_path(
    overrides: dict[str, dict[str, Any]] | None = None,
) -> list[StepSpec]:
    """
    Generate a plan where all steps succeed.

    Steps:
        1. echo_start: Echo "Starting happy path"
        2. brief_sleep: Sleep 0.5s
        3. echo_end: Echo "Happy path complete"

    Args:
        overrides: Optional dict mapping step_id to param overrides

    Returns:
        List of StepSpec for Engine execution
    """
    overrides = overrides or {}

    steps = [
        _make_step(
            step_id="echo_start",
            name="Echo Start",
            fn_name="step_echo",
            params={"message": "Starting happy path"},
        ),
        _make_step(
            step_id="brief_sleep",
            name="Brief Sleep",
            fn_name="step_sleep",
            params={"duration_sec": 0.5},
            deps=["echo_start"],
        ),
        _make_step(
            step_id="echo_end",
            name="Echo End",
            fn_name="step_echo",
            params={"message": "Happy path complete"},
            deps=["brief_sleep"],
        ),
    ]

    # Apply overrides by step_id
    return _apply_overrides(steps, overrides)


# ---------------------------------------------------------------------------
# Recipe: random_fail
# ---------------------------------------------------------------------------


def random_fail(
    overrides: dict[str, dict[str, Any]] | None = None,
) -> list[StepSpec]:
    """
    Generate a plan with probabilistic failure.

    Steps:
        1. echo_start: Echo "Starting random test"
        2. random_step: 50% chance of failure
        3. echo_end: Echo "Random test survived" (only if step 2 succeeds)

    Args:
        overrides: Optional dict mapping step_id to param overrides

    Returns:
        List of StepSpec for Engine execution
    """
    overrides = overrides or {}

    steps = [
        _make_step(
            step_id="echo_start",
            name="Echo Start",
            fn_name="step_echo",
            params={"message": "Starting random failure test"},
        ),
        _make_step(
            step_id="random_step",
            name="Random Failure Step",
            fn_name="step_random_fail",
            params={
                "failure_probability": 0.5,
                "success_message": "Survived random failure!",
                "failure_message": "Random failure triggered",
            },
            deps=["echo_start"],
        ),
        _make_step(
            step_id="echo_end",
            name="Echo End",
            fn_name="step_echo",
            params={"message": "Random test completed successfully"},
            deps=["random_step"],
        ),
    ]

    # Apply overrides by step_id
    return _apply_overrides(steps, overrides)


# ---------------------------------------------------------------------------
# Recipe: fail_fast
# ---------------------------------------------------------------------------


def fail_fast(
    overrides: dict[str, dict[str, Any]] | None = None,
) -> list[StepSpec]:
    """
    Generate a plan that fails on the first step.

    Steps:
        1. fail_immediately: Always fails
        2. never_reached: Would echo but never runs

    Args:
        overrides: Optional dict mapping step_id to param overrides

    Returns:
        List of StepSpec for Engine execution
    """
    overrides = overrides or {}

    steps = [
        _make_step(
            step_id="fail_immediately",
            name="Fail Immediately",
            fn_name="step_fail",
            params={"error_message": "Fail fast: first step failure"},
        ),
        _make_step(
            step_id="never_reached",
            name="Never Reached",
            fn_name="step_echo",
            params={"message": "This should never execute"},
            deps=["fail_immediately"],
        ),
    ]

    return _apply_overrides(steps, overrides)


# ---------------------------------------------------------------------------
# Recipe: fail_mid_plan
# ---------------------------------------------------------------------------


def fail_mid_plan(
    overrides: dict[str, dict[str, Any]] | None = None,
) -> list[StepSpec]:
    """
    Generate a plan that succeeds initially then fails mid-execution.

    Steps:
        1. echo_start: Echo "Starting..."
        2. brief_sleep: Sleep 0.3s
        3. mid_failure: Always fails
        4. never_reached: Would echo but never runs

    Args:
        overrides: Optional dict mapping step_id to param overrides

    Returns:
        List of StepSpec for Engine execution
    """
    overrides = overrides or {}

    steps = [
        _make_step(
            step_id="echo_start",
            name="Echo Start",
            fn_name="step_echo",
            params={"message": "Starting mid-fail scenario"},
        ),
        _make_step(
            step_id="brief_sleep",
            name="Brief Sleep",
            fn_name="step_sleep",
            params={"duration_sec": 0.3},
            deps=["echo_start"],
        ),
        _make_step(
            step_id="mid_failure",
            name="Mid Failure",
            fn_name="step_fail",
            params={"error_message": "Planned mid-execution failure"},
            deps=["brief_sleep"],
        ),
        _make_step(
            step_id="never_reached",
            name="Never Reached",
            fn_name="step_echo",
            params={"message": "This should never execute"},
            deps=["mid_failure"],
        ),
    ]

    return _apply_overrides(steps, overrides)


# ---------------------------------------------------------------------------
# Recipe: long_running
# ---------------------------------------------------------------------------


def long_running(
    overrides: dict[str, dict[str, Any]] | None = None,
) -> list[StepSpec]:
    """
    Generate a plan with extended sleep for timeout/cancellation testing.

    Steps:
        1. echo_start: Echo "Starting long run"
        2. long_sleep: Sleep 30s (configurable via overrides)
        3. echo_end: Echo "Long run complete"

    Args:
        overrides: Optional dict mapping step_id to param overrides
            Tip: Override long_sleep params to adjust duration

    Returns:
        List of StepSpec for Engine execution
    """
    overrides = overrides or {}

    steps = [
        _make_step(
            step_id="echo_start",
            name="Echo Start",
            fn_name="step_echo",
            params={"message": "Starting long-running scenario"},
        ),
        _make_step(
            step_id="long_sleep",
            name="Long Sleep",
            fn_name="step_sleep",
            params={"duration_sec": 30.0},  # Default 30s, override for shorter tests
            deps=["echo_start"],
        ),
        _make_step(
            step_id="echo_end",
            name="Echo End",
            fn_name="step_echo",
            params={"message": "Long-running scenario complete"},
            deps=["long_sleep"],
        ),
    ]

    return _apply_overrides(steps, overrides)


# ---------------------------------------------------------------------------
# Recipe: artifact_write
# ---------------------------------------------------------------------------


def artifact_write(
    overrides: dict[str, dict[str, Any]] | None = None,
) -> list[StepSpec]:
    """
    Generate a plan that writes files and registers artifacts.

    Steps:
        1. echo_start: Echo "Starting artifact write"
        2. write_file_1: Write test_output_1.txt
        3. write_file_2: Write test_output_2.txt (parallel-eligible)
        4. echo_end: Echo completion message

    Args:
        overrides: Optional dict mapping step_id to param overrides

    Returns:
        List of StepSpec for Engine execution
    """
    overrides = overrides or {}

    steps = [
        _make_step(
            step_id="echo_start",
            name="Echo Start",
            fn_name="step_echo",
            params={"message": "Starting artifact write scenario"},
        ),
        _make_step(
            step_id="write_file_1",
            name="Write File 1",
            fn_name="step_write_file",
            params={
                "filename": "test_output_1.txt",
                "content": "Content from first write step",
            },
            deps=["echo_start"],
        ),
        _make_step(
            step_id="write_file_2",
            name="Write File 2",
            fn_name="step_write_file",
            params={
                "filename": "test_output_2.txt",
                "content": "Content from second write step",
            },
            deps=["echo_start"],  # Both file writes can run in parallel
        ),
        _make_step(
            step_id="echo_end",
            name="Echo End",
            fn_name="step_echo",
            params={"message": "Artifact write complete"},
            deps=["write_file_1", "write_file_2"],
        ),
    ]

    return _apply_overrides(steps, overrides)


# ---------------------------------------------------------------------------
# Recipe: data_fetch_plan  (planning-time — called from handler, not registry)
# ---------------------------------------------------------------------------


def data_fetch_plan_steps(fetched_message: str) -> list[StepSpec]:
    """
    Generate steps for the data_fetch_plan scenario.

    This function is NOT in the RECIPES registry because it requires the
    already-fetched message to be passed in at plan-generation time. The handler
    fetches the reference document during generate_plan_draft() and then calls
    this function so the fetched content is baked into the step params.

    The resulting plan therefore carries observable proof that the CouchDB
    fetch happened at planning time: the echo_fetched step's message param
    contains the value from the reference document.

    Args:
        fetched_message: The content retrieved from CouchDB during planning.
            Baked verbatim into the echo_fetched step's message param.

    Returns:
        List of StepSpec with fetched data embedded in params
    """
    return [
        _make_step(
            step_id="echo_fetched",
            name="Echo Plan-Time Fetch",
            fn_name="step_echo",
            params={"message": fetched_message},
        ),
        _make_step(
            step_id="echo_confirm",
            name="Confirm Plan-Time Fetch",
            fn_name="step_echo",
            params={"message": "Plan-time CouchDB fetch baked into this plan!"},
            deps=["echo_fetched"],
        ),
    ]


# ---------------------------------------------------------------------------
# Recipe: data_fetch_exec
# ---------------------------------------------------------------------------


def data_fetch_exec(
    overrides: dict[str, dict[str, Any]] | None = None,
) -> list[StepSpec]:
    """
    Generate a plan that fetches from CouchDB at execution time.

    The step_fetch_from_db step uses ctx.data.couchdb() at runtime, so the
    fetch happens when the Engine runs the step — not during planning. The
    fetched document appears in the step's emitted events and result metrics,
    which makes it visible in the execution record.

    Steps:
        1. fetch_doc: Fetch data_access_test:reference_doc from yggdrasil_db
        2. echo_confirm: Echo confirmation message (depends on fetch_doc)

    Args:
        overrides: Optional dict mapping step_id to param overrides.
            Use to point at a different connection/doc_id if needed.

    Returns:
        List of StepSpec for Engine execution
    """
    overrides = overrides or {}

    steps = [
        _make_step(
            step_id="fetch_doc",
            name="Fetch Reference Doc",
            fn_name="step_fetch_from_db",
            params={
                "connection": "yggdrasil_db",
                "doc_id": "data_access_test:reference_doc",
            },
        ),
        _make_step(
            step_id="echo_confirm",
            name="Confirm Execution-Time Fetch",
            fn_name="step_echo",
            params={"message": "Execution-time CouchDB fetch complete!"},
            deps=["fetch_doc"],
        ),
    ]

    return _apply_overrides(steps, overrides)


# ---------------------------------------------------------------------------
# Recipe: data_access_denied
# ---------------------------------------------------------------------------


def data_access_denied(
    overrides: dict[str, dict[str, Any]] | None = None,
) -> list[StepSpec]:
    """
    Generate a plan that verifies DataAccess correctly rejects unauthorized access.

    Two denial cases are tested in sequence:
      1. projects_db — has no data_access block → DataAccessDeniedError (no policy)
      2. mock_resource — has data_access but test_realm not in allowlist
             → DataAccessDeniedError (realm not in allowlist)

    Each step succeeds only if the expected denial is raised; it fails hard
    if access is unexpectedly granted.

    Steps:
        1. verify_no_policy: projects_db has no data_access policy
        2. verify_not_allowlisted: mock_resource allows only tenx
        3. echo_pass: All denials verified

    Args:
        overrides: Optional param overrides by step_id

    Returns:
        List of StepSpec for Engine execution
    """
    overrides = overrides or {}

    steps = [
        _make_step(
            step_id="verify_no_policy",
            name="Verify No-Policy Denial",
            fn_name="step_expect_denied",
            params={"connection": "projects_db"},
        ),
        _make_step(
            step_id="verify_not_allowlisted",
            name="Verify Not-Allowlisted Denial",
            fn_name="step_expect_denied",
            params={"connection": "mock_resource"},
            deps=["verify_no_policy"],
        ),
        _make_step(
            step_id="echo_pass",
            name="All Denials Verified",
            fn_name="step_echo",
            params={"message": "All data-access denial cases passed as expected!"},
            deps=["verify_not_allowlisted"],
        ),
    ]

    return _apply_overrides(steps, overrides)


# ---------------------------------------------------------------------------
# Recipe: data_fetch_all_methods
# ---------------------------------------------------------------------------


def data_fetch_all_methods(
    overrides: dict[str, dict[str, Any]] | None = None,
) -> list[StepSpec]:
    """
    Exercise every read method on CouchDBReadClient in sequence.

    Uses step_exercise_all_fetch_methods which calls get, require, find,
    find_one, fetch_by_field, and require_one in a single step. Each
    method's result is reported in the step metrics and emitted as a
    step.all_fetch_methods event so all outcomes are visible in the
    execution record.

    Steps:
        1. exercise_all: Runs all six fetch methods against yggdrasil_db
        2. echo_confirm: Confirms all methods completed without error

    Args:
        overrides: Optional param overrides by step_id.
            Override exercise_all params to target a different connection,
            doc_id, or selector_type.

    Returns:
        List of StepSpec for Engine execution
    """
    overrides = overrides or {}

    steps = [
        _make_step(
            step_id="exercise_all",
            name="Exercise All Fetch Methods",
            fn_name="step_exercise_all_fetch_methods",
            params={
                "connection": "yggdrasil_db",
                "doc_id": "data_access_test:reference_doc",
                "selector_type": "ygg_test_reference",
            },
        ),
        _make_step(
            step_id="echo_confirm",
            name="All Methods Passed",
            fn_name="step_echo",
            params={"message": "All six fetch methods succeeded end-to-end!"},
            deps=["exercise_all"],
        ),
    ]

    return _apply_overrides(steps, overrides)


# ---------------------------------------------------------------------------
# Recipe: data_verify_limit_clamping
# ---------------------------------------------------------------------------


def data_verify_limit_clamping(
    overrides: dict[str, dict[str, Any]] | None = None,
) -> list[StepSpec]:
    """
    Verify that DataAccess clamps find() results to policy.max_limit.

    Uses the ``yggdrasil_db_clamped`` connection (max_limit: 2). The step
    requests 100 documents but expects at most 2 to be returned, proving
    the policy is enforced by CouchDBReadClient regardless of what the
    caller requests.

    Pre-condition: The ``yggdrasil`` database must contain at least 3
    documents with ``type == "ygg_test_reference"`` so that a non-clamped
    query would return more than ``max_limit``.

    Steps:
        1. verify_clamp: Request 100 docs, confirm at most 2 returned
        2. echo_pass: Confirm clamping enforcement passed

    Args:
        overrides: Optional param overrides by step_id.
            Override verify_clamp params (e.g. expected_max) if the
            connection's max_limit differs from the default 2.

    Returns:
        List of StepSpec for Engine execution
    """
    overrides = overrides or {}

    steps = [
        _make_step(
            step_id="verify_clamp",
            name="Verify Limit Clamping",
            fn_name="step_verify_limit_clamping",
            params={
                "connection": "yggdrasil_db_clamped",
                "selector_type": "ygg_test_reference",
                "request_limit": 100,
                "expected_max": 2,
            },
        ),
        _make_step(
            step_id="echo_pass",
            name="Clamping Enforced",
            fn_name="step_echo",
            params={"message": "max_limit clamping enforced — policy is working!"},
            deps=["verify_clamp"],
        ),
    ]

    return _apply_overrides(steps, overrides)


# ---------------------------------------------------------------------------
# Helper: Apply parameter overrides
# ---------------------------------------------------------------------------


def _apply_overrides(
    steps: list[StepSpec],
    overrides: dict[str, dict[str, Any]],
) -> list[StepSpec]:
    """
    Apply parameter overrides to steps by step_id.

    Args:
        steps: List of StepSpec to modify
        overrides: Dict mapping step_id to param dict overrides

    Returns:
        Modified list with overrides applied
    """
    if not overrides:
        return steps

    for step in steps:
        if step.step_id in overrides:
            # Merge overrides into existing params
            step.params.update(overrides[step.step_id])

    return steps


# ---------------------------------------------------------------------------
# Recipe registry
# ---------------------------------------------------------------------------

RECIPES: dict[str, Any] = {
    "happy_path": happy_path,
    "random_fail": random_fail,
    "fail_fast": fail_fast,
    "fail_mid_plan": fail_mid_plan,
    "long_running": long_running,
    "artifact_write": artifact_write,
    "data_fetch_exec": data_fetch_exec,
    "data_access_denied": data_access_denied,
    "data_fetch_all_methods": data_fetch_all_methods,
    "data_verify_limit_clamping": data_verify_limit_clamping,
}


def get_recipe(name: str):
    """
    Get recipe function by name.

    Args:
        name: Recipe name (e.g., "happy_path")

    Returns:
        Recipe builder function

    Raises:
        KeyError: If recipe name not found
    """
    if name not in RECIPES:
        raise KeyError(
            f"Unknown test realm recipe: {name}. " f"Available: {list(RECIPES.keys())}"
        )
    return RECIPES[name]
