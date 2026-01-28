"""
Test realm templates for generating test plans.

Templates are factory functions that return a list of StepSpec objects.
Each template represents a different test scenario:

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
# Template: happy_path
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
# Template: random_fail
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
# Template: fail_fast
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
# Template: fail_mid_plan
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
# Template: long_running
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
# Template: artifact_write
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
# Template registry
# ---------------------------------------------------------------------------

TEMPLATES: dict[str, Any] = {
    "happy_path": happy_path,
    "random_fail": random_fail,
    "fail_fast": fail_fast,
    "fail_mid_plan": fail_mid_plan,
    "long_running": long_running,
    "artifact_write": artifact_write,
}


def get_template(name: str):
    """
    Get template function by name.

    Args:
        name: Template name (e.g., "happy_path")

    Returns:
        Template builder function

    Raises:
        KeyError: If template name not found
    """
    if name not in TEMPLATES:
        raise KeyError(
            f"Unknown test realm template: {name}. "
            f"Available: {list(TEMPLATES.keys())}"
        )
    return TEMPLATES[name]
