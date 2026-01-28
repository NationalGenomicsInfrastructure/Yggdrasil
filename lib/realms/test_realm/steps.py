"""
Test realm steps for dev/test scenarios.

These steps are designed for testing the Yggdrasil execution pipeline:
- step_echo: Simple success with message emission
- step_sleep: Configurable delay (for long-running scenarios)
- step_fail: Always fails with configurable message
- step_write_file: Writes a file and registers artifact
- step_random_fail: Probabilistic failure for chaos testing

NOTE: These steps are synchronous (def, not async def) because the
Engine.run() method is synchronous. Using async def would return
unawaited coroutines.
"""

import random
import time
from pathlib import Path
from typing import Any

from yggdrasil.flow.model import StepResult
from yggdrasil.flow.step import StepContext


class _SimpleRef:
    """Simple artifact reference for test realm step registration."""

    def __init__(self, key: str):
        self.key_val = key

    def key(self) -> str:
        return self.key_val

    def resolve_path(self, scope_dir: Path) -> Path:
        # Not used for registration, but required by protocol
        return scope_dir / self.key_val


def step_echo(ctx: StepContext, message: str = "Hello from test realm") -> StepResult:
    """
    Simple echo step that emits a message and succeeds.

    Args:
        ctx: Step execution context
        message: Message to echo (default: "Hello from test realm")

    Returns:
        StepResult with message in metrics
    """
    ctx.emit("step.echo", message=message)
    return StepResult(metrics={"echoed_message": message})


def step_sleep(ctx: StepContext, duration_sec: float = 1.0) -> StepResult:
    """
    Sleep step for simulating long-running operations.

    Emits progress events at 25%, 50%, 75%, and 100%.

    Args:
        ctx: Step execution context
        duration_sec: Sleep duration in seconds (default: 1.0)

    Returns:
        StepResult with actual sleep duration in metrics
    """
    quarter = duration_sec / 4.0

    ctx.emit("step.progress", percent=0, message="Starting sleep")
    time.sleep(quarter)
    ctx.emit("step.progress", percent=25, message="25% complete")
    time.sleep(quarter)
    ctx.emit("step.progress", percent=50, message="50% complete")
    time.sleep(quarter)
    ctx.emit("step.progress", percent=75, message="75% complete")
    time.sleep(quarter)
    ctx.emit("step.progress", percent=100, message="Sleep complete")

    return StepResult(metrics={"slept_seconds": duration_sec})


def step_fail(
    ctx: StepContext, error_message: str = "Intentional failure"
) -> StepResult:
    """
    Always-fail step for testing failure handling.

    Args:
        ctx: Step execution context
        error_message: Error message to raise (default: "Intentional failure")

    Raises:
        RuntimeError: Always raised with the configured message
    """
    ctx.emit("step.failing", reason=error_message)
    raise RuntimeError(error_message)


def step_write_file(
    ctx: StepContext,
    filename: str = "test_output.txt",
    content: str = "Test content from test realm",
) -> StepResult:
    """
    Write a file to workdir and register it as an artifact.

    Args:
        ctx: Step execution context
        filename: Name of file to create (default: "test_output.txt")
        content: Content to write (default: "Test content from test realm")

    Returns:
        StepResult with artifact registered
    """
    # Write to workdir
    output_path = ctx.workdir / filename
    output_path.write_text(content)

    # Register artifact using record_artifact
    # Create a simple artifact ref for registration
    artifact = ctx.record_artifact(
        _SimpleRef(key=f"test_file:{filename}"),
        path=output_path,
    )

    return StepResult(
        artifacts=[artifact],
        metrics={"bytes_written": len(content), "filename": filename},
    )


def step_random_fail(
    ctx: StepContext,
    failure_probability: float = 0.5,
    success_message: str = "Lucky! Survived random failure",
    failure_message: str = "Unlucky! Random failure triggered",
) -> StepResult:
    """
    Probabilistic failure step for chaos testing.

    Args:
        ctx: Step execution context
        failure_probability: Probability of failure, 0.0-1.0 (default: 0.5)
        success_message: Message on success
        failure_message: Message on failure

    Returns:
        StepResult if successful

    Raises:
        RuntimeError: With configured probability
    """
    roll = random.random()
    ctx.emit("step.random_roll", roll=roll, threshold=failure_probability)

    if roll < failure_probability:
        ctx.emit("step.failing", reason=failure_message, roll=roll)
        raise RuntimeError(failure_message)

    return StepResult(
        metrics={
            "roll": roll,
            "threshold": failure_probability,
            "survived": True,
            "message": success_message,
        }
    )


# ---------------------------------------------------------------------------
# Step registry for fn_ref resolution
# ---------------------------------------------------------------------------

STEPS: dict[str, Any] = {
    "step_echo": step_echo,
    "step_sleep": step_sleep,
    "step_fail": step_fail,
    "step_write_file": step_write_file,
    "step_random_fail": step_random_fail,
}


def get_step_fn(name: str):
    """
    Get step function by name.

    Args:
        name: Step function name (e.g., "step_echo")

    Returns:
        The step function (synchronous)

    Raises:
        KeyError: If step name not found
    """
    if name not in STEPS:
        raise KeyError(f"Unknown test realm step: {name}")
    return STEPS[name]
