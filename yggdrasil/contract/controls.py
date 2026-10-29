"""Checks on the control fields that decide whether and how a plan runs.

A missing, null or malformed control field is invalid control state, never a
default: there is no "an absent ``run_token`` means 0", and no "an absent
failure policy means ``fail_fast``". Each function below reads one part of
that state from a stored plan document and returns its verdict together with
one description per problem, naming the field and the recorded value:

- :func:`token_pair`: the execution tokens, under the strict token rule;
- :func:`failure_policy_of`: the plan's failure policy;
- :func:`execution_definition_problems`: whether the plan and its steps carry
  every execution field, before deserialization can fill one in with a
  default.

Stored content never makes them raise, whatever its shape, so every caller
that has to decide something about a stored plan gets the same answer from
the same rule.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from yggdrasil.contract.diagnostics import describe_recorded

RUN_TOKEN_FIELD = "run_token"
EXECUTED_RUN_TOKEN_FIELD = "executed_run_token"
PLAN_FIELD = "plan"
FAILURE_POLICY_FIELD = "failure_policy"
STEPS_FIELD = "steps"

# The token bounds. ``executed_run_token`` is -1 until a first request is
# recorded. The upper bound, 2**53 - 1, is the largest integer that every JSON
# client, browsers included, represents exactly.
MIN_RUN_TOKEN = 0
MIN_EXECUTED_RUN_TOKEN = -1
MAX_TOKEN = 2**53 - 1

# The failure policies a plan may name. Plain strings, so the contract does
# not depend on ``yggdrasil.flow``; a test keeps them equal to its vocabulary.
FAILURE_POLICIES = frozenset({"fail_fast", "continue_independent"})

# The fields the plan and each of its steps must carry. Deserialization
# supplies a default for most of them, so their presence is checked first.
PLAN_DEFINITION_FIELDS = ("plan_id", "realm", "scope", "steps", "failure_policy")
STEP_DEFINITION_FIELDS = (
    "step_id",
    "name",
    "fn_ref",
    "params",
    "deps",
    "scope",
    "inputs",
    "outputs",
)


@dataclass(frozen=True)
class TokenPair:
    """A plan document's execution tokens, and whether they form a valid pair.

    The pair is valid when both tokens are present JSON integers that are not
    booleans, ``0 <= run <= MAX_TOKEN``, ``-1 <= executed <= MAX_TOKEN``, and
    ``executed <= run``. Callers use ``run`` and ``executed`` only when
    ``valid``.

    Attributes:
        valid: Whether the pair is valid.
        run: ``run_token``, if it is valid on its own; None otherwise.
        executed: ``executed_run_token``, if it is valid on its own; None
            otherwise.
        problems: One description per problem; empty when the pair is valid.
    """

    valid: bool
    run: int | None
    executed: int | None
    problems: tuple[str, ...]


@dataclass(frozen=True)
class FailurePolicyCheck:
    """A plan document's failure policy, and whether it is a known one.

    Attributes:
        valid: Whether ``plan.failure_policy`` is present and one of
            FAILURE_POLICIES.
        policy: The policy, when valid; None otherwise.
        problems: One description per problem; empty when valid.
    """

    valid: bool
    policy: str | None
    problems: tuple[str, ...]


def token_pair(document: object) -> TokenPair:
    """Read a plan document's execution tokens under the strict token rule.

    The same rule judges a stored pair and the pair a write would produce, so
    a caller that is about to change one token passes the document as it
    would be written.

    Args:
        document: The plan document, of any shape.

    Returns:
        TokenPair: The tokens and the verdict; never raises on stored content.
    """
    if not isinstance(document, Mapping):
        problem = _not_expected("the plan document", document, "an object")
        return TokenPair(valid=False, run=None, executed=None, problems=(problem,))
    run, run_problem = _token(document, RUN_TOKEN_FIELD, MIN_RUN_TOKEN)
    executed, executed_problem = _token(
        document, EXECUTED_RUN_TOKEN_FIELD, MIN_EXECUTED_RUN_TOKEN
    )
    problems = [p for p in (run_problem, executed_problem) if p is not None]
    if run is not None and executed is not None and executed > run:
        problems.append(
            f"{EXECUTED_RUN_TOKEN_FIELD} {executed} is above {RUN_TOKEN_FIELD} {run}"
        )
    return TokenPair(
        valid=not problems, run=run, executed=executed, problems=tuple(problems)
    )


def failure_policy_of(document: object) -> FailurePolicyCheck:
    """Read a plan document's failure policy, with no default.

    Valid only when ``plan.failure_policy`` is present and one of
    FAILURE_POLICIES, matched exactly: ``"Fail_Fast"`` is unknown.

    Args:
        document: The plan document, of any shape.

    Returns:
        FailurePolicyCheck: The policy and the verdict; never raises on stored
        content.
    """
    plan = _plan_of(document)
    if isinstance(plan, str):
        return _invalid_policy(plan)
    path = f"{PLAN_FIELD}.{FAILURE_POLICY_FIELD}"
    absent = _absent(plan, FAILURE_POLICY_FIELD, path)
    if absent is not None:
        return _invalid_policy(absent)
    value = plan[FAILURE_POLICY_FIELD]
    if not isinstance(value, str):
        return _invalid_policy(_not_expected(path, value, "a string"))
    if value not in FAILURE_POLICIES:
        known = sorted(FAILURE_POLICIES)
        return _invalid_policy(
            f"{path} {describe_recorded(value)} is not one of {known}"
        )
    return FailurePolicyCheck(valid=True, policy=value, problems=())


def execution_definition_problems(document: object) -> tuple[str, ...]:
    """Return what is missing from a plan document's execution definition.

    Checks presence, not values: ``plan`` is an object carrying every field of
    PLAN_DEFINITION_FIELDS, ``plan.steps`` is a list of objects, and each step
    carries every field of STEP_DEFINITION_FIELDS. A field holding ``null``
    counts as missing. What the values hold (types inside containers, step
    IDs, dependency targets) is left to deserialization and preflight.

    Without this check, deserialization fills the gaps with defaults: a plan
    without ``steps`` becomes an empty plan that succeeds, and a step without
    ``deps`` loses its ordering.

    Args:
        document: The plan document, of any shape.

    Returns:
        tuple[str, ...]: One problem per missing field, naming its path, such
        as ``plan.steps[2].deps is missing``; empty when the definition is
        complete. Never raises on stored content.
    """
    plan = _plan_of(document)
    if isinstance(plan, str):
        return (plan,)
    problems = _absent_fields(plan, PLAN_DEFINITION_FIELDS, PLAN_FIELD)
    steps = plan.get(STEPS_FIELD)
    if steps is None:  # missing or null, already reported
        return tuple(problems)
    if not isinstance(steps, list):
        problems.append(_not_expected(f"{PLAN_FIELD}.{STEPS_FIELD}", steps, "an array"))
        return tuple(problems)
    for index, step in enumerate(steps):
        path = f"{PLAN_FIELD}.{STEPS_FIELD}[{index}]"
        if isinstance(step, Mapping):
            problems.extend(_absent_fields(step, STEP_DEFINITION_FIELDS, path))
        else:
            problems.append(_not_expected(path, step, "an object"))
    return tuple(problems)


def _token(
    document: Mapping[str, Any], field: str, minimum: int
) -> tuple[int | None, str | None]:
    """Return one token if it is valid on its own, else the problem with it."""
    absent = _absent(document, field, field)
    if absent is not None:
        return None, absent
    value = document[field]
    # bool is a subclass of int, and 5.0 == 5: only a true int is a token.
    if type(value) is not int:
        return None, _not_expected(field, value, "a JSON integer")
    if value < minimum:
        return None, f"{field} {describe_recorded(value)} is below {minimum}"
    if value > MAX_TOKEN:
        return (
            None,
            f"{field} {describe_recorded(value)} is above the maximum {MAX_TOKEN}",
        )
    return value, None


def _invalid_policy(problem: str) -> FailurePolicyCheck:
    """Return the verdict on a failure policy that is not valid."""
    return FailurePolicyCheck(valid=False, policy=None, problems=(problem,))


def _plan_of(document: object) -> Mapping[str, Any] | str:
    """Return a plan document's ``plan`` object, or the problem preventing it."""
    if not isinstance(document, Mapping):
        return _not_expected("the plan document", document, "an object")
    absent = _absent(document, PLAN_FIELD, PLAN_FIELD)
    if absent is not None:
        return absent
    plan = document[PLAN_FIELD]
    if not isinstance(plan, Mapping):
        return _not_expected(PLAN_FIELD, plan, "an object")
    return plan


def _absent_fields(
    container: Mapping[str, Any], fields: tuple[str, ...], path: str
) -> list[str]:
    """Return a problem for each of fields that container lacks or holds as null."""
    return [
        problem
        for field in fields
        if (problem := _absent(container, field, f"{path}.{field}")) is not None
    ]


def _absent(container: Mapping[str, Any], field: str, path: str) -> str | None:
    """Return the problem if field is missing or null in container, else None."""
    if field not in container:
        return f"{path} is missing"
    if container[field] is None:
        return f"{path} is null"
    return None


def _not_expected(path: str, value: object, expected: str) -> str:
    """Describe a recorded value of the wrong kind, with its type."""
    name = type(value).__name__
    article = "an" if name[:1].lower() in "aeiou" else "a"
    return f"{path} {describe_recorded(value)} is {article} {name}, not {expected}"
