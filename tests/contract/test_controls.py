"""The control checks: strict tokens, a known failure policy, every execution field.

Each case starts from a document the plan-document builder made and changes
one field, so it shows exactly which field the check judges.
"""

import unittest
from collections.abc import Callable
from typing import Any

from tests.plan_store_support import SCOPE, make_plan
from yggdrasil.contract.controls import (
    FAILURE_POLICIES,
    MAX_TOKEN,
    PLAN_DEFINITION_FIELDS,
    STEP_DEFINITION_FIELDS,
    FailurePolicyCheck,
    TokenPair,
    execution_definition_problems,
    failure_policy_of,
    token_pair,
)
from yggdrasil.flow.model import VALID_FAILURE_POLICIES
from yggdrasil.storage.plan_documents import build_plan_document

# Stands for a field removed from the built document.
ABSENT = object()

# Stored values of awkward shapes. 10**5000 is too long for Python to render
# as text, so problem descriptions have to cope without it.
HOSTILE: tuple[Any, ...] = (
    None,
    True,
    5.0,
    float("nan"),
    float("inf"),
    10**400,
    -(10**400),
    10**5000,
    "",
    "x" * 10_000,
    [],
    [[[[[[[[[[1]]]]]]]]]],
    [10**5000],
    {},
    {"a": {"b": [None]}},
    [{"step_id": "s1"}] * 3,
)


def _put(container: dict[str, Any], key: str, value: Any) -> None:
    """Set key in container to value, or remove it for ABSENT."""
    if value is ABSENT:
        del container[key]
    else:
        container[key] = value


def plan_document(**fields: Any) -> dict[str, Any]:
    """A built plan document with top-level fields replaced; ABSENT removes one."""
    doc = build_plan_document(make_plan(), "test_realm", dict(SCOPE), auto_run=True)
    for key, value in fields.items():
        _put(doc, key, value)
    return doc


def with_plan_field(key: str, value: Any) -> dict[str, Any]:
    """A built plan document with one ``plan`` field replaced or removed."""
    doc = plan_document()
    _put(doc["plan"], key, value)
    return doc


def with_step(index: int, value: Any) -> dict[str, Any]:
    """A built plan document with one step replaced by value."""
    doc = plan_document()
    doc["plan"]["steps"][index] = value
    return doc


def with_step_field(index: int, key: str, value: Any) -> dict[str, Any]:
    """A built plan document with one field of one step replaced or removed."""
    doc = plan_document()
    _put(doc["plan"]["steps"][index], key, value)
    return doc


class TestTokenPair(unittest.TestCase):
    """One rule for both tokens: present, true ints, in range, executed <= run."""

    def test_a_built_document_has_a_valid_pair(self):
        self.assertEqual(token_pair(plan_document()), TokenPair(True, 0, -1, ()))

    def test_valid_pairs(self):
        for run, executed in (
            (0, -1),
            (0, 0),
            (4, 3),
            (4, 4),
            (MAX_TOKEN, -1),
            (MAX_TOKEN, MAX_TOKEN),
        ):
            with self.subTest(run=run, executed=executed):
                document = plan_document(run_token=run, executed_run_token=executed)
                self.assertEqual(
                    token_pair(document), TokenPair(True, run, executed, ())
                )

    def test_each_invalid_run_token_is_refused_on_its_own(self):
        for value in (
            ABSENT,
            None,
            "5",
            5.0,
            True,
            False,
            -1,
            MAX_TOKEN + 1,
            [5],
            {"value": 5},
        ):
            with self.subTest(run_token=value):
                pair = token_pair(plan_document(run_token=value, executed_run_token=-1))
                self.assertFalse(pair.valid)
                self.assertIsNone(pair.run)
                self.assertEqual(pair.executed, -1)  # still valid on its own
                self.assertEqual(len(pair.problems), 1, pair.problems)
                self.assertTrue(pair.problems[0].startswith("run_token "))

    def test_each_invalid_executed_token_is_refused_on_its_own(self):
        for value in (
            ABSENT,
            None,
            "5",
            5.0,
            True,
            False,
            -2,
            MAX_TOKEN + 1,
            [5],
            {"value": 5},
        ):
            with self.subTest(executed_run_token=value):
                pair = token_pair(
                    plan_document(run_token=MAX_TOKEN, executed_run_token=value)
                )
                self.assertFalse(pair.valid)
                self.assertEqual(pair.run, MAX_TOKEN)  # still valid on its own
                self.assertIsNone(pair.executed)
                self.assertEqual(len(pair.problems), 1, pair.problems)
                self.assertTrue(pair.problems[0].startswith("executed_run_token "))

    def test_executed_above_run_is_invalid_though_each_token_is_valid(self):
        pair = token_pair(plan_document(run_token=4, executed_run_token=5))
        self.assertEqual(
            pair,
            TokenPair(False, 4, 5, ("executed_run_token 5 is above run_token 4",)),
        )

    def test_problems_name_the_field_and_the_recorded_value(self):
        cases = [
            ({"run_token": "5"}, "run_token '5' is a str, not a JSON integer"),
            ({"run_token": 5.0}, "run_token 5.0 is a float, not a JSON integer"),
            ({"run_token": True}, "run_token True is a bool, not a JSON integer"),
            ({"run_token": ABSENT}, "run_token is missing"),
            ({"run_token": None}, "run_token is null"),
            ({"run_token": -1}, "run_token -1 is below 0"),
            ({"executed_run_token": -2}, "executed_run_token -2 is below -1"),
            (
                {"run_token": MAX_TOKEN + 1},
                f"run_token {MAX_TOKEN + 1} is above the maximum {MAX_TOKEN}",
            ),
        ]
        for fields, problem in cases:
            with self.subTest(problem=problem):
                pair = token_pair(plan_document(**fields))
                self.assertEqual(pair.problems, (problem,))

    def test_both_tokens_invalid_reports_both(self):
        pair = token_pair(plan_document(run_token="5", executed_run_token=ABSENT))
        self.assertEqual(
            pair.problems,
            (
                "run_token '5' is a str, not a JSON integer",
                "executed_run_token is missing",
            ),
        )

    def test_a_document_that_is_not_an_object_has_no_valid_pair(self):
        for document in (None, [], [{"run_token": 1}], "doc", 4):
            with self.subTest(document=document):
                pair = token_pair(document)
                self.assertEqual(
                    (pair.valid, pair.run, pair.executed), (False, None, None)
                )
                self.assertEqual(len(pair.problems), 1)
                self.assertTrue(pair.problems[0].startswith("the plan document "))


class TestFailurePolicyOf(unittest.TestCase):
    """The policy must be present and known; nothing defaults to fail_fast."""

    def test_vocabulary_agrees_with_the_flow_model(self):
        self.assertEqual(FAILURE_POLICIES, VALID_FAILURE_POLICIES)

    def test_each_known_policy_is_valid(self):
        for policy in sorted(FAILURE_POLICIES):
            with self.subTest(policy=policy):
                document = with_plan_field("failure_policy", policy)
                self.assertEqual(
                    failure_policy_of(document), FailurePolicyCheck(True, policy, ())
                )

    def test_absent_or_unknown_policy_is_invalid(self):
        known = "['continue_independent', 'fail_fast']"
        cases = [
            (ABSENT, "plan.failure_policy is missing"),
            (None, "plan.failure_policy is null"),
            (7, "plan.failure_policy 7 is an int, not a string"),
            (
                ["fail_fast"],
                "plan.failure_policy ['fail_fast'] is a list, not a string",
            ),
            ("Fail_Fast", f"plan.failure_policy 'Fail_Fast' is not one of {known}"),
            ("bogus", f"plan.failure_policy 'bogus' is not one of {known}"),
            ("", f"plan.failure_policy '' is not one of {known}"),
        ]
        for value, problem in cases:
            with self.subTest(value=value):
                result = failure_policy_of(with_plan_field("failure_policy", value))
                self.assertEqual(result, FailurePolicyCheck(False, None, (problem,)))

    def test_a_missing_or_malformed_plan_is_named(self):
        cases = [
            (plan_document(plan=ABSENT), "plan is missing"),
            (plan_document(plan=None), "plan is null"),
            (plan_document(plan=["steps"]), "plan ['steps'] is a list, not an object"),
            (plan_document(plan="p"), "plan 'p' is a str, not an object"),
            ([], "the plan document [] is a list, not an object"),
            (None, "the plan document None is a NoneType, not an object"),
        ]
        for document, problem in cases:
            with self.subTest(problem=problem):
                result = failure_policy_of(document)
                self.assertEqual(result, FailurePolicyCheck(False, None, (problem,)))


class TestExecutionDefinitionProblems(unittest.TestCase):
    """Every execution field is present before deserialization can default it."""

    def test_the_required_fields_are_the_contracts(self):
        self.assertEqual(
            PLAN_DEFINITION_FIELDS,
            ("plan_id", "realm", "scope", "steps", "failure_policy"),
        )
        self.assertEqual(
            STEP_DEFINITION_FIELDS,
            (
                "step_id",
                "name",
                "fn_ref",
                "params",
                "deps",
                "scope",
                "inputs",
                "outputs",
            ),
        )

    def test_a_built_document_is_complete(self):
        self.assertEqual(execution_definition_problems(plan_document()), ())

    def test_each_missing_or_null_plan_field_is_named(self):
        for key in PLAN_DEFINITION_FIELDS:
            for value, state in ((ABSENT, "missing"), (None, "null")):
                with self.subTest(field=key, state=state):
                    problems = execution_definition_problems(
                        with_plan_field(key, value)
                    )
                    self.assertEqual(problems, (f"plan.{key} is {state}",))

    def test_each_missing_or_null_step_field_is_named(self):
        for key in STEP_DEFINITION_FIELDS:
            for value, state in ((ABSENT, "missing"), (None, "null")):
                with self.subTest(field=key, state=state):
                    problems = execution_definition_problems(
                        with_step_field(1, key, value)
                    )
                    self.assertEqual(problems, (f"plan.steps[1].{key} is {state}",))

    def test_every_problem_is_reported_at_once(self):
        doc = plan_document()
        del doc["plan"]["failure_policy"]
        del doc["plan"]["steps"][0]["deps"]
        doc["plan"]["steps"][1]["outputs"] = None

        self.assertEqual(
            execution_definition_problems(doc),
            (
                "plan.failure_policy is missing",
                "plan.steps[0].deps is missing",
                "plan.steps[1].outputs is null",
            ),
        )

    def test_steps_must_be_a_list(self):
        cases = [
            ({"s1": {}}, "plan.steps {'s1': {}} is a dict, not an array"),
            ("s1", "plan.steps 's1' is a str, not an array"),
            (3, "plan.steps 3 is an int, not an array"),
        ]
        for value, problem in cases:
            with self.subTest(steps=value):
                problems = execution_definition_problems(
                    with_plan_field("steps", value)
                )
                self.assertEqual(problems, (problem,))

    def test_each_step_must_be_an_object(self):
        doc = plan_document()
        doc["plan"]["steps"].extend(["s3", 7])

        self.assertEqual(
            execution_definition_problems(doc),
            (
                "plan.steps[2] 's3' is a str, not an object",
                "plan.steps[3] 7 is an int, not an object",
            ),
        )

    def test_a_missing_or_malformed_plan_is_the_only_problem(self):
        cases = [
            (plan_document(plan=ABSENT), "plan is missing"),
            (plan_document(plan=None), "plan is null"),
            (plan_document(plan=[]), "plan [] is a list, not an object"),
            ("doc", "the plan document 'doc' is a str, not an object"),
        ]
        for document, problem in cases:
            with self.subTest(problem=problem):
                self.assertEqual(execution_definition_problems(document), (problem,))

    def test_values_are_left_to_deserialization_and_preflight(self):
        # Presence only: an empty step list, or a dependency list of the wrong
        # type, is present. Judging values is deserialization's and preflight's job.
        doc = plan_document()
        doc["plan"]["scope"] = "not an object"
        doc["plan"]["steps"][0]["deps"] = "s0"
        doc["plan"]["steps"][0]["step_id"] = 7
        doc["plan"]["steps"][1]["params"] = []
        self.assertEqual(execution_definition_problems(doc), ())
        self.assertEqual(
            execution_definition_problems(with_plan_field("steps", [])), ()
        )


class TestNoStoredValueMakesACheckRaise(unittest.TestCase):
    """Whatever a stored document holds, every check answers."""

    PLACEMENTS: dict[str, Callable[[Any], Any]] = {
        "document": lambda value: value,
        "run_token": lambda value: plan_document(run_token=value),
        "executed_run_token": lambda value: plan_document(executed_run_token=value),
        "plan": lambda value: plan_document(plan=value),
        "plan.failure_policy": lambda value: with_plan_field("failure_policy", value),
        "plan.steps": lambda value: with_plan_field("steps", value),
        "plan.steps[1]": lambda value: with_step(1, value),
        "plan.steps[1].deps": lambda value: with_step_field(1, "deps", value),
    }

    # Where every hostile value is invalid, and for which checks.
    INVALID_FOR: dict[str, tuple[str, ...]] = {
        "document": ("tokens", "policy", "definition"),
        "run_token": ("tokens",),
        "executed_run_token": ("tokens",),
        "plan": ("policy", "definition"),
        "plan.failure_policy": ("policy",),
        "plan.steps[1]": ("definition",),
    }

    def test_hostile_values_in_every_position(self):
        for where, place in self.PLACEMENTS.items():
            for case, value in enumerate(HOSTILE):
                # value itself may be too long to render as a subTest label.
                with self.subTest(where=where, case=case, type=type(value).__name__):
                    document = place(value)
                    pair = token_pair(document)
                    policy = failure_policy_of(document)
                    definition = execution_definition_problems(document)

                    verdicts = {
                        "tokens": pair.valid,
                        "policy": policy.valid,
                        "definition": not definition,
                    }
                    for check in self.INVALID_FOR.get(where, ()):
                        self.assertFalse(verdicts[check], check)
                    for problem in (*pair.problems, *policy.problems, *definition):
                        self.assertIsInstance(problem, str)
                        self.assertLess(len(problem), 200, problem)


if __name__ == "__main__":
    unittest.main()
