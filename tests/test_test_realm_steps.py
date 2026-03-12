"""
Regression tests for lib/realms/test_realm/steps.py.

Verifies that every step function registered in the test realm is a proper
Yggdrasil Flow step (i.e. decorated with @step), so that lifecycle events
are emitted automatically and we never silently regress to plain functions.
"""

import unittest

from yggdrasil.flow.utils.callable_ref import resolve_callable

_FN_REF_PREFIX = "lib.realms.test_realm.steps"

# All fn_ref names referenced from test_realm recipes / custom step parsing.
_ALL_STEP_NAMES = [
    "step_echo",
    "step_sleep",
    "step_fail",
    "step_write_file",
    "step_random_fail",
    "step_fetch_from_db",
    "step_expect_denied",
    "step_exercise_all_fetch_methods",
    "step_verify_limit_clamping",
    "step_emit_metadata",
]


class TestTestRealmStepsAreDecorated(unittest.TestCase):
    """
    Ensure every test realm step resolved via fn_ref carries the _step_name
    attribute that the @step decorator sets. If a step loses its decorator,
    hasattr(fn, '_step_name') will be False and this test will catch it.
    """

    def _fn_ref(self, name: str) -> str:
        return f"{_FN_REF_PREFIX}.{name}"

    def test_all_steps_are_callable(self):
        """resolve_callable must return a callable for every registered step."""
        for name in _ALL_STEP_NAMES:
            with self.subTest(step=name):
                fn = resolve_callable(self._fn_ref(name))
                self.assertTrue(
                    callable(fn),
                    f"resolve_callable('{name}') did not return a callable",
                )

    def test_all_steps_have_step_name_attribute(self):
        """
        Every step must carry _step_name — the attribute set by @step.
        If this fails, the function is a plain def that will never emit
        step.started / step.succeeded / step.failed.
        """
        for name in _ALL_STEP_NAMES:
            with self.subTest(step=name):
                fn = resolve_callable(self._fn_ref(name))
                self.assertTrue(
                    hasattr(fn, "_step_name"),
                    f"Step '{name}' is missing _step_name — did you forget @step?",
                )

    def test_step_name_attribute_matches_function_name(self):
        """_step_name should match the bare function name (decorator default)."""
        for name in _ALL_STEP_NAMES:
            with self.subTest(step=name):
                fn = resolve_callable(self._fn_ref(name))
                if hasattr(fn, "_step_name"):
                    self.assertEqual(
                        fn._step_name,
                        name,
                        f"Step '{name}' has _step_name={fn._step_name!r}, expected {name!r}",
                    )
