"""Showing a recorded value in a diagnostic: bounded, and never raising."""

import unittest

from yggdrasil.contract.diagnostics import MAX_LENGTH, describe_recorded

# More digits than Python converts to text (4300 by default).
HUGE = 10**5000


def nested(depth: int) -> list:
    """A list nested depth levels deep around a single integer."""
    value: list = [1]
    for _ in range(depth - 1):
        value = [value]
    return value


class _Unrepresentable:
    """A value whose representation fails."""

    def __repr__(self) -> str:
        raise RuntimeError("no representation")


class TestDescribeRecorded(unittest.TestCase):
    """One renderer for every recorded value a diagnostic shows."""

    def test_a_short_value_is_shown_as_its_representation(self):
        for value, shown in (
            ("5", "'5'"),
            (5, "5"),
            (5.0, "5.0"),
            (True, "True"),
            (None, "None"),
            ([5], "[5]"),
            ({"value": 5}, "{'value': 5}"),
            ("", "''"),
        ):
            with self.subTest(shown=shown):
                self.assertEqual(describe_recorded(value), shown)

    def test_a_huge_integer_is_shown_by_its_type(self):
        for value in (HUGE, -HUGE):
            with self.subTest(sign="-" if value < 0 else "+"):
                self.assertEqual(describe_recorded(value), "<unrepresentable int>")

    def test_a_container_holding_a_huge_integer_is_shown_by_its_type(self):
        for value, shown in (
            ([HUGE], "<unrepresentable list>"),
            ({"family": [HUGE]}, "<unrepresentable dict>"),
            ({"a": {"b": [None, HUGE]}}, "<unrepresentable dict>"),
        ):
            with self.subTest(shown=shown):
                self.assertEqual(describe_recorded(value), shown)

    def test_a_value_whose_representation_fails_is_still_described(self):
        shown = describe_recorded(_Unrepresentable())
        self.assertIn("_Unrepresentable", shown)
        self.assertLessEqual(len(shown), MAX_LENGTH)

    def test_a_long_integer_that_can_be_shown_is_shortened(self):
        value = 10**4000
        shown = describe_recorded(value)
        self.assertLessEqual(len(shown), MAX_LENGTH)
        self.assertTrue(shown.startswith("1000"), shown)

    def test_a_long_string_is_shortened(self):
        shown = describe_recorded("x" * 100_000)
        self.assertLessEqual(len(shown), MAX_LENGTH)
        self.assertTrue(shown.startswith("'xxx"), shown)

    def test_wide_and_deep_values_are_bounded(self):
        cases = {
            "long list": list(range(100_000)),
            "wide dict": {f"key{i}": i for i in range(100_000)},
            "deep list": nested(500),
            "many long strings": [{"k" * 1000: "v" * 1000} for _ in range(10)],
            "deep dicts of long strings": {"a" * 500: {"b" * 500: {"c" * 500: 1}}},
        }
        for case, value in cases.items():
            with self.subTest(case=case):
                shown = describe_recorded(value)
                self.assertIsInstance(shown, str)
                self.assertLessEqual(len(shown), MAX_LENGTH)

    def test_a_cut_description_says_it_was_cut(self):
        shown = describe_recorded([{"k" * 1000: "v" * 1000} for _ in range(10)])
        self.assertEqual(len(shown), MAX_LENGTH)
        self.assertTrue(shown.endswith("..."), shown)


if __name__ == "__main__":
    unittest.main()
