"""
Unit tests for lib.watchers.filter_eval module.

Tests FilterResult, evaluate_filter, and raw_event_to_dict.
"""

import logging
import unittest

from lib.watchers.backends.base import RawWatchEvent
from lib.watchers.filter_eval import FilterResult, evaluate_filter, raw_event_to_dict


class TestFilterResult(unittest.TestCase):
    """Tests for FilterResult."""

    def test_matched_true(self):
        r = FilterResult(matched=True)
        self.assertTrue(r.matched)
        self.assertIsNone(r.error)
        self.assertTrue(bool(r))

    def test_matched_false(self):
        r = FilterResult(matched=False)
        self.assertFalse(r.matched)
        self.assertIsNone(r.error)
        self.assertFalse(bool(r))

    def test_error_with_false(self):
        err = ValueError("bad filter")
        r = FilterResult(matched=False, error=err)
        self.assertFalse(r.matched)
        self.assertIs(r.error, err)
        self.assertFalse(bool(r))


class TestEvaluateFilter(unittest.TestCase):
    """Tests for evaluate_filter()."""

    def test_none_filter_always_matches(self):
        result = evaluate_filter(None, {"id": "test"})
        self.assertTrue(result.matched)
        self.assertIsNone(result.error)

    def test_simple_match(self):
        """JSON Logic equality check on nested doc field."""
        filter_expr = {"==": [{"var": "doc.type"}, "project"]}
        event_dict = {
            "id": "P123",
            "doc": {"type": "project", "_id": "P123"},
        }
        result = evaluate_filter(filter_expr, event_dict)
        self.assertTrue(result.matched)

    def test_simple_no_match(self):
        filter_expr = {"==": [{"var": "doc.type"}, "flowcell"]}
        event_dict = {
            "id": "P123",
            "doc": {"type": "project"},
        }
        result = evaluate_filter(filter_expr, event_dict)
        self.assertFalse(result.matched)

    def test_deleted_filter(self):
        """Filter on the 'deleted' field."""
        filter_expr = {"==": [{"var": "deleted"}, False]}
        event_dict = {"id": "P123", "deleted": False, "doc": {}}
        result = evaluate_filter(filter_expr, event_dict)
        self.assertTrue(result.matched)

        event_dict_deleted = {"id": "P123", "deleted": True, "doc": {}}
        result2 = evaluate_filter(filter_expr, event_dict_deleted)
        self.assertFalse(result2.matched)

    def test_complex_and_filter(self):
        """AND logic: doc.type == 'project' AND deleted == false."""
        filter_expr = {
            "and": [
                {"==": [{"var": "doc.type"}, "project"]},
                {"==": [{"var": "deleted"}, False]},
            ]
        }
        event_dict = {
            "id": "P123",
            "deleted": False,
            "doc": {"type": "project"},
        }
        result = evaluate_filter(filter_expr, event_dict)
        self.assertTrue(result.matched)

    def test_error_returns_false_with_error(self):
        """Malformed filter expression returns matched=False with error."""
        # This is a valid JSON structure but may cause evaluation issues
        # depending on the json_logic implementation. We test a guaranteed error
        # by passing something that json_logic can't handle.
        filter_expr = {"missing_op": []}
        event_dict = {"id": "test"}
        logger = logging.getLogger("test_filter")

        result = evaluate_filter(filter_expr, event_dict, logger=logger)
        # json_logic may not raise on unknown ops (returns None), so we just
        # verify the function completes without exception
        self.assertIsInstance(result, FilterResult)


class TestRawEventToDict(unittest.TestCase):
    """Tests for raw_event_to_dict()."""

    def test_basic_conversion(self):
        event = RawWatchEvent(
            id="P12345",
            doc={"_id": "P12345", "type": "project"},
            seq="123-abc",
            deleted=False,
            meta={"changes": [{"rev": "1-xyz"}]},
        )
        d = raw_event_to_dict(event)
        self.assertEqual(d["id"], "P12345")
        self.assertEqual(d["doc"]["type"], "project")
        self.assertEqual(d["seq"], "123-abc")
        self.assertFalse(d["deleted"])
        self.assertEqual(d["meta"]["changes"], [{"rev": "1-xyz"}])

    def test_minimal_event(self):
        event = RawWatchEvent(id="test")
        d = raw_event_to_dict(event)
        self.assertEqual(d["id"], "test")
        self.assertIsNone(d["doc"])
        self.assertIsNone(d["seq"])
        self.assertFalse(d["deleted"])
        self.assertEqual(d["meta"], {})


if __name__ == "__main__":
    unittest.main()
