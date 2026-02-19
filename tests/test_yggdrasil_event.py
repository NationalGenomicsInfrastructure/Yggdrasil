import unittest

from lib.core_utils.event_types import EventType
from lib.watchers.abstract_watcher import YggdrasilEvent


class TestYggdrasilEvent(unittest.TestCase):

    def test_event_attributes(self):
        event = YggdrasilEvent(EventType.PROJECT_CHANGE, {"data": 123}, "test_source")
        self.assertEqual(event.event_type, EventType.PROJECT_CHANGE)
        self.assertEqual(event.payload, {"data": 123})
        self.assertEqual(event.source, "test_source")
        self.assertIsNotNone(event.timestamp)

    def test_slots_prevent_extra_attributes(self):
        event = YggdrasilEvent(EventType.DELIVERY_READY, {}, "source")
        with self.assertRaises(AttributeError):
            event.new_attr = "not allowed"  # type: ignore

    def test_repr(self):
        event = YggdrasilEvent(EventType.FLOWCELL_READY, 42, "my_source")
        rep = repr(event)
        self.assertIn(EventType.FLOWCELL_READY, rep)
        self.assertIn("42", rep)
        self.assertIn("my_source", rep)

    def test_instance_independence(self):
        a = YggdrasilEvent(EventType.PROJECT_CHANGE, {"data": 123}, "test_source_X")
        b = YggdrasilEvent(EventType.DELIVERY_READY, {"data": 321}, "test_source_Y")
        a.source = "test_source_Z"

        self.assertNotEqual(a.source, b.source)
        self.assertEqual(b.source, "test_source_Y")


if __name__ == "__main__":
    unittest.main()
