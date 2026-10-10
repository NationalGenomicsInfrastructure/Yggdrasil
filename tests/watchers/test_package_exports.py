"""Tests for the yggdrasil.watchers package exports."""

import importlib
import unittest

import yggdrasil.watchers as watchers


class TestWatchersPackageExports(unittest.TestCase):
    """Every name in ``__all__`` resolves, eagerly or on first access."""

    def test_all_exports_resolve_to_their_defining_module(self):
        for name in watchers.__all__:
            with self.subTest(name=name):
                value = getattr(watchers, name)
                module = importlib.import_module(value.__module__)
                self.assertIs(getattr(module, name), value)

    def test_lazy_exports_are_listed(self):
        self.assertTrue(set(watchers._LAZY_EXPORTS) <= set(watchers.__all__))
        self.assertTrue(set(watchers._LAZY_EXPORTS) <= set(dir(watchers)))

    def test_unknown_attribute_raises(self):
        with self.assertRaises(AttributeError):
            watchers.CouchDBCheckpointStore  # noqa: B018

    def test_from_import_of_lazy_name(self):
        from yggdrasil.watchers import WatcherManager
        from yggdrasil.watchers.manager import WatcherManager as direct

        self.assertIs(WatcherManager, direct)


if __name__ == "__main__":
    unittest.main()
