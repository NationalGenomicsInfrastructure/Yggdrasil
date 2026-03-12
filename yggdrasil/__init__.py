"""
Public namespace for Yggdrasil.

Real yggdrasil modules (cli, core, flow, etc.) live under ``yggdrasil/``.
Modules under ``lib/`` (core_utils, watchers, couchdb, …) are aliased
into this namespace **lazily** via a custom meta-path finder.

    import lib.core_utils.event_types as A
    import yggdrasil.core_utils.event_types as B
    assert A is not B             # wrapper module
    assert A.EventType is B.EventType  # Enum identity preserved

Unlike the previous eager-import shim, this approach:
  • Does NOT import all of ``lib/*`` at ``yggdrasil`` import time
  • Redirects on first access only (lazy)
    • Preserves attribute identity (same Enum, same singletons)
"""

from __future__ import annotations

import importlib.abc
import importlib.machinery
import importlib.util
import pkgutil
import sys
from importlib import import_module
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

_root = Path(__file__).resolve().parent.parent
_lib = _root / "lib"

# Ensure the project root is importable (so ``import lib.core_utils`` works).
for _p in (str(_root),):
    if _p not in sys.path:
        sys.path.insert(0, _p)


# ---------------------------------------------------------------------------
# Lazy alias finder: yggdrasil.<lib_pkg>.* → lib.<lib_pkg>.*
# ---------------------------------------------------------------------------

# Top-level package names that exist under lib/ (e.g. core_utils, watchers).
# Anything NOT in this set is a *real* yggdrasil sub-package (flow, core, …)
# and must be handled by the default path-based finder.
_LIB_PACKAGES: frozenset[str] = frozenset(
    name for _, name, ispkg in pkgutil.iter_modules([str(_lib)]) if ispkg
)


class _AliasLoader(importlib.abc.Loader):
    """Loader that creates alias wrapper modules for lib.* packages.

    Instead of returning the lib.* module directly (which would cause
    ``_init_module_attrs`` to clobber the original's ``__spec__``, ``__name__``,
    etc. and trigger reimports), we create a *new* module object whose
    ``__getattr__`` delegates to the lib module.  Direct ``__dict__`` access
    for already-populated attributes also works because we copy them.

    This preserves the critical property:
        from yggdrasil.core_utils.event_types import EventType  →  same class
        from lib.core_utils.event_types import EventType         →  same class
    """

    def create_module(self, spec):
        """Create an alias module that delegates to the lib module."""
        lib_name = "lib." + spec.name.removeprefix("yggdrasil.")
        lib_mod = sys.modules.get(lib_name)
        if lib_mod is None:
            return None

        import types

        alias = types.ModuleType(spec.name)
        # Copy all current attributes (Enum classes, functions, etc.)
        # so `from yggdrasil.X import Y` works immediately.
        alias.__dict__.update(lib_mod.__dict__)
        # Stash the lib module for __getattr__ delegation
        alias.__dict__["_ygg_lib_module"] = lib_mod
        # Copy __path__ for packages so child imports work
        if hasattr(lib_mod, "__path__"):
            alias.__path__ = list(lib_mod.__path__)
        return alias

    def exec_module(self, module):
        """Install __getattr__ for lazy delegation to lib module."""
        lib_mod = module.__dict__.get("_ygg_lib_module")
        if lib_mod is None:
            return

        def _alias_getattr(name):
            # Delegate attribute access to the canonical lib module
            # This catches attributes added after the initial copy
            try:
                return getattr(lib_mod, name)
            except AttributeError:
                raise AttributeError(
                    f"module {module.__name__!r} has no attribute {name!r}"
                ) from None

        module.__getattr__ = _alias_getattr


# Single shared instance.
_alias_loader = _AliasLoader()


class _LibAliasFinder(importlib.abc.MetaPathFinder):
    """
    Meta-path finder that lazily redirects ``yggdrasil.<X>`` imports to
    ``lib.<X>`` when *X* is a top-level package under ``lib/``.

    Strategy: pre-populate ``sys.modules`` with alias entries and also
    set the child attribute on the parent package **before** returning
    the spec.  This way the import machinery's ``_find_and_load_unlocked``
    finds the module in ``sys.modules`` immediately and returns it —
    without calling ``_load_unlocked`` at all (which would clobber
    ``__spec__`` and other attributes on the shared module object).

    The trick: we return ``None`` from ``find_spec`` after seeding
    ``sys.modules``.  ``_find_and_load_unlocked`` has an early-return:

        if name in sys.modules:
            return sys.modules[name]

    So if we insert the module BEFORE Python's ``_find_spec`` call
    returns… we need to be earlier.  Actually, ``_find_and_load_unlocked``
    checks ``sys.modules`` BEFORE calling ``_find_spec``.  So seeding
    here (inside ``_find_spec`` on a meta-path finder) is too late for
    the current name — but we CAN pre-seed CHILD names so they hit
    the early-return on their own ``_find_and_load`` call.

    Revised strategy: for the *current* name, we must return a spec.
    We use ``importlib.util.spec_from_file_location`` pointed at the
    lib module's file, which gives a proper ``SourceFileLoader``.
    But that would re-exec the file…

    Final strategy: Return a spec with our ``_AliasLoader``, but have
    ``create_module`` return a NEW module that *wraps* the lib module
    via ``__getattr__`` delegation.  This avoids clobbering the lib
    module's ``__spec__`` while preserving attribute identity.
    """

    def find_spec(
        self,
        fullname: str,
        path: object = None,
        target: object = None,
    ) -> importlib.machinery.ModuleSpec | None:
        # Fast exit: not our prefix
        if not fullname.startswith("yggdrasil."):
            return None

        # Already loaded — nothing to do
        if fullname in sys.modules:
            return None

        # Only intercept lib/* packages (not flow/, core/, cli, …)
        suffix = fullname.removeprefix("yggdrasil.")
        top_pkg = suffix.split(".")[0]
        if top_pkg not in _LIB_PACKAGES:
            return None

        # Import the canonical lib.* module
        lib_name = f"lib.{suffix}"
        try:
            import_module(lib_name)
        except ImportError:
            return None

        lib_mod = sys.modules.get(lib_name)
        if lib_mod is None:
            return None

        # Build a spec whose loader creates a wrapper module.
        # The wrapper delegates attribute access to lib_mod while
        # keeping its own __spec__/__name__ (so _init_module_attrs
        # doesn't clobber the lib module's metadata).
        is_pkg = hasattr(lib_mod, "__path__")
        spec = importlib.machinery.ModuleSpec(
            fullname,
            _alias_loader,
            origin=getattr(lib_mod, "__file__", None),
            is_package=is_pkg,
        )
        if is_pkg:
            spec.submodule_search_locations = list(lib_mod.__path__)
        return spec


# Install BEFORE any lib/ package is imported via yggdrasil.*
sys.meta_path.insert(0, _LibAliasFinder())

# NOTE: Do NOT override __path__.  The default __path__ (pointing at
# yggdrasil/) lets Python find real sub-packages (flow/, core/, …).
# The _LibAliasFinder handles lib/ aliasing without __path__ tricks.


# ---------------------------------------------------------------------------
# Package version
# ---------------------------------------------------------------------------

try:
    __version__ = version("yggdrasil")
except PackageNotFoundError:  # local checkout without install
    from setuptools_scm import get_version

    __version__ = get_version(root=_root, relative_to=__file__)
