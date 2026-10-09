"""Yggdrasil: event-driven orchestration of realm-defined workflows."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("yggdrasil")
except PackageNotFoundError:  # local checkout without install
    from setuptools_scm import get_version

    __version__ = get_version(root="..", relative_to=__file__)
