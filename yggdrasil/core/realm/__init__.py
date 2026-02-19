"""
Realm infrastructure: descriptor and discovery.

This module provides the RealmDescriptor dataclass and discover_realms()
function for the unified realm entry-point system.

Usage::

    from yggdrasil.core.realm import RealmDescriptor, discover_realms

Realm implementations live in separate packages (e.g., lib/realms/test_realm,
external tenx/smartseq3 packages) and register via ``ygg.realm`` entry points.
"""

from yggdrasil.core.realm.descriptor import RealmDescriptor
from yggdrasil.core.realm.discovery import discover_realms

__all__ = ["RealmDescriptor", "discover_realms"]
