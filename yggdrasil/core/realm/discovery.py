"""
Unified realm discovery via ``ygg.realm`` entry-point group.

Each entry point must reference a **provider function**::

    def get_realm_descriptor() -> RealmDescriptor | None

The provider may return ``None`` to opt out at runtime (e.g. when the
realm is disabled or a required dependency is missing).

Entry point registration (in external realm's ``pyproject.toml``)::

    [project.entry-points."ygg.realm"]
    my_realm = "my_realm:get_realm_descriptor"

Note
----
Entry point names (left-hand side) are arbitrary discovery keys.  Only
``descriptor.realm_id`` is used for realm identity.
"""

from __future__ import annotations

import importlib.metadata
import logging

from yggdrasil.core.realm.descriptor import RealmDescriptor


def discover_realms(
    logger: logging.Logger | None = None,
) -> list[RealmDescriptor]:
    """
    Discover realms from all installed packages via ``ygg.realm`` entry points.

    Returns:
        List of :class:`RealmDescriptor` instances (``None`` returns are
        skipped silently).
    """
    logger = logger or logging.getLogger("RealmDiscovery")
    eps = list(importlib.metadata.entry_points(group="ygg.realm"))

    # Deduplicate entry points (importlib.metadata can return duplicates
    # for editable installs)
    seen: set[tuple[str, str]] = set()
    unique_eps = []
    for ep in eps:
        key = (ep.name, ep.value)
        if key not in seen:
            seen.add(key)
            unique_eps.append(ep)

    logger.debug(
        "Found %d 'ygg.realm' entry point(s) (%d unique)",
        len(eps),
        len(unique_eps),
    )

    descriptors: list[RealmDescriptor] = []

    for ep in unique_eps:
        try:
            provider_fn = ep.load()
        except Exception as e:
            logger.exception("Failed to load realm provider '%s': %s", ep.name, e)
            continue

        try:
            descriptor = provider_fn()
        except Exception as e:
            logger.exception(
                "Realm provider '%s' raised during get_realm_descriptor(): %s",
                ep.name,
                e,
            )
            continue

        if descriptor is None:
            logger.debug("Realm provider '%s' returned None; skipping.", ep.name)
            continue

        if not descriptor.realm_id:
            logger.error(
                "Realm provider '%s' returned descriptor with empty realm_id; skipping.",
                ep.name,
            )
            continue

        logger.info(
            "Discovered realm '%s' (handlers=%d, watchspecs=%s)",
            descriptor.realm_id,
            len(descriptor.handler_classes),
            (
                "callable"
                if callable(descriptor.watchspecs)
                else len(descriptor.watchspecs)
            ),
        )
        descriptors.append(descriptor)

    logger.info("Total realms discovered: %d", len(descriptors))
    return descriptors
