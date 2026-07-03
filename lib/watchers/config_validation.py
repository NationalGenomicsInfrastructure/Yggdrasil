"""Validation helpers for watcher-to-config wiring.

This module intentionally validates only the narrow contract between
WatchSpecs, watcher backend registration, and ``external_systems`` config.
It is small and reusable so broader startup/config validators can call into it
without duplicating watcher-specific checks.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from lib.core_utils.external_systems_resolver import resolve_connection
from lib.watchers.backends.base import WatcherBackend
from lib.watchers.watchspec import BoundWatchSpec


@dataclass(frozen=True)
class WatcherConfigValidationIssue:
    """
    One watcher/config wiring validation issue.

    Attributes:
        kind: Short machine-readable issue category (e.g. "unknown_backend").
        realms: Realm IDs sharing this backend/connection pair.
        backend: The watcher backend name declared in the WatchSpec.
        connection: The external_systems connection name declared in the WatchSpec.
        detail: Human-readable explanation of the problem.
    """

    kind: str
    realms: tuple[str, ...]
    backend: str
    connection: str
    detail: str

    def format(self) -> str:
        """
        Format this issue for operator-facing startup logs.

        Returns:
            A single-line human-readable description of the issue.
        """
        if len(self.realms) == 1:
            realm_text = f"realm '{self.realms[0]}'"
        else:
            realm_text = "realms " + ", ".join(f"'{realm}'" for realm in self.realms)

        return (
            f"{realm_text} uses backend '{self.backend}' with connection "
            f"'{self.connection}': {self.detail}"
        )


class WatcherConfigurationError(RuntimeError):
    """
    Raised when daemon watcher config wiring is invalid.

    Attributes:
        issues: The wiring issues that caused validation to fail.
        config_path: Path to the config file being validated, if known.
    """

    def __init__(
        self,
        issues: Iterable[WatcherConfigValidationIssue],
        *,
        config_path: str | Path | None = None,
    ) -> None:
        """
        Initialize the configuration error.

        Args:
            issues: The wiring issues that caused validation to fail.
            config_path: Optional path to the loaded config file for diagnostics.
        """
        self.issues = tuple(issues)
        self.config_path = Path(config_path) if config_path is not None else None
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        """Build the exception message from self.issues."""
        location = f" in {self.config_path}" if self.config_path else ""
        header = f"Watcher configuration error{location}"

        if not self.issues:
            return header

        if len(self.issues) == 1:
            return f"{header}: {self.issues[0].format()}"

        formatted_issues = "\n".join(f"- {issue.format()}" for issue in self.issues)
        return f"{header}: {len(self.issues)} issue(s)\n{formatted_issues}"


def validate_watcher_config_wiring(
    bound_specs: Iterable[BoundWatchSpec],
    external_systems: Mapping[str, Any],
    backend_registry: Mapping[str, type[WatcherBackend]],
    *,
    config_path: str | Path | None = None,
) -> None:
    """Validate WatchSpecs against watcher backend and connection config.

    Args:
        bound_specs: WatchSpecs bound to their owning realm.
        external_systems: The ``external_systems`` config slice.
        backend_registry: Registered watcher backend classes keyed by backend name.
        config_path: Optional path to the loaded config file for diagnostics.

    Raises:
        WatcherConfigurationError: If any wiring issue is found.
    """
    available_backends = _format_available(backend_registry.keys())
    raw_connections = external_systems.get("connections") or {}
    connections = raw_connections if isinstance(raw_connections, Mapping) else {}
    available_connections = _format_available(connections.keys())
    issues: list[WatcherConfigValidationIssue] = []

    for (backend, connection), realms in _group_specs(bound_specs).items():
        if backend not in backend_registry:
            issues.append(
                WatcherConfigValidationIssue(
                    kind="unknown_backend",
                    realms=realms,
                    backend=backend,
                    connection=connection,
                    detail=(
                        "watcher backend is not registered. "
                        f"Available backends: {available_backends}"
                    ),
                )
            )

        try:
            resolved_conn = resolve_connection(connection, dict(external_systems))
        except KeyError as exc:
            if connection not in connections:
                detail = (
                    "connection is not configured. "
                    f"Available connections: {available_connections}"
                )
            else:
                detail = f"connection could not be resolved: {_exception_message(exc)}"

            issues.append(
                WatcherConfigValidationIssue(
                    kind="invalid_connection",
                    realms=realms,
                    backend=backend,
                    connection=connection,
                    detail=detail,
                )
            )
            continue

        endpoint_backend = resolved_conn.endpoint.backend_type
        if endpoint_backend and endpoint_backend != backend:
            issues.append(
                WatcherConfigValidationIssue(
                    kind="backend_mismatch",
                    realms=realms,
                    backend=backend,
                    connection=connection,
                    detail=(
                        f"WatchSpec backend is '{backend}' but configured endpoint "
                        f"backend is '{endpoint_backend}'"
                    ),
                )
            )

    if issues:
        raise WatcherConfigurationError(issues, config_path=config_path)


def _group_specs(
    bound_specs: Iterable[BoundWatchSpec],
) -> dict[tuple[str, str], tuple[str, ...]]:
    """Group bound specs by (backend, connection), with sorted, deduped realm ids."""
    grouped: dict[tuple[str, str], list[str]] = {}

    for bound_spec in bound_specs:
        key = (bound_spec.spec.backend, bound_spec.spec.connection)
        realms = grouped.setdefault(key, [])
        if bound_spec.realm_id not in realms:
            realms.append(bound_spec.realm_id)

    return {
        key: tuple(sorted(realms))
        for key, realms in sorted(grouped.items(), key=lambda item: item[0])
    }


def _format_available(names: Iterable[str]) -> str:
    """Return a comma-separated, sorted list of names, or "none" if empty."""
    formatted = ", ".join(sorted(str(name) for name in names))
    return formatted or "none"


def _exception_message(exc: KeyError) -> str:
    """Return the KeyError's message without its repr-style quoting."""
    if exc.args:
        return str(exc.args[0])
    return str(exc)
