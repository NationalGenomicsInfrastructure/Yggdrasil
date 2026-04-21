"""WatcherManager orchestrates watcher backend lifecycle and fan-out.

Responsibilities:
- Parse connection config (endpoints + connections)
- Instantiate backend instances (one per unique resource)
- Start/stop backends concurrently
- Validate backend type consistency
- Register BoundWatchSpecs and deduplicate backend groups
- Fan-out raw events to matching WatchSpecs with filter evaluation
- Transform raw events into domain-level YggdrasilEvents
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from lib.core_utils.external_systems_resolver import resolve_connection
from lib.core_utils.logging_utils import custom_logger
from lib.watchers.abstract_watcher import YggdrasilEvent
from lib.watchers.backends.base import CheckpointStore, RawWatchEvent, WatcherBackend
from lib.watchers.backends.checkpoint_store import CouchDBCheckpointStore
from lib.watchers.filter_eval import FilterResult, evaluate_filter, raw_event_to_dict

if TYPE_CHECKING:
    from collections.abc import Callable

    from lib.watchers.watchspec import BoundWatchSpec

logger = custom_logger(__name__)


@dataclass
class WatcherBackendGroup:
    """
    Represents a deduplicated watcher backend instance.

    Group key: (backend_type, connection_name)
    The connection fully identifies the resource via config.

    Multiple WatchSpecs from different realms can share the same
    backend group if they watch the same (backend, connection).

    Attributes:
        backend_type: Backend type identifier (e.g., "couchdb", "fs")
        connection: Logical connection name (references config)
        backend_instance: The actual WatcherBackend instance (set after instantiation)
    """

    backend_type: str
    connection: str
    backend_instance: WatcherBackend | None = None

    @property
    def key(self) -> tuple[str, str]:
        """
        Deduplication key for this backend group.

        Returns:
            (backend_type, connection) tuple
        """
        return (self.backend_type, self.connection)


class WatcherManager:
    """
    Orchestrates watcher backend lifecycle.

    The WatcherManager is responsible for:
    - Accepting BoundWatchSpecs via add_watchspec() and deduplicating backend groups internally
    - Deduplicating backends (one per unique resource)
    - Resolving connection configuration from endpoints + connections
    - Instantiating and managing backend lifecycle
    - Validating backend type consistency

    Configuration structure required for ``config``:
        {
            "endpoints": {
                "couchdb": {
                    "backend": "couchdb",
                    "url": "https://...",
                    "auth": {"user_env": "...", "pass_env": "..."}
                },
                ...
            },
            "connections": {
                "projects_db": {
                    "endpoint": "couchdb",
                    "resource": {"db": "projects"}
                },
                ...
            }
        }

    Example usage:
        config = load_config()
        manager = WatcherManager(config)

        # Register BoundWatchSpecs (typically done during realm setup)
        manager.add_watchspec(bound_spec)

        # Start all backends
        await manager.start()

        # ... run until shutdown ...

        # Stop all backends
        await manager.stop()
    """

    # Backend registry: maps backend type names to classes
    _backend_registry: dict[str, type[WatcherBackend]] = {}

    def __init__(
        self,
        config: dict[str, Any],
        on_event: Callable[[YggdrasilEvent], None] | None = None,
        checkpoint_store: CheckpointStore | None = None,
        logger: logging.Logger | None = None,
        watcher_policy: dict[str, Any] | None = None,
    ):
        """
        Initialize the WatcherManager.

        Args:
            config: The ``external_systems`` config slice with "endpoints" and
                    "connections" keys. Typically ``main_config["external_systems"]``.
                    Structure::

                        {
                            "endpoints": {
                                "couchdb": {
                                    "backend": "couchdb",
                                    "url": "https://...",
                                    "auth": {"user_env": "...", "pass_env": "..."}
                                }
                            },
                            "connections": {
                                "projects_db": {
                                    "endpoint": "couchdb",
                                    "resource": {"db": "projects"}
                                }
                            }
                        }

            on_event: Callback invoked for each transformed YggdrasilEvent.
                      Typically YggdrasilCore.handle_event.
            checkpoint_store: Storage for backend checkpoints.
                              Defaults to CouchDBCheckpointStore.
            logger: Optional logger instance.
            watcher_policy: Optional dict with retry policy overrides:
                            ``max_observation_retries`` (int, default 3) and
                            ``observation_retry_delay_s`` (float, default 1.0).
                            If None or empty, hardcoded defaults are used.
                            Typically ``main_config.get("watchers", {})``.
        """
        self.config = config
        self._on_event = on_event
        self.checkpoint_store = checkpoint_store or CouchDBCheckpointStore()
        self._logger = logger or custom_logger(f"{__name__}.{type(self).__name__}")
        self._watcher_policy_override = watcher_policy

        self._watcher_groups: dict[tuple[str, str], WatcherBackendGroup] = {}
        # BoundWatchSpecs grouped by backend group key
        self._bound_specs: dict[tuple[str, str], list[BoundWatchSpec]] = {}
        self._consumer_tasks: list[asyncio.Task[None]] = []
        self._running = False

    # -------------------------------------------------------------------------
    # Backend Registry
    # -------------------------------------------------------------------------

    @classmethod
    def register_backend(cls, name: str, backend_cls: type[WatcherBackend]) -> None:
        """
        Register a backend type.

        This is typically called at module load time to register
        available backend implementations.

        Args:
            name: Backend type identifier (e.g., "couchdb", "fs")
            backend_cls: WatcherBackend subclass

        Example:
            WatcherManager.register_backend("couchdb", CouchDBBackend)
        """
        cls._backend_registry[name] = backend_cls
        logger.debug("Registered backend type: %s -> %s", name, backend_cls.__name__)

    @classmethod
    def get_registered_backends(cls) -> dict[str, type[WatcherBackend]]:
        """Return a copy of the backend registry."""
        return dict(cls._backend_registry)

    # -------------------------------------------------------------------------
    # Backend Group Management (internal)
    # -------------------------------------------------------------------------

    def _ensure_watcher_group(
        self,
        backend_type: str,
        connection: str,
    ) -> WatcherBackendGroup:
        """
        Ensure a watcher backend group exists for (backend_type, connection),
        creating it if needed. Returns the existing group on duplicate.

        Called internally by add_watchspec().

        Args:
            backend_type: Backend type identifier (e.g., "couchdb")
            connection: Logical connection name from config

        Returns:
            WatcherBackendGroup (existing or newly created)
        """
        key = (backend_type, connection)

        if key in self._watcher_groups:
            self._logger.debug("Watcher group already exists: %s", key)
            return self._watcher_groups[key]

        group = WatcherBackendGroup(
            backend_type=backend_type,
            connection=connection,
        )
        self._watcher_groups[key] = group
        self._logger.info("Created watcher backend group: %s", key)
        return group

    def get_watcher_groups(self) -> dict[tuple[str, str], WatcherBackendGroup]:
        """Return a copy of the watcher groups dict."""
        return dict(self._watcher_groups)

    # -------------------------------------------------------------------------
    # WatchSpec Management
    # -------------------------------------------------------------------------

    def add_watchspec(self, bound_spec: BoundWatchSpec) -> None:
        """
        Register a BoundWatchSpec and ensure its backend group exists.

        Multiple BoundWatchSpecs sharing the same (backend, connection)
        will share a single backend instance.  Each will be evaluated
        independently during fan-out.

        Args:
            bound_spec: WatchSpec bound to its owning realm.
        """
        key = bound_spec.backend_group_key

        # Ensure backend group exists (dedup)
        self._ensure_watcher_group(
            backend_type=bound_spec.spec.backend,
            connection=bound_spec.spec.connection,
        )

        # Register spec for fan-out
        self._bound_specs.setdefault(key, []).append(bound_spec)

        self._logger.debug(
            "Registered WatchSpec: realm=%s, backend=%s, connection=%s, event_type=%s",
            bound_spec.realm_id,
            bound_spec.spec.backend,
            bound_spec.spec.connection,
            bound_spec.spec.event_type.name,
        )

    def get_bound_specs(self) -> dict[tuple[str, str], list[BoundWatchSpec]]:
        """Return a copy of the bound specs dict."""
        return {k: list(v) for k, v in self._bound_specs.items()}

    # -------------------------------------------------------------------------
    # Configuration Resolution
    # -------------------------------------------------------------------------

    def _resolve_connection_config(self, connection_name: str) -> dict[str, Any]:
        """
        Resolve full backend config from endpoints + connections.

        Uses external_systems_resolver for endpoint/connection lookup, then
        merges watch-specific settings (WatcherManager-only concern) on top.

        Algorithm:
        1. Delegate endpoint + connection lookup to resolve_connection()
        2. Build base config from resolved endpoint (backend, url)
        3. Merge global watcher defaults (start_seq etc.)
        4. Merge per-connection watch settings (poll_interval, limit etc.)
        5. Merge resource (db, path, etc.)
        6. Pass auth env var names (not values) for backend credential resolution

        Resource semantics per backend:
        - couchdb: {"db": "database_name"}
        - fs: {"path": "/watch/dir", "recursive": true}
        - postgres: {"table": "schema.table"} or {"channel": "notify_channel"}

        Env var handling (all under "auth" key):
        - Env var NAMES are passed through, not resolved values
        - Backend/factory resolves env vars at client creation time

        Args:
            connection_name: Logical connection name

        Returns:
            Resolved configuration dict for backend instantiation

        Raises:
            KeyError: If connection, endpoint, or required URL is missing
        """
        # Delegate endpoint + connection lookup to shared resolver
        resolved_conn = resolve_connection(connection_name, self.config)

        # Build base config from resolver output
        resolved: dict[str, Any] = {
            "backend": resolved_conn.endpoint.backend_type,
            "url": resolved_conn.endpoint.url,
        }

        # Merge watch-specific settings (WatcherManager-only concern).
        # Precedence: defaults < connection.watch < resource
        defaults = self.config.get("defaults", {})
        if isinstance(defaults, dict):
            resolved.update(defaults)

        connections = self.config.get("connections", {})
        conn = connections.get(connection_name, {})
        watch = conn.get("watch", {})
        if isinstance(watch, dict):
            resolved.update(watch)

        # Merge resource (backend-specific: db, path, etc.)
        resource = conn.get("resource", {})
        resolved.update(resource)

        # Use pre-resolved endpoint auth fields — already computed by resolve_connection().
        # ResolvedEndpoint carries user_env, pass_env and dsn_env (optional).
        resolved["user_env"] = resolved_conn.endpoint.user_env
        resolved["pass_env"] = resolved_conn.endpoint.pass_env
        if resolved_conn.endpoint.dsn_env is not None:
            resolved["dsn_env"] = resolved_conn.endpoint.dsn_env

        return resolved

    # -------------------------------------------------------------------------
    # Backend Instantiation
    # -------------------------------------------------------------------------

    def _resolve_watcher_policy(self) -> dict[str, Any]:
        """Return the resolved watcher retry policy.

        Uses the ``watcher_policy`` dict passed at construction time.
        Missing or unset keys fall back to the defaults specified in
        the ``dict.get()`` calls below (``3`` and ``1.0``).

        Returns:
            Dict with keys:
            - ``max_observation_retries`` (int, default 3)
            - ``observation_retry_delay_s`` (float, default 1.0)
        """
        raw: dict[str, Any] = self._watcher_policy_override or {}
        return {
            "max_observation_retries": int(raw.get("max_observation_retries", 3)),
            "observation_retry_delay_s": float(
                raw.get("observation_retry_delay_s", 1.0)
            ),
        }

    def _instantiate_watcher_backends(self) -> None:
        """
        Instantiate WatcherBackend for each group.

        Called before start(). Validates that endpoint.backend matches
        group.backend_type.

        Raises:
            ValueError: If backend type is unknown or mismatched
            KeyError: If connection config is invalid
            RuntimeError: If env var resolution fails
        """
        policy = self._resolve_watcher_policy()

        for key, group in self._watcher_groups.items():
            if group.backend_instance is not None:
                continue

            backend_cls = self._backend_registry.get(group.backend_type)
            if not backend_cls:
                raise ValueError(
                    f"Unknown backend type: {group.backend_type}. "
                    f"Available: {list(self._backend_registry.keys())}"
                )

            config = self._resolve_connection_config(group.connection)

            # Validate backend type consistency (WatchSpec vs endpoint config)
            endpoint_backend = config.get("backend")
            if endpoint_backend and endpoint_backend != group.backend_type:
                raise ValueError(
                    f"Backend type mismatch for connection '{group.connection}': "
                    f"WatchSpec says '{group.backend_type}', "
                    f"endpoint says '{endpoint_backend}'"
                )

            # Merge global watcher policy on top of connection config.
            # Policy keys are unlikely to collide with connection-specific keys;
            # warn if they do to surface misconfiguration early.
            for policy_key in policy:
                if policy_key in config:
                    self._logger.warning(
                        "Watcher policy key '%s' shadows a connection config key "
                        "for connection '%s'; policy value wins.",
                        policy_key,
                        group.connection,
                    )
            config = {**config, **policy}

            # Stable backend_key: {backend}:{connection}
            backend_key = f"{group.backend_type}:{group.connection}"

            group.backend_instance = backend_cls(
                backend_key=backend_key,
                config=config,
                checkpoint_store=self.checkpoint_store,
                logger=self._logger,
            )
            self._logger.info("Instantiated watcher backend: %s", backend_key)

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    async def start(self) -> None:
        """
        Start all registered watcher backends.

        Instantiates backends (if not already done), starts them concurrently,
        then spawns one consumer task per registered backend group.

        Contract:
        - Instantiates backends if not already done
        - Starts all backends concurrently (quick, non-blocking)
        - Logs failures but continues with other backends
        - Returns after all backends have attempted start

        Raises:
            Various exceptions from _instantiate_watcher_backends() on config errors
        """
        if self._running:
            self._logger.warning("WatcherManager already running")
            return

        self._logger.info("WatcherManager starting...")

        # Instantiate backends (may raise on config errors)
        self._instantiate_watcher_backends()
        self._running = True

        # Start all backends concurrently (start() returns quickly)
        start_tasks: list[asyncio.Task[None]] = []
        backend_keys: list[str] = []

        for group in self._watcher_groups.values():
            if group.backend_instance:
                task = asyncio.create_task(group.backend_instance.start())
                start_tasks.append(task)
                backend_keys.append(group.backend_instance.backend_key)

        if not start_tasks:
            self._logger.warning("No watcher backends to start")
            return

        self._logger.info("Starting %d watcher backend(s)...", len(start_tasks))
        results = await asyncio.gather(*start_tasks, return_exceptions=True)

        # Log any failures (backend marked unavailable but manager continues)
        for backend_key, result in zip(backend_keys, results):
            if isinstance(result, Exception):
                self._logger.error(
                    "Backend '%s' failed to start: %s",
                    backend_key,
                    result,
                    exc_info=result,
                )

        started_count = sum(1 for r in results if not isinstance(r, Exception))
        self._logger.info(
            "Watcher backends started: %d/%d",
            started_count,
            len(start_tasks),
        )

        # Spawn consumer tasks for fan-out (one per backend group with specs)
        for key, group in self._watcher_groups.items():
            if group.backend_instance and key in self._bound_specs:
                task = asyncio.create_task(
                    self._consume_backend(group, self._bound_specs[key])
                )
                self._consumer_tasks.append(task)

        if self._consumer_tasks:
            self._logger.info(
                "Spawned %d consumer task(s) for fan-out",
                len(self._consumer_tasks),
            )

    # -------------------------------------------------------------------------
    # Fan-out: Backend -> WatchSpecs -> YggdrasilEvents
    # -------------------------------------------------------------------------

    async def _consume_backend(
        self,
        group: WatcherBackendGroup,
        bound_specs: list[BoundWatchSpec],
    ) -> None:
        """
        Consume events from a backend and fan-out to matching WatchSpecs.

        Runs as a long-lived task, one per backend group.  Each
        :class:`RawWatchEvent` is evaluated against *all* bound specs
        for this group.  Matching specs produce :class:`YggdrasilEvent`
        objects which are dispatched via ``self._on_event``.

        Args:
            group: The watcher backend group to consume from.
            bound_specs: All BoundWatchSpecs registered for this group.
        """
        backend = group.backend_instance
        if not backend:
            return

        backend_key = backend.backend_key
        self._logger.info(
            "Consumer started for backend '%s' (%d spec(s))",
            backend_key,
            len(bound_specs),
        )

        try:
            async for raw_event in backend.events():
                # Defensive: skip if somehow we get None (sentinel leak)
                if raw_event is None:
                    self._logger.warning(
                        "Received None event from backend '%s', skipping", backend_key
                    )
                    continue
                self._fan_out(raw_event, bound_specs, source=backend_key)
        except asyncio.CancelledError:
            self._logger.debug("Consumer cancelled for backend '%s'", backend_key)
        except Exception as exc:
            self._logger.exception(
                "Consumer error for backend '%s': %s", backend_key, exc
            )

    def _fan_out(
        self,
        raw_event: RawWatchEvent,
        bound_specs: list[BoundWatchSpec],
        source: str,
    ) -> None:
        """
        Evaluate a raw event against all bound specs and dispatch matches.

        For each matching BoundWatchSpec:
        1. Evaluate filter_expr (if any)
        2. Call build_scope() to extract scope
        3. Call build_payload() to build domain payload
        4. Inject routing hints (realm_id, target_handlers)
        5. Emit YggdrasilEvent via on_event callback

        Args:
            raw_event: The raw backend event.
            bound_specs: All specs registered for the source backend group.
            source: Backend key string for event source identification.
        """
        event_dict = raw_event_to_dict(raw_event)

        for bs in bound_specs:
            spec = bs.spec

            # Step 1: Filter evaluation
            result: FilterResult = evaluate_filter(
                spec.filter_expr, event_dict, logger=self._logger
            )
            if not result:
                if result.error:
                    self._logger.warning(
                        "Filter error for realm '%s' on event '%s': %s",
                        bs.realm_id,
                        raw_event.id,
                        result.error,
                    )
                continue

            # Step 2-3: Build scope and payload
            try:
                scope = spec.build_scope(raw_event)
                payload = spec.build_payload(raw_event)
            except Exception as exc:
                self._logger.error(
                    "Scope/payload build failed for realm '%s' on event '%s': %s",
                    bs.realm_id,
                    raw_event.id,
                    exc,
                )
                continue

            # Step 4: Inject routing hints (into a copy to avoid cross-spec mutation)
            payload = dict(payload)
            payload["realm_id"] = bs.realm_id
            payload["scope"] = scope
            if spec.target_handlers:
                payload["target_handlers"] = spec.target_handlers

            # Step 5: Emit YggdrasilEvent
            ygg_event = YggdrasilEvent(
                event_type=spec.event_type,
                payload=payload,
                source=source,
            )

            if self._on_event:
                try:
                    self._on_event(ygg_event)
                except Exception as exc:
                    self._logger.error(
                        "on_event callback failed for realm '%s', event '%s': %s",
                        bs.realm_id,
                        raw_event.id,
                        exc,
                        exc_info=True,
                    )
            else:
                self._logger.warning(
                    "No on_event callback; dropping event for realm '%s'",
                    bs.realm_id,
                )

    async def stop(self) -> None:
        """
        Stop all watcher backends gracefully.

        Contract:
        - Cancels consumer tasks
        - Stops all backends
        - Waits for cleanup
        """
        if not self._running:
            self._logger.debug("WatcherManager not running")
            return

        self._logger.info("WatcherManager stopping...")
        self._running = False

        # Cancel consumer tasks
        for task in self._consumer_tasks:
            task.cancel()

        if self._consumer_tasks:
            await asyncio.gather(*self._consumer_tasks, return_exceptions=True)
            self._consumer_tasks.clear()

        # Stop all backends
        stop_tasks: list[asyncio.Task[None]] = []
        for group in self._watcher_groups.values():
            if group.backend_instance:
                task = asyncio.create_task(group.backend_instance.stop())
                stop_tasks.append(task)

        if stop_tasks:
            self._logger.info("Stopping %d watcher backend(s)...", len(stop_tasks))
            await asyncio.gather(*stop_tasks, return_exceptions=True)

        self._logger.info("All watcher backends stopped.")

    @property
    def is_running(self) -> bool:
        """True if the manager is currently running."""
        return self._running


# -------------------------------------------------------------------------
# Backend Registration (at module load time)
# -------------------------------------------------------------------------


def _register_default_backends() -> None:
    """Register default backend implementations."""
    from lib.watchers.backends.couchdb import CouchDBBackend

    WatcherManager.register_backend("couchdb", CouchDBBackend)


# Register defaults when module is imported
_register_default_backends()
