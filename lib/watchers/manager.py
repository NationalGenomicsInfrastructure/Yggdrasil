"""
WatcherManager orchestrates watcher backend lifecycle.

Phase 1 responsibilities:
- Parse connection config (endpoints + connections)
- Instantiate backend instances (one per unique resource)
- Start/stop backends concurrently
- Validate backend type consistency

Phase 2 will add:
- WatchSpec collection from realms
- Fan-out to matching specs
- Filter evaluation
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from lib.watchers.backends.base import CheckpointStore, WatcherBackend
from lib.watchers.backends.checkpoint_store import CouchDBCheckpointStore

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


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
    - Collecting watcher groups from WatchSpecs (Phase 2)
    - Deduplicating backends (one per unique resource)
    - Resolving connection configuration from endpoints + connections
    - Instantiating and managing backend lifecycle
    - Validating backend type consistency

    Configuration structure expected in self.config:
        {
            "endpoints": {
                "couch_primary": {
                    "backend": "couchdb",
                    "url": "https://...",
                    "auth": {"user_env": "...", "pass_env": "..."}
                },
                ...
            },
            "connections": {
                "projects_db": {
                    "endpoint": "couch_primary",
                    "resource": {"db": "projects"}
                },
                ...
            }
        }

    Example usage:
        config = load_config()
        manager = WatcherManager(config)

        # Add watcher groups (typically done by realm discovery in Phase 2)
        manager.add_watcher_group("couchdb", "projects_db")

        # Start all backends
        await manager.start()

        # ... run until shutdown ...

        # Stop all backends
        await manager.stop()
    """

    # Backend registry: maps backend type names to classes
    _backend_registry: dict[str, type[WatcherBackend]] = {}

    # Config key in main.json for external systems
    CONFIG_KEY = "external_systems"

    def __init__(
        self,
        config: dict[str, Any] | None = None,
        checkpoint_store: CheckpointStore | None = None,
        logger: logging.Logger | None = None,
    ):
        """
        Initialize the WatcherManager.

        Args:
            config: Configuration dict with "endpoints" and "connections" keys.
                    If None, loads from ConfigLoader("main.json")["external_systems"].
            checkpoint_store: Storage for backend checkpoints.
                              Defaults to CouchDBCheckpointStore.
            logger: Optional logger instance.

        The config structure expected:
            {
                "endpoints": {
                    "couch_primary": {
                        "backend": "couchdb",
                        "url": "https://...",
                        "auth": {"user_env": "...", "pass_env": "..."}
                    }
                },
                "connections": {
                    "projects_db": {
                        "endpoint": "couch_primary",
                        "resource": {"db": "projects"}
                    }
                }
            }

        If not passed explicitly, config is loaded from:
            ConfigLoader().load_config("main.json")["external_systems"]
        """
        if config is None:
            config = self._load_default_config()

        self.config = config
        self.checkpoint_store = checkpoint_store or CouchDBCheckpointStore()
        self._logger = logger or logging.getLogger(f"{__name__}.WatcherManager")

        self._watcher_groups: dict[tuple[str, str], WatcherBackendGroup] = {}
        self._consumer_tasks: list[asyncio.Task[None]] = []
        self._running = False

    @classmethod
    def _load_default_config(cls) -> dict[str, Any]:
        """
        Load external_systems config from main.json via ConfigLoader.

        Returns:
            Dict with "endpoints" and "connections" keys.

        Raises:
            KeyError: If "external_systems" key is missing from main.json
        """
        from lib.core_utils.config_loader import ConfigLoader

        full_config = ConfigLoader().load_config("main.json")
        external_systems = full_config.get(cls.CONFIG_KEY)

        if external_systems is None:
            logger.warning(
                "No '%s' key found in main.json; using empty config",
                cls.CONFIG_KEY,
            )
            return {"endpoints": {}, "connections": {}}

        return external_systems

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
    # Watcher Group Management
    # -------------------------------------------------------------------------

    def add_watcher_group(
        self,
        backend_type: str,
        connection: str,
    ) -> WatcherBackendGroup:
        """
        Add a watcher backend group if not already present.

        Returns existing group if duplicate (deduplication).
        The connection fully identifies the resource via config.

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
        self._logger.info("Added watcher group: %s", key)
        return group

    def get_watcher_groups(self) -> dict[tuple[str, str], WatcherBackendGroup]:
        """Return a copy of the watcher groups dict."""
        return dict(self._watcher_groups)

    # -------------------------------------------------------------------------
    # Configuration Resolution
    # -------------------------------------------------------------------------

    def _resolve_connection_config(self, connection_name: str) -> dict[str, Any]:
        """
        Resolve full backend config from endpoints + connections.

        Algorithm:
        1. Look up connection_name in config["connections"]
        2. Get endpoint_name from connection["endpoint"]
        3. Look up endpoint in config["endpoints"]
        4. Merge: endpoint config + connection["resource"]
        5. Resolve env var references in auth section

        Resource semantics per backend:
        - couchdb: {"db": "database_name"}
        - fs: {"path": "/watch/dir", "recursive": true}
        - postgres: {"table": "schema.table"} or {"channel": "notify_channel"}

        Env var resolution (all under "auth" key):
        - {"user_env": "VAR_NAME"} → resolved["user"] = os.environ["VAR_NAME"]
        - {"pass_env": "VAR_NAME"} → resolved["pass_env_name"] = "VAR_NAME"
          (pass_env_name is the env var NAME, not resolved password;
           CouchDBConnectionManager handles resolution for rotation detection)
        - {"dsn_env": "VAR_NAME"} → resolved["dsn"] = os.environ["VAR_NAME"]
        - Missing env var → RuntimeError (fatal at startup)

        Args:
            connection_name: Logical connection name

        Returns:
            Resolved configuration dict for backend instantiation

        Raises:
            KeyError: If connection or endpoint not found in config
            RuntimeError: If required env var is missing
        """
        connections = self.config.get("connections", {})
        endpoints = self.config.get("endpoints", {})

        if connection_name not in connections:
            raise KeyError(f"Unknown connection: {connection_name}")

        conn = connections[connection_name]
        endpoint_name = conn.get("endpoint")

        if not endpoint_name:
            raise KeyError(
                f"Connection '{connection_name}' missing required 'endpoint' key"
            )

        if endpoint_name not in endpoints:
            raise KeyError(f"Unknown endpoint: {endpoint_name}")

        endpoint = endpoints[endpoint_name]

        # Build merged config
        resolved: dict[str, Any] = {
            "backend": endpoint.get("backend"),
            "url": endpoint.get("url"),
        }

        # Merge resource (backend-specific)
        resource = conn.get("resource", {})
        resolved.update(resource)

        # Resolve auth env vars (all must be under "auth" key)
        auth = endpoint.get("auth", {})
        self._resolve_auth_env_vars(auth, resolved, endpoint_name)

        return resolved

    def _resolve_auth_env_vars(
        self,
        auth: dict[str, Any],
        resolved: dict[str, Any],
        endpoint_name: str,
    ) -> None:
        """
        Resolve environment variable references in auth config.

        All env var references must be under the "auth" key for consistency.

        Args:
            auth: The auth section from endpoint config
            resolved: The resolved config dict to update
            endpoint_name: For error messages

        Raises:
            RuntimeError: If a required env var is missing

        Note:
            For CouchDB backends, pass_env is passed as pass_env_name (not resolved)
            to allow CouchDBConnectionManager to handle password rotation detection.
        """
        # user_env -> user (resolved immediately)
        if "user_env" in auth:
            env_var = auth["user_env"]
            if env_var not in os.environ:
                raise RuntimeError(
                    f"Missing required env var '{env_var}' for endpoint '{endpoint_name}'"
                )
            resolved["user"] = os.environ[env_var]

        # pass_env -> pass_env_name (NOT resolved; connection manager handles it)
        # CouchDBConnectionManager.get_server() enforces env var existence and
        # handles password rotation detection via hash comparison.
        if "pass_env" in auth:
            resolved["pass_env_name"] = auth["pass_env"]

        # dsn_env -> dsn (for Postgres, etc.)
        if "dsn_env" in auth:
            env_var = auth["dsn_env"]
            if env_var not in os.environ:
                raise RuntimeError(
                    f"Missing required env var '{env_var}' for endpoint '{endpoint_name}'"
                )
            resolved["dsn"] = os.environ[env_var]

    # -------------------------------------------------------------------------
    # Backend Instantiation
    # -------------------------------------------------------------------------

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

        Phase 1: Starts backends only (no consumption).
        Phase 2 will add event consumption tasks.

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

    async def stop(self) -> None:
        """
        Stop all watcher backends gracefully.

        Contract:
        - Stops all backends
        - Cancels any consumer tasks (Phase 2)
        - Waits for cleanup
        """
        if not self._running:
            self._logger.debug("WatcherManager not running")
            return

        self._logger.info("WatcherManager stopping...")
        self._running = False

        # Cancel consumer tasks (Phase 2)
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
