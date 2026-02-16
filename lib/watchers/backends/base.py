"""
Core abstractions for watcher backends.

This module defines the foundational types and interfaces for the generic
watcher infrastructure:

- RawWatchEvent: Backend-agnostic event emitted by all backends
- Checkpoint: Resume-from-last-position marker
- CheckpointStore: Abstract interface for checkpoint persistence
- WatcherBackend: Abstract base class for all backend implementations

Design principles:
- Backends are generic and contain NO domain/realm logic
- Backends emit RawWatchEvent objects via internal queue
- Backends manage their own checkpoints for resume semantics
- Backends handle transient failures with internal retry
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from asyncio import Queue, Task
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__.split(".")[-1])


@dataclass(frozen=True)
class RawWatchEvent:
    """
    Backend-agnostic event emitted by watcher backends.

    This is the universal event format that all backends produce.
    Domain-specific interpretation happens in WatchSpecs (realm layer).

    Attributes:
        id: Unique identifier for the changed resource (doc_id, file path, row pk)
        doc: Document/record content (None for deletions or FS events)
        seq: Backend-specific sequence marker (CouchDB seq, None for FS)
        deleted: True if this event represents a deletion
        meta: Backend-specific metadata (e.g., CouchDB changes entry fields)

    Examples:
        # CouchDB change
        RawWatchEvent(
            id="project:P12345",
            doc={"_id": "...", "type": "project", ...},
            seq="123-abc",
            deleted=False,
            meta={"changes": [{"rev": "1-xyz"}]}
        )

        # Filesystem event (future)
        RawWatchEvent(
            id="/data/flowcells/FC001/RTAComplete.txt",
            doc=None,
            seq=None,
            deleted=False,
            meta={"event_type": "created", "is_directory": False}
        )
    """

    id: str
    doc: dict[str, Any] | None = None
    seq: str | int | None = None
    deleted: bool = False
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class Checkpoint:
    """
    Backend checkpoint for resume-from-last-position semantics.

    Checkpoints allow backends to resume from the last processed position
    after restart, avoiding reprocessing of already-handled events.

    Attributes:
        backend_key: Unique identifier for this backend instance.
                     Format: "{backend}:{connection}" (e.g., "couchdb:projects_db")
        value: Backend-specific checkpoint value (e.g., CouchDB seq string)
        updated_at: ISO timestamp of last update (UTC)

    The checkpoint key is deterministic and stable across restarts,
    derived from the backend type and connection name (not resource,
    since connection fully identifies the resource via config).
    """

    backend_key: str
    value: str | int | None
    updated_at: str | None = None


class CheckpointStore(ABC):
    """
    Abstract interface for checkpoint persistence.

    Default implementation (CouchDBCheckpointStore) stores checkpoints
    in the yggdrasil internal database.

    Methods are synchronous in v1. If the underlying storage is async,
    callers should use asyncio.to_thread() as needed.

    Thread safety: Implementations should be thread-safe for concurrent
    access from multiple backend instances.
    """

    @abstractmethod
    def load(self, backend_key: str) -> Checkpoint | None:
        """
        Load checkpoint for the given backend key.

        Args:
            backend_key: Unique identifier for the backend instance.
                         Format: "{backend}:{connection}"

        Returns:
            Checkpoint if found, None otherwise.
        """
        ...

    @abstractmethod
    def save(self, checkpoint: Checkpoint) -> None:
        """
        Persist checkpoint.

        Args:
            checkpoint: The checkpoint to save. Will overwrite any existing
                        checkpoint with the same backend_key.
        """
        ...


class WatcherBackend(ABC):
    """
    Abstract base for all watcher backends.

    Backends are responsible for:
    - Connecting to an external system (CouchDB, filesystem, Postgres, etc.)
    - Emitting RawWatchEvent objects via internal queue
    - Managing their own checkpoints for resume semantics
    - Handling transient failures with internal retry

    Backends must NOT contain domain/realm logic. They are generic
    infrastructure that multiple realms can share.

    Lifecycle contract:
    - start(): Quick — initializes connection, spawns internal producer task, returns
    - events(): Async iterator yielding from internal queue (call after start())
    - stop(): Cancels producer task, puts sentinel, cleans up

    Queue semantics:
    - Producer (_produce_events) puts RawWatchEvent objects on queue
    - Producer puts None sentinel when exiting normally
    - stop() only adds sentinel if producer was cancelled before it could
    - Consumer (events()) yields until None sentinel received

    Example usage:
        backend = CouchDBBackend(backend_key="couchdb:projects_db", ...)
        await backend.start()

        async for event in backend.events():
            # Process event
            pass

        await backend.stop()
    """

    def __init__(
        self,
        backend_key: str,
        config: dict[str, Any],
        checkpoint_store: CheckpointStore,
        queue_maxsize: int = 1000,
        logger: logging.Logger | None = None,
    ):
        """
        Initialize the watcher backend.

        Args:
            backend_key: Unique identifier for this backend instance.
                         Format: "{backend}:{connection}" (e.g., "couchdb:projects_db")
            config: Resolved configuration dict (from WatcherManager).
                    Contains connection URL, credentials, resource details.
            checkpoint_store: Storage for checkpoint persistence.
            queue_maxsize: Maximum size of internal event queue.
                           Default 1000; tune based on expected throughput.
            logger: Optional logger. If None, uses module logger.
        """
        self.backend_key = backend_key
        self.config = config
        self.checkpoint_store = checkpoint_store
        self._queue_maxsize = queue_maxsize
        self._logger = logger or logging.getLogger(
            f"{__name__}.{self.__class__.__name__}"
        )

        self._running = False
        self._event_queue: Queue[RawWatchEvent | None] = Queue(maxsize=queue_maxsize)
        self._producer_task: Task[None] | None = None

    @property
    def is_running(self) -> bool:
        """True if the backend is currently running."""
        return self._running

    @abstractmethod
    async def _produce_events(self) -> None:
        """
        Internal producer loop. Subclasses implement this to:
        - Connect to external system
        - Poll/stream for changes
        - Put RawWatchEvent objects onto self._event_queue
        - Handle retries internally
        - Exit when self._running is False

        Sentinel contract:
        - MUST use a finally block to put sentinel
        - Use put_nowait(None) with try/except QueueFull to avoid blocking
        - This ensures clean shutdown even if queue is full

        Example pattern::

            try:
                while self._running:
                    # poll and emit events
                    pass
            except asyncio.CancelledError:
                pass  # Normal cancellation
            finally:
                try:
                    self._event_queue.put_nowait(None)
                except asyncio.QueueFull:
                    pass  # Consumer will see _running=False
        """
        ...

    async def start(self) -> None:
        """
        Start the backend. Returns quickly after spawning producer task.

        Contract:
        - Spawns _produce_events() as background task
        - Returns immediately (does NOT block)
        - Idempotent: calling start() on running backend is a no-op

        May raise on fatal configuration errors (e.g., missing credentials),
        but connection errors are typically handled by _produce_events() retry.
        """
        if self._running:
            self._logger.debug("Backend %s already running", self.backend_key)
            return

        self._logger.info("Starting backend: %s", self.backend_key)
        try:
            self._running = True
            self._producer_task = asyncio.create_task(
                self._produce_events(),
                name=f"producer:{self.backend_key}",
            )
        except Exception:
            self._running = False
            raise

    async def stop(self) -> None:
        """
        Stop the backend gracefully.

        Contract:
        - Sets _running = False (signals producer to exit)
        - Cancels producer task if still running
        - Does NOT block on queue operations (producer handles sentinel)

        Note: Producer puts sentinel in a finally block using put_nowait.
        This avoids potential deadlock if the queue is full during shutdown.
        """
        if not self._running:
            self._logger.debug("Backend %s not running", self.backend_key)
            return

        self._logger.info("Stopping backend: %s", self.backend_key)
        self._running = False

        if self._producer_task and not self._producer_task.done():
            self._producer_task.cancel()
            try:
                await self._producer_task
            except asyncio.CancelledError:
                pass  # Expected when cancelling

        self._logger.info("Backend stopped: %s", self.backend_key)

    async def events(self) -> AsyncIterator[RawWatchEvent]:
        """
        Async iterator yielding RawWatchEvent objects from internal queue.

        Must be called after start(). Yields until None sentinel received.

        Usage:
            async for event in backend.events():
                # Process event
                print(event.id, event.doc)
        """
        while True:
            event = await self._event_queue.get()
            if event is None:
                self._logger.debug(
                    "Received sentinel, ending event stream for %s", self.backend_key
                )
                break
            yield event

    def load_checkpoint(self) -> Checkpoint | None:
        """
        Load checkpoint from store.

        Returns:
            The last saved checkpoint, or None if no checkpoint exists.
        """
        return self.checkpoint_store.load(self.backend_key)

    def save_checkpoint(self, value: str | int | None) -> None:
        """
        Save checkpoint to store.

        Args:
            value: Backend-specific checkpoint value (e.g., CouchDB seq string).
                   The backend_key is automatically set from this instance.
        """
        cp = Checkpoint(
            backend_key=self.backend_key,
            value=value,
            updated_at=datetime.now(UTC).isoformat(),
        )
        self.checkpoint_store.save(cp)
        self._logger.debug(
            "Saved checkpoint for '%s': value='%s'", self.backend_key, value
        )
