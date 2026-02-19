import datetime
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

from lib.core_utils.event_types import EventType
from lib.core_utils.logging_utils import custom_logger


class YggdrasilEvent:
    """
    A lightweight container for events that watchers produce and YggdrasilCore consumes.

    Attributes:
        event_type (EventType): The event type (e.g. EventType.PROJECT_CHANGE).
        payload (Any): Arbitrary data relevant to the event (e.g. info about a changed file).
        source (str): Identifier of the event source (e.g. "filesystem", "couchdb").
        timestamp (datetime.datetime): When the event was created.
    """

    __slots__ = ("event_type", "payload", "source", "timestamp")

    def __init__(self, event_type: EventType | str, payload: Any, source: str):
        if isinstance(event_type, EventType):
            normalized = event_type
        elif isinstance(event_type, str):
            try:
                normalized = EventType(event_type)
            except ValueError:
                try:
                    normalized = EventType[event_type]
                except KeyError as exc:
                    raise ValueError(f"Unknown event_type: {event_type!r}") from exc
        else:
            raise TypeError(
                "event_type must be an EventType or str convertible to EventType"
            )

        self.event_type: EventType = normalized
        self.payload = payload
        self.source = source
        self.timestamp = datetime.datetime.now()

    def __repr__(self) -> str:
        return (
            f"YggdrasilEvent("
            f"event_type={self.event_type!r}, "
            f"payload={self.payload!r}, "
            f"source={self.source!r}, "
            f"timestamp={self.timestamp.isoformat()})"
        )


class AbstractWatcher(ABC):
    """
    Base class for watchers that monitor an external system (filesystem, database, etc.)
    and produce YggdrasilEvent objects. Each watcher typically runs in an asynchronous
    loop until stopped, emitting events to a callback as changes occur.

    Subclasses must implement:
      - start(): Begin monitoring (set self._running = True) until stopped.
      - stop():  Stop monitoring (set self._running = False) and clean up resources.

    Example usage:
        class MyFileWatcher(AbstractWatcher):
            async def start(self):
                self._running = True
                while self._running:
                    # Poll or listen for file changes
                    ...
                    await self.emit(payload, source="filesystem")
                    ...
                    await asyncio.sleep(1)

            async def stop(self):
                self._running = False
                # Additional resource cleanup if needed
    """

    def __init__(
        self,
        on_event: Callable[[YggdrasilEvent], None],
        event_type: EventType,
        name: str | None = None,
        logger: logging.Logger | None = None,
    ):
        """
        Args:
            on_event: A callback to be invoked with a YggdrasilEvent whenever
                a relevant change is detected.
            event_type: EventType that handlers will register for
            name: optional human name for tracking/logging
            logger: (Optional) A logger instance. If None, a default logger
                named after the concrete subclass will be used.
        """
        self._running = False
        self._on_event_callback = on_event
        self.event_type = event_type
        self.name = name or self.__class__.__name__
        self._logger = logger or custom_logger(f"{__name__}.{type(self).__name__}")

    @abstractmethod
    async def start(self):
        """
        Start monitoring. Typically, this involves setting self._running = True
        and entering an asynchronous loop until self._running is False.
        Subclasses should catch exceptions within the loop to prevent
        silent failures, and log errors via self._logger.
        """
        pass

    @abstractmethod
    async def stop(self):
        """
        Stop monitoring. Typically, this involves setting self._running = False
        and cleaning up any resources (e.g., closing files, stopping observers).
        """
        pass

    @property
    def is_running(self) -> bool:
        """Indicates whether the watcher is actively monitoring for events."""
        return self._running

    async def emit(self, payload: Any, source: str | None = None) -> None:
        """
        Create a YggdrasilEvent and invoke the callback. Subclasses can call
        this method whenever they detect a new event to be processed.

        Args:
            payload: Any data relevant to the event (e.g., file paths, doc IDs).
            source: Identifier of the source (e.g., "filesystem", "couchdb").

        Raises:
            Exception: Propagates any exception if the callback fails. Typically
            such exceptions should be handled in the subclass.
        """
        event = YggdrasilEvent(self.event_type, payload, source or self.name)
        # self._logger.debug(f"Emitting event from {source}: {event}")
        self._on_event_callback(event)
