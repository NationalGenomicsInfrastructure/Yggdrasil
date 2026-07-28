from __future__ import annotations

import asyncio
import os

from lib.core_utils.runtime_paths import resolve_event_spool
from lib.ops.consumer import FileSpoolConsumer
from lib.ops.sinks.couch import OpsWriter
from lib.storage.protocols import OpsSnapshotSink

# TODO: consider moving to lib/ops/services/ when more services land.


class OpsConsumerService:
    """Periodic event-spool consumer writing plan_status snapshots.

    The snapshot sink is normally injected by YggdrasilCore from its
    InternalStorageBundle. Bare construction falls back to the legacy
    CouchDB ``OpsWriter`` (honoring ``OPS_DB``, deprecated).
    """

    def __init__(
        self,
        interval_sec: float = 2.0,
        db_name: str | None = None,
        *,
        writer: OpsSnapshotSink | None = None,
    ):
        """
        Args:
            interval_sec: Seconds between spool consumption cycles.
            db_name: Legacy CouchDB ops database name override; only used
                when no ``writer`` is injected.
            writer: Injected snapshot sink (backend-neutral).
        """
        self.interval = interval_sec
        self.spool = resolve_event_spool()
        if writer is None:
            writer = OpsWriter(
                db_name=db_name or os.environ.get("OPS_DB") or "yggdrasil_ops"
            )
        self.writer = writer
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    async def _loop(self) -> None:
        consumer = FileSpoolConsumer(self.spool, self.writer)
        while not self._stop.is_set():
            consumer.consume()  # small burst of work
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.interval)
            except TimeoutError:
                pass

    def start(self) -> None:
        if not self._task or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(self._loop(), name="ops-consumer")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            try:
                await self._task
            except asyncio.CancelledError:
                # Task was cancelled (expected during shutdown)
                pass
