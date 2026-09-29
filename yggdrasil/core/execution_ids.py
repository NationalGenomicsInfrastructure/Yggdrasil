"""Allocate and reserve execution IDs that order the attempts at a plan.

An execution ID is ``<timestamp>_<suffix>``: the timestamp in UTC at
microsecond precision (``%Y%m%dT%H%M%S%fZ``), the shape run IDs already have,
and the suffix four lowercase hexadecimal characters, for example
``20260924T120000000001Z_c68e``. Its fixed width makes lexicographic order the
same as timestamp order, which is what lets a reader pick the most recent
attempt at a plan by comparing IDs alone. The ID is fixed before any step runs,
so replaying or re-delivering an old attempt's events cannot reorder attempts.

A clock is not monotonic, so the timestamp is allocated rather than read. It is
the greatest of:

- the current time;
- one microsecond above the timestamp the same allocator allocated last, for
  any plan. One allocator's IDs are therefore strictly increasing even when
  the clock stands still or moves backwards, and it never gives two
  allocations the same timestamp. That floor lives in the allocator instance:
  each engine has its own allocator;
- one microsecond above the latest attempt already recorded for the plan, as
  its history (see :class:`AttemptHistory`) holds it. In a file spool that is
  every canonical attempt directory, including an empty one left when an
  attempt was cancelled or crashed before recording anything. The history is
  read on every allocation, so this holds across restarts, including a restart
  after which the clock is behind the last recorded attempt, and across
  sequential allocators that share one spool.

Once allocated, a candidate is *reserved* (see :func:`reserve_execution_id`):
its attempt directory is created exclusively, so no attempt ever writes into
another's. A candidate whose directory already exists is left alone and the
next candidate, necessarily with a later timestamp, is tried.

The suffix separates attempts whose histories overlap, such as those of
independent allocators or histories that were deleted and restored; it does
not order them. Order comes from the timestamp alone, and the full ID is only a
deterministic tie-break between equal timestamps. Attempts made concurrently
by independent processes, or by two allocators at once, are not ordered
against each other: each reads the history before the other has recorded
anything. Without a history, only the allocator's own floor holds, so a clock
that is behind after a restart can order a new attempt below an old one. No
database is consulted for a floor.

An ID is *canonical* only if it has exactly that shape and its timestamp is a
real date and time (see :func:`execution_timestamp`); the allocator produces
nothing else. Only a caller-built attempt context can carry a non-canonical
ID, and such an ID orders below every canonical one (see
:func:`execution_order_key`), however closely it resembles one. It can
therefore never hide an allocated attempt, and sets no floor.

The timestamp in an ID is an ordering key, not a record of when the attempt
started; the attempt's report carries that.
"""

from __future__ import annotations

import re
import threading
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Protocol

_TIMESTAMP_FORMAT = "%Y%m%dT%H%M%S%fZ"
_RESOLUTION = timedelta(microseconds=1)

# Hexadecimal characters in the uniqueness suffix.
SUFFIX_LENGTH = 4

# Candidate IDs tried, in all, before reservation gives up.
RESERVATION_CANDIDATES = 3

# The whole shape of a canonical ID: the timestamp at its fixed width and the
# suffix in lowercase hex. ASCII digits only, since \d would also match other
# scripts' digits.
_CANONICAL_ID = re.compile(
    r"([0-9]{8}T[0-9]{12}Z)_[0-9a-f]{" + str(SUFFIX_LENGTH) + r"}"
)


class AttemptHistory(Protocol):
    """Where the attempts already recorded for a plan can be read back."""

    def recorded_execution_ids(self, realm: str, plan_id: str) -> list[str]:
        """Return the execution IDs recorded for a plan.

        Args:
            realm: The plan's realm.
            plan_id: The plan.

        Returns:
            list[str]: The recorded execution IDs, in any order.
        """
        ...


class AttemptReserver(Protocol):
    """Where a new attempt's execution ID is claimed before it is used."""

    def reserve(self, realm: str, plan_id: str, execution_id: str) -> bool:
        """Claim an execution ID for a new attempt at a plan, exclusively.

        Args:
            realm: The plan's realm.
            plan_id: The plan.
            execution_id: The ID to claim.

        Returns:
            bool: True if the ID is now claimed for the caller; False if it
            was already taken, in which case nothing was changed.

        Raises:
            Exception: If whether the ID could be claimed cannot be settled,
                such as an I/O failure. That is never a collision.
        """
        ...


class ReservationExhaustedError(Exception):
    """Every candidate execution ID for a new attempt was already taken."""


def format_execution_id(timestamp: datetime, suffix: str) -> str:
    """Build a canonical execution ID from its timestamp and uniqueness suffix.

    Args:
        timestamp: A timezone-aware timestamp; converted to UTC.
        suffix: The uniqueness suffix: four lowercase hex characters.

    Returns:
        str: The execution ID.

    Raises:
        ValueError: If the ID would not be canonical: the suffix is not four
            lowercase hex characters, or the year does not take four digits.
    """
    stamp = timestamp.astimezone(UTC).strftime(_TIMESTAMP_FORMAT)
    execution_id = f"{stamp}_{suffix}"
    if execution_timestamp(execution_id) is None:
        raise ValueError(
            f"{execution_id!r} is not a canonical execution ID: the suffix must "
            f"be {SUFFIX_LENGTH} lowercase hex characters, and the year take "
            f"four digits"
        )
    return execution_id


def execution_timestamp(execution_id: str) -> datetime | None:
    """Return the timestamp of a canonical execution ID.

    An ID is canonical only if it has exactly the shape
    :func:`format_execution_id` produces: the timestamp at its fixed width, an
    underscore and four lowercase hex characters, with the timestamp a real
    date and time. Parsing the timestamp alone is not enough: the parser
    accepts fields without their padding, so ``2026111T000000000000Z`` would
    read as 1 November while sorting above IDs allocated after that. The
    allocator's floor and the consumer's ordering both go through this one
    check, so they cannot disagree about which IDs are ordered by their
    timestamp.

    Args:
        execution_id: The execution ID.

    Returns:
        datetime | None: The ID's timestamp, in UTC; None if the ID is not
        canonical.
    """
    match = _CANONICAL_ID.fullmatch(execution_id)
    if match is None:
        return None
    stamp = match.group(1)
    try:
        timestamp = datetime.strptime(stamp, _TIMESTAMP_FORMAT)
    except ValueError:
        return None
    # The fixed widths leave the parser a single reading; requiring it to
    # round-trip rules out any leniency the parser still has.
    if timestamp.strftime(_TIMESTAMP_FORMAT) != stamp:
        return None
    return timestamp.replace(tzinfo=UTC)


def execution_order_key(execution_id: str) -> tuple[bool, str]:
    """Return the key attempts at a plan are ordered by.

    Canonical IDs (see :func:`execution_timestamp`) order by their timestamp,
    which for their fixed-width shape is the same as their name order; the
    suffix only breaks ties between equal timestamps. Any other ID orders
    below every canonical one, then by name.

    Args:
        execution_id: The execution ID.

    Returns:
        tuple[bool, str]: Whether the ID is canonical, and the ID.
    """
    return (execution_timestamp(execution_id) is not None, execution_id)


def _utc_now() -> datetime:
    """Return the current time in UTC."""
    return datetime.now(UTC)


def _new_suffix() -> str:
    """Return a new uniqueness suffix: the first four hex characters of a UUID."""
    return uuid.uuid4().hex[:SUFFIX_LENGTH]


class ExecutionIdAllocator:
    """Allocates execution IDs for every attempt made through one engine.

    One allocator must serve every way an attempt can start through an engine,
    both the operational callers and direct ``Engine.run`` calls, which is why
    the engine owns it and callers use the engine's. Two allocators share only
    what their common history records.

    Safe to call from several threads at once.
    """

    def __init__(
        self,
        history: AttemptHistory | None = None,
        *,
        clock: Callable[[], datetime] = _utc_now,
        new_suffix: Callable[[], str] = _new_suffix,
    ) -> None:
        """Initialize the allocator.

        Args:
            history: Where the plan's recorded attempts are read back from;
                normally the spool the engine publishes attempts to. None
                keeps ordering within this allocator only.
            clock: Returns the current time, timezone-aware; replaceable so
                tests can hold or rewind it.
            new_suffix: Returns a new uniqueness suffix, four lowercase hex
                characters; replaceable for tests.
        """
        self._history = history
        self._clock = clock
        self._new_suffix = new_suffix
        self._lock = threading.Lock()
        self._last: datetime | None = None

    @property
    def history(self) -> AttemptHistory | None:
        """Where recorded attempts are read back from, if anywhere."""
        return self._history

    def allocate(self, realm: str, plan_id: str) -> str:
        """Allocate a candidate execution ID for a new attempt at a plan.

        Reads the plan's history first, which blocks: call it off the event
        loop. The history is read outside the allocator's lock, so a slow read
        for one plan does not hold up allocations for others. The candidate is
        not claimed; see :func:`reserve_execution_id`.

        Args:
            realm: The plan's realm.
            plan_id: The plan.

        Returns:
            str: A new canonical execution ID that orders above every ID this
            allocator has returned and every ID recorded for the plan.

        Raises:
            ValueError: If the clock returns a timezone-naive datetime, or the
                suffix source returns something other than four lowercase hex
                characters.
            Exception: Whatever reading the history raised.
        """
        recorded = self._recorded_floor(realm, plan_id)
        with self._lock:
            now = self._clock()
            if now.tzinfo is None:
                raise ValueError(
                    "The execution-ID clock must return a timezone-aware datetime"
                )
            candidates = [now.astimezone(UTC)]
            candidates.extend(
                floor + _RESOLUTION
                for floor in (self._last, recorded)
                if floor is not None
            )
            timestamp = max(candidates)
            self._last = timestamp
            return format_execution_id(timestamp, self._new_suffix())

    def _recorded_floor(self, realm: str, plan_id: str) -> datetime | None:
        """Return the latest timestamp recorded for a plan, if any.

        IDs that are not canonical order below every canonical ID anyway, so
        they set no floor and are ignored.

        Args:
            realm: The plan's realm.
            plan_id: The plan.

        Returns:
            datetime | None: The latest recorded timestamp; None if the plan
            has no history, or the allocator has none.
        """
        if self._history is None:
            return None
        stamps = [
            stamp
            for stamp in map(
                execution_timestamp,
                self._history.recorded_execution_ids(realm, plan_id),
            )
            if stamp is not None
        ]
        return max(stamps, default=None)


def reserve_execution_id(
    allocator: ExecutionIdAllocator,
    reserver: AttemptReserver | None,
    realm: str,
    plan_id: str,
) -> tuple[str, bool]:
    """Allocate an execution ID for a new attempt and claim it.

    The one way an attempt's ID is chosen, for operational callers and direct
    ``Engine.run`` calls alike. Each candidate comes from ``allocator``, so a
    candidate found taken is followed by one with a later timestamp, even when
    the clock stands still or moves backwards; a different suffix is never
    accepted in place of a later timestamp. At most
    :data:`RESERVATION_CANDIDATES` candidates are tried. A candidate found
    taken is left exactly as it was. Any other failure is not a collision and
    ends the attempt to reserve at once. Blocks: call it off the event loop.

    Args:
        allocator: Allocates the candidates.
        reserver: Claims a candidate, exclusively; None when there is nowhere
            to claim one, and the first candidate is then used unclaimed.
        realm: The plan's realm.
        plan_id: The plan.

    Returns:
        tuple[str, bool]: The execution ID, and whether it was claimed.

    Raises:
        ReservationExhaustedError: If every candidate was already taken.
        Exception: Whatever allocating or claiming a candidate raised,
            other than finding it taken.
    """
    taken: list[str] = []
    for _ in range(RESERVATION_CANDIDATES):
        execution_id = allocator.allocate(realm, plan_id)
        if reserver is None:
            return execution_id, False
        if reserver.reserve(realm, plan_id, execution_id):
            return execution_id, True
        taken.append(execution_id)
    raise ReservationExhaustedError(
        f"Could not reserve an execution ID for plan '{plan_id}' (realm "
        f"'{realm}'): all {RESERVATION_CANDIDATES} candidates were already "
        f"taken: {', '.join(taken)}"
    )
