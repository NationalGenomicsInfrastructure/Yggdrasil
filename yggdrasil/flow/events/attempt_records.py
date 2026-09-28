"""Where a file spool keeps each execution attempt's events.

A :class:`~yggdrasil.flow.events.emitter.FileSpoolEmitter` files every event of
an attempt under that attempt's own directory, named after its execution ID::

    <spool>/<realm>/<plan_id>/attempts/<execution_id>/
        plan_attempt_started.json
        plan_attempt_report.json
        steps/<step_id>/0001_step_started.json
                        0002_step_succeeded.json

The attempt directory keeps one attempt's records apart from another's, so the
two plan-level records have fixed names. Each step directory holds that step's
events in this attempt as one numbered stream, whoever published them: the
engine's skip, block and retry events, the ``@step`` wrapper's lifecycle, the
step's own progress and artifacts, and its DataAccess write traces. Numbers
have at least four digits and order numerically. A step directory records what
was observed about the step; it does not mean the step ran. A blocked step has
one too.

Each ``/``-separated part of a step ID becomes one directory level, so
``lane_1/process`` is filed in ``steps/lane_1/process/``. A part the
filesystem would read as something else is escaped with a leading ``%`` (see
:func:`step_events_dir`), so every step ID has a directory of its own, and no
step's directory is ever another step's event file. Readers only open the step
directories an attempt's step inventory names, and read only the files
directly in each, so a step whose ID nests under another's stays apart from
it. Events keep the step's own ID; only the directory name is escaped.

**Two readers, two jobs.** The execution-ID allocator reads only the names of
a plan's attempt directories (:class:`SpoolAttemptHistory`): every attempt
directory sets the floor new IDs are allocated above, including an empty one
left by an attempt that was cancelled or crashed before recording anything.
The ops consumer shows an attempt, so it needs a record: an attempt is
*observed* only once its start or report record exists, and an empty
reservation never looks like a running attempt. Both order attempts through
``yggdrasil.core.execution_ids``, so they agree on which is newest.

Before an attempt publishes anything, its directory is *reserved*: created
exclusively, so no attempt ever writes into another's
(:class:`SpoolAttemptDirectories`).

Events published outside any attempt, by a step context or DataAccess trace
that carries no execution correlation, keep the older layout,
``<plan_id>/<step_id>/<run_id>/``, which never overlaps ``attempts/``.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TypeGuard

# Published when an attempt is admitted, before its plan is validated or any
# step runs: the execution's identity and its planned step inventory.
ATTEMPT_STARTED_EVENT = "plan.attempt_started"

# Published once when an attempt ends, however it ends: its closed report.
ATTEMPT_REPORT_EVENT = "plan.attempt_report"

# Published once per blocked step, when a failure blocks it.
STEP_BLOCKED_EVENT = "step.blocked"

# The directory under a plan's spool directory holding one directory per
# attempt, and the directory under an attempt's holding one per step.
ATTEMPTS_DIR = "attempts"
STEPS_DIR = "steps"

# Prefixes a part of a step ID that would be ambiguous as a directory name.
# No file the spool writes starts with it (see step_event_filename).
STEP_PART_ESCAPE = "%"

# Endings of the files the spool writes into a step's directory: an event, and
# an event while it is being written.
_EVENT_FILE_SUFFIXES = (".json", ".json.tmp")


def record_filename(event_type: str) -> str:
    """Return the file name of an attempt's plan-level record of one type.

    Args:
        event_type: The record's event type, e.g. ``plan.attempt_started``.

    Returns:
        str: The file name, e.g. ``plan_attempt_started.json``.
    """
    return f"{event_type.replace('.', '_')}.json"


def step_event_filename(seq: int, event_type: str) -> str:
    """Return the file name of one event in a step's numbered stream.

    Args:
        seq: The event's number in the step's stream, from 1.
        event_type: The event's type, e.g. ``step.started``.

    Returns:
        str: The file name, e.g. ``0001_step_started.json``. Numbers past 9999
        take more digits; readers order by the event's ``seq``, not the name.
        It starts with the number and ends in ``.json``, so it never starts
        with :data:`STEP_PART_ESCAPE`, which is what keeps it apart from the
        escaped directory of a step nested below (see
        :func:`step_events_dir`).
    """
    return f"{seq:04d}_{event_type.replace('.', '_')}.json"


def is_record_key(execution_id: object) -> TypeGuard[str]:
    """Whether an execution ID can name an attempt's directory in the spool.

    An ID that is empty, is a relative path component, or contains a path
    separator would put an attempt's events somewhere other than in its own
    directory beside the plan's other attempts.

    Args:
        execution_id: The candidate execution ID.

    Returns:
        bool: True if the ID is a nonempty string usable as a directory name.
    """
    return (
        isinstance(execution_id, str)
        and execution_id not in ("", ".", "..")
        and "/" not in execution_id
        and "\\" not in execution_id
    )


def attempts_dir(root: str | Path, realm: str, plan_id: str) -> Path:
    """Return the directory holding a plan's attempt directories.

    Args:
        root: The spool root.
        realm: The plan's realm.
        plan_id: The plan.

    Returns:
        Path: ``<root>/<realm>/<plan_id>/attempts``.
    """
    return Path(root) / realm / plan_id / ATTEMPTS_DIR


def attempt_dir(root: str | Path, realm: str, plan_id: str, execution_id: str) -> Path:
    """Return the directory holding one attempt's events.

    Args:
        root: The spool root.
        realm: The plan's realm.
        plan_id: The plan.
        execution_id: The attempt.

    Returns:
        Path: ``<root>/<realm>/<plan_id>/attempts/<execution_id>``.

    Raises:
        ValueError: If the execution ID cannot name a directory (see
            :func:`is_record_key`).
    """
    if not is_record_key(execution_id):
        raise ValueError(
            f"Execution ID {execution_id!r} cannot name an attempt directory"
        )
    return attempts_dir(root, realm, plan_id) / execution_id


def _step_dir_part(part: str) -> str:
    """Return the directory name for one ``/``-separated part of a step ID.

    A part is escaped with :data:`STEP_PART_ESCAPE` when, used as it is, it
    would not name a directory of its own: an empty part or ``.`` would be
    normalized away, and ``..`` would name the parent; a part ending in
    ``.json`` or ``.json.tmp`` could be the name of an event file the spool
    writes into the step directory above it; and a part already starting with
    the escape could be mistaken for an escaped one. Every other part is kept
    as it is.

    Args:
        part: One part of a step ID.

    Returns:
        str: The directory name. Distinct parts always give distinct names,
        and an escaped name never is ``.`` or ``..``, never is empty, and
        never equals an event file's name, since event file names never start
        with the escape.
    """
    if (
        part in ("", ".", "..")
        or part.startswith(STEP_PART_ESCAPE)
        or part.endswith(_EVENT_FILE_SUFFIXES)
    ):
        return STEP_PART_ESCAPE + part
    return part


def step_events_dir(attempt_directory: Path, step_id: str) -> Path:
    """Return the directory holding one step's events within an attempt.

    The step ID is split at ``/``, and each part becomes one directory level,
    escaped where it would be ambiguous (see :func:`_step_dir_part`):

    - ``lane_1/process`` is filed in ``steps/lane_1/process``;
    - ``lane/./process`` in ``steps/lane/%./process``, apart from
      ``lane/process``;
    - ``lane/0001_step_started.json`` in ``steps/lane/%0001_step_started.json``,
      apart from ``lane``'s event file of that name.

    Distinct step IDs therefore always get distinct directories inside
    ``steps/``, whatever characters they use. The writer and every reader use
    this one mapping.

    Args:
        attempt_directory: The attempt's directory (see :func:`attempt_dir`).
        step_id: The step.

    Returns:
        Path: The step's directory under ``<attempt_directory>/steps``.
    """
    parts = (_step_dir_part(part) for part in step_id.split("/"))
    return attempt_directory.joinpath(STEPS_DIR, *parts)


def step_id_of_dir(name: str) -> str:
    """Return the step ID a single-level step directory name stands for.

    The inverse of :func:`step_events_dir` for a step ID without ``/``, for a
    reader that has to list step directories instead of taking step IDs from
    an attempt's inventory.

    Args:
        name: A directory name directly under ``steps/``.

    Returns:
        str: The step ID.
    """
    return name.removeprefix(STEP_PART_ESCAPE)


def attempt_spool_path(
    realm: str,
    plan_id: str,
    execution_id: str,
    filename: str,
    *,
    step_id: str | None = None,
) -> dict[str, str]:
    """Return the ``_spool_path`` hints that file an event under its attempt.

    Args:
        realm: The plan's realm.
        plan_id: The plan.
        execution_id: The attempt the event belongs to.
        filename: The event's file name.
        step_id: The step whose stream the event belongs to; None for a
            plan-level record.

    Returns:
        dict[str, str]: The hints a FileSpoolEmitter routes the event by.
    """
    hints = {
        "realm": realm,
        "plan_id": plan_id,
        "execution_id": execution_id,
        "filename": filename,
    }
    if step_id is not None:
        hints["step_id"] = step_id
    return hints


class SpoolAttemptHistory:
    """Reads back the attempts a file spool holds for a plan.

    This is the history an execution-ID allocator consults so that a new
    attempt sorts above every attempt already there, including attempts made
    by an earlier process. It reads only the names of the plan's attempt
    directories, never an event: every attempt directory counts, including an
    empty reservation, and an unreadable or corrupt record does not remove its
    directory's place in the order. It reads only the spool it was given:
    nothing here resolves a default spool location.

    Attributes:
        root: The spool root, as passed to the FileSpoolEmitter that wrote it.
    """

    def __init__(self, root: str | Path) -> None:
        """Initialize the reader.

        Args:
            root: The spool root to read.
        """
        self.root = Path(root)

    def recorded_execution_ids(self, realm: str, plan_id: str) -> list[str]:
        """Return the name of every attempt directory of a plan.

        Names that are not canonical execution IDs are returned too; the
        allocator gives them no floor.

        Args:
            realm: The plan's realm.
            plan_id: The plan.

        Returns:
            list[str]: The attempt directories' names, in no particular order;
            none if the plan has no attempt directory yet.

        Raises:
            OSError: If the plan's attempt directories exist but cannot be
                listed. The same spool is about to receive the new attempt, so
                that failure is raised for the caller to classify.
        """
        try:
            with os.scandir(attempts_dir(self.root, realm, plan_id)) as entries:
                return [entry.name for entry in entries if entry.is_dir()]
        except FileNotFoundError:
            return []


class SpoolAttemptDirectories:
    """Reserves new attempts' directories in a file spool.

    Reserving an attempt creates its directory, and only succeeds if the
    directory did not exist yet, so two attempts can never share one, whoever
    else is allocating IDs for the same plan. A reserved directory stays
    reserved even if its attempt never publishes anything. It reserves only in
    the spool it was given: nothing here resolves a default spool location.

    Attributes:
        root: The spool root, as passed to the FileSpoolEmitter that writes
            the attempts' events.
    """

    def __init__(self, root: str | Path) -> None:
        """Initialize the reserver.

        Args:
            root: The spool root to reserve in.
        """
        self.root = Path(root)

    def reserve(self, realm: str, plan_id: str, execution_id: str) -> bool:
        """Create an attempt's directory, unless it already exists.

        The plan's ``attempts`` directory is created first, if needed. Only
        the attempt directory itself is created exclusively, with no separate
        existence check, so finding it already there is the one outcome that
        counts as the ID being taken.

        Args:
            realm: The plan's realm.
            plan_id: The plan.
            execution_id: The attempt.

        Returns:
            bool: True if the directory was created; False if it already
            existed, in which case it is left exactly as it was.

        Raises:
            ValueError: If the execution ID cannot name a directory.
            OSError: If the directory cannot be created for any other reason,
                including failures creating its parents, or a file standing
                where a parent directory should be.
        """
        directory = attempt_dir(self.root, realm, plan_id, execution_id)
        directory.parent.mkdir(parents=True, exist_ok=True)
        try:
            directory.mkdir()
        except FileExistsError:
            return False
        return True
