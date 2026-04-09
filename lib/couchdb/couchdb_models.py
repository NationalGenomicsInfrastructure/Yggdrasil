"""
Typed structures for CouchDB _changes feed consumption.

Kept in lib/couchdb/ alongside couchdb_connection.py because these types
are CouchDB-specific and used by both the connection layer and the backend.
"""

from __future__ import annotations

import dataclasses
import enum


class FeedMode(str, enum.Enum):
    """CouchDB _changes feed mode.

    String values match the ``feed=`` query-parameter values accepted by CouchDB.
    The ``str`` mixin lets instances be passed directly as parameter values without
    calling ``.value``.
    """

    NORMAL = "normal"
    LONGPOLL = "longpoll"


@dataclasses.dataclass(frozen=True)
class ChangesRow:
    """One row from a CouchDB _changes response.

    Attributes:
        id:      Document ID.
        seq:     CouchDB sequence token for this change.
        deleted: True if the document was deleted.
        rev:     First revision token from ``changes[0].rev``.  Informational
                 only — not used in checkpoint or dispatch logic.
    """

    id: str
    seq: str | int
    deleted: bool = False
    rev: str | None = None


@dataclasses.dataclass(frozen=True)
class ChangesBatch:
    """Parsed result of one CouchDB _changes HTTP response.

    Attributes:
        rows:     Parsed change rows.
        last_seq: CouchDB sequence token representing the end of this response.
                  Use as ``since=`` on the next poll.
        pending:  Number of changes still waiting to be fetched.
                  ``0`` means the feed is caught up.
    """

    rows: list[ChangesRow]
    last_seq: str | int | None
    pending: int
