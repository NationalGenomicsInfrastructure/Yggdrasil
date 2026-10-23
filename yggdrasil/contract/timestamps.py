"""The canonical timestamp spelling, and the checks built on it.

Every timestamp a Yggdrasil writer creates in a plan document or a snapshot is
an RFC 3339 date-time in UTC, to the whole second, ending in ``Z``:
``2026-10-22T10:00:00Z``. With one spelling per instant, two timestamps of the
same instant are also equal as text. ``yggdrasil.flow.utils.ygg_time.utcnow_iso``
writes the current time in this spelling; the functions here only check
values they are given and never read the clock.

:data:`CANONICAL_PATTERN` uses ``[0-9]``, not ``\\d``: in Python, ``\\d`` also
matches non-ASCII digits, while JSON Schema patterns follow ECMA-262, where it
does not. ``[0-9]`` means the same to both. Every check matches the whole
string with ``re.fullmatch``, because in Python ``$`` also matches just before
a final newline, which would let ``"2026-10-22T10:00:00Z\\n"`` through.
"""

from __future__ import annotations

import re
from datetime import datetime

CANONICAL_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
CANONICAL_PATTERN = "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"

_CANONICAL = re.compile(CANONICAL_PATTERN)

# The ``date-time`` grammar of RFC 3339, section 5.6. ABNF literals are
# case-insensitive, so ``t`` and ``z`` are valid spellings too.
_DATE_TIME = re.compile(
    r"(?P<year>[0-9]{4})-(?P<month>[0-9]{2})-(?P<day>[0-9]{2})"
    r"[Tt](?P<hour>[0-9]{2}):(?P<minute>[0-9]{2}):(?P<second>[0-9]{2})"
    r"(?:\.[0-9]+)?"
    r"(?:[Zz]|[+-](?P<offset_hour>[0-9]{2}):(?P<offset_minute>[0-9]{2}))"
)

# The UTC suffix earlier releases wrote on plan-document timestamps.
_LEGACY_UTC_SUFFIX = "+00:00"


def is_canonical(value: object) -> bool:
    """Return whether value is a timestamp in the canonical spelling.

    It has to match CANONICAL_PATTERN and parse with CANONICAL_FORMAT: the
    pattern fixes the field widths, which parsing alone does not, and parsing
    rejects a date or time that does not exist, such as ``2026-02-30``, which
    the pattern alone allows.

    Args:
        value: The value to check, of any type.

    Returns:
        bool: True for a string such as ``2026-10-22T10:00:00Z``.
    """
    if not isinstance(value, str) or _CANONICAL.fullmatch(value) is None:
        return False
    try:
        datetime.strptime(value, CANONICAL_FORMAT)
    except ValueError:
        return False
    return True


def is_date_time(value: object) -> bool:
    """Return whether value is an RFC 3339 date-time naming a real date and time.

    Any valid spelling counts: ``Z`` or a numeric offset, with or without a
    fraction of a second, with ``T`` and ``Z`` in either case. The date and
    time have to exist (no ``2026-02-30``, no hour 24, a year from 0001), and
    an offset's hours and minutes have to be in range. Forms that ISO 8601
    allows and RFC 3339 does not are rejected: a date alone, a space instead
    of ``T``, the compact ``20261022T100000Z``. So is a leap second (``:60``),
    which Python's datetime cannot represent and no Yggdrasil writer produces.

    Whether the spelling is canonical is a separate question, answered by
    :func:`is_canonical`.

    Args:
        value: The value to check, of any type.

    Returns:
        bool: True for a valid RFC 3339 date-time string.
    """
    if not isinstance(value, str):
        return False
    match = _DATE_TIME.fullmatch(value)
    if match is None:
        return False
    try:
        datetime(
            int(match["year"]),
            int(match["month"]),
            int(match["day"]),
            int(match["hour"]),
            int(match["minute"]),
            int(match["second"]),
        )
    except ValueError:
        return False
    if match["offset_hour"] is None:
        return True
    return int(match["offset_hour"]) <= 23 and int(match["offset_minute"]) <= 59


def canonical_from_legacy(value: object) -> str | None:
    """Return the canonical spelling of a timestamp an earlier release wrote.

    Earlier releases wrote whole-second UTC times ending in ``Z`` or in
    ``+00:00``. Either one, when it names a real date and time, comes back in
    the canonical spelling of the same instant. Anything else is None: another
    offset (``-00:00`` included), a fraction of a second, a lowercase ``t`` or
    ``z``, surrounding text, a value that is not a string. Nothing is
    truncated or guessed.

    Args:
        value: The recorded value, of any type.

    Returns:
        str | None: The canonical spelling, or None if value is not one of the
        two legacy spellings.
    """
    if not isinstance(value, str):
        return None
    if value.endswith(_LEGACY_UTC_SUFFIX):
        value = value.removesuffix(_LEGACY_UTC_SUFFIX) + "Z"
    return value if is_canonical(value) else None
