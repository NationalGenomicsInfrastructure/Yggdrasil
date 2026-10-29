"""Showing a recorded value inside a diagnostic.

A message about a stored document often has to show a value the document
recorded: a refused marker, a token that is not an integer, a status that is
not ``approved``. Stored content can hold anything, and showing it naively
fails in two ways. A huge value floods the log line that reports it. And
representing some values raises: Python refuses to convert an integer with
more digits than ``sys.get_int_max_str_digits()`` allows (4300 by default) to
text, even inside a container, so the diagnostic itself would fail instead of
reporting the problem.

:func:`describe_recorded` is the one way Yggdrasil shows a recorded value in a
diagnostic: bounded, and never raising, whatever the value holds.
"""

from __future__ import annotations

import reprlib

# The longest description describe_recorded returns.
MAX_LENGTH = 200

# reprlib renders only the first items of a container, a few levels of
# nesting and the first characters of a string or other scalar; the result is
# then cut to MAX_LENGTH characters.
_REPR = reprlib.Repr()
_REPR.maxstring = 80
_REPR.maxother = 80


def describe_recorded(value: object) -> str:
    """Return a bounded representation of a recorded value, for a diagnostic.

    A value whose representation fails, such as an integer too long to
    convert to text or a container holding one, is shown by its type name
    instead: ``<unrepresentable int>``.

    Args:
        value: The recorded value, of any type.

    Returns:
        str: At most MAX_LENGTH characters; never raises.
    """
    try:
        text = _REPR.repr(value)
    except Exception:
        return f"<unrepresentable {type(value).__name__}>"
    if len(text) > MAX_LENGTH:
        return text[: MAX_LENGTH - 3] + "..."
    return text
