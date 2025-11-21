from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

T = TypeVar("T")


# These are the only public APIs you call elsewhere
def structure(data: Any, typ: type[T]) -> T:
    return _TO(data, typ)


def unstructure(obj: Any) -> Any:
    return _FROM(obj)


# --- wiring (private) ---

_TO: Callable[[Any, type], Any]
_FROM: Callable[[Any], Any]

try:
    import cattrs

    _conv = cattrs.Converter()
    _TO = _conv.structure
    _FROM = _conv.unstructure
except Exception:
    # graceful fallback; Potential to swap to Pydantic later by only editing this block
    _TO = lambda data, typ: data
    _FROM = lambda obj: obj
