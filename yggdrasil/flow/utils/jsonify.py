from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Any


def _is_dc_instance(x: Any) -> bool:
    # dataclasses.is_dataclass(x) is True for classes *and* instances.
    # We only want instances for asdict().
    return is_dataclass(x) and not isinstance(x, type)


def to_jsonable(x: Any) -> Any:
    # Primitives fast path
    if x is None or isinstance(x, (bool, int, float, str)):
        return x

    # Common singletons
    if isinstance(x, (date, datetime)):
        return x.isoformat()
    if isinstance(x, Enum):
        return x.value
    if isinstance(x, Path):
        return str(x)

    # Dataclass *instances* only
    if _is_dc_instance(x):
        # asdict returns only builtins/containers -> recurse to be safe
        return to_jsonable(asdict(x))

    # Mapping
    if isinstance(x, Mapping):
        return {str(k): to_jsonable(v) for k, v in x.items()}

    # Sets (stable order for determinism)
    if isinstance(x, set):
        return [to_jsonable(v) for v in sorted(x, key=lambda v: str(v))]

    # Sequences but not strings/bytes
    if isinstance(x, Sequence) and not isinstance(x, (str, bytes, bytearray)):
        return [to_jsonable(v) for v in x]

    # Objects with a to_dict() method (guarded to satisfy Pylance)
    td = getattr(x, "to_dict", None)
    if callable(td):
        return to_jsonable(td())

    # Last resort
    return str(x)
