"""
Type-driven parameter coercion based on function signatures.

Coerces string values to Path when the function signature declares Path,
Optional[Path], or Union[Path, None]. Uses inspect.unwrap() to safely
handle decorated functions.
"""

import inspect
import types
from pathlib import Path
from typing import Annotated, Any, Union, get_args, get_origin, get_type_hints


def _unwrap_annotated(tp: Any) -> Any:
    """Strip Annotated[T, ...] → T."""
    if get_origin(tp) is Annotated:
        return get_args(tp)[0]
    return tp


def _is_path_type(tp: Any) -> bool:
    """
    Check if tp is Path or Optional[Path] (Union[Path, None]).

    Handles:
      - Path
      - Optional[Path]  (which is Union[Path, None])
      - Union[Path, None]  (typing.Union syntax)
      - Path | None  (PEP 604 syntax, Python 3.10+)
    """
    # Direct Path
    if tp is Path:
        return True

    # Union types (both typing.Union and PEP 604 | syntax)
    origin = get_origin(tp)
    if origin in (Union, types.UnionType):
        args = get_args(tp)
        # Union[Path, None] or Path | None → args = (Path, NoneType)
        non_none_args = [a for a in args if a is not type(None)]
        return len(non_none_args) == 1 and non_none_args[0] is Path

    return False


def coerce_params_to_signature_types(fn: Any, params: dict[str, Any]) -> dict[str, Any]:
    """
    Coerce string params to Path where the function signature expects Path.

    Uses inspect.unwrap() to get the original function past any decorators,
    then reads type hints to determine where Path coercion is needed.

    Handles:
      - Path
      - Annotated[Path, ...]
      - Optional[Path]
      - Union[Path, None]  (typing.Union syntax)
      - Path | None  (PEP 604 syntax, Python 3.10+)

    Only coerces values that are currently strings. Already-Path values
    or None values are left unchanged.

    Args:
        fn: The callable (may be decorated)
        params: Parameter dict (typically spec.params from StepSpec)

    Returns:
        New dict with string→Path coercion applied where appropriate
    """
    # Safety: unwrap decorators to get the original function
    try:
        unwrapped = inspect.unwrap(fn)
    except Exception:
        unwrapped = fn

    # Get type hints (include_extras=True preserves Annotated)
    try:
        hints = get_type_hints(unwrapped, include_extras=True)
    except Exception:
        # If we can't get hints (e.g., forward refs, missing imports), return as-is
        return params

    coerced = dict(params)

    for name, value in params.items():
        # Only coerce strings
        if not isinstance(value, str):
            continue

        # Skip if no hint for this parameter
        if name not in hints:
            continue

        annot = hints[name]

        # Strip Annotated wrapper if present
        base_type = _unwrap_annotated(annot)

        # Check if it's a Path-like type
        if _is_path_type(base_type):
            coerced[name] = Path(value)

    return coerced
