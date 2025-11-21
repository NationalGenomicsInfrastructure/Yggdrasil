import inspect
from collections.abc import Callable
from importlib import import_module
from typing import Any


def fn_ref_from_callable(fn: Callable[..., Any]) -> str:
    """Return 'module.path:func_name' for a callable (unwraps decorators)."""
    fn = inspect.unwrap(fn)
    return f"{fn.__module__}:{fn.__name__}"


def resolve_callable(fn_ref: str | Callable[..., Any]) -> Callable[..., Any]:
    if callable(fn_ref):
        return fn_ref
    if ":" in fn_ref:
        mod_name, func_name = fn_ref.split(":", 1)
    else:
        mod_name, func_name = fn_ref.rsplit(".", 1)
    mod = import_module(mod_name)
    return getattr(mod, func_name)
