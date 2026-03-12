"""
JSON Logic filter evaluation for WatchSpecs.

Provides a thin wrapper around ``panzi_json_logic`` with explicit
error reporting via :class:`FilterResult`.
"""

from __future__ import annotations

import logging
from dataclasses import asdict
from typing import TYPE_CHECKING, Any

import json_logic  # panzi_json_logic

if TYPE_CHECKING:
    from lib.watchers.backends.base import RawWatchEvent


class FilterResult:
    """Result of filter evaluation."""

    __slots__ = ("matched", "error")

    def __init__(self, matched: bool, error: Exception | None = None):
        self.matched = matched
        self.error = error

    def __bool__(self) -> bool:
        return self.matched


def evaluate_filter(
    filter_expr: dict[str, Any] | None,
    raw_event_dict: dict[str, Any],
    logger: logging.Logger | None = None,
) -> FilterResult:
    """
    Evaluate a JSON Logic *filter_expr* against a raw event dict.

    Args:
        filter_expr:
            JSON Logic expression, or ``None`` (always matches).
        raw_event_dict:
            :class:`RawWatchEvent` converted to ``dict`` via
            :func:`raw_event_to_dict`.
        logger:
            Optional logger for error reporting.

    Returns:
        :class:`FilterResult` with ``matched=True/False`` and
        ``error`` if evaluation failed.

    Error handling:
        * Evaluation errors → ``matched=False`` with ``error`` populated.
        * Caller can distinguish "filter rejected" from "filter errored".
        * Errors are logged if a logger is provided.
    """
    if filter_expr is None:
        return FilterResult(matched=True)

    try:
        result = json_logic.jsonLogic(filter_expr, raw_event_dict)
        return FilterResult(matched=bool(result))
    except Exception as e:
        if logger:
            logger.warning(
                "Filter evaluation error (treating as no-match): %s | "
                "filter=%r | event_id=%s",
                e,
                filter_expr,
                raw_event_dict.get("id", "?"),
            )
        return FilterResult(matched=False, error=e)


def raw_event_to_dict(event: RawWatchEvent) -> dict[str, Any]:
    """Convert a :class:`RawWatchEvent` dataclass to ``dict`` for filter evaluation."""
    return asdict(event)
