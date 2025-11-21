from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

from .builder import PlanBuilder

Guard = Callable[[dict[str, Any]], bool]
BuildFn = Callable[[PlanBuilder, dict[str, Any]], None]


@dataclass
class StepRule:
    name: str
    when: Guard
    build: BuildFn
    order: int = 100


@dataclass
class StepRuleRegistry:
    _items: list[StepRule] = field(default_factory=list)

    def rule(self, name: str, *, when: Guard, order: int = 100):
        """Explicit decorator."""

        def deco(fn: BuildFn) -> BuildFn:
            self._items.append(StepRule(name=name, when=when, build=fn, order=order))
            return fn

        return deco

    def when(
        self,
        *,
        key: str | None = None,
        equals: Any | None = None,
        in_: Iterable[Any] | None = None,
        present: bool | None = None,
        predicate: Guard | None = None,
        name: str | None = None,
        order: int = 100,
    ):
        """Sugar decorator: @registry.when(key='assay', equals='GEX', name='libcsv_for_gex')"""

        def guard(facts: dict[str, Any]) -> bool:
            if key is not None:
                if present is True and key not in facts:
                    return False
                if equals is not None and facts.get(key) != equals:
                    return False
                if in_ is not None and facts.get(key) not in set(in_):
                    return False
            if predicate is not None and not predicate(facts):
                return False
            return True

        def deco(fn: BuildFn) -> BuildFn:
            self._items.append(
                StepRule(name=name or fn.__name__, when=guard, build=fn, order=order)
            )
            return fn

        return deco

    def active_for(self, facts: dict[str, Any]) -> list[StepRule]:
        return [r for r in sorted(self._items, key=lambda x: x.order) if r.when(facts)]

    def apply(self, *, builder: PlanBuilder, facts: dict[str, Any]) -> None:
        """
        Sugar to activate and build all matching rules.
        """
        for r in self.active_for(facts):
            r.build(builder, facts)


# Handy predicates if you like explicit guards sometimes
def when_eq(key: str, value: Any) -> Guard:
    return lambda facts: facts.get(key) == value


def when_in(key: str, values: Iterable[Any]) -> Guard:
    vs = set(values)
    return lambda facts: facts.get(key) in vs
