from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class DistilledFacts:
    """Realm-agnostic envelope; realm-specific payload is in `data`."""

    realm: str
    scope: dict[str, Any]
    version: int = 1
    data: dict[str, Any] = field(default_factory=dict)


class FactDistiller(ABC):
    """Contract every realm implements."""

    @abstractmethod
    def distil_facts(
        self, *, doc: dict[str, Any], realm: str, scope: dict[str, Any]
    ) -> DistilledFacts: ...
