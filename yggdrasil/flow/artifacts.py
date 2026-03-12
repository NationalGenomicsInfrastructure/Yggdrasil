from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


class ArtifactRefProtocol(Protocol):
    def key(self) -> str: ...
    def resolve_path(self, scope_dir: Path) -> Path: ...


def ensure_artifact_ref(ref: object) -> ArtifactRefProtocol:
    if not hasattr(ref, "key") or not hasattr(ref, "resolve_path"):
        raise TypeError("artifact ref must implement key() and resolve_path(scope_dir)")
    return ref  # type: ignore[return-value]


@dataclass(frozen=True)
class SimpleArtifactRef:
    key_name: str
    folder: str
    filename: str | None = None

    def key(self) -> str:
        return self.key_name

    def resolve_path(self, scope_dir: Path) -> Path:
        base = scope_dir / self.folder
        base.mkdir(parents=True, exist_ok=True)
        return base if self.filename is None else (base / self.filename)
