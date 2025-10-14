import hashlib
import os
from pathlib import Path


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def dirhash_stats(root: Path) -> str:
    h = hashlib.sha256()
    files: list[Path] = []
    for root_dir, _, filenames in os.walk(root):
        for f in filenames:
            files.append(Path(root_dir, f))
    for fpath in sorted(files):
        rel = fpath.relative_to(root)
        st = fpath.stat()
        h.update(str(rel).encode())
        h.update(str(st.st_size).encode())
        h.update(str(int(st.st_mtime)).encode())
    return f"dirhash:{h.hexdigest()}"
