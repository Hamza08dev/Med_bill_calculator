# sha256 for provenance
# src/utils/hashing.py
from __future__ import annotations
import hashlib
from pathlib import Path

_CHUNK = 1024 * 1024

def sha256_file(path: str | Path) -> str:
    p = Path(path)
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(_CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()
