# resolves law_version_id
# src/utils/versioning.py
from __future__ import annotations
from pathlib import Path

def infer_law_version_id(version_dir: str | Path) -> str:
    """Assumes version_dir looks like law_versions/<law_version_id>"""
    p = Path(version_dir)
    return p.name
