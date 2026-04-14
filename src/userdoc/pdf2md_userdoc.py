# src/userdoc/pdf2md_userdoc.py
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Any
import json, shutil

from src.ingest.pdf2md import PDF2MDConfig, run_pdf_to_markdown

@dataclass
class UserDocOutputs:
    pages_jsonl: Path
    markdown_path: Path

def process_user_pdf(case_id: str, pdf_path: Path, out_dir: Path, cfg: PDF2MDConfig, force_rebuild: bool=False) -> UserDocOutputs:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    outputs = run_pdf_to_markdown(
        law_version_id=case_id,  # names <case_id>.pages.jsonl
        pdf_path=pdf_path,
        out_dir=out_dir,
        cfg=cfg,
        page_subset=None,
        force_rebuild=force_rebuild,
    )
    # rename markdown to <case_id>.md (do NOT change src/ingest/pdf2md.py)
    target_md = out_dir / f"{case_id}.md"
    if outputs.markdown_path != target_md:
        try:
            shutil.copyfile(outputs.markdown_path, target_md)
        except Exception:
            target_md.write_text(outputs.markdown_path.read_text(encoding="utf-8"), encoding="utf-8")
    return UserDocOutputs(pages_jsonl=outputs.pages_jsonl_path, markdown_path=target_md)

def read_pages_jsonl(pages_jsonl: Path) -> List[Dict[str, Any]]:
    rows = []
    with pages_jsonl.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                rows.append(json.loads(line))
            except Exception:
                pass
    return rows
