# src/userdoc/detect_forms.py

from __future__ import annotations
from pathlib import Path
from typing import List, Dict, Any
import re, json
from .pdf2md_userdoc import read_pages_jsonl

def _count_hits(text: str, markers: List[str]) -> int:
    return sum(1 for m in markers if re.search(m, text or "", re.I))

def detect_forms(pages_jsonl: Path, forms_cfg: Dict[str, Any], out_path: Path) -> List[Dict[str, Any]]:
    pages = read_pages_jsonl(pages_jsonl)
    page_labels: List[Dict[str, Any]] = []
    thr = forms_cfg.get("unknown", {}).get("score_threshold", 0)

    for p in pages:
        txt = p.get("text") or ""
        scores = {}
        for name, spec in (forms_cfg.get("forms") or {}).items():
            hits = _count_hits(txt, spec.get("markers", []))
            if hits:
                scores[name] = hits * spec.get("score", 1)
        best = max(scores.items(), key=lambda kv: kv[1])[0] if scores else "unknown"
        if scores and max(scores.values()) < thr:
            best = "unknown"
        page_labels.append({"page": p["page"], "label": best, "scores": scores, "text_len": len(txt)})

    # merge contiguous spans with ≤gap pages
    merged: List[Dict[str, Any]] = []
    gap = forms_cfg.get("window_merge", {}).get("max_gap_pages", 0)
    last = None
    for rec in page_labels:
        lbl = rec["label"]
        if last is None:
            last = {"type": lbl, "start": rec["page"], "end": rec["page"]}
            continue
        if lbl == last["type"] or (lbl != "unknown" and rec["page"] - last["end"] <= gap and lbl == last["type"]):
            last["end"] = rec["page"]
        else:
            merged.append(last)
            last = {"type": lbl, "start": rec["page"], "end": rec["page"]}
    if last: merged.append(last)

    out = {"pages": page_labels, "spans": merged}
    Path(out_path).write_text(json.dumps(out, indent=2), encoding="utf-8")
    return merged
