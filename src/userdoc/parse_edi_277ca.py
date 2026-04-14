# src/userdoc/parse_edi_277ca.py

from __future__ import annotations
from pathlib import Path
from typing import Dict, Any, List
import json, re

from .pdf2md_userdoc import read_pages_jsonl

STATUS_RE = re.compile(r"\b(ACCEPT|ACCEPTED|REJECT|REJECTED|ACCEPTANCE|REJECTION)\b", re.I)
DATE_RE   = re.compile(r"\b(?:20\d{2}[-/]\d{2}[-/]\d{2}|\d{2}[/-]\d{2}[/-]\d{4})\b")
TIME_RE   = re.compile(r"\b([01]\d|2[0-3]):\d{2}(?::\d{2})?\b")

def parse_edi_277ca(pages_jsonl: Path, segments_json: Path, out_json: Path) -> Dict[str, Any]:
    pages = read_pages_jsonl(pages_jsonl)
    segs  = json.loads(Path(segments_json).read_text(encoding="utf-8"))

    targets = set()
    for sp in segs.get("spans", []):
        if sp["type"] == "edi_277ca":
            targets.update(range(sp["start"], sp["end"]+1))

    events: List[Dict[str, Any]] = []
    for p in pages:
        if targets and p["page"] not in targets:
            continue
        txt = p.get("text") or ""
        if not txt.strip():
            continue
        status = None
        m = STATUS_RE.search(txt)
        if m:
            status = m.group(1).upper()
        dates = DATE_RE.findall(txt)
        times = TIME_RE.findall(txt)
        events.append({
            "page": p["page"],
            "status": status,
            "dates": dates,
            "times": times,
        })

    payload = {"events": events}
    Path(out_json).write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload
