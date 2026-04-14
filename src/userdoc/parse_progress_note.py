# src/userdoc/parse_progress_note.py

from __future__ import annotations
from typing import List, Dict, Any
import re
from .shared import CPT_RE

# Blocks look like:
# "CPT Codes:\nName Code Units\nOffice O/p Est Mod 30 Min 99214 1\n..."
# "HCPC Codes:\nName Code Units\nInjection ... J1100 4\n..."

HEADER_RE = re.compile(r"\b(CPT Codes:|HCPC Codes:)\b", re.I)
LINE_RE = re.compile(r"^(?P<name>.+?)\s+(?P<code>[A-Z]?\d{4,5})\s+(?P<units>\d+)\s*$")

def parse_progress_note(pages: List[str], spans: List[Dict]) -> Dict[str, Any]:
    lines: List[Dict] = []
    for s in spans:
        if s["type"] != "progress_note": continue
        text = pages[s["page"]]
        # find header index
        if not HEADER_RE.search(text): 
            continue
        seen = False
        for ln in text.splitlines():
            m = LINE_RE.match(ln.strip())
            if m:
                code = m.group("code")
                units = int(m.group("units"))
                lines.append({
                    "code": code,
                    "units": units,
                    "source_form": "progress_note",
                    "provenance": {"pages":[s["page"]]}
                })
    return {"lines": lines}
