# src/userdoc/page_llm_extractor.py
from __future__ import annotations
from pathlib import Path
from typing import Dict, Any, List
import json

from .pdf2md_userdoc import read_pages_jsonl
from .shared import load_yaml_from_configs, is_cpt_hcpcs, looks_like_icd10
from src.llm.client import llm_complete

PROMPT = """You are an expert medical billing analyst.
From the page text below, extract ONLY billable claim line items (CPT or HCPCS). STRICTLY:
- CPT must be exactly 5 digits (e.g., 97110, 98941).
- HCPCS must be 1 letter + 4 digits (e.g., J1100, A4550). DO NOT return diagnosis (ICD-10) codes like M54.16, S13.4XXA.
- Each item MUST include a billed_amount if it appears on page; set null only if truly not present.

Return ONLY a JSON array of objects with keys:
- code, modifiers (0-4 2-char strings), units (int, default 1),
- dos_from (YYYY-MM-DD or null), dos_to (YYYY-MM-DD or null or same as from),
- place_of_service (2-digit or "11"),
- billed_amount (number or null).

If no items, return [].

Page Text:
"""

def extract_all_pages_llm(pages_jsonl: Path, out_json: Path) -> Dict[str, Any]:
    pages = read_pages_jsonl(pages_jsonl)
    model_cfg = load_yaml_from_configs("models.yaml").get("userdoc_extractor", {})

    lines: List[Dict[str, Any]] = []
    for p in pages:
        txt = (p.get("text") or "")
        if not txt.strip():
            continue
        try:
            raw = llm_complete(model_cfg, PROMPT + txt)
            s = raw[raw.find("["): raw.rfind("]")+1]
            arr = json.loads(s)
            for l in arr:
                code = str(l.get("code","")).upper()
                if looks_like_icd10(code) or not is_cpt_hcpcs(code):
                    continue
                lines.append({
                    "code": code,
                    "modifiers": [str(m).upper() for m in (l.get("modifiers") or [])],
                    "units": int(round(float(l.get("units", 1)))),
                    "dos_from": l.get("dos_from"),
                    "dos_to": l.get("dos_to") or l.get("dos_from"),
                    "place_of_service": str(l.get("place_of_service") or "11"),
                    "billed_amount": float(l["billed_amount"]) if l.get("billed_amount") is not None else None,
                    "rendering_role": None,
                    "source_form": "llm_fullpage",
                    "provenance": {"page": p["page"]},
                })
        except Exception:
            pass

    payload = {"lines": lines}
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload
