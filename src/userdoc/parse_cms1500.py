# src/userdoc/parse_cms1500.py
from __future__ import annotations
from pathlib import Path
from typing import Dict, Any, List
import json

from .pdf2md_userdoc import read_pages_jsonl
from .shared import (
    CPT_RE, DATE_ANY_RE, parse_money, normalize_date, guess_pos_default, load_yaml_from_configs,
    is_cpt_hcpcs, looks_like_icd10
)
from .normalize_modifiers import normalize_modifiers, extract_role_units
from src.llm.client import llm_complete

PROMPT = """You are an expert medical billing analyst.
From the page text below (CMS-1500 or any claim-like layout), extract ONLY CPT/HCPCS claim line items.
- CPT: exactly 5 digits (e.g., 97110)
- HCPCS: 1 letter + 4 digits (e.g., J1100). DO NOT return ICD-10 (e.g., M54.16, S13.4XXA).
- Return billed_amount when present; use null only if truly not readable.

Return ONLY a JSON array of objects:
- code, modifiers (0-4 items), units (int), dos_from, dos_to, place_of_service, billed_amount.

Page Text:
"""

def _llm_extract(text: str, model_cfg: dict, page_no: int) -> List[Dict[str, Any]]:
    if not model_cfg.get("enabled", False): return []
    try:
        raw = llm_complete(model_cfg, PROMPT + (text or ""))
        s = raw[raw.find("["): raw.rfind("]")+1]
        arr = json.loads(s)
        out = []
        for l in arr:
            code = str(l.get("code","")).upper()
            if looks_like_icd10(code) or not is_cpt_hcpcs(code):
                continue
            out.append({
                "code": code,
                "modifiers": [str(m).upper() for m in (l.get("modifiers") or [])],
                "units": int(round(float(l.get("units", 1)))),
                "dos_from": l.get("dos_from"),
                "dos_to": l.get("dos_to") or l.get("dos_from"),
                "place_of_service": str(l.get("place_of_service") or "11"),
                "billed_amount": float(l["billed_amount"]) if l.get("billed_amount") is not None else None,
                "rendering_role": None,
                "source_form": "cms1500_llm",
                "provenance": {"page": page_no},
            })
        return out
    except Exception:
        return []

def _regex_extract(text: str, alias_map: dict, page_no: int) -> List[Dict[str, Any]]:
    out = []
    for ln in (text or "").splitlines():
        mc = CPT_RE.search(ln)
        if not mc: 
            continue
        code = mc.group(1).upper()
        if looks_like_icd10(code) or not is_cpt_hcpcs(code):
            continue
        amt  = parse_money(ln)
        mods = normalize_modifiers(ln, alias_map)
        role, units = extract_role_units(ln)
        md = DATE_ANY_RE.search(ln)
        d0 = normalize_date(md.group(1)) if md else None
        out.append({
            "code": code, "modifiers": mods, "units": int(round(units)) if units else 1,
            "dos_from": d0, "dos_to": d0, "place_of_service": guess_pos_default(),
            "billed_amount": amt, "rendering_role": role, "source_form": "cms1500_regex",
            "provenance": {"page": page_no},
        })
    return out

def parse_cms1500(pages_jsonl: Path, segments_json: Path, out_json: Path) -> Dict[str, Any]:
    pages = read_pages_jsonl(pages_jsonl)
    segs  = json.loads(Path(segments_json).read_text(encoding="utf-8"))
    model_cfg = load_yaml_from_configs("models.yaml").get("userdoc_extractor", {})
    alias_map = load_yaml_from_configs("provider_hints.yaml").get("modifier_aliases", {})

    labeled_targets = set()
    for sp in segs.get("spans", []):
        if sp["type"] == "cms1500":
            labeled_targets.update(range(sp["start"], sp["end"]+1))

    lines: List[Dict[str, Any]] = []
    # pass 1: labeled pages
    for p in pages:
        if p["page"] not in labeled_targets:
            continue
        txt = p.get("text") or ""
        lines.extend(_llm_extract(txt, model_cfg, p["page"]))
        lines.extend(_regex_extract(txt, alias_map, p["page"]))

    # salvage pass: if nothing found, scan ALL pages (helps 0425/4469/3473)
    if not lines:
        for p in pages:
            txt = p.get("text") or ""
            if not txt.strip(): 
                continue
            lines.extend(_llm_extract(txt, model_cfg, p["page"]))
            lines.extend(_regex_extract(txt, alias_map, p["page"]))

    payload = {"lines": lines}
    Path(out_json).write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload
