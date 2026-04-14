# src/userdoc/parse_nf3.py
from __future__ import annotations
from pathlib import Path
from typing import Dict, Any, List
import json

from .pdf2md_userdoc import read_pages_jsonl
from .shared import CPT_RE, ZIP5_RE, DATE_ANY_RE, parse_money, normalize_date, guess_pos_default, load_yaml_from_configs, is_cpt_hcpcs, looks_like_icd10
from .normalize_modifiers import normalize_modifiers, extract_role_units
from src.llm.client import llm_complete

PROMPT = """You are an expert medical billing analyst.
From the page text below (NF-3 or any claim-like layout), extract ONLY CPT/HCPCS claim line items.
- CPT: 5 digits; HCPCS: 1 letter + 4 digits. DO NOT return ICD-10 (e.g., M54.16, S13.4XXA).
- Include billed_amount when present; null only if truly unreadable.

Return ONLY a JSON array of objects: code, modifiers, units, dos_from, dos_to, place_of_service, billed_amount.

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
                "source_form": "nf3_llm",
                "provenance": {"page": page_no},
            })
        return out
    except Exception:
        return []

def _light_regex(text: str, alias_map: dict, page_no: int) -> List[Dict[str, Any]]:
    out = []
    for ln in (text or "").splitlines():
        if "$" not in ln and not CPT_RE.search(ln): 
            continue
        m = CPT_RE.search(ln)
        if not m: 
            continue
        code = m.group(1).upper()
        if looks_like_icd10(code) or not is_cpt_hcpcs(code):
            continue
        amt  = parse_money(ln)
        mods = normalize_modifiers(ln, alias_map)
        role, units = extract_role_units(ln)
        d = DATE_ANY_RE.search(ln)
        d0 = normalize_date(d.group(1)) if d else None
        out.append({
            "code": code, "modifiers": mods, "units": int(round(units)) if units else 1,
            "dos_from": d0, "dos_to": d0, "place_of_service": guess_pos_default(),
            "billed_amount": amt, "rendering_role": role, "source_form": "nf3_regex",
            "provenance": {"page": page_no},
        })
    return out

def parse_nf3(pages_jsonl: Path, segments_json: Path, out_json: Path) -> Dict[str, Any]:
    pages = read_pages_jsonl(pages_jsonl)
    model_cfg = load_yaml_from_configs("models.yaml").get("userdoc_extractor", {})
    alias_map = load_yaml_from_configs("provider_hints.yaml").get("modifier_aliases", {})

    segs = json.loads(Path(segments_json).read_text(encoding="utf-8"))
    targets = set()
    for sp in segs.get("spans", []):
        if sp["type"] == "nf3":
            targets.update(range(sp["start"], sp["end"]+1))

    lines: List[Dict[str, Any]] = []
    zips:  List[str] = []
    # pass 1: labeled NF-3
    for p in pages:
        if p["page"] not in targets:
            continue
        txt = p.get("text") or ""
        zips.extend([m.group(1) for m in ZIP5_RE.finditer(txt)])
        lines.extend(_llm_extract(txt, model_cfg, p["page"]))
        lines.extend(_light_regex(txt, alias_map, p["page"]))

    # salvage over ALL pages if empty
    if not lines:
        for p in pages:
            txt = p.get("text") or ""
            zips.extend([m.group(1) for m in ZIP5_RE.finditer(txt)])
            lines.extend(_llm_extract(txt, model_cfg, p["page"]))
            lines.extend(_light_regex(txt, alias_map, p["page"]))

    payload = {"lines": lines, "zips": sorted(set(zips))}
    Path(out_json).write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload
