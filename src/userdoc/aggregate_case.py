# src/userdoc/aggregate_case.py
from __future__ import annotations
from typing import List, Dict, Any, Tuple, Optional
from collections import defaultdict
from pathlib import Path
import json, re

from .pdf2md_userdoc import read_pages_jsonl
from .parse_provider_type import infer_provider_type_and_zip
from .shared import ZIP5_RE, parse_money, is_cpt_hcpcs
from .build_payload import build_calc_payload

CPT_HCPCS_RE = re.compile(r"^(?:[A-Za-z]\d{4}|\d{5})$")

def _key(l: Dict) -> Tuple:
    return (
        l.get("code"),
        tuple(sorted(l.get("modifiers", []) or [])),
        l.get("dos_from"),
        l.get("dos_to"),
        l.get("place_of_service") or "11",
        l.get("rendering_role") or "",
    )

def _canon_mod_list(v: Any) -> List[str]:
    out: List[str] = []
    if v is None:
        return out
    seq = v if isinstance(v, list) else [v]
    for x in seq:
        s = str(x or "").strip().upper()
        if s and re.fullmatch(r"[A-Z0-9]{2}", s) and s not in out:
            out.append(s)
    return out

def _canon_pos(v: Any) -> str:
    s = str(v or "").strip()
    if not s:
        return "11"
    if re.fullmatch(r"\d{1,2}", s):
        return s.zfill(2)
    return "11"

def _canon_units(v: Any) -> int:
    try:
        n = int(round(float(v)))
        return n if n > 0 else 1
    except Exception:
        return 1

def _canon_amount(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        try:
            return float(v)
        except Exception:
            return None
    return parse_money(str(v))

def _canon_code(v: Any) -> str:
    return str(v or "").strip().upper()

def _normalize_line(l: Dict[str, Any]) -> Dict[str, Any]:
    code = _canon_code(l.get("code"))
    return {
        "code": code,
        "modifiers": _canon_mod_list(l.get("modifiers")),
        "units": _canon_units(l.get("units", 1)),
        "dos_from": l.get("dos_from"),
        "dos_to": l.get("dos_to") or l.get("dos_from"),
        "place_of_service": _canon_pos(l.get("place_of_service")),
        "billed_amount": _canon_amount(l.get("billed_amount")),
        "rendering_role": l.get("rendering_role"),
        "source_form": l.get("source_form"),
        "provenance": l.get("provenance", {}),
    }

def aggregate_lines(nf3_lines: List[Dict], pn_lines: List[Dict]) -> List[Dict]:
    pn_units = defaultdict(int)
    for l in pn_lines:
        pn_units[_canon_code(l.get("code"))] += _canon_units(l.get("units"))

    grouped: Dict[Tuple, Dict] = {}
    for l in nf3_lines:
        k = _key(l)
        u = l.get("units")
        if u is None:
            u = pn_units.get(l.get("code")) or 1
        if k not in grouped:
            l2 = dict(l); l2["units"] = u
            grouped[k] = l2
        else:
            grouped[k]["units"] = (grouped[k].get("units") or 0) + (u or 0)

    out = list(grouped.values())

    nf3_codes = {l.get("code") for l in nf3_lines}
    for l in pn_lines:
        if l.get("code") not in nf3_codes:
            out.append({
                "code": l.get("code"),
                "modifiers": _canon_mod_list(l.get("modifiers")),
                "units": _canon_units(l.get("units")),
                "dos_from": l.get("dos_from"),
                "dos_to": l.get("dos_to") or l.get("dos_from"),
                "place_of_service": _canon_pos(l.get("place_of_service")),
                "billed_amount": _canon_amount(l.get("billed_amount")),
                "rendering_role": l.get("rendering_role"),
                "source_form": "progress_note",
                "provenance": l.get("provenance", {})
            })
    return out

def _load_json(path: Optional[Path]) -> Dict[str, Any]:
    if not path: return {"lines": [], "zips": []}
    p = Path(path)
    if not p.exists(): return {"lines": [], "zips": []}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"lines": [], "zips": []}

def aggregate_case(
    *,
    law_version_id: str,
    case_id: str,
    pages_jsonl: Path,
    nf3_json: Optional[Path] = None,
    cms1500_json: Optional[Path] = None,
    progress_json: Optional[Path] = None,
    ar1_json: Optional[Path] = None,
    edi_277ca_json: Optional[Path] = None,
    out_json: Optional[Path] = None,
    qa_json: Optional[Path] = None,
    keep_non_cpt: bool = False,
) -> Dict[str, Any]:
    """
    Loads all parsed JSONs (any may be missing), normalizes, merges, filters, and returns the calculator payload.
    Also carries AR-1 totals and 277CA events in payload['source'] for QA.
    """
    pages = read_pages_jsonl(Path(pages_jsonl))
    all_page_text = [p.get("text") or "" for p in pages]

    nf3_payload = _load_json(nf3_json)
    cms_payload = _load_json(cms1500_json)
    pn_payload  = _load_json(progress_json)
    ar1_payload = _load_json(ar1_json) if ar1_json else {}
    edi_payload = _load_json(edi_277ca_json) if edi_277ca_json else {}

    preferred_zips = []
    for pay in (nf3_payload, cms_payload, pn_payload):
        preferred_zips.extend(pay.get("zips") or [])
    if not preferred_zips:
        header = "\n".join(all_page_text[:6])
        preferred_zips = list({m.group(1) for m in ZIP5_RE.finditer(header)})

    hints = infer_provider_type_and_zip(all_page_text, preferred_zips)
    provider_type = hints["provider_type"]
    region_zip = hints["service_region_zip"]
    service_addresses = hints.get("service_addresses", [])

    cms_lines = [_normalize_line(l) for l in (cms_payload.get("lines") or [])]
    nf3_lines = [_normalize_line(l) for l in (nf3_payload.get("lines") or [])]
    pn_lines  = [_normalize_line(l) for l in (pn_payload.get("lines")  or [])]

    if cms_lines and nf3_lines:
        merged = aggregate_lines(nf3_lines + cms_lines, pn_lines)
    elif nf3_lines:
        merged = aggregate_lines(nf3_lines, pn_lines)
    elif cms_lines:
        merged = aggregate_lines(cms_lines, pn_lines)
    else:
        merged = aggregate_lines([], pn_lines)

    merged = [_normalize_line(l) for l in merged]

    if not keep_non_cpt:
        merged = [l for l in merged if is_cpt_hcpcs(l.get("code", ""))]

    case_meta = {
        "case_id": case_id,
        "extracted_via": ",".join([k for k, v in [
            ("cms1500", bool(cms_payload.get("lines"))),
            ("nf3", bool(nf3_payload.get("lines"))),
            ("progress", bool(pn_payload.get("lines")))
        ] if v]) or "unknown",
        "ar1": {
            "claimed": ar1_payload.get("claimed"),
            "paid": ar1_payload.get("paid"),
            "in_dispute": ar1_payload.get("in_dispute"),
            "itemized": ar1_payload.get("items") or [],
        },
        "edi_277ca": edi_payload.get("events") or [],
        "service_addresses": service_addresses,
    }

    payload = build_calc_payload(
        law_version_id=law_version_id,
        zip_code=region_zip or "",
        provider_type=provider_type,
        lines=merged,
        source=case_meta
    )

    if out_json:
        Path(out_json).parent.mkdir(parents=True, exist_ok=True)
        Path(out_json).write_text(json.dumps(payload, indent=2), encoding="utf-8")

    if qa_json:
        qa_blob = {
            "nf3_raw": nf3_payload.get("lines") or [],
            "cms1500_raw": cms_payload.get("lines") or [],
            "progress_raw": pn_payload.get("lines") or [],
            "ar1": ar1_payload,
            "edi_277ca": edi_payload,
            "provider_hints": {
                "provider_type": provider_type,
                "service_region_zip": region_zip,
                "service_addresses": service_addresses,
            },
            "postmerge_kept": len(merged),
        }
        Path(qa_json).write_text(json.dumps(qa_blob, indent=2), encoding="utf-8")

    return payload
