# src/userdoc/normalize_modifiers.py
from __future__ import annotations
import re
from typing import List, Tuple, Dict, Optional, Any

__all__ = ["normalize_modifiers", "extract_role_units"]

MOD_TOKEN_RE = re.compile(
    r"""
    (?:
        (?:^|[\s,;/\-\(\)])
        (?:M(?:OD)?\s*:?\s*)?
        (?:
            (?P<num>\d{2})
            |
            (?P<alfa>[A-Z]{2})
        )
        (?:$|[\s,;/\-\)\.])
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)

UNITS_RE = re.compile(
    r"""
    (?:
        (?:^|[^A-Za-z0-9])
        (?:
            (?:(?P<label>(?:units?|qty|quantity|unit))\s*[:=]?\s*(?P<n1>\d{1,3}(?:\.\d{1,2})?))
            |
            (?:x\s*(?P<n2>\d{1,3}(?:\.\d{1,2})?))
            |
            (?:(?P<n3>\d{1,3}(?:\.\d{1,2})?)\s*(?:u|units?)\b)
            |
            (?:\b24G\s*[:=]?\s*(?P<n4>\d{1,3}(?:\.\d{1,2})?))
        )
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)

ROLE_HINTS = {
    "PA": re.compile(r"\bphysician'?s?\s+assistant\b|\bPA\b", re.IGNORECASE),
    "NP": re.compile(r"\bnurse\s+practitioner\b|\bNP\b", re.IGNORECASE),
    "MD": re.compile(r"\b(MD|M\.D\.)\b|\bphysician\b", re.IGNORECASE),
    "DO": re.compile(r"\bDO\b|\bD\.O\.\b|\bosteopath(ic)?\b", re.IGNORECASE),
    "DC": re.compile(r"\bchiropractic\b|\bD\.?C\.?\b", re.IGNORECASE),
    "DPM": re.compile(r"\bpodiatr(y|ist)\b|\bDPM\b", re.IGNORECASE),
}

DEFAULT_ALIAS_MAP: Dict[str, str] = {
    "PC": "26",
    "TC": "TC",
    "LT": "LT", "RT": "RT",
    "BILAT": "50", "BILATERAL": "50",
    "REDUCED": "52",
    "DISTINCT": "59",
    "XE": "XE", "XP": "XP", "XS": "XS", "XU": "XU",
    "PA": "PA", "NP": "NP",
}

def _canon_two_chars(tok: str) -> Optional[str]:
    if not tok: return None
    s = str(tok).strip().upper()
    if re.fullmatch(r"\d{2}", s): return s
    if re.fullmatch(r"[A-Z]{2}", s): return s
    return None

def _build_alias_map(config_aliases: Dict[str, Any] | None) -> Dict[str, str]:
    alias: Dict[str, str] = dict(DEFAULT_ALIAS_MAP)
    if not config_aliases:
        return alias
    for canonical, variants in config_aliases.items():
        canon = _canon_two_chars(canonical) or str(canonical).upper()
        if isinstance(variants, list):
            for v in variants:
                if not v: continue
                alias[str(v).upper()] = canon
        else:
            alias[str(variants).upper()] = canon
    return alias

def normalize_modifiers(text: str, alias_map: Dict[str, Any] | None = None) -> List[str]:
    up_text = (text or "").upper()
    alias = _build_alias_map(alias_map)

    found: List[str] = []
    for m in MOD_TOKEN_RE.finditer(text or ""):
        tok = m.group("num") or m.group("alfa") or ""
        span_text = up_text[max(0, m.start()-16): m.end()+16]
        for k, canon in alias.items():
            if re.search(rf"\b{k}\b", span_text):
                can = _canon_two_chars(canon)
                if can and can not in found:
                    found.append(can)
        can = _canon_two_chars(tok)
        if can and can not in found:
            can = _canon_two_chars(alias.get(can, can)) or can
            if can not in found:
                found.append(can)

    for k, canon in alias.items():
        if re.search(rf"\b{k}\b", up_text):
            can = _canon_two_chars(canon)
            if can and can not in found:
                found.append(can)

    return found

def _extract_units(text: str) -> Optional[float]:
    if not text: return None
    best = None
    for m in UNITS_RE.finditer(text):
        n = None
        for key in ("n1", "n2", "n3", "n4"):
            if m.group(key):
                try:
                    n = float(m.group(key)); break
                except Exception:
                    n = None
        if n is not None:
            best = n
    return best

def _extract_role(text: str) -> Optional[str]:
    if not text: return None
    for role, rx in ROLE_HINTS.items():
        if rx.search(text): return role
    if re.search(r"\bPA\b", text): return "PA"
    if re.search(r"\bNP\b", text): return "NP"
    return None

def extract_role_units(text: str) -> Tuple[Optional[str], Optional[float]]:
    return _extract_role(text or ""), _extract_units(text or "")
