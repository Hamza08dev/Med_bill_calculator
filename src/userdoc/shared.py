# src/userdoc/shared.py
from __future__ import annotations
from pathlib import Path
from typing import Optional, Any
from datetime import datetime
import re, yaml

def get_repo_root() -> Path:
    return Path(__file__).resolve().parents[2]

CONFIGS_DIR = get_repo_root() / "configs"

def load_yaml_from_configs(rel_path: str, default: Any = None) -> Any:
    p = CONFIGS_DIR / rel_path
    if p.exists():
        return yaml.safe_load(p.read_text(encoding="utf-8"))
    for base in [Path.cwd(), Path.cwd().parent]:
        q = base / "configs" / rel_path
        if q.exists():
            return yaml.safe_load(q.read_text(encoding="utf-8"))
    return {} if default is None else default

# Strict CPT/HCPCS: 5 digits OR (letter in allowed set) + 4 digits.
# We EXCLUDE letters that commonly collide with ICD-10 (M, N, O, W, X, Y, Z) and dental D.
_ALLOWED_HCPCS_PREFIX = set(list("ABCEGHJKLPQRSTUV"))
CPT_RE      = re.compile(r"\b([A-Za-z][0-9]{4}|[0-9]{5})\b")
ZIP5_RE     = re.compile(r"\b(\d{5})(?:-\d{4})?\b")
MONEY_RE    = re.compile(r"\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})|[0-9]+(?:\.[0-9]{2}))")
DATE_ANY_RE = re.compile(r"\b(\d{2}[/-]\d{2}[/-]\d{2,4}|\d{4}-\d{2}-\d{2})\b")

def parse_money(s: str) -> Optional[float]:
    m = MONEY_RE.search(s or "")
    if not m: return None
    try:
        return float(m.group(1).replace(",", ""))
    except Exception:
        return None

def _normalize_two_digit_year(y: int) -> int:
    return 2000 + y if y <= 29 else 1900 + y

def normalize_date(s: str) -> Optional[str]:
    if not s: return None
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y"):
        try: return datetime.strptime(s, fmt).date().isoformat()
        except Exception: pass
    for fmt in ("%m/%d/%y", "%m-%d-%y"):
        try:
            dt = datetime.strptime(s, fmt)
            if dt.year < 100:
                dt = dt.replace(year=_normalize_two_digit_year(dt.year))
            return dt.date().isoformat()
        except Exception:
            pass
    return None

def guess_pos_default() -> str:
    return "11"

def detect_provider_type(blob: str) -> str:
    b = (blob or "").lower()
    if "chiropract" in b or " d.c." in b or " dc " in b: return "chiropractic"
    if "podiat"    in b or " d.p.m." in b or " dpm " in b:  return "podiatry"
    if any(k in b for k in ["behavioral", "behavioural", "psycho", "lcsw", "psychiatr"]):
        return "behavioural"
    return "medical"

def is_cpt_hcpcs(code: str) -> bool:
    s = (code or "").strip().upper()
    if re.fullmatch(r"\d{5}", s):          # CPT
        return True
    if re.fullmatch(r"[A-Z]\d{4}", s):     # HCPCS-like
        return s[0] in _ALLOWED_HCPCS_PREFIX
    return False

def looks_like_icd10(s: str) -> bool:
    """
    Heuristics to reject diagnosis codes often OCR'd without dots (e.g., M5416, S134XXA).
    """
    u = (s or "").upper()
    if "." in u: return True
    # patterns with trailing letter stages (A/D/S) or X fillers
    if re.fullmatch(r"[A-Z]\d{2}[A-Z0-9]{2,4}[A-Z]", u): return True
    if re.search(r"[A-Z]X{1,2}[A-Z]$", u): return True
    # high-risk initial letters (ICD buckets)
    return u[:1] in {"M", "N", "O", "R", "S", "T", "V", "W", "X", "Y", "Z"}
