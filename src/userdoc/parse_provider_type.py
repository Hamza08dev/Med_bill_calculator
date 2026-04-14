# src/userdoc/parse_provider_type.py
from __future__ import annotations
from typing import List, Dict, Any, Tuple
from pathlib import Path
import re, yaml

from .shared import detect_provider_type, ZIP5_RE

# optional LLM classifier (only used if configs/models.yaml -> userdoc_extractor.enabled = true)
try:
    from src.llm.client import llm_complete
except Exception:
    llm_complete = None  # graceful fallback

def _load_patterns(cfg_path: Path) -> Dict[str, List[str]]:
    """
    Load provider hint patterns. Structure expected (but optional):
      chiropractic: ["chiropractic", "d.c.", ...]
      podiatry: ["podiatr", "d.p.m.", ...]
      behavioural: ["psychology", "lcsw", ...]
      medical: ["m.d.", "d.o.", "internal medicine", ...]
    """
    defaults = {
        "chiropractic": ["chiropract", " d.c.", " dc ", "doctor of chiropractic"],
        "podiatry":     ["podiatr", " d.p.m.", " dpm ", "doctor of podiatric"],
        "behavioural":  ["psychology", "psychologist", "lcsw", "behavioral", "behavioural", "psychiatry", "psychiatric"],
        "medical":      [" m.d.", " m.d ", " d.o.", "internal medicine", "family practice", "orthopedic", "pain management"],
    }
    try:
        if cfg_path.exists():
            data = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
            pats = data.get("provider_hints", {})
            for k, v in pats.items():
                if isinstance(v, list) and v:
                    defaults[k] = v
    except Exception:
        pass
    return defaults

def _score_types(text: str, patterns: Dict[str, List[str]]) -> Dict[str, int]:
    b = (text or "").lower()
    scores = {k: 0 for k in patterns.keys()}
    for k, words in patterns.items():
        for w in words:
            if w.lower() in b:
                scores[k] += 1
    return scores

def _winner(scores: Dict[str, int]) -> Tuple[str, int, int]:
    sorted_kv = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
    top, topv = sorted_kv[0]
    secondv = sorted_kv[1][1] if len(sorted_kv) > 1 else 0
    return top, topv, secondv

def _llm_provider_guess(text: str, models_cfg_path: Path) -> str | None:
    if llm_complete is None or not models_cfg_path.exists():
        return None
    try:
        models_cfg = yaml.safe_load(models_cfg_path.read_text(encoding="utf-8")) or {}
        mcfg = models_cfg.get("userdoc_extractor", {})
        if not mcfg.get("enabled", False):
            return None
        prompt = (
            "Classify the provider type mentioned in the text as exactly one of: "
            "'medical', 'chiropractic', 'podiatry', 'behavioural'. "
            "Return ONLY the single word.\n\n"
            f"Text:\n{text[:3500]}"
        )
        out = (llm_complete(mcfg, prompt) or "").strip().lower()
        out = re.sub(r"[^a-z]", "", out)
        return out if out in {"medical","chiropractic","podiatry","behavioural"} else None
    except Exception:
        return None

def provider_hints_from_text(pages: List[str], cfg_path: str = "configs/provider_hints.yaml") -> Dict[str, Any]:
    """
    Inspect early pages + entire doc for provider type signals and zips.
    Returns:
      {
        "votes": {"medical": n, "chiropractic": n, "podiatry": n, "behavioural": n},
        "zips": ["11556", ...],
        "provider_type_guess": "medical"
      }
    """
    patterns = _load_patterns(Path(cfg_path))
    header_blob = "\n".join(pages[:6])
    full_blob = "\n".join(pages)

    # scores from header and full doc
    s_header = _score_types(header_blob, patterns)
    s_full   = _score_types(full_blob, patterns)
    votes = {k: s_header.get(k,0) + s_full.get(k,0) for k in patterns.keys()}

    # heuristic winner
    guess, topv, secondv = _winner(votes)
    # if close/weak, try LLM fallback (optional)
    if topv <= 1 or (topv - secondv) <= 0:
        llm_guess = _llm_provider_guess(header_blob or full_blob, Path("configs/models.yaml"))
        if llm_guess:
            guess = llm_guess

    # collect possible zips
    zips = list({m.group(1) for m in ZIP5_RE.finditer(header_blob)}) or \
           list({m.group(1) for m in ZIP5_RE.finditer(full_blob)})

    # final tidy
    if not guess:
        guess = detect_provider_type(header_blob)

    return {
        "votes": votes,
        "zips": zips,
        "provider_type_guess": guess or "medical",
    }

def infer_provider_type_and_zip(pages: List[str], preferred_zips: List[str]) -> Dict[str, Any]:
    """
    Small adapter used by the aggregation stage.
    - prefer preferred_zips (from NF-3/CMS-1500 lines)
    - else pick a ZIP seen near the header
    """
    hints = provider_hints_from_text(pages)
    ptype = hints["provider_type_guess"]
    zips = preferred_zips[:] if preferred_zips else hints["zips"]
    return {
        "provider_type": ptype,
        "service_region_zip": zips[0] if zips else None,
        "service_addresses": [{"zip": z, "text": ""} for z in zips],
        "votes": hints["votes"],
    }
