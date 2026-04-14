from __future__ import annotations
from pathlib import Path
from typing import List, Optional, Dict, Any
import json, requests, re
from src.utils.slugify import slugify

def load_yaml(p: Path) -> dict:
    import yaml
    return yaml.safe_load(p.read_text(encoding="utf-8"))

def load_model_cfg(models_yaml: Path) -> Dict[str, Any]:
    data = load_yaml(models_yaml)
    return data.get("models", {}).get("table_namer", {})

def llm_slug_or_canonical(headers: List[str], sample_rows: List[List[str]], headings: List[str],
                          allowed_canonical: List[str], model_cfg: Dict[str, Any]) -> Optional[str]:
    if not (model_cfg and model_cfg.get("enabled")):
        return None
    if model_cfg.get("provider") != "ollama":
        return None
    endpoint = model_cfg.get("endpoint", "http://localhost:11434/api/generate")
    model = model_cfg.get("model", "llama3.2")
    rules = (
        "Task: Given a Markdown table's HEADERS/ROWS and nearby HEADINGS, "
        "return either (a) one exact filename from ALLOWED (if it fits), or "
        "(b) a short slug (kebab case) describing the table. Return a single token only."
    )
    prompt = f"""{rules}

ALLOWED:
{json.dumps(allowed_canonical, indent=2)}

HEADINGS:
{json.dumps(headings)}

HEADERS:
{json.dumps(headers)}

SAMPLE ROWS (up to 3):
{json.dumps(sample_rows[:3], ensure_ascii=False)}
"""
    try:
        r = requests.post(endpoint, json={"model": model, "prompt": prompt, "stream": False, "options": {"temperature": 0.0}}, timeout=30)
        r.raise_for_status()
        out = (r.json().get("response") or "").strip().splitlines()[-1].strip()
        return out
    except Exception:
        return None

def dynamic_name(headers: List[str], rows: List[List[str]], headings: List[str],
                 canonical_candidate: Optional[str], allowed_canonical: List[str],
                 model_cfg: Dict[str, Any], fallback_idx: int) -> (str, str):
    """
    Returns (raw_filename, canonical_filename_or_empty)
    raw_filename is always something; canonical may be empty if unknown.
    """
    # If we already know the canonical file (e.g., recognized by heuristics), keep it
    canonical = canonical_candidate if (canonical_candidate in allowed_canonical) else ""

    # Ask LLM for either a canonical or a slug
    vote = llm_slug_or_canonical(headers, rows, headings, allowed_canonical, model_cfg)
    if vote:
        if vote in allowed_canonical and not canonical:
            canonical = vote
        elif not canonical:
            slug = slugify(vote, allow_dot=False)
            raw_name = f"table_{fallback_idx:03d}__{slug}.csv"
            return raw_name, canonical

    # Default raw filename using last heading
    tail = headings[-1] if headings else "table"
    slug = slugify(re.sub(r"[^a-z0-9\-]+", "-", tail.lower()), allow_dot=False)
    raw_name = f"table_{fallback_idx:03d}__{slug}.csv"
    return raw_name, canonical
