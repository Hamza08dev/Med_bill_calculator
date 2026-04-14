# src/userdoc/parse_ar1.py
from __future__ import annotations
from pathlib import Path
from typing import Dict, Any, List
import json, re

from .pdf2md_userdoc import read_pages_jsonl
from .shared import load_yaml_from_configs, normalize_date, parse_money
from src.llm.client import llm_complete

MONEY = r'([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})|[0-9]+(?:\.[0-9]{2}))'

KEYS = {
    "claimed": [r"total amount claimed", r"amount billed", r"total billed", r"total claim(ed)?"],
    "paid":    [r"amount paid", r"paid to date", r"total paid"],
    "dispute": [r"amount in dispute", r"in dispute", r"disputed amount", r"claim[s]? in dispute"],
}

PROMPT = """You are reviewing an AR-1 (AAA Arbitration Request) page.
Extract:
- totals: claimed, paid, in_dispute (numbers, or null if absent)
- items: an array of {dos_from, dos_to, amount} found in any 'Claims in Dispute' or line-item table

Return ONLY JSON:
{
  "claimed": number|null, "paid": number|null, "in_dispute": number|null,
  "items":[{"dos_from": "YYYY-MM-DD|null", "dos_to":"YYYY-MM-DD|null", "amount": number|null}, ...]
}

Page Text:
"""

def _find_money(label_list, text: str):
    t = text or ""
    for lab in label_list:
        m = re.search(rf"{lab}\D+\$?\s*{MONEY}", t, re.I)
        if m:
            val = m.group(1)
            try:
                return float(val.replace(",", ""))
            except Exception:
                pass
    return None

def summarize_ar1(pages_jsonl: Path, segments_json: Path, out_path: Path) -> Dict[str, Any]:
    pages = read_pages_jsonl(pages_jsonl)
    segs  = json.loads(Path(segments_json).read_text(encoding="utf-8"))
    model_cfg = load_yaml_from_configs("models.yaml").get("userdoc_extractor", {})

    ar_pages = set()
    for sp in segs.get("spans", []):
        if sp.get("type") == "ar1":
            ar_pages.update(range(sp["start"], sp["end"]+1))

    claimed = paid = dispute = None
    items: List[Dict[str, Any]] = []

    # regex quick pass
    for p in pages:
        if p["page"] not in ar_pages: 
            continue
        text = p.get("text") or ""
        if claimed is None: claimed = _find_money(KEYS["claimed"], text)
        if paid    is None: paid    = _find_money(KEYS["paid"], text)
        if dispute is None: dispute = _find_money(KEYS["dispute"], text)

    # LLM pass per AR page to pick line items
    for p in pages:
        if p["page"] not in ar_pages: 
            continue
        txt = p.get("text") or ""
        if not txt.strip(): 
            continue
        try:
            raw = llm_complete(model_cfg, PROMPT + txt)
            s = raw[raw.find("{"): raw.rfind("}")+1]
            obj = json.loads(s)
            # merge totals if present
            if claimed is None and obj.get("claimed") is not None: claimed = float(obj["claimed"])
            if paid    is None and obj.get("paid")    is not None: paid    = float(obj["paid"])
            if dispute is None and obj.get("in_dispute") is not None: dispute = float(obj["in_dispute"])
            for it in (obj.get("items") or []):
                items.append({
                    "dos_from": normalize_date(it.get("dos_from")) if it.get("dos_from") else None,
                    "dos_to":   normalize_date(it.get("dos_to"))   if it.get("dos_to") else None,
                    "amount":   parse_money(str(it.get("amount"))) if it.get("amount") is not None else None,
                    "page":     p["page"],
                })
        except Exception:
            pass

    summary = {
        "claimed": claimed,
        "paid": paid,
        "in_dispute": dispute,
        "items": items,
        "pages": sorted(ar_pages),
        "summary": f"claimed={claimed} paid={paid} dispute={dispute}",
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary
