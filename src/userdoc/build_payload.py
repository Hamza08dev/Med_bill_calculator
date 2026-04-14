# src/userdoc/build_payload.py
from typing import List, Dict, Any
from datetime import date

def build_calc_payload(
    law_version_id: str,
    zip_code: str,
    provider_type: str,           # "medical" | "chiropractic" | "podiatry" | "behavioural"
    lines: List[Dict[str, Any]],  # each: {code, description?, units, dos_from, dos_to?, billed_amount?, modifiers?}
    source: Dict[str, Any]
) -> Dict[str, Any]:
    # ensure minimal shaping and defaults
    shaped = []
    for ln in lines:
        shaped.append({
            "code": ln.get("code", "").upper(),
            "modifiers": ln.get("modifiers", []),
            "units": ln.get("units", 1),
            "dos_from": ln.get("dos_from"),
            "dos_to": ln.get("dos_to", ln.get("dos_from")),
            "place_of_service": ln.get("place_of_service", ""),
            "billed_amount": ln.get("billed_amount", None),
        })
    return {
        "law_version_id": law_version_id,
        "service_region_zip": str(zip_code),
        "provider_type": provider_type,
        "lines": shaped,
        "source": source,
    }
