from __future__ import annotations
from pathlib import Path
from typing import Optional, List, Dict, Any
from .pdf2md_userdoc import load_pdf_or_text
from .detect_forms import detect_sections
from .parse_nf3 import parse_nf3
from .parse_progress_note import parse_progress_note
from .parse_provider_type import infer_provider_type_and_zip
from .aggregate_case import aggregate_lines, build_payload

def run_userdoc_pipeline(pdf_path: Path,
                         law_version_id: str,
                         case_id: Optional[str] = None,
                         text_by_page: Optional[List[str]] = None) -> Dict[str, Any]:
    loaded = load_pdf_or_text(pdf_path, text_by_page)
    pages = loaded["pages"]
    spans = detect_sections(pages)

    nf3 = parse_nf3(pages, spans)
    pn = parse_progress_note(pages, spans)

    idn = infer_provider_type_and_zip(pages, preferred_zips=nf3.get("zips", []))
    lines = aggregate_lines(nf3_lines=nf3["lines"], pn_lines=pn["lines"])

    return build_payload(
        law_version_id=law_version_id,
        provider_type=idn["provider_type"],
        region_zip=idn["service_region_zip"],
        lines=lines,
        service_addresses=idn["service_addresses"],
        case_meta={"case_id": case_id or "", "extracted_via":"nf3|progress_note", "file": str(pdf_path)}
    )
