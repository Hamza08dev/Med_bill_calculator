#!/usr/bin/env python
from __future__ import annotations
import argparse, json, yaml
from pathlib import Path
from src.ingest.pdf2md import PDF2MDConfig
from src.userdoc.pdf2md_userdoc import process_user_pdf
from src.userdoc.detect_forms import detect_forms
from src.userdoc.parse_cms1500 import parse_cms1500
from src.userdoc.parse_nf3 import parse_nf3
from src.userdoc.parse_provider_type import provider_hints_from_text
from src.userdoc.aggregate_case import aggregate_case

def run_case(case_dir: Path, law_version_id: str):
    pdf = next(case_dir.glob("*.pdf"))
    derived = case_dir / "derived"
    cfg = PDF2MDConfig(ocr_enable=True, ocr_dpi=300, min_chars_no_ocr=50)
    u = process_user_pdf(case_dir.name, pdf, derived, cfg)

    forms_cfg = yaml.safe_load((Path("configs/userdoc/forms.yaml")).read_text(encoding="utf-8"))
    seg_path = derived / "segments.json"
    detect_forms(u.pages_jsonl, forms_cfg, seg_path)

    cms = parse_cms1500(u.pages_jsonl, seg_path, derived / "cms1500.lines.json")
    nf3 = parse_nf3(u.pages_jsonl, seg_path, derived / "nf3.lines.json")

    prov_cfg = yaml.safe_load(Path("configs/provider_hints.yaml").read_text(encoding="utf-8"))
    prov_hints = provider_hints_from_text(u.pages_jsonl, prov_cfg)

    can_dir = Path("law_versions") / law_version_id / "derived" / "tables" / "canonical"
    payload = aggregate_case(
        case_id=case_dir.name,
        law_version_id=law_version_id,
        service_zip_hint="",
        cms1500_lines=cms,
        nf3_lines=nf3,
        ar1_summary={},
        provider_kw_hints=prov_hints,
        canonical_dir=can_dir,
        out_path=derived / "case_extract.json",
    )
    print(json.dumps(payload, indent=2))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cases-glob", default="cases/*/*application*.pdf")
    ap.add_argument("--law-version", default="ny_2018_01")
    args = ap.parse_args()
    for pdf in Path().glob(args.cases_glob):
        run_case(pdf.parent, args.law_version)

if __name__ == "__main__":
    main()
