from __future__ import annotations
from pathlib import Path
from typing import Dict, List
import csv, json, re

from src.ingest.md_table_extractor import extract_md_tables
from src.ingest.table_namer import load_yaml, load_model_cfg, dynamic_name

def _write_csv(path: Path, headers: List[str], rows: List[List[str]]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        w.writerow(headers)
        for r in rows:
            w.writerow(r)

def _is_conversion_table(headings: List[str]) -> bool:
    return any(re.search(r"conversion\s+factors?", h, re.I) for h in headings)

def _is_zip_ranges_table(headers: List[str], headings: List[str]) -> bool:
    hs = [h.strip().lower() for h in headers]
    looks = ("from" in hs and "thru" in hs) or ("from" in hs and "through" in hs)
    return looks or any(re.search(r"\bregion\s+(i{1,3}|iv)\b", h, re.I) for h in headings)

def export_md_tables_to_csv(
    law_version_id: str,
    version_dir: Path,
    configs_dir: Path = Path("configs"),
) -> Dict[str, any]:
    version_dir = Path(version_dir)
    md_path = version_dir / "derived" / "law.md"

    sections_cfg = load_yaml(configs_dir / "sections.yaml")
    providers_cfg = load_yaml(configs_dir / "providers.yaml")
    header_map = load_yaml(configs_dir / "header_map.yaml")
    model_cfg = load_model_cfg(configs_dir / "models.yaml")

    raw_dir = version_dir / "derived" / "tables" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    tables = extract_md_tables(md_path, sections_cfg, providers_cfg)

    allowed_canonical = []
    # Build allowed canonical names from sections × providers (+ special tables)
    for sec in sections_cfg["sections"].keys():
        for prov in providers_cfg["providers"].keys():
            allowed_canonical.append(f"{sec}_{prov}.csv")
    # fix casing for eM and PhysicalMedicine
    allowed_canonical = [n.replace("em_", "eM_").replace("physicalmedicine_", "PhysicalMedicine_") for n in allowed_canonical]
    allowed_canonical += ["Section_conversion.csv", "Zip_regions.csv", "behavioural.csv"]

    # aggregation buckets (for special merges)
    merge_zip: List[List[str]] = []   # rows: [From, Thru, Region]
    zip_header = ["From", "Thru", "Region"]

    index: List[Dict] = []
    for t in tables:
        headings_joined = t.heading_path
        # Heuristic canonical (specials)
        canonical_hint = ""
        if _is_conversion_table(headings_joined):
            canonical_hint = "Section_conversion.csv"
        elif _is_zip_ranges_table(t.headers, headings_joined):
            canonical_hint = "Zip_regions.csv"
        elif t.section_hint:
            prov = t.provider_hint or "medical"
            if prov == "behavioural":
                canonical_hint = "behavioural.csv"
            else:
                name = f"{t.section_hint}_{prov}.csv"
                name = name.replace("em_", "eM_").replace("physicalmedicine_", "PhysicalMedicine_")
                canonical_hint = name

        raw_name, canonical = dynamic_name(
            t.headers, t.rows, t.heading_path, canonical_hint, allowed_canonical, model_cfg, t.idx
        )

        # special handling merges
        if canonical == "Zip_regions.csv":
            # normalize From/Thru + Region
            hs = [h.strip() for h in t.headers]
            # find first two number-like columns (fallback)
            if "From" in hs and "Thru" in hs:
                i_from, i_thru = hs.index("From"), hs.index("Thru")
            else:
                i_from, i_thru = 0, 1
            reg = t.region_hint or ""
            for r in t.rows:
                f = r[i_from] if len(r) > i_from else ""
                th = r[i_thru] if len(r) > i_thru else ""
                if f or th:
                    merge_zip.append([f, th, reg])
        elif canonical == "Section_conversion.csv":
            _write_csv(raw_dir / canonical, t.headers, t.rows)  # keep raw one too
        else:
            _write_csv(raw_dir / raw_name, t.headers, t.rows)

        index.append({
            "idx": t.idx,
            "raw_filename": canonical if canonical in ["Section_conversion.csv"] else raw_name,
            "canonical_filename": canonical,
            "start_line": t.start_line,
            "end_line": t.end_line,
            "headers": t.headers,
            "n_rows": len(t.rows),
            "heading_path": t.heading_path,
            "section_hint": t.section_hint,
            "provider_hint": t.provider_hint,
            "region_hint": t.region_hint,
        })

    # flush zip aggregate
    if merge_zip:
        _write_csv(raw_dir / "Zip_regions.csv", zip_header, merge_zip)

    # write index
    (raw_dir / "tables_index.json").write_text(json.dumps({
        "law_version_id": law_version_id,
        "raw_dir": str(raw_dir),
        "count": len(index),
        "items": index,
    }, indent=2), encoding="utf-8")

    return {"raw_dir": str(raw_dir), "count": len(index)}
