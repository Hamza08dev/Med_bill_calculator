from __future__ import annotations
from pathlib import Path
from typing import Dict, List
import csv, json, re

from src.ingest.table_namer import load_yaml

def _norm_header(h: str) -> str:
    return re.sub(r"\s+", " ", h.strip().lower())

def _map_headers(headers: List[str], header_map: dict) -> Dict[str, int]:
    norm = [_norm_header(h) for h in headers]
    out: Dict[str, int] = {}
    for key, aliases in header_map["code_header_map"].items():
        lowered_aliases = [a.lower() for a in aliases]
        for i, nh in enumerate(norm):
            if nh == key or nh in lowered_aliases:
                out[key] = i
                break
    return out

def _read_csv(path: Path) -> (List[str], List[List[str]]):
    with path.open("r", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    if not rows:
        return [], []
    return rows[0], rows[1:]

def _detect_flags(texts: List[str]) -> Dict[str, bool]:
    blob = " | ".join([t for t in texts if t]).strip()
    # Note: These are heuristic, but match the “Icons” legend in NY FS.
    return {
        "is_addon": ("+" in blob),                 # Add-on service
        "is_mod51_exempt": (("*" in blob) or (" B " in f" {blob} ")),  # star or ' B ' marker
        "is_state_specific": ("∞" in blob),
        "is_altered_cpt": ("®" in blob),
    }

def _clean_rvu(rvu_cell: str) -> (str, bool):
    if rvu_cell is None:
        return "", False
    s = rvu_cell.strip().upper()
    if s == "BR":
        return "", True
    # keep as original string; let downstream parse to Decimal
    return rvu_cell.strip(), False

def canonicalize(
    law_version_id: str,
    version_dir: Path,
    configs_dir: Path = Path("configs"),
) -> Dict[str, any]:
    version_dir = Path(version_dir)
    raw_dir = version_dir / "derived" / "tables" / "raw"
    can_dir = version_dir / "derived" / "tables" / "canonical"
    can_dir.mkdir(parents=True, exist_ok=True)

    sections_cfg = load_yaml(configs_dir / "sections.yaml")
    providers_cfg = load_yaml(configs_dir / "providers.yaml")
    header_map = load_yaml(configs_dir / "header_map.yaml")

    # Build target canonical names
    target_names = set()
    for sec in sections_cfg["sections"].keys():
        for prov in providers_cfg["providers"].keys():
            n = f"{sec}_{prov}.csv".replace("em_", "eM_").replace("physicalmedicine_", "PhysicalMedicine_")
            target_names.add(n)
    target_names |= {"Section_conversion.csv", "Zip_regions.csv", "behavioural.csv"}

    # Copy the two special files if present (raw → canonical as-is)
    for s in ["Section_conversion.csv", "Zip_regions.csv"]:
        p = raw_dir / s
        if p.exists():
            headers, rows = _read_csv(p)
            with (can_dir / s).open("w", encoding="utf-8", newline="") as f:
                w = csv.writer(f)
                if headers: w.writerow(headers)
                for r in rows: w.writerow(r)

    # Load discovery index from export phase
    index_path = raw_dir / "tables_index.json"
    if not index_path.exists():
        raise FileNotFoundError(f"{index_path} not found. Run export_md_tables_to_csv first.")
    index = json.loads(index_path.read_text(encoding="utf-8"))

    buckets: Dict[str, Dict[str, any]] = {}

    def ensure_bucket(name: str):
        if name not in buckets:
            # Add **flag columns** that calculator/rules can use
            buckets[name] = {
                "headers": ["code", "description", "rvu", "fud", "pctc_split",
                            "is_addon", "is_mod51_exempt", "is_state_specific", "is_altered_cpt", "is_br"],
                "rows": []
            }

    for item in index["items"]:
        cname = item.get("canonical_filename") or ""
        if not cname or cname in ["Section_conversion.csv", "Zip_regions.csv"]:
            continue
        src = raw_dir / item["raw_filename"]
        if not src.exists():
            continue

        # Only collect known canonical targets
        if cname not in target_names:
            # keep behavioral as flat file if detected
            if cname != "behavioural.csv":
                continue

        headers, rows = _read_csv(src)
        if not headers:
            continue
        mapped = _map_headers(headers, header_map)
        ensure_bucket(cname)

        for r in rows:
            code = r[mapped["code"]] if "code" in mapped else ""
            desc = r[mapped["description"]] if "description" in mapped else ""
            rvu_raw = r[mapped["rvu"]] if "rvu" in mapped else ""
            fud = r[mapped["fud"]] if "fud" in mapped else ""
            pctc = r[mapped["pctc_split"]] if "pctc_split" in mapped else ""
            sym_cell = r[mapped["symbols"]] if "symbols" in mapped else ""

            rvu, is_br = _clean_rvu(rvu_raw)
            flags = _detect_flags([sym_cell, desc])

            buckets[cname]["rows"].append([
                code, desc, rvu, fud, pctc,
                flags["is_addon"], flags["is_mod51_exempt"], flags["is_state_specific"], flags["is_altered_cpt"], is_br
            ])

    # write out
    for name, payload in buckets.items():
        with (can_dir / name).open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(payload["headers"])
            w.writerows(payload["rows"])

    manifest = {
        "law_version_id": law_version_id,
        "canonical_dir": str(can_dir),
        "files": sorted([p.name for p in can_dir.glob("*.csv")]),
    }
    (can_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest
