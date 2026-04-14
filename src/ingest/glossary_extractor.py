# abbreviations/glossary with anchors
# src/ingest/glossary_extractor.py
from __future__ import annotations
import argparse, json, sys
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

def df_load(p: Path) -> pd.DataFrame:
    try:
        return pd.read_parquet(p)
    except Exception:
        raise SystemExit(f"cannot read parquet: {p}")

def build_glossary(df: pd.DataFrame) -> dict:
    glossary = {}
    for _, r in df.iterrows():
        if r.get("tname") == "glossary":
            term = (r.get("code") or r.get("description") or "").strip()
            # try to interpret description like "RVU — Relative Value Unit"
            if term and r.get("description"):
                term_up = term.split("—")[0].split("-")[0].strip()
                if term_up and term_up.isupper():
                    glossary[term_up] = r["description"].strip()
            elif term.isupper() and r.get("footnotes"):
                glossary[term] = " ".join(r["footnotes"])
    return glossary

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--version-dir", required=True)
    args = ap.parse_args()
    vdir = Path(args.version_dir)
    tables = vdir / "derived" / "tables.parquet"
    out = vdir / "derived" / "glossary.json"
    assert tables.exists(), f"missing {tables}"
    df = df_load(tables)
    glossary = build_glossary(df)
    out.write_text(json.dumps(glossary, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[glossary] wrote {len(glossary)} terms -> {out}")

if __name__ == "__main__":
    main()
