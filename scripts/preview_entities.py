# scripts/preview_entities.py
from __future__ import annotations
import argparse, json
from pathlib import Path
import pandas as pd

def main():
    ap = argparse.ArgumentParser(description="Summarize Phase-1 entities")
    ap.add_argument("--version-dir", required=True)
    args = ap.parse_args()
    vdir = Path(args.version_dir); ddir = vdir / "derived"

    pq = ddir / "tables.parquet"
    if pq.exists():
        df = pd.read_parquet(pq)
        print("\n== tables.parquet breakdown ==")
        print(df.groupby("tname").size().reset_index(name="rows").sort_values("rows", ascending=False).to_string(index=False))
        for t in ["conversion_factors","zip_regions","zip_regions_numbered","specialty_classifications",
                  "codes_new","codes_changed_values","codes_changed_desc","codes_deleted","fud_defs"]:
            sub = df[df["tname"]==t].head(12)
            if not sub.empty:
                print(f"\n== sample: {t} ==")
                print(sub.to_string(index=False))
    else:
        print(f"missing {pq}")

    for name in ["glossary.json","modifiers.json","icons.json","anchors.json","crossrefs.json","rules.base.json","rules.pc_tc.json"]:
        p = ddir / name
        if p.exists():
            print(f"\n== {name} ==")
            try:
                js = json.loads(p.read_text(encoding="utf-8"))
                if isinstance(js, list):
                    print(f"{len(js)} items; sample:\n{json.dumps(js[:2], indent=2)}")
                else:
                    print(json.dumps(js, indent=2)[:800])
            except Exception:
                print(p.read_text(encoding="utf-8")[:800])

if __name__ == "__main__":
    main()
