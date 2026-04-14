# "see Table/Rule" → cross‑refs
# src/ingest/crossref_extractor.py
from __future__ import annotations
import argparse, json, re, sys
from pathlib import Path
from typing import List, Dict

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

SEC_SIG = re.compile(r"§\s*([0-9]+(?:\.[0-9]+)*(?:\([a-z]\))?)")
TBL_SIG = re.compile(r"\b(?:see|per|refer to)?\s*Table\s+(\d+)\b", re.I)
MD_H = re.compile(r"^\s{0,3}(#{1,6})\s+(?P<title>.+?)\s*$")

def load_anchors(p: Path) -> List[Dict]:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return []

def current_anchor(line_no: int, anchors: List[Dict]) -> str | None:
    prev = None
    for a in anchors:
        if int(a.get("line", 0)) <= line_no:
            prev = a.get("id")
        else:
            break
    return prev

def find_clause_anchor_by_token(token: str, anchors: List[Dict]) -> str | None:
    t = token.lower()
    for a in anchors:
        if t in a.get("title", "").lower():
            return a.get("id")
    return None

def extract_crossrefs(md_path: Path, anchors_path: Path, out_path: Path) -> None:
    anchors = load_anchors(anchors_path)
    anchors = sorted(anchors, key=lambda x: int(x.get("line", 0)))

    lines = md_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    refs: List[Dict] = []

    for i, ln in enumerate(lines, 1):
        src = current_anchor(i, anchors)
        if not src:
            continue

        # Table refs
        for m in TBL_SIG.finditer(ln):
            tbl_id = f"tbl-{m.group(1)}"
            refs.append({
                "source_anchor": src,
                "target_type": "Table",
                "target_id": tbl_id,
                "phrase": m.group(0),
                "line": i
            })

        # Clause refs like §4.2(a)
        for m in SEC_SIG.finditer(ln):
            token = m.group(1)
            target = find_clause_anchor_by_token(token, anchors)
            if target:
                refs.append({
                    "source_anchor": src,
                    "target_type": "Clause",
                    "target_id": target,
                    "phrase": m.group(0),
                    "line": i
                })

    out_path.write_text(json.dumps(refs, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[xrefs] wrote {len(refs)} refs -> {out_path}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--version-dir", required=True)
    args = ap.parse_args()
    vdir = Path(args.version_dir)
    md = vdir / "derived" / "law.md"
    anchors = vdir / "derived" / "anchors.json"
    out = vdir / "derived" / "crossrefs.json"
    assert md.exists(), f"missing {md}"
    assert anchors.exists(), f"missing {anchors}"
    extract_crossrefs(md, anchors, out)

if __name__ == "__main__":
    main()
