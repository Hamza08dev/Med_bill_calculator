# headings/clauses → anchors
# src/ingest/anchor_extractor.py
from __future__ import annotations
import argparse, json, re, sys
from pathlib import Path
from typing import List, Dict

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

SLUG_SAFE = re.compile(r"[^a-z0-9]+")
SEC_PATTERNS = [
    re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*$"),             # Markdown #, ## ...
    re.compile(r"^\s*(Section|SECTION|Article|ARTICLE)\s+.+$"),     # Word-style headings
    re.compile(r"^\s*\d+(\.\d+)+\s+.+$"),                           # 1.2.3 style
    re.compile(r"^\s*[A-Z][A-Z0-9 \-]{8,}$"),                       # ALL CAPS blocks
]

def slugify(text: str) -> str:
    s = text.lower().strip()
    s = SLUG_SAFE.sub("-", s).strip("-")
    return s or "x"

def best_page_for_title(title: str, toc: List[Dict]) -> int | None:
    t = title.lower().strip()
    candidates = [(abs(len(t) - len(x.get("title",""))), x) for x in toc if t in x.get("title","").lower()]
    if candidates:
        return int(sorted(candidates, key=lambda z: z[0])[0][1].get("page"))
    return None

def extract_anchors(md_path: Path, toc_path: Path, out_path: Path) -> None:
    md = md_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    toc = []
    if toc_path.exists():
        try:
            toc = json.loads(toc_path.read_text(encoding="utf-8"))
        except Exception:
            toc = []

    anchors = []
    seen = set()
    for i, line in enumerate(md, 1):
        title = None
        # markdown heading
        m = re.match(r"^\s{0,3}(#{1,6})\s+(?P<title>.+?)\s*$", line)
        if m:
            title = m.group("title").strip()
        else:
            for pat in SEC_PATTERNS[1:]:
                if pat.match(line.strip()):
                    title = line.strip()
                    break
        if not title:
            continue

        base_slug = slugify(title)[:64]
        anchor_id = f"sec-{base_slug}"
        # ensure unique
        k = 2
        while anchor_id in seen:
            anchor_id = f"{anchor_id}-{k}"
            k += 1
        seen.add(anchor_id)

        page = best_page_for_title(title, toc)
        anchors.append({
            "id": anchor_id,
            "title": title,
            "type": "Clause",
            "line": i,
            "page": page,
            "path": title  # simple path for now
        })

    out_path.write_text(json.dumps(anchors, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[anchors] wrote {len(anchors)} anchors -> {out_path}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--version-dir", required=True)
    args = ap.parse_args()
    vdir = Path(args.version_dir)
    md = vdir / "derived" / "law.md"
    toc = vdir / "derived" / f"{vdir.name}.toc.json"
    out = vdir / "derived" / "anchors.json"
    assert md.exists(), f"missing {md}"
    extract_anchors(md, toc, out)

if __name__ == "__main__":
    main()
