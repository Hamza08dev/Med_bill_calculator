# scripts/make_snapshot.py
#!/usr/bin/env python
from __future__ import annotations  # must be first executable statement

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

# Add project root to sys.path so "src.*" imports work when running as a script
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.hashing import sha256_file
from src.utils.versioning import infer_law_version_id
from src.ingest.pdf2md import PDF2MDConfig, run_pdf_to_markdown


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Phase 0: create/refresh law snapshot + digest"
    )
    ap.add_argument("--version-dir", required=True, help="e.g. law_versions/ny_2018_01")
    ap.add_argument(
        "--pdf", required=True, help="path to raw PDF under <version-dir>/raw/*.pdf"
    )
    ap.add_argument(
        "--force-rebuild",
        action="store_true",
        help="force re-extract pages/toc/md even if derived files exist",
    )
    ap.add_argument(
        "--ocr-disable", action="store_true", help="skip OCR fallback during extraction"
    )
    args = ap.parse_args()

    version_dir = Path(args.version_dir).resolve()
    raw_pdf = Path(args.pdf).resolve()

    assert version_dir.exists(), f"Version dir not found: {version_dir}"
    assert raw_pdf.exists(), f"Raw PDF not found: {raw_pdf}"
    assert raw_pdf.is_file(), "PDF must be a file"

    law_version_id = infer_law_version_id(version_dir)
    derived_dir = version_dir / "derived"
    derived_dir.mkdir(parents=True, exist_ok=True)

    # Run or reuse PDF→MD pipeline
    cfg = PDF2MDConfig(ocr_enable=not args.ocr_disable)
    outputs = run_pdf_to_markdown(
        law_version_id=law_version_id,
        pdf_path=raw_pdf,
        out_dir=derived_dir,
        cfg=cfg,
        force_rebuild=args.force_rebuild,
    )

    # Build/refresh digest.json
    digest_path = derived_dir / "digest.json"
    artifacts: dict[str, dict[str, str]] = {
        "raw_pdf": {
            "path": str(raw_pdf.relative_to(version_dir)),
            "sha256": sha256_file(raw_pdf),
        },
    }

    for label, path in {
        "law_md": outputs.markdown_path,
        "pages_jsonl": outputs.pages_jsonl_path,
        "toc_json": outputs.toc_json_path,
    }.items():
        p = Path(path)
        if p.exists():
            artifacts[label] = {
                "path": str(p.relative_to(version_dir)),
                "sha256": sha256_file(p),
            }

    digest = {
        "law_version_id": law_version_id,
        "created_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "artifacts": artifacts,
    }
    digest_path.write_text(json.dumps(digest, indent=2), encoding="utf-8")

    print(f"[OK] Snapshot ready for {law_version_id}")
    print(f" - PDF:      {raw_pdf}")
    print(f" - Markdown: {outputs.markdown_path}")
    print(f" - Pages:    {outputs.pages_jsonl_path}")
    print(f" - ToC:      {outputs.toc_json_path}")
    print(f" - Digest:   {digest_path}")


if __name__ == "__main__":
    main()
