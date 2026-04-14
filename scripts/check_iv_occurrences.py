import argparse
import os
import re
from pathlib import Path
from typing import Dict, List, Tuple

from pypdf import PdfReader


def extract_text_from_pdf(pdf_path: Path) -> str:
    """Extract concatenated text from all pages of a PDF using pypdf."""
    reader = PdfReader(str(pdf_path))
    texts: List[str] = []
    for page in reader.pages:
        try:
            texts.append(page.extract_text() or "")
        except Exception:
            # If a page fails to extract, continue; we're only doing keyword match
            texts.append("")
    return "\n".join(texts)


def count_keyword_occurrences(text: str, keyword: str) -> int:
    """Count whole-word, case-sensitive occurrences of keyword in text.

    Uses regex word boundaries to avoid matching inside larger tokens.
    """
    if not keyword:
        return 0
    pattern = rf"\b{re.escape(keyword)}\b"
    return len(re.findall(pattern, text))


def get_pages_with_keyword(pdf_path: Path, keyword: str, mode: str) -> List[int]:
    """Return 1-based page numbers that match the keyword according to mode.

    mode:
    - "page_strict": page reduced (remove all whitespace) equals exactly keyword
    - "token": page contains keyword as a standalone token (\bkeyword\b)
    """
    reader = PdfReader(str(pdf_path))
    matching_pages: List[int] = []
    token_pattern = rf"\b{re.escape(keyword)}\b"
    for i, page in enumerate(reader.pages, start=1):
        try:
            t = page.extract_text() or ""
        except Exception:
            t = ""
        if not keyword:
            continue
        if mode == "page_strict":
            # Remove all whitespace and compare exact
            reduced = re.sub(r"\s+", "", t)
            if reduced == keyword:
                matching_pages.append(i)
        else:  # token mode
            if re.search(token_pattern, t) is not None:
                matching_pages.append(i)
    return matching_pages


def scan_pdfs_for_keyword(base_dir: Path, keyword: str, limit: int | None = None) -> List[Tuple[Path, int]]:
    """Return list of (pdf_path, count) for each PDF under base_dir (non-recursive).

    If limit is provided, only the first N PDFs (sorted by name) are processed.
    """
    results: List[Tuple[Path, int]] = []
    pdfs = [e for e in sorted(base_dir.iterdir()) if e.is_file() and e.suffix.lower() == ".pdf"]
    if limit is not None:
        pdfs = pdfs[: max(0, limit)]
    for entry in pdfs:
        text = extract_text_from_pdf(entry)
        count = count_keyword_occurrences(text, keyword)
        results.append((entry, count))
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Check PDFs for occurrences of an exact keyword (case-sensitive).")
    parser.add_argument(
        "--dir",
        dest="pdf_dir",
        type=str,
        default=str(Path(__file__).resolve().parents[1] / "final_cases_final"),
        help="Directory containing PDFs (default: fee-schedule-kag/final_cases)",
    )
    parser.add_argument(
        "--keyword",
        dest="keyword",
        type=str,
        default="IV",
        help="Exact keyword to match (case-sensitive). Default: 'IV'",
    )
    parser.add_argument(
        "--min",
        dest="min_count",
        type=int,
        default=2,
        help="Minimum required occurrences. Default: 2",
    )
    parser.add_argument(
        "--report",
        dest="report_path",
        type=str,
        default="",
        help="Optional path to write a TSV report (path\tcount).",
    )
    parser.add_argument(
        "--limit",
        dest="limit",
        type=int,
        default=None,
        help="Limit number of PDFs to process (default: process all)",
    )
    parser.add_argument(
        "--pages-report",
        dest="pages_report",
        type=str,
        default=str(Path(__file__).resolve().parents[1] / "logs" / "iv_pages.txt"),
        help="Path to write a text report listing file and page numbers where the keyword occurs.",
    )
    parser.add_argument(
        "--pages-mode",
        dest="pages_mode",
        choices=["page_strict", "token"],
        default="page_strict",
        help=(
            "page_strict: page contains only the keyword (ignoring whitespace). "
            "token: page contains keyword as a standalone token."
        ),
    )

    args = parser.parse_args()

    pdf_dir = Path(args.pdf_dir)
    if not pdf_dir.exists() or not pdf_dir.is_dir():
        raise SystemExit(f"Directory not found or not a directory: {pdf_dir}")

    results = []
    pdfs = [e for e in sorted(pdf_dir.iterdir()) if e.is_file() and e.suffix.lower() == ".pdf"]
    if args.limit is not None:
        pdfs = pdfs[: max(0, args.limit)]

    # Print per-file summary and track failures
    num_pdfs = 0
    num_meet_threshold = 0
    failures: List[Tuple[Path, int]] = []

    total = len(pdfs)
    pages_report_entries: List[str] = []
    for idx, pdf_path in enumerate(pdfs, start=1):
        text = extract_text_from_pdf(pdf_path)
        pages = get_pages_with_keyword(pdf_path, args.keyword, args.pages_mode)
        # Count based on pages mode: in page_strict mode, count = number of matching pages
        if args.pages_mode == "page_strict":
            count = len(pages)
        else:
            count = count_keyword_occurrences(text, args.keyword)
        results.append((pdf_path, count))
        num_pdfs += 1
        meets = count >= args.min_count
        if meets:
            num_meet_threshold += 1
        else:
            failures.append((pdf_path, count))
        contains = "Yes" if count > 0 else "No"
        print(f"[{idx}/{total}] {pdf_path.name}\tcount={count}\tcontains_IV={contains}")
        # Prepare pages report line
        pages_str = ",".join(str(p) for p in pages) if pages else ""
        pages_report_entries.append(f"{pdf_path}\t{pages_str}")

    print("\n--- Summary ---")
    print(f"Total PDFs: {num_pdfs}")
    print(f">= {args.min_count} occurrences: {num_meet_threshold}")
    print(f"< {args.min_count} occurrences: {len(failures)}")

    if args.report_path:
        report_path = Path(args.report_path)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with open(report_path, "w", encoding="utf-8") as f:
            f.write("file\tcount\n")
            for pdf_path, count in results:
                f.write(f"{pdf_path}\t{count}\n")
        print(f"Report written to: {report_path}")

    # Always write pages report if a path is specified (defaults to logs/iv_pages.txt)
    if args.pages_report:
        pages_report_path = Path(args.pages_report)
        pages_report_path.parent.mkdir(parents=True, exist_ok=True)
        with open(pages_report_path, "w", encoding="utf-8") as f:
            f.write("file\tpages\n")
            for line in pages_report_entries:
                f.write(line + "\n")
        print(f"Pages report written to: {pages_report_path}")

    # Exit with non-zero if any failures
    if failures:
        # List failures for convenience
        print("\nFiles with fewer than required occurrences:")
        for pdf_path, count in failures:
            print(f"- {pdf_path.name}: {count}")
        raise SystemExit(2)


if __name__ == "__main__":
    main()
