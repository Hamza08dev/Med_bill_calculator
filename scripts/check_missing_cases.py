#!/usr/bin/env python3
import re
import sys
from pathlib import Path
from typing import Set, Tuple, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CASE_EXTRACTS_DIR = PROJECT_ROOT / "case_extracts"
FINAL_CASES_DIR = PROJECT_ROOT / "final_cases"


def extract_id_from_case_extract_name(path: Path) -> Optional[str]:
    # Expect names like: case_extract_1369_0325.json -> 13690325
    stem = path.stem  # e.g., case_extract_1369_0325
    parts = stem.split("_")
    if len(parts) >= 3 and parts[-2].isdigit() and parts[-1].isdigit():
        return f"{parts[-2]}{parts[-1]}"
    # Fallback: try last two 4-digit groups anywhere
    m = re.search(r"(\d{4})[\-_](\d{4})", path.name)
    if m:
        return f"{m.group(1)}{m.group(2)}"
    # Fallback: last 8 consecutive digits
    m2 = re.search(r"(\d{8})(?!.*\d)", path.name)
    return m2.group(1) if m2 else None


def extract_id_from_final_name(path: Path) -> Optional[str]:
    name = path.name
    # Prefer two 4-digit group pattern first (e.g., *_1369-0325.* or *_1369_0325.*)
    m = re.search(r"(\d{4})[\-_](\d{4})(?!.*(\d{4})[\-_](\d{4}))", name)
    if m:
        return f"{m.group(1)}{m.group(2)}"
    # Then, take the last 8 consecutive digits in the filename
    m2 = re.search(r"(\d{8})(?!.*\d)", name)
    if m2:
        return m2.group(1)
    # If there's a longer digit run at the end, use its last 8
    m3 = re.search(r"(\d{9,})(?!.*\d)", name)
    if m3:
        digits = m3.group(1)
        return digits[-8:]
    return None


def collect_ids() -> Tuple[Set[str], Set[str]]:
    extract_ids: Set[str] = set()
    final_ids: Set[str] = set()

    if CASE_EXTRACTS_DIR.exists():
        for p in sorted(CASE_EXTRACTS_DIR.glob("case_extract_*.json")):
            cid = extract_id_from_case_extract_name(p)
            if cid:
                extract_ids.add(cid)

    if FINAL_CASES_DIR.exists():
        for p in FINAL_CASES_DIR.rglob("*.pdf"):
            fid = extract_id_from_final_name(p)
            if fid:
                final_ids.add(fid)

    return extract_ids, final_ids


def main() -> int:
    extract_ids, final_ids = collect_ids()

    missing_in_finals = sorted(extract_ids - final_ids)
    missing_in_extracts = sorted(final_ids - extract_ids)
    matching_ids = sorted(extract_ids & final_ids)

    print("Scan summary:")
    print(f"- case_extracts found IDs: {len(extract_ids)}")
    print(f"- final_cases   found IDs: {len(final_ids)}")
    print(f"- matching IDs            : {len(matching_ids)}")
    print()

    if extract_ids == final_ids:
        print("All IDs match 1:1 between case_extracts and final_cases.")
        return 0

    if missing_in_finals:
        print("IDs present in case_extracts but MISSING in final_cases:")
        for mid in missing_in_finals:
            print(mid)
    else:
        print("No IDs missing in final_cases.")

    print()

    if missing_in_extracts:
        print("IDs present in final_cases but MISSING in case_extracts:")
        for mid in missing_in_extracts:
            print(mid)
    else:
        print("No IDs missing in case_extracts.")

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
