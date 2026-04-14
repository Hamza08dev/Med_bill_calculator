#!/usr/bin/env python3
"""
Clean CPT codes in case_extracts to only keep first 5 characters (digits/letters).
Removes modifiers like "-25", ",PA" etc. after the first 5 characters.
"""
import json
import re
from pathlib import Path
from typing import Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CASE_EXTRACTS_DIR = PROJECT_ROOT / "case_extracts"


def extract_cpt_code(raw_code: str) -> str:
    """Extract only the first 5 characters (digits or letters) from a CPT code."""
    if not raw_code:
        return ""
    
    # Remove whitespace and take first 5 characters
    cleaned = raw_code.strip()
    if len(cleaned) < 5:
        return cleaned
    
    # Take first 5 characters (which should be the CPT code)
    cpt = cleaned[:5]
    
    # If there's a space after 5th character, we've confirmed it's a modifier pattern
    if len(cleaned) > 5 and cleaned[5] == ' ':
        return cpt
    
    # Otherwise just return first 5 chars
    return cpt


def clean_case_extract(file_path: Path) -> bool:
    """Clean CPT codes in a single case_extract file. Returns True if changes were made."""
    try:
        with file_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        
        has_changes = False
        lines = data.get("lines", [])
        
        for line in lines:
            original_code = line.get("code", "")
            cleaned_code = extract_cpt_code(original_code)
            
            if original_code != cleaned_code:
                line["code"] = cleaned_code
                has_changes = True
        
        if has_changes:
            # Write back the cleaned data
            with file_path.open("w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        
        return has_changes
    
    except Exception as e:
        print(f"[error] Failed to process {file_path.name}: {e}")
        return False


def main():
    print("[clean] Starting CPT code cleaning in case_extracts...")
    
    if not CASE_EXTRACTS_DIR.exists():
        print(f"[clean][error] Directory not found: {CASE_EXTRACTS_DIR}")
        return 1
    
    case_files = sorted(CASE_EXTRACTS_DIR.glob("case_extract_*.json"))
    if not case_files:
        print(f"[clean] No case_extract_*.json files found in {CASE_EXTRACTS_DIR}")
        return 0
    
    print(f"[clean] Found {len(case_files)} case extract files")
    
    cleaned_count = 0
    
    for case_file in case_files:
        if clean_case_extract(case_file):
            print(f"[clean] Cleaned: {case_file.name}")
            cleaned_count += 1
    
    print(f"[clean] Done. Cleaned {cleaned_count}/{len(case_files)} files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

