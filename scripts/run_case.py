from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import List

from pdf_parser_pipeline import PDFParserPipeline, load_api_key_from_config, get_case_extract_filename


def find_pdfs_by_code(pdf_dir: Path, code: str) -> List[Path]:
    """Return all PDFs in pdf_dir whose filenames contain the 8-digit code.
    
    Handles two formats:
    1. Old format: "17-24-1366-6697.pdf" - checks if code matches last 8 digits
    2. New format: "17-24-1375-4465_8218282.pdf" - checks if code matches middle 8 digits (1375-4465)

    Example: 
        - For code "13680799", a filename like "17-24-1368-0799.pdf" matches (ends with code)
        - For code "13754465", a filename like "17-24-1375-4465_8218282.pdf" matches (middle 8 digits)
    """
    if not re.fullmatch(r"\d{8}", code):
        raise ValueError("case code must be exactly 8 digits")
    if not pdf_dir.exists() or not pdf_dir.is_dir():
        raise FileNotFoundError(f"Directory not found or not a directory: {pdf_dir}")

    matches: List[Path] = []
    for entry in sorted(pdf_dir.iterdir()):
        if not (entry.is_file() and entry.suffix.lower() == ".pdf"):
            continue
        
        stem = entry.stem
        
        # Check for new format: has underscore followed by digits
        if '_' in stem:
            # New format: check if code matches middle 8 digits (between dashes before underscore)
            # Pattern: XX-XX-XXXX-XXXX_suffix
            parts = stem.split('_')
            if len(parts) == 2:
                prefix_part = parts[0]  # "17-24-1375-4465"
                dash_parts = prefix_part.split('-')
                if len(dash_parts) >= 4:
                    # Get middle 8 digits (last two groups before underscore)
                    middle_8 = f"{dash_parts[-2]}{dash_parts[-1]}"  # "13754465"
                    if middle_8 == code:
                        matches.append(entry)
                        continue
        
        # Old format: check if code matches last 8 digits
        digits_only = re.sub(r"\D", "", stem)
        if digits_only.endswith(code):
            matches.append(entry)
    
    return matches


def run_for_code(
    code: str,
    api_key: str,
    pdf_dir: Path,
    output_dir: Path,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    pdfs = find_pdfs_by_code(pdf_dir, code)
    if not pdfs:
        raise SystemExit(f"No PDF found in {pdf_dir} containing code: {code}")

    print(f"Found {len(pdfs)} matching PDF(s) for code {code}")

    # Create log directory for form not detected logs
    log_dir = output_dir.parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    print(f"Log directory: {log_dir}")

    pipeline = PDFParserPipeline(api_key)

    successful = 0
    failed = 0
    skipped = 0
    not_detected = 0

    for pdf_path in pdfs:
        # Check if output file already exists (resume functionality)
        output_filename = get_case_extract_filename(pdf_path)
        output_file = output_dir / output_filename
        
        if output_file.exists():
            print("\n" + "#" * 60)
            print(f"Processing: {pdf_path.name}")
            print("#" * 60)
            print(f"⏭️  Skipping {pdf_path.name} - output file already exists: {output_file.name}")
            skipped += 1
            continue
        
        try:
            print("\n" + "#" * 60)
            print(f"Processing: {pdf_path.name}")
            print("#" * 60)

            result = pipeline.process_pdf(pdf_path, log_dir=log_dir)

            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2, ensure_ascii=False)

            # Check if form was not detected
            if result.get("form_detection") == "not_detected":
                not_detected += 1
                print(f"\n⚠️  Processed but no form detected: {output_file.name}")
            else:
                print(f"\n✅ Successfully processed: {output_file.name}")
                successful += 1
        except Exception as e:
            print(f"\n❌ Failed to process {pdf_path.name}: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print("\n" + "=" * 60)
    print("Summary:")
    print(f"  Successful: {successful}")
    print(f"  Form not detected: {not_detected}")
    print(f"  Failed: {failed}")
    print(f"  Skipped: {skipped}")
    print(f"  Total: {len(pdfs)}")
    if not_detected > 0:
        print(f"\n  📝 Form not detected log: {log_dir / 'form_not_detected.log'}")
    print("=" * 60)


def main() -> None:
    script_root = Path(__file__).resolve()
    project_root = script_root.parents[1]

    default_pdf_dir = project_root / "final_cases_final"
    default_output_dir = project_root / "case_extract_auto"
    default_config = project_root / "configs" / "db_config.json"

    parser = argparse.ArgumentParser(
        description=(
            "Run PDF Parser Pipeline for a specific 8-digit case code. "
            "Searches for PDF(s) in final_cases_final whose filenames contain the code."
        )
    )
    parser.add_argument("code", type=str, help="8-digit case code to process (e.g., 12345678)")
    parser.add_argument(
        "--pdf-dir",
        dest="pdf_dir",
        type=str,
        default=str(default_pdf_dir),
        help=f"Directory containing PDFs (default: {default_pdf_dir})",
    )
    parser.add_argument(
        "--output-dir",
        dest="output_dir",
        type=str,
        default=str(default_output_dir),
        help=f"Output directory for case_extract.json files (default: {default_output_dir})",
    )
    parser.add_argument(
        "--api-key",
        dest="api_key",
        type=str,
        default=None,
        help="Gemini API key (default: load from configs/db_config.json)",
    )
    parser.add_argument(
        "--config",
        dest="config_path",
        type=str,
        default=str(default_config),
        help=f"Config path to load API key if --api-key not provided (default: {default_config})",
    )

    args = parser.parse_args()

    api_key: str
    if args.api_key:
        api_key = args.api_key
    else:
        api_key = load_api_key_from_config(Path(args.config_path))

    run_for_code(
        code=args.code,
        api_key=api_key,
        pdf_dir=Path(args.pdf_dir),
        output_dir=Path(args.output_dir),
    )


if __name__ == "__main__":
    main()


