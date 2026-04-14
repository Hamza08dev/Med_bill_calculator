"""
Multithreaded PDF Parser Pipeline.

This module mirrors the behavior of `pdf_parser_pipeline.py` but processes PDFs
concurrently using a thread pool to reduce overall wall time when handling many
files. Each worker thread maintains its own `PDFParserPipeline` instance to
avoid sharing stateful clients.
"""
from __future__ import annotations

import json
import os
import re
import base64
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed

from pypdf import PdfReader
import fitz  # PyMuPDF for image extraction
from PIL import Image
import io
import google.generativeai as genai
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.messages import HumanMessage
from pydantic import BaseModel, Field


# =============================================================================
# Phase 1: Strict Mode Keyword Matching (from check_iv_occurrences.py)
# =============================================================================


def get_pages_with_keyword_strict(pdf_path: Path, keyword: str) -> List[int]:
    """Return 1-based page numbers that match the keyword in strict mode.

    This exactly matches the logic from check_iv_occurrences.py
    get_pages_with_keyword() with mode="page_strict".
    """
    reader = PdfReader(str(pdf_path))
    matching_pages: List[int] = []

    total_pages = len(reader.pages)

    for i, page in enumerate(reader.pages, start=1):
        try:
            t = page.extract_text() or ""
        except Exception:
            t = ""
        if not keyword:
            continue

        reduced = re.sub(r"\s+", "", t)

        if keyword in reduced or reduced == keyword:
            if reduced == keyword:
                matching_pages.append(i)
                print(f"    ✅ Page {i}: MATCH (reduced='{reduced}', keyword='{keyword}')")
            else:
                print(
                    "    ⚠️  Page "
                    f"{i}: Contains keyword but doesn't match exactly "
                    f"(reduced='{reduced[:50]}...', length={len(reduced)})"
                )

    print(f"    📊 Scanned {total_pages} pages, found {len(matching_pages)} matches: {matching_pages}")
    return matching_pages


def find_start_page(pdf_path: Path, keyword: str = "IV") -> Tuple[int, List[int]]:
    """Find the start page for processing."""
    matching_pages = get_pages_with_keyword_strict(pdf_path, keyword)

    print(f"  🔍 Found {len(matching_pages)} IV occurrences on pages: {matching_pages}")

    if len(matching_pages) >= 1:
        start_page = matching_pages[0] + 1
        print(f"  ✅ Starting from page {start_page} (2nd IV occurrence)")
        return start_page, matching_pages
    else:
        start_page = 5
        return start_page, matching_pages


# =============================================================================
# Phase 2: Router/Classifier (Gemini 2.5 Flash)
# =============================================================================


@dataclass
class ClassifiedPage:
    page_number: int
    form_type: Optional[str] = None
    contains_cpt_codes: bool = False
    confidence: float = 0.0
    image: Optional[Image.Image] = None
    text_content: str = ""
    is_section_end: bool = False


class FormClassificationResponse(BaseModel):
    form_type: Optional[str] = Field(
        None,
        description="Type of form: 'health_insurance_form', 'nf3', 'other', or None if not a form",
    )
    contains_cpt_codes: bool = Field(
        False,
        description="Whether this page contains CPT codes",
    )
    confidence: float = Field(
        0.0,
        description="Confidence score (0.0 to 1.0)",
    )


def extract_page_as_image(pdf_path: Path, page_number: int, dpi: int = 300) -> Optional[Image.Image]:
    """Extract a page from PDF as a PIL Image."""
    try:
        doc = fitz.open(str(pdf_path))
        page = doc.load_page(page_number - 1)
        mat = fitz.Matrix(dpi / 72, dpi / 72)
        pix = page.get_pixmap(matrix=mat)
        img_data = pix.tobytes("png")
        img = Image.open(io.BytesIO(img_data))
        doc.close()
        return img
    except Exception as e:
        print(f"  ⚠️  Error extracting page {page_number} as image: {e}")
        return None


def image_to_base64(image: Image.Image) -> str:
    """Convert PIL Image to base64 string for LangChain."""
    buffered = io.BytesIO()
    image.save(buffered, format="PNG")
    img_base64 = base64.b64encode(buffered.getvalue()).decode("utf-8")
    return img_base64


class RouterClassifier:
    """Router/Classifier using Gemini 2.5 Flash to identify form types from images."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        genai.configure(api_key=api_key)

        self.classifier_model = ChatGoogleGenerativeAI(
            model="gemini-2.5-flash",
            temperature=0.1,
            google_api_key=api_key,
        )

        self.classifier_prompt_text = """
You are an expert document router. Your job is to look at this page IMAGE and classify it.
Respond with ONLY a single word.

Follow these rules:

1.  If the image is a "HEALTH INSURANCE CLAIM FORM" (it will say this at the top and/or "FORM 1500" in the footer, with numbered boxes like "Box 1", "Box 24"), respond with:
    **HEALTH_INSURANCE_FORM**

2.  If the image contains a table titled "15. REPORT OF SERVICES RENDERED" (like an NF-3 or pharmacy bill), respond with:
    **NF3_FORM**

3.  If the image is clearly the *end* of this section (e.g., it's a page titled with the Roman numeral "V" or "Medical Literature"), respond with:
    **SECTION_END**

4.  If the image is anything else (a W-9, an "Assignment of Benefits" text page, a legal brief, or any other form you don't recognize), respond with:
    **JUNK**

Your response must be *only* one of these four capitalized strings.
"""

    def classify_page(self, page_number: int, page_image: Image.Image) -> ClassifiedPage:
        """Classify a single page from its image."""
        try:
            img_base64 = image_to_base64(page_image)
            data_url = f"data:image/png;base64,{img_base64}"

            message = HumanMessage(
                content=[
                    {"type": "text", "text": self.classifier_prompt_text},
                    {"type": "image_url", "image_url": data_url},
                ]
            )

            response = self.classifier_model.invoke([message])

            raw_response = response.content.strip()
            print(f"  📝 Page {page_number} raw response: {raw_response}")

            response_text = response.content.strip().upper()
            response_text = (
                response_text.replace("*", "").replace("`", "").replace('"', "").replace("'", "").strip()
            )

            words = response_text.split()

            form_type = None
            is_section_end = False

            for word in words:
                if "HEALTH_INSURANCE_FORM" in word:
                    form_type = "health_insurance_form"
                    break
                elif "SECTION_END" in word:
                    is_section_end = True
                    form_type = None
                    break
                elif "JUNK" in word:
                    form_type = None
                    break

            if form_type is None and not is_section_end:
                if "HEALTH_INSURANCE_FORM" in response_text:
                    form_type = "health_insurance_form"
                elif "NF3_FORM" in response_text or ("NF3" in response_text and "FORM" in response_text):
                    form_type = "nf3"
                elif "SECTION_END" in response_text:
                    is_section_end = True
                    form_type = None
                elif "JUNK" in response_text:
                    form_type = None
                else:
                    print(f"  ⚠️  Unexpected classification response: {response.content.strip()[:100]}")
                    form_type = None

            contains_cpt_codes = form_type in ["health_insurance_form", "nf3"]

            return ClassifiedPage(
                page_number=page_number,
                form_type=form_type,
                contains_cpt_codes=contains_cpt_codes,
                confidence=1.0 if form_type else 0.0,
                image=page_image,
                text_content="",
                is_section_end=is_section_end,
            )

        except Exception as e:
            print(f"  ⚠️  Classification error on page {page_number}: {e}")
            return ClassifiedPage(
                page_number=page_number,
                form_type=None,
                contains_cpt_codes=False,
                confidence=0.0,
                image=page_image,
                text_content="",
                is_section_end=False,
            )

    def find_form_pages(
        self,
        pdf_path: Path,
        start_page: int,
        target_form_type: Optional[str] = None,
    ) -> List[ClassifiedPage]:
        """Find consecutive pages that match the form type."""
        doc = fitz.open(str(pdf_path))
        total_pages = len(doc)
        doc.close()

        form_pages: List[ClassifiedPage] = []

        for i in range(start_page - 1, total_pages):
            page_num = i + 1

            page_image = extract_page_as_image(pdf_path, page_num)
            if page_image is None:
                continue

            classified = self.classify_page(page_num, page_image)

            if classified.is_section_end:
                print(f"  ⏹️  Page {page_num}: Section end detected, stopping collection")
                break

            if target_form_type:
                if classified.form_type == target_form_type:
                    form_pages.append(classified)
                    print(
                        f"  ✅ Page {page_num}: {target_form_type} detected "
                        f"(contains CPT: {classified.contains_cpt_codes})"
                    )
                elif form_pages:
                    print(
                        f"  ⏹️  Page {page_num}: Different form type ({classified.form_type}), stopping collection"
                    )
                    break
                else:
                    if classified.contains_cpt_codes:
                        form_pages.append(classified)
                        print("  ✅ Page "
                              f"{page_num}: No form match but contains CPT codes, including")
                    elif classified.form_type:
                        print(f"  🔍 Page {page_num}: {classified.form_type} (looking for {target_form_type})")
            else:
                if classified.form_type or classified.contains_cpt_codes:
                    form_pages.append(classified)
                    print(
                        f"  ✅ Page {page_num}: {classified.form_type} detected "
                        f"(contains CPT: {classified.contains_cpt_codes})"
                    )
                elif form_pages:
                    break

        return form_pages

    def route_pdf(
        self,
        pdf_path: Path,
        start_page: int,
    ) -> Tuple[Optional[str], List[ClassifiedPage]]:
        """Route a PDF to find form type and relevant pages."""
        print(f"  🔍 Routing PDF starting from page {start_page}...")

        doc = fitz.open(str(pdf_path))
        total_pages = len(doc)
        doc.close()

        detected_form_type = None
        form_start_page = start_page
        scan_pages = min(20, total_pages - start_page + 1)

        for i in range(start_page - 1, min(start_page - 1 + scan_pages, total_pages)):
            page_num = i + 1

            page_image = extract_page_as_image(pdf_path, page_num)
            if page_image is None:
                continue

            classified = self.classify_page(page_num, page_image)

            if classified.is_section_end:
                print(f"  ⏹️  Section end detected on page {page_num} during routing")
                continue

            if classified.form_type:
                detected_form_type = classified.form_type
                form_start_page = page_num
                print(f"  📋 Detected form type: {detected_form_type} on page {page_num}")
                break

        if not detected_form_type:
            print("  📋 No form type detected")
            return None, []

        print(f"  🔍 Searching for {detected_form_type} pages starting from page {form_start_page}...")
        form_pages = self.find_form_pages(pdf_path, form_start_page, detected_form_type)

        return detected_form_type, form_pages


# =============================================================================
# Phase 3: Extractor (Gemini 2.5 Pro)
# =============================================================================


class ExtractedLineData(BaseModel):
    code: str = Field(..., description="CPT or HCPCS code")
    units: int = Field(1, description="Number of units")
    billed_amount: Optional[float] = Field(None, description="Billed amount")


class ExtractedCaseData(BaseModel):
    service_region_zip: Optional[str] = Field(None, description="Patient zip code from Box 5")
    provider_type: str = Field("medical", description="Provider type: 'medical', 'chiropractic', etc.")
    designation: Optional[str] = Field(None, description="Physician/Provider name with credentials")
    lines: List[ExtractedLineData] = Field(default_factory=list, description="List of CPT code lines")


class DataExtractor:
    """Extractor using Gemini models to extract structured data."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        genai.configure(api_key=api_key)

        self.extractor_model = ChatGoogleGenerativeAI(
            model="gemini-2.5-flash",
            temperature=0.1,
            google_api_key=api_key,
        )

        self.prompts = {
            "health_insurance_form": """
You are an expert medical billing analyst. Analyze the provided Health Insurance Claim Form (CMS-1500) images and extract structured billing information.

Extract the following information from the form images:
1. Patient Zip Code: Look in Box 5 "PATIENT'S ADDRESS (No., Street) ... ZIP CODE" 5 digit zip code
2. Provider Type: Check Box 33 "BILLING PROVIDER INFO & PH #" - if "chiropractic" is mentioned, set to "chiropractic", otherwise "medical"
3. Designation: Look in Box 31 "SIGNATURE OF PHYSICIAN OR SUPPLIER". INCLUDE FULL DESIGNATION with all credentials (MD, PA, NP, DO, DC,etc.) if present. Only look at box 31 for this
4. CPT Code Lines: For each line in Box 24 or from the table:
   - Code (24.D): CPT/HCPCS code 5 digits or 1 letter + 4 digits
   - Units (24.G): units
   - Billed Amount (24.F): Charges

Return a JSON object in this exact format:
{
    "service_region_zip": "11795",
    "provider_type": "medical",
    "designation": "Dr. John Smith, MD",
    "lines": [
        {
            "code": "99214",
            "units": 1,
            "billed_amount": 127.41
        }
    ]
}

IMPORTANT:
- Extract ALL CPT code lines from all page images provided
- Include full physician names with all credentials
- Only respond with the JSON object, no other text or markdown
""",
            "nf3": """
You are an expert medical billing analyst. Analyze the provided "VERIFICATION OF TREATMENT" (NYS FORM NF-3) images. These pages may be sent together.

Extract the following:
1.  **Patient Zip Code**: Look in **Box 15**, in the "PLACE OF SERVICE INCLUDING ZIP CODE" column. Use the zip code from the first line (e.g., "11730").
2.  **Provider Type**: Look in **Box 15**, in the "FEE SCHEDULE TREATMENT CODE" column (e.g., "DC"). If not found, default to "medical". if "DC" is found, set to "chiropractic".
3.  **Designation**: Look for the provider's name in **Box 15** ("FEE SCHEDULE TREATMENT CODE"), if pa or np is found, set to "pa" or "np". if md or do is found, set to "md". if dc is found, set to "dc". Only look at the Fee Schedule Treatment Code column for this
4.  **CPT Code Lines**: For each line in the table in **Box 15** ("REPORT OF SERVICES RENDERED"):
    * `code`: (FEE SCHEDULE TREATMENT CODE column). Note: This may be an NDC code like "51672-3008-05" or a code like "S9430". Extract the first 5 digits of the code in this column and nothing that comes after it for the code. It can contain a letter and 4 digits like "J1100". or 5 digits like "99214".
    * `units`: (Unit column). Extract the number from the Unit column.
    * `billed_amount`: (CHARGES column).

Return a JSON object in this exact format (do not include fields that are not present):
{
    "service_region_zip": "11730",
    "provider_type": "PHARMACY",
    "designation": "pa",
    "lines": [
        { "code": "51672-3008-05", "units": 200, "billed_amount": 1524.00 },
        { "code": "6838-2005-101", "units": 30, "billed_amount": 145.50 },
        { "code": "52817-0332-00", "units": 30, "billed_amount": 32.70 },
        { "code": "S9430", "units": 1, "billed_amount": 5.00 }
    ]
}
IMPORTANT: Only respond with the JSON object. Do not add markdown or explanations.
""",
            "other": """
You are an expert medical billing analyst. Analyze the provided medical form images.

Extract CPT codes, billed amounts, zip codes, and provider information.

Return a JSON object in this exact format:
{
    "service_region_zip": "11795",
    "provider_type": "medical",
    "designation": "Dr. John Smith, MD",
    "lines": [
        { "code": "99214", "units": 1, "billed_amount": 127.41 }
    ]
}
Only respond with the JSON object, no other text or markdown.
""",
        }
        }

    def extract_from_pages(
        self,
        classified_pages: List[ClassifiedPage],
        form_type: str,
    ) -> Dict[str, Any]:
        """Extract data from classified pages (images)."""
        if not classified_pages:
            return {
                "law_version_id": "ny_2018_01",
                "service_region_zip": None,
                "provider_type": "medical",
                "designation": None,
                "lines": [],
            }

        prompt_template = self.prompts.get(
            form_type,
            self.prompts["health_insurance_form"],
        )

        try:
            pages_count = len(classified_pages)
            if pages_count <= 3:
                model_name = "gemini-2.5-flash"
                print(f"  📤 Extracting data using Gemini 2.5 Flash from {pages_count} page(s)...")
            else:
                model_name = "gemini-2.5-pro"
                print(f"  📤 Extracting data using Gemini 2.5 Pro from {pages_count} page(s)...")

            extractor_model = ChatGoogleGenerativeAI(
                model=model_name,
                temperature=0.1,
                google_api_key=self.api_key,
            )

            content = [{"type": "text", "text": prompt_template}]

            for cp in classified_pages:
                if cp.image is not None:
                    content.append({"type": "text", "text": f"\n--- Page {cp.page_number} ---\n"})
                    img_base64 = image_to_base64(cp.image)
                    data_url = f"data:image/png;base64,{img_base64}"
                    content.append({"type": "image_url", "image_url": data_url})

            message = HumanMessage(content=content)
            response = extractor_model.invoke([message])

            response_text = response.content.strip()

            if "```json" in response_text:
                start = response_text.find("```json") + 7
                end = response_text.find("```", start)
                if end > start:
                    response_text = response_text[start:end].strip()
            elif "```" in response_text:
                start = response_text.find("```") + 3
                end = response_text.find("```", start)
                if end > start:
                    response_text = response_text[start:end].strip()

            if "{" in response_text and "}" in response_text:
                start = response_text.find("{")
                end = response_text.rfind("}") + 1
                response_text = response_text[start:end]

            extracted_data = json.loads(response_text)

            return {
                "law_version_id": "ny_2018_01",
                "service_region_zip": extracted_data.get("service_region_zip"),
                "provider_type": extracted_data.get("provider_type", "medical"),
                "designation": extracted_data.get("designation"),
                "lines": extracted_data.get("lines", []),
            }

        except Exception as e:
            print(f"  ❌ Extraction error: {e}")
            import traceback

            traceback.print_exc()
            if "response_text" in locals():
                print(f"  Response preview: {response_text[:200]}...")
            return {
                "law_version_id": "ny_2018_01",
                "service_region_zip": None,
                "provider_type": "medical",
                "designation": None,
                "lines": [],
            }


# =============================================================================
# Main Pipeline
# =============================================================================


class PDFParserPipeline:
    """Main pipeline orchestrating all three phases."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.classifier = RouterClassifier(api_key)
        self.extractor = DataExtractor(api_key)

    def process_pdf(self, pdf_path: Path, log_dir: Optional[Path] = None) -> Dict[str, Any]:
        """Process a PDF through all three phases."""
        print(f"\n{'='*60}")
        print(f"Processing: {pdf_path.name}")
        print(f"{'='*60}")

        print("\n📌 Phase 1: Strict Mode Keyword Matching")
        start_page, all_matching_pages = find_start_page(pdf_path, "IV")

        print("\n🔀 Phase 2: Router/Classifier (Gemini 2.5 Flash)")
        form_type, classified_pages = self.classifier.route_pdf(pdf_path, start_page)
        print(f"  Found {len(classified_pages)} pages with form type: {form_type}")

        if not form_type or not classified_pages:
            print("  ⚠️  No form detected, returning not_detected result")
            if log_dir:
                log_form_not_detected(pdf_path, log_dir, start_page)
            return {
                "law_version_id": "ny_2018_01",
                "service_region_zip": None,
                "provider_type": "medical",
                "designation": None,
                "lines": [],
                "form_detection": "not_detected",
            }

        print(f"\n📝 Phase 3: Data Extraction (Model: Flash if ≤3 pages, Pro if >3 pages)")
        result = self.extractor.extract_from_pages(classified_pages, form_type)

        print(f"  ✅ Extracted {len(result.get('lines', []))} CPT code lines")
        if result.get("lines"):
            codes = [line.get("code") for line in result["lines"]]
            print(f"  CPT Codes: {codes}")

        return result


def log_form_not_detected(pdf_path: Path, log_dir: Path, start_page: int = None) -> None:
    """Log files where form was not detected to a log file."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "form_not_detected.log"

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    start_page_info = f" (start_page: {start_page})" if start_page else ""
    log_entry = f"{timestamp} | {pdf_path.name}{start_page_info}\n"

    with open(log_file, "a", encoding="utf-8") as f:
        f.write(log_entry)

    print(f"  📝 Logged to: {log_file}")


def get_case_extract_filename(pdf_path: Path) -> str:
    """Generate output filename following legacy rules."""
    stem = pdf_path.stem

    underscore_match = re.search(r"_(\d+)$", stem)
    if underscore_match:
        parts = stem.split("_")
        if len(parts) == 2:
            prefix_part = parts[0]
            suffix = parts[1]

            dash_parts = prefix_part.split("-")
            if len(dash_parts) >= 4:
                middle_8 = f"{dash_parts[-2]}_{dash_parts[-1]}"
                return f"case_extract_{middle_8}_{suffix}.json"

    digits_only = re.sub(r"\D", "", stem)

    if len(digits_only) >= 8:
        last_8_digits = digits_only[-8:]
        formatted = f"{last_8_digits[:4]}_{last_8_digits[4:]}"
        return f"case_extract_{formatted}.json"
    else:
        return f"{stem}_case_extract.json"


def load_api_key_from_config(config_path: Path = Path("configs/db_config.json")) -> str:
    """Load Gemini API key from db_config.json."""
    try:
        config = json.loads(config_path.read_text(encoding="utf-8"))
        return config["gemini"]["api_key"]
    except Exception as e:
        raise ValueError(f"Could not load API key from {config_path}: {e}")


_thread_local = threading.local()


def _get_thread_pipeline(api_key: str) -> PDFParserPipeline:
    """Return a pipeline bound to the current thread."""
    pipeline = getattr(_thread_local, "pipeline", None)
    if pipeline is None:
        pipeline = PDFParserPipeline(api_key)
        _thread_local.pipeline = pipeline
    return pipeline


def _process_single_pdf(
    pdf_path: Path,
    output_dir: Path,
    log_dir: Path,
    api_key: str,
) -> Tuple[str, Optional[str], Optional[Path]]:
    """Process a single PDF, returning status and output path."""
    pipeline = _get_thread_pipeline(api_key)

    try:
        result = pipeline.process_pdf(pdf_path, log_dir=log_dir)

        output_filename = get_case_extract_filename(pdf_path)
        output_file = output_dir / output_filename
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)

        if result.get("form_detection") == "not_detected":
            status = "not_detected"
            print(f"\n⚠️  Processed but no form detected: {output_file.name}")
        else:
            status = "successful"
            print(f"\n✅ Successfully processed: {output_file.name}")

        return status, pdf_path.name, output_file
    except Exception as e:
        print(f"\n❌ Failed to process {pdf_path.name}: {e}")
        import traceback

        traceback.print_exc()
        return "failed", pdf_path.name, None


def main():
    """Main function to process PDFs concurrently."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Multithreaded PDF Parser Pipeline - Extract CPT codes and billing info",
    )
    parser.add_argument(
        "--dir",
        dest="pdf_dir",
        type=str,
        default=str(Path(__file__).resolve().parents[1] / "final_cases_final"),
        help="Directory containing PDFs (default: fee-schedule-kag/final_cases_final)",
    )
    parser.add_argument(
        "--output-dir",
        dest="output_dir",
        type=str,
        default=str(Path(__file__).resolve().parents[1] / "case_extracts"),
        help="Output directory for case_extract.json files (default: fee-schedule-kag/case_extracts)",
    )
    parser.add_argument(
        "--limit",
        dest="limit",
        type=int,
        default=None,
        help="Limit number of PDFs to process (default: process all)",
    )
    parser.add_argument(
        "--api-key",
        dest="api_key",
        type=str,
        default=None,
        help="Gemini API key (default: load from configs/db_config.json)",
    )
    parser.add_argument(
        "--max-workers",
        dest="max_workers",
        type=int,
        default=min(4, (os.cpu_count() or 4)),
        help="Number of worker threads for concurrent processing (default: 4 or CPU count)",
    )

    args = parser.parse_args()

    if args.api_key:
        api_key = args.api_key
    else:
        config_path = Path(__file__).resolve().parents[1] / "configs" / "db_config.json"
        api_key = load_api_key_from_config(config_path)

    pdf_dir = Path(args.pdf_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    log_dir = Path(__file__).resolve().parents[1] / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    if not pdf_dir.exists() or not pdf_dir.is_dir():
        raise SystemExit(f"Directory not found or not a directory: {pdf_dir}")

    pdfs = [e for e in sorted(pdf_dir.iterdir()) if e.is_file() and e.suffix.lower() == ".pdf"]
    if args.limit is not None:
        pdfs = pdfs[: max(0, args.limit)]

    print(f"Found {len(pdfs)} PDFs to process")
    print(f"Output directory: {output_dir}")
    print(f"Log directory: {log_dir}")
    print(f"Using up to {args.max_workers} worker threads")

    successful = 0
    failed = 0
    skipped = 0
    not_detected = 0

    tasks: List[Path] = []

    for pdf_path in pdfs:
        output_filename = get_case_extract_filename(pdf_path)
        output_file = output_dir / output_filename

        if output_file.exists():
            print(f"\n{'#'*60}")
            print(f"PDF: {pdf_path.name}")
            print(f"{'#'*60}")
            print(f"⏭️  Skipping {pdf_path.name} - output file already exists: {output_file.name}")
            skipped += 1
            continue

        tasks.append(pdf_path)

    if not tasks:
        print("No PDFs left to process after skipping existing outputs.")
        print(f"\n{'='*60}")
        print("Summary:")
        print(f"  Successful: {successful}")
        print(f"  Form not detected: {not_detected}")
        print(f"  Failed: {failed}")
        print(f"  Skipped: {skipped}")
        print(f"  Total queued: {len(tasks)}")
        print(f"{'='*60}")
        return

    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        future_to_pdf = {
            executor.submit(_process_single_pdf, pdf_path, output_dir, log_dir, api_key): pdf_path
            for pdf_path in tasks
        }

        for future in as_completed(future_to_pdf):
            status, pdf_name, output_file = future.result()
            if status == "successful":
                successful += 1
            elif status == "not_detected":
                not_detected += 1
            else:
                failed += 1

    print(f"\n{'='*60}")
    print("Summary:")
    print(f"  Successful: {successful}")
    print(f"  Form not detected: {not_detected}")
    print(f"  Failed: {failed}")
    print(f"  Skipped: {skipped}")
    print(f"  Total processed: {len(tasks)}")
    if not_detected > 0:
        print(f"\n  📝 Form not detected log: {log_dir / 'form_not_detected.log'}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()

