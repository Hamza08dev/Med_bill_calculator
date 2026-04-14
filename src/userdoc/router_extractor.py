# src/userdoc/router_extractor.py
from __future__ import annotations
import json
import re
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
import fitz  # PyMuPDF
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import PydanticOutputParser
from pydantic import BaseModel, Field
import google.generativeai as genai
import pytesseract
from .shared import load_yaml_from_configs


@dataclass
class FormPage:
    """Represents a page containing a health insurance form"""
    page_number: int
    confidence: float
    form_type: str
    text_content: str


@dataclass
class ExtractedData:
    """Represents extracted data from a health insurance form"""
    cpt_code: Optional[str] = None
    zip_code: Optional[str] = None
    units: Optional[float] = None
    physician_name: Optional[str] = None
    provider_type: str = "medical"
    place_of_service: Optional[str] = None
    billed_amount: Optional[float] = None
    dos_from: Optional[str] = None
    dos_to: Optional[str] = None


class HealthInsuranceFormData(BaseModel):
    """Pydantic model for structured output from health insurance forms"""
    cpt_code: Optional[str] = Field(None, description="CPT code from Box 24.D")
    zip_code: Optional[str] = Field(None, description="Patient zip code from Box 5")
    units: Optional[float] = Field(None, description="Units from Box 24.G")
    physician_name: Optional[str] = Field(None, description="Physician name from Box 32")
    provider_type: str = Field("medical", description="Provider type: 'chiropractic' if mentioned in Box 33, otherwise 'medical'")
    place_of_service: Optional[str] = Field(None, description="Place of service code")
    billed_amount: Optional[float] = Field(None, description="Billed amount")
    dos_from: Optional[str] = Field(None, description="Date of service from")
    dos_to: Optional[str] = Field(None, description="Date of service to")


class RouterExtractor:
    """Router extractor system for health insurance forms"""
    
    def __init__(self, api_key: str):
        """Initialize the router extractor with API key"""
        self.api_key = api_key
        genai.configure(api_key=api_key)
        
        # Initialize extractor model (router uses keyword matching)
        self.extractor_model = ChatGoogleGenerativeAI(
            model="gemini-2.0-flash-exp",
            temperature=0.1,
            google_api_key=api_key
        )
        
        # Extractor prompt for detailed data extraction
        self.extractor_prompt = ChatPromptTemplate.from_template("""
You are an expert medical billing analyst. Extract specific information from this health insurance claim form.

Extract the following information:
1. CPT Code: Look in Box 24.D "PROCEDURES, SERVICES, OR SUPPLIES (Explain Unusual Circumstances) CPT/HCPCS"
2. Zip Code: Look in Box 5 "PATIENT'S ADDRESS (No., Street) ... ZIP CODE"
3. Units: Look in Box 24.G "DAYS OR UNITS"
4. Physician Name: Look in Box 32 "SERVICE FACILITY LOCATION INFORMATION" and Box 33 for physician/designation information. INCLUDE FULL DESIGNATION with all credentials (MD, PA, NP, DO, etc.) if present.
5. Provider Type: Check Box 33 "BILLING PROVIDER INFO & PH #" - if "chiropractic" is mentioned, set to "chiropractic", otherwise "medical"
6. Place of Service: Look in Box 24.B
7. Billed Amount: Look in Box 24.F "CHARGES"
8. Date of Service: Look in Box 24.A "DATE(S) OF SERVICE"

Form text:
{form_text}

IMPORTANT: Respond with a JSON object in this exact format:
{{
    "cpt_code": "99214",
    "zip_code": "11795",
    "units": 1.0,
    "physician_name": "Dr. John Smith, MD",
    "provider_type": "medical",
    "place_of_service": "11",
    "billed_amount": 127.41,
    "dos_from": "2024-03-18",
    "dos_to": "2024-03-18"
}}

For physician_name: Extract the complete name AND all credentials/titles (Dr., MD, PA, NP, DO, etc.). If you see "Dr. Jane Doe, MD" or "John Smith, PA" or similar, capture everything including the credentials.

Extract only the information that is clearly visible and readable. If information is not present or unclear, set to null.
Do not include any other text, explanations, or markdown formatting.
""")
        
        # Initialize output parser
        self.output_parser = PydanticOutputParser(pydantic_object=HealthInsuranceFormData)
        
    def extract_pages_from_pdf(self, pdf_path: Path, use_ocr: bool = False) -> List[Dict[str, Any]]:
        """Extract text from all pages of a PDF, optionally using OCR if needed"""
        pages = []
        doc = fitz.open(str(pdf_path))
        
        for page_num in range(len(doc)):
            page = doc.load_page(page_num)
            
            # Try to extract text normally first
            text = page.get_text("text")
            
            # If text is very short or empty, try OCR only if enabled
            if use_ocr and (not text or len(text.strip()) < 50):
                try:
                    # Convert page to image and use OCR
                    pix = page.get_pixmap(dpi=300)  # High DPI for better OCR
                    img_data = pix.tobytes("png")
                    
                    # Try OCR with pytesseract
                    try:
                        import pytesseract
                        from PIL import Image
                        import io
                        
                        img = Image.open(io.BytesIO(img_data))
                        ocr_text = pytesseract.image_to_string(img, lang='eng')
                        if ocr_text and len(ocr_text.strip()) > len(text.strip()):
                            text = ocr_text
                            print(f"OCR successful for page {page_num + 1}: extracted {len(text)} characters")
                    except Exception as ocr_error:
                        print(f"OCR failed for page {page_num + 1}: {ocr_error}")
                        
                except Exception as e:
                    print(f"Image conversion failed for page {page_num + 1}: {e}")
            
            pages.append({
                "page": page_num + 1,
                "text": text or ""
            })
        
        doc.close()
        return pages
    
    def route_pages(self, pages: List[Dict[str, Any]]) -> List[FormPage]:
        """Use keyword matching to identify pages containing health insurance forms"""
        form_pages = []
        
        for page in pages:
            if not page["text"].strip():
                continue
                
            text = page["text"].upper()  # Convert to uppercase for case-insensitive matching
            
            # More flexible keyword matching to handle OCR issues
            # Look for variations of "HEALTH INSURANCE CLAIM FORM"
            health_insurance_variations = [
                "HEALTH INSURANCE CLAIM FORM",
                "HEAL TH INSURANCE CLAIM FORM",  # OCR issue with space
                "HEALTH INSURANCE CLAIM",
                "INSURANCE CLAIM FORM",
                "HEALTH INSURANCE FORM",
                "HEAL TH INSURANCE CLAIM",  # OCR variations
                "HEAL TH INSURANCE FORM",
                "HEALTH INSURANCE",
                "CLAIM FORM",
                "MEDICAL CLAIM FORM",
                "BILLING FORM"
            ]
            
            # Look for NUCC variations
            nucc_variations = [
                "NATIONAL UNIFORM CLAIM COMMITTEE",
                "NUCC",
                "NATIONAL UNIFORM CLAIM",
                "UNIFORM CLAIM COMMITTEE"
            ]
            
            # Check if any health insurance variation is present
            has_health_insurance_form = any(variation in text for variation in health_insurance_variations)
            
            # Check if any NUCC variation is present
            has_nucc = any(variation in text for variation in nucc_variations)
            
            # Form is detected if we have any health insurance form variation AND any NUCC variation
            if has_health_insurance_form and has_nucc:
                # Calculate confidence based on keyword presence
                confidence = 0.9  # High confidence for keyword match
                
                # Determine form type
                form_type = "health_insurance_claim_form"
                
                form_pages.append(FormPage(
                    page_number=page["page"],
                    confidence=confidence,
                    form_type=form_type,
                    text_content=page["text"]
                ))
                
                print(f"✅ Health insurance claim form detected on page {page['page']} (confidence: {confidence:.2f})")
            else:
                # Fallback: Look for pages with multiple billing-related terms (for scanned forms)
                billing_terms = [
                    "CPT", "HCPCS", "PROCEDURE", "CODE", "BILLED", "AMOUNT", 
                    "DATE OF SERVICE", "PATIENT", "INSURED", "PROVIDER",
                    "DIAGNOSIS", "MODIFIER", "UNITS", "CHARGES"
                ]
                
                billing_score = sum(1 for term in billing_terms if term in text)
                
                # If we have a high billing score and some form indicators, it might be a scanned form
                # Also check for pages with "CLAIM FORM" even without NUCC
                has_claim_form = "CLAIM FORM" in text
                
                if billing_score >= 5 and (has_health_insurance_form or has_nucc or has_claim_form):
                    confidence = 0.7  # Lower confidence for fallback detection
                    form_type = "health_insurance_claim_form"
                    
                    form_pages.append(FormPage(
                        page_number=page["page"],
                        confidence=confidence,
                        form_type=form_type,
                        text_content=page["text"]
                    ))
                    
                    print(f"✅ Health insurance claim form detected on page {page['page']} (fallback, confidence: {confidence:.2f})")
                else:
                    print(f"❌ No health insurance claim form on page {page['page']}")
                    if has_health_insurance_form:
                        print(f"   - Has health insurance claim form variation: ✅")
                    else:
                        print(f"   - Has health insurance claim form variation: ❌")
                    if has_nucc:
                        print(f"   - Has NUCC variation: ✅")
                    else:
                        print(f"   - Has NUCC variation: ❌")
                    print(f"   - Billing terms score: {billing_score}/15")
        
        return form_pages
    
    def extract_form_data(self, form_page: FormPage) -> ExtractedData:
        """Extract detailed data from a health insurance form page"""
        try:
            # Use the structured output parser
            response = self.extractor_model.invoke(
                self.extractor_prompt.format(form_text=form_page.text_content)
            )
            
            # Clean and parse JSON response
            response_text = response.content.strip()
            
            # Try to extract JSON from response if it's wrapped in markdown
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
            
            # Try to find JSON object in the response
            if "{" in response_text and "}" in response_text:
                start = response_text.find("{")
                end = response_text.rfind("}") + 1
                response_text = response_text[start:end]
            
            # Parse the structured output
            parsed_data = json.loads(response_text)
            
            return ExtractedData(
                cpt_code=parsed_data.get("cpt_code"),
                zip_code=parsed_data.get("zip_code"),
                units=parsed_data.get("units"),
                physician_name=parsed_data.get("physician_name"),
                provider_type=parsed_data.get("provider_type", "medical"),
                place_of_service=parsed_data.get("place_of_service"),
                billed_amount=parsed_data.get("billed_amount"),
                dos_from=parsed_data.get("dos_from"),
                dos_to=parsed_data.get("dos_to")
            )
            
        except Exception as e:
            print(f"Error extracting data from page {form_page.page_number}: {e}")
            if 'response' in locals():
                print(f"Response was: {response.content[:200]}...")
            return ExtractedData()
    
    def process_pdf(self, pdf_path: Path, use_ocr: bool = False) -> List[ExtractedData]:
        """Process a PDF and extract health insurance form data"""
        print(f"Processing PDF: {pdf_path}")
        
        # Extract pages
        pages = self.extract_pages_from_pdf(pdf_path, use_ocr=use_ocr)
        print(f"Extracted {len(pages)} pages")
        
        # Route pages to find forms
        form_pages = self.route_pages(pages)
        print(f"Found {len(form_pages)} form pages")
        
        # Extract data from each form page
        extracted_data = []
        for form_page in form_pages:
            print(f"Extracting data from page {form_page.page_number} (confidence: {form_page.confidence:.2f})")
            data = self.extract_form_data(form_page)
            extracted_data.append(data)
        
        return extracted_data
    
    def create_case_extract_json(self, extracted_data: List[ExtractedData], 
                                law_version_id: str = "ny_2018_01") -> Dict[str, Any]:
        """Create case_extract.json format from extracted data"""
        if not extracted_data:
            return {
                "law_version_id": law_version_id,
                "service_region_zip": None,
                "provider_type": "medical",
                "designation": None,
                "lines": []
            }
        
        # Use the first valid data entry as primary
        primary_data = next((d for d in extracted_data if d.cpt_code), extracted_data[0])
        
        # Build lines array
        lines = []
        for data in extracted_data:
            if data.cpt_code:
                line = {
                    "code": data.cpt_code,
                    "modifiers": [],
                    "units": int(data.units) if data.units else 1,
                    "dos_from": data.dos_from,
                    "dos_to": data.dos_to or data.dos_from,
                    "place_of_service": data.place_of_service or "11",
                    "billed_amount": data.billed_amount
                }
                lines.append(line)
        
        return {
            "law_version_id": law_version_id,
            "service_region_zip": primary_data.zip_code,
            "provider_type": primary_data.provider_type,
            "designation": primary_data.physician_name,
            "lines": lines
        }


def load_api_key_from_config(config_path: Path = Path("configs/db_config.json")) -> str:
    """Load Gemini API key from db_config.json"""
    try:
        config = json.loads(config_path.read_text(encoding="utf-8"))
        return config["gemini"]["api_key"]
    except Exception as e:
        raise ValueError(f"Could not load API key from {config_path}: {e}")


def process_pdf_with_router_extractor(pdf_path: Path, 
                                    law_version_id: str = "ny_2018_01",
                                    api_key: Optional[str] = None) -> Dict[str, Any]:
    """Main function to process a PDF using the router extractor system"""
    if api_key is None:
        api_key = load_api_key_from_config()
    
    extractor = RouterExtractor(api_key)
    extracted_data = extractor.process_pdf(pdf_path)
    return extractor.create_case_extract_json(extracted_data, law_version_id)
