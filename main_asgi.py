from __future__ import annotations

import asyncio
import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal
from uuid import uuid4

from fastapi import Body, FastAPI, File, HTTPException, UploadFile
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from pydantic import BaseModel, Field, validator

import logging
from logging.handlers import RotatingFileHandler

from scripts.pdf_parser_pipeline import PDFParserPipeline, get_case_extract_filename
from scripts.batch_calc_case_extracts import (
    discover_case_extracts,
    derive_case_id,
    calculate_for_case_extract,
    connect_driver,
    ensure_kg_initialized,
    ensure_output_dir,
)
from scripts.fee_calculator_service import calculate_custom_fee
from src.calc.fee_engine import FeeEngine

APP_NAME = "No-Fault Fee Schedule Orchestrator"
APP_VERSION = "2.0.0"

load_dotenv(override=True)

REPO_ROOT = Path(__file__).resolve().parent
LOG_DIR = REPO_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
TEMP_UPLOAD_DIR = REPO_ROOT / "temp_uploads"
TEMP_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
CASE_EXTRACTS_DIR = REPO_ROOT / "case_extracts"
KG_OUTPUT_DIR = REPO_ROOT / "kg_calc"
PDF_INPUT_DIR = REPO_ROOT / "final_cases_final"
CASE_NUMBER_PATTERN = re.compile(r"^\d{8}$")

LOG_FILE = LOG_DIR / "app.log"
logger = logging.getLogger(APP_NAME)
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=3, encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

if not logger.handlers:
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

app = FastAPI(title=APP_NAME, version=APP_VERSION)

# Allow frontend (e.g. Vite on localhost:5173) to call this API during development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten later for prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class CalculatorLinePayload(BaseModel):
    code: str = Field(..., min_length=1, max_length=32)
    units: int = Field(1, gt=0, le=99)
    billed_amount: float | None = Field(default=0, ge=0)


class FeeCalculatorRequest(BaseModel):
    zip_code: str = Field(..., pattern=r"^\d{5}$")
    provider_type: Literal["medical", "podiatry", "chiropractic"]
    designation: str | None = Field(default=None, max_length=64)
    is_np_pa: bool = False
    skip_ground_rules: bool = Field(default=False)
    lines: List[CalculatorLinePayload]

    @validator("lines")
    def validate_lines(cls, value: List[CalculatorLinePayload]):
        if not value:
            raise ValueError("At least one CPT line is required.")
        return value

def load_db_config() -> Dict[str, Any]:
    root = Path.cwd()
    base = root / "configs" / "db_config.json"
    local = root / "configs" / "db_config.local.json"

    cfg: Dict[str, Any] = {}
    if base.exists():
        try:
            cfg = json.loads(base.read_text(encoding="utf-8"))
        except Exception:
            cfg = {}
    if local.exists():
        try:
            c2 = json.loads(local.read_text(encoding="utf-8"))
            if isinstance(c2, dict):
                cfg.update(c2)
        except Exception:
            pass

    neo = cfg.get("neo4j", {}) or {}
    neo["uri"] = os.getenv("NEO4J_URI", neo.get("uri"))
    neo["user"] = os.getenv("NEO4J_USER", neo.get("user"))
    neo["password"] = os.getenv("NEO4J_PASSWORD", neo.get("password"))
    cfg["neo4j"] = neo

    gem = cfg.get("gemini", {}) or {}
    gem["api_key"] = os.getenv("GEMINI_API_KEY", gem.get("api_key"))
    cfg["gemini"] = gem

    data = cfg.get("data", {}) or {}
    data["directory"] = os.getenv("DATA_DIR", data.get("directory"))
    cfg["data"] = data

    return cfg

def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

def _process_pdf(api_key: str, pdf_path: Path, log_dir: Path) -> Dict[str, Any]:
    pipeline = PDFParserPipeline(api_key)
    return pipeline.process_pdf(pdf_path, log_dir=log_dir)


def _find_pdf_for_case_number(case_number: str) -> Path | None:
    """Locate the PDF that matches the provided 8-digit case number."""
    if not CASE_NUMBER_PATTERN.fullmatch(case_number):
        return None

    if not PDF_INPUT_DIR.exists():
        return None

    first_half, second_half = case_number[:4], case_number[4:]
    hyphen_token = f"-{first_half}-{second_half}"
    underscore_token = f"{first_half}_{second_half}"

    matches: List[Path] = []
    for pdf_path in sorted(PDF_INPUT_DIR.glob("*.pdf")):
        stem = pdf_path.stem
        if (
            hyphen_token in stem
            or underscore_token in stem
            or case_number in stem
        ):
            matches.append(pdf_path)

    if not matches:
        return None

    # Prefer exact suffix matches to disambiguate multiple files.
    preferred: List[Path] = []
    for candidate in matches:
        stem = candidate.stem
        if stem.endswith(hyphen_token) or stem.endswith(underscore_token):
            preferred.append(candidate)
        elif stem.endswith(f"{underscore_token}".rstrip("_")):
            preferred.append(candidate)

    if len(preferred) == 1:
        return preferred[0]
    if len(matches) == 1:
        return matches[0]

    raise ValueError(
        f"Multiple PDF files matched case number {case_number}: "
        + ", ".join(p.name for p in matches)
    )


def _build_kg_calc_payload(calc_result: Dict[str, Any] | None) -> Dict[str, Any]:
    """Normalize FeeEngine results into kg_calc.json structure."""
    if not isinstance(calc_result, dict):
        calc_result = {}
    else:
        calc_result = dict(calc_result)  # shallow copy

    calc_result.pop("explanation", None)
    calc_result.pop("legal_explanation", None)

    calc_items = calc_result.get("calculation_results", []) or []
    line_results: List[Dict[str, Any]] = []
    for item in calc_items:
        if not isinstance(item, dict) or "error" in item:
            continue
        cpt = item.get("cpt_code")
        fee_val_raw = item.get("calculated_fee", 0)
        try:
            fee_val = float(fee_val_raw or 0)
        except (TypeError, ValueError):
            fee_val = 0.0
        line_results.append(
            {
                "cpt_code": cpt,
                "calculated_fee": round(fee_val, 2),
                "modifier_applied": item.get("modifier_applied", ""),
                "rvu": item.get("rvu"),
                "conversion_factor": item.get("conversion_factor"),
                "schedule": item.get("schedule"),
                "units": item.get("units", 1),
                "explanation": f"Fee calculated for CPT {cpt}: ",
            }
        )

    try:
        total_amount = round(float(calc_result.get("total_calculated_amount", 0)), 2)
    except (TypeError, ValueError):
        total_amount = 0.0

    return {
        "total_calculated_amount": total_amount,
        "line_results": line_results,
        "region": calc_result.get("region"),
        "provider_type": calc_result.get("provider_type"),
        "designation": calc_result.get("designation", ""),
    }


def _calculate_in_memory_kg_calc(engine: FeeEngine, case_json: Dict[str, Any]) -> Dict[str, Any]:
    calc_result = calculate_for_case_extract(engine, case_json)
    return _build_kg_calc_payload(calc_result)

def _run_pdf_parser_job(
    api_key: str,
    pdf_dir: Path,
    output_dir: Path,
    log_dir: Path,
    limit: int | None = None,
    overwrite: bool = False,
) -> Dict[str, Any]:
    """Mirror scripts/pdf_parser_pipeline.py main() for API triggers."""
    if not api_key:
        raise RuntimeError("Gemini API key not configured")
    if not pdf_dir.exists():
        raise FileNotFoundError(f"PDF directory not found: {pdf_dir}")

    output_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    pdfs = [p for p in sorted(pdf_dir.glob("*.pdf")) if p.is_file()]
    if limit is not None:
        pdfs = pdfs[: max(0, limit)]

    results = {
        "total": len(pdfs),
        "processed": 0,
        "skipped": 0,
        "failed": 0,
        "details": [],
        "log_file": str(log_dir / "form_not_detected.log"),
    }

    if not pdfs:
        return results

    pipeline = PDFParserPipeline(api_key)

    for pdf_path in pdfs:
        output_filename = get_case_extract_filename(pdf_path)
        output_file = output_dir / output_filename

        had_existing = output_file.exists()
        if had_existing and not overwrite:
            results["skipped"] += 1
            results["details"].append(
                {"pdf": pdf_path.name, "status": "skipped", "reason": "output exists"}
            )
            continue

        try:
            result = pipeline.process_pdf(pdf_path, log_dir=log_dir)
            output_file.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
            status = "ok" if result.get("form_detection") != "not_detected" else "not_detected"
            results["processed"] += 1
            detail = {
                "pdf": pdf_path.name,
                "status": status,
                "output_file": output_filename,
            }
            if overwrite and had_existing:
                detail["overwritten"] = True
            results["details"].append(detail)
        except Exception as exc:
            results["failed"] += 1
            results["details"].append(
                {"pdf": pdf_path.name, "status": "failed", "error": str(exc)}
            )
            logger.exception("[pdf_parser_job] Failed %s: %s", pdf_path.name, exc)

    return results

def _run_batch_calculations(cfg: Dict[str, Any]) -> Dict[str, Any]:
    ensure_output_dir(KG_OUTPUT_DIR)
    case_files = discover_case_extracts(CASE_EXTRACTS_DIR)
    if not case_files:
        return {
            "message": f"No case_extract_*.json found in {CASE_EXTRACTS_DIR}",
            "processed": 0,
            "skipped": 0,
            "failed": 0,
            "total": 0,
        }

    neo_cfg = cfg.get("neo4j", {}) or {}
    data_dir = (cfg.get("data", {}) or {}).get("directory", "")
    if not data_dir:
        raise RuntimeError("DATA_DIR not configured; set DATA_DIR env or update configs/db_config.json")

    # Debug logging
    password_value = neo_cfg.get("password", "")
    logger.info(f"[batch_calc] Neo4j config - URI: {neo_cfg.get('uri')}, User: {neo_cfg.get('user')}, Password length: {len(password_value) if password_value else 0}")
    logger.info(f"[batch_calc] Environment overrides - NEO4J_PASSWORD: {'set' if os.getenv('NEO4J_PASSWORD') else 'not set'}")

    driver = None
    try:
        driver = connect_driver(neo_cfg)
        ensure_kg_initialized(driver, data_dir)
        engine = FeeEngine(driver, data_dir, api_key=None)
    except Exception as exc:
        if driver:
            try:
                driver.close()
            except Exception:
                pass
        raise RuntimeError(f"Failed to initialize FeeEngine: {exc}") from exc

    results = {
        "total": len(case_files),
        "processed": 0,
        "skipped": 0,
        "failed": 0,
        "details": [],
    }

    existing_outputs = {p.name for p in KG_OUTPUT_DIR.glob("*.json")}

    try:
        for case_path in case_files:
            case_id = derive_case_id(case_path)
            out_name = f"kg_calc_{case_id.replace('-', '_')}.json"
            out_path = KG_OUTPUT_DIR / out_name

            if out_name in existing_outputs or out_path.exists():
                results["skipped"] += 1
                results["details"].append({"case_id": case_id, "status": "skipped", "reason": "output exists"})
                continue

            try:
                case_json = json.loads(case_path.read_text(encoding="utf-8"))
                calc_result = calculate_for_case_extract(engine, case_json)
                output_payload = _build_kg_calc_payload(calc_result)

                out_path.write_text(json.dumps(output_payload, ensure_ascii=False, indent=2), encoding="utf-8")
                results["processed"] += 1
                results["details"].append({"case_id": case_id, "status": "ok", "output_file": out_name})
            except Exception as exc:
                results["failed"] += 1
                results["details"].append({"case_id": case_id, "status": "failed", "error": str(exc)})
                logger.exception("[batch_calc] Failed %s: %s", case_id, exc)
    finally:
        if driver:
            try:
                driver.close()
            except Exception:
                pass

    return results


@app.post("/v1/fees/calculate")
async def calculate_fees(payload: FeeCalculatorRequest):
    cfg = load_db_config()
    effective_designation = (payload.designation or "").strip()
    if payload.is_np_pa and not effective_designation:
        effective_designation = "NP/PA"

    request_payload = {
        "zip_code": payload.zip_code,
        "provider_type": payload.provider_type,
        "designation": effective_designation,
        "skip_ground_rules": payload.skip_ground_rules,
        "lines": [line.dict() for line in payload.lines],
    }

    try:
        calc_result = await asyncio.to_thread(calculate_custom_fee, cfg, request_payload)
        return JSONResponse(content=_build_kg_calc_payload(calc_result))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("[fee_calc] Failed: %s", exc)
        raise HTTPException(status_code=500, detail="Failed to calculate fees") from exc

@app.get("/v1/health")
def health():
    cfg = load_db_config()
    neo_cfg = cfg.get("neo4j", {}) or {}
    
    # Debug: Log what credentials are being used (without showing full password)
    password_value = neo_cfg.get("password", "")
    password_preview = f"{password_value[:4]}..." if password_value and len(password_value) > 4 else "not set"
    
    logger.info(f"[health] Neo4j config - URI: {neo_cfg.get('uri')}, User: {neo_cfg.get('user')}, Password length: {len(password_value) if password_value else 0}")
    
    # Test connection
    connection_status = "unknown"
    connection_error = None
    try:
        from scripts.batch_calc_case_extracts import connect_driver
        driver = connect_driver(neo_cfg)
        driver.verify_connectivity()
        connection_status = "connected"
        driver.close()
        logger.info("[health] Neo4j connection successful")
    except Exception as e:
        connection_status = "failed"
        connection_error = str(e)
        logger.error(f"[health] Neo4j connection failed: {e}", exc_info=True)
    
    return {
        "status": "ok",
        "app": APP_NAME,
        "version": APP_VERSION,
        "time": _now_iso(),
        "neo4j_uri": neo_cfg.get("uri"),
        "neo4j_user": neo_cfg.get("user"),
        "neo4j_password_length": len(password_value) if password_value else 0,
        "neo4j_password_preview": password_preview,
        "neo4j_connection": connection_status,
        "neo4j_error": connection_error,
        "env_override": {
            "NEO4J_URI": os.getenv("NEO4J_URI"),
            "NEO4J_USER": os.getenv("NEO4J_USER"),
            "NEO4J_PASSWORD_set": bool(os.getenv("NEO4J_PASSWORD")),
        },
        "case_extracts_dir": str(CASE_EXTRACTS_DIR),
        "kg_output_dir": str(KG_OUTPUT_DIR),
    }

@app.post("/v1/pdf/parse")
async def parse_pdf(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files are supported")

    cfg = load_db_config()
    api_key = (cfg.get("gemini", {}) or {}).get("api_key")
    if not api_key:
        raise HTTPException(status_code=500, detail="Gemini API key not configured")

    temp_path = TEMP_UPLOAD_DIR / f"{uuid4()}_{file.filename}"

    try:
        temp_path.write_bytes(await file.read())
        logger.info("[pdf_parse] Stored upload at %s", temp_path)
        result = await asyncio.to_thread(_process_pdf, api_key, temp_path, LOG_DIR)
        return JSONResponse(content=result)
    except Exception as exc:
        logger.exception("[pdf_parse] Failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"Failed to parse PDF: {exc}") from exc
    finally:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except Exception:
                pass

@app.post("/v1/pdf/batch")
async def batch_parse_pdfs(limit: int | None = None):
    cfg = load_db_config()
    api_key = (cfg.get("gemini", {}) or {}).get("api_key")
    if not api_key:
        raise HTTPException(status_code=500, detail="Gemini API key not configured")

    try:
        results = await asyncio.to_thread(
            _run_pdf_parser_job,
            api_key,
            PDF_INPUT_DIR,
            CASE_EXTRACTS_DIR,
            LOG_DIR,
            limit,
            False,
        )
        return JSONResponse(content=results)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("[pdf_batch] Failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc

@app.post("/v1/scripts/pdf-parser")
async def run_pdf_parser_script(
    limit: int | None = None,
    pdf_dir: str | None = None,
    output_dir: str | None = None,
    overwrite: bool = False,
):
    """Trigger scripts/pdf_parser_pipeline.py logic over a directory."""
    cfg = load_db_config()
    api_key = (cfg.get("gemini", {}) or {}).get("api_key")
    if not api_key:
        raise HTTPException(status_code=500, detail="Gemini API key not configured")

    target_pdf_dir = Path(pdf_dir) if pdf_dir else PDF_INPUT_DIR
    target_output_dir = Path(output_dir) if output_dir else CASE_EXTRACTS_DIR

    try:
        results = await asyncio.to_thread(
            _run_pdf_parser_job,
            api_key,
            target_pdf_dir,
            target_output_dir,
            LOG_DIR,
            limit,
            overwrite,
        )
        return JSONResponse(content=results)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("[pdf_parser_script] Failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc

@app.post("/v1/calc/batch")
async def run_batch_calculations():
    cfg = load_db_config()
    try:
        result = await asyncio.to_thread(_run_batch_calculations, cfg)
        return JSONResponse(content=result)
    except Exception as exc:
        logger.exception("[batch_calc] Failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc

@app.post("/v1/scripts/batch-calc")
async def run_batch_calc_script(data_dir: str | None = None):
    """Trigger scripts/batch_calc_case_extracts.py calculations."""
    cfg = load_db_config()
    if data_dir:
        cfg["data"] = (cfg.get("data") or {}) | {"directory": data_dir}
    try:
        result = await asyncio.to_thread(_run_batch_calculations, cfg)
        return JSONResponse(content=result)
    except Exception as exc:
        logger.exception("[batch_calc_script] Failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/v1/cases/instant-kg-calc")
async def calculate_case_kg(case_number: str = Body(..., embed=True)):
    """Run PDF parsing and fee calculation for a specific case number without writing files."""
    normalized_case_number = (case_number or "").strip()
    if not CASE_NUMBER_PATTERN.fullmatch(normalized_case_number):
        raise HTTPException(status_code=400, detail="case_number must be exactly 8 digits.")

    cfg = load_db_config()
    api_key = (cfg.get("gemini", {}) or {}).get("api_key")
    if not api_key:
        raise HTTPException(status_code=500, detail="Gemini API key not configured")

    try:
        pdf_path = _find_pdf_for_case_number(normalized_case_number)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    if not pdf_path:
        raise HTTPException(
            status_code=404,
            detail=f"No PDF found in {PDF_INPUT_DIR} for case_number {normalized_case_number}",
        )

    try:
        case_extract = await asyncio.to_thread(_process_pdf, api_key, pdf_path, LOG_DIR)
    except Exception as exc:
        logger.exception("[case_calc] PDF parsing failed for %s: %s", normalized_case_number, exc)
        raise HTTPException(status_code=500, detail=f"Failed to process PDF: {exc}") from exc

    if case_extract.get("form_detection") == "not_detected":
        raise HTTPException(
            status_code=422,
            detail="Form not detected in PDF; cannot generate knowledge graph calculation.",
        )

    data_dir = (cfg.get("data", {}) or {}).get("directory", "")
    if not data_dir:
        raise HTTPException(
            status_code=500,
            detail="DATA_DIR not configured; set DATA_DIR env or update configs/db_config.json",
        )

    neo_cfg = cfg.get("neo4j", {}) or {}
    driver = None
    try:
        driver = connect_driver(neo_cfg)
        ensure_kg_initialized(driver, data_dir)
        engine = FeeEngine(driver, data_dir, api_key=None)
        kg_payload = await asyncio.to_thread(_calculate_in_memory_kg_calc, engine, case_extract)
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("[case_calc] KG calculation failed for %s: %s", normalized_case_number, exc)
        raise HTTPException(status_code=500, detail=f"Failed to calculate KG: {exc}") from exc
    finally:
        if driver:
            try:
                driver.close()
            except Exception:
                pass

    response_body = {
        "case_number": normalized_case_number,
        "source_pdf": pdf_path.name,
        "kg_calc": kg_payload,
    }
    return JSONResponse(content=response_body)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main_asgi:app", host="0.0.0.0", port=8000)
