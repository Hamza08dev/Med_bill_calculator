from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

from scripts.batch_calc_case_extracts import connect_driver, ensure_kg_initialized
from src.calc.fee_engine import FeeEngine

PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass
class CalculatorLine:
    code: str
    units: int = 1
    billed_amount: float = 0.0


def _normalize_lines(raw_lines: List[Dict[str, Any]]) -> List[CalculatorLine]:
    """Coerce incoming payload lines into FeeEngine friendly structures."""
    normalized: List[CalculatorLine] = []
    for idx, line in enumerate(raw_lines or []):
        code = str(line.get("code", "")).strip()
        if not code:
            continue

        try:
            units = int(line.get("units") or 1)
        except (TypeError, ValueError):
            units = 1
        units = max(units, 1)

        billed_raw = line.get("billed_amount", 0)
        billed_amount = 0.0
        if billed_raw not in (None, "", False):
            try:
                billed_amount = float(billed_raw)
            except (TypeError, ValueError):
                billed_amount = 0.0

        normalized.append(CalculatorLine(code=code, units=units, billed_amount=billed_amount))

    if not normalized:
        raise ValueError("At least one CPT line with a code is required.")

    return normalized


def _resolve_data_dir(data_dir: str | None) -> str:
    if not data_dir:
        raise RuntimeError(
            "DATA_DIR not configured; set DATA_DIR env variable or configs/db_config.json."
        )

    if os.path.isabs(data_dir):
        resolved = Path(data_dir)
    else:
        resolved = PROJECT_ROOT / data_dir

    if not resolved.exists():
        raise FileNotFoundError(f"Data directory not found: {resolved}")

    return str(resolved)


def calculate_custom_fee(cfg: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    """Run FeeEngine for an ad-hoc calculator request."""
    neo_cfg = cfg.get("neo4j", {}) or {}
    data_dir = _resolve_data_dir((cfg.get("data", {}) or {}).get("directory"))

    normalized_lines = _normalize_lines(payload.get("lines") or [])
    zip_code = str(payload.get("zip_code", "")).strip()
    if not zip_code:
        raise ValueError("zip_code is required.")

    provider_type = str(payload.get("provider_type", "")).strip().lower()
    if provider_type not in {"medical", "podiatry", "chiropractic"}:
        raise ValueError("provider_type must be medical, podiatry, or chiropractic.")

    designation = str(payload.get("designation") or "").strip()
    skip_ground_rules = bool(payload.get("skip_ground_rules", False))

    driver = None
    try:
        driver = connect_driver(neo_cfg)
        ensure_kg_initialized(driver, data_dir)
        engine = FeeEngine(driver, data_dir, api_key=None)
        fee_result = engine.calculate_fees_with_explanation(
            [line.__dict__ for line in normalized_lines],
            zip_code,
            provider_type,
            designation,
            skip_ground_rules=skip_ground_rules,
        )
        return fee_result
    finally:
        if driver:
            try:
                driver.close()
            except Exception:
                pass

