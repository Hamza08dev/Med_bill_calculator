#!/usr/bin/env python3
import os
import sys
import json
from pathlib import Path
from typing import Dict, Any, List

# Ensure project root on path
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Lazy imports to avoid hard failures when not needed
try:
    from neo4j import GraphDatabase  # type: ignore
    from src.calc.fee_engine import FeeEngine  # type: ignore
    from src.kg.graph_builder import setup_knowledge_graph  # type: ignore
except Exception:
    GraphDatabase = None  # type: ignore
    FeeEngine = None  # type: ignore
    setup_knowledge_graph = None  # type: ignore

CONFIG_PATH = PROJECT_ROOT / "configs" / "db_config.json"
CASE_EXTRACTS_DIR = PROJECT_ROOT / "case_extracts"
OUTPUT_DIR = PROJECT_ROOT / "kg_calc"


def load_db_config(config_path: Path) -> Dict[str, Any]:
    if not config_path.exists():
        raise FileNotFoundError(f"Config not found at: {config_path}")
    with config_path.open("r", encoding="utf-8") as f:
        cfg = json.load(f)
    # Allow env overrides similar to main_asgi.load_db_config
    neo = cfg.get("neo4j", {}) or {}
    neo["uri"] = os.getenv("NEO4J_URI", neo.get("uri"))
    neo["user"] = os.getenv("NEO4J_USER", neo.get("user"))
    neo["password"] = os.getenv("NEO4J_PASSWORD", neo.get("password"))
    cfg["neo4j"] = neo

    data = cfg.get("data", {}) or {}
    data["directory"] = os.getenv("DATA_DIR", data.get("directory"))
    cfg["data"] = data

    gem = cfg.get("gemini", {}) or {}
    gem["api_key"] = os.getenv("GEMINI_API_KEY", gem.get("api_key"))
    cfg["gemini"] = gem
    return cfg


def discover_case_extracts(root: Path) -> List[Path]:
    if not root.exists():
        return []
    return sorted([p for p in root.glob("case_extract_*.json") if p.is_file()])


def derive_case_id(file_path: Path) -> str:
    # Support both old and new formats:
    #   - case_extract_1369_0325.json -> 1369-0325
    #   - case_extract_1387_9025_8702104.json -> 1387-9025-8702104
    stem = file_path.stem
    try:
        # Take everything after the fixed prefix and convert underscores to dashes
        suffix_after_prefix = stem.split("case_extract_", 1)[1]
        return suffix_after_prefix.replace("_", "-")
    except Exception:
        # Fallback: replace underscores in entire stem
        return stem.replace("_", "-")


def ensure_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def connect_driver(neo_cfg: Dict[str, Any]):
    if GraphDatabase is None:
        raise RuntimeError("neo4j driver not available. Ensure neo4j package is installed.")
    uri = neo_cfg.get("uri")
    user = neo_cfg.get("user")
    pwd = neo_cfg.get("password")
    if not uri or not user:
        raise RuntimeError("NEO4J config incomplete: uri/user required")
    if not pwd:
        raise RuntimeError("NEO4J config incomplete: password is required")
    
    # Log connection attempt (without logging password)
    print(f"[connect_driver] Connecting to Neo4j at {uri} as user '{user}' (password length: {len(pwd)} chars)")
    
    driver = GraphDatabase.driver(uri, auth=(user, pwd))
    try:
        driver.verify_connectivity()
        print("[connect_driver] Connection verified successfully")
    except Exception as e:
        driver.close()
        error_msg = f"Failed to connect to Neo4j at {uri} as user '{user}': {str(e)}"
        print(f"[connect_driver] ERROR: {error_msg}")
        raise RuntimeError(error_msg) from e
    return driver


def check_kg_exists(driver) -> bool:
    """Check if knowledge graph has been initialized by looking for Procedure nodes."""
    try:
        with driver.session(database="neo4j") as session:
            result = session.run("MATCH (p:Procedure) RETURN count(p) AS count LIMIT 1").single()
            count = result.get("count", 0) if result else 0
            return count > 0
    except Exception:
        # If query fails, assume KG doesn't exist
        return False


def ensure_kg_initialized(driver, data_dir: str) -> None:
    """Ensure knowledge graph is initialized, creating it if necessary."""
    if setup_knowledge_graph is None:
        raise RuntimeError("setup_knowledge_graph not available. Ensure all dependencies are installed.")
    
    if not check_kg_exists(driver):
        print("[batch] Knowledge graph not found. Initializing...")
        # Resolve data_dir path (might be relative to PROJECT_ROOT)
        if not os.path.isabs(data_dir):
            full_data_dir = PROJECT_ROOT / data_dir
        else:
            full_data_dir = Path(data_dir)
        
        if not full_data_dir.exists():
            raise FileNotFoundError(f"Data directory not found: {full_data_dir}")
        
        setup_knowledge_graph(driver, str(full_data_dir))
        print("[batch] Knowledge graph initialized successfully.")
    else:
        print("[batch] Knowledge graph already exists. Skipping initialization.")


def calculate_for_case_extract(engine: "FeeEngine", case_json: Dict[str, Any]) -> Dict[str, Any]:
    zip_code = str(case_json.get("service_region_zip", "")).strip()
    provider_type = str(case_json.get("provider_type", "")).strip()
    designation = str(case_json.get("designation", "")).strip()
    line_items = case_json.get("lines", [])
    return engine.calculate_fees_with_explanation(line_items, zip_code, provider_type, designation)


def main() -> int:
    print("[batch] Starting KG calculations from case_extracts …")
    cfg = load_db_config(CONFIG_PATH)
    neo_cfg = cfg.get("neo4j", {})
    data_dir = (cfg.get("data", {}) or {}).get("directory", "")
    api_key = (cfg.get("gemini", {}) or {}).get("api_key")

    if not data_dir:
        print("[batch][fatal] DATA_DIR not configured in configs/db_config.json or env.")
        return 2

    case_files = discover_case_extracts(CASE_EXTRACTS_DIR)
    if not case_files:
        print(f"[batch] No case_extract_*.json found in {CASE_EXTRACTS_DIR}")
        return 0

    ensure_output_dir(OUTPUT_DIR)

    # Pre-scan existing outputs for idempotency
    existing_outputs = {p.name for p in OUTPUT_DIR.glob("*.json")}

    driver = None
    try:
        driver = connect_driver(neo_cfg)
        # Ensure knowledge graph is initialized
        ensure_kg_initialized(driver, data_dir)
        # Pass None for api_key to skip explanation generation
        engine = FeeEngine(driver, data_dir, api_key=None)
    except Exception as e:
        print(f"[batch][fatal] Could not initialize FeeEngine/driver: {e}")
        return 3

    processed = skipped = failed = 0

    for case_path in case_files:
        case_id = derive_case_id(case_path)
        out_name = f"kg_calc_{case_id.replace('-', '_')}.json"
        out_path = OUTPUT_DIR / out_name

        if out_name in existing_outputs or out_path.exists():
            print(f"[batch][skip] {case_id}: output exists -> {out_name}")
            skipped += 1
            continue

        try:
            with case_path.open("r", encoding="utf-8") as f:
                case_json = json.load(f)
        except Exception as e:
            print(f"[batch][fail] {case_id}: cannot read JSON: {e}")
            failed += 1
            continue

        try:
            result = calculate_for_case_extract(engine, case_json)
            # Remove high-level explanations if any present
            if isinstance(result, dict):
                result.pop("explanation", None)
                result.pop("legal_explanation", None)

            # Build output to exactly match the required kg_calc format
            calc_results = result.get("calculation_results", []) if isinstance(result, dict) else []
            line_results: List[Dict[str, Any]] = []
            for item in calc_results:
                if isinstance(item, dict) and "error" not in item:
                    cpt = item.get("cpt_code")
                    fee_val = float(item.get("calculated_fee", 0)) if item.get("calculated_fee") is not None else 0.0
                    line_results.append({
                        "cpt_code": cpt,
                        "calculated_fee": round(fee_val, 2),
                        "modifier_applied": item.get("modifier_applied", ""),
                        "rvu": item.get("rvu"),
                        "conversion_factor": item.get("conversion_factor"),
                        "schedule": item.get("schedule"),
                        "units": item.get("units", 1),
                        "explanation": f"Fee calculated for CPT {cpt}: ${round(fee_val, 2):.2f}"
                    })

            out_obj = {
                "total_calculated_amount": round(float(result.get("total_calculated_amount", 0)), 2),
                "line_results": line_results,
                "region": result.get("region"),
                "provider_type": result.get("provider_type"),
                "designation": result.get("designation", "")
            }

            with out_path.open("w", encoding="utf-8") as f:
                json.dump(out_obj, f, ensure_ascii=False, indent=2)
            print(f"[batch][ok] {case_id}: wrote {out_name}")
            processed += 1
        except Exception as e:
            print(f"[batch][fail] {case_id}: calculation error: {e}")
            failed += 1

    if driver is not None:
        try:
            driver.close()
        except Exception:
            pass

    print(f"[batch] Done. processed={processed} skipped={skipped} failed={failed}")
    return 0 if failed == 0 else 4


if __name__ == "__main__":
    raise SystemExit(main())
