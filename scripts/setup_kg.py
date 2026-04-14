#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path
from neo4j import GraphDatabase

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.kg.graph_builder import setup_knowledge_graph


def load_config():
    config_path = PROJECT_ROOT / "configs" / "db_config.json"
    with config_path.open("r") as f:
        return json.load(f)


def main():
    print("[KG] Loading configuration...")
    cfg = load_config()
    
    neo_cfg = cfg.get("neo4j", {})
    data_dir = (cfg.get("data", {}) or {}).get("directory", "")
    
    if not data_dir:
        print("[KG][ERROR] data.directory not configured in db_config.json")
        return 1
    
    full_data_dir = PROJECT_ROOT / data_dir
    if not full_data_dir.exists():
        print(f"[KG][ERROR] Data directory not found: {full_data_dir}")
        return 2
    
    print(f"[KG] Connecting to Neo4j at {neo_cfg.get('uri')}...")
    driver = GraphDatabase.driver(
        neo_cfg["uri"],
        auth=(neo_cfg["user"], neo_cfg["password"])
    )
    
    try:
        driver.verify_connectivity()
        print("[KG] Connected. Building knowledge graph...")
        setup_knowledge_graph(driver, str(full_data_dir))
        print("[KG] Knowledge graph setup complete!")
        return 0
    except Exception as e:
        print(f"[KG][ERROR] Failed to setup KG: {e}")
        return 3
    finally:
        driver.close()


if __name__ == "__main__":
    raise SystemExit(main())

