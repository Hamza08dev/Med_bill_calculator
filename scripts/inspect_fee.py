#!/usr/bin/env python3
"""
Script to inspect fee calculation details including RVU and all components.
Usage: python scripts/inspect_fee.py <cpt_code> <zip_code> <provider_type> [designation]
"""

import json
import os
import sys
from neo4j import GraphDatabase

# Add parent directory to path to import modules
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.calc.fee_engine import FeeEngine

def load_config(path='configs/db_config.json'):
    config_path = os.path.join(os.path.dirname(__file__), '..', path)
    with open(config_path, 'r') as f:
        return json.load(f)

def print_fee_details(result, cpt_code, zip_code, provider_type, designation=""):
    """Print detailed fee calculation information"""
    print(f"\n{'='*60}")
    print(f"FEE CALCULATION INSPECTION")
    print(f"{'='*60}")
    print(f"CPT Code: {cpt_code}")
    print(f"ZIP Code: {zip_code}")
    print(f"Provider Type: {provider_type}")
    print(f"Designation: {designation if designation else 'None'}")
    print(f"{'='*60}")
    
    if "error" in result:
        print(f"❌ ERROR: {result['error']}")
        return
    
    print(f"✅ CALCULATION SUCCESSFUL")
    print(f"{'='*60}")
    
    # Schedule determination details
    if result.get('schedule_determination'):
        sched_det = result['schedule_determination']
        print(f"🎯 SCHEDULE DETERMINATION:")
        print(f"   • Available Schedules: {sched_det.get('available_schedules', 'N/A')}")
        print(f"   • Selected Schedule: {sched_det.get('selected_schedule', 'N/A')}")
        print(f"   • Provider Type Requested: {sched_det.get('provider_type_requested', 'N/A')}")
        print(f"   • Selection Reason: {sched_det.get('reason', 'N/A')}")
    
    # Core calculation details
    print(f"\n📊 CORE VALUES:")
    print(f"   • RVU (Relative Value Unit): {result.get('rvu', 'N/A')}")
    print(f"   • Conversion Factor: {result.get('conversion_factor', 'N/A')}")
    print(f"   • Region: {result.get('region', 'N/A')}")
    print(f"   • Sector: {result.get('sector', 'N/A')}")
    print(f"   • Schedule: {result.get('schedule', 'N/A')}")
    
    print(f"\n💰 FEE BREAKDOWN:")
    print(f"   • Base Fee (RVU × CF): ${result.get('global_fee', 'N/A')}")
    
    if result.get('calculated_fee') != result.get('global_fee'):
        print(f"   • Final Fee (after modifiers): ${result.get('calculated_fee', 'N/A')}")
        if result.get('modifier_applied'):
            print(f"   • Modifier Applied: {result.get('modifier_applied')}")
    
    # PC/TC Split details
    if result.get('pc_tc_split'):
        print(f"\n🔀 PC/TC SPLIT:")
        print(f"   • Split Ratio: {result.get('pc_tc_split')}")
        print(f"   • Professional Component: ${result.get('professional_component_fee', 'N/A')}")
        print(f"   • Technical Component: ${result.get('technical_component_fee', 'N/A')}")
    
    print(f"\n📋 FULL JSON OUTPUT:")
    print(json.dumps(result, indent=2))

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print("Usage: python scripts/inspect_fee.py <cpt_code> <zip_code> <provider_type> [designation]")
        print("\nExamples:")
        print("  python scripts/inspect_fee.py 99214 11795 medical")
        print("  python scripts/inspect_fee.py 99214 11795 medical md")
        print("  python scripts/inspect_fee.py 99214 11795 chiropractic")
        sys.exit(1)
    
    cpt_code = sys.argv[1]
    zip_code = sys.argv[2]
    provider_type = sys.argv[3]
    designation = sys.argv[4] if len(sys.argv) > 4 else ""
    
    try:
        # Load configuration and connect to Neo4j
        config = load_config()
        
        print("🔌 Connecting to Neo4j...")
        driver = GraphDatabase.driver(
            config['neo4j']['uri'], 
            auth=(config['neo4j']['user'], config['neo4j']['password'])
        )
        driver.verify_connectivity()
        print("✅ Connected to Neo4j successfully!")
        
        # Initialize fee engine
        engine = FeeEngine(driver, config['data']['directory'])
        
        # Get fee calculation with full details
        result = engine.get_fee(cpt_code, zip_code, provider_type, designation)
        
        # Print detailed results
        print_fee_details(result, cpt_code, zip_code, provider_type, designation)
        
        # Close connection
        driver.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
