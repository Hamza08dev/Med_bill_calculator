#!/usr/bin/env python3
"""
Generate legal explanations for fee calculations using LangChain Structured Output.
Usage: python scripts/generate_legal_explanation.py <last_4_digits_of_case_id>
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

def display_legal_explanation(explanation_text):
    """Display the legal explanation in a formatted way"""
    print("\n" + "="*80)
    print("LEGAL FEE CALCULATION EXPLANATION")
    print("="*80)
    print(explanation_text)
    print("="*80)

def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/generate_legal_explanation.py <last_4_digits_of_case_id>")
        print("\nThis script generates a lawyer-ready explanation of fee calculations.")
        sys.exit(1)
    
    four_digit_code = sys.argv[1]
    
    try:
        # Find case directory
        base_cases_dir = os.path.join(os.path.dirname(__file__), '..', 'cases')
        case_id_prefix = None
        all_case_dirs = [d for d in os.listdir(base_cases_dir) if os.path.isdir(os.path.join(base_cases_dir, d))]
        
        for case_dir in all_case_dirs:
            if case_dir.endswith(f"-{four_digit_code}"):
                case_id_prefix = case_dir
                break
        
        if not case_id_prefix:
            print(f"Error: No case directory found ending with '{four_digit_code}'.")
            sys.exit(1)

        # Load case data
        claim_file_path = os.path.join(base_cases_dir, case_id_prefix, "derived", "case_extract.json")
        with open(claim_file_path, 'r') as f:
            claim_data = json.load(f)

        # Load configuration and connect to Neo4j
        config = load_config()
        
        print("🔌 Connecting to Neo4j...")
        driver = GraphDatabase.driver(
            config['neo4j']['uri'], 
            auth=(config['neo4j']['user'], config['neo4j']['password'])
        )
        driver.verify_connectivity()
        print("✅ Connected to Neo4j successfully!")
        
        # Initialize fee engine with explanation capability
        engine = FeeEngine(driver, config['data']['directory'], config['gemini']['api_key'])
        
        print(f"\n📋 Processing Case: {case_id_prefix}")
        
        # Extract claim details
        provider_type = claim_data['provider_type']
        zip_code = claim_data['service_region_zip']
        designation = claim_data.get('designation', "")
        line_items = claim_data['lines']
        
        print(f"📍 Region ZIP: {zip_code}")
        print(f"🏥 Provider Type: {provider_type}")
        print(f"👨‍⚕️ Designation: {designation}")
        print(f"📊 Line Items: {len(line_items)}")
        
        # Calculate fees with explanation
        print("\n🧮 Calculating fees and generating explanation...")
        calculation_result = engine.calculate_fees_with_explanation(line_items, zip_code, provider_type, designation)
        
        # Display results
        if calculation_result.get("legal_explanation"):
            display_legal_explanation(calculation_result["legal_explanation"])
            
            # Save to file
            explanation_file = os.path.join(base_cases_dir, case_id_prefix, "derived", "legal_explanation.txt")
            with open(explanation_file, 'w') as f:
                f.write(calculation_result["legal_explanation"])
            
            print(f"\n💾 Legal explanation saved to: {explanation_file}")
            
        else:
            print("\n❌ Could not generate legal explanation.")
            print("Possible reasons:")
            print("- Google Gemini API key not configured")
            print("- LangChain dependencies not installed")
            print("- No valid calculations found")
            
            # Show basic calculation results
            print(f"\n📊 Basic Calculation Results:")
            print(f"Total Amount: ${calculation_result['total_calculated_amount']:.2f}")
            print(f"Region: {calculation_result['region']}")
            
            for result in calculation_result["calculation_results"]:
                if "error" not in result:
                    print(f"  CPT {result['cpt_code']}: ${result.get('calculated_fee', 0):.2f}")
                else:
                    print(f"  CPT {result['cpt_code']}: ERROR - {result['error']}")
        
        # Close connection
        driver.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
