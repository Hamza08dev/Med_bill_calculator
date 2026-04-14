import json
import os
import sys
from neo4j import GraphDatabase

# Add parent directory to path to import modules
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.kg.graph_builder import setup_knowledge_graph
from src.calc.fee_engine import FeeEngine
# Removed unused LangChain imports - using simple JSON formatting instead

def load_config(path='configs/db_config.json'):
    config_path = os.path.join(os.path.dirname(__file__), '..', path)
    with open(config_path, 'r') as f:
        return json.load(f)

if __name__ == '__main__':
    try:
        if len(sys.argv) < 2:
            print("Usage: python calc_runner.py <last_4_digits_of_case_id>")
            sys.exit(1)
        
        four_digit_code = sys.argv[1]
        
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

        claim_file_path = os.path.join(base_cases_dir, case_id_prefix, "derived", "case_extract.json")
        with open(claim_file_path, 'r') as f:
            claim_data = json.load(f)

        config = load_config()
        NEO4J_URI = config['neo4j']['uri']
        NEO4J_USER = config['neo4j']['user']
        NEO4J_PASSWORD = config['neo4j']['password']
        GOOGLE_API_KEY = config['gemini']['api_key']
        DATA_DIR = config['data']['directory']
        # Temporarily disable LLM formatting to test core functionality
        # output_parser = PydanticOutputParser(pydantic_object=FeeCalculationResult)
        # format_instructions = output_parser.get_format_instructions()
        
        # llm = GooglePalm(google_api_key=config['gemini']['api_key'])
        # prompt = PromptTemplate(
        #     template="Format the following fee calculation result into a JSON object.\n{format_instructions}\nCalculation Result:\n{result}",
        #     input_variables=["result"],
        #     partial_variables={"format_instructions": format_instructions}
        # )
        # chain = prompt | llm | output_parser

        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        driver.verify_connectivity()
        
        setup_knowledge_graph(driver, DATA_DIR) 
        
        # Initialize fee engine with API key for explanation generation
        engine = FeeEngine(driver, DATA_DIR, GOOGLE_API_KEY)
        
        print(f"\n--- Processing Case: {case_id_prefix} (File: {os.path.basename(claim_file_path)}) ---")
        
        # Extract common claim details
        provider_type = claim_data['provider_type']
        zip_code = claim_data['service_region_zip']
        designation = claim_data.get('designation', "")
        line_items = claim_data['lines']
        
        # Use the new explanation-enabled calculation method
        calculation_result = engine.calculate_fees_with_explanation(line_items, zip_code, provider_type, designation)
        
        # Format output for compatibility with existing structure
        final_output = {
            "total_calculated_amount": calculation_result["total_calculated_amount"],
            "line_results": [],
            "region": calculation_result["region"],
            "provider_type": calculation_result["provider_type"],
            "designation": calculation_result["designation"]
        }
        
        for result in calculation_result["calculation_results"]:
            if "error" not in result:
                formatted_line = {
                    "cpt_code": result["cpt_code"],
                    "calculated_fee": result.get('calculated_fee', 0),
                    "modifier_applied": result.get('modifier_applied', ''),
                    "rvu": result.get('rvu'),
                    "conversion_factor": result.get('conversion_factor'),
                    "schedule": result.get('schedule'),
                    "units": result.get('units', 1),
                    "explanation": f"Fee calculated for CPT {result['cpt_code']}: ${result.get('calculated_fee', 0)}"
                }
                final_output["line_results"].append(formatted_line)
            else:
                final_output["line_results"].append({"cpt_code": result["cpt_code"], "error": result["error"]})
        
        # Add legal explanation if available
        if calculation_result.get("legal_explanation"):
            final_output["legal_explanation"] = calculation_result["legal_explanation"]
        
        output_path = os.path.join(base_cases_dir, case_id_prefix, "derived", "kg_calc.json")
        with open(output_path, 'w') as f:
            json.dump(final_output, f, indent=2)
            
        print(f"\nSUCCESS: Calculation complete. Output saved to: {output_path}")

        # --- FIX: Add driver.close() ---
        driver.close()
        
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")