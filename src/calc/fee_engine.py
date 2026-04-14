import os
import pandas as pd
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Dict, Any

class FeeEngine:
    def __init__(self, driver, data_dir, api_key=None):
        self.driver = driver
        self._load_zip_data(data_dir)
        self.api_key = api_key
        
        # Initialize explanation generator if API key is provided
        self.explanation_generator = None
        if api_key:
            try:
                from .explanation_generator import ExplanationGenerator
                self.explanation_generator = ExplanationGenerator(api_key)
            except ImportError:
                print("Warning: Could not import explanation generator. Install langchain dependencies.")

    def _load_zip_data(self, data_dir):
        try:
            zip_df = pd.read_csv(os.path.join(data_dir, 'Zip_regions.csv'))
            zip_df.columns = [c.strip().lower().replace(' ', '_') for c in zip_df.columns]
            zip_df['zip_start'] = zip_df['zip_start'].astype(int)
            zip_df['zip_end'] = zip_df['zip_end'].astype(int)
            self.zip_ranges = zip_df.to_dict('records')
        except FileNotFoundError:
            raise FileNotFoundError(f"FATAL ERROR: Zip_regions.csv not found in '{data_dir}' directory.")

    def _get_region_from_zip(self, zip_code):
        try: z = int(str(zip_code).strip()[:5])
        except (ValueError, TypeError): return None
        for r in self.zip_ranges:
            if r['zip_start'] <= z <= r['zip_end']:
                return str(r.get('region')).strip()
        return None

    def _normalize_region_name(self, raw_region, session):
        for strategy in [raw_region, f"Region {raw_region}"]:
            res = session.run("MATCH (r:Region) WHERE r.name CONTAINS $r RETURN r.name AS name LIMIT 1", r=strategy).single()
            if res: return res['name']
        return None

    def get_fee(self, cpt_code, zip_code, provider_type, designation=""):
        raw_region = self._get_region_from_zip(zip_code)
        if not raw_region: return {"error": f"Region not found for ZIP code {zip_code}"}

        with self.driver.session(database="neo4j") as session:
            region_node_name = self._normalize_region_name(raw_region, session)
            if not region_node_name: return {"error": f"Region node not found for raw region '{raw_region}'."}

            # First, check which schedules this CPT code exists in
            schedules_result = session.run("""
                MATCH (p:Procedure {code: $cpt_code})-[:IN_SCHEDULE]->(sch:Schedule)
                RETURN collect(DISTINCT sch.name) AS schedules
            """, cpt_code=str(cpt_code)).single()

            if not schedules_result or not schedules_result['schedules']:
                return {"error": f"CPT code '{cpt_code}' not found in any fee schedule."}

            available_schedules = schedules_result['schedules']
            
            # If CPT exists in only one schedule, use that schedule
            if len(available_schedules) == 1:
                schedule_name = available_schedules[0]
            else:
                # CPT exists in multiple schedules - use provider_type to determine which one
                normalized_provider_type = provider_type.strip().lower()
                
                if normalized_provider_type in available_schedules:
                    schedule_name = normalized_provider_type
                else:
                    return {
                        "error": f"CPT code '{cpt_code}' exists in multiple schedules: {available_schedules}. "
                                f"Provider type '{provider_type}' does not match any of them. "
                                f"Please specify one of: {', '.join(available_schedules)}",
                        "available_schedules": available_schedules,
                        "requested_provider_type": provider_type
                    }

            # Now get the fee calculation with the determined schedule
            result = session.run("""
                MATCH (p:Procedure {code: $cpt_code})-[:BELONGS_TO]->(s:Sector)
                MATCH (p)-[:IN_SCHEDULE]->(sch:Schedule {name: $schedule_name})
                MATCH (r:Region {name: $region})-[cf:HAS_CONVERSION_FACTOR {schedule: sch.name}]->(s)
                RETURN p.rvu AS rvu, s.name AS sector, sch.name AS schedule,
                       cf.value AS conversion_factor, p.pc_tc_split AS pc_tc_split
            """, cpt_code=str(cpt_code), region=region_node_name, schedule_name=schedule_name).single()

        if not result: 
            return {"error": f"Could not find a fee for CPT '{cpt_code}' in region '{region_node_name}' under the '{schedule_name}' schedule."}
        
        rvu_val = result.get('rvu')
        if rvu_val is None: return {"error": f"RVU missing for CPT '{cpt_code}'."}
        
        rvu = Decimal(str(rvu_val)); cf = Decimal(str(result.get('conversion_factor', 0)))
        base_fee = (rvu * cf)
        final_fee = base_fee
        provenance = {
            "cpt_code": cpt_code, 
            "zip_code": zip_code, 
            "region": region_node_name, 
            "schedule": result['schedule'],
            "sector": result['sector'], 
            "rvu": float(rvu), 
            "conversion_factor": float(cf),
            "global_fee": float(base_fee.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)),
            "schedule_determination": {
                "available_schedules": available_schedules,
                "selected_schedule": schedule_name,
                "provider_type_requested": provider_type,
                "reason": "single_schedule" if len(available_schedules) == 1 else "provider_type_match"
            }
        }
        

        # Apply PA/NP 80% rule: PA and NP get 80%, exempt MD/DR/DC/DO designations (100%)
        # Handle all variations: MD, m.d, M.D., DO, D.O., NP, PA, etc.
        designation_normalized = ""
        if designation:
            # Remove periods and convert to lowercase for matching
            designation_normalized = designation.replace('.', '').lower().strip()
        
        # Exempt keywords: MD, DO, DC, DR, Doctor get 100% (no 80% reduction)
        exempt_keywords = ["md", "dr", "doctor", "dc", "do"]
        is_exempt = any(kw in designation_normalized for kw in exempt_keywords)
        
        # Apply 80% rule to PA, NP, and any other non-exempt designations
        if designation and not is_exempt:
            final_fee = base_fee * Decimal('0.8')
            provenance['modifier_applied'] = "PA/NP (80%)"
        provenance['calculated_fee'] = float(final_fee.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))

        pc_tc_split = result['pc_tc_split']

        if pc_tc_split:
            try:
                pc_percent, tc_percent = map(int, pc_tc_split.split('/'))
                pc_fee = base_fee * (Decimal(pc_percent) / Decimal(100))
                tc_fee = base_fee * (Decimal(tc_percent) / Decimal(100))
                provenance['pc_tc_split'] = pc_tc_split
                provenance['professional_component_fee'] = float(pc_fee.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
                provenance['technical_component_fee'] = float(tc_fee.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
            except (ValueError, IndexError):
                provenance['error'] = f"Invalid PC/TC split format: '{pc_tc_split}'"
        return provenance
    
    def _apply_ground_rules(self, calculation_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Apply surgery ground rule (SGR5) and radiology ground rule (RGR3)"""
        
        # Separate results by sector
        surgery_results = []
        radiology_results = []
        other_results = []
        
        for result in calculation_results:
            if "error" in result:
                other_results.append(result)
                continue
                
            sector = result.get('sector', '').lower()
            
            if 'surgery' in sector or 'surgical' in sector:
                surgery_results.append(result)
            elif 'radiology' in sector or 'imaging' in sector:
                radiology_results.append(result)
            else:
                other_results.append(result)
        
        # Apply surgery ground rule (SGR5): 100% highest, 50% rest
        if len(surgery_results) > 1:
            surgery_results = self._apply_surgery_ground_rule(surgery_results)
        
        # Apply radiology ground rule (RGR3): 100% highest, 75% rest  
        if len(radiology_results) > 1:
            radiology_results = self._apply_radiology_ground_rule(radiology_results)
        
        # Combine all results
        return surgery_results + radiology_results + other_results
    
    def _apply_surgery_ground_rule(self, surgery_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Apply SGR5: 100% highest value, 50% for the rest"""
        
        # Find the highest calculated fee
        max_fee = max(result.get('calculated_fee', 0) for result in surgery_results)
        
        # Find all results with the maximum fee
        max_fee_results = [result for result in surgery_results if result.get('calculated_fee', 0) == max_fee]
        
        # Keep only the first one at 100%, apply 50% to the rest
        first_max_applied = False
        
        for result in surgery_results:
            current_fee = result.get('calculated_fee', 0)
            
            if current_fee == max_fee:
                if not first_max_applied:
                    # Keep the first instance at 100% (already calculated)
                    if 'modifier_applied' not in result or not result['modifier_applied']:
                        result['modifier_applied'] = 'SGR5'
                    elif 'SGR5' not in result['modifier_applied']:
                        result['modifier_applied'] += ', SGR5'
                    first_max_applied = True
                else:
                    # Apply 50% reduction to subsequent instances of the same max fee
                    original_fee = result.get('calculated_fee', 0)
                    result['calculated_fee'] = round(original_fee * 0.5, 2)
                    
                    if 'modifier_applied' not in result or not result['modifier_applied']:
                        result['modifier_applied'] = 'SGR5 (50%)'
                    elif 'SGR5' not in result['modifier_applied']:
                        result['modifier_applied'] += ', SGR5 (50%)'
            else:
                # Apply 50% reduction to all non-max fees
                original_fee = result.get('calculated_fee', 0)
                result['calculated_fee'] = round(original_fee * 0.5, 2)
                
                if 'modifier_applied' not in result or not result['modifier_applied']:
                    result['modifier_applied'] = 'SGR5 (50%)'
                elif 'SGR5' not in result['modifier_applied']:
                    result['modifier_applied'] += ', SGR5 (50%)'
        
        return surgery_results
    
    def _apply_radiology_ground_rule(self, radiology_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Apply RGR3: 100% highest value, 75% for the rest"""
        
        # Find the highest calculated fee
        max_fee = max(result.get('calculated_fee', 0) for result in radiology_results)
        
        # Keep only the first one at 100%, apply 75% to the rest
        first_max_applied = False
        
        for result in radiology_results:
            current_fee = result.get('calculated_fee', 0)
            
            if current_fee == max_fee:
                if not first_max_applied:
                    # Keep the first instance at 100% (already calculated)
                    if 'modifier_applied' not in result or not result['modifier_applied']:
                        result['modifier_applied'] = 'RGR3'
                    elif 'RGR3' not in result['modifier_applied']:
                        result['modifier_applied'] += ', RGR3'
                    first_max_applied = True
                else:
                    # Apply 75% reduction to subsequent instances of the same max fee
                    original_fee = result.get('calculated_fee', 0)
                    result['calculated_fee'] = round(original_fee * 0.75, 2)
                    
                    if 'modifier_applied' not in result or not result['modifier_applied']:
                        result['modifier_applied'] = 'RGR3 (75%)'
                    elif 'RGR3' not in result['modifier_applied']:
                        result['modifier_applied'] += ', RGR3 (75%)'
            else:
                # Apply 75% reduction to all non-max fees
                original_fee = result.get('calculated_fee', 0)
                result['calculated_fee'] = round(original_fee * 0.75, 2)
                
                if 'modifier_applied' not in result or not result['modifier_applied']:
                    result['modifier_applied'] = 'RGR3 (75%)'
                elif 'RGR3' not in result['modifier_applied']:
                    result['modifier_applied'] += ', RGR3 (75%)'
        
        return radiology_results
    
    def calculate_fees_with_explanation(self, line_items: List[Dict[str, Any]], zip_code: str, provider_type: str, designation: str = "", skip_ground_rules: bool = False) -> Dict[str, Any]:
        """Calculate fees for multiple line items and generate legal explanation"""
        
        # Calculate fees for each line item
        calculation_results = []
        total_amount = 0
        region = None
        
        for line_item in line_items:
            cpt_code = line_item.get('code', '')
            units = line_item.get('units', 1)
            billed_amount_raw = line_item.get('billed_amount', 0)

            # Normalize billed_amount to float, handling different zero representations: 0, 0.0, "0", None
            try:
                billed_amount = float(billed_amount_raw) if billed_amount_raw not in (None, '') else 0.0
            except (ValueError, TypeError):
                billed_amount = 0.0

            # For J-codes that don't exist in fee schedule, use billed_amount
            if cpt_code.startswith('J'):
                result = self.get_fee(cpt_code, zip_code, provider_type, designation)
                if "error" in result:
                    # Use billed_amount for J-codes not in fee schedule (already includes units)
                    rounded_billed_amount = round(billed_amount, 2)
                    j_result = {
                        'cpt_code': cpt_code,
                        'calculated_fee': rounded_billed_amount,
                        'modifier_applied': 'J-code billed amount',
                        'units': units,
                        'total_fee_for_units': rounded_billed_amount,
                        'explanation': f"J-code {cpt_code} using billed amount: ${rounded_billed_amount}"
                    }
                    calculation_results.append(j_result)
                    total_amount += rounded_billed_amount
                    continue
            
            result = self.get_fee(cpt_code, zip_code, provider_type, designation)
            
            if "error" not in result:
                # Store region from first successful calculation
                if region is None:
                    region = result.get('region', 'Unknown')
                
                # Add line item context and round calculated fees
                result['cpt_code'] = cpt_code
                result['units'] = units
                rounded_calculated_fee = round(result.get('calculated_fee', 0), 2)
                result['calculated_fee'] = rounded_calculated_fee
                result['total_fee_for_units'] = round(rounded_calculated_fee * units, 2)
                
                calculation_results.append(result)
                total_amount += round(rounded_calculated_fee * units, 2)
            else:
                # Use billed_amount when code is not found in fee schedule
                rounded_billed_amount = round(billed_amount, 2)
                fallback_result = {
                    'cpt_code': cpt_code,
                    'calculated_fee': rounded_billed_amount,
                    'modifier_applied': 'Billed amount (not in fee schedule)',
                    'units': units,
                    'total_fee_for_units': rounded_billed_amount,
                    'explanation': f"Code {cpt_code} not in fee schedule, using billed amount: ${rounded_billed_amount}"
                }
                calculation_results.append(fallback_result)
                total_amount += rounded_billed_amount
        
        # Apply ground rules (SGR5 for surgery, RGR3 for radiology) only if not skipped
        if not skip_ground_rules:
            calculation_results = self._apply_ground_rules(calculation_results)
        
        # Recalculate total amount after applying ground rules
        # Note: J-codes already have their total amount in calculated_fee, don't multiply by units
        total_amount = 0
        for result in calculation_results:
            if "error" not in result:
                if result.get('modifier_applied') == 'J-code billed amount':
                    # J-codes: calculated_fee already includes units, don't multiply again
                    total_amount += round(result.get('calculated_fee', 0), 2)
                else:
                    # Regular CPT codes: multiply by units
                    total_amount += round(result.get('calculated_fee', 0) * result.get('units', 1), 2)
        
        # Generate explanation if generator is available
        explanation = None
        legal_explanation_text = None
        
        if self.explanation_generator and region and calculation_results:
            try:
                # Filter out error items for explanation
                valid_results = [r for r in calculation_results if "error" not in r]
                
                if valid_results:
                    explanation = self.explanation_generator.generate_explanation(region, valid_results)
                    legal_explanation_text = self.explanation_generator.format_for_legal_document(explanation)
            except Exception as e:
                print(f"Warning: Could not generate explanation: {e}")
        
        return {
            "calculation_results": calculation_results,
            "total_calculated_amount": round(total_amount, 2),
            "region": region,
            "provider_type": provider_type,
            "designation": designation,
            "explanation": explanation,
            "legal_explanation": legal_explanation_text
        }