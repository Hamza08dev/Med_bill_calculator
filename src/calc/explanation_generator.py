"""
LangChain Structured Output for Fee Calculation Explanations
Generates precise, lawyer-ready explanations for fee calculations.
"""

from pydantic import BaseModel, Field
from typing import List, Optional
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.output_parsers import PydanticOutputParser
from langchain.prompts import PromptTemplate
import json

class LineItemExplanation(BaseModel):
    """Explanation for a single CPT code line item"""
    cpt_code: str = Field(description="The CPT procedure code")
    rvu: float = Field(description="Relative Value Unit for this procedure")
    conversion_factor: float = Field(description="Regional conversion factor applied")
    base_fee: float = Field(description="Base fee calculation (RVU × Conversion Factor)")
    modifier_applied: Optional[str] = Field(default=None, description="Any modifier applied (e.g., PA/NP 80% rule)")
    final_fee: float = Field(description="Final calculated fee after modifiers")
    calculation_breakdown: str = Field(description="Exact mathematical breakdown")

class FeeCalculationExplanation(BaseModel):
    """Complete fee calculation explanation for legal review"""
    region: str = Field(description="Geographic region for fee calculation")
    line_items: List[LineItemExplanation] = Field(description="Detailed breakdown for each CPT code")
    total_calculated_amount: float = Field(description="Total amount for all line items")
    summary: str = Field(description="Concise summary of the calculation")

class ExplanationGenerator:
    """Generates structured explanations for fee calculations using LangChain"""
    
    def __init__(self, api_key: str):
        self.llm = ChatGoogleGenerativeAI(
            model="gemini-2.0-flash",
            google_api_key=api_key,
            temperature=0.1  # Low temperature for consistent, factual output
        )
        
        # Set up the output parser
        self.output_parser = PydanticOutputParser(pydantic_object=FeeCalculationExplanation)
        
        # Create the prompt template
        self.prompt = PromptTemplate(
            template="""You are a legal fee calculation expert. Generate a precise, lawyer-ready explanation of this medical fee calculation.

REGION: {region}

CALCULATION DATA:
{calculation_data}

{format_instructions}

Generate a precise explanation that includes:
1. The region clearly stated
2. Each line item with exact breakdown: CPT code, RVU, conversion factor, and calculated fee
3. Any modifiers applied (like PA/NP 80% rule) with exact calculation
4. Total amount
5. Keep explanations factual, precise, and suitable for legal documentation""",
            input_variables=["region", "calculation_data"],
            partial_variables={"format_instructions": self.output_parser.get_format_instructions()}
        )
        
        self.chain = self.prompt | self.llm | self.output_parser
    
    def generate_explanation(self, region: str, calculation_data: List[dict]) -> FeeCalculationExplanation:
        """Generate structured explanation for fee calculations"""
        
        # Format the calculation data for the prompt
        formatted_data = []
        total_amount = 0
        
        for item in calculation_data:
            if "error" in item:
                continue
                
            base_fee = item.get('global_fee', 0)
            final_fee = item.get('calculated_fee', 0)
            modifier = item.get('modifier_applied', '')
            
            # Build calculation breakdown string
            breakdown = f"{item.get('rvu', 0)} × {item.get('conversion_factor', 0)}"
            if modifier:
                if "80%" in modifier:
                    breakdown += f" × 0.80 (PA/NP modifier)"
                else:
                    breakdown += f" × {modifier}"
            
            formatted_item = {
                "cpt_code": item.get('cpt_code', ''),
                "rvu": item.get('rvu', 0),
                "conversion_factor": item.get('conversion_factor', 0),
                "base_fee": base_fee,
                "modifier": modifier,
                "final_fee": final_fee,
                "breakdown": breakdown
            }
            
            formatted_data.append(formatted_item)
            total_amount += final_fee
        
        calculation_text = json.dumps(formatted_data, indent=2)
        
        try:
            result = self.chain.invoke({
                "region": region,
                "calculation_data": calculation_text
            })
            
            # Ensure total is set correctly
            result.total_calculated_amount = total_amount
            
            return result
            
        except Exception as e:
            # Fallback to manual generation if LLM fails
            return self._generate_fallback_explanation(region, formatted_data, total_amount)
    
    def _generate_fallback_explanation(self, region: str, calculation_data: List[dict], total_amount: float) -> FeeCalculationExplanation:
        """Generate explanation manually if LLM fails"""
        
        line_items = []
        for item in calculation_data:
            line_item = LineItemExplanation(
                cpt_code=item["cpt_code"],
                rvu=item["rvu"],
                conversion_factor=item["conversion_factor"],
                base_fee=item["base_fee"],
                modifier_applied=item["modifier"] if item["modifier"] else None,
                final_fee=item["final_fee"],
                calculation_breakdown=item["breakdown"]
            )
            line_items.append(line_item)
        
        summary = f"Fee calculation for {len(line_items)} procedure(s) in {region}. Total calculated amount: ${total_amount:.2f}"
        
        return FeeCalculationExplanation(
            region=region,
            line_items=line_items,
            total_calculated_amount=total_amount,
            summary=summary
        )
    
    def format_for_legal_document(self, explanation: FeeCalculationExplanation) -> str:
        """Format the explanation as a legal document"""
        
        lines = []
        lines.append(f"MEDICAL FEE CALCULATION - REGION: {explanation.region}")
        lines.append("=" * 60)
        lines.append("")
        
        for i, item in enumerate(explanation.line_items, 1):
            lines.append(f"Line Item {i}: CPT Code {item.cpt_code}")
            lines.append(f"  RVU: {item.rvu}")
            lines.append(f"  Regional Conversion Factor: {item.conversion_factor}")
            lines.append(f"  Calculation: {item.calculation_breakdown}")
            lines.append(f"  Base Fee: ${item.base_fee:.2f}")
            
            if item.modifier_applied:
                lines.append(f"  Modifier Applied: {item.modifier_applied}")
            
            lines.append(f"  Final Fee: ${item.final_fee:.2f}")
            lines.append("")
        
        lines.append("-" * 40)
        lines.append(f"TOTAL CALCULATED AMOUNT: ${explanation.total_calculated_amount:.2f}")
        lines.append("-" * 40)
        lines.append("")
        lines.append(f"Summary: {explanation.summary}")
        
        return "\n".join(lines)
