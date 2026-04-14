#!/usr/bin/env python3
"""
Simple HTTP server to serve the fee calculator UI and handle fee calculations
"""
import json
import os
import sys
from pathlib import Path
from http.server import HTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import threading
import webbrowser
from typing import Dict, Any, List

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

try:
    from src.calc.fee_engine import FeeEngine
    from neo4j import GraphDatabase
    FEE_ENGINE_AVAILABLE = True
except ImportError as e:
    print(f"Warning: FeeEngine not available: {e}")
    FEE_ENGINE_AVAILABLE = False

class FeeCalculatorHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(project_root), **kwargs)
    
    def do_GET(self):
        if self.path == '/' or self.path == '/calculator':
            # Serve the HTML file
            self.path = '/simple_fee_calculator.html'
        return super().do_GET()
    
    def do_POST(self):
        if self.path == '/api/calculate':
            self.handle_calculate_request()
        else:
            self.send_error(404, "Not Found")
    
    def handle_calculate_request(self):
        try:
            # Read request body
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data.decode('utf-8'))
            
            print(f"Received calculation request: {data}")
            
            # Extract form data
            zip_code = data.get('zip_code', '')
            provider_type = data.get('provider_type', '')
            designation = data.get('designation', 'MD')
            line_items = data.get('line_items', [])
            
            print(f"Extracted data - ZIP: {zip_code}, Provider: {provider_type}, Designation: {designation}, Items: {line_items}")
            
            if not zip_code or not provider_type or not line_items:
                self.send_error_response("Missing required fields")
                return
            
            # Calculate fees
            if FEE_ENGINE_AVAILABLE:
                print("Attempting to use FeeEngine...")
                result = self.calculate_with_fee_engine(zip_code, provider_type, designation, line_items)
            else:
                print("FeeEngine not available, using mock calculation...")
                result = self.calculate_mock_fees(zip_code, provider_type, designation, line_items)
            
            print(f"Calculation result: {result}")
            
            # Send response
            self.send_json_response(result)
            
        except Exception as e:
            print(f"Error in calculation: {e}")
            self.send_error_response(f"Calculation error: {str(e)}")
    
    def calculate_with_fee_engine(self, zip_code: str, provider_type: str, designation: str, line_items: List[Dict]) -> Dict[str, Any]:
        """Calculate fees using the actual FeeEngine"""
        try:
            # Load database configuration
            db_config = self.load_db_config()
            neo_config = db_config.get('neo4j', {})
            data_dir = db_config.get('data', {}).get('directory', '')
            
            if not neo_config.get('uri') or not data_dir:
                raise Exception("Database configuration not found")
            
            # Connect to Neo4j
            driver = GraphDatabase.driver(
                neo_config['uri'], 
                auth=(neo_config['user'], neo_config['password'])
            )
            
            # Initialize fee engine
            fee_engine = FeeEngine(driver, data_dir)
            
            # Calculate fees
            result = fee_engine.calculate_fees_with_explanation(
                line_items, zip_code, provider_type, designation
            )
            
            driver.close()
            return result
            
        except Exception as e:
            print(f"FeeEngine calculation failed: {e}")
            return self.calculate_mock_fees(zip_code, provider_type, designation, line_items)
    
    def calculate_mock_fees(self, zip_code: str, provider_type: str, designation: str, line_items: List[Dict]) -> Dict[str, Any]:
        """Calculate mock fees when FeeEngine is not available"""
        calculation_results = []
        total_amount = 0
        
        # More realistic mock fees based on common CPT codes
        mock_fee_ranges = {
            '99213': 120.0,  # Office visit
            '99214': 180.0,  # Office visit
            '99215': 250.0,  # Office visit
            '99212': 80.0,   # Office visit
            '99211': 50.0,   # Office visit
            '98940': 45.0,   # Chiropractic manipulation
            '98941': 55.0,   # Chiropractic manipulation
            '98942': 65.0,   # Chiropractic manipulation
            '97110': 35.0,   # Therapeutic exercise
            '97112': 40.0,   # Neuromuscular reeducation
            '97140': 30.0,   # Manual therapy
        }
        
        for item in line_items:
            cpt_code = item.get('code', '')
            units = item.get('units', 1)
            
            # Get mock fee based on CPT code, default to 100 if not found
            base_fee = mock_fee_ranges.get(cpt_code, 100.0)
            
            # Apply PA/NP reduction if applicable
            if designation and ('pa' in designation.lower() or 'np' in designation.lower()):
                base_fee *= 0.8
            
            total_fee = base_fee * units
            total_amount += total_fee
            
            calculation_results.append({
                'cpt_code': cpt_code,
                'calculated_fee': base_fee,
                'units': units,
                'total_fee_for_units': total_fee,
                'modifier_applied': 'PA/NP (80%)' if designation and ('pa' in designation.lower() or 'np' in designation.lower()) else None
            })
        
        return {
            'calculation_results': calculation_results,
            'total_calculated_amount': total_amount,
            'region': f'Region for ZIP {zip_code}',
            'provider_type': provider_type,
            'designation': designation,
            'explanation': 'Mock calculation - FeeEngine not available. Install Neo4j and configure database for real calculations.'
        }
    
    def load_db_config(self) -> Dict[str, Any]:
        """Load database configuration"""
        config_path = project_root / 'configs' / 'db_config.json'
        if config_path.exists():
            with open(config_path, 'r') as f:
                return json.load(f)
        return {}
    
    def send_json_response(self, data: Dict[str, Any]):
        """Send JSON response"""
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
        
        response = json.dumps(data, indent=2)
        self.wfile.write(response.encode('utf-8'))
    
    def send_error_response(self, message: str):
        """Send error response"""
        self.send_response(400)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        
        error_response = {'error': message}
        response = json.dumps(error_response, indent=2)
        self.wfile.write(response.encode('utf-8'))
    
    def do_OPTIONS(self):
        """Handle CORS preflight requests"""
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

def start_server(port=8080):
    """Start the HTTP server"""
    server_address = ('', port)
    httpd = HTTPServer(server_address, FeeCalculatorHandler)
    
    print(f"Fee Calculator Server starting on port {port}")
    print(f"Open your browser and go to: http://localhost:{port}")
    print("Press Ctrl+C to stop the server")
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")
        httpd.shutdown()

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Start the Fee Calculator server')
    parser.add_argument('--port', type=int, default=8080, help='Port to run the server on')
    parser.add_argument('--open-browser', action='store_true', help='Automatically open browser')
    
    args = parser.parse_args()
    
    if args.open_browser:
        # Open browser after a short delay
        def open_browser():
            import time
            time.sleep(1)
            webbrowser.open(f'http://localhost:{args.port}')
        
        browser_thread = threading.Thread(target=open_browser)
        browser_thread.daemon = True
        browser_thread.start()
    
    start_server(args.port)
