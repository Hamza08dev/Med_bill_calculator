import json
import google.generativeai as genai

class LLMInterface:
    def __init__(self, api_key):
        try:
            genai.configure(api_key=api_key)
            self.model = genai.GenerativeModel('gemini-2.5-flash')
        except Exception as e:
            print(f"Could not configure Gemini API. Error: {e}")
            self.model = None

    def format_response(self, data):
        if not self.model: return json.dumps(data, indent=2)
        
        prompt = f"""
        You are a helpful assistant explaining medical billing. Convert the following JSON data into a clear, human-readable summary.
        - Start with the Global Fee and how it was calculated.
        - If professional_component_fee and technical_component_fee exist, explain them as a separate point.
        - Mention the Fee Schedule used.

        JSON data:
        {json.dumps(data, indent=2)}
        """
        response = self.model.generate_content(prompt)
        return response.text.strip()