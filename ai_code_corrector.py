"""
AI Code Corrector for Alteryx to PySpark Conversion
This module sends complete code to AI for corrections and optimizations
Returns only the changed lines to save tokens
"""

import os
import json
import re
from typing import Dict, List, Any, Optional, Tuple
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class CodeCorrector:
    def __init__(self, model_provider: str = 'gemini', api_key: Optional[str] = None):
        """
        Initialize the AI Code Corrector
        
        Args:
            model_provider: 'gemini', 'openai', 'ollama', or 'local'
            api_key: API key for the selected provider
        """
        self.model_provider = model_provider
        self.api_key = api_key or os.getenv('GEMINI_API_KEY')
        self.ollama_url = "http://localhost:11434"
        
    def clean_code(self, code: str) -> Tuple[str, Dict[int, str]]:
        """
        Remove comments and unnecessary whitespace, keep line mapping
        Returns cleaned code and mapping of new line numbers to original
        """
        lines = code.split('\n')
        cleaned_lines = []
        line_mapping = {}
        new_line_num = 1
        
        for i, line in enumerate(lines, 1):
            # Remove comments but keep the code
            code_part = line.split('#')[0].rstrip()
            
            # Skip completely empty lines (but keep lines with just indentation)
            if code_part or line.strip().startswith('#'):
                if code_part:  # Only add non-comment lines
                    cleaned_lines.append(code_part)
                    line_mapping[new_line_num] = i
                    new_line_num += 1
        
        return '\n'.join(cleaned_lines), line_mapping
    
    def create_correction_prompt(self, xml_content: str, pyspark_code: str, workflow_info: Dict) -> str:
        """
        Create a focused prompt for code correction
        """
        # Clean the code first
        cleaned_code, line_mapping = self.clean_code(pyspark_code)
        
        prompt = f"""You are an expert data engineer specializing in migrating Alteryx workflows to idiomatic PySpark. Your task is to correct and refactor a flawed, auto-generated PySpark script that contains logical errors from literal tool-by-tool translation.

ALTERYX XML:
```xml
{xml_content}
```

FLAWED PYSPARK SCRIPT:
```python
{cleaned_code}
```

KEY PROBLEMS TO FIX:

1. **CrossTab Misimplementation**: The script incorrectly implements CrossTab as simple groupBy/concat_ws instead of using .pivot()
   
2. **Multi-Row Formula Errors**: Multi-Row Formula logic should use PySpark Window functions with lag()/lead(), not simple transformations
   
3. **Poor Variable Names**: Replace cryptic names like df_7_T, df_13_F with descriptive names like sales_summary_df, customer_filtered_df
   
4. **Text to Columns Issues**: After splitting text, use explode() to properly handle array columns
   
5. **Missing Alteryx Logic**:
   - Filter tools with complex conditions (AND/OR logic)
   - Join tools requiring specific join types (left_outer, left_anti, cross)
   - Summarize tools needing multiple aggregations
   - Sort tools with multi-field sorting
   - Unique tools requiring distinct() or dropDuplicates()
   
6. **Macro Handling**: For "Unknown" Alteryx macros, infer purpose from context (e.g., apply headers, parse dates, clean data)

7. **Common Mistakes**:
   - Missing header=True, inferSchema=True in CSV reading
   - Not handling null values properly
   - Incorrect date/time parsing
   - Missing proper output format specifications
   - Not using broadcast() for small dimension tables in joins

REQUIREMENTS FOR YOUR OUTPUT:

Return a JSON object with your corrections:
{{
  "corrections": [
    {{"line": X, "original": "old code", "corrected": "new idiomatic code with proper function"}},
    // Fix each problematic line
  ],
  "new_lines": [
    {{"after_line": X, "code": "from pyspark.sql import Window"}},
    {{"after_line": X, "code": "from pyspark.sql.functions import *"}},
    // Add necessary imports and new logic
  ],
  "variable_renames": [
    {{"old": "df_7_T", "new": "sales_summary_df"}},
    // Provide all variable renamings
  ],
  "accuracy_score": 0-100,
  "improvements": [
    "Fixed CrossTab to use proper pivot() function",
    "Implemented Multi-Row Formula with Window functions",
    "Renamed variables to be descriptive",
    // List all improvements made
  ],
  "workflow_purpose": "Brief description of what this workflow does"
}}

CRITICAL: 
- Understand the workflow's overall goal from the XML
- Rewrite logic holistically, don't just patch it
- Use idiomatic PySpark (pivot(), Window, explode, regexp_extract, etc.)
- Ensure the corrected code is production-ready
- Make the code readable and maintainable"""
        
        return prompt, cleaned_code, line_mapping
    
    def correct_with_gemini(self, xml_content: str, pyspark_code: str, workflow_info: Dict) -> Dict:
        """
        Use Google Gemini to correct the code
        """
        try:
            import google.generativeai as genai
            
            # Configure Gemini
            genai.configure(api_key=self.api_key)
            model = genai.GenerativeModel('gemini-1.5-flash')
            
            # Create correction prompt
            prompt, cleaned_code, line_mapping = self.create_correction_prompt(
                xml_content, pyspark_code, workflow_info
            )
            
            # Get corrections from Gemini
            response = model.generate_content(prompt)
            response_text = response.text
            
            # Parse the response
            corrections = self.parse_correction_response(response_text, line_mapping)
            corrections['cleaned_code'] = cleaned_code
            corrections['original_code'] = pyspark_code
            
            return corrections
            
        except Exception as e:
            print(f"Gemini correction failed: {e}")
            return {
                'error': str(e),
                'corrections': [],
                'accuracy_score': 0,
                'improvements': [],
                'original_code': pyspark_code
            }
    
    def correct_with_ollama(self, xml_content: str, pyspark_code: str, workflow_info: Dict) -> Dict:
        """
        Use Ollama for code correction
        """
        try:
            # Create correction prompt
            prompt, cleaned_code, line_mapping = self.create_correction_prompt(
                xml_content, pyspark_code, workflow_info
            )
            
            # Call Ollama API
            response = requests.post(
                f"{self.ollama_url}/api/generate",
                json={
                    "model": "llama3.2:1b",
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": 0.3,
                        "num_predict": 2048
                    }
                },
                timeout=45
            )
            
            if response.status_code == 200:
                result = response.json()
                response_text = result.get('response', '')
                
                # Parse the response
                corrections = self.parse_correction_response(response_text, line_mapping)
                corrections['cleaned_code'] = cleaned_code
                corrections['original_code'] = pyspark_code
                
                return corrections
            else:
                return {
                    'error': f"Ollama API error: {response.status_code}",
                    'corrections': [],
                    'accuracy_score': 0,
                    'original_code': pyspark_code
                }
                
        except Exception as e:
            print(f"Ollama correction failed: {e}")
            return {
                'error': str(e),
                'corrections': [],
                'accuracy_score': 0,
                'original_code': pyspark_code
            }
    
    def parse_correction_response(self, response_text: str, line_mapping: Dict) -> Dict:
        """
        Parse the AI response to extract corrections
        """
        try:
            # Try to extract JSON from response
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if json_match:
                data = json.loads(json_match.group())
                
                # Map cleaned line numbers back to original
                if 'corrections' in data:
                    for correction in data['corrections']:
                        cleaned_line = correction['line']
                        if cleaned_line in line_mapping:
                            correction['original_line'] = line_mapping[cleaned_line]
                        else:
                            correction['original_line'] = cleaned_line
                
                if 'new_lines' in data:
                    for new_line in data['new_lines']:
                        after_line = new_line.get('after_line', 0)
                        if after_line in line_mapping:
                            new_line['after_original_line'] = line_mapping[after_line]
                        else:
                            new_line['after_original_line'] = after_line
                
                # Handle variable renames if present
                if 'variable_renames' not in data:
                    data['variable_renames'] = []
                
                # Add workflow purpose if present
                if 'workflow_purpose' not in data:
                    data['workflow_purpose'] = ''
                
                return data
            else:
                # Fallback: try to parse structured text
                return self.parse_text_corrections(response_text)
                
        except json.JSONDecodeError:
            return self.parse_text_corrections(response_text)
    
    def parse_text_corrections(self, response_text: str) -> Dict:
        """
        Parse non-JSON text response for corrections
        """
        corrections = []
        improvements = []
        accuracy_score = 70  # Default score
        
        # Try to extract accuracy score
        score_match = re.search(r'accuracy[:\s]+(\d+)', response_text, re.IGNORECASE)
        if score_match:
            accuracy_score = int(score_match.group(1))
        
        # Extract line corrections
        line_pattern = r'line\s+(\d+)[:\s]+(.+?)[\n\r]'
        for match in re.finditer(line_pattern, response_text, re.IGNORECASE):
            line_num = int(match.group(1))
            correction_text = match.group(2)
            
            # Try to split into original and corrected
            if '->' in correction_text or 'to' in correction_text:
                parts = re.split(r'->|to', correction_text)
                if len(parts) == 2:
                    corrections.append({
                        'line': line_num,
                        'original': parts[0].strip(),
                        'corrected': parts[1].strip()
                    })
        
        return {
            'corrections': corrections,
            'accuracy_score': accuracy_score,
            'improvements': improvements,
            'new_lines': []
        }
    
    def apply_corrections(self, original_code: str, corrections: Dict) -> str:
        """
        Apply the corrections to the original code
        """
        lines = original_code.split('\n')
        code_text = '\n'.join(lines)
        
        # Apply variable renames first (if any)
        for rename in corrections.get('variable_renames', []):
            old_name = rename.get('old', '')
            new_name = rename.get('new', '')
            if old_name and new_name:
                # Replace variable names throughout the code
                import re
                # Match whole words only to avoid partial replacements
                pattern = r'\b' + re.escape(old_name) + r'\b'
                code_text = re.sub(pattern, new_name, code_text)
        
        # Split back into lines after renames
        lines = code_text.split('\n')
        
        # Apply line corrections
        for correction in corrections.get('corrections', []):
            line_idx = correction.get('original_line', correction['line']) - 1
            if 0 <= line_idx < len(lines):
                # If we've done renames, the correction might need updating
                corrected_line = correction['corrected']
                for rename in corrections.get('variable_renames', []):
                    old_name = rename.get('old', '')
                    new_name = rename.get('new', '')
                    if old_name and new_name:
                        import re
                        pattern = r'\b' + re.escape(old_name) + r'\b'
                        corrected_line = re.sub(pattern, new_name, corrected_line)
                lines[line_idx] = corrected_line
        
        # Add new lines (in reverse order to maintain line numbers)
        for new_line in sorted(corrections.get('new_lines', []), 
                              key=lambda x: x.get('after_original_line', x.get('after_line', 0)), 
                              reverse=True):
            insert_after = new_line.get('after_original_line', new_line.get('after_line', 0))
            new_code = new_line['code']
            
            # Apply renames to new lines too
            for rename in corrections.get('variable_renames', []):
                old_name = rename.get('old', '')
                new_name = rename.get('new', '')
                if old_name and new_name:
                    import re
                    pattern = r'\b' + re.escape(old_name) + r'\b'
                    new_code = re.sub(pattern, new_name, new_code)
            
            if 0 <= insert_after <= len(lines):
                lines.insert(insert_after, new_code)
        
        return '\n'.join(lines)
    
    def format_corrected_code(self, code: str) -> str:
        """
        Format the corrected code nicely
        """
        # Add proper imports at the top if missing
        if 'from pyspark.sql import SparkSession' not in code:
            imports = [
                "from pyspark.sql import SparkSession",
                "from pyspark.sql.functions import *",
                "from pyspark.sql.types import *",
                ""
            ]
            code = '\n'.join(imports) + code
        
        # Ensure proper spacing
        lines = code.split('\n')
        formatted_lines = []
        prev_was_import = False
        
        for line in lines:
            is_import = line.strip().startswith(('import ', 'from '))
            
            # Add blank line after imports block
            if prev_was_import and not is_import and line.strip():
                formatted_lines.append('')
            
            formatted_lines.append(line)
            prev_was_import = is_import
        
        return '\n'.join(formatted_lines)
    
    def get_corrections(self, xml_content: str, pyspark_code: str, workflow_info: Dict) -> Dict:
        """
        Main method to get code corrections from AI
        """
        if self.model_provider == 'gemini':
            corrections = self.correct_with_gemini(xml_content, pyspark_code, workflow_info)
        elif self.model_provider == 'ollama':
            corrections = self.correct_with_ollama(xml_content, pyspark_code, workflow_info)
        else:
            # Return no corrections for local/unsupported
            corrections = {
                'corrections': [],
                'accuracy_score': 75,
                'improvements': ['AI correction not available for this provider'],
                'original_code': pyspark_code
            }
        
        # Apply corrections if any
        if corrections.get('corrections') or corrections.get('new_lines'):
            corrected_code = self.apply_corrections(pyspark_code, corrections)
            corrections['corrected_code'] = self.format_corrected_code(corrected_code)
        else:
            corrections['corrected_code'] = pyspark_code
        
        return corrections


# Example usage
if __name__ == "__main__":
    corrector = CodeCorrector(model_provider='gemini')
    
    sample_xml = "<AlteryxDocument>...</AlteryxDocument>"
    sample_code = """
    # This is a comment
    df = spark.read.csv('file.csv')
    
    # Another comment
    df.show()
    """
    
    result = corrector.get_corrections(sample_xml, sample_code, {'total_tools': 2})
    print(json.dumps(result, indent=2))