"""
AI Code Analyzer for Alteryx to PySpark Conversion
This module analyzes the generated PySpark code and compares it with the original XML
to provide corrections, optimizations, and best practices.

Supports:
- Ollama (free, local LLMs)
- OpenAI API
- Local rule-based analysis
"""

import os
import json
import xml.etree.ElementTree as ET
from typing import Dict, List, Any, Optional
import re
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class CodeAnalyzer:
    def __init__(self, api_key: Optional[str] = None, use_local: bool = False, use_ollama: bool = True, 
                 use_gemini: bool = False, gemini_api_key: Optional[str] = None, model_choice: Optional[str] = None):
        """
        Initialize the AI Code Analyzer
        
        Args:
            api_key: OpenAI API key (optional if using local analysis or Ollama)
            use_local: Use local rule-based analysis instead of AI
            use_ollama: Use Ollama for free local AI analysis
            use_gemini: Use Google Gemini for AI analysis
            gemini_api_key: Google Gemini API key
            model_choice: Specific model to use (e.g., 'ollama', 'openai', 'gemini', 'local')
        """
        self.api_key = api_key or os.getenv('OPENAI_API_KEY')
        self.gemini_api_key = gemini_api_key or os.getenv('GEMINI_API_KEY')
        self.use_local = use_local
        self.use_ollama = use_ollama
        self.use_gemini = use_gemini
        self.model_choice = model_choice
        self.ollama_url = "http://localhost:11434"
        # Try to use the best available model
        self.preferred_models = ["llama3.2:1b", "llama3.2:3b", "codellama:7b", "tinyllama"]
        self.ollama_model = "llama3.2:1b"  # Default to llama3.2:1b for faster response
        
        # Define analysis rules for local processing
        self.optimization_rules = {
            'redundant_selects': 'Multiple consecutive Select operations can be combined',
            'inefficient_joins': 'Consider broadcast joins for small datasets',
            'missing_cache': 'Add .cache() for DataFrames used multiple times',
            'column_pruning': 'Select only required columns early in the pipeline',
            'partitioning': 'Consider repartitioning for better performance',
            'coalesce_output': 'Use coalesce(1) before writing single output files'
        }
    
    def check_ollama_available(self) -> bool:
        """Check if Ollama is running and available"""
        try:
            response = requests.get(f"{self.ollama_url}/api/tags")
            return response.status_code == 200
        except:
            return False
    
    def get_ollama_models(self) -> List[str]:
        """Get list of available Ollama models"""
        try:
            response = requests.get(f"{self.ollama_url}/api/tags")
            if response.status_code == 200:
                data = response.json()
                return [model['name'] for model in data.get('models', [])]
        except:
            pass
        return []
    
    def analyze_with_ollama(self, xml_content: str, pyspark_code: str, workflow_info: Dict) -> Dict:
        """Use Ollama for free local AI analysis"""
        try:
            # Check if Ollama is available
            if not self.check_ollama_available():
                print("Ollama not available, falling back to local analysis")
                return self.analyze_locally(xml_content, pyspark_code, workflow_info)
            
            # Check if any models are available
            models = self.get_ollama_models()
            if not models:
                print("No Ollama models found. Please run: ollama pull llama3.2:1b")
                analysis = self.analyze_locally(xml_content, pyspark_code, workflow_info)
                analysis['message'] = "No Ollama models installed. Please run: ollama pull llama3.2:1b"
                return analysis
            
            # Use the best available model
            selected_model = None
            for preferred in self.preferred_models:
                if preferred in models:
                    selected_model = preferred
                    break
            
            if not selected_model:
                selected_model = models[0]  # Use first available if no preferred found
                
            self.ollama_model = selected_model
            print(f"Using Ollama model: {self.ollama_model}")
            
            # Prepare structured prompt for better analysis
            prompt = f"""You are an expert in Alteryx and PySpark. Analyze this workflow conversion.

ORIGINAL ALTERYX XML (showing {len(xml_content)} characters):
```xml
{xml_content[:5000]}
```

GENERATED PYSPARK CODE:
```python
{pyspark_code}
```

PROVIDE A STRUCTURED ANALYSIS:

1. WORKFLOW PURPOSE:
Describe in 1-2 sentences what this Alteryx workflow does based on the XML.

2. CONVERSION ACCURACY:
List each Alteryx tool from the XML and whether it's correctly converted:
- Tool ID X (Type): [Converted/Missing/Incorrect]

3. CODE QUALITY SCORE: X/10
Explain why you gave this score.

4. CRITICAL ISSUES:
List any bugs or errors that will prevent the code from working:
- Issue 1: [description]
- Issue 2: [description]

5. IMPROVEMENTS NEEDED:
List specific improvements for the PySpark code:
- Add error handling for file reading
- Include schema validation
- Optimize performance by...

6. MISSING FEATURES:
What Alteryx functionality is not implemented in PySpark:
- Feature 1: [description]
- Feature 2: [description]

Keep your response structured and specific."""

            # Call Ollama API with appropriate timeout
            response = requests.post(
                f"{self.ollama_url}/api/generate",
                json={
                    "model": self.ollama_model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": 0.3,  # Lower temperature for more focused analysis
                        "num_predict": 1024  # Moderate response length for faster analysis
                    }
                },
                timeout=30  # Reasonable timeout for faster models
            )
            
            if response.status_code == 200:
                result = response.json()
                ai_response = result.get('response', '')
                
                # Parse the AI response
                analysis = {
                    "score": 7,
                    "correctness": "Unknown",
                    "missing_features": [],
                    "issues": [],
                    "corrections": [],
                    "optimizations": [],
                    "best_practices": [],
                    "source": "ollama"
                }
                
                # Try to parse as JSON first
                try:
                    # Check if response contains JSON
                    json_match = re.search(r'\{.*\}', ai_response, re.DOTALL)
                    if json_match:
                        parsed = json.loads(json_match.group())
                        analysis.update(parsed)
                except:
                    pass
                
                # Parse structured response from Ollama
                if ai_response:
                    # Split response into sections for better parsing
                    lines = ai_response.split('\n')
                    current_section = ""
                    
                    for line in lines:
                        # Detect section headers
                        if 'WORKFLOW PURPOSE' in line.upper():
                            current_section = "purpose"
                        elif 'CONVERSION ACCURACY' in line.upper():
                            current_section = "accuracy"
                        elif 'CODE QUALITY SCORE' in line.upper():
                            current_section = "score"
                            # Extract score from this section
                            score_match = re.search(r'(\d+)/10', line)
                            if score_match:
                                analysis['score'] = int(score_match.group(1))
                        elif 'CRITICAL ISSUES' in line.upper():
                            current_section = "issues"
                        elif 'IMPROVEMENTS NEEDED' in line.upper():
                            current_section = "improvements"
                        elif 'MISSING FEATURES' in line.upper():
                            current_section = "missing"
                        
                        # Parse content based on current section
                        elif current_section and line.strip():
                            if current_section == "purpose" and len(line.strip()) > 10:
                                analysis['workflow_description'] = line.strip()
                            
                            elif current_section == "accuracy":
                                if 'converted' in line.lower() or 'missing' in line.lower():
                                    if 'missing' in line.lower():
                                        tool_match = re.search(r'Tool ID (\d+)[^:]*:\s*(.+)', line)
                                        if tool_match:
                                            analysis['missing_features'].append(f"Tool {tool_match.group(1)}: {tool_match.group(2)}")
                            
                            elif current_section == "issues":
                                if line.strip().startswith('-') or re.match(r'^\d+\.', line.strip()):
                                    issue_text = re.sub(r'^[-\d.]\s*', '', line.strip())
                                    if issue_text and len(issue_text) > 5:
                                        analysis['issues'].append({"message": issue_text, "severity": "high"})
                            
                            elif current_section == "improvements":
                                if line.strip().startswith('-') or re.match(r'^\d+\.', line.strip()):
                                    improvement_text = re.sub(r'^[-\d.]\s*', '', line.strip())
                                    if improvement_text and len(improvement_text) > 5:
                                        analysis['optimizations'].append({"message": improvement_text, "severity": "medium"})
                            
                            elif current_section == "missing":
                                if line.strip().startswith('-') or 'Feature' in line:
                                    feature_text = re.sub(r'^[-\d.]\s*Feature \d+:\s*', '', line.strip())
                                    if feature_text and len(feature_text) > 5:
                                        analysis['missing_features'].append(feature_text)
                    
                    # Set correctness based on found issues
                    if analysis['issues']:
                        analysis['correctness'] = "⚠️ Conversion has issues that need attention"
                    elif analysis['missing_features']:
                        analysis['correctness'] = "⚠️ Some Alteryx features not implemented"
                    else:
                        analysis['correctness'] = "✅ Conversion appears correct"
                    
                    # Store full response for reference
                    analysis['full_analysis'] = ai_response
                
                return analysis
            else:
                print(f"Ollama API error: {response.status_code}")
                return self.analyze_locally(xml_content, pyspark_code, workflow_info)
                
        except Exception as e:
            print(f"Ollama analysis failed: {e}")
            return self.analyze_locally(xml_content, pyspark_code, workflow_info)
    
    def analyze_with_gemini(self, xml_content: str, pyspark_code: str, workflow_info: Dict) -> Dict:
        """
        Use Google Gemini API to analyze the code
        """
        try:
            import google.generativeai as genai
            
            # Configure Gemini
            genai.configure(api_key=self.gemini_api_key)
            
            # Initialize the model (use gemini-1.5-flash for free tier)
            model = genai.GenerativeModel('gemini-1.5-flash')
            
            # Create structured prompt
            prompt = f"""You are an expert in Alteryx and PySpark. Analyze this workflow conversion comprehensively.

ORIGINAL ALTERYX XML WORKFLOW:
```xml
{xml_content[:8000]}
```

GENERATED PYSPARK CODE:
```python
{pyspark_code}
```

WORKFLOW DETAILS:
- Total Tools: {workflow_info.get('total_tools', 0)}
- Tool Types: {', '.join([t['type'] for t in workflow_info.get('tools', [])])}

PROVIDE DETAILED ANALYSIS:

1. WORKFLOW PURPOSE:
Explain what this Alteryx workflow does based on the XML analysis.

2. CONVERSION ACCURACY:
Check if all Alteryx tools are correctly converted to PySpark.

3. CODE QUALITY SCORE: X/10
Rate the quality of the generated PySpark code.

4. ISSUES FOUND:
List any bugs, errors, or incorrect conversions.

5. OPTIMIZATIONS:
Suggest performance improvements for the PySpark code.

6. MISSING FEATURES:
Identify any Alteryx functionality not implemented in PySpark.

7. BEST PRACTICES:
Recommend code improvements and best practices.

Provide response in JSON format with keys: workflow_purpose, score, conversion_accuracy, issues, optimizations, missing_features, best_practices"""
            
            # Generate response
            response = model.generate_content(prompt)
            
            # Parse AI response
            ai_response = response.text
            
            # Try to extract JSON from response
            import json
            import re
            
            try:
                # Look for JSON in the response
                json_match = re.search(r'\{[^}]*\}', ai_response, re.DOTALL)
                if json_match:
                    analysis = json.loads(json_match.group())
                else:
                    # Try to parse the entire response as JSON
                    analysis = json.loads(ai_response)
                    
                analysis['source'] = 'gemini'
                
                # Ensure score is an integer
                if 'score' in analysis:
                    score_str = str(analysis['score'])
                    score_match = re.search(r'(\d+)', score_str)
                    if score_match:
                        analysis['score'] = int(score_match.group(1))
                
                # Format the response for our report
                formatted_analysis = {
                    "score": analysis.get('score', 7),
                    "workflow_description": analysis.get('workflow_purpose', ''),
                    "correctness": analysis.get('conversion_accuracy', 'Unknown'),
                    "issues": [],
                    "optimizations": [],
                    "missing_features": [],
                    "best_practices": [],
                    "source": "gemini",
                    "full_analysis": ai_response
                }
                
                # Convert issues to our format
                if 'issues' in analysis:
                    if isinstance(analysis['issues'], list):
                        formatted_analysis['issues'] = [
                            {"message": issue, "severity": "high"} 
                            for issue in analysis['issues'] if issue
                        ]
                    elif isinstance(analysis['issues'], str):
                        formatted_analysis['issues'] = [{"message": analysis['issues'], "severity": "high"}]
                
                # Convert optimizations
                if 'optimizations' in analysis:
                    if isinstance(analysis['optimizations'], list):
                        formatted_analysis['optimizations'] = [
                            {"message": opt, "severity": "medium"} 
                            for opt in analysis['optimizations'] if opt
                        ]
                    elif isinstance(analysis['optimizations'], str):
                        formatted_analysis['optimizations'] = [{"message": analysis['optimizations'], "severity": "medium"}]
                
                # Convert missing features
                if 'missing_features' in analysis:
                    if isinstance(analysis['missing_features'], list):
                        formatted_analysis['missing_features'] = analysis['missing_features']
                    elif isinstance(analysis['missing_features'], str):
                        formatted_analysis['missing_features'] = [analysis['missing_features']]
                
                # Convert best practices
                if 'best_practices' in analysis:
                    if isinstance(analysis['best_practices'], list):
                        formatted_analysis['best_practices'] = [
                            {"message": bp, "severity": "low"} 
                            for bp in analysis['best_practices'] if bp
                        ]
                    elif isinstance(analysis['best_practices'], str):
                        formatted_analysis['best_practices'] = [{"message": analysis['best_practices'], "severity": "low"}]
                
                return formatted_analysis
                
            except json.JSONDecodeError:
                # If not valid JSON, parse text response
                return self._parse_text_response(ai_response, 'gemini')
                
        except Exception as e:
            print(f"Gemini analysis failed: {e}")
            # Fallback to local analysis
            return self.analyze_locally(xml_content, pyspark_code, workflow_info)
    
    def _parse_text_response(self, response_text: str, source: str) -> Dict:
        """Parse non-JSON text response from AI models"""
        analysis = {
            "score": 7,
            "correctness": "Unknown",
            "issues": [],
            "optimizations": [],
            "missing_features": [],
            "best_practices": [],
            "source": source,
            "full_analysis": response_text
        }
        
        # Try to extract score
        score_match = re.search(r'(\d+)/10', response_text)
        if score_match:
            analysis['score'] = int(score_match.group(1))
        
        # Extract sections based on common patterns
        lines = response_text.split('\n')
        for line in lines:
            if 'issue' in line.lower() or 'error' in line.lower() or 'bug' in line.lower():
                if len(line.strip()) > 10:
                    analysis['issues'].append({"message": line.strip(), "severity": "high"})
            elif 'optimi' in line.lower() or 'performance' in line.lower():
                if len(line.strip()) > 10:
                    analysis['optimizations'].append({"message": line.strip(), "severity": "medium"})
            elif 'missing' in line.lower() or 'not implemented' in line.lower():
                if len(line.strip()) > 10:
                    analysis['missing_features'].append(line.strip())
        
        return analysis
    
    def analyze_with_ai(self, xml_content: str, pyspark_code: str, workflow_info: Dict) -> Dict:
        """
        Use OpenAI API to analyze the code
        """
        try:
            from openai import OpenAI
            
            # Initialize OpenAI client
            client = OpenAI(api_key=self.api_key)
            
            # Create structured prompt
            prompt = f"""You are an expert in Alteryx and PySpark. Analyze this workflow conversion comprehensively.

ORIGINAL ALTERYX XML WORKFLOW:
```xml
{xml_content[:8000]}
```

GENERATED PYSPARK CODE:
```python
{pyspark_code}
```

WORKFLOW DETAILS:
- Total Tools: {workflow_info.get('total_tools', 0)}
- Tool Types: {', '.join([t['type'] for t in workflow_info.get('tools', [])])}

PROVIDE DETAILED ANALYSIS:

1. WORKFLOW PURPOSE:
Explain what this Alteryx workflow does based on the XML analysis.

2. CONVERSION ACCURACY:
Check if all Alteryx tools are correctly converted to PySpark.

3. CODE QUALITY SCORE: X/10
Rate the quality of the generated PySpark code.

4. ISSUES FOUND:
List any bugs, errors, or incorrect conversions.

5. OPTIMIZATIONS:
Suggest performance improvements for the PySpark code.

6. MISSING FEATURES:
Identify any Alteryx functionality not implemented in PySpark.

7. BEST PRACTICES:
Recommend code improvements and best practices.

Provide response in JSON format with keys: workflow_purpose, score, conversion_accuracy, issues, optimizations, missing_features, best_practices"""
            
            # Call OpenAI API
            response = client.chat.completions.create(
                model="gpt-4o-mini",  # Using the latest model
                messages=[
                    {"role": "system", "content": "You are an expert in Alteryx workflows and PySpark data engineering. Provide detailed technical analysis."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=2000,
                response_format={"type": "json_object"}  # Force JSON response
            )
            
            # Parse AI response
            ai_response = response.choices[0].message.content
            try:
                analysis = json.loads(ai_response)
                analysis['source'] = 'openai'
                
                # Ensure score is an integer
                if 'score' in analysis:
                    score_str = str(analysis['score'])
                    # Extract number from string like "8/10" or just "8"
                    score_match = re.search(r'(\d+)', score_str)
                    if score_match:
                        analysis['score'] = int(score_match.group(1))
                
                # Format the response for our report
                formatted_analysis = {
                    "score": analysis.get('score', 7),
                    "workflow_description": analysis.get('workflow_purpose', ''),
                    "correctness": analysis.get('conversion_accuracy', 'Unknown'),
                    "issues": [],
                    "optimizations": [],
                    "missing_features": [],
                    "best_practices": [],
                    "source": "openai",
                    "full_analysis": ai_response
                }
                
                # Convert issues to our format
                if 'issues' in analysis:
                    if isinstance(analysis['issues'], list):
                        formatted_analysis['issues'] = [
                            {"message": issue, "severity": "high"} 
                            for issue in analysis['issues'] if issue
                        ]
                    elif isinstance(analysis['issues'], str):
                        formatted_analysis['issues'] = [{"message": analysis['issues'], "severity": "high"}]
                
                # Convert optimizations
                if 'optimizations' in analysis:
                    if isinstance(analysis['optimizations'], list):
                        formatted_analysis['optimizations'] = [
                            {"message": opt, "severity": "medium"} 
                            for opt in analysis['optimizations'] if opt
                        ]
                    elif isinstance(analysis['optimizations'], str):
                        formatted_analysis['optimizations'] = [{"message": analysis['optimizations'], "severity": "medium"}]
                
                # Convert missing features
                if 'missing_features' in analysis:
                    if isinstance(analysis['missing_features'], list):
                        formatted_analysis['missing_features'] = analysis['missing_features']
                    elif isinstance(analysis['missing_features'], str):
                        formatted_analysis['missing_features'] = [analysis['missing_features']]
                
                # Convert best practices
                if 'best_practices' in analysis:
                    if isinstance(analysis['best_practices'], list):
                        formatted_analysis['best_practices'] = [
                            {"message": bp, "severity": "low"} 
                            for bp in analysis['best_practices'] if bp
                        ]
                    elif isinstance(analysis['best_practices'], str):
                        formatted_analysis['best_practices'] = [{"message": analysis['best_practices'], "severity": "low"}]
                
                return formatted_analysis
                
            except json.JSONDecodeError:
                # If not valid JSON, still try to extract useful information
                return {
                    "score": 7,
                    "full_analysis": ai_response,
                    "source": "openai",
                    "issues": [],
                    "optimizations": [],
                    "best_practices": []
                }
                
        except Exception as e:
            print(f"OpenAI analysis failed: {e}")
            # Fallback to local analysis
            return self.analyze_locally(xml_content, pyspark_code, workflow_info)
    
    def analyze_locally(self, xml_content: str, pyspark_code: str, workflow_info: Dict) -> Dict:
        """
        Perform rule-based local analysis without AI
        """
        analysis = {
            "score": 0,
            "issues": [],
            "optimizations": [],
            "best_practices": [],
            "warnings": [],
            "source": "local"
        }
        
        # Calculate base score
        score = 10
        
        # Check for common patterns and issues
        code_lines = pyspark_code.split('\n')
        
        # 1. Check for multiple consecutive selects
        select_count = sum(1 for line in code_lines if '.select(' in line)
        if select_count > 3:
            analysis['optimizations'].append({
                "type": "redundant_selects",
                "message": "Multiple Select operations detected. Consider combining them for better performance.",
                "severity": "medium"
            })
            score -= 0.5
        
        # 2. Check for missing cache on reused DataFrames
        df_assignments = {}
        for line in code_lines:
            if '=' in line and 'df_' in line:
                df_name = re.search(r'(df_\d+)', line)
                if df_name:
                    df_assignments[df_name.group(1)] = df_assignments.get(df_name.group(1), 0) + 1
        
        for df_name, count in df_assignments.items():
            if count > 2 and f'{df_name}.cache()' not in pyspark_code:
                analysis['optimizations'].append({
                    "type": "missing_cache",
                    "message": f"DataFrame {df_name} is used {count} times. Consider adding .cache() for better performance.",
                    "severity": "medium"
                })
                score -= 0.3
        
        # 3. Check for proper error handling
        if 'try:' not in pyspark_code:
            analysis['best_practices'].append({
                "type": "error_handling",
                "message": "Consider adding try-except blocks for error handling in production code.",
                "severity": "low"
            })
            score -= 0.2
        
        # 4. Check for joins without broadcast hints
        if '.join(' in pyspark_code and 'broadcast(' not in pyspark_code:
            analysis['optimizations'].append({
                "type": "join_optimization",
                "message": "Consider using broadcast joins for small dimension tables to improve performance.",
                "severity": "low"
            })
        
        # 5. Check for output without coalesce
        if '.write.' in pyspark_code and '.coalesce(' not in pyspark_code:
            analysis['optimizations'].append({
                "type": "output_optimization",
                "message": "Consider using coalesce() or repartition() before writing output for optimal file sizes.",
                "severity": "low"
            })
        
        # 6. Check XML vs Code alignment
        try:
            root = ET.fromstring(xml_content)
            xml_tools = root.findall('.//Node')
            code_steps = len([l for l in code_lines if '# Step' in l])
            
            if len(xml_tools) != code_steps:
                analysis['warnings'].append({
                    "type": "conversion_mismatch",
                    "message": f"XML has {len(xml_tools)} tools but code has {code_steps} steps. Some tools may not be converted.",
                    "severity": "high"
                })
                score -= 1
        except:
            pass
        
        # 7. Check for SQL injection vulnerabilities
        if 'spark.sql(f' in pyspark_code or 'spark.sql(' in pyspark_code:
            if not re.search(r'spark\.sql\(["\'].*?["\']', pyspark_code):
                analysis['issues'].append({
                    "type": "security",
                    "message": "Potential SQL injection risk. Use parameterized queries or sanitize inputs.",
                    "severity": "high"
                })
                score -= 1
        
        # 8. Check for hardcoded paths
        if re.search(r'["\']\/.*?\.csv["\']|["\']C:\\.*?["\']', pyspark_code):
            analysis['best_practices'].append({
                "type": "hardcoded_paths",
                "message": "Hardcoded file paths detected. Consider using configuration files or environment variables.",
                "severity": "medium"
            })
            score -= 0.5
        
        # Calculate final score
        analysis['score'] = max(1, min(10, score))
        
        # Add summary
        total_issues = len(analysis['issues']) + len(analysis['warnings'])
        total_suggestions = len(analysis['optimizations']) + len(analysis['best_practices'])
        
        analysis['summary'] = {
            "total_issues": total_issues,
            "total_suggestions": total_suggestions,
            "conversion_quality": "Good" if analysis['score'] >= 7 else "Needs Improvement" if analysis['score'] >= 5 else "Poor"
        }
        
        return analysis
    
    def generate_recommendations(self, analysis: Dict) -> List[str]:
        """
        Generate actionable recommendations from analysis
        """
        recommendations = []
        
        # Priority 1: Critical issues
        for issue in analysis.get('issues', []):
            if isinstance(issue, dict) and issue.get('severity') == 'high':
                recommendations.append(f"🔴 CRITICAL: {issue.get('message', '')}")
            elif isinstance(issue, str):
                recommendations.append(f"🔴 ISSUE: {issue}")
        
        # Priority 2: Warnings
        for warning in analysis.get('warnings', []):
            if isinstance(warning, dict):
                recommendations.append(f"⚠️ WARNING: {warning.get('message', '')}")
            else:
                recommendations.append(f"⚠️ WARNING: {warning}")
        
        # Priority 3: Optimizations
        for opt in analysis.get('optimizations', [])[:3]:  # Top 3 optimizations
            if isinstance(opt, dict):
                recommendations.append(f"⚡ OPTIMIZE: {opt.get('message', '')}")
            else:
                recommendations.append(f"⚡ OPTIMIZE: {opt}")
        
        # Priority 4: Best practices
        for bp in analysis.get('best_practices', [])[:2]:  # Top 2 best practices
            if isinstance(bp, dict):
                recommendations.append(f"💡 TIP: {bp.get('message', '')}")
            else:
                recommendations.append(f"💡 TIP: {bp}")
        
        return recommendations
    
    def format_analysis_report(self, analysis: Dict) -> str:
        """
        Format the analysis into a readable report
        """
        report = []
        report.append("=" * 60)
        report.append("CODE ANALYSIS REPORT")
        report.append("=" * 60)
        report.append("")
        
        # Score
        score = analysis.get('score', 0)
        report.append(f"Quality Score: {score}/10 {'⭐' * int(score)}")
        report.append(f"Analysis Source: {analysis.get('source', 'local')}")
        report.append("")
        
        # Workflow Description
        if 'workflow_description' in analysis:
            report.append("WORKFLOW DESCRIPTION")
            report.append("-" * 30)
            report.append(analysis['workflow_description'])
            report.append("")
        
        # Correctness Check
        if 'correctness' in analysis and analysis['correctness'] != "Unknown":
            report.append("CONVERSION CORRECTNESS")
            report.append("-" * 30)
            report.append(f"{analysis['correctness']}")
            report.append("")
        
        # Summary
        if 'summary' in analysis:
            report.append("SUMMARY")
            report.append("-" * 30)
            summary = analysis['summary']
            # Check if summary is a dict or string
            if isinstance(summary, dict):
                report.append(f"Conversion Quality: {summary.get('conversion_quality', 'Unknown')}")
                report.append(f"Total Issues: {summary.get('total_issues', 0)}")
                report.append(f"Total Suggestions: {summary.get('total_suggestions', 0)}")
            else:
                # If summary is a string (from Ollama), just display it
                report.append(summary[:200] + "..." if len(summary) > 200 else summary)
            report.append("")
        
        # Missing Features
        if analysis.get('missing_features'):
            report.append("MISSING ALTERYX FEATURES")
            report.append("-" * 30)
            for feature in analysis['missing_features']:
                if isinstance(feature, dict):
                    report.append(f"• {feature.get('message', '')}")
                else:
                    report.append(f"• {feature}")
            report.append("")
        
        # Corrections
        if analysis.get('corrections'):
            report.append("SUGGESTED CORRECTIONS")
            report.append("-" * 30)
            for correction in analysis['corrections']:
                if isinstance(correction, dict):
                    report.append(f"• {correction.get('message', '')}")
                else:
                    report.append(f"• {correction}")
            report.append("")
        
        # Issues
        if analysis.get('issues'):
            report.append("ISSUES FOUND")
            report.append("-" * 30)
            for issue in analysis['issues']:
                if isinstance(issue, dict):
                    report.append(f"• [{issue.get('severity', 'unknown').upper()}] {issue.get('message', '')}")
                else:
                    report.append(f"• {issue}")
            report.append("")
        
        # Warnings
        if analysis.get('warnings'):
            report.append("WARNINGS")
            report.append("-" * 30)
            for warning in analysis['warnings']:
                if isinstance(warning, dict):
                    report.append(f"• {warning.get('message', '')}")
                else:
                    report.append(f"• {warning}")
            report.append("")
        
        # Optimizations
        if analysis.get('optimizations'):
            report.append("OPTIMIZATION OPPORTUNITIES")
            report.append("-" * 30)
            for opt in analysis['optimizations']:
                if isinstance(opt, dict):
                    report.append(f"• {opt.get('message', '')}")
                else:
                    report.append(f"• {opt}")
            report.append("")
        
        # Best Practices
        if analysis.get('best_practices'):
            report.append("BEST PRACTICE SUGGESTIONS")
            report.append("-" * 30)
            for bp in analysis['best_practices']:
                if isinstance(bp, dict):
                    report.append(f"• {bp.get('message', '')}")
                else:
                    report.append(f"• {bp}")
            report.append("")
        
        # Recommendations
        recommendations = self.generate_recommendations(analysis)
        if recommendations:
            report.append("TOP RECOMMENDATIONS")
            report.append("-" * 30)
            for i, rec in enumerate(recommendations, 1):
                report.append(f"{i}. {rec}")
            report.append("")
        
        # Full AI Analysis (if available and from Ollama)
        if analysis.get('source') == 'ollama' and analysis.get('full_analysis'):
            report.append("DETAILED AI ANALYSIS")
            report.append("-" * 30)
            # Format the full analysis text nicely
            full_text = analysis['full_analysis']
            # Split into paragraphs and format
            paragraphs = full_text.split('\n\n')
            for para in paragraphs[:5]:  # Show first 5 paragraphs
                if para.strip():
                    # Wrap long lines
                    words = para.split()
                    current_line = []
                    for word in words:
                        current_line.append(word)
                        if len(' '.join(current_line)) > 70:
                            report.append(' '.join(current_line[:-1]))
                            current_line = [word]
                    if current_line:
                        report.append(' '.join(current_line))
                    report.append("")
        
        report.append("=" * 60)
        
        return "\n".join(report)


# Example usage
if __name__ == "__main__":
    # Test with sample data
    analyzer = CodeAnalyzer(use_local=True)
    
    sample_code = """
    df_1 = spark.read.csv("input.csv")
    df_2 = df_1.select("col1", "col2")
    df_3 = df_2.select("col1")
    df_4 = df_1.join(df_3, "col1")
    df_5 = df_4.select("*")
    df_5.write.csv("output.csv")
    """
    
    sample_xml = "<AlteryxDocument><Nodes></Nodes></AlteryxDocument>"
    sample_info = {"total_tools": 5, "tools": [{"type": "Input"}, {"type": "Select"}]}
    
    analysis = analyzer.analyze_locally(sample_xml, sample_code, sample_info)
    report = analyzer.format_analysis_report(analysis)
    print(report)