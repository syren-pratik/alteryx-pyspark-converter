"""
Code Formatter utility for formatting and organizing generated PySpark code.

Handles code formatting, optimization hints, and documentation generation.
"""

import re
from typing import List, Dict, Any, Optional
from textwrap import dedent, indent

from ..core.logger import get_logger
from ...config.settings import Settings


class CodeFormatter:
    """Formats and optimizes generated PySpark code."""
    
    def __init__(self):
        """Initialize the code formatter."""
        self.logger = get_logger()
    
    def format_code(self, code: str) -> str:
        """
        Format PySpark code for better readability.
        
        Args:
            code: Raw generated code
            
        Returns:
            Formatted code string
        """
        try:
            # Normalize line endings
            code = code.replace('\r\n', '\n').replace('\r', '\n')
            
            # Remove excessive blank lines
            code = re.sub(r'\n{3,}', '\n\n', code)
            
            # Clean up indentation
            code = self._normalize_indentation(code)
            
            # Add optimization hints if enabled
            if Settings.ENABLE_BROADCAST_HINTS or Settings.ENABLE_CACHING_HINTS:
                code = self._add_optimization_hints(code)
            
            # Format based on style preference
            if Settings.OUTPUT_CODE_STYLE == "formatted":
                code = self._apply_formatting_rules(code)
            elif Settings.OUTPUT_CODE_STYLE == "commented":
                code = self._add_detailed_comments(code)
            
            return code
            
        except Exception as e:
            self.logger.log_error(
                component="CodeFormatter",
                error_type="FormattingError",
                message=f"Failed to format code: {str(e)}"
            )
            return code  # Return unformatted code if formatting fails
    
    def _normalize_indentation(self, code: str) -> str:
        """Normalize indentation in the code."""
        lines = code.split('\n')
        normalized_lines = []
        
        for line in lines:
            # Convert tabs to spaces
            line = line.expandtabs(4)
            
            # Remove trailing whitespace
            line = line.rstrip()
            
            normalized_lines.append(line)
        
        return '\n'.join(normalized_lines)
    
    def _apply_formatting_rules(self, code: str) -> str:
        """Apply formatting rules for better readability."""
        lines = code.split('\n')
        formatted_lines = []
        
        in_multiline_statement = False
        
        for i, line in enumerate(lines):
            stripped = line.strip()
            
            # Skip empty lines in multiline statements
            if not stripped and in_multiline_statement:
                continue
            
            # Check for multiline statements
            if stripped.endswith('\\'):
                in_multiline_statement = True
            elif in_multiline_statement and not stripped.endswith('\\'):
                in_multiline_statement = False
            
            # Add spacing around operators
            if '=' in line and not line.strip().startswith('#'):
                line = re.sub(r'\s*=\s*', ' = ', line)
            
            # Format method chaining
            if '.' in line and not line.strip().startswith('#'):
                line = self._format_method_chaining(line)
            
            # Add blank line before comments (except inline comments)
            if stripped.startswith('#') and i > 0 and lines[i-1].strip():
                if not lines[i-1].strip().startswith('#'):
                    formatted_lines.append('')
            
            formatted_lines.append(line)
        
        return '\n'.join(formatted_lines)
    
    def _format_method_chaining(self, line: str) -> str:
        """Format method chaining for better readability."""
        # If line contains multiple method calls, format them nicely
        if line.count('.') > 2 and not line.strip().startswith('#'):
            # Look for patterns like df.method1().method2().method3()
            pattern = r'(\w+)(\.[a-zA-Z_]\w*\([^)]*\)){2,}'
            match = re.search(pattern, line)
            
            if match:
                # Format with proper indentation
                indent_level = len(line) - len(line.lstrip())
                base_indent = ' ' * indent_level
                continuation_indent = ' ' * (indent_level + 4)
                
                # Split by dots and reformat
                parts = line.split('.')
                if len(parts) > 3:  # Only reformat if there are many method calls
                    formatted = parts[0] + ' \\'
                    for part in parts[1:]:
                        formatted += f'\n{continuation_indent}.{part}'
                        if not part.endswith('\\'):
                            formatted += ' \\'
                    
                    # Remove the last backslash
                    formatted = formatted.rstrip(' \\')
                    return formatted
        
        return line
    
    def _add_optimization_hints(self, code: str) -> str:
        """Add optimization hints to the code."""
        lines = code.split('\n')
        optimized_lines = []
        
        for line in lines:
            optimized_lines.append(line)
            
            # Add broadcast hints for small DataFrames in joins
            if Settings.ENABLE_BROADCAST_HINTS and '.join(' in line:
                optimized_lines.append('# Hint: Consider using broadcast() for small DataFrames')
                optimized_lines.append('# Example: df1.join(broadcast(df2), "key")')
            
            # Add caching hints for DataFrames used multiple times
            if Settings.ENABLE_CACHING_HINTS and ' = ' in line and 'df_' in line:
                if any(op in line for op in ['filter', 'select', 'withColumn', 'join']):
                    optimized_lines.append('# Hint: Consider caching if this DataFrame is used multiple times')
                    optimized_lines.append(f'# Example: {line.split("=")[0].strip()}.cache()')
        
        return '\n'.join(optimized_lines)
    
    def _add_detailed_comments(self, code: str) -> str:
        """Add detailed comments explaining the code."""
        lines = code.split('\n')
        commented_lines = []
        
        for line in lines:
            stripped = line.strip()
            
            # Add explanatory comments for complex operations
            if '=' in stripped and not stripped.startswith('#'):
                operation_type = self._identify_operation_type(stripped)
                if operation_type:
                    commented_lines.append(f'# {operation_type}')
            
            commented_lines.append(line)
        
        return '\n'.join(commented_lines)
    
    def _identify_operation_type(self, line: str) -> Optional[str]:
        """Identify the type of operation for commenting."""
        line_lower = line.lower()
        
        if '.read.' in line_lower:
            return 'Reading data from source'
        elif '.write.' in line_lower:
            return 'Writing data to destination'
        elif '.filter(' in line_lower:
            return 'Filtering rows based on condition'
        elif '.select(' in line_lower:
            return 'Selecting and transforming columns'
        elif '.join(' in line_lower:
            return 'Joining datasets'
        elif '.union(' in line_lower:
            return 'Combining datasets vertically'
        elif '.groupby(' in line_lower or '.groupBy(' in line_lower:
            return 'Grouping data for aggregation'
        elif '.orderby(' in line_lower or '.orderBy(' in line_lower:
            return 'Sorting data'
        elif '.withcolumn(' in line_lower or '.withColumn(' in line_lower:
            return 'Adding or modifying column'
        elif '.drop(' in line_lower:
            return 'Removing columns'
        elif '.distinct(' in line_lower:
            return 'Removing duplicate rows'
        elif '.collect(' in line_lower:
            return 'Collecting results to driver'
        elif '.show(' in line_lower:
            return 'Displaying data sample'
        
        return None
    
    def generate_code_documentation(self, workflow_name: str, statistics: Dict[str, Any], 
                                  tools: List[Dict[str, Any]]) -> str:
        """
        Generate comprehensive documentation for the converted code.
        
        Args:
            workflow_name: Name of the workflow
            statistics: Conversion statistics
            tools: List of tools in the workflow
            
        Returns:
            Documentation string
        """
        doc = f'''"""
Converted PySpark Code Documentation
===================================

Workflow: {workflow_name}
Generated: {self._get_timestamp()}
Converter Version: {Settings.APP_VERSION}

Conversion Statistics:
- Total Tools: {statistics.get('total_tools', 0)}
- Supported Tools: {statistics.get('supported_tools', 0)}
- Unsupported Tools: {statistics.get('unsupported_tools', 0)}
- Support Rate: {statistics.get('support_rate', 0)}%
- Conversion Time: {statistics.get('conversion_time', 0):.2f} seconds

Tool Breakdown:
{self._generate_tool_breakdown(tools)}

Performance Considerations:
- Enable adaptive query execution for better performance
- Consider caching DataFrames that are reused multiple times
- Use broadcast joins for small lookup tables
- Monitor Spark UI for optimization opportunities

Dependencies:
- PySpark 3.0 or higher
- Java 8 or higher
- Sufficient cluster resources for data volume

Usage:
1. Ensure all input files are accessible
2. Adjust file paths as needed for your environment
3. Configure Spark session parameters for your cluster
4. Run the script in a PySpark environment

"""'''
        return doc
    
    def _generate_tool_breakdown(self, tools: List[Dict[str, Any]]) -> str:
        """Generate a breakdown of tools in the workflow."""
        tool_counts = {}
        
        for tool in tools:
            tool_type = tool.get('name', tool.get('type', 'Unknown'))
            tool_counts[tool_type] = tool_counts.get(tool_type, 0) + 1
        
        breakdown = []
        for tool_type, count in sorted(tool_counts.items()):
            breakdown.append(f"- {tool_type}: {count}")
        
        return '\n'.join(breakdown)
    
    def _get_timestamp(self) -> str:
        """Get current timestamp in readable format."""
        import datetime
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    def optimize_imports(self, code: str) -> str:
        """Optimize and organize imports in the generated code."""
        lines = code.split('\n')
        
        # Extract imports
        imports = []
        other_lines = []
        
        for line in lines:
            if line.strip().startswith(('from ', 'import ')):
                imports.append(line)
            else:
                other_lines.append(line)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_imports = []
        for imp in imports:
            if imp not in seen:
                unique_imports.append(imp)
                seen.add(imp)
        
        # Sort imports
        pyspark_imports = [imp for imp in unique_imports if 'pyspark' in imp]
        other_imports = [imp for imp in unique_imports if 'pyspark' not in imp]
        
        # Combine back
        organized_imports = sorted(other_imports) + sorted(pyspark_imports)
        
        if organized_imports:
            return '\n'.join(organized_imports) + '\n\n' + '\n'.join(other_lines)
        else:
            return '\n'.join(other_lines)