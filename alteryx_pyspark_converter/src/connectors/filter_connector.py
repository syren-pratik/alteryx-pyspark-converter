"""
Filter Connector for Alteryx Filter tools.

Handles conversion of Alteryx Filter tools to PySpark filter operations.
"""

from typing import Dict, Any, List
from ..core.base_connector import BaseConnector, ToolInfo, ConversionResult
from ..core.logger import get_logger


class FilterConnector(BaseConnector):
    """Connector for Alteryx Filter tools."""
    
    def __init__(self):
        super().__init__(
            connector_name="FilterConnector",
            supported_tools=["AlteryxBasePluginsGui.Filter.Filter"]
        )
        self.logger = get_logger()
    
    def validate_config(self, config: Dict[str, Any]) -> tuple[bool, List[str]]:
        """Validate Filter tool configuration."""
        errors = []
        
        # Check for filter expression
        if 'Expression' not in config:
            errors.append("Missing 'Expression' configuration")
        
        expression = config.get('Expression', '')
        if not expression.strip():
            errors.append("Filter expression cannot be empty")
        
        return len(errors) == 0, errors
    
    def convert_tool(self, tool_info: ToolInfo) -> ConversionResult:
        """Convert Filter tool to PySpark code."""
        self._increment_conversion_count()
        
        try:
            config = tool_info.config
            
            # Get input DataFrame variable
            if not tool_info.inputs:
                error_msg = "Filter tool has no input connections"
                self.logger.log_error(
                    component=self.connector_name,
                    error_type="NoInputConnection",
                    message=error_msg,
                    details={'tool_id': tool_info.tool_id}
                )
                return ConversionResult(
                    success=False,
                    code="",
                    variables_created=[],
                    variables_used=[],
                    error_message=error_msg
                )
            
            input_var = self._generate_variable_name(tool_info.inputs[0])
            true_output_var = self._generate_variable_name(tool_info.tool_id, "true")
            false_output_var = self._generate_variable_name(tool_info.tool_id, "false")
            
            # Parse filter expression
            expression = config.get('Expression', '')
            pyspark_expression = self._convert_filter_expression(expression)
            
            # Generate filter code
            code = self._generate_filter_code(input_var, true_output_var, false_output_var, pyspark_expression)
            
            self.logger.app_logger.debug(f"Generated filter code for expression: {expression}")
            
            return ConversionResult(
                success=True,
                code=code,
                variables_created=[true_output_var, false_output_var],
                variables_used=[input_var]
            )
            
        except Exception as e:
            self._increment_error_count()
            error_msg = f"Failed to convert Filter tool: {str(e)}"
            self.logger.log_error(
                component=self.connector_name,
                error_type="ConversionError",
                message=error_msg,
                details={'tool_id': tool_info.tool_id}
            )
            
            return ConversionResult(
                success=False,
                code="",
                variables_created=[],
                variables_used=[],
                error_message=error_msg
            )
    
    def _generate_filter_code(self, input_var: str, true_var: str, false_var: str, expression: str) -> str:
        """Generate PySpark filter code."""
        return f'''# Filter data based on condition
{true_var} = {input_var}.filter({expression})
{false_var} = {input_var}.filter(~({expression}))'''
    
    def _convert_filter_expression(self, expression: str) -> str:
        """Convert Alteryx filter expression to PySpark expression."""
        # Clean the expression
        expr = expression.strip()
        
        # Handle common Alteryx patterns
        # Replace field references [Field] with col("Field")
        import re
        
        # Replace [FieldName] with col("FieldName")
        expr = re.sub(r'\[([^\]]+)\]', r'col("\1")', expr)
        
        # Replace common operators
        replacements = {
            ' AND ': ' & ',
            ' and ': ' & ',
            ' OR ': ' | ',
            ' or ': ' | ',
            ' NOT ': ' ~',
            ' not ': ' ~',
            ' = ': ' == ',
            '!': '~',
            'ISNULL(': 'isnull(',
            'ISNOTNULL(': '~isnull(',
            'IsNull(': 'isnull(',
            'IsNotNull(': '~isnull(',
            'IN(': 'isin(',
            'CONTAINS(': 'contains(',
            'StartsWith(': 'startswith(',
            'EndsWith(': 'endswith(',
            'Length(': 'length(',
            'UPPER(': 'upper(',
            'LOWER(': 'lower(',
            'TRIM(': 'trim('
        }
        
        for old, new in replacements.items():
            expr = expr.replace(old, new)
        
        # Handle string literals - ensure they're properly quoted
        # This is a basic implementation, might need refinement
        expr = re.sub(r'(?<!col\(")(?<!")(\b[A-Za-z_][A-Za-z0-9_]*\b)(?!")', r'"\1"', expr)
        
        # Fix double quotes around column references that got caught by the above
        expr = re.sub(r'col\(""([^"]+)""\)', r'col("\1")', expr)
        
        # Handle numeric comparisons
        expr = re.sub(r'col\("(\d+(?:\.\d+)?)")', r'\1', expr)
        
        return expr