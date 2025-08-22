"""
Formula Connector for Alteryx Formula tools.

Handles conversion of Alteryx Formula tools to PySpark withColumn operations.
"""

from typing import Dict, Any, List
import re
from ..core.base_connector import BaseConnector, ToolInfo, ConversionResult
from ..core.logger import get_logger


class FormulaConnector(BaseConnector):
    """Connector for Alteryx Formula tools."""
    
    def __init__(self):
        super().__init__(
            connector_name="FormulaConnector",
            supported_tools=["AlteryxBasePluginsGui.Formula.Formula"]
        )
        self.logger = get_logger()
    
    def validate_config(self, config: Dict[str, Any]) -> tuple[bool, List[str]]:
        """Validate Formula tool configuration."""
        errors = []
        
        # Check for formula expression
        if 'Expression' not in config and 'FormulaFields' not in config:
            errors.append("Missing formula configuration")
        
        return len(errors) == 0, errors
    
    def convert_tool(self, tool_info: ToolInfo) -> ConversionResult:
        """Convert Formula tool to PySpark code."""
        self._increment_conversion_count()
        
        try:
            config = tool_info.config
            
            # Get input DataFrame variable
            if not tool_info.inputs:
                error_msg = "Formula tool has no input connections"
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
            output_var = self._generate_variable_name(tool_info.tool_id)
            
            # Parse formula configuration
            formulas = self._parse_formula_config(config)
            
            # Generate formula code
            code = self._generate_formula_code(input_var, output_var, formulas)
            
            self.logger.app_logger.debug(f"Generated formula code for {len(formulas)} formulas")
            
            return ConversionResult(
                success=True,
                code=code,
                variables_created=[output_var],
                variables_used=[input_var]
            )
            
        except Exception as e:
            self._increment_error_count()
            error_msg = f"Failed to convert Formula tool: {str(e)}"
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
    
    def _parse_formula_config(self, config: Dict[str, Any]) -> List[Dict[str, str]]:
        """Parse formula configuration from Alteryx config."""
        formulas = []
        
        # Method 1: Single expression
        if 'Expression' in config:
            field_name = config.get('field', 'calculated_field')
            expression = config['Expression']
            formulas.append({
                'field': field_name,
                'expression': expression
            })
        
        # Method 2: Multiple formula fields
        if 'FormulaFields' in config:
            formula_fields = config['FormulaFields']
            if isinstance(formula_fields, list):
                for formula_field in formula_fields:
                    if isinstance(formula_field, dict):
                        field_name = formula_field.get('@field', 'calculated_field')
                        expression = formula_field.get('@expression', '')
                        if expression:
                            formulas.append({
                                'field': field_name,
                                'expression': expression
                            })
            elif isinstance(formula_fields, dict):
                field_name = formula_fields.get('@field', 'calculated_field')
                expression = formula_fields.get('@expression', '')
                if expression:
                    formulas.append({
                        'field': field_name,
                        'expression': expression
                    })
        
        # Fallback: create a simple calculation if no formulas found
        if not formulas:
            formulas.append({
                'field': 'calculated_field',
                'expression': 'NULL'
            })
        
        return formulas
    
    def _generate_formula_code(self, input_var: str, output_var: str, formulas: List[Dict[str, str]]) -> str:
        """Generate PySpark formula code."""
        
        code_lines = [f"# Apply formula calculations"]
        current_var = input_var
        
        for i, formula in enumerate(formulas):
            field_name = formula['field']
            expression = formula['expression']
            
            # Convert Alteryx expression to PySpark
            pyspark_expr = self._convert_alteryx_expression(expression)
            
            if i == len(formulas) - 1:
                # Last formula, use output variable name
                next_var = output_var
            else:
                # Intermediate variable
                next_var = f"{output_var}_step{i+1}"
            
            code_lines.append(f'{next_var} = {current_var}.withColumn("{field_name}", {pyspark_expr})')
            current_var = next_var
        
        return '\n'.join(code_lines)
    
    def _convert_alteryx_expression(self, expression: str) -> str:
        """Convert Alteryx formula expression to PySpark expression."""
        expr = expression.strip()
        
        # Replace field references [Field] with col("Field")
        expr = re.sub(r'\[([^\]]+)\]', r'col("\1")', expr)
        
        # Common Alteryx function mappings
        function_mappings = {
            'ISNULL': 'isnull',
            'IsNull': 'isnull',
            'Length': 'length',
            'UPPER': 'upper',
            'Upper': 'upper',
            'LOWER': 'lower',
            'Lower': 'lower',
            'TRIM': 'trim',
            'Trim': 'trim',
            'LEFT': 'substring',
            'Right': 'substring',
            'MID': 'substring',
            'SUBSTRING': 'substring',
            'CONCATENATE': 'concat',
            'REPLACE': 'regexp_replace',
            'ToNumber': 'cast',
            'ToString': 'cast',
            'YEAR': 'year',
            'MONTH': 'month',
            'DAY': 'dayofmonth',
            'DATEADD': 'date_add',
            'DATEDIFF': 'datediff',
            'NOW': 'current_timestamp',
            'TODAY': 'current_date'
        }
        
        # Replace function names
        for alteryx_func, pyspark_func in function_mappings.items():
            pattern = rf'\b{alteryx_func}\s*\('
            replacement = f'{pyspark_func}('
            expr = re.sub(pattern, replacement, expr, flags=re.IGNORECASE)
        
        # Handle special cases
        # Convert IF statements to when/otherwise
        if_pattern = r'IF\s+(.+?)\s+THEN\s+(.+?)\s+ELSE\s+(.+?)\s+ENDIF'
        def replace_if(match):
            condition = match.group(1)
            then_expr = match.group(2)
            else_expr = match.group(3)
            return f'when({condition}, {then_expr}).otherwise({else_expr})'
        
        expr = re.sub(if_pattern, replace_if, expr, flags=re.IGNORECASE | re.DOTALL)
        
        # Handle mathematical operators
        expr = expr.replace(' + ', ' + ').replace(' - ', ' - ').replace(' * ', ' * ').replace(' / ', ' / ')
        
        # Handle string concatenation
        expr = expr.replace(' + ', ', ') if 'concat' in expr else expr
        
        # Handle NULL values
        expr = expr.replace('NULL', 'lit(None)')
        
        # Handle literal strings and numbers
        # This is a basic implementation - might need refinement
        expr = re.sub(r'(?<!")(\b\d+\.?\d*\b)(?!")', r'lit(\1)', expr)
        expr = re.sub(r'"([^"]*)"', r'lit("\1")', expr)
        
        return expr