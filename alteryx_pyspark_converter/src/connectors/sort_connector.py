"""
Sort Connector for Alteryx Sort tools.

Handles conversion of Alteryx Sort tools to PySpark orderBy operations.
"""

from typing import Dict, Any, List
from ..core.base_connector import BaseConnector, ToolInfo, ConversionResult
from ..core.logger import get_logger


class SortConnector(BaseConnector):
    """Connector for Alteryx Sort tools."""
    
    def __init__(self):
        super().__init__(
            connector_name="SortConnector",
            supported_tools=["AlteryxBasePluginsGui.Sort.Sort"]
        )
        self.logger = get_logger()
    
    def validate_config(self, config: Dict[str, Any]) -> tuple[bool, List[str]]:
        """Validate Sort tool configuration."""
        errors = []
        # Sort tool can work with default configuration
        return True, errors
    
    def convert_tool(self, tool_info: ToolInfo) -> ConversionResult:
        """Convert Sort tool to PySpark code."""
        self._increment_conversion_count()
        
        try:
            config = tool_info.config
            
            # Get input DataFrame variable
            if not tool_info.inputs:
                error_msg = "Sort tool has no input connections"
                return ConversionResult(
                    success=False,
                    code="",
                    variables_created=[],
                    variables_used=[],
                    error_message=error_msg
                )
            
            input_var = self._generate_variable_name(tool_info.inputs[0])
            output_var = self._generate_variable_name(tool_info.tool_id)
            
            # Parse sort configuration
            sort_fields = self._parse_sort_config(config)
            
            # Generate sort code
            code = self._generate_sort_code(input_var, output_var, sort_fields)
            
            return ConversionResult(
                success=True,
                code=code,
                variables_created=[output_var],
                variables_used=[input_var]
            )
            
        except Exception as e:
            self._increment_error_count()
            error_msg = f"Failed to convert Sort tool: {str(e)}"
            
            return ConversionResult(
                success=False,
                code="",
                variables_created=[],
                variables_used=[],
                error_message=error_msg
            )
    
    def _parse_sort_config(self, config: Dict[str, Any]) -> List[Dict[str, str]]:
        """Parse sort configuration from Alteryx config."""
        sort_fields = []
        
        # Look for sort field information
        if 'Properties' in config:
            properties = config['Properties']
            
            # Handle SortFields configuration
            if 'SortFields' in properties:
                sort_fields_config = properties['SortFields']
                
                if isinstance(sort_fields_config, list):
                    for field_config in sort_fields_config:
                        if isinstance(field_config, dict):
                            field_name = field_config.get('@field', '')
                            order = field_config.get('@order', 'Ascending')
                            if field_name:
                                sort_fields.append({
                                    'field': field_name,
                                    'order': order
                                })
                elif isinstance(sort_fields_config, dict):
                    field_name = sort_fields_config.get('@field', '')
                    order = sort_fields_config.get('@order', 'Ascending')
                    if field_name:
                        sort_fields.append({
                            'field': field_name,
                            'order': order
                        })
        
        # If no fields found, assume first column ascending
        if not sort_fields:
            sort_fields = [{'field': 'first_column', 'order': 'Ascending'}]
        
        return sort_fields
    
    def _generate_sort_code(self, input_var: str, output_var: str, sort_fields: List[Dict[str, str]]) -> str:
        """Generate PySpark sort code."""
        
        # Build sort expressions
        sort_expressions = []
        for field_info in sort_fields:
            field_name = field_info['field']
            order = field_info['order']
            
            if field_name == 'first_column':
                # Use the first column of the DataFrame
                sort_expr = f'{input_var}.columns[0]'
            else:
                sort_expr = f'"{field_name}"'
            
            if order.lower() == 'descending':
                sort_expr = f'desc({sort_expr})'
            
            sort_expressions.append(sort_expr)
        
        if sort_expressions:
            sort_clause = ', '.join(sort_expressions)
            return f'''# Sort data by specified columns
{output_var} = {input_var}.orderBy({sort_clause})'''
        else:
            return f'''# Sort data (default ordering)
{output_var} = {input_var}.orderBy({input_var}.columns[0])'''