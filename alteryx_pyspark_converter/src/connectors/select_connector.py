"""
Select Connector for Alteryx Select tools.

Handles conversion of Alteryx Select tools to PySpark select and column operations.
"""

from typing import Dict, Any, List
from ..core.base_connector import BaseConnector, ToolInfo, ConversionResult
from ..core.logger import get_logger


class SelectConnector(BaseConnector):
    """Connector for Alteryx Select tools."""
    
    def __init__(self):
        super().__init__(
            connector_name="SelectConnector",
            supported_tools=["AlteryxBasePluginsGui.AlteryxSelect.AlteryxSelect"]
        )
        self.logger = get_logger()
    
    def validate_config(self, config: Dict[str, Any]) -> tuple[bool, List[str]]:
        """Validate Select tool configuration."""
        errors = []
        
        # Select tool can work without explicit configuration
        # It will include all columns by default if no OrderChanged is specified
        
        return True, errors
    
    def convert_tool(self, tool_info: ToolInfo) -> ConversionResult:
        """Convert Select tool to PySpark code."""
        self._increment_conversion_count()
        
        try:
            config = tool_info.config
            
            # Get input DataFrame variable
            if not tool_info.inputs:
                error_msg = "Select tool has no input connections"
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
            
            # Parse select operations
            code = self._generate_select_code(input_var, output_var, config)
            
            self.logger.app_logger.debug(f"Generated select code for tool {tool_info.tool_id}")
            
            return ConversionResult(
                success=True,
                code=code,
                variables_created=[output_var],
                variables_used=[input_var]
            )
            
        except Exception as e:
            self._increment_error_count()
            error_msg = f"Failed to convert Select tool: {str(e)}"
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
    
    def _generate_select_code(self, input_var: str, output_var: str, config: Dict[str, Any]) -> str:
        """Generate PySpark select code based on configuration."""
        
        # Check if there are specific column operations
        if 'OrderChanged' in config and config['OrderChanged'] == 'True':
            # Parse column operations from configuration
            select_operations = self._parse_column_operations(config)
            
            if select_operations:
                return f'''# Select and transform columns
{output_var} = {input_var}.select({', '.join(select_operations)})'''
            else:
                # No specific operations, select all columns
                return f'''# Select all columns
{output_var} = {input_var}'''
        else:
            # No column reordering, just pass through
            return f'''# Pass through all columns
{output_var} = {input_var}'''
    
    def _parse_column_operations(self, config: Dict[str, Any]) -> List[str]:
        """Parse column operations from Alteryx Select configuration."""
        operations = []
        
        # Look for field configurations
        # Alteryx stores field info in various formats, try to handle common ones
        
        # Method 1: Check for Selection nodes in configuration
        if 'Properties' in config:
            properties = config['Properties']
            
            # Handle different configuration structures
            if isinstance(properties, dict):
                # Look for Selection elements
                selections = properties.get('Selection', [])
                if not isinstance(selections, list):
                    selections = [selections] if selections else []
                
                for selection in selections:
                    if isinstance(selection, dict):
                        field_name = selection.get('@field', '')
                        selected = selection.get('@selected', 'True')
                        renamed = selection.get('@rename', field_name)
                        
                        if selected == 'True' and field_name:
                            if renamed and renamed != field_name:
                                # Column is being renamed
                                operations.append(f'col("{field_name}").alias("{renamed}")')
                            else:
                                # Column is selected as-is
                                operations.append(f'col("{field_name}")')
        
        # Method 2: Try to find field information in other config locations
        if not operations:
            # Look for field names in other possible locations
            for key, value in config.items():
                if 'field' in key.lower() and isinstance(value, str):
                    operations.append(f'col("{value}")')
        
        # Method 3: If no specific fields found, assume select all
        if not operations:
            operations = ['*']
        
        return operations
    
    def _convert_alteryx_type_to_spark(self, alteryx_type: str) -> str:
        """Convert Alteryx data type to Spark SQL type."""
        type_mapping = {
            'V_String': 'StringType()',
            'V_WString': 'StringType()',
            'Int16': 'ShortType()',
            'Int32': 'IntegerType()',
            'Int64': 'LongType()',
            'FixedDecimal': 'DecimalType()',
            'Float': 'FloatType()',
            'Double': 'DoubleType()',
            'Bool': 'BooleanType()',
            'Date': 'DateType()',
            'DateTime': 'TimestampType()'
        }
        
        return type_mapping.get(alteryx_type, 'StringType()')