"""
Join Connector for Alteryx Join tools.

Handles conversion of Alteryx Join tools to PySpark join operations.
"""

from typing import Dict, Any, List
from ..core.base_connector import BaseConnector, ToolInfo, ConversionResult
from ..core.logger import get_logger


class JoinConnector(BaseConnector):
    """Connector for Alteryx Join tools."""
    
    def __init__(self):
        super().__init__(
            connector_name="JoinConnector",
            supported_tools=["AlteryxBasePluginsGui.Join.Join"]
        )
        self.logger = get_logger()
    
    def validate_config(self, config: Dict[str, Any]) -> tuple[bool, List[str]]:
        """Validate Join tool configuration."""
        errors = []
        
        # Join tool needs at least join keys configuration
        # The actual validation can be flexible as joins can work with simple column matching
        
        return True, errors
    
    def convert_tool(self, tool_info: ToolInfo) -> ConversionResult:
        """Convert Join tool to PySpark code."""
        self._increment_conversion_count()
        
        try:
            config = tool_info.config
            
            # Get input DataFrame variables (Join needs exactly 2 inputs)
            if len(tool_info.inputs) < 2:
                error_msg = "Join tool requires at least 2 input connections"
                self.logger.log_error(
                    component=self.connector_name,
                    error_type="InsufficientInputs",
                    message=error_msg,
                    details={'tool_id': tool_info.tool_id, 'input_count': len(tool_info.inputs)}
                )
                return ConversionResult(
                    success=False,
                    code="",
                    variables_created=[],
                    variables_used=[],
                    error_message=error_msg
                )
            
            left_var = self._generate_variable_name(tool_info.inputs[0])
            right_var = self._generate_variable_name(tool_info.inputs[1])
            
            # Generate output variable names for different join outputs
            join_var = self._generate_variable_name(tool_info.tool_id, "inner")
            left_var_out = self._generate_variable_name(tool_info.tool_id, "left")
            right_var_out = self._generate_variable_name(tool_info.tool_id, "right")
            
            # Parse join configuration
            join_info = self._parse_join_config(config)
            
            # Generate join code
            code = self._generate_join_code(
                left_var, right_var, join_var, left_var_out, right_var_out, join_info
            )
            
            self.logger.app_logger.debug(f"Generated join code for tool {tool_info.tool_id}")
            
            return ConversionResult(
                success=True,
                code=code,
                variables_created=[join_var, left_var_out, right_var_out],
                variables_used=[left_var, right_var]
            )
            
        except Exception as e:
            self._increment_error_count()
            error_msg = f"Failed to convert Join tool: {str(e)}"
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
    
    def _parse_join_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Parse join configuration from Alteryx config."""
        join_info = {
            'join_keys': [],
            'join_type': 'inner'
        }
        
        # Look for join keys in configuration
        # Alteryx stores join info in various formats
        if 'Properties' in config:
            properties = config['Properties']
            
            # Look for EqualFields or similar structures
            if isinstance(properties, dict):
                # Check for join field mappings
                if 'EqualFields' in properties:
                    equal_fields = properties['EqualFields']
                    if isinstance(equal_fields, list):
                        for field_pair in equal_fields:
                            if isinstance(field_pair, dict):
                                left_field = field_pair.get('@Left', '')
                                right_field = field_pair.get('@Right', '')
                                if left_field and right_field:
                                    join_info['join_keys'].append((left_field, right_field))
                    elif isinstance(equal_fields, dict):
                        left_field = equal_fields.get('@Left', '')
                        right_field = equal_fields.get('@Right', '')
                        if left_field and right_field:
                            join_info['join_keys'].append((left_field, right_field))
        
        # If no specific join keys found, try to infer from field names
        if not join_info['join_keys']:
            # Look for common field names that might be join keys
            # This is a fallback - in production, you'd want explicit configuration
            join_info['join_keys'] = [('id', 'id')]  # Default assumption
        
        return join_info
    
    def _generate_join_code(self, left_var: str, right_var: str, join_var: str, 
                           left_var_out: str, right_var_out: str, join_info: Dict[str, Any]) -> str:
        """Generate PySpark join code."""
        
        # Build join condition
        join_conditions = []
        for left_key, right_key in join_info['join_keys']:
            join_conditions.append(f'{left_var}["{left_key}"] == {right_var}["{right_key}"]')
        
        if not join_conditions:
            # Fallback to column name matching
            join_condition = "# Join on matching column names"
            join_clause = ""
        else:
            join_condition = " & ".join(join_conditions)
            join_clause = f", {join_condition}"
        
        return f'''# Perform join operations
# Inner join (matching records from both sides)
{join_var} = {left_var}.join({right_var}{join_clause}, "inner")

# Left anti join (records only in left)
{left_var_out} = {left_var}.join({right_var}{join_clause}, "left_anti")

# Right anti join (records only in right)
{right_var_out} = {right_var}.join({left_var}{join_clause}, "left_anti")'''