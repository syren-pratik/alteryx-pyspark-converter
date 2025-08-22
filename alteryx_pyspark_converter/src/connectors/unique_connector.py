"""
Unique Connector for Alteryx Unique tools.

Handles conversion of Alteryx Unique tools to PySpark distinct operations.
"""

from typing import Dict, Any, List
from ..core.base_connector import BaseConnector, ToolInfo, ConversionResult
from ..core.logger import get_logger


class UniqueConnector(BaseConnector):
    """Connector for Alteryx Unique tools."""
    
    def __init__(self):
        super().__init__(
            connector_name="UniqueConnector",
            supported_tools=["AlteryxBasePluginsGui.Unique.Unique"]
        )
        self.logger = get_logger()
    
    def validate_config(self, config: Dict[str, Any]) -> tuple[bool, List[str]]:
        """Validate Unique tool configuration."""
        errors = []
        # Unique tool can work without specific configuration
        return True, errors
    
    def convert_tool(self, tool_info: ToolInfo) -> ConversionResult:
        """Convert Unique tool to PySpark code."""
        self._increment_conversion_count()
        
        try:
            config = tool_info.config
            
            # Get input DataFrame variable
            if not tool_info.inputs:
                error_msg = "Unique tool has no input connections"
                return ConversionResult(
                    success=False,
                    code="",
                    variables_created=[],
                    variables_used=[],
                    error_message=error_msg
                )
            
            input_var = self._generate_variable_name(tool_info.inputs[0])
            unique_output_var = self._generate_variable_name(tool_info.tool_id, "unique")
            duplicate_output_var = self._generate_variable_name(tool_info.tool_id, "duplicates")
            
            # Generate unique code
            code = self._generate_unique_code(input_var, unique_output_var, duplicate_output_var, config)
            
            return ConversionResult(
                success=True,
                code=code,
                variables_created=[unique_output_var, duplicate_output_var],
                variables_used=[input_var]
            )
            
        except Exception as e:
            self._increment_error_count()
            error_msg = f"Failed to convert Unique tool: {str(e)}"
            
            return ConversionResult(
                success=False,
                code="",
                variables_created=[],
                variables_used=[],
                error_message=error_msg
            )
    
    def _generate_unique_code(self, input_var: str, unique_var: str, duplicate_var: str, config: Dict[str, Any]) -> str:
        """Generate PySpark unique code."""
        
        return f'''# Remove duplicates and identify unique/duplicate records
{unique_var} = {input_var}.distinct()
{duplicate_var} = {input_var}.subtract({unique_var})'''