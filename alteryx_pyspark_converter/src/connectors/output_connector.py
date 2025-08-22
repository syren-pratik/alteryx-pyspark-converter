"""
Output Data Connector for Alteryx Output Data tools.

Handles conversion of Alteryx Output Data tools to PySpark write operations.
"""

from typing import Dict, Any, List
from ..core.base_connector import BaseConnector, ToolInfo, ConversionResult
from ..core.logger import get_logger


class OutputDataConnector(BaseConnector):
    """Connector for Alteryx Output Data tools."""
    
    def __init__(self):
        super().__init__(
            connector_name="OutputDataConnector",
            supported_tools=["AlteryxBasePluginsGui.DbFileOutput.DbFileOutput"]
        )
        self.logger = get_logger()
    
    def validate_config(self, config: Dict[str, Any]) -> tuple[bool, List[str]]:
        """Validate Output Data tool configuration."""
        errors = []
        
        # Check for required configuration
        if 'File' not in config:
            errors.append("Missing 'File' configuration")
        
        file_path = config.get('File', '')
        if not file_path:
            errors.append("File path cannot be empty")
        
        return len(errors) == 0, errors
    
    def convert_tool(self, tool_info: ToolInfo) -> ConversionResult:
        """Convert Output Data tool to PySpark code."""
        self._increment_conversion_count()
        
        try:
            config = tool_info.config
            file_path = config.get('File', '')
            
            # Get input DataFrame variable
            if not tool_info.inputs:
                error_msg = "Output tool has no input connections"
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
            
            # Detect output format
            output_format = self._detect_output_format(file_path)
            
            # Generate PySpark write code
            code = self._generate_write_code(input_var, file_path, output_format, config)
            
            self.logger.app_logger.debug(f"Generated write code for {file_path} as {output_format}")
            
            return ConversionResult(
                success=True,
                code=code,
                variables_created=[],
                variables_used=[input_var]
            )
            
        except Exception as e:
            self._increment_error_count()
            error_msg = f"Failed to convert Output Data tool: {str(e)}"
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
    
    def _detect_output_format(self, file_path: str) -> str:
        """Detect output format based on file extension."""
        file_path_lower = file_path.lower()
        
        if file_path_lower.endswith('.csv'):
            return 'csv'
        elif file_path_lower.endswith(('.xlsx', '.xls')):
            return 'excel'
        elif file_path_lower.endswith('.parquet'):
            return 'parquet'
        elif file_path_lower.endswith('.json'):
            return 'json'
        elif file_path_lower.endswith('.txt'):
            return 'text'
        else:
            return 'parquet'  # Default to Parquet for better performance
    
    def _generate_write_code(self, input_var: str, file_path: str, output_format: str, config: Dict[str, Any]) -> str:
        """Generate PySpark write code based on output format."""
        
        # Check for overwrite mode
        write_mode = "overwrite" if config.get('ChangeFileOption', 'Overwrite') == 'Overwrite' else "append"
        
        if output_format == 'csv':
            # Check for delimiter and header options
            delimiter = config.get('Delimiter', ',')
            include_header = config.get('WriteHeaderLine', 'True') == 'True'
            
            if delimiter == '\\t':
                delimiter = '\t'
            
            return f'''# Write to CSV file
{input_var}.write.mode("{write_mode}").option("header", "{str(include_header).lower()}").option("delimiter", "{delimiter}").csv("{file_path}")'''
        
        elif output_format == 'excel':
            # Note: PySpark doesn't natively support Excel writing, need additional library
            return f'''# Write to Excel file (requires spark-excel library)
{input_var}.write.mode("{write_mode}").format("com.crealytics.spark.excel") \\
    .option("header", "true") \\
    .save("{file_path}")'''
        
        elif output_format == 'parquet':
            return f'''# Write to Parquet file
{input_var}.write.mode("{write_mode}").parquet("{file_path}")'''
        
        elif output_format == 'json':
            return f'''# Write to JSON file
{input_var}.write.mode("{write_mode}").json("{file_path}")'''
        
        elif output_format == 'text':
            return f'''# Write to text file
{input_var}.write.mode("{write_mode}").text("{file_path}")'''
        
        else:
            # Default to Parquet
            return f'''# Write to file (Parquet format)
{input_var}.write.mode("{write_mode}").parquet("{file_path}")'''