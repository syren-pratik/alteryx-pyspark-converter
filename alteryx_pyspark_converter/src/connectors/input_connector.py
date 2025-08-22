"""
Input Data Connector for Alteryx Input Data tools.

Handles conversion of Alteryx Input Data tools to PySpark read operations.
"""

from typing import Dict, Any, List
from ..core.base_connector import BaseConnector, ToolInfo, ConversionResult
from ..core.logger import get_logger


class InputDataConnector(BaseConnector):
    """Connector for Alteryx Input Data tools."""
    
    def __init__(self):
        super().__init__(
            connector_name="InputDataConnector",
            supported_tools=["AlteryxBasePluginsGui.DbFileInput.DbFileInput"]
        )
        self.logger = get_logger()
    
    def validate_config(self, config: Dict[str, Any]) -> tuple[bool, List[str]]:
        """Validate Input Data tool configuration."""
        errors = []
        
        # Check for required configuration
        if 'File' not in config:
            errors.append("Missing 'File' configuration")
        
        file_path = config.get('File', '')
        if not file_path:
            errors.append("File path cannot be empty")
        
        return len(errors) == 0, errors
    
    def convert_tool(self, tool_info: ToolInfo) -> ConversionResult:
        """Convert Input Data tool to PySpark code."""
        self._increment_conversion_count()
        
        try:
            config = tool_info.config
            file_path = config.get('File', '')
            
            # Detect file format
            file_format = self._detect_file_format(file_path)
            
            # Generate variable name
            var_name = self._generate_variable_name(tool_info.tool_id)
            
            # Generate PySpark read code based on file format
            code = self._generate_read_code(var_name, file_path, file_format, config)
            
            self.logger.app_logger.debug(f"Generated read code for {file_path} as {file_format}")
            
            return ConversionResult(
                success=True,
                code=code,
                variables_created=[var_name],
                variables_used=[]
            )
            
        except Exception as e:
            self._increment_error_count()
            error_msg = f"Failed to convert Input Data tool: {str(e)}"
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
    
    def _detect_file_format(self, file_path: str) -> str:
        """Detect file format based on file extension."""
        file_path_lower = file_path.lower()
        
        if file_path_lower.endswith('.csv'):
            return 'csv'
        elif file_path_lower.endswith(('.xlsx', '.xls')):
            return 'excel'
        elif file_path_lower.endswith('.parquet'):
            return 'parquet'
        elif file_path_lower.endswith('.json'):
            return 'json'
        elif file_path_lower.endswith('.sql'):
            return 'sql'
        elif file_path_lower.endswith('.txt'):
            return 'text'
        else:
            return 'csv'  # Default fallback
    
    def _generate_read_code(self, var_name: str, file_path: str, file_format: str, config: Dict[str, Any]) -> str:
        """Generate PySpark read code based on file format."""
        
        if file_format == 'csv':
            # Check for delimiter and header options
            delimiter = config.get('Delimiter', ',')
            has_header = config.get('FirstRowData', 'True') == 'False'  # Alteryx logic is inverted
            
            if delimiter == '\\t':
                delimiter = '\t'
            
            return f'''# Read CSV file
{var_name} = spark.read.option("header", "{str(has_header).lower()}").option("delimiter", "{delimiter}").csv("{file_path}")'''
        
        elif file_format == 'excel':
            # Note: PySpark doesn't natively support Excel, need additional library
            sheet_name = config.get('Sheet', 'Sheet1')
            return f'''# Read Excel file (requires spark-excel library)
{var_name} = spark.read.format("com.crealytics.spark.excel") \\
    .option("header", "true") \\
    .option("sheetName", "{sheet_name}") \\
    .load("{file_path}")'''
        
        elif file_format == 'parquet':
            return f'''# Read Parquet file
{var_name} = spark.read.parquet("{file_path}")'''
        
        elif file_format == 'json':
            return f'''# Read JSON file
{var_name} = spark.read.json("{file_path}")'''
        
        elif file_format == 'sql':
            return f'''# Read SQL file as text
{var_name}_text = spark.read.text("{file_path}")
# Note: Execute the SQL query from the file content
# {var_name} = spark.sql({var_name}_text.collect()[0][0])'''
        
        elif file_format == 'text':
            return f'''# Read text file
{var_name} = spark.read.text("{file_path}")'''
        
        else:
            # Default to CSV
            return f'''# Read file (assuming CSV format)
{var_name} = spark.read.option("header", "true").csv("{file_path}")'''