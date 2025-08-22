"""
Base connector interface for Alteryx tool converters.

Defines the contract that all tool connectors must implement.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from dataclasses import dataclass


@dataclass
class ToolInfo:
    """Information about a tool being processed."""
    tool_id: str
    tool_type: str
    tool_name: str
    config: Dict[str, Any]
    inputs: List[str]
    outputs: List[str]


@dataclass
class ConversionResult:
    """Result of a tool conversion."""
    success: bool
    code: str
    variables_created: List[str]
    variables_used: List[str]
    error_message: Optional[str] = None
    warnings: List[str] = None
    
    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []


class BaseConnector(ABC):
    """
    Abstract base class for all Alteryx tool connectors.
    
    Each connector is responsible for converting a specific type of Alteryx tool
    to equivalent PySpark code.
    """
    
    def __init__(self, connector_name: str, supported_tools: List[str]):
        """
        Initialize the connector.
        
        Args:
            connector_name: Name of the connector
            supported_tools: List of Alteryx tool types this connector supports
        """
        self.connector_name = connector_name
        self.supported_tools = supported_tools
        self._conversion_count = 0
        self._error_count = 0
    
    @abstractmethod
    def convert_tool(self, tool_info: ToolInfo) -> ConversionResult:
        """
        Convert an Alteryx tool to PySpark code.
        
        Args:
            tool_info: Information about the tool to convert
            
        Returns:
            ConversionResult with the generated code and metadata
        """
        pass
    
    @abstractmethod
    def validate_config(self, config: Dict[str, Any]) -> tuple[bool, List[str]]:
        """
        Validate the tool configuration.
        
        Args:
            config: Tool configuration dictionary
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        pass
    
    def supports_tool(self, tool_type: str) -> bool:
        """Check if this connector supports the given tool type."""
        return tool_type in self.supported_tools
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get usage statistics for this connector."""
        return {
            'connector_name': self.connector_name,
            'supported_tools': self.supported_tools,
            'conversion_count': self._conversion_count,
            'error_count': self._error_count,
            'success_rate': (self._conversion_count - self._error_count) / max(1, self._conversion_count) * 100
        }
    
    def _increment_conversion_count(self):
        """Increment the conversion counter."""
        self._conversion_count += 1
    
    def _increment_error_count(self):
        """Increment the error counter."""
        self._error_count += 1
    
    def _generate_variable_name(self, tool_id: str, suffix: str = "") -> str:
        """Generate a PySpark DataFrame variable name."""
        clean_id = "".join(c for c in tool_id if c.isalnum() or c == "_")
        if suffix:
            return f"df_{clean_id}_{suffix}"
        return f"df_{clean_id}"
    
    def _format_pyspark_imports(self) -> str:
        """Get common PySpark imports needed by most connectors."""
        return """from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window"""
    
    def _escape_sql_string(self, value: str) -> str:
        """Escape a string for use in SQL expressions."""
        return value.replace("'", "''").replace("\\", "\\\\")
    
    def _parse_alteryx_expression(self, expression: str) -> str:
        """
        Parse an Alteryx expression and convert to PySpark equivalent.
        
        This is a basic implementation that can be overridden by specific connectors.
        """
        # Basic replacements for common Alteryx functions
        replacements = {
            '[': 'col("',
            ']': '")',
            'ISNULL': 'isnull',
            'NOT ': '~',
            ' AND ': ' & ',
            ' OR ': ' | ',
            'REGEX_Match': 'regexp_like',
            'ToNumber': 'cast',
            'ToString': 'cast'
        }
        
        result = expression
        for old, new in replacements.items():
            result = result.replace(old, new)
            
        return result