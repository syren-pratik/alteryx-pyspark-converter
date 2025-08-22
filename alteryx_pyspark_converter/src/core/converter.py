"""
Main Alteryx to PySpark Converter.

Orchestrates the conversion process using the connector registry and logging system.
"""

import xml.etree.ElementTree as ET
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
import time
from collections import defaultdict, deque
import concurrent.futures

from .base_connector import ToolInfo, ConversionResult
from .registry import get_registry
from .logger import get_logger
from ..utils.xml_parser import XMLParser
from ..utils.dependency_resolver import DependencyResolver
from ..utils.code_formatter import CodeFormatter
from ...config.settings import Settings


class ConversionContext:
    """Context object to track conversion state."""
    
    def __init__(self, workflow_name: str):
        self.workflow_name = workflow_name
        self.start_time = time.time()
        self.variables_created = {}  # tool_id -> variable_name
        self.variables_used = defaultdict(list)  # variable_name -> [tool_ids]
        self.errors = []
        self.warnings = []
        self.performance_metrics = {}
        
    def add_variable(self, tool_id: str, variable_name: str):
        """Track a variable created by a tool."""
        self.variables_created[tool_id] = variable_name
        
    def use_variable(self, variable_name: str, tool_id: str):
        """Track usage of a variable by a tool."""
        self.variables_used[variable_name].append(tool_id)
        
    def add_error(self, error: str):
        """Add an error to the context."""
        self.errors.append(error)
        
    def add_warning(self, warning: str):
        """Add a warning to the context."""
        self.warnings.append(warning)
        
    def get_duration(self) -> float:
        """Get conversion duration in seconds."""
        return time.time() - self.start_time


class AlteryxPySparkConverter:
    """
    Main converter class that orchestrates the conversion of Alteryx workflows to PySpark code.
    
    This class provides the high-level interface for converting .yxmd files and manages
    the entire conversion pipeline.
    """
    
    def __init__(self):
        """Initialize the converter."""
        self.registry = get_registry()
        self.logger = get_logger()
        self.xml_parser = XMLParser()
        self.dependency_resolver = DependencyResolver()
        self.code_formatter = CodeFormatter()
        
        # Auto-discover and register connectors
        if Settings.AUTO_DISCOVER_CONNECTORS:
            discovered_count = self.registry.auto_discover_connectors()
            self.logger.app_logger.info(f"Auto-discovered {discovered_count} connectors")
    
    def convert_workflow_file(self, file_path: str) -> Dict[str, Any]:
        """
        Convert an Alteryx workflow file to PySpark code.
        
        Args:
            file_path: Path to the .yxmd file
            
        Returns:
            Dictionary containing conversion results
        """
        file_path = Path(file_path)
        workflow_name = file_path.stem
        
        self.logger.app_logger.info(f"Starting conversion of workflow: {workflow_name}")
        context = ConversionContext(workflow_name)
        
        try:
            # Parse the XML workflow
            workflow_data = self.xml_parser.parse_workflow_file(str(file_path))
            if not workflow_data:
                error_msg = f"Failed to parse workflow file: {file_path}"
                context.add_error(error_msg)
                self.logger.log_conversion_failure(workflow_name, error_msg)
                return self._create_error_result(context, error_msg)
            
            # Extract tools and connections
            tools = workflow_data.get('tools', [])
            connections = workflow_data.get('connections', [])
            
            if not tools:
                error_msg = "No tools found in workflow"
                context.add_error(error_msg)
                self.logger.log_conversion_failure(workflow_name, error_msg)
                return self._create_error_result(context, error_msg)
            
            self.logger.log_conversion_start(workflow_name, len(tools))
            
            # Resolve dependencies and sort tools
            sorted_tools = self.dependency_resolver.sort_tools_by_dependencies(tools, connections)
            
            # Convert tools to PySpark code
            conversion_results = self._convert_tools(sorted_tools, connections, context)
            
            # Generate final code
            final_code = self._generate_final_code(conversion_results, context)
            
            # Calculate statistics
            stats = self._calculate_conversion_stats(tools, conversion_results, context)
            
            # Log successful conversion
            self.logger.log_conversion_success(
                workflow_name, 
                stats['supported_tools'], 
                stats['unsupported_tools']
            )
            
            return {
                'success': True,
                'workflow_name': workflow_name,
                'code': final_code,
                'statistics': stats,
                'conversion_time': context.get_duration(),
                'errors': context.errors,
                'warnings': context.warnings,
                'tools': tools,
                'connections': connections,
                'sorted_tools': sorted_tools
            }
            
        except Exception as e:
            error_msg = f"Conversion failed with exception: {str(e)}"
            context.add_error(error_msg)
            self.logger.log_conversion_failure(workflow_name, error_msg)
            return self._create_error_result(context, error_msg)
    
    def _convert_tools(self, tools: List[Dict[str, Any]], connections: List[Dict[str, Any]], 
                      context: ConversionContext) -> List[ConversionResult]:
        """Convert a list of tools to PySpark code."""
        results = []
        
        # Build connection mapping for quick lookup
        connection_map = defaultdict(list)
        for conn in connections:
            connection_map[conn['to_tool']].append(conn['from_tool'])
        
        # Convert tools in dependency order
        for tool in tools:
            tool_id = tool.get('id', '')
            tool_type = tool.get('type', '')
            
            # Get input connections
            input_tools = connection_map.get(tool_id, [])
            
            # Create tool info
            tool_info = ToolInfo(
                tool_id=tool_id,
                tool_type=tool_type,
                tool_name=tool.get('name', tool_type),
                config=tool.get('config', {}),
                inputs=input_tools,
                outputs=[]  # Will be filled by dependency resolver if needed
            )
            
            # Convert the tool
            start_time = time.time()
            result = self.registry.convert_tool(tool_info)
            conversion_time = time.time() - start_time
            
            # Track performance
            context.performance_metrics[tool_id] = {
                'tool_type': tool_type,
                'conversion_time': conversion_time,
                'success': result.success
            }
            
            # Update context with variables
            for var in result.variables_created:
                context.add_variable(tool_id, var)
            
            for var in result.variables_used:
                context.use_variable(var, tool_id)
            
            # Track errors and warnings
            if not result.success and result.error_message:
                context.add_error(f"Tool {tool_id} ({tool_type}): {result.error_message}")
            
            for warning in result.warnings:
                context.add_warning(f"Tool {tool_id} ({tool_type}): {warning}")
            
            results.append(result)
            
            # Check error limits
            if Settings.STRICT_MODE and not result.success:
                break
            
            if len(context.errors) >= Settings.MAX_ERRORS_PER_WORKFLOW:
                context.add_warning("Maximum error limit reached, stopping conversion")
                break
        
        return results
    
    def _generate_final_code(self, results: List[ConversionResult], context: ConversionContext) -> str:
        """Generate the final PySpark code from conversion results."""
        
        # Start with imports and session initialization
        code_sections = [
            "# Generated PySpark code from Alteryx workflow",
            f"# Workflow: {context.workflow_name}",
            f"# Generated on: {time.strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "# Import required libraries",
            "from pyspark.sql import SparkSession",
            "from pyspark.sql.functions import *",
            "from pyspark.sql.types import *",
            "from pyspark.sql.window import Window",
            "",
            Settings.get_pyspark_session_code(),
            ""
        ]
        
        # Add tool-specific code
        code_sections.append("# Workflow Steps")
        for i, result in enumerate(results):
            if result.success and result.code:
                code_sections.append(f"# Step {i+1}")
                code_sections.append(result.code)
                code_sections.append("")
        
        # Add cleanup code
        if Settings.OUTPUT_CODE_STYLE == "commented":
            code_sections.extend([
                "# Cleanup",
                "# spark.stop()  # Uncomment to stop Spark session"
            ])
        
        # Join all sections
        final_code = "\n".join(code_sections)
        
        # Format the code
        if Settings.OUTPUT_CODE_STYLE == "formatted":
            final_code = self.code_formatter.format_code(final_code)
        
        return final_code
    
    def _calculate_conversion_stats(self, tools: List[Dict[str, Any]], 
                                  results: List[ConversionResult], 
                                  context: ConversionContext) -> Dict[str, Any]:
        """Calculate conversion statistics."""
        
        total_tools = len(tools)
        successful_conversions = sum(1 for r in results if r.success)
        failed_conversions = total_tools - successful_conversions
        
        # Count tool types
        tool_type_counts = defaultdict(int)
        supported_tool_types = defaultdict(int)
        
        for i, tool in enumerate(tools):
            tool_type = tool.get('type', 'Unknown')
            tool_type_counts[tool_type] += 1
            
            if i < len(results) and results[i].success:
                supported_tool_types[tool_type] += 1
        
        # Calculate support rate
        support_rate = (successful_conversions / total_tools * 100) if total_tools > 0 else 0
        
        return {
            'total_tools': total_tools,
            'supported_tools': successful_conversions,
            'unsupported_tools': failed_conversions,
            'support_rate': round(support_rate, 2),
            'tool_type_counts': dict(tool_type_counts),
            'supported_tool_types': dict(supported_tool_types),
            'conversion_time': context.get_duration(),
            'performance_metrics': context.performance_metrics,
            'error_count': len(context.errors),
            'warning_count': len(context.warnings)
        }
    
    def _create_error_result(self, context: ConversionContext, error_msg: str) -> Dict[str, Any]:
        """Create an error result dictionary."""
        return {
            'success': False,
            'workflow_name': context.workflow_name,
            'code': f"# Error: {error_msg}",
            'error_message': error_msg,
            'statistics': {
                'total_tools': 0,
                'supported_tools': 0,
                'unsupported_tools': 0,
                'support_rate': 0,
                'conversion_time': context.get_duration(),
                'error_count': len(context.errors),
                'warning_count': len(context.warnings)
            },
            'conversion_time': context.get_duration(),
            'errors': context.errors,
            'warnings': context.warnings
        }
    
    def get_supported_tools(self) -> List[str]:
        """Get list of supported tool types."""
        return self.registry.get_supported_tools()
    
    def get_converter_statistics(self) -> Dict[str, Any]:
        """Get comprehensive converter statistics."""
        registry_stats = self.registry.get_registry_statistics()
        logger_stats = self.logger.get_tool_usage_statistics()
        
        return {
            'registry_stats': registry_stats,
            'usage_stats': logger_stats,
            'settings': Settings.get_environment_info()
        }
    
    def export_analytics_report(self, output_file: Optional[str] = None) -> str:
        """Export comprehensive analytics report."""
        return self.logger.export_analytics_report(output_file)