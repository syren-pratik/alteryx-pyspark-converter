"""
Connector registry for managing and discovering Alteryx tool connectors.

Provides a centralized registry for all connectors with automatic discovery
and factory pattern implementation.
"""

from typing import Dict, List, Type, Optional, Any
import importlib
import pkgutil
from pathlib import Path

from .base_connector import BaseConnector, ToolInfo, ConversionResult
from .logger import get_logger


class ConnectorRegistry:
    """
    Registry for managing Alteryx tool connectors.
    
    Provides automatic discovery, registration, and factory methods for connectors.
    """
    
    def __init__(self):
        """Initialize the connector registry."""
        self._connectors: Dict[str, BaseConnector] = {}
        self._tool_mapping: Dict[str, str] = {}  # tool_type -> connector_name
        self.logger = get_logger()
        
    def register_connector(self, connector: BaseConnector) -> None:
        """
        Register a connector in the registry.
        
        Args:
            connector: The connector instance to register
        """
        connector_name = connector.connector_name
        
        if connector_name in self._connectors:
            self.logger.log_error(
                component="ConnectorRegistry",
                error_type="DuplicateConnector",
                message=f"Connector {connector_name} already registered"
            )
            return
        
        self._connectors[connector_name] = connector
        
        # Map tool types to connector
        for tool_type in connector.supported_tools:
            if tool_type in self._tool_mapping:
                self.logger.log_error(
                    component="ConnectorRegistry",
                    error_type="DuplicateToolMapping",
                    message=f"Tool type {tool_type} already mapped to {self._tool_mapping[tool_type]}"
                )
                continue
            self._tool_mapping[tool_type] = connector_name
            
        self.logger.log_connector_registration(connector_name, connector.supported_tools)
    
    def get_connector(self, tool_type: str) -> Optional[BaseConnector]:
        """
        Get the appropriate connector for a tool type.
        
        Args:
            tool_type: The Alteryx tool type
            
        Returns:
            The connector instance or None if not found
        """
        connector_name = self._tool_mapping.get(tool_type)
        if connector_name:
            return self._connectors.get(connector_name)
        return None
    
    def is_tool_supported(self, tool_type: str) -> bool:
        """Check if a tool type is supported by any connector."""
        return tool_type in self._tool_mapping
    
    def get_supported_tools(self) -> List[str]:
        """Get list of all supported tool types."""
        return list(self._tool_mapping.keys())
    
    def get_all_connectors(self) -> Dict[str, BaseConnector]:
        """Get all registered connectors."""
        return self._connectors.copy()
    
    def convert_tool(self, tool_info: ToolInfo) -> ConversionResult:
        """
        Convert a tool using the appropriate connector.
        
        Args:
            tool_info: Information about the tool to convert
            
        Returns:
            ConversionResult with the generated code or error
        """
        start_time = self.logger.app_logger.info(f"Converting tool: {tool_info.tool_type} (ID: {tool_info.tool_id})")
        
        connector = self.get_connector(tool_info.tool_type)
        
        if not connector:
            error_msg = f"No connector found for tool type: {tool_info.tool_type}"
            self.logger.log_error(
                component="ConnectorRegistry",
                error_type="UnsupportedTool",
                message=error_msg,
                details={'tool_id': tool_info.tool_id, 'tool_type': tool_info.tool_type}
            )
            self.logger.log_tool_usage(tool_info.tool_type, tool_info.tool_id, supported=False)
            
            return ConversionResult(
                success=False,
                code="",
                variables_created=[],
                variables_used=[],
                error_message=error_msg
            )
        
        try:
            # Validate configuration first
            is_valid, validation_errors = connector.validate_config(tool_info.config)
            if not is_valid:
                error_msg = f"Invalid configuration for {tool_info.tool_type}: {'; '.join(validation_errors)}"
                self.logger.log_error(
                    component=connector.connector_name,
                    error_type="ConfigurationValidation",
                    message=error_msg,
                    details={'tool_id': tool_info.tool_id, 'validation_errors': validation_errors}
                )
                return ConversionResult(
                    success=False,
                    code="",
                    variables_created=[],
                    variables_used=[],
                    error_message=error_msg
                )
            
            # Convert the tool
            result = connector.convert_tool(tool_info)
            
            # Log success/failure
            if result.success:
                self.logger.log_tool_usage(tool_info.tool_type, tool_info.tool_id, supported=True)
                connector._increment_conversion_count()
            else:
                self.logger.log_error(
                    component=connector.connector_name,
                    error_type="ConversionFailure",
                    message=result.error_message or "Unknown conversion error",
                    details={'tool_id': tool_info.tool_id}
                )
                connector._increment_error_count()
            
            return result
            
        except Exception as e:
            error_msg = f"Exception during conversion of {tool_info.tool_type}: {str(e)}"
            self.logger.log_error(
                component=connector.connector_name,
                error_type="UnhandledException",
                message=error_msg,
                details={'tool_id': tool_info.tool_id, 'exception': str(e)}
            )
            connector._increment_error_count()
            
            return ConversionResult(
                success=False,
                code="",
                variables_created=[],
                variables_used=[],
                error_message=error_msg
            )
    
    def auto_discover_connectors(self, connectors_package: str = "alteryx_pyspark_converter.src.connectors") -> int:
        """
        Automatically discover and register all connectors in the connectors package.
        
        Args:
            connectors_package: Package path containing connector modules
            
        Returns:
            Number of connectors discovered and registered
        """
        discovered_count = 0
        
        try:
            # Import the connectors package
            package = importlib.import_module(connectors_package)
            package_path = package.__path__
            
            # Iterate through all modules in the package
            for finder, module_name, ispkg in pkgutil.iter_modules(package_path):
                if ispkg:
                    continue
                    
                try:
                    # Import the module
                    full_module_name = f"{connectors_package}.{module_name}"
                    module = importlib.import_module(full_module_name)
                    
                    # Look for connector classes
                    for attr_name in dir(module):
                        attr = getattr(module, attr_name)
                        
                        # Check if it's a connector class (subclass of BaseConnector but not BaseConnector itself)
                        if (isinstance(attr, type) and 
                            issubclass(attr, BaseConnector) and 
                            attr != BaseConnector):
                            
                            try:
                                # Instantiate and register the connector
                                connector_instance = attr()
                                self.register_connector(connector_instance)
                                discovered_count += 1
                                
                            except Exception as e:
                                self.logger.log_error(
                                    component="ConnectorRegistry",
                                    error_type="ConnectorInstantiation",
                                    message=f"Failed to instantiate connector {attr_name}",
                                    details={'module': full_module_name, 'error': str(e)}
                                )
                                
                except Exception as e:
                    self.logger.log_error(
                        component="ConnectorRegistry",
                        error_type="ModuleImport",
                        message=f"Failed to import connector module {module_name}",
                        details={'error': str(e)}
                    )
                    
        except Exception as e:
            self.logger.log_error(
                component="ConnectorRegistry",
                error_type="PackageDiscovery",
                message=f"Failed to discover connectors in package {connectors_package}",
                details={'error': str(e)}
            )
        
        self.logger.app_logger.info(f"Auto-discovered {discovered_count} connectors")
        return discovered_count
    
    def get_registry_statistics(self) -> Dict[str, Any]:
        """Get comprehensive statistics about the registry."""
        connector_stats = {}
        total_conversions = 0
        total_errors = 0
        
        for name, connector in self._connectors.items():
            stats = connector.get_statistics()
            connector_stats[name] = stats
            total_conversions += stats['conversion_count']
            total_errors += stats['error_count']
        
        return {
            'total_connectors': len(self._connectors),
            'total_supported_tools': len(self._tool_mapping),
            'supported_tool_types': list(self._tool_mapping.keys()),
            'total_conversions': total_conversions,
            'total_errors': total_errors,
            'overall_success_rate': (total_conversions - total_errors) / max(1, total_conversions) * 100,
            'connector_statistics': connector_stats
        }


# Global registry instance
_registry_instance = None

def get_registry() -> ConnectorRegistry:
    """Get the global connector registry instance."""
    global _registry_instance
    if _registry_instance is None:
        _registry_instance = ConnectorRegistry()
    return _registry_instance