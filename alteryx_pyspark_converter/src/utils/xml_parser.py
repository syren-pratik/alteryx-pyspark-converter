"""
XML Parser utility for parsing Alteryx workflow files.

Handles the parsing of .yxmd files and extraction of tools and connections.
"""

import xml.etree.ElementTree as ET
from typing import Dict, Any, List, Optional
from pathlib import Path

from ..core.logger import get_logger


class XMLParser:
    """Parser for Alteryx XML workflow files."""
    
    def __init__(self):
        """Initialize the XML parser."""
        self.logger = get_logger()
    
    def parse_workflow_file(self, file_path: str) -> Optional[Dict[str, Any]]:
        """
        Parse an Alteryx workflow file (.yxmd).
        
        Args:
            file_path: Path to the .yxmd file
            
        Returns:
            Dictionary containing parsed workflow data or None if parsing fails
        """
        try:
            file_path = Path(file_path)
            
            if not file_path.exists():
                self.logger.log_error(
                    component="XMLParser",
                    error_type="FileNotFound",
                    message=f"Workflow file not found: {file_path}"
                )
                return None
            
            # Parse the XML file
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            # Extract workflow information
            workflow_data = {
                'file_path': str(file_path),
                'workflow_name': file_path.stem,
                'tools': self._extract_tools(root),
                'connections': self._extract_connections(root),
                'metadata': self._extract_metadata(root)
            }
            
            self.logger.app_logger.debug(
                f"Parsed workflow: {len(workflow_data['tools'])} tools, "
                f"{len(workflow_data['connections'])} connections"
            )
            
            return workflow_data
            
        except ET.ParseError as e:
            self.logger.log_error(
                component="XMLParser",
                error_type="XMLParseError",
                message=f"Failed to parse XML file: {str(e)}",
                details={'file_path': str(file_path)}
            )
            return None
            
        except Exception as e:
            self.logger.log_error(
                component="XMLParser",
                error_type="UnexpectedError",
                message=f"Unexpected error parsing workflow: {str(e)}",
                details={'file_path': str(file_path)}
            )
            return None
    
    def _extract_tools(self, root: ET.Element) -> List[Dict[str, Any]]:
        """Extract tool information from the XML."""
        tools = []
        
        # Look for Nodes in the XML structure
        nodes = root.findall('.//Node')
        
        for node in nodes:
            tool_id = node.get('ToolID', '')
            
            # Find the tool properties
            gui_settings = node.find('GuiSettings')
            properties = node.find('Properties')
            
            # Extract tool type from the engine DLL
            engine_dll = node.find('EngineSettings/EngineDll')
            tool_type = engine_dll.text if engine_dll is not None else 'Unknown'
            
            # Extract tool configuration
            config = self._extract_tool_config(properties) if properties is not None else {}
            
            # Extract GUI information for position, etc.
            gui_info = self._extract_gui_info(gui_settings) if gui_settings is not None else {}
            
            tool_data = {
                'id': tool_id,
                'type': tool_type,
                'name': self._get_tool_name(tool_type),
                'config': config,
                'gui_info': gui_info,
                'xml_node': node  # Keep reference for detailed parsing if needed
            }
            
            tools.append(tool_data)
            
        return tools
    
    def _extract_connections(self, root: ET.Element) -> List[Dict[str, Any]]:
        """Extract connection information from the XML."""
        connections = []
        
        # Look for Connections in the XML structure
        connection_nodes = root.findall('.//Connection')
        
        for conn in connection_nodes:
            origin = conn.find('Origin')
            destination = conn.find('Destination')
            
            if origin is not None and destination is not None:
                connection_data = {
                    'from_tool': origin.get('ToolID', ''),
                    'from_output': origin.get('Connection', ''),
                    'to_tool': destination.get('ToolID', ''),
                    'to_input': destination.get('Connection', ''),
                    'xml_node': conn
                }
                connections.append(connection_data)
        
        return connections
    
    def _extract_tool_config(self, properties: ET.Element) -> Dict[str, Any]:
        """Extract tool configuration from Properties element."""
        config = {}
        
        # Convert XML properties to dictionary
        config = self._xml_to_dict(properties)
        
        return config
    
    def _extract_gui_info(self, gui_settings: ET.Element) -> Dict[str, Any]:
        """Extract GUI information (position, size, etc.)."""
        gui_info = {}
        
        position = gui_settings.find('Position')
        if position is not None:
            gui_info['x'] = float(position.get('x', 0))
            gui_info['y'] = float(position.get('y', 0))
        
        # Extract other GUI properties as needed
        gui_info.update(self._xml_to_dict(gui_settings))
        
        return gui_info
    
    def _extract_metadata(self, root: ET.Element) -> Dict[str, Any]:
        """Extract workflow metadata."""
        metadata = {}
        
        # Extract various metadata fields
        metadata['alteryx_version'] = root.get('AlteryxVersion', '')
        metadata['xml_version'] = root.get('version', '')
        
        # Look for workflow properties
        properties = root.find('Properties')
        if properties is not None:
            metadata.update(self._xml_to_dict(properties))
        
        return metadata
    
    def _xml_to_dict(self, element: ET.Element) -> Dict[str, Any]:
        """Convert XML element to dictionary recursively."""
        result = {}
        
        # Add attributes
        if element.attrib:
            for key, value in element.attrib.items():
                result[f'@{key}'] = value
        
        # Add text content
        if element.text and element.text.strip():
            if list(element):
                # Has children, use 'text' key
                result['text'] = element.text.strip()
            else:
                # No children, text is the value
                return element.text.strip()
        
        # Process child elements
        children_dict = {}
        for child in element:
            child_data = self._xml_to_dict(child)
            
            if child.tag in children_dict:
                # Multiple children with same tag - convert to list
                if not isinstance(children_dict[child.tag], list):
                    children_dict[child.tag] = [children_dict[child.tag]]
                children_dict[child.tag].append(child_data)
            else:
                children_dict[child.tag] = child_data
        
        result.update(children_dict)
        
        # If only attributes, return them directly
        if len(result) == len(element.attrib):
            return {k.lstrip('@'): v for k, v in result.items()}
        
        return result
    
    def _get_tool_name(self, tool_type: str) -> str:
        """Get human-readable tool name from tool type."""
        tool_name_mapping = {
            'AlteryxBasePluginsGui.DbFileInput.DbFileInput': 'Input Data',
            'AlteryxBasePluginsGui.DbFileOutput.DbFileOutput': 'Output Data',
            'AlteryxBasePluginsGui.AlteryxSelect.AlteryxSelect': 'Select',
            'AlteryxBasePluginsGui.Filter.Filter': 'Filter',
            'AlteryxBasePluginsGui.Formula.Formula': 'Formula',
            'AlteryxBasePluginsGui.Join.Join': 'Join',
            'AlteryxBasePluginsGui.Union.Union': 'Union',
            'AlteryxBasePluginsGui.Sort.Sort': 'Sort',
            'AlteryxBasePluginsGui.Unique.Unique': 'Unique',
            'AlteryxBasePluginsGui.Summarize.Summarize': 'Summarize',
            'AlteryxBasePluginsGui.BrowseV2.BrowseV2': 'Browse',
            'AlteryxBasePluginsGui.TextInput.TextInput': 'Text Input',
            'AlteryxBasePluginsGui.RecordID.RecordID': 'Record ID',
            'AlteryxBasePluginsGui.FindReplace.FindReplace': 'Find Replace'
        }
        
        return tool_name_mapping.get(tool_type, tool_type.split('.')[-1] if '.' in tool_type else tool_type)
    
    def validate_workflow_file(self, file_path: str) -> tuple[bool, List[str]]:
        """
        Validate an Alteryx workflow file.
        
        Args:
            file_path: Path to the workflow file
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        try:
            file_path = Path(file_path)
            
            # Check file existence
            if not file_path.exists():
                errors.append(f"File does not exist: {file_path}")
                return False, errors
            
            # Check file extension
            if file_path.suffix.lower() not in ['.yxmd', '.yxwz']:
                errors.append(f"Invalid file extension: {file_path.suffix}")
            
            # Try to parse XML
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            # Basic structure validation
            if root.tag != 'AlteryxDocument':
                errors.append("Invalid root element - expected 'AlteryxDocument'")
            
            # Check for required elements
            if not root.findall('.//Node'):
                errors.append("No tools found in workflow")
            
        except ET.ParseError as e:
            errors.append(f"XML parsing error: {str(e)}")
        except Exception as e:
            errors.append(f"Validation error: {str(e)}")
        
        return len(errors) == 0, errors