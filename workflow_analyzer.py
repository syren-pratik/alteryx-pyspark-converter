#!/usr/bin/env python3
"""
Comprehensive Alteryx Workflow Analyzer

Scans all YXMD files, extracts tool usage, creates a master database,
and verifies code coverage for each tool type.
"""

import os
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Any, Set
from collections import defaultdict
import json
from datetime import datetime
import csv


class WorkflowAnalyzer:
    """Analyzes all Alteryx workflows and creates a comprehensive database."""
    
    def __init__(self, directory_path: str):
        """Initialize the analyzer with a directory path."""
        self.directory_path = Path(directory_path)
        self.tool_database = defaultdict(lambda: {
            'count': 0,
            'files': [],
            'configurations': [],
            'connections': []
        })
        self.file_statistics = {}
        self.supported_tools = set()
        self.unsupported_tools = set()
        
    def scan_all_workflows(self) -> Dict[str, Any]:
        """Scan all YXMD files in the directory and subdirectories."""
        print(f"\n{'='*80}")
        print("ALTERYX WORKFLOW ANALYZER")
        print(f"{'='*80}\n")
        
        yxmd_files = list(self.directory_path.rglob("*.yxmd"))
        print(f"Found {len(yxmd_files)} YXMD files to analyze\n")
        
        for i, file_path in enumerate(yxmd_files, 1):
            print(f"[{i}/{len(yxmd_files)}] Analyzing: {file_path.name}")
            self._analyze_workflow(file_path)
        
        return self._generate_report()
    
    def _analyze_workflow(self, file_path: Path) -> None:
        """Analyze a single workflow file."""
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            # Extract tools
            tools = self._extract_tools(root)
            connections = self._extract_connections(root)
            
            # Store file statistics
            self.file_statistics[str(file_path)] = {
                'name': file_path.name,
                'total_tools': len(tools),
                'unique_tool_types': len(set(t['type'] for t in tools)),
                'connections': len(connections),
                'tools': tools
            }
            
            # Update master database
            for tool in tools:
                tool_type = tool['type']
                self.tool_database[tool_type]['count'] += 1
                self.tool_database[tool_type]['files'].append(file_path.name)
                
                # Store sample configuration
                if len(self.tool_database[tool_type]['configurations']) < 3:
                    self.tool_database[tool_type]['configurations'].append({
                        'file': file_path.name,
                        'config': tool.get('config', {})
                    })
            
            # Track connections
            for conn in connections:
                for tool in tools:
                    if tool['id'] == conn['from_tool']:
                        from_type = tool['type']
                        for tool2 in tools:
                            if tool2['id'] == conn['to_tool']:
                                to_type = tool2['type']
                                self.tool_database[from_type]['connections'].append(to_type)
                                break
                        break
            
        except Exception as e:
            print(f"  ERROR: Failed to parse {file_path.name}: {e}")
            self.file_statistics[str(file_path)] = {
                'name': file_path.name,
                'error': str(e)
            }
    
    def _extract_tools(self, root: ET.Element) -> List[Dict[str, Any]]:
        """Extract tools from XML."""
        tools = []
        nodes = root.findall('.//Node')
        
        for node in nodes:
            tool_id = node.get('ToolID', '')
            
            # Get tool type from GuiSettings Plugin attribute
            gui_settings = node.find('GuiSettings')
            tool_type = 'Unknown'
            if gui_settings is not None and 'Plugin' in gui_settings.attrib:
                tool_type = gui_settings.get('Plugin', 'Unknown')
            
            # Get configuration
            properties = node.find('Properties')
            config_element = properties.find('Configuration') if properties is not None else None
            config = self._xml_to_dict(config_element) if config_element is not None else {}
            
            tools.append({
                'id': tool_id,
                'type': tool_type,
                'config': config
            })
        
        return tools
    
    def _extract_connections(self, root: ET.Element) -> List[Dict[str, Any]]:
        """Extract connections from XML."""
        connections = []
        connection_nodes = root.findall('.//Connection')
        
        for conn in connection_nodes:
            origin = conn.find('Origin')
            destination = conn.find('Destination')
            
            if origin is not None and destination is not None:
                connections.append({
                    'from_tool': origin.get('ToolID', ''),
                    'from_output': origin.get('Connection', ''),
                    'to_tool': destination.get('ToolID', ''),
                    'to_input': destination.get('Connection', '')
                })
        
        return connections
    
    def _xml_to_dict(self, element: ET.Element) -> Dict[str, Any]:
        """Convert XML element to dictionary."""
        if element is None:
            return {}
            
        result = {}
        
        # Add attributes
        if element.attrib:
            for key, value in element.attrib.items():
                result[f'@{key}'] = value
        
        # Add text content
        if element.text and element.text.strip():
            if list(element):
                result['text'] = element.text.strip()
            else:
                return element.text.strip()
        
        # Process children
        for child in element:
            child_data = self._xml_to_dict(child)
            if child.tag in result:
                if not isinstance(result[child.tag], list):
                    result[child.tag] = [result[child.tag]]
                result[child.tag].append(child_data)
            else:
                result[child.tag] = child_data
        
        return result
    
    def _generate_report(self) -> Dict[str, Any]:
        """Generate comprehensive report."""
        # Sort tools by usage
        sorted_tools = sorted(
            self.tool_database.items(),
            key=lambda x: x[1]['count'],
            reverse=True
        )
        
        # Calculate statistics
        total_tools_found = sum(data['count'] for _, data in sorted_tools)
        unique_tool_types = len(sorted_tools)
        
        print(f"\n{'='*80}")
        print("ANALYSIS COMPLETE")
        print(f"{'='*80}\n")
        
        print(f"Total tools found: {total_tools_found}")
        print(f"Unique tool types: {unique_tool_types}")
        print(f"Files analyzed: {len(self.file_statistics)}")
        
        return {
            'summary': {
                'total_tools_found': total_tools_found,
                'unique_tool_types': unique_tool_types,
                'files_analyzed': len(self.file_statistics),
                'timestamp': datetime.now().isoformat()
            },
            'tool_database': dict(self.tool_database),
            'file_statistics': self.file_statistics,
            'sorted_tools': sorted_tools
        }
    
    def save_database(self, output_file: str = "alteryx_tool_database.json"):
        """Save the database to a JSON file."""
        report = self._generate_report()
        
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"\nDatabase saved to: {output_file}")
        
        # Also save a CSV for easy viewing
        self._save_csv_report(report)
    
    def _save_csv_report(self, report: Dict[str, Any]):
        """Save a CSV report of tool usage."""
        csv_file = "alteryx_tool_usage_report.csv"
        
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Tool Type', 'Usage Count', 'Files Count', 'Example Files'])
            
            for tool_type, data in report['sorted_tools']:
                example_files = ', '.join(list(set(data['files']))[:3])
                writer.writerow([
                    tool_type,
                    data['count'],
                    len(set(data['files'])),
                    example_files
                ])
        
        print(f"CSV report saved to: {csv_file}")
    
    def verify_code_coverage(self, simple_converter) -> Dict[str, Any]:
        """Verify which tools are supported by our converter."""
        print(f"\n{'='*80}")
        print("CODE COVERAGE VERIFICATION")
        print(f"{'='*80}\n")
        
        coverage_report = {
            'supported': [],
            'unsupported': [],
            'coverage_percentage': 0
        }
        
        for tool_type, data in self.tool_database.items():
            if simple_converter._is_tool_supported(tool_type):
                coverage_report['supported'].append({
                    'tool': tool_type,
                    'usage_count': data['count']
                })
            else:
                coverage_report['unsupported'].append({
                    'tool': tool_type,
                    'usage_count': data['count']
                })
        
        # Calculate coverage
        total_usage = sum(data['count'] for _, data in self.tool_database.items())
        supported_usage = sum(item['usage_count'] for item in coverage_report['supported'])
        
        coverage_report['coverage_percentage'] = (supported_usage / total_usage * 100) if total_usage > 0 else 0
        
        print(f"Supported tools: {len(coverage_report['supported'])}")
        print(f"Unsupported tools: {len(coverage_report['unsupported'])}")
        print(f"Coverage by usage: {coverage_report['coverage_percentage']:.2f}%")
        
        # Show top unsupported tools
        print("\nTop 10 unsupported tools by usage:")
        unsupported_sorted = sorted(coverage_report['unsupported'], key=lambda x: x['usage_count'], reverse=True)
        for i, item in enumerate(unsupported_sorted[:10], 1):
            print(f"  {i}. {item['tool']}: {item['usage_count']} uses")
        
        return coverage_report
    
    def print_usage_statistics(self):
        """Print detailed usage statistics."""
        print(f"\n{'='*80}")
        print("TOOL USAGE STATISTICS")
        print(f"{'='*80}\n")
        
        sorted_tools = sorted(
            self.tool_database.items(),
            key=lambda x: x[1]['count'],
            reverse=True
        )
        
        print("Top 20 Most Used Tools:")
        print(f"{'Rank':<5} {'Tool Type':<60} {'Count':<10} {'Files'}")
        print("-" * 90)
        
        for i, (tool_type, data) in enumerate(sorted_tools[:20], 1):
            tool_name = tool_type.split('.')[-1] if '.' in tool_type else tool_type
            file_count = len(set(data['files']))
            print(f"{i:<5} {tool_name:<60} {data['count']:<10} {file_count}")
        
        # Print tool connection patterns
        print(f"\n{'='*80}")
        print("COMMON TOOL CONNECTION PATTERNS")
        print(f"{'='*80}\n")
        
        connection_patterns = defaultdict(int)
        for tool_type, data in self.tool_database.items():
            for connected_tool in data['connections']:
                pattern = f"{tool_type.split('.')[-1]} -> {connected_tool.split('.')[-1]}"
                connection_patterns[pattern] += 1
        
        sorted_patterns = sorted(connection_patterns.items(), key=lambda x: x[1], reverse=True)
        
        print("Top 10 Connection Patterns:")
        for i, (pattern, count) in enumerate(sorted_patterns[:10], 1):
            print(f"  {i}. {pattern}: {count} times")


def main():
    """Main function to run the analyzer."""
    # Directory containing YXMD files
    directory = "/Users/pratikbharuka/Downloads/alteryx_latest_code/unread_alteryx_files"
    
    # Create analyzer
    analyzer = WorkflowAnalyzer(directory)
    
    # Scan all workflows
    analyzer.scan_all_workflows()
    
    # Save database
    analyzer.save_database()
    
    # Print usage statistics
    analyzer.print_usage_statistics()
    
    # Verify code coverage with simple converter
    try:
        from simple_converter_backup import SimpleAlteryxConverter
        converter = SimpleAlteryxConverter()
        coverage_report = analyzer.verify_code_coverage(converter)
        
        # Save coverage report
        with open("code_coverage_report.json", 'w') as f:
            json.dump(coverage_report, f, indent=2)
        print("\nCode coverage report saved to: code_coverage_report.json")
        
    except ImportError:
        print("\nWarning: Could not import SimpleAlteryxConverter for coverage verification")
    
    print(f"\n{'='*80}")
    print("ANALYSIS COMPLETE - All reports generated successfully!")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    main()