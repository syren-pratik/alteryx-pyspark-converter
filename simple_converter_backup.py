"""
Simplified Alteryx to PySpark Converter for backup use.
"""

import xml.etree.ElementTree as ET
from typing import Dict, Any, List, Optional
from collections import defaultdict
import re


class SimpleAlteryxConverter:
    """Simplified converter for immediate use."""
    
    def __init__(self):
        """Initialize the simple converter."""
        self.supported_tools = {
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
            'AlteryxBasePluginsGui.FindReplace.FindReplace': 'Find Replace',
            'AlteryxBasePluginsGui.XMLParse.XMLParse': 'XMLParse',
            'AlteryxBasePluginsGui.CrossTab.CrossTab': 'CrossTab',
            'AlteryxBasePluginsGui.Transpose.Transpose': 'Transpose',
            'AlteryxBasePluginsGui.RegEx.RegEx': 'RegEx',
            'AlteryxBasePluginsGui.DynamicSelect.DynamicSelect': 'DynamicSelect',
            'AlteryxBasePluginsGui.DynamicRename.DynamicRename': 'DynamicRename',
            'AlteryxBasePluginsGui.TextBox.TextBox': 'TextBox',
            'AlteryxBasePluginsGui.MultiRowFormula.MultiRowFormula': 'Multi-Row Formula',
            'AlteryxBasePluginsGui.BlockUntilDone.BlockUntilDone': 'Block Until Done'
        }
    
    def convert_workflow(self, file_path: str) -> Dict[str, Any]:
        """Convert workflow file to PySpark code."""
        try:
            # Parse XML
            tree = ET.parse(file_path)
            root = tree.getroot()
            print(f"Successfully parsed XML, root tag: {root.tag}")
            
            # Extract tools and connections
            try:
                tools = self._extract_tools(root)
                print(f"Extracted {len(tools)} tools")
            except Exception as e:
                print(f"Error extracting tools: {e}")
                raise e
                
            try:
                connections = self._extract_connections(root)
                print(f"Extracted {len(connections)} connections")
            except Exception as e:
                print(f"Error extracting connections: {e}")
                raise e
            
            if not tools:
                return {
                    'success': False,
                    'error': 'No tools found in workflow',
                    'tools': [],
                    'connections': []
                }
            
            # Generate code
            try:
                code = self._generate_code(tools, connections)
                lines_count = len(code.split('\n'))
                print(f"Generated {lines_count} lines of code")
            except Exception as e:
                print(f"Error generating code: {e}")
                print(f"Error type: {type(e).__name__}")
                # Try to identify which tool caused the issue
                for i, tool in enumerate(tools):
                    print(f"Tool {i+1}: {tool.get('type', 'Unknown')} (ID: {tool.get('id', 'Unknown')})")
                raise e
            
            # Generate workflow steps summary
            try:
                workflow_steps = self._generate_workflow_steps(tools, connections)
                print(f"Generated {len(workflow_steps)} workflow steps")
            except Exception as e:
                print(f"Error generating workflow steps: {e}")
                raise e
            
            return {
                'success': True,
                'code': code,
                'tools': tools,
                'connections': connections,
                'workflow_name': file_path.split('/')[-1].replace('.yxmd', ''),
                'workflow_steps': workflow_steps
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'tools': [],
                'connections': []
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
            
            # Get GUI info for positioning
            gui_settings = node.find('GuiSettings')
            gui_info = {}
            if gui_settings is not None:
                position = gui_settings.find('Position')
                if position is not None:
                    gui_info['x'] = float(position.get('x', 0))
                    gui_info['y'] = float(position.get('y', 0))
            
            tools.append({
                'id': tool_id,
                'type': tool_type,
                'name': self.supported_tools.get(tool_type, tool_type.split('.')[-1] if '.' in tool_type else tool_type),
                'config': config,
                'gui_info': gui_info
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
    
    def _generate_code(self, tools: List[Dict[str, Any]], connections: List[Dict[str, Any]]) -> str:
        """Generate PySpark code with enhanced workflow documentation."""
        # Extract workflow documentation from TextBox tools
        workflow_docs = self._extract_workflow_documentation(tools)
        
        code_lines = [
            "# " + "="*70,
            "# ALTERYX TO PYSPARK WORKFLOW CONVERSION",
            "# " + "="*70
        ]
        
        # Add workflow documentation if found
        if workflow_docs:
            code_lines.extend([
                "#",
                "# WORKFLOW DOCUMENTATION:"
            ])
            for doc in workflow_docs:
                for line in doc.split('\n'):
                    if line.strip():
                        code_lines.append(f"# {line.strip()}")
            code_lines.append("#")
        
        code_lines.extend([
            "# " + "-"*70,
            "# SETUP AND IMPORTS",
            "# " + "-"*70,
            "",
            "from pyspark.sql import SparkSession",
            "from pyspark.sql.functions import *",
            "from pyspark.sql.types import *",
            "from pyspark.sql.window import Window",
            "",
            "# Initialize Spark Session",
            "spark = SparkSession.builder.appName('AlteryxWorkflow').getOrCreate()",
            "",
            "# " + "-"*70,
            "# WORKFLOW STEPS",
            "# " + "-"*70
        ])
        
        # Add workflow steps summary
        workflow_steps = self._generate_workflow_steps(tools, connections)
        if workflow_steps:
            code_lines.append("#")
            code_lines.append("# WORKFLOW OVERVIEW:")
            for step in workflow_steps:
                code_lines.append(f"# {step['step_number']:2d}. {step['description']} [{step['tool_type']}]")
            code_lines.append("#")
        
        code_lines.append("")
        
        # Sort tools by dependencies
        sorted_tools = self._sort_tools_by_dependencies(tools, connections)
        
        # Generate code for each tool with enhanced documentation
        step_counter = 1
        for tool in sorted_tools:
            # Skip documentation tools for step numbering
            if tool['type'] not in ['AlteryxGuiToolkit.TextBox.TextBox', 'AlteryxGuiToolkit.ToolContainer.ToolContainer']:
                # Find the corresponding workflow step for additional context
                corresponding_step = None
                for step in workflow_steps:
                    if step['tool_id'] == tool['id']:
                        corresponding_step = step
                        break
                
                tool_code = self._generate_tool_code(tool, connections, step_counter, corresponding_step)
                if tool_code:
                    code_lines.extend(tool_code)
                    code_lines.append("")
                    step_counter += 1
            else:
                # Still generate comments for documentation tools
                tool_code = self._generate_tool_code(tool, connections, None, None)
                if tool_code:
                    code_lines.extend(tool_code)
                    code_lines.append("")
        
        # Add workflow completion
        code_lines.extend([
            "# " + "-"*70,
            "# WORKFLOW COMPLETION",
            "# " + "-"*70,
            "",
            "print('Workflow completed successfully!')",
            f"print(f'Total steps executed: {len(workflow_steps) if workflow_steps else 0}')",
            "# spark.stop()  # Uncomment if running as standalone script"
        ])
        
        return "\n".join(code_lines)
    
    def _sort_tools_by_dependencies(self, tools: List[Dict[str, Any]], connections: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Sort tools by dependencies."""
        # Build dependency graph
        tool_map = {tool['id']: tool for tool in tools}
        dependencies = defaultdict(set)
        
        # Track all tools that are referenced in connections
        tools_in_connections = set()
        
        for conn in connections:
            to_tool = conn['to_tool']
            from_tool = conn['from_tool']
            tools_in_connections.add(to_tool)
            tools_in_connections.add(from_tool)
            
            # Only add dependency if both tools exist
            if to_tool in tool_map and from_tool in tool_map:
                dependencies[to_tool].add(from_tool)
        
        # Topological sort
        result = []
        visited = set()
        temp_visited = set()
        
        def visit(tool_id):
            if tool_id in temp_visited:
                return  # Cycle detected, skip
            if tool_id in visited:
                return
            
            temp_visited.add(tool_id)
            for dep in dependencies.get(tool_id, set()):
                if dep in tool_map:
                    visit(dep)
            temp_visited.remove(tool_id)
            visited.add(tool_id)
            if tool_id in tool_map:
                result.append(tool_map[tool_id])
        
        # First process all tools in connections
        for tool in tools:
            if tool['id'] in tools_in_connections and tool['id'] not in visited:
                visit(tool['id'])
        
        # Then add any disconnected tools (like TextBox, Browse, etc.)
        for tool in tools:
            if tool['id'] not in visited:
                result.append(tool)
                visited.add(tool['id'])
        
        return result
    
    def _generate_tool_code(self, tool: Dict[str, Any], connections: List[Dict[str, Any]], step_num: Optional[int], workflow_step: Optional[Dict[str, Any]] = None) -> List[str]:
        """Generate code for a specific tool."""
        tool_type = tool['type']
        tool_id = tool['id']
        config = tool.get('config', {})
        
        # Create step header with enhanced formatting
        if step_num is not None:
            step_header = f"Step {step_num}: {tool['name']} (ID: {tool_id})"
            code_lines = [
                f"# {step_header}",
                f"# {'-' * len(step_header)}"
            ]
            # Add workflow step description if available
            if workflow_step and workflow_step.get('description'):
                code_lines.append(f"# Description: {workflow_step['description']}")
        else:
            code_lines = [f"# {tool['name']} (ID: {tool_id})"]
        
        if tool_type == 'AlteryxBasePluginsGui.DbFileInput.DbFileInput':
            # Input Data tool with proper file format detection
            file_path = config.get('File', 'input_file.csv')
            file_format = config.get('FileFormat', '0')  # Get file format code
            
            # Fix backslashes in path
            file_path = file_path.replace('\\', '/')
            # Remove Excel sheet reference if present
            if '|||' in file_path:
                file_path = file_path.split('|||')[0]
            
            var_name = f"df_{tool_id}"
            
            # Map Alteryx file format codes to proper read methods
            # FileFormat="19" = .yxdb (Alteryx database)
            # FileFormat="0" = CSV
            # FileFormat="25" = Parquet
            if file_format == '19' or file_path.lower().endswith('.yxdb'):
                code_lines.append(f'# Note: .yxdb files are Alteryx proprietary format')
                code_lines.append(f'# Converting to Parquet equivalent for PySpark')
                parquet_path = file_path.replace('.yxdb', '.parquet')
                code_lines.append(f'{var_name} = spark.read.parquet("{parquet_path}")')
            elif file_format == '25' or file_path.lower().endswith('.parquet'):
                code_lines.append(f'{var_name} = spark.read.parquet("{file_path}")')
            elif file_path.lower().endswith('.csv') or file_format == '0':
                code_lines.append(f'{var_name} = spark.read.option("header", "true").csv("{file_path}")')
            elif file_path.lower().endswith(('.xlsx', '.xls')):
                code_lines.append(f'# Note: Reading Excel requires spark-excel library')
                code_lines.append(f'{var_name} = spark.read.format("com.crealytics.spark.excel").option("header", "true").load("{file_path}")')
            else:
                # Default based on file extension
                if file_path.lower().endswith('.yxdb'):
                    code_lines.append(f'# Note: .yxdb files should be converted to Parquet for PySpark')
                    parquet_path = file_path.replace('.yxdb', '.parquet')
                    code_lines.append(f'{var_name} = spark.read.parquet("{parquet_path}")')
                else:
                    code_lines.append(f'{var_name} = spark.read.option("header", "true").csv("{file_path}")')
        
        elif tool_type == 'AlteryxBasePluginsGui.DbFileOutput.DbFileOutput':
            # Output Data tool
            file_path = config.get('File', 'output_file.csv')
            # Fix backslashes in path
            file_path = file_path.replace('\\', '/')
            # Remove Excel sheet reference if present
            if '|||' in file_path:
                file_path = file_path.split('|||')[0]
            
            input_tools = self._get_input_tools(tool_id, connections)
            
            if input_tools:
                input_var = f"df_{input_tools[0]}"
                if file_path.lower().endswith('.csv'):
                    code_lines.append(f'{input_var}.write.mode("overwrite").option("header", "true").csv("{file_path}")')
                elif file_path.lower().endswith(('.xlsx', '.xls')):
                    code_lines.append(f'# Note: Writing Excel requires additional libraries')
                    code_lines.append(f'{input_var}.write.mode("overwrite").format("com.crealytics.spark.excel").option("header", "true").save("{file_path}")')
                elif file_path.lower().endswith('.parquet'):
                    code_lines.append(f'{input_var}.write.mode("overwrite").parquet("{file_path}")')
                else:
                    code_lines.append(f'{input_var}.write.mode("overwrite").parquet("{file_path}")')
        
        elif tool_type == 'AlteryxBasePluginsGui.Filter.Filter':
            # Filter tool with proper condition parsing
            input_tools = self._get_input_tools(tool_id, connections)
            if input_tools:
                input_var = f"df_{input_tools[0]}"
                var_name_true = f"df_{tool_id}_T"
                var_name_false = f"df_{tool_id}_F"
                
                # Extract filter configuration from XML
                filter_config = self._extract_filter_configuration(config)
                
                if filter_config['expression']:
                    expression = filter_config['expression']
                    pyspark_expr = self._convert_filter_expression_detailed(filter_config)
                    
                    code_lines.append(f'# Filter: {expression}')
                    code_lines.append(f'{var_name_true} = {input_var}.filter({pyspark_expr})')
                    code_lines.append(f'{var_name_false} = {input_var}.filter(~({pyspark_expr}))')
                else:
                    # Fallback
                    code_lines.append(f'# Filter: No condition specified')
                    code_lines.append(f'{var_name_true} = {input_var}')
                    code_lines.append(f'{var_name_false} = {input_var}.limit(0)  # Empty DataFrame')
        
        elif tool_type == 'AlteryxBasePluginsGui.AlteryxSelect.AlteryxSelect':
            # Select tool
            input_tools = self._get_input_tools(tool_id, connections)
            if input_tools:
                input_var = f"df_{input_tools[0]}"
                var_name = f"df_{tool_id}"
                code_lines.append(f'{var_name} = {input_var}  # Select columns as needed')
        
        elif tool_type == 'AlteryxBasePluginsGui.Formula.Formula':
            # Formula tool with actual expression parsing
            input_tools = self._get_input_tools(tool_id, connections)
            if input_tools:
                input_var = f"df_{input_tools[0]}"
                var_name = f"df_{tool_id}"
                
                # Extract actual formula fields from XML
                formula_fields = self._extract_formula_fields_xml(config)
                
                if formula_fields:
                    code_lines.append(f'# Apply formula transformations')
                    current_df = input_var
                    
                    for i, formula_field in enumerate(formula_fields):
                        field_name = formula_field['field']
                        expression = formula_field['expression']
                        
                        # Convert Alteryx expression to PySpark
                        pyspark_expr = self._convert_alteryx_expression_detailed(expression)
                        
                        if i == len(formula_fields) - 1:
                            # Last formula - use final variable name
                            code_lines.append(f'{var_name} = {current_df}.withColumn("{field_name}", {pyspark_expr})')
                        else:
                            # Intermediate step
                            temp_var = f'{var_name}_step{i+1}'
                            code_lines.append(f'{temp_var} = {current_df}.withColumn("{field_name}", {pyspark_expr})')
                            current_df = temp_var
                else:
                    code_lines.append(f'{var_name} = {input_var}  # No formula configuration found')
        
        elif tool_type == 'AlteryxBasePluginsGui.Union.Union':
            # Union tool
            input_tools = self._get_input_tools(tool_id, connections)
            if len(input_tools) >= 2:
                var_name = f"df_{tool_id}"
                union_expr = f"df_{input_tools[0]}"
                for input_tool in input_tools[1:]:
                    union_expr += f".union(df_{input_tool})"
                code_lines.append(f'{var_name} = {union_expr}')
            elif input_tools:
                var_name = f"df_{tool_id}"
                code_lines.append(f'{var_name} = df_{input_tools[0]}  # Single input to union')
        
        elif tool_type == 'AlteryxBasePluginsGui.Join.Join':
            # Join tool with actual join key extraction
            input_tools = self._get_input_tools(tool_id, connections)
            if len(input_tools) >= 2:
                left_var = f"df_{input_tools[0]}"
                right_var = f"df_{input_tools[1]}"
                var_name = f"df_{tool_id}"
                
                # Extract actual join configuration
                join_info = self._extract_join_configuration(config)
                
                if join_info['left_fields'] and join_info['right_fields']:
                    # Use actual join keys from XML
                    left_field = join_info['left_fields'][0]
                    right_field = join_info['right_fields'][0]
                    
                    if left_field == right_field:
                        # Same column name - simple join
                        code_lines.append(f'{var_name} = {left_var}.join({right_var}, "{left_field}", "inner")')
                    else:
                        # Different column names - explicit condition
                        code_lines.append(f'{var_name} = {left_var}.join({right_var}, {left_var}["{left_field}"] == {right_var}["{right_field}"], "inner")')
                    
                    # Add column selection based on SelectConfiguration
                    if join_info['select_fields']:
                        selected_cols = [f'col("{field}")' for field in join_info['select_fields']]
                        cols_str = ', '.join(selected_cols)
                        code_lines.append(f'{var_name} = {var_name}.select({cols_str})')
                else:
                    # Fallback to generic join
                    code_lines.append(f'{var_name} = {left_var}.join({right_var}, "key", "inner")  # TODO: Specify actual join keys')
        
        elif tool_type == 'AlteryxBasePluginsGui.RecordID.RecordID':
            # Record ID tool
            input_tools = self._get_input_tools(tool_id, connections)
            if input_tools:
                input_var = f"df_{input_tools[0]}"
                var_name = f"df_{tool_id}"
                code_lines.append(f'# Add sequential row numbers')
                code_lines.append(f'from pyspark.sql.window import Window')
                code_lines.append(f'{var_name} = {input_var}.withColumn("RecordID", row_number().over(Window.orderBy(monotonically_increasing_id())))')
        
        elif tool_type == 'AlteryxBasePluginsGui.XMLParse.XMLParse':
            # XML Parse tool
            input_tools = self._get_input_tools(tool_id, connections)
            if input_tools:
                input_var = f"df_{input_tools[0]}"
                var_name = f"df_{tool_id}"
                xml_field = config.get('XMLField', 'xml_column')
                code_lines.append(f'# Parse XML data from column: {xml_field}')
                code_lines.append(f'from pyspark.sql.functions import from_json, schema_of_json')
                code_lines.append(f'{var_name} = {input_var}  # XML parsing requires custom UDF or spark-xml library')
        
        elif tool_type == 'AlteryxBasePluginsGui.CrossTab.CrossTab':
            # CrossTab (Pivot) tool
            input_tools = self._get_input_tools(tool_id, connections)
            if input_tools:
                input_var = f"df_{input_tools[0]}"
                var_name = f"df_{tool_id}"
                code_lines.append(f'# Pivot table transformation')
                code_lines.append(f'{var_name} = {input_var}.groupBy("group_column").pivot("pivot_column").agg(first("value_column"))')
        
        elif tool_type == 'AlteryxBasePluginsGui.Transpose.Transpose':
            # Transpose tool
            input_tools = self._get_input_tools(tool_id, connections)
            if input_tools:
                input_var = f"df_{input_tools[0]}"
                var_name = f"df_{tool_id}"
                code_lines.append(f'# Transpose columns to rows')
                code_lines.append(f'from pyspark.sql.functions import expr, array, struct, explode')
                code_lines.append(f'{var_name} = {input_var}.select("*", explode(array([struct(lit(c).alias("column"), col(c).alias("value")) for c in {input_var}.columns])).alias("transposed"))')
        
        elif tool_type == 'AlteryxBasePluginsGui.RegEx.RegEx':
            # RegEx tool
            input_tools = self._get_input_tools(tool_id, connections)
            if input_tools:
                input_var = f"df_{input_tools[0]}"
                var_name = f"df_{tool_id}"
                code_lines.append(f'# Apply regular expression')
                code_lines.append(f'{var_name} = {input_var}.withColumn("regex_result", regexp_extract(col("text_column"), r"pattern", 1))')
        
        elif tool_type == 'AlteryxBasePluginsGui.DynamicSelect.DynamicSelect':
            # Dynamic Select tool
            input_tools = self._get_input_tools(tool_id, connections)
            if input_tools:
                input_var = f"df_{input_tools[0]}"
                var_name = f"df_{tool_id}"
                code_lines.append(f'# Dynamic column selection')
                code_lines.append(f'{var_name} = {input_var}.select([c for c in {input_var}.columns if c.startswith("prefix_")])')
        
        elif tool_type == 'AlteryxBasePluginsGui.DynamicRename.DynamicRename':
            # Dynamic Rename tool
            input_tools = self._get_input_tools(tool_id, connections)
            if input_tools:
                input_var = f"df_{input_tools[0]}"
                var_name = f"df_{tool_id}"
                code_lines.append(f'# Dynamic column renaming')
                code_lines.append(f'renamed_cols = {{c: c.replace("old_", "new_") for c in {input_var}.columns}}')
                code_lines.append(f'{var_name} = {input_var}.select([col(c).alias(renamed_cols.get(c, c)) for c in {input_var}.columns])')
        
        elif tool_type == 'AlteryxBasePluginsGui.Summarize.Summarize':
            # Summarize (Group By) tool
            input_tools = self._get_input_tools(tool_id, connections)
            if input_tools:
                input_var = f"df_{input_tools[0]}"
                var_name = f"df_{tool_id}"
                code_lines.append(f'# Group by and aggregate')
                code_lines.append(f'{var_name} = {input_var}.groupBy("group_column").agg(')
                code_lines.append(f'    sum("numeric_column").alias("sum_value"),')
                code_lines.append(f'    avg("numeric_column").alias("avg_value"),')
                code_lines.append(f'    count("*").alias("count")')
                code_lines.append(f')')
        
        elif tool_type == 'AlteryxGuiToolkit.TextBox.TextBox':
            # TextBox tool (documentation/comments) - Enhanced formatting
            text_content = config.get('Text', 'Documentation')
            if text_content and text_content.strip():
                code_lines.append("# " + "="*50)
                code_lines.append("# WORKFLOW DOCUMENTATION")
                code_lines.append("# " + "="*50)
                for line in text_content.split('\n'):
                    if line.strip():
                        code_lines.append(f"# {line.strip()}")
                code_lines.append("# " + "="*50)
            else:
                code_lines.append("# Documentation section")
        
        elif tool_type == 'AlteryxBasePluginsGui.Sort.Sort':
            # Sort tool
            input_tools = self._get_input_tools(tool_id, connections)
            if input_tools:
                input_var = f"df_{input_tools[0]}"
                var_name = f"df_{tool_id}"
                code_lines.append(f'{var_name} = {input_var}.orderBy("column_name")  # Adjust column name')
        
        else:
            # Unsupported tool
            input_tools = self._get_input_tools(tool_id, connections)
            if input_tools:
                input_var = f"df_{input_tools[0]}"
                var_name = f"df_{tool_id}"
                code_lines.append(f'{var_name} = {input_var}  # TODO: Implement {tool["name"]} functionality')
            else:
                code_lines.append(f'# TODO: Implement {tool["name"]} functionality')
        
        return code_lines
    
    def _get_input_tools(self, tool_id: str, connections: List[Dict[str, Any]]) -> List[str]:
        """Get input tool IDs for a given tool."""
        inputs = []
        for conn in connections:
            if conn['to_tool'] == tool_id:
                from_tool = conn['from_tool']
                from_output = conn.get('from_output', '')
                
                # Handle Filter tool outputs (T for True, F for False)
                if from_output in ['True', 'T']:
                    inputs.append(f"{from_tool}_T")
                elif from_output in ['False', 'F']:
                    inputs.append(f"{from_tool}_F")
                else:
                    inputs.append(from_tool)
        return inputs
    
    def _convert_filter_expression(self, expression: str) -> str:
        """Convert Alteryx filter expression to PySpark."""
        # Basic conversion
        expr = expression
        expr = re.sub(r'\[([^\]]+)\]', r'col("\1")', expr)
        expr = expr.replace(' AND ', ' & ')
        expr = expr.replace(' OR ', ' | ')
        expr = expr.replace(' = ', ' == ')
        return expr
    
    def _extract_workflow_documentation(self, tools: List[Dict[str, Any]]) -> List[str]:
        """Extract documentation from TextBox tools for workflow header."""
        docs = []
        for tool in tools:
            if tool['type'] == 'AlteryxGuiToolkit.TextBox.TextBox':
                text_content = tool.get('config', {}).get('Text', '')
                if text_content and text_content.strip():
                    # Only include substantial documentation (more than 10 chars)
                    if len(text_content.strip()) > 10:
                        docs.append(text_content.strip())
        return docs
    
    def _generate_workflow_steps(self, tools: List[Dict[str, Any]], connections: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Generate workflow steps summary for UI display."""
        # Sort tools by dependencies first
        sorted_tools = self._sort_tools_by_dependencies(tools, connections)
        
        workflow_steps = []
        step_counter = 1
        
        for tool in sorted_tools:
            # Skip documentation tools for step numbering
            if tool['type'] not in ['AlteryxGuiToolkit.TextBox.TextBox', 'AlteryxGuiToolkit.ToolContainer.ToolContainer']:
                
                # Get input tools for this step
                input_tools = self._get_input_tools(tool['id'], connections)
                input_description = ""
                if input_tools:
                    input_description = f" (from: {', '.join([f'df_{inp}' for inp in input_tools[:2]])})"
                    if len(input_tools) > 2:
                        input_description += f" +{len(input_tools)-2} more"
                
                # Get output connections
                output_tools = [conn['to_tool'] for conn in connections if conn['from_tool'] == tool['id']]
                output_description = ""
                if output_tools:
                    output_description = f" → {len(output_tools)} output(s)"
                
                # Create step description
                step_description = self._get_step_description(tool)
                
                workflow_steps.append({
                    'step_number': step_counter,
                    'tool_id': tool['id'],
                    'tool_name': tool['name'],
                    'tool_type': tool['type'].split('.')[-1] if '.' in tool['type'] else tool['type'],
                    'description': step_description,
                    'input_description': input_description,
                    'output_description': output_description,
                    'variable_name': f"df_{tool['id']}",
                    'has_inputs': len(input_tools) > 0,
                    'has_outputs': len(output_tools) > 0
                })
                step_counter += 1
        
        return workflow_steps
    
    def _get_step_description(self, tool: Dict[str, Any]) -> str:
        """Get a human-readable description of what this tool does."""
        tool_type = tool['type']
        config = tool.get('config', {})
        
        if tool_type == 'AlteryxBasePluginsGui.DbFileInput.DbFileInput':
            file_path = config.get('File', 'input_file')
            file_name = file_path.split('/')[-1] if '/' in file_path else file_path.split('\\')[-1]
            return f"Read data from {file_name}"
            
        elif tool_type == 'AlteryxBasePluginsGui.DbFileOutput.DbFileOutput':
            file_path = config.get('File', 'output_file')
            file_name = file_path.split('/')[-1] if '/' in file_path else file_path.split('\\')[-1]
            return f"Write data to {file_name}"
            
        elif tool_type == 'AlteryxBasePluginsGui.Filter.Filter':
            expression = config.get('Expression', 'condition')
            return f"Filter records where {expression[:30]}{'...' if len(expression) > 30 else ''}"
            
        elif tool_type == 'AlteryxBasePluginsGui.AlteryxSelect.AlteryxSelect':
            return "Select and configure columns"
            
        elif tool_type == 'AlteryxBasePluginsGui.Formula.Formula':
            formula_fields = config.get('FormulaFields', {})
            if formula_fields:
                return "Apply formula calculations"
            return "Transform data with formulas"
            
        elif tool_type == 'AlteryxBasePluginsGui.Union.Union':
            return "Combine datasets vertically"
            
        elif tool_type == 'AlteryxBasePluginsGui.Join.Join':
            return "Join datasets on common keys"
            
        elif tool_type == 'AlteryxBasePluginsGui.Summarize.Summarize':
            return "Group and aggregate data"
            
        elif tool_type == 'AlteryxBasePluginsGui.Sort.Sort':
            return "Sort records by specified columns"
            
        elif tool_type == 'AlteryxBasePluginsGui.Unique.Unique':
            return "Remove duplicate records"
            
        elif tool_type == 'AlteryxBasePluginsGui.RecordID.RecordID':
            return "Add sequential row numbers"
            
        elif tool_type == 'AlteryxBasePluginsGui.XMLParse.XMLParse':
            return "Parse XML data into columns"
            
        elif tool_type == 'AlteryxBasePluginsGui.CrossTab.CrossTab':
            return "Pivot data from rows to columns"
            
        elif tool_type == 'AlteryxBasePluginsGui.Transpose.Transpose':
            return "Transform columns to rows"
            
        elif tool_type == 'AlteryxBasePluginsGui.RegEx.RegEx':
            return "Extract data using regular expressions"
            
        else:
            # Generic description for other tools
            tool_name = tool['name']
            return f"Process data using {tool_name}"
    
    def _extract_join_configuration(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract join configuration from XML."""
        join_info = {
            'left_fields': [],
            'right_fields': [],
            'select_fields': []
        }
        
        # Extract join fields
        if 'JoinInfo' in config:
            join_infos = config['JoinInfo']
            if not isinstance(join_infos, list):
                join_infos = [join_infos]
            
            for join_item in join_infos:
                connection = join_item.get('@connection', '')
                if 'Field' in join_item:
                    fields = join_item['Field']
                    if not isinstance(fields, list):
                        fields = [fields]
                    
                    for field in fields:
                        field_name = field.get('@field', field) if isinstance(field, dict) else field
                        if connection == 'Left':
                            join_info['left_fields'].append(field_name)
                        elif connection == 'Right':
                            join_info['right_fields'].append(field_name)
        
        # Extract select configuration
        if 'SelectConfiguration' in config:
            select_config = config['SelectConfiguration']
            if 'Configuration' in select_config and 'SelectFields' in select_config['Configuration']:
                select_fields = select_config['Configuration']['SelectFields']
                if 'SelectField' in select_fields:
                    fields = select_fields['SelectField']
                    if not isinstance(fields, list):
                        fields = [fields]
                    
                    for field in fields:
                        if field.get('@selected', 'False') == 'True':
                            field_name = field.get('@field', '')
                            if field_name and field_name != '*Unknown':
                                join_info['select_fields'].append(field_name)
        
        return join_info
    
    def _extract_formula_fields_xml(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract formula fields from XML configuration."""
        formula_fields = []
        
        if 'FormulaFields' in config and 'FormulaField' in config['FormulaFields']:
            fields = config['FormulaFields']['FormulaField']
            if not isinstance(fields, list):
                fields = [fields]
            
            for field in fields:
                formula_fields.append({
                    'field': field.get('@field', ''),
                    'expression': field.get('@expression', ''),
                    'type': field.get('@type', 'V_WString'),
                    'size': field.get('@size', '')
                })
        
        return formula_fields
    
    def _extract_filter_configuration(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract filter configuration from XML."""
        filter_info = {
            'mode': 'Simple',
            'expression': '',
            'field': '',
            'operator': '',
            'operand': ''
        }
        
        # Check for custom expression first
        if 'Expression' in config:
            filter_info['mode'] = 'Custom'
            filter_info['expression'] = config['Expression']
        elif 'Simple' in config:
            # Simple filter mode
            simple_config = config['Simple']
            filter_info['field'] = simple_config.get('Field', '')
            filter_info['operator'] = simple_config.get('Operator', '=')
            
            if 'Operands' in simple_config and 'Operand' in simple_config['Operands']:
                operand = simple_config['Operands']['Operand']
                filter_info['operand'] = operand if isinstance(operand, str) else str(operand)
                
                # Build expression from simple components
                field = filter_info['field']
                op = filter_info['operator']
                val = filter_info['operand']
                filter_info['expression'] = f'[{field}] {op} {val}'
        
        return filter_info
    
    def _convert_alteryx_expression_detailed(self, expression: str) -> str:
        """Convert Alteryx expression to PySpark with detailed parsing."""
        if not expression:
            return "lit(None)"
        
        try:
            expr = expression.strip()
            expr_preview = expr[:100]  # First 100 chars
            print(f"Converting expression: {expr_preview}...")  # Log preview
            
            # Handle Switch statements
            if expr.upper().startswith('SWITCH'):
                return self._convert_switch_expression(expr)
            
            # Handle IF statements
            if expr.upper().startswith('IF '):
                return self._convert_if_expression(expr)
            
            # Handle DateTimeDiff function
            if 'DateTimeDiff' in expr:
                expr = self._convert_datetime_diff(expr)
            
            # Handle DateTimeFormat function
            if 'DateTimeFormat' in expr:
                expr = self._convert_datetime_format(expr)
            
            # Handle Contains function
            if 'Contains' in expr:
                expr = self._convert_contains_function(expr)
            
            # Replace field references [Field] with col("Field")
            expr = re.sub(r'\[([^\]]+)\]', r'col("\1")', expr)
            
            # Handle string literals
            expr = re.sub(r'"([^"]*)"', r'lit("\1")', expr)
            
            # Handle numeric literals
            expr = re.sub(r'\b(\d+\.?\d*)\b', r'lit(\1)', expr)
            
            return expr
            
        except Exception as e:
            print(f"Error converting expression '{expression}': {e}")
            # Return a safe fallback
            return f'lit("ERROR: {str(e)[:50]}")'
    
    def _convert_switch_expression(self, expression: str) -> str:
        """Convert Alteryx Switch expression to PySpark case/when."""
        # Extract Switch components: Switch([field], [field], "value1", "result1", "value2", "result2")
        match = re.match(r'Switch\\s*\\(([^,]+),([^,]+)(?:,([^)]+))?\\)', expression, re.IGNORECASE)
        if match:
            field = match.group(1).strip()
            default_field = match.group(2).strip()
            cases = match.group(3) if match.group(3) else ""
            
            # Convert field reference
            field_ref = re.sub(r'\[([^\]]+)\]', r'col("\1")', field)
            
            # Parse cases
            case_parts = [c.strip().strip('"') for c in cases.split(',') if c.strip()]
            
            if len(case_parts) >= 2:
                when_clauses = []
                for i in range(0, len(case_parts), 2):
                    if i + 1 < len(case_parts):
                        value = case_parts[i]
                        result = case_parts[i + 1]
                        when_clauses.append(f'when({field_ref} == lit("{value}"), lit("{result}"))')
                
                return f'{"".join(when_clauses)}.otherwise({field_ref})'
        
        return f'col("unknown_field")'
    
    def _convert_if_expression(self, expression: str) -> str:
        """Convert Alteryx IF expression to PySpark when/otherwise."""
        # Handle IF DateTimeDiff(...) <= 3 then 1 else 0 endif
        if_pattern = r'IF\\s+(.+?)\\s+then\\s+(.+?)\\s+else\\s+(.+?)\\s+endif'
        match = re.search(if_pattern, expression, re.IGNORECASE | re.DOTALL)
        
        if match:
            condition = match.group(1).strip()
            then_val = match.group(2).strip()
            else_val = match.group(3).strip()
            
            # Convert condition
            condition = self._convert_condition(condition)
            
            return f'when({condition}, lit({then_val})).otherwise(lit({else_val}))'
        
        return 'lit(None)'
    
    def _convert_condition(self, condition: str) -> str:
        """Convert Alteryx condition to PySpark."""
        # Handle DateTimeDiff
        if 'DateTimeDiff' in condition:
            condition = self._convert_datetime_diff(condition)
        
        # Replace field references
        condition = re.sub(r'\[([^\]]+)\]', r'col("\1")', condition)
        
        # Handle operators
        condition = condition.replace(' and ', ' & ')
        condition = condition.replace(' AND ', ' & ')
        condition = condition.replace(' or ', ' | ')
        condition = condition.replace(' OR ', ' | ')
        condition = condition.replace('=', '==')
        
        return condition
    
    def _convert_datetime_diff(self, expression: str) -> str:
        """Convert DateTimeDiff function to PySpark."""
        # DateTimeDiff("2017-01-01 00:00:00",[Opp Created Date],"months")
        pattern = r'DateTimeDiff\\s*\\(\\s*"([^"]+)"\\s*,\\s*\\[([^\\]]+)\\]\\s*,\\s*"([^"]+)"\\s*\\)'
        match = re.search(pattern, expression)
        
        if match:
            start_date = match.group(1)
            field_name = match.group(2)
            unit = match.group(3)
            
            if unit == 'months':
                replacement = f'months_between(col("{field_name}"), lit("{start_date}"))'
            elif unit == 'days':
                replacement = f'datediff(col("{field_name}"), lit("{start_date}"))'
            else:
                replacement = f'datediff(col("{field_name}"), lit("{start_date}"))'
            
            return expression.replace(match.group(0), replacement)
        
        return expression
    
    def _convert_datetime_format(self, expression: str) -> str:
        """Convert DateTimeFormat function to PySpark."""
        # DateTimeFormat([Close Date],'%Y')
        pattern = r'DateTimeFormat\\s*\\(\\s*\\[([^\\]]+)\\]\\s*,\\s*\'([^\']+)\'\\s*\\)'
        match = re.search(pattern, expression)
        
        if match:
            field_name = match.group(1)
            format_str = match.group(2)
            
            # Convert format string
            spark_format = format_str.replace('%Y', 'yyyy').replace('%m', 'MM').replace('%d', 'dd')
            replacement = f'date_format(col("{field_name}"), "{spark_format}")'
            
            return expression.replace(match.group(0), replacement)
        
        return expression
    
    def _convert_contains_function(self, expression: str) -> str:
        """Convert Contains function to PySpark."""
        # Contains([Name], "3m")
        pattern = r'Contains\\s*\\(\\s*\\[([^\\]]+)\\]\\s*,\\s*"([^"]+)"\\s*\\)'
        match = re.search(pattern, expression)
        
        if match:
            field_name = match.group(1)
            search_value = match.group(2)
            replacement = f'col("{field_name}").contains("{search_value}")'
            
            return expression.replace(match.group(0), replacement)
        
        return expression
    
    def _convert_filter_expression_detailed(self, filter_config: Dict[str, Any]) -> str:
        """Convert filter configuration to PySpark expression."""
        if filter_config['mode'] == 'Custom':
            # Handle custom expression
            expr = filter_config['expression']
            
            # Replace field references
            expr = re.sub(r'\[([^\]]+)\]', r'col("\1")', expr)
            
            # Handle operators
            expr = expr.replace('!=', '!=').replace('<>', '!=')
            
            return expr
        else:
            # Handle simple filter
            field = filter_config['field']
            operator = filter_config['operator']
            operand = filter_config['operand']
            
            # Map operators
            op_map = {
                '=': '==',
                '!=': '!=',
                '<>': '!=',
                '>': '>',
                '<': '<',
                '>=': '>=',
                '<=': '<='
            }
            
            spark_op = op_map.get(operator, '==')
            
            # Handle numeric vs string operands
            try:
                float(operand)
                return f'col("{field}") {spark_op} {operand}'
            except ValueError:
                return f'col("{field}") {spark_op} "{operand}"'
    
    def _is_tool_supported(self, tool_type: str) -> bool:
        """Check if tool type is supported."""
        return tool_type in self.supported_tools