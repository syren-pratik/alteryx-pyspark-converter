"""
Dependency Resolver utility for sorting tools based on their connections.

Handles topological sorting and dependency management for workflow tools.
"""

from typing import Dict, Any, List, Set
from collections import defaultdict, deque

from ..core.logger import get_logger
from ...config.settings import Settings


class DependencyResolver:
    """Resolves dependencies between workflow tools and sorts them topologically."""
    
    def __init__(self):
        """Initialize the dependency resolver."""
        self.logger = get_logger()
    
    def sort_tools_by_dependencies(self, tools: List[Dict[str, Any]], 
                                 connections: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Sort tools based on their dependencies using topological sorting.
        
        Args:
            tools: List of tool dictionaries
            connections: List of connection dictionaries
            
        Returns:
            List of tools sorted in dependency order
        """
        try:
            # Build tool lookup map
            tool_map = {tool['id']: tool for tool in tools}
            
            # Build dependency graph
            graph = self._build_dependency_graph(tools, connections)
            
            # Perform topological sort
            sorted_tool_ids = self._topological_sort(graph, list(tool_map.keys()))
            
            # Return sorted tools
            sorted_tools = []
            for tool_id in sorted_tool_ids:
                if tool_id in tool_map:
                    sorted_tools.append(tool_map[tool_id])
                else:
                    # Handle missing tools by creating placeholder
                    self.logger.log_error(
                        component="DependencyResolver",
                        error_type="MissingTool",
                        message=f"Tool {tool_id} referenced in connections but not found in tools list"
                    )
                    # Create a placeholder tool
                    placeholder_tool = {
                        'id': tool_id,
                        'type': 'Unknown',
                        'name': 'Unknown Tool',
                        'config': {},
                        'gui_info': {},
                        'is_placeholder': True
                    }
                    sorted_tools.append(placeholder_tool)
            
            self.logger.app_logger.debug(f"Sorted {len(sorted_tools)} tools by dependencies")
            return sorted_tools
            
        except Exception as e:
            self.logger.log_error(
                component="DependencyResolver",
                error_type="SortingError",
                message=f"Failed to sort tools by dependencies: {str(e)}"
            )
            # Fallback: sort by tool priority and then by ID
            return self._fallback_sort(tools)
    
    def _build_dependency_graph(self, tools: List[Dict[str, Any]], 
                              connections: List[Dict[str, Any]]) -> Dict[str, Set[str]]:
        """Build a dependency graph from tools and connections."""
        
        # Initialize graph with all tools
        graph = defaultdict(set)
        all_tool_ids = {tool['id'] for tool in tools}
        
        # Add all tools to graph
        for tool_id in all_tool_ids:
            graph[tool_id] = set()
        
        # Add dependencies from connections
        for connection in connections:
            from_tool = connection['from_tool']
            to_tool = connection['to_tool']
            
            # to_tool depends on from_tool
            if to_tool not in graph:
                graph[to_tool] = set()
            graph[to_tool].add(from_tool)
            
            # Ensure from_tool is in graph
            if from_tool not in graph:
                graph[from_tool] = set()
        
        return dict(graph)
    
    def _topological_sort(self, graph: Dict[str, Set[str]], all_nodes: List[str]) -> List[str]:
        """
        Perform topological sorting using Kahn's algorithm.
        
        Args:
            graph: Dependency graph where graph[node] = set of dependencies
            all_nodes: List of all nodes
            
        Returns:
            Topologically sorted list of nodes
        """
        # Calculate in-degrees
        in_degree = defaultdict(int)
        
        # Initialize in-degrees
        for node in all_nodes:
            in_degree[node] = 0
        
        # Calculate actual in-degrees
        for node, dependencies in graph.items():
            in_degree[node] = len(dependencies)
        
        # Find nodes with no dependencies
        queue = deque([node for node in all_nodes if in_degree[node] == 0])
        result = []
        
        # Process nodes level by level
        while queue:
            # Sort current level by priority and then by ID for consistency
            current_level = list(queue)
            queue.clear()
            
            # Sort by priority (if available) and then by ID
            current_level.sort(key=lambda x: (
                Settings.TOOL_PRIORITIES.get(graph.get(x, {}).get('type', ''), 999),
                x
            ))
            
            for node in current_level:
                result.append(node)
                
                # Remove this node from other nodes' dependencies
                for other_node, dependencies in graph.items():
                    if node in dependencies:
                        dependencies.remove(node)
                        in_degree[other_node] -= 1
                        
                        # If other_node has no more dependencies, add to queue
                        if in_degree[other_node] == 0:
                            queue.append(other_node)
        
        # Check for cycles
        if len(result) != len(all_nodes):
            remaining_nodes = set(all_nodes) - set(result)
            self.logger.log_error(
                component="DependencyResolver",
                error_type="CyclicDependency",
                message=f"Cyclic dependencies detected involving nodes: {remaining_nodes}"
            )
            # Add remaining nodes to result anyway
            result.extend(sorted(remaining_nodes))
        
        return result
    
    def _fallback_sort(self, tools: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Fallback sorting method using tool priorities and IDs."""
        
        def sort_key(tool):
            tool_type = tool.get('type', '')
            priority = Settings.TOOL_PRIORITIES.get(tool_type, 999)
            tool_id = tool.get('id', '')
            return (priority, tool_id)
        
        sorted_tools = sorted(tools, key=sort_key)
        
        self.logger.app_logger.warning("Used fallback sorting due to dependency resolution failure")
        return sorted_tools
    
    def analyze_dependencies(self, tools: List[Dict[str, Any]], 
                           connections: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze the dependency structure of a workflow.
        
        Args:
            tools: List of tool dictionaries
            connections: List of connection dictionaries
            
        Returns:
            Dictionary containing dependency analysis results
        """
        graph = self._build_dependency_graph(tools, connections)
        
        # Find root nodes (no dependencies)
        root_nodes = [node for node, deps in graph.items() if not deps]
        
        # Find leaf nodes (no dependents)
        all_dependencies = set()
        for deps in graph.values():
            all_dependencies.update(deps)
        
        leaf_nodes = [node for node in graph.keys() if node not in all_dependencies]
        
        # Calculate levels
        levels = self._calculate_levels(graph)
        
        # Find potential bottlenecks (nodes with many dependents)
        dependent_count = defaultdict(int)
        for node, deps in graph.items():
            for dep in deps:
                dependent_count[dep] += 1
        
        bottlenecks = [(node, count) for node, count in dependent_count.items() if count > 2]
        bottlenecks.sort(key=lambda x: x[1], reverse=True)
        
        return {
            'total_tools': len(tools),
            'total_connections': len(connections),
            'root_nodes': root_nodes,
            'leaf_nodes': leaf_nodes,
            'max_depth': max(levels.values()) if levels else 0,
            'tool_levels': levels,
            'potential_bottlenecks': bottlenecks[:5],  # Top 5 bottlenecks
            'has_cycles': len(self._topological_sort(graph, list(graph.keys()))) != len(graph)
        }
    
    def _calculate_levels(self, graph: Dict[str, Set[str]]) -> Dict[str, int]:
        """Calculate the level (depth) of each node in the dependency graph."""
        levels = {}
        
        def calculate_level(node: str, visited: Set[str]) -> int:
            if node in visited:
                return 0  # Avoid infinite recursion in cycles
            
            if node in levels:
                return levels[node]
            
            visited.add(node)
            
            dependencies = graph.get(node, set())
            if not dependencies:
                level = 0
            else:
                level = max(calculate_level(dep, visited) for dep in dependencies) + 1
            
            visited.remove(node)
            levels[node] = level
            return level
        
        for node in graph.keys():
            calculate_level(node, set())
        
        return levels
    
    def validate_workflow_structure(self, tools: List[Dict[str, Any]], 
                                  connections: List[Dict[str, Any]]) -> tuple[bool, List[str]]:
        """
        Validate the structure of a workflow.
        
        Args:
            tools: List of tool dictionaries
            connections: List of connection dictionaries
            
        Returns:
            Tuple of (is_valid, list_of_issues)
        """
        issues = []
        
        # Check for basic issues
        tool_ids = {tool['id'] for tool in tools}
        
        # Check connections reference valid tools
        for connection in connections:
            from_tool = connection['from_tool']
            to_tool = connection['to_tool']
            
            if from_tool not in tool_ids:
                issues.append(f"Connection references unknown source tool: {from_tool}")
            
            if to_tool not in tool_ids:
                issues.append(f"Connection references unknown target tool: {to_tool}")
        
        # Check for isolated tools
        connected_tools = set()
        for connection in connections:
            connected_tools.add(connection['from_tool'])
            connected_tools.add(connection['to_tool'])
        
        isolated_tools = tool_ids - connected_tools
        if isolated_tools:
            issues.append(f"Isolated tools found: {isolated_tools}")
        
        # Check for cycles
        graph = self._build_dependency_graph(tools, connections)
        sorted_nodes = self._topological_sort(graph, list(tool_ids))
        
        if len(sorted_nodes) != len(tool_ids):
            issues.append("Cyclic dependencies detected in workflow")
        
        return len(issues) == 0, issues