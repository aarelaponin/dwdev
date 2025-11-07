"""
Dependency Manager for TA-RDM Source Ingestion.

Resolves table load order based on dependencies (foreign keys, lookups, etc.).
Implements topological sort to determine correct execution sequence.
"""

import logging
from typing import List, Dict, Set, Any, Optional
from collections import defaultdict, deque

from metadata.catalog import MetadataCatalog

logger = logging.getLogger(__name__)


class CircularDependencyError(Exception):
    """Raised when circular dependencies are detected."""
    pass


class DependencyManager:
    """
    Manages table dependencies and determines load order.

    Uses topological sort to resolve dependencies and detect cycles.
    """

    def __init__(self, catalog: MetadataCatalog):
        """
        Initialize dependency manager.

        Args:
            catalog: Metadata catalog instance
        """
        self.catalog = catalog
        self.dependency_graph = defaultdict(list)
        self.reverse_graph = defaultdict(list)
        self.in_degree = defaultdict(int)

    def build_dependency_graph(self, mapping_ids: List[int]):
        """
        Build dependency graph for a set of mappings.

        Args:
            mapping_ids: List of mapping IDs to process
        """
        self.dependency_graph.clear()
        self.reverse_graph.clear()
        self.in_degree.clear()

        logger.info(f"Building dependency graph for {len(mapping_ids)} mappings")

        for mapping_id in mapping_ids:
            # Initialize in-degree for this mapping
            if mapping_id not in self.in_degree:
                self.in_degree[mapping_id] = 0

            # Get dependencies for this mapping
            dependencies = self.catalog.get_dependencies(mapping_id)

            for dep in dependencies:
                parent_id = dep['parent_mapping_id']

                # Add edge from parent to child
                self.dependency_graph[parent_id].append(mapping_id)
                self.reverse_graph[mapping_id].append(parent_id)

                # Increment in-degree for child
                self.in_degree[mapping_id] += 1

                # Ensure parent is in graph
                if parent_id not in self.in_degree:
                    self.in_degree[parent_id] = 0

        logger.info(
            f"Dependency graph built: {len(self.dependency_graph)} nodes, "
            f"{sum(len(v) for v in self.dependency_graph.values())} edges"
        )

    def resolve_execution_order(self, mapping_ids: List[int]) -> List[int]:
        """
        Resolve execution order using topological sort.

        Args:
            mapping_ids: List of mapping IDs to order

        Returns:
            List[int]: Mapping IDs in execution order

        Raises:
            CircularDependencyError: If circular dependencies detected
        """
        # Build dependency graph
        self.build_dependency_graph(mapping_ids)

        # Topological sort using Kahn's algorithm
        execution_order = []
        queue = deque()

        # Start with nodes that have no dependencies
        for mapping_id in mapping_ids:
            if self.in_degree[mapping_id] == 0:
                queue.append(mapping_id)

        logger.info(f"Starting topological sort with {len(queue)} independent mappings")

        # Process queue
        while queue:
            # Get next node with no dependencies
            current = queue.popleft()
            execution_order.append(current)

            # Reduce in-degree for dependent nodes
            for child in self.dependency_graph[current]:
                if child in mapping_ids:  # Only process mappings in our set
                    self.in_degree[child] -= 1

                    # If all dependencies satisfied, add to queue
                    if self.in_degree[child] == 0:
                        queue.append(child)

        # Check if all mappings were processed
        if len(execution_order) != len(mapping_ids):
            # Circular dependency detected
            unprocessed = set(mapping_ids) - set(execution_order)
            self._detect_circular_dependencies(unprocessed)

        logger.info(f"Execution order resolved: {len(execution_order)} mappings")
        return execution_order

    def _detect_circular_dependencies(self, unprocessed: Set[int]):
        """
        Detect and report circular dependencies.

        Args:
            unprocessed: Set of mapping IDs that could not be processed

        Raises:
            CircularDependencyError: With details about the cycle
        """
        # Find a cycle using DFS
        visited = set()
        path = []

        def dfs(node):
            if node in path:
                # Found cycle
                cycle_start = path.index(node)
                cycle = path[cycle_start:] + [node]
                return cycle
            if node in visited:
                return None

            visited.add(node)
            path.append(node)

            for child in self.dependency_graph[node]:
                if child in unprocessed:
                    result = dfs(child)
                    if result:
                        return result

            path.pop()
            return None

        # Start DFS from unprocessed nodes
        for node in unprocessed:
            if node not in visited:
                cycle = dfs(node)
                if cycle:
                    # Get mapping names for better error message
                    cycle_names = []
                    for mapping_id in cycle:
                        mapping = self.catalog.get_table_mapping(mapping_id)
                        if mapping:
                            cycle_names.append(
                                f"{mapping['mapping_code']} (ID: {mapping_id})"
                            )
                        else:
                            cycle_names.append(f"ID: {mapping_id}")

                    cycle_str = " -> ".join(cycle_names)
                    raise CircularDependencyError(
                        f"Circular dependency detected: {cycle_str}"
                    )

        # If we get here, there's a cycle but we couldn't find it
        raise CircularDependencyError(
            f"Circular dependency detected among mappings: {unprocessed}"
        )

    def get_execution_levels(self, mapping_ids: List[int]) -> List[List[int]]:
        """
        Group mappings into execution levels.

        Mappings at the same level can be executed in parallel.

        Args:
            mapping_ids: List of mapping IDs

        Returns:
            List[List[int]]: List of levels, each containing mapping IDs
        """
        # Get execution order
        execution_order = self.resolve_execution_order(mapping_ids)

        # Group by dependency level
        levels = []
        processed = set()
        remaining = set(execution_order)

        while remaining:
            # Find mappings with all dependencies satisfied
            current_level = []

            for mapping_id in execution_order:
                if mapping_id in remaining:
                    # Check if all dependencies are processed
                    dependencies = self.reverse_graph.get(mapping_id, [])
                    if all(dep in processed or dep not in mapping_ids
                          for dep in dependencies):
                        current_level.append(mapping_id)

            if not current_level:
                # Should not happen if topological sort worked
                break

            levels.append(current_level)
            processed.update(current_level)
            remaining -= set(current_level)

        logger.info(f"Grouped into {len(levels)} execution levels")
        for i, level in enumerate(levels):
            logger.debug(f"  Level {i + 1}: {len(level)} mappings")

        return levels

    def get_dependencies_for_mapping(self, mapping_id: int) -> List[Dict[str, Any]]:
        """
        Get all dependencies for a mapping.

        Args:
            mapping_id: Mapping ID

        Returns:
            List[Dict]: Dependency information
        """
        return self.catalog.get_dependencies(mapping_id)

    def validate_dependencies(self, mapping_ids: List[int]) -> bool:
        """
        Validate that all dependencies are satisfied.

        Args:
            mapping_ids: List of mapping IDs to validate

        Returns:
            bool: True if all dependencies are satisfied

        Raises:
            ValueError: If dependencies are missing
        """
        mapping_id_set = set(mapping_ids)
        missing_dependencies = []

        for mapping_id in mapping_ids:
            dependencies = self.catalog.get_dependencies(mapping_id)

            for dep in dependencies:
                parent_id = dep['parent_mapping_id']

                # Check if parent is in our set
                if parent_id not in mapping_id_set:
                    parent_mapping = self.catalog.get_table_mapping(parent_id)
                    child_mapping = self.catalog.get_table_mapping(mapping_id)

                    missing_dependencies.append({
                        'child': child_mapping['mapping_code'] if child_mapping else f"ID:{mapping_id}",
                        'parent': parent_mapping['mapping_code'] if parent_mapping else f"ID:{parent_id}",
                        'dependency_type': dep.get('dependency_type', 'UNKNOWN')
                    })

        if missing_dependencies:
            error_msg = "Missing dependencies:\n"
            for dep in missing_dependencies:
                error_msg += (
                    f"  - {dep['child']} depends on {dep['parent']} "
                    f"({dep['dependency_type']})\n"
                )

            logger.warning(error_msg)
            # Don't raise error, just warn - parent might be already loaded
            return False

        logger.info("All dependencies satisfied")
        return True

    def visualize_dependencies(self, mapping_ids: List[int]) -> str:
        """
        Create a text visualization of dependencies.

        Args:
            mapping_ids: List of mapping IDs

        Returns:
            str: Text representation of dependency graph
        """
        self.build_dependency_graph(mapping_ids)

        lines = ["Dependency Graph:", "=" * 60]

        for mapping_id in mapping_ids:
            mapping = self.catalog.get_table_mapping(mapping_id)
            mapping_name = mapping['mapping_code'] if mapping else f"ID:{mapping_id}"

            dependencies = self.reverse_graph.get(mapping_id, [])
            dependents = self.dependency_graph.get(mapping_id, [])

            lines.append(f"\n{mapping_name} (ID: {mapping_id})")

            if dependencies:
                lines.append("  Depends on:")
                for dep_id in dependencies:
                    dep_mapping = self.catalog.get_table_mapping(dep_id)
                    dep_name = dep_mapping['mapping_code'] if dep_mapping else f"ID:{dep_id}"
                    lines.append(f"    - {dep_name}")

            if dependents:
                lines.append("  Required by:")
                for dep_id in dependents:
                    dep_mapping = self.catalog.get_table_mapping(dep_id)
                    dep_name = dep_mapping['mapping_code'] if dep_mapping else f"ID:{dep_id}"
                    lines.append(f"    - {dep_name}")

            if not dependencies and not dependents:
                lines.append("  (No dependencies)")

        return "\n".join(lines)
