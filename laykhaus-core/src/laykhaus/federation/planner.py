"""
Query Planning Engine for LaykHaus Federation.

Generates optimal execution plans for federated queries.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set

from laykhaus.connectors.base import BaseConnector, PushdownType
from laykhaus.connectors.capability_contracts import CapabilityMatcher
from laykhaus.connectors.connection_manager import connection_manager
from laykhaus.core.logging import get_logger
from laykhaus.federation.parser import ParsedQuery, TableReference

logger = get_logger(__name__)


class OperationType(Enum):
    """Types of operations in a query plan."""
    SCAN = "scan"  # Table scan
    FILTER = "filter"  # WHERE clause
    PROJECT = "project"  # SELECT columns
    JOIN = "join"  # JOIN operation
    AGGREGATE = "aggregate"  # GROUP BY
    SORT = "sort"  # ORDER BY
    LIMIT = "limit"  # LIMIT/OFFSET
    FEDERATE = "federate"  # Cross-source operation
    UNION = "union"  # Combine results


class ExecutionLocation(Enum):
    """Where an operation is executed."""
    SOURCE = "source"  # Pushed down to data source
    FEDERATION = "federation"  # Executed in federation layer
    HYBRID = "hybrid"  # Partially pushed down


@dataclass
class PlanNode:
    """
    Node in the query execution plan tree.
    """
    operation: OperationType
    location: ExecutionLocation
    source: Optional[str] = None  # Data source name
    tables: List[TableReference] = field(default_factory=list)
    columns: List[str] = field(default_factory=list)
    condition: Optional[str] = None
    children: List["PlanNode"] = field(default_factory=list)
    
    # Cost estimates
    estimated_rows: int = 0
    estimated_cost: float = 0.0
    estimated_time_ms: float = 0.0
    
    # Metadata
    can_pushdown: bool = False
    pushdown_sql: Optional[str] = None
    
    def add_child(self, child: "PlanNode"):
        """Add a child node to this plan node."""
        self.children.append(child)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for visualization."""
        return {
            "operation": self.operation.value,
            "location": self.location.value,
            "source": self.source,
            "tables": [str(t) for t in self.tables],
            "columns": self.columns,
            "condition": self.condition,
            "estimated_rows": self.estimated_rows,
            "estimated_cost": self.estimated_cost,
            "estimated_time_ms": self.estimated_time_ms,
            "can_pushdown": self.can_pushdown,
            "children": [child.to_dict() for child in self.children]
        }


@dataclass
class QueryPlan:
    """
    Complete execution plan for a federated query.
    """
    parsed_query: ParsedQuery
    root_node: PlanNode
    total_estimated_cost: float = 0.0
    total_estimated_time_ms: float = 0.0
    pushdown_coverage: float = 0.0  # Percentage of operations pushed down
    requires_federation: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API response."""
        return {
            "query": self.parsed_query.raw_query[:200],  # First 200 chars
            "requires_federation": self.requires_federation,
            "pushdown_coverage": f"{self.pushdown_coverage:.1%}",
            "estimated_cost": self.total_estimated_cost,
            "estimated_time_ms": self.total_estimated_time_ms,
            "plan": self.root_node.to_dict()
        }
    
    def explain(self) -> str:
        """Generate human-readable execution plan."""
        lines = [
            "Query Execution Plan",
            "=" * 50,
            f"Requires Federation: {self.requires_federation}",
            f"Pushdown Coverage: {self.pushdown_coverage:.1%}",
            f"Estimated Cost: {self.total_estimated_cost:.2f}",
            f"Estimated Time: {self.total_estimated_time_ms:.0f}ms",
            "",
            "Execution Tree:",
            self._explain_node(self.root_node, 0)
        ]
        return "\n".join(lines)
    
    def _explain_node(self, node: PlanNode, indent: int) -> str:
        """Recursively explain a plan node."""
        prefix = "  " * indent + "→ "
        
        parts = [
            f"{prefix}{node.operation.value.upper()}"
        ]
        
        if node.location == ExecutionLocation.SOURCE:
            parts.append(f" [PUSHDOWN to {node.source}]")
        elif node.location == ExecutionLocation.FEDERATION:
            parts.append(" [FEDERATION]")
        
        if node.tables:
            parts.append(f" tables: {', '.join(t.table for t in node.tables)}")
        
        if node.condition:
            parts.append(f" condition: {node.condition[:50]}...")
        
        parts.append(f" (rows: ~{node.estimated_rows}, cost: {node.estimated_cost:.2f})")
        
        result = ["".join(parts)]
        
        for child in node.children:
            result.append(self._explain_node(child, indent + 1))
        
        return "\n".join(result)


class QueryPlanner:
    """
    Plans optimal execution strategies for federated queries.
    """
    
    def __init__(self):
        """Initialize query planner."""
        self.logger = get_logger(__name__)
        self.capability_matcher = CapabilityMatcher()
    
    def create_plan(self, parsed_query: ParsedQuery) -> QueryPlan:
        """
        Create an execution plan for a parsed query.
        
        Args:
            parsed_query: Parsed SQL query
        
        Returns:
            QueryPlan with execution strategy
        """
        self.logger.debug(f"Creating plan for query with {len(parsed_query.tables)} tables")
        
        # Check if federation is needed
        requires_federation = parsed_query.cross_source
        
        if requires_federation:
            # Create federated execution plan
            root_node = self._create_federated_plan(parsed_query)
        else:
            # Create single-source plan
            root_node = self._create_single_source_plan(parsed_query)
        
        # Calculate costs
        self._estimate_costs(root_node)
        
        # Calculate pushdown coverage
        total_ops, pushed_ops = self._count_operations(root_node)
        pushdown_coverage = pushed_ops / total_ops if total_ops > 0 else 0
        
        plan = QueryPlan(
            parsed_query=parsed_query,
            root_node=root_node,
            total_estimated_cost=root_node.estimated_cost,
            total_estimated_time_ms=root_node.estimated_time_ms,
            pushdown_coverage=pushdown_coverage,
            requires_federation=requires_federation
        )
        
        self.logger.info(
            f"Created plan: federation={requires_federation}, "
            f"pushdown={pushdown_coverage:.1%}, "
            f"cost={plan.total_estimated_cost:.2f}"
        )
        
        return plan
    
    def _create_single_source_plan(self, query: ParsedQuery) -> PlanNode:
        """Create execution plan for single-source query."""
        source = query.tables[0].source if query.tables else "postgres_main"
        connector = connection_manager.get_connector(source)
        
        if not connector:
            # Fallback to federation if connector not available
            return self._create_federation_fallback_plan(query)
        
        # Check if we can push down the entire query
        if self._can_pushdown_full_query(connector, query):
            # Create single pushdown node
            return PlanNode(
                operation=OperationType.SCAN,
                location=ExecutionLocation.SOURCE,
                source=source,
                tables=query.tables,
                columns=query.columns,
                condition=query.where_clause,
                can_pushdown=True,
                pushdown_sql=query.raw_query
            )
        
        # Build operation tree with partial pushdown
        return self._build_operation_tree(query, connector, source)
    
    def _create_federated_plan(self, query: ParsedQuery) -> PlanNode:
        """Create execution plan for cross-source query."""
        # Group tables by source
        source_tables = query.get_source_tables()
        
        # Create scan nodes for each source
        source_nodes = []
        for source, tables in source_tables.items():
            connector = connection_manager.get_connector(source)
            if connector:
                node = self._create_source_scan_node(source, tables, query, connector)
                source_nodes.append(node)
        
        if not source_nodes:
            return self._create_federation_fallback_plan(query)
        
        # Create federation node to combine results
        if len(source_nodes) == 1:
            root = source_nodes[0]
        else:
            # Need to federate multiple sources
            root = PlanNode(
                operation=OperationType.FEDERATE,
                location=ExecutionLocation.FEDERATION,
                children=source_nodes
            )
            
            # Add JOIN operations if needed
            if query.joins:
                for join in query.joins:
                    join_node = PlanNode(
                        operation=OperationType.JOIN,
                        location=ExecutionLocation.FEDERATION,
                        condition=join.condition,
                        tables=[join.left_table, join.right_table]
                    )
                    join_node.add_child(root)
                    root = join_node
        
        # Add federation-level operations
        root = self._add_federation_operations(root, query)
        
        return root
    
    def _create_federation_fallback_plan(self, query: ParsedQuery) -> PlanNode:
        """Create fallback plan when sources aren't available."""
        return PlanNode(
            operation=OperationType.SCAN,
            location=ExecutionLocation.FEDERATION,
            tables=query.tables,
            columns=query.columns,
            condition=query.where_clause
        )
    
    def _build_operation_tree(
        self, 
        query: ParsedQuery, 
        connector: BaseConnector,
        source: str
    ) -> PlanNode:
        """Build operation tree with pushdown decisions."""
        capabilities = connector.get_capabilities()
        
        # Start with table scan
        root = PlanNode(
            operation=OperationType.SCAN,
            location=ExecutionLocation.SOURCE,
            source=source,
            tables=query.tables,
            can_pushdown=True
        )
        
        # Add WHERE clause
        if query.where_clause:
            if capabilities.can_pushdown(PushdownType.FILTER):
                root.condition = query.where_clause
                root.can_pushdown = True
            else:
                filter_node = PlanNode(
                    operation=OperationType.FILTER,
                    location=ExecutionLocation.FEDERATION,
                    condition=query.where_clause
                )
                filter_node.add_child(root)
                root = filter_node
        
        # Add GROUP BY
        if query.group_by:
            if capabilities.can_pushdown(PushdownType.AGGREGATION):
                agg_node = PlanNode(
                    operation=OperationType.AGGREGATE,
                    location=ExecutionLocation.SOURCE,
                    source=source,
                    columns=query.group_by,
                    can_pushdown=True
                )
            else:
                agg_node = PlanNode(
                    operation=OperationType.AGGREGATE,
                    location=ExecutionLocation.FEDERATION,
                    columns=query.group_by
                )
            agg_node.add_child(root)
            root = agg_node
        
        # Add ORDER BY
        if query.order_by:
            if capabilities.can_pushdown(PushdownType.SORT):
                sort_node = PlanNode(
                    operation=OperationType.SORT,
                    location=ExecutionLocation.SOURCE,
                    source=source,
                    columns=[col for col, _ in query.order_by],
                    can_pushdown=True
                )
            else:
                sort_node = PlanNode(
                    operation=OperationType.SORT,
                    location=ExecutionLocation.FEDERATION,
                    columns=[col for col, _ in query.order_by]
                )
            sort_node.add_child(root)
            root = sort_node
        
        # Add LIMIT
        if query.limit:
            if capabilities.can_pushdown(PushdownType.LIMIT):
                limit_node = PlanNode(
                    operation=OperationType.LIMIT,
                    location=ExecutionLocation.SOURCE,
                    source=source,
                    estimated_rows=query.limit,
                    can_pushdown=True
                )
            else:
                limit_node = PlanNode(
                    operation=OperationType.LIMIT,
                    location=ExecutionLocation.FEDERATION,
                    estimated_rows=query.limit
                )
            limit_node.add_child(root)
            root = limit_node
        
        # Add projection
        if query.columns and query.columns != ["*"]:
            if capabilities.can_pushdown(PushdownType.PROJECTION):
                project_node = PlanNode(
                    operation=OperationType.PROJECT,
                    location=ExecutionLocation.SOURCE,
                    source=source,
                    columns=query.columns,
                    can_pushdown=True
                )
            else:
                project_node = PlanNode(
                    operation=OperationType.PROJECT,
                    location=ExecutionLocation.FEDERATION,
                    columns=query.columns
                )
            project_node.add_child(root)
            root = project_node
        
        return root
    
    def _create_source_scan_node(
        self,
        source: str,
        tables: List[TableReference],
        query: ParsedQuery,
        connector: BaseConnector
    ) -> PlanNode:
        """Create scan node for a specific source."""
        capabilities = connector.get_capabilities()
        
        # Determine what can be pushed down
        pushdown_where = (
            query.where_clause 
            if query.where_clause and capabilities.can_pushdown(PushdownType.FILTER)
            else None
        )
        
        pushdown_columns = (
            query.columns
            if query.columns != ["*"] and capabilities.can_pushdown(PushdownType.PROJECTION)
            else ["*"]
        )
        
        # Build pushdown SQL
        pushdown_sql = self._build_pushdown_sql(
            tables, 
            pushdown_columns,
            pushdown_where
        )
        
        return PlanNode(
            operation=OperationType.SCAN,
            location=ExecutionLocation.SOURCE,
            source=source,
            tables=tables,
            columns=pushdown_columns,
            condition=pushdown_where,
            can_pushdown=True,
            pushdown_sql=pushdown_sql
        )
    
    def _add_federation_operations(self, root: PlanNode, query: ParsedQuery) -> PlanNode:
        """Add federation-level operations to the plan."""
        # Add final projection if needed
        if query.columns and query.columns != ["*"]:
            project_node = PlanNode(
                operation=OperationType.PROJECT,
                location=ExecutionLocation.FEDERATION,
                columns=query.columns
            )
            project_node.add_child(root)
            root = project_node
        
        # Add final sort if needed
        if query.order_by:
            sort_node = PlanNode(
                operation=OperationType.SORT,
                location=ExecutionLocation.FEDERATION,
                columns=[col for col, _ in query.order_by]
            )
            sort_node.add_child(root)
            root = sort_node
        
        # Add final limit if needed
        if query.limit:
            limit_node = PlanNode(
                operation=OperationType.LIMIT,
                location=ExecutionLocation.FEDERATION,
                estimated_rows=query.limit
            )
            limit_node.add_child(root)
            root = limit_node
        
        return root
    
    def _can_pushdown_full_query(
        self,
        connector: BaseConnector,
        query: ParsedQuery
    ) -> bool:
        """Check if entire query can be pushed down to source."""
        capabilities = connector.get_capabilities()
        
        # Check all required features are supported
        for feature in query.required_features:
            if not capabilities.supports_sql_feature(feature):
                return False
        
        # Check all pushdown opportunities are supported
        for pushdown in query.pushdown_opportunities:
            if not capabilities.can_pushdown(pushdown):
                return False
        
        return True
    
    def _build_pushdown_sql(
        self,
        tables: List[TableReference],
        columns: List[str],
        where_clause: Optional[str]
    ) -> str:
        """Build SQL to push down to source."""
        # Build SELECT clause
        select_cols = ", ".join(columns) if columns else "*"
        
        # Build FROM clause
        from_tables = ", ".join(t.table for t in tables)
        
        # Build complete SQL
        sql = f"SELECT {select_cols} FROM {from_tables}"
        
        if where_clause:
            sql += f" WHERE {where_clause}"
        
        return sql
    
    def _estimate_costs(self, node: PlanNode):
        """Estimate costs for a plan node and its children."""
        # Estimate children first
        for child in node.children:
            self._estimate_costs(child)
        
        # Base costs by operation type
        base_costs = {
            OperationType.SCAN: 10.0,
            OperationType.FILTER: 2.0,
            OperationType.PROJECT: 1.0,
            OperationType.JOIN: 20.0,
            OperationType.AGGREGATE: 15.0,
            OperationType.SORT: 10.0,
            OperationType.LIMIT: 1.0,
            OperationType.FEDERATE: 5.0,
            OperationType.UNION: 3.0,
        }
        
        # Get base cost
        base_cost = base_costs.get(node.operation, 5.0)
        
        # Adjust for location
        if node.location == ExecutionLocation.FEDERATION:
            base_cost *= 1.5  # Federation operations are more expensive
        elif node.location == ExecutionLocation.SOURCE and node.can_pushdown:
            base_cost *= 0.7  # Pushdown is cheaper
        
        # Add children costs
        total_cost = base_cost + sum(child.estimated_cost for child in node.children)
        
        # Estimate rows (simplified)
        if node.operation == OperationType.SCAN:
            node.estimated_rows = 1000  # Default estimate
        elif node.operation == OperationType.FILTER:
            node.estimated_rows = int(node.children[0].estimated_rows * 0.3) if node.children else 100
        elif node.operation == OperationType.LIMIT:
            node.estimated_rows = min(node.estimated_rows or 100, 100)
        else:
            node.estimated_rows = max(
                child.estimated_rows for child in node.children
            ) if node.children else 100
        
        # Set final estimates
        node.estimated_cost = total_cost
        node.estimated_time_ms = total_cost * 10  # Simple time estimate
    
    def _count_operations(self, node: PlanNode) -> tuple[int, int]:
        """Count total operations and pushed-down operations."""
        total = 1
        pushed = 1 if node.location == ExecutionLocation.SOURCE else 0
        
        for child in node.children:
            child_total, child_pushed = self._count_operations(child)
            total += child_total
            pushed += child_pushed
        
        return total, pushed