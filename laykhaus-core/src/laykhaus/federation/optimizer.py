"""
Query Optimizer for LaykHaus Federation Engine.

Optimizes query execution plans for better performance and cost.
"""

from copy import deepcopy
from typing import List, Optional, Tuple

from laykhaus.connectors.connection_manager import connection_manager
from laykhaus.core.logging import get_logger
from laykhaus.federation.planner import (
    ExecutionLocation,
    OperationType,
    PlanNode,
    QueryPlan,
)

logger = get_logger(__name__)


class OptimizationRule:
    """Base class for optimization rules."""
    
    def apply(self, node: PlanNode) -> Optional[PlanNode]:
        """
        Apply optimization rule to a plan node.
        
        Args:
            node: Plan node to optimize
        
        Returns:
            Optimized node or None if no optimization applied
        """
        raise NotImplementedError


class PredicatePushdownRule(OptimizationRule):
    """Push filter predicates closer to data sources."""
    
    def apply(self, node: PlanNode) -> Optional[PlanNode]:
        """Push filters down the plan tree."""
        if node.operation != OperationType.FILTER:
            return None
        
        if not node.children:
            return None
        
        child = node.children[0]
        
        # Check if we can push filter to source
        if child.operation == OperationType.SCAN and child.source:
            connector = connection_manager.get_connector(child.source)
            if connector:
                capabilities = connector.get_capabilities()
                from laykhaus.connectors.base import PushdownType
                
                if capabilities.can_pushdown(PushdownType.FILTER):
                    # Merge filter into scan
                    child.condition = node.condition
                    child.location = ExecutionLocation.SOURCE
                    child.can_pushdown = True
                    logger.debug(f"Pushed filter to source {child.source}")
                    return child
        
        # Check if we can push through projection
        if child.operation == OperationType.PROJECT:
            # Swap filter and projection
            node.children = child.children
            child.children = [node]
            logger.debug("Pushed filter through projection")
            return child
        
        return None


class ProjectionPushdownRule(OptimizationRule):
    """Push column projections to reduce data transfer."""
    
    def apply(self, node: PlanNode) -> Optional[PlanNode]:
        """Push projections down the plan tree."""
        if node.operation != OperationType.PROJECT:
            return None
        
        if not node.children:
            return None
        
        child = node.children[0]
        
        # Check if we can push projection to source
        if child.operation == OperationType.SCAN and child.source:
            connector = connection_manager.get_connector(child.source)
            if connector:
                capabilities = connector.get_capabilities()
                from laykhaus.connectors.base import PushdownType
                
                if capabilities.can_pushdown(PushdownType.PROJECTION):
                    # Merge projection into scan
                    child.columns = node.columns
                    child.location = ExecutionLocation.SOURCE
                    child.can_pushdown = True
                    logger.debug(f"Pushed projection to source {child.source}")
                    return child
        
        return None


class JoinReorderingRule(OptimizationRule):
    """Reorder joins for optimal performance."""
    
    def apply(self, node: PlanNode) -> Optional[PlanNode]:
        """Reorder joins based on estimated cardinality."""
        if node.operation != OperationType.JOIN:
            return None
        
        if len(node.children) < 2:
            return None
        
        # Sort children by estimated rows (smallest first)
        sorted_children = sorted(node.children, key=lambda n: n.estimated_rows)
        
        if sorted_children != node.children:
            node.children = sorted_children
            logger.debug("Reordered joins by cardinality")
            return node
        
        return None


class LimitPushdownRule(OptimizationRule):
    """Push LIMIT operations to reduce intermediate results."""
    
    def apply(self, node: PlanNode) -> Optional[PlanNode]:
        """Push limits down where possible."""
        if node.operation != OperationType.LIMIT:
            return None
        
        if not node.children:
            return None
        
        child = node.children[0]
        
        # Push limit to source scan
        if child.operation == OperationType.SCAN and child.source:
            connector = connection_manager.get_connector(child.source)
            if connector:
                capabilities = connector.get_capabilities()
                from laykhaus.connectors.base import PushdownType
                
                if capabilities.can_pushdown(PushdownType.LIMIT):
                    child.estimated_rows = node.estimated_rows
                    child.can_pushdown = True
                    logger.debug(f"Pushed limit to source {child.source}")
                    # Keep limit at federation level too for safety
                    return node
        
        # Push limit through sort (they work together)
        if child.operation == OperationType.SORT:
            node.children = child.children
            child.children = [node]
            logger.debug("Pushed limit through sort")
            return child
        
        return None


class AggregationPushdownRule(OptimizationRule):
    """Push aggregations to data sources."""
    
    def apply(self, node: PlanNode) -> Optional[PlanNode]:
        """Push aggregations where possible."""
        if node.operation != OperationType.AGGREGATE:
            return None
        
        if not node.children:
            return None
        
        child = node.children[0]
        
        # Push aggregation to source
        if child.operation == OperationType.SCAN and child.source:
            connector = connection_manager.get_connector(child.source)
            if connector:
                capabilities = connector.get_capabilities()
                from laykhaus.connectors.base import PushdownType
                
                if capabilities.can_pushdown(PushdownType.AGGREGATION):
                    # Create pushed aggregation node
                    pushed_agg = PlanNode(
                        operation=OperationType.AGGREGATE,
                        location=ExecutionLocation.SOURCE,
                        source=child.source,
                        columns=node.columns,
                        can_pushdown=True
                    )
                    pushed_agg.add_child(child)
                    
                    # Keep federation aggregation for final merge
                    node.location = ExecutionLocation.FEDERATION
                    node.children = [pushed_agg]
                    
                    logger.debug(f"Pushed partial aggregation to source {child.source}")
                    return node
        
        return None


class QueryOptimizer:
    """
    Optimizes query execution plans using various optimization rules.
    """
    
    def __init__(self):
        """Initialize query optimizer."""
        self.logger = get_logger(__name__)
        self.rules = [
            PredicatePushdownRule(),
            ProjectionPushdownRule(),
            JoinReorderingRule(),
            LimitPushdownRule(),
            AggregationPushdownRule(),
        ]
    
    def optimize(self, plan: QueryPlan) -> QueryPlan:
        """
        Optimize a query execution plan.
        
        Args:
            plan: Original query plan
        
        Returns:
            Optimized query plan
        """
        self.logger.debug("Starting query optimization")
        
        # Create a copy to avoid modifying original
        optimized_plan = deepcopy(plan)
        
        # Apply optimization rules iteratively
        iterations = 0
        max_iterations = 10
        improved = True
        
        while improved and iterations < max_iterations:
            improved = False
            iterations += 1
            
            # Apply rules to each node in the tree
            optimized_plan.root_node = self._optimize_node(
                optimized_plan.root_node
            )
            
            # Check if any optimization was applied
            old_cost = plan.total_estimated_cost
            self._recalculate_costs(optimized_plan)
            new_cost = optimized_plan.total_estimated_cost
            
            if new_cost < old_cost * 0.95:  # 5% improvement threshold
                improved = True
                self.logger.debug(
                    f"Iteration {iterations}: Cost reduced from {old_cost:.2f} to {new_cost:.2f}"
                )
        
        # Recalculate final metrics
        self._recalculate_costs(optimized_plan)
        self._recalculate_pushdown_coverage(optimized_plan)
        
        self.logger.info(
            f"Optimization complete: {iterations} iterations, "
            f"cost {plan.total_estimated_cost:.2f} → {optimized_plan.total_estimated_cost:.2f}, "
            f"pushdown {plan.pushdown_coverage:.1%} → {optimized_plan.pushdown_coverage:.1%}"
        )
        
        return optimized_plan
    
    def _optimize_node(self, node: PlanNode) -> PlanNode:
        """Recursively optimize a plan node."""
        # Optimize children first
        if node.children:
            node.children = [
                self._optimize_node(child) for child in node.children
            ]
        
        # Apply optimization rules
        for rule in self.rules:
            optimized = rule.apply(deepcopy(node))
            if optimized:
                # Rule was applied successfully
                return optimized
        
        return node
    
    def _recalculate_costs(self, plan: QueryPlan):
        """Recalculate costs after optimization."""
        self._calculate_node_costs(plan.root_node)
        plan.total_estimated_cost = plan.root_node.estimated_cost
        plan.total_estimated_time_ms = plan.root_node.estimated_time_ms
    
    def _calculate_node_costs(self, node: PlanNode):
        """Calculate costs for a node and its children."""
        # Calculate children first
        for child in node.children:
            self._calculate_node_costs(child)
        
        # Base costs by operation and location
        base_costs = {
            (OperationType.SCAN, ExecutionLocation.SOURCE): 5.0,
            (OperationType.SCAN, ExecutionLocation.FEDERATION): 15.0,
            (OperationType.FILTER, ExecutionLocation.SOURCE): 1.0,
            (OperationType.FILTER, ExecutionLocation.FEDERATION): 3.0,
            (OperationType.PROJECT, ExecutionLocation.SOURCE): 0.5,
            (OperationType.PROJECT, ExecutionLocation.FEDERATION): 2.0,
            (OperationType.JOIN, ExecutionLocation.SOURCE): 10.0,
            (OperationType.JOIN, ExecutionLocation.FEDERATION): 25.0,
            (OperationType.AGGREGATE, ExecutionLocation.SOURCE): 8.0,
            (OperationType.AGGREGATE, ExecutionLocation.FEDERATION): 18.0,
            (OperationType.SORT, ExecutionLocation.SOURCE): 5.0,
            (OperationType.SORT, ExecutionLocation.FEDERATION): 12.0,
            (OperationType.LIMIT, ExecutionLocation.SOURCE): 0.5,
            (OperationType.LIMIT, ExecutionLocation.FEDERATION): 1.0,
            (OperationType.FEDERATE, ExecutionLocation.FEDERATION): 10.0,
        }
        
        # Get base cost
        key = (node.operation, node.location)
        base_cost = base_costs.get(key, 10.0)
        
        # Adjust for data size
        if node.estimated_rows > 0:
            size_factor = min(node.estimated_rows / 1000, 10)  # Cap at 10x
            base_cost *= (1 + size_factor * 0.1)
        
        # Add children costs
        children_cost = sum(child.estimated_cost for child in node.children)
        
        node.estimated_cost = base_cost + children_cost
        node.estimated_time_ms = node.estimated_cost * 10  # Simple conversion
    
    def _recalculate_pushdown_coverage(self, plan: QueryPlan):
        """Recalculate pushdown coverage after optimization."""
        total_ops, pushed_ops = self._count_operations(plan.root_node)
        plan.pushdown_coverage = pushed_ops / total_ops if total_ops > 0 else 0
    
    def _count_operations(self, node: PlanNode) -> Tuple[int, int]:
        """Count total and pushed operations."""
        total = 1
        pushed = 1 if node.location == ExecutionLocation.SOURCE else 0
        
        for child in node.children:
            child_total, child_pushed = self._count_operations(child)
            total += child_total
            pushed += child_pushed
        
        return total, pushed