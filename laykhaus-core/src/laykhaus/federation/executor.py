"""
Federation Executor for LaykHaus.

Executes query plans across heterogeneous data sources.
"""

import asyncio
from typing import Any, Dict, List, Optional

from laykhaus.connectors.base import QueryResult
from laykhaus.connectors.connection_manager import connection_manager
from laykhaus.core.logging import get_logger, log_performance
from laykhaus.federation.optimizer import QueryOptimizer
from laykhaus.federation.parser import QueryParser
from laykhaus.federation.planner import (
    ExecutionLocation,
    OperationType,
    PlanNode,
    QueryPlan,
    QueryPlanner,
)

logger = get_logger(__name__)


class FederationExecutor:
    """
    Executes federated queries according to optimized execution plans.
    """
    
    def __init__(self):
        """Initialize federation executor."""
        self.logger = get_logger(__name__)
        self.parser = QueryParser()
        self.planner = QueryPlanner()
        self.optimizer = QueryOptimizer()
    
    @log_performance("Federated query execution")
    async def execute(self, query: str, user_context: Optional[Dict] = None) -> QueryResult:
        """
        Execute a federated SQL query.
        
        Args:
            query: SQL query string
            user_context: User context for security/personalization
        
        Returns:
            QueryResult with federated data
        """
        try:
            # Parse the query
            parsed_query = self.parser.parse(query)
            
            # Create execution plan
            plan = self.planner.create_plan(parsed_query)
            
            # Optimize the plan
            optimized_plan = self.optimizer.optimize(plan)
            
            # Log execution plan
            self.logger.info(f"Executing query with plan:\n{optimized_plan.explain()}")
            
            # Execute the plan
            result = await self._execute_plan(optimized_plan, user_context)
            
            # Add execution metadata
            result.metadata.update({
                "plan": optimized_plan.to_dict(),
                "pushdown_coverage": optimized_plan.pushdown_coverage,
                "estimated_cost": optimized_plan.total_estimated_cost,
            })
            
            return result
            
        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            raise
    
    async def _execute_plan(
        self,
        plan: QueryPlan,
        user_context: Optional[Dict] = None
    ) -> QueryResult:
        """Execute an optimized query plan."""
        return await self._execute_node(plan.root_node, user_context)
    
    async def _execute_node(
        self,
        node: PlanNode,
        user_context: Optional[Dict] = None
    ) -> QueryResult:
        """
        Recursively execute a plan node.
        
        Args:
            node: Plan node to execute
            user_context: User context
        
        Returns:
            QueryResult from executing this node
        """
        self.logger.debug(
            f"Executing {node.operation.value} at {node.location.value}"
        )
        
        # Execute based on operation type
        if node.operation == OperationType.SCAN:
            return await self._execute_scan(node, user_context)
        
        elif node.operation == OperationType.FILTER:
            return await self._execute_filter(node, user_context)
        
        elif node.operation == OperationType.PROJECT:
            return await self._execute_project(node, user_context)
        
        elif node.operation == OperationType.JOIN:
            return await self._execute_join(node, user_context)
        
        elif node.operation == OperationType.AGGREGATE:
            return await self._execute_aggregate(node, user_context)
        
        elif node.operation == OperationType.SORT:
            return await self._execute_sort(node, user_context)
        
        elif node.operation == OperationType.LIMIT:
            return await self._execute_limit(node, user_context)
        
        elif node.operation == OperationType.FEDERATE:
            return await self._execute_federate(node, user_context)
        
        else:
            raise NotImplementedError(
                f"Operation {node.operation} not implemented"
            )
    
    async def _execute_scan(
        self,
        node: PlanNode,
        user_context: Optional[Dict] = None
    ) -> QueryResult:
        """Execute a table scan operation."""
        if node.location == ExecutionLocation.SOURCE and node.source:
            # Execute at source with pushdown
            connector = connection_manager.get_connector(node.source)
            if not connector:
                raise RuntimeError(f"Connector {node.source} not available")
            
            # Build query for pushdown
            if node.pushdown_sql:
                query = node.pushdown_sql
            else:
                query = self._build_scan_query(node)
            
            # Execute query at source
            result = await connector.execute_query(query)
            result.source_name = node.source
            
            return result
        else:
            # Federation-level scan (shouldn't normally happen)
            return self._create_empty_result()
    
    async def _execute_filter(
        self,
        node: PlanNode,
        user_context: Optional[Dict] = None
    ) -> QueryResult:
        """Execute a filter operation."""
        # Execute child first
        if not node.children:
            return self._create_empty_result()
        
        child_result = await self._execute_node(node.children[0], user_context)
        
        if node.location == ExecutionLocation.FEDERATION:
            # Apply filter at federation level
            filtered_rows = []
            for row in child_result.rows:
                if self._evaluate_condition(row, node.condition):
                    filtered_rows.append(row)
            
            child_result.rows = filtered_rows
            child_result.row_count = len(filtered_rows)
        
        return child_result
    
    async def _execute_project(
        self,
        node: PlanNode,
        user_context: Optional[Dict] = None
    ) -> QueryResult:
        """Execute a projection operation."""
        # Execute child first
        if not node.children:
            return self._create_empty_result()
        
        child_result = await self._execute_node(node.children[0], user_context)
        
        if node.location == ExecutionLocation.FEDERATION:
            # Apply projection at federation level
            if node.columns and node.columns != ["*"]:
                projected_rows = []
                for row in child_result.rows:
                    projected_row = {
                        col: row.get(col) for col in node.columns
                    }
                    projected_rows.append(projected_row)
                
                child_result.rows = projected_rows
                child_result.schema = {
                    col: child_result.schema.get(col, "unknown")
                    for col in node.columns
                }
        
        return child_result
    
    async def _execute_join(
        self,
        node: PlanNode,
        user_context: Optional[Dict] = None
    ) -> QueryResult:
        """Execute a join operation."""
        if len(node.children) < 2:
            return self._create_empty_result()
        
        # Execute children in parallel
        child_results = await asyncio.gather(*[
            self._execute_node(child, user_context)
            for child in node.children
        ])
        
        # Perform join at federation level
        left_result = child_results[0]
        right_result = child_results[1]
        
        # Simple nested loop join (would optimize in production)
        joined_rows = []
        for left_row in left_result.rows:
            for right_row in right_result.rows:
                if self._evaluate_join_condition(
                    left_row, right_row, node.condition
                ):
                    # Merge rows
                    joined_row = {**left_row, **right_row}
                    joined_rows.append(joined_row)
        
        # Merge schemas
        merged_schema = {**left_result.schema, **right_result.schema}
        
        return QueryResult(
            rows=joined_rows,
            schema=merged_schema,
            row_count=len(joined_rows),
            execution_time_ms=left_result.execution_time_ms + right_result.execution_time_ms,
            from_cache=False,
            source_name="federated"
        )
    
    async def _execute_aggregate(
        self,
        node: PlanNode,
        user_context: Optional[Dict] = None
    ) -> QueryResult:
        """Execute an aggregation operation."""
        # Execute child first
        if not node.children:
            return self._create_empty_result()
        
        child_result = await self._execute_node(node.children[0], user_context)
        
        if node.location == ExecutionLocation.FEDERATION:
            # Perform aggregation at federation level
            # This is simplified - production would handle various aggregate functions
            aggregated = {}
            
            for row in child_result.rows:
                # Create group key
                if node.columns:  # GROUP BY columns
                    key = tuple(row.get(col) for col in node.columns)
                else:
                    key = ()
                
                if key not in aggregated:
                    aggregated[key] = {
                        "count": 0,
                        "rows": []
                    }
                
                aggregated[key]["count"] += 1
                aggregated[key]["rows"].append(row)
            
            # Convert to result rows
            result_rows = []
            for key, agg_data in aggregated.items():
                result_row = {}
                
                # Add grouping columns
                if node.columns:
                    for i, col in enumerate(node.columns):
                        result_row[col] = key[i]
                
                # Add aggregate results
                result_row["count"] = agg_data["count"]
                
                result_rows.append(result_row)
            
            child_result.rows = result_rows
            child_result.row_count = len(result_rows)
        
        return child_result
    
    async def _execute_sort(
        self,
        node: PlanNode,
        user_context: Optional[Dict] = None
    ) -> QueryResult:
        """Execute a sort operation."""
        # Execute child first
        if not node.children:
            return self._create_empty_result()
        
        child_result = await self._execute_node(node.children[0], user_context)
        
        if node.location == ExecutionLocation.FEDERATION:
            # Sort at federation level
            if node.columns:
                child_result.rows.sort(
                    key=lambda row: tuple(row.get(col) for col in node.columns)
                )
        
        return child_result
    
    async def _execute_limit(
        self,
        node: PlanNode,
        user_context: Optional[Dict] = None
    ) -> QueryResult:
        """Execute a limit operation."""
        # Execute child first
        if not node.children:
            return self._create_empty_result()
        
        child_result = await self._execute_node(node.children[0], user_context)
        
        if node.location == ExecutionLocation.FEDERATION:
            # Apply limit at federation level
            if node.estimated_rows > 0:
                child_result.rows = child_result.rows[:node.estimated_rows]
                child_result.row_count = len(child_result.rows)
        
        return child_result
    
    async def _execute_federate(
        self,
        node: PlanNode,
        user_context: Optional[Dict] = None
    ) -> QueryResult:
        """Execute a federation operation combining multiple sources."""
        if not node.children:
            return self._create_empty_result()
        
        # Execute children in parallel
        child_results = await asyncio.gather(*[
            self._execute_node(child, user_context)
            for child in node.children
        ])
        
        # Combine results
        combined_rows = []
        combined_schema = {}
        total_time = 0
        
        for result in child_results:
            combined_rows.extend(result.rows)
            combined_schema.update(result.schema)
            total_time += result.execution_time_ms
        
        return QueryResult(
            rows=combined_rows,
            schema=combined_schema,
            row_count=len(combined_rows),
            execution_time_ms=total_time,
            from_cache=False,
            source_name="federated"
        )
    
    def _build_scan_query(self, node: PlanNode) -> str:
        """Build SQL query for a scan node."""
        # Build SELECT clause
        if node.columns and node.columns != ["*"]:
            select_clause = ", ".join(node.columns)
        else:
            select_clause = "*"
        
        # Build FROM clause
        from_clause = ", ".join(t.table for t in node.tables) if node.tables else "dual"
        
        # Build WHERE clause
        where_clause = f" WHERE {node.condition}" if node.condition else ""
        
        return f"SELECT {select_clause} FROM {from_clause}{where_clause}"
    
    def _evaluate_condition(self, row: Dict, condition: str) -> bool:
        """
        Evaluate a filter condition on a row.
        Simplified - production would use proper expression evaluation.
        """
        # This is a placeholder - real implementation would parse and evaluate
        # the condition properly
        return True
    
    def _evaluate_join_condition(
        self,
        left_row: Dict,
        right_row: Dict,
        condition: str
    ) -> bool:
        """
        Evaluate a join condition.
        Simplified - production would use proper expression evaluation.
        """
        # This is a placeholder - real implementation would parse and evaluate
        # the join condition properly
        return True
    
    def _create_empty_result(self) -> QueryResult:
        """Create an empty query result."""
        return QueryResult(
            rows=[],
            schema={},
            row_count=0,
            execution_time_ms=0.0,
            from_cache=False,
            source_name="federated"
        )