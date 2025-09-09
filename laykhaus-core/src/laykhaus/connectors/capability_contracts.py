"""
Capability contract validation and enforcement framework.
Ensures connectors accurately report and honor their capabilities.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set

from laykhaus.connectors.base import (
    BaseConnector,
    CapabilityContract,
    PushdownType,
    QueryResult,
    SQLFeature,
)
from laykhaus.core.logging import get_logger

logger = get_logger(__name__)


@dataclass
class CapabilityValidationResult:
    """Result of capability validation."""
    connector_id: str
    is_valid: bool
    passed_tests: List[str]
    failed_tests: List[str]
    warnings: List[str]
    coverage_percentage: float
    
    @property
    def summary(self) -> str:
        """Get validation summary."""
        return (
            f"Validation {'PASSED' if self.is_valid else 'FAILED'} for {self.connector_id}\n"
            f"Tests: {len(self.passed_tests)} passed, {len(self.failed_tests)} failed\n"
            f"Coverage: {self.coverage_percentage:.1f}%\n"
            f"Warnings: {len(self.warnings)}"
        )


class CapabilityContractValidator:
    """
    Validates that connectors accurately implement their declared capabilities.
    This is crucial for query optimization and pushdown decisions.
    """
    
    def __init__(self):
        """Initialize validator."""
        self.logger = get_logger(__name__)
        self.test_queries = self._generate_test_queries()
    
    def _generate_test_queries(self) -> Dict[SQLFeature, List[str]]:
        """Generate test queries for each SQL feature."""
        return {
            SQLFeature.SELECT: [
                "SELECT * FROM test_table",
                "SELECT col1, col2 FROM test_table",
            ],
            SQLFeature.WHERE: [
                "SELECT * FROM test_table WHERE col1 = 1",
                "SELECT * FROM test_table WHERE col1 > 10 AND col2 < 100",
            ],
            SQLFeature.JOIN: [
                "SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id",
                "SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id",
            ],
            SQLFeature.GROUP_BY: [
                "SELECT col1, COUNT(*) FROM test_table GROUP BY col1",
                "SELECT col1, col2, SUM(col3) FROM test_table GROUP BY col1, col2",
            ],
            SQLFeature.HAVING: [
                "SELECT col1, COUNT(*) FROM test_table GROUP BY col1 HAVING COUNT(*) > 5",
            ],
            SQLFeature.ORDER_BY: [
                "SELECT * FROM test_table ORDER BY col1",
                "SELECT * FROM test_table ORDER BY col1 DESC, col2 ASC",
            ],
            SQLFeature.LIMIT: [
                "SELECT * FROM test_table LIMIT 10",
                "SELECT * FROM test_table LIMIT 10 OFFSET 20",
            ],
            SQLFeature.SUBQUERY: [
                "SELECT * FROM test_table WHERE col1 IN (SELECT id FROM other_table)",
            ],
            SQLFeature.WINDOW_FUNCTIONS: [
                "SELECT col1, ROW_NUMBER() OVER (PARTITION BY col2 ORDER BY col3) FROM test_table",
            ],
            SQLFeature.CTE: [
                "WITH cte AS (SELECT * FROM test_table) SELECT * FROM cte",
            ],
        }
    
    async def validate_connector(
        self, 
        connector: BaseConnector,
        run_destructive_tests: bool = False
    ) -> CapabilityValidationResult:
        """
        Validate a connector's declared capabilities.
        
        Args:
            connector: Connector to validate
            run_destructive_tests: Whether to run tests that modify data
        
        Returns:
            CapabilityValidationResult with detailed results
        """
        self.logger.info(f"Starting capability validation for {connector.connector_id}")
        
        contract = connector.get_capabilities()
        passed_tests = []
        failed_tests = []
        warnings = []
        
        # Validate contract consistency
        contract_errors = contract.validate_contract()
        if contract_errors:
            warnings.extend(contract_errors)
        
        # Test declared SQL features
        for feature in contract.sql_features:
            if feature in self.test_queries:
                for query in self.test_queries[feature]:
                    test_name = f"{feature.value}: {query[:50]}..."
                    
                    if await self._test_sql_feature(connector, query):
                        passed_tests.append(test_name)
                    else:
                        failed_tests.append(test_name)
                        self.logger.warning(
                            f"Connector {connector.connector_id} failed test for {feature.value}"
                        )
        
        # Test pushdown capabilities
        for pushdown_type in contract.pushdown_capabilities:
            test_name = f"Pushdown: {pushdown_type.value}"
            
            if await self._test_pushdown_capability(connector, pushdown_type):
                passed_tests.append(test_name)
            else:
                failed_tests.append(test_name)
        
        # Calculate coverage
        total_tests = len(passed_tests) + len(failed_tests)
        coverage = (len(passed_tests) / total_tests * 100) if total_tests > 0 else 0
        
        # Determine if validation passed
        is_valid = len(failed_tests) == 0 and len(warnings) <= 3
        
        result = CapabilityValidationResult(
            connector_id=connector.connector_id,
            is_valid=is_valid,
            passed_tests=passed_tests,
            failed_tests=failed_tests,
            warnings=warnings,
            coverage_percentage=coverage
        )
        
        self.logger.info(f"Validation complete: {result.summary}")
        
        return result
    
    async def _test_sql_feature(
        self, 
        connector: BaseConnector, 
        query: str
    ) -> bool:
        """
        Test if a connector can execute a query with specific SQL feature.
        
        Args:
            connector: Connector to test
            query: Test query
        
        Returns:
            True if test passed
        """
        try:
            # Attempt to execute the query
            # In real implementation, would use test tables
            result = await connector.execute_query(query)
            return isinstance(result, QueryResult)
        except Exception as e:
            self.logger.debug(f"Feature test failed: {e}")
            return False
    
    async def _test_pushdown_capability(
        self, 
        connector: BaseConnector, 
        pushdown_type: PushdownType
    ) -> bool:
        """
        Test if a pushdown operation works correctly.
        
        Args:
            connector: Connector to test
            pushdown_type: Type of pushdown to test
        
        Returns:
            True if pushdown works
        """
        # Test queries for each pushdown type
        test_cases = {
            PushdownType.FILTER: "SELECT * FROM test_table WHERE col1 = 1",
            PushdownType.PROJECTION: "SELECT col1, col2 FROM test_table",
            PushdownType.AGGREGATION: "SELECT COUNT(*) FROM test_table",
            PushdownType.JOIN: "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id",
            PushdownType.SORT: "SELECT * FROM test_table ORDER BY col1",
            PushdownType.LIMIT: "SELECT * FROM test_table LIMIT 10",
        }
        
        query = test_cases.get(pushdown_type)
        if not query:
            return True  # No test for this pushdown type
        
        try:
            result = await connector.execute_query(query)
            return isinstance(result, QueryResult)
        except:
            return False


class CapabilityMatcher:
    """
    Matches query requirements with connector capabilities
    to determine optimal execution strategy.
    """
    
    def __init__(self):
        """Initialize matcher."""
        self.logger = get_logger(__name__)
    
    def can_execute_query(
        self, 
        connector: BaseConnector,
        required_features: Set[SQLFeature],
        required_pushdowns: Set[PushdownType]
    ) -> bool:
        """
        Check if a connector can execute a query with given requirements.
        
        Args:
            connector: Connector to check
            required_features: SQL features needed
            required_pushdowns: Pushdown operations needed
        
        Returns:
            True if connector can handle the query
        """
        contract = connector.get_capabilities()
        
        # Check SQL features
        for feature in required_features:
            if not contract.supports_sql_feature(feature):
                self.logger.debug(
                    f"Connector {connector.connector_id} lacks feature {feature.value}"
                )
                return False
        
        # Check pushdown capabilities
        for pushdown in required_pushdowns:
            if not contract.can_pushdown(pushdown):
                self.logger.debug(
                    f"Connector {connector.connector_id} cannot pushdown {pushdown.value}"
                )
                return False
        
        return True
    
    def calculate_pushdown_score(
        self, 
        connector: BaseConnector,
        operations: List[PushdownType]
    ) -> float:
        """
        Calculate a score indicating how much of the query can be pushed down.
        
        Args:
            connector: Connector to evaluate
            operations: List of operations in the query
        
        Returns:
            Score from 0.0 (no pushdown) to 1.0 (full pushdown)
        """
        if not operations:
            return 1.0
        
        contract = connector.get_capabilities()
        pushable = sum(1 for op in operations if contract.can_pushdown(op))
        
        return pushable / len(operations)
    
    def select_best_connector(
        self,
        connectors: List[BaseConnector],
        required_features: Set[SQLFeature],
        required_pushdowns: Set[PushdownType]
    ) -> Optional[BaseConnector]:
        """
        Select the best connector for a query based on capabilities.
        
        Args:
            connectors: Available connectors
            required_features: Required SQL features
            required_pushdowns: Required pushdown operations
        
        Returns:
            Best connector or None if no suitable connector
        """
        eligible = [
            c for c in connectors
            if self.can_execute_query(c, required_features, required_pushdowns)
        ]
        
        if not eligible:
            return None
        
        # Score connectors by pushdown capability
        scored = [
            (c, self.calculate_pushdown_score(c, list(required_pushdowns)))
            for c in eligible
        ]
        
        # Return connector with highest pushdown score
        scored.sort(key=lambda x: x[1], reverse=True)
        return scored[0][0]


class DifferentialTester:
    """
    Tests query execution consistency between pushdown and federated execution.
    This ensures that optimizations don't change query semantics.
    """
    
    def __init__(self):
        """Initialize differential tester."""
        self.logger = get_logger(__name__)
    
    async def test_pushdown_correctness(
        self,
        connector: BaseConnector,
        query: str,
        federated_result: QueryResult,
        tolerance: float = 0.001
    ) -> bool:
        """
        Test if pushdown execution produces same results as federated execution.
        
        Args:
            connector: Connector with pushdown
            query: Query to test
            federated_result: Result from federated execution
            tolerance: Tolerance for floating point comparisons
        
        Returns:
            True if results match
        """
        try:
            # Execute with pushdown
            pushdown_result = await connector.execute_query(query)
            
            # Compare results
            return self._compare_results(
                federated_result, 
                pushdown_result, 
                tolerance
            )
            
        except Exception as e:
            self.logger.error(f"Differential test failed: {e}")
            return False
    
    def _compare_results(
        self,
        result1: QueryResult,
        result2: QueryResult,
        tolerance: float
    ) -> bool:
        """
        Compare two query results for equivalence.
        
        Args:
            result1: First result
            result2: Second result
            tolerance: Numeric tolerance
        
        Returns:
            True if results are equivalent
        """
        # Check row count
        if result1.row_count != result2.row_count:
            self.logger.debug(
                f"Row count mismatch: {result1.row_count} vs {result2.row_count}"
            )
            return False
        
        # Check schema
        if set(result1.schema.keys()) != set(result2.schema.keys()):
            self.logger.debug("Schema mismatch")
            return False
        
        # Sort rows for comparison (order might differ)
        rows1 = sorted(result1.rows, key=lambda r: str(r))
        rows2 = sorted(result2.rows, key=lambda r: str(r))
        
        # Compare rows
        for r1, r2 in zip(rows1, rows2):
            for key in r1.keys():
                v1, v2 = r1[key], r2.get(key)
                
                # Handle numeric comparisons with tolerance
                if isinstance(v1, (int, float)) and isinstance(v2, (int, float)):
                    if abs(v1 - v2) > tolerance:
                        self.logger.debug(
                            f"Value mismatch for {key}: {v1} vs {v2}"
                        )
                        return False
                elif v1 != v2:
                    self.logger.debug(
                        f"Value mismatch for {key}: {v1} vs {v2}"
                    )
                    return False
        
        return True