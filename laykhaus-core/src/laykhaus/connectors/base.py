"""
Base connector interface and abstract classes for all data source connectors.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, AsyncIterator, Dict, List, Optional, Set

from laykhaus.core.logging import get_logger

logger = get_logger(__name__)


class ConnectorType(Enum):
    """Supported connector types."""
    POSTGRESQL = "postgresql"
    KAFKA = "kafka"
    SPARK = "spark"
    MINIO = "minio"
    REST_API = "rest_api"


class SQLFeature(Enum):
    """SQL features that a connector might support."""
    SELECT = "select"
    WHERE = "where"
    JOIN = "join"
    GROUP_BY = "group_by"
    HAVING = "having"
    ORDER_BY = "order_by"
    LIMIT = "limit"
    OFFSET = "offset"
    UNION = "union"
    SUBQUERY = "subquery"
    WINDOW_FUNCTIONS = "window_functions"
    CTE = "common_table_expressions"
    TRANSACTIONS = "transactions"


class DataType(Enum):
    """Data types that a connector might support."""
    INTEGER = "integer"
    BIGINT = "bigint"
    DECIMAL = "decimal"
    FLOAT = "float"
    DOUBLE = "double"
    VARCHAR = "varchar"
    TEXT = "text"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"
    JSON = "json"
    BINARY = "binary"
    ARRAY = "array"
    STRUCT = "struct"


class AggregateFunction(Enum):
    """Aggregate functions that a connector might support."""
    COUNT = "count"
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    GROUP_CONCAT = "group_concat"
    STDDEV = "stddev"
    VARIANCE = "variance"


class JoinType(Enum):
    """Join types that a connector might support."""
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"
    CROSS = "cross"
    SEMI = "semi"
    ANTI = "anti"


class PushdownType(Enum):
    """Types of operations that can be pushed down to source."""
    FILTER = "filter"
    PROJECTION = "projection"
    AGGREGATION = "aggregation"
    JOIN = "join"
    SORT = "sort"
    LIMIT = "limit"


class TransactionLevel(Enum):
    """Transaction support levels."""
    NONE = "none"
    READ_UNCOMMITTED = "read_uncommitted"
    READ_COMMITTED = "read_committed"
    REPEATABLE_READ = "repeatable_read"
    SERIALIZABLE = "serializable"


class ConsistencyLevel(Enum):
    """Data consistency guarantees."""
    EVENTUAL = "eventual"
    STRONG = "strong"
    BOUNDED_STALENESS = "bounded_staleness"
    SESSION = "session"
    CONSISTENT_PREFIX = "consistent_prefix"


@dataclass
class CapabilityContract:
    """
    Formal contract defining what operations a connector supports.
    This is used for capability-aware query optimization.
    """
    connector_id: str
    connector_type: ConnectorType
    sql_features: Set[SQLFeature] = field(default_factory=set)
    data_types: Set[DataType] = field(default_factory=set)
    aggregate_functions: Set[AggregateFunction] = field(default_factory=set)
    join_types: Set[JoinType] = field(default_factory=set)
    pushdown_capabilities: Set[PushdownType] = field(default_factory=set)
    transaction_support: TransactionLevel = TransactionLevel.NONE
    consistency_guarantee: ConsistencyLevel = ConsistencyLevel.EVENTUAL
    max_concurrent_connections: int = 100
    supports_streaming: bool = False
    supports_schema_discovery: bool = True
    supports_statistics: bool = False
    supports_explain_plan: bool = False
    
    def can_pushdown(self, operation: PushdownType) -> bool:
        """Check if an operation can be pushed down."""
        return operation in self.pushdown_capabilities
    
    def supports_sql_feature(self, feature: SQLFeature) -> bool:
        """Check if a SQL feature is supported."""
        return feature in self.sql_features
    
    def supports_data_type(self, data_type: DataType) -> bool:
        """Check if a data type is supported."""
        return data_type in self.data_types
    
    def validate_contract(self) -> List[str]:
        """
        Validate the capability contract for consistency.
        Returns list of validation errors.
        """
        errors = []
        
        # If connector supports joins, it should support WHERE
        if SQLFeature.JOIN in self.sql_features:
            if SQLFeature.WHERE not in self.sql_features:
                errors.append("JOIN support requires WHERE support")
        
        # If connector supports GROUP BY, it should support aggregates
        if SQLFeature.GROUP_BY in self.sql_features:
            if not self.aggregate_functions:
                errors.append("GROUP BY support requires aggregate functions")
        
        # If transaction support is claimed, verify consistency level
        if self.transaction_support != TransactionLevel.NONE:
            if self.consistency_guarantee == ConsistencyLevel.EVENTUAL:
                errors.append("Transaction support requires stronger than eventual consistency")
        
        return errors


@dataclass
class ConnectionConfig:
    """Configuration for establishing connection to a data source."""
    host: str
    port: int
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    ssl_enabled: bool = False
    connection_timeout: int = 30
    query_timeout: int = 300
    pool_size: int = 10
    extra_params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class QueryResult:
    """Result of a query execution."""
    rows: List[Dict[str, Any]]
    schema: Dict[str, str]  # column_name -> data_type
    row_count: int
    execution_time_ms: float
    from_cache: bool = False
    source_name: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HealthStatus:
    """Health status of a connector."""
    healthy: bool
    latency_ms: float
    message: str
    last_check: str
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Statistics:
    """Statistics about a data source."""
    table_count: int
    total_rows: Optional[int] = None
    total_size_bytes: Optional[int] = None
    last_updated: Optional[str] = None
    table_statistics: Dict[str, Dict[str, Any]] = field(default_factory=dict)


class BaseConnector(ABC):
    """
    Abstract base class for all data source connectors.
    Defines the interface that all connectors must implement.
    """
    
    def __init__(self, connector_id: str, config: ConnectionConfig):
        """Initialize connector with configuration."""
        self.connector_id = connector_id
        self.config = config
        self.logger = get_logger(f"{__name__}.{self.__class__.__name__}")
        self._connected = False
        self._capability_contract: Optional[CapabilityContract] = None
    
    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the data source."""
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection to the data source."""
        pass
    
    @abstractmethod
    async def execute_query(self, query: str, params: Optional[Dict] = None) -> QueryResult:
        """
        Execute a query against the data source.
        
        Args:
            query: SQL query or equivalent
            params: Query parameters
        
        Returns:
            QueryResult with data and metadata
        """
        pass
    
    @abstractmethod
    def get_capabilities(self) -> CapabilityContract:
        """
        Get the capability contract for this connector.
        
        Returns:
            CapabilityContract describing what this connector supports
        """
        pass
    
    @abstractmethod
    async def get_schema(self) -> Dict[str, Any]:
        """
        Discover and return the schema of the data source.
        
        Returns:
            Schema information as a dictionary
        """
        pass
    
    @abstractmethod
    async def health_check(self) -> HealthStatus:
        """
        Perform a health check on the connection.
        
        Returns:
            HealthStatus indicating connection health
        """
        pass
    
    @abstractmethod
    async def get_statistics(self) -> Statistics:
        """
        Get statistics about the data source.
        
        Returns:
            Statistics object with metadata
        """
        pass
    
    async def test_connection(self) -> bool:
        """
        Test if the connection is working.
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            health = await self.health_check()
            # health_check returns a HealthStatus object with a 'healthy' field
            return health.healthy if hasattr(health, 'healthy') else False
        except Exception:
            return False
    
    async def subscribe_to_changes(self) -> AsyncIterator[Dict[str, Any]]:
        """
        Subscribe to real-time changes from the data source.
        Optional - only for streaming sources.
        
        Yields:
            Change events as dictionaries
        """
        raise NotImplementedError(f"{self.__class__.__name__} does not support streaming")
    
    async def execute_batch(self, queries: List[str]) -> List[QueryResult]:
        """
        Execute multiple queries in batch.
        Optional - can fall back to sequential execution.
        
        Args:
            queries: List of queries to execute
        
        Returns:
            List of QueryResults
        """
        results = []
        for query in queries:
            result = await self.execute_query(query)
            results.append(result)
        return results
    
    async def get_explain_plan(self, query: str) -> Dict[str, Any]:
        """
        Get the execution plan for a query.
        Optional - not all connectors support this.
        
        Args:
            query: Query to explain
        
        Returns:
            Execution plan as dictionary
        """
        raise NotImplementedError(f"{self.__class__.__name__} does not support explain plans")
    
    @property
    def is_connected(self) -> bool:
        """Check if connector is connected."""
        return self._connected
    
    def __str__(self) -> str:
        """String representation."""
        return f"{self.__class__.__name__}(id={self.connector_id})"
    
    def __repr__(self) -> str:
        """Detailed representation."""
        return (
            f"{self.__class__.__name__}("
            f"id={self.connector_id}, "
            f"connected={self._connected}, "
            f"host={self.config.host}:{self.config.port}"
            f")"
        )