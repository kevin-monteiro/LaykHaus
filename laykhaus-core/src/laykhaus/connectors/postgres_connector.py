"""
PostgreSQL connector implementation with advanced pushdown capabilities.
"""

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional

import asyncpg
from asyncpg.pool import Pool

from laykhaus.connectors.base import (
    AggregateFunction,
    BaseConnector,
    CapabilityContract,
    ConnectionConfig,
    ConnectorType,
    ConsistencyLevel,
    DataType,
    HealthStatus,
    JoinType,
    PushdownType,
    QueryResult,
    SQLFeature,
    Statistics,
    TransactionLevel,
)
from laykhaus.core.logging import get_logger, log_performance

logger = get_logger(__name__)


class PostgreSQLConnector(BaseConnector):
    """
    High-performance PostgreSQL connector with advanced pushdown capabilities.
    Supports full SQL pushdown, transactions, and connection pooling.
    """
    
    def __init__(self, connector_id: str, config: ConnectionConfig):
        """Initialize PostgreSQL connector."""
        super().__init__(connector_id, config)
        self.pool: Optional[Pool] = None
        self._capability_contract = self._define_capabilities()
    
    def _define_capabilities(self) -> CapabilityContract:
        """Define PostgreSQL connector capabilities."""
        return CapabilityContract(
            connector_id=self.connector_id,
            connector_type=ConnectorType.POSTGRESQL,
            sql_features={
                SQLFeature.SELECT,
                SQLFeature.WHERE,
                SQLFeature.JOIN,
                SQLFeature.GROUP_BY,
                SQLFeature.HAVING,
                SQLFeature.ORDER_BY,
                SQLFeature.LIMIT,
                SQLFeature.OFFSET,
                SQLFeature.UNION,
                SQLFeature.SUBQUERY,
                SQLFeature.WINDOW_FUNCTIONS,
                SQLFeature.CTE,
                SQLFeature.TRANSACTIONS,
            },
            data_types={
                DataType.INTEGER,
                DataType.BIGINT,
                DataType.DECIMAL,
                DataType.FLOAT,
                DataType.DOUBLE,
                DataType.VARCHAR,
                DataType.TEXT,
                DataType.BOOLEAN,
                DataType.DATE,
                DataType.TIMESTAMP,
                DataType.JSON,
                DataType.BINARY,
                DataType.ARRAY,
            },
            aggregate_functions={
                AggregateFunction.COUNT,
                AggregateFunction.SUM,
                AggregateFunction.AVG,
                AggregateFunction.MIN,
                AggregateFunction.MAX,
                AggregateFunction.GROUP_CONCAT,
                AggregateFunction.STDDEV,
                AggregateFunction.VARIANCE,
            },
            join_types={
                JoinType.INNER,
                JoinType.LEFT,
                JoinType.RIGHT,
                JoinType.FULL,
                JoinType.CROSS,
            },
            pushdown_capabilities={
                PushdownType.FILTER,
                PushdownType.PROJECTION,
                PushdownType.AGGREGATION,
                PushdownType.JOIN,
                PushdownType.SORT,
                PushdownType.LIMIT,
            },
            transaction_support=TransactionLevel.SERIALIZABLE,
            consistency_guarantee=ConsistencyLevel.STRONG,
            max_concurrent_connections=self.config.pool_size,
            supports_streaming=False,
            supports_schema_discovery=True,
            supports_statistics=True,
            supports_explain_plan=True,
        )
    
    async def connect(self) -> None:
        """Establish connection pool to PostgreSQL."""
        try:
            self.logger.info(
                "Connecting to PostgreSQL",
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
            )
            
            # Build connection DSN
            dsn = (
                f"postgresql://{self.config.username}:{self.config.password}"
                f"@{self.config.host}:{self.config.port}/{self.config.database}"
            )
            
            # Create connection pool
            self.pool = await asyncpg.create_pool(
                dsn,
                min_size=2,
                max_size=self.config.pool_size,
                command_timeout=self.config.query_timeout,
                server_settings={
                    'jit': 'off',  # Disable JIT for more predictable performance
                }
            )
            
            # Test connection
            async with self.pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            
            self._connected = True
            self.logger.info("Successfully connected to PostgreSQL")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
    
    async def disconnect(self) -> None:
        """Close connection pool."""
        if self.pool:
            await self.pool.close()
            self._connected = False
            self.logger.info("Disconnected from PostgreSQL")
    
    @log_performance("PostgreSQL query execution")
    async def execute_query(
        self, 
        query: str, 
        params: Optional[Dict] = None
    ) -> QueryResult:
        """
        Execute SQL query against PostgreSQL.
        
        Args:
            query: SQL query
            params: Query parameters
        
        Returns:
            QueryResult with data and metadata
        """
        if not self._connected or not self.pool:
            raise RuntimeError("Not connected to PostgreSQL")
        
        start_time = asyncio.get_event_loop().time()
        
        async with self.pool.acquire() as conn:
            try:
                # Prepare and execute query
                if params:
                    # Convert params to positional arguments for asyncpg
                    stmt = await conn.prepare(query)
                    rows = await stmt.fetch(*params.values())
                else:
                    rows = await conn.fetch(query)
                
                # Extract schema from first row
                schema = {}
                if rows:
                    for key in rows[0].keys():
                        # Get PostgreSQL type
                        type_query = f"""
                        SELECT data_type 
                        FROM information_schema.columns 
                        WHERE column_name = $1 
                        LIMIT 1
                        """
                        dtype = await conn.fetchval(type_query, key)
                        schema[key] = dtype or "unknown"
                
                # Convert rows to list of dicts
                result_rows = [dict(row) for row in rows]
                
                execution_time = (asyncio.get_event_loop().time() - start_time) * 1000
                
                return QueryResult(
                    rows=result_rows,
                    schema=schema,
                    row_count=len(result_rows),
                    execution_time_ms=execution_time,
                    from_cache=False,
                    source_name=self.connector_id,
                    metadata={
                        "query": query,
                        "params": params,
                    }
                )
                
            except Exception as e:
                self.logger.error(f"Query execution failed: {e}", query=query)
                raise
    
    def get_capabilities(self) -> CapabilityContract:
        """Get connector capabilities."""
        return self._capability_contract
    
    async def get_schema(self) -> Dict[str, Any]:
        """
        Discover PostgreSQL schema.
        
        Returns:
            Schema information including tables, columns, and types
        """
        if not self._connected or not self.pool:
            raise RuntimeError("Not connected to PostgreSQL")
        
        schema_query = """
        SELECT 
            t.table_schema,
            t.table_name,
            array_agg(
                json_build_object(
                    'column_name', c.column_name,
                    'data_type', c.data_type,
                    'is_nullable', c.is_nullable,
                    'column_default', c.column_default
                ) ORDER BY c.ordinal_position
            ) as columns
        FROM information_schema.tables t
        JOIN information_schema.columns c 
            ON t.table_schema = c.table_schema 
            AND t.table_name = c.table_name
        WHERE t.table_schema NOT IN ('pg_catalog', 'information_schema')
        GROUP BY t.table_schema, t.table_name
        ORDER BY t.table_schema, t.table_name
        """
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(schema_query)
            
            schema = {}
            for row in rows:
                schema_name = row['table_schema']
                table_name = row['table_name']
                
                if schema_name not in schema:
                    schema[schema_name] = {}
                
                schema[schema_name][table_name] = {
                    'columns': row['columns'],
                    'row_count': await self._get_table_row_count(
                        conn, schema_name, table_name
                    )
                }
            
            return schema
    
    async def _get_table_row_count(
        self, 
        conn: asyncpg.Connection, 
        schema: str, 
        table: str
    ) -> int:
        """Get approximate row count for a table."""
        try:
            query = f"SELECT COUNT(*) FROM {schema}.{table}"
            count = await conn.fetchval(query)
            return count
        except:
            return -1  # Unknown
    
    async def health_check(self) -> HealthStatus:
        """
        Check PostgreSQL connection health.
        
        Returns:
            HealthStatus with latency and status
        """
        start_time = asyncio.get_event_loop().time()
        
        try:
            if not self.pool:
                return HealthStatus(
                    healthy=False,
                    latency_ms=0,
                    message="Connection pool not initialized",
                    last_check=datetime.utcnow().isoformat(),
                    error="No connection pool"
                )
            
            async with self.pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            
            latency_ms = (asyncio.get_event_loop().time() - start_time) * 1000
            
            return HealthStatus(
                healthy=True,
                latency_ms=latency_ms,
                message="PostgreSQL is healthy",
                last_check=datetime.utcnow().isoformat(),
                metadata={
                    "pool_size": self.pool.get_size() if self.pool else 0,
                    "pool_free": self.pool.get_idle_size() if self.pool else 0,
                }
            )
            
        except Exception as e:
            latency_ms = (asyncio.get_event_loop().time() - start_time) * 1000
            
            return HealthStatus(
                healthy=False,
                latency_ms=latency_ms,
                message="PostgreSQL health check failed",
                last_check=datetime.utcnow().isoformat(),
                error=str(e)
            )
    
    async def get_statistics(self) -> Statistics:
        """
        Get PostgreSQL statistics.
        
        Returns:
            Statistics about tables and database
        """
        if not self._connected or not self.pool:
            raise RuntimeError("Not connected to PostgreSQL")
        
        stats_query = """
        SELECT 
            schemaname,
            tablename,
            n_live_tup as row_count,
            pg_total_relation_size(schemaname||'.'||tablename) as size_bytes,
            last_vacuum,
            last_autovacuum,
            last_analyze,
            last_autoanalyze
        FROM pg_stat_user_tables
        ORDER BY schemaname, tablename
        """
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(stats_query)
            
            table_stats = {}
            total_rows = 0
            total_size = 0
            
            for row in rows:
                table_name = f"{row['schemaname']}.{row['tablename']}"
                table_stats[table_name] = {
                    'row_count': row['row_count'],
                    'size_bytes': row['size_bytes'],
                    'last_vacuum': row['last_vacuum'].isoformat() if row['last_vacuum'] else None,
                    'last_analyze': row['last_analyze'].isoformat() if row['last_analyze'] else None,
                }
                total_rows += row['row_count'] or 0
                total_size += row['size_bytes'] or 0
            
            return Statistics(
                table_count=len(table_stats),
                total_rows=total_rows,
                total_size_bytes=total_size,
                last_updated=datetime.utcnow().isoformat(),
                table_statistics=table_stats
            )
    
    async def get_explain_plan(self, query: str) -> Dict[str, Any]:
        """
        Get query execution plan from PostgreSQL.
        
        Args:
            query: SQL query to explain
        
        Returns:
            Execution plan as dictionary
        """
        if not self._connected or not self.pool:
            raise RuntimeError("Not connected to PostgreSQL")
        
        explain_query = f"EXPLAIN (FORMAT JSON, ANALYZE FALSE) {query}"
        
        async with self.pool.acquire() as conn:
            result = await conn.fetchval(explain_query)
            return result[0] if result else {}
    
    async def execute_batch(self, queries: List[str]) -> List[QueryResult]:
        """
        Execute multiple queries efficiently using a transaction.
        
        Args:
            queries: List of SQL queries
        
        Returns:
            List of QueryResults
        """
        if not self._connected or not self.pool:
            raise RuntimeError("Not connected to PostgreSQL")
        
        results = []
        
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                for query in queries:
                    result = await self.execute_query(query)
                    results.append(result)
        
        return results