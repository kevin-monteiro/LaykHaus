"""
Spark-based Federated Query Executor.

This module orchestrates federated query execution across heterogeneous data sources.
It delegates Spark-specific operations to the SparkExecutionEngine while focusing on
federation logic such as SQL parsing, transformation, and data source registration.

Key Responsibilities:
- Parse SQL to identify data sources
- Transform SQL for Spark view compatibility  
- Coordinate data source registration
- Orchestrate query execution through Spark engine

The actual Spark operations (SQL execution, DataFrame creation, etc.) are handled
by the SparkExecutionEngine in the integrations layer for better separation of concerns.

Author: LaykHaus Team
Version: 2.0.0
"""

from typing import Any, Dict, List, Optional
import logging
from dataclasses import dataclass

from laykhaus.integrations.spark import SparkExecutionEngine
from laykhaus.connectors.connection_manager import connection_manager

# Configure module logger
logger = logging.getLogger(__name__)


@dataclass
class FederationResult:
    """
    Container for federated query execution results.
    
    Attributes:
        data: List of dictionaries containing query result rows
        columns: List of column names in the result set
        row_count: Total number of rows returned
        execution_time_ms: Query execution time in milliseconds
        execution_plan: Optional Spark execution plan for debugging
        statistics: Optional dictionary of execution statistics
    """
    data: List[Dict[str, Any]]
    columns: List[str]
    row_count: int
    execution_time_ms: float
    execution_plan: Optional[str] = None
    statistics: Optional[Dict[str, Any]] = None


class SparkFederatedExecutor:
    """
    Orchestrates federated query execution using Spark as the execution engine.
    
    This class focuses on federation logic while delegating Spark-specific operations
    to the SparkExecutionEngine. It handles SQL parsing, transformation, and 
    coordination of data source registration.
    
    Attributes:
        spark_engine: SparkExecutionEngine instance for Spark operations
        connection_manager: Manager for all data source connections
    """
    
    def __init__(self):
        """Initialize the federated executor with Spark engine and connection manager."""
        self.spark_engine = SparkExecutionEngine()
        self.connection_manager = connection_manager
    
    def execute_federated_query(self, sql: str, context: Optional[Dict[str, Any]] = None) -> FederationResult:
        """
        Execute a federated SQL query across multiple data sources.
        
        This method orchestrates the federation process:
        1. Parse SQL to identify referenced tables
        2. Register data sources as Spark views
        3. Transform SQL for Spark compatibility
        4. Delegate execution to Spark engine
        5. Package and return results
        
        Args:
            sql: SQL query string with fully-qualified table names
                 Format: datasource.schema.table or datasource.`endpoint`
            context: Optional execution context (reserved for future use)
            
        Returns:
            FederationResult: Query results with execution metadata
            
        Raises:
            Exception: If no data sources can be registered or query fails
            
        Example:
            sql = '''
                SELECT p.panel_id, k.power_output
                FROM postgres.solar.panels p
                JOIN kafka.`panel-telemetry` k ON p.panel_id = k.panel_id
                WHERE p.status = 'active'
            '''
            result = executor.execute_federated_query(sql)
        """
        import time
        import re
        
        start_time = time.time()
        
        try:
            logger.info(f"Executing federated query: {sql[:100]}...")
            
            # ========== STEP 1: Parse SQL to identify data sources ==========
            # Pattern matches: datasource.schema.table or datasource.endpoint or datasource.`table-name`
            table_pattern = r'((?:postgres)\.[\w.-]+\.[\w-]+|(?:kafka|rest_api)\.[\w.-]+|(?:postgres|kafka|rest_api)\.`[^`]+`)'
            referenced_tables = re.findall(table_pattern, sql, re.IGNORECASE)
            referenced_tables = [ref.lower() for ref in referenced_tables]
            logger.info(f"Found referenced tables: {referenced_tables}")
            
            # ========== STEP 2: Register data sources ==========
            from .data_provider import DataProvider
            logger.info("Registering data sources")
            
            # Get Spark session from engine for data provider
            spark_session = self.spark_engine.get_spark_session()
            data_provider = DataProvider(spark_session, self.connection_manager)
            views = data_provider.register_data_sources(referenced_tables)
            
            if not views:
                raise Exception("Failed to register any data sources. Check your connector configurations.")
            
            # ========== STEP 3: Transform SQL for Spark views ==========
            modified_sql = self._transform_sql_for_spark(sql)
            logger.info(f"Transformed SQL for Spark: {modified_sql[:100]}")
            
            # ========== STEP 4: Execute query with Spark engine ==========
            data, columns, execution_plan = self.spark_engine.execute_sql(modified_sql, limit=100)
            
            # ========== STEP 5: Package results ==========
            execution_time = (time.time() - start_time) * 1000
            
            return FederationResult(
                data=data,
                columns=columns,
                row_count=len(data),
                execution_time_ms=execution_time,
                execution_plan=execution_plan[:500],  # Truncate for readability
                statistics={
                    "sources_accessed": len(referenced_tables) if referenced_tables else 1,
                    "execution_engine": "Apache Spark"
                }
            )
            
        except Exception as e:
            logger.error(f"Failed to execute federated query: {e}")
            raise
    
    def _transform_sql_for_spark(self, sql: str) -> str:
        """
        Transform SQL to use Spark view naming conventions.
        
        Spark views use underscores instead of dots and remove special characters.
        This method transforms the SQL to match the registered view names.
        
        Args:
            sql: Original SQL with datasource.schema.table format
            
        Returns:
            str: Transformed SQL with Spark view names
            
        Transformations:
            - postgres.schema.table -> postgres_schema_table
            - kafka.`topic-name` -> kafka_topic_name  
            - rest_api.endpoint -> rest_api_endpoint
        """
        import re
        
        # Transform postgres.schema.table to postgres_schema_table
        modified_sql = re.sub(r'\bpostgres\.(\w+)\.(\w+)\b', r'postgres_\1_\2', sql)
        
        # Transform kafka.`topic-name` or kafka.topic_name to kafka_topic_name
        modified_sql = re.sub(
            r'\bkafka\.`([^`]+)`|\bkafka\.([\w-]+)',
            lambda m: f"kafka_{(m.group(1) or m.group(2)).replace('-', '_').replace('`', '')}",
            modified_sql
        )
        
        # Transform rest_api.`endpoint` or rest_api.endpoint to rest_api_endpoint  
        modified_sql = re.sub(
            r'\brest_api\.`([^`]+)`|\brest_api\.([\w_-]+)',
            lambda m: f"rest_api_{(m.group(1) or m.group(2)).replace('-', '_').replace('`', '')}",
            modified_sql
        )
        
        return modified_sql
    
    def execute_simple_query(self, sql: str) -> FederationResult:
        """
        Execute a simple SQL query against already-registered Spark views.
        
        This bypasses the federation logic and executes directly against Spark.
        Useful for queries against single sources or pre-joined views.
        
        Args:
            sql: SQL query string to execute
            
        Returns:
            FederationResult: Query results with execution metadata
        """
        import time
        start_time = time.time()
        
        try:
            # Delegate to Spark engine
            data, columns, execution_plan = self.spark_engine.execute_sql(sql, limit=1000)
            
            execution_time = (time.time() - start_time) * 1000
            
            return FederationResult(
                data=data,
                columns=columns,
                row_count=len(data),
                execution_time_ms=execution_time,
                execution_plan=None,  # Skip plan for simple queries
                statistics=None
            )
            
        except Exception as e:
            logger.error(f"Failed to execute simple query: {e}")
            raise
    
    async def load_connector_to_spark(self, connector_id: str, table_name: str = None) -> bool:
        """
        Load data from a connector into Spark as a DataFrame.
        
        This method is for testing/debugging, allowing manual loading of specific
        tables into Spark's catalog.
        
        Args:
            connector_id: Unique identifier of the connector
            table_name: Optional table name for PostgreSQL
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            connector = self.connection_manager.get_connector(connector_id)
            if not connector:
                logger.warning(f"Connector {connector_id} not found")
                return False
            
            # For PostgreSQL, load specific table
            if 'postgres' in connector_id and table_name:
                query = f"SELECT * FROM solar.{table_name}"
                result = await connector.execute_query(query)
                
                if result and result.rows:
                    # Create DataFrame and register view
                    df = self.spark_engine.create_dataframe_from_data(result.rows)
                    self.spark_engine.create_temp_view(df, f"postgres_{table_name}")
                    logger.info(f"Loaded {table_name} from PostgreSQL into Spark")
                    return True
            
            # For other connectors, just register
            logger.info(f"Connector {connector_id} registered")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load connector {connector_id}: {e}")
            return False
    
    def get_federation_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about the federation engine.
        
        Returns:
            dict: Statistics from the Spark engine plus federation metadata
        """
        # Get Spark statistics and add federation-specific info
        stats = self.spark_engine.get_spark_statistics()
        stats.update({
            "federation_enabled": True,
            "connector_count": len(self.connection_manager.list_connectors()),
            "optimization_rules": [
                "predicate_pushdown",
                "projection_pushdown", 
                "join_reordering",
                "partition_pruning"
            ]
        })
        return stats
    
    def stop(self):
        """Shutdown the federation executor and release Spark resources."""
        self.spark_engine.stop()