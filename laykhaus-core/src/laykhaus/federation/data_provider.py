"""
Data provider for federation.
Delegates to specific integrations for data source connections.
"""

from typing import Dict, Optional
from pyspark.sql import SparkSession, DataFrame
import structlog

logger = structlog.get_logger(__name__)


class DataProvider:
    """
    Provides data connections for federation by delegating to specific integrations.
    Each integration handles its own Spark DataFrame creation logic.
    """
    
    def __init__(self, spark: SparkSession, connection_manager):
        self.spark = spark
        self.connection_manager = connection_manager
        
    def register_data_sources(self, referenced_tables: list) -> Dict[str, DataFrame]:
        """
        Register data sources based on referenced tables in the query.
        Delegates to specific integration components for DataFrame creation.
        
        Args:
            referenced_tables: List of table names referenced in the SQL query
            
        Returns:
            Dictionary of view_name -> DataFrame
            
        Raises:
            Exception: If any data source connection fails
        """
        views = {}
        
        for table_ref in referenced_tables:
            parts = table_ref.split('.')
            datasource = parts[0]
            
            try:
                if datasource == 'postgres':
                    view = self._register_postgres_table(table_ref)
                elif datasource == 'kafka':
                    view = self._register_kafka_topic(table_ref)
                elif datasource == 'rest_api':
                    view = self._register_rest_endpoint(table_ref)
                else:
                    logger.warning(f"Unknown data source type: {datasource}")
                    continue
                
                if view:
                    views.update(view)
                else:
                    raise Exception(f"Failed to register data source: {table_ref}")
                    
            except Exception as e:
                logger.error(f"Error registering {table_ref}: {e}")
                raise
        
        logger.info(f"Successfully registered {len(views)} data sources")
        return views
    
    def _register_postgres_table(self, table_ref: str) -> Optional[Dict[str, DataFrame]]:
        """
        Register PostgreSQL table by delegating to PostgreSQL integration.
        
        Args:
            table_ref: Table reference like 'postgres.schema.table'
            
        Returns:
            Dictionary with view_name -> DataFrame or None if connection fails
        """
        try:
            parts = table_ref.split('.')
            if len(parts) != 3:
                logger.error(f"Invalid PostgreSQL table reference: {table_ref}")
                return None
                
            datasource, schema, table = parts
            
            # Find PostgreSQL connector
            connector = self._find_connector('pgsql_')
            if not connector:
                logger.error("No PostgreSQL connector found")
                return None
            
            # Delegate to PostgreSQL integration
            df = connector.create_spark_dataframe(self.spark, schema, table)
            
            # Register as temporary view
            view_name = f"{datasource}_{schema}_{table}".replace('-', '_')
            df.createOrReplaceTempView(view_name)
            
            logger.info(f"Registered PostgreSQL table: {table_ref} as {view_name}")
            return {view_name: df}
            
        except Exception as e:
            logger.error(f"Failed to register PostgreSQL table {table_ref}: {e}")
            return None
    
    def _register_kafka_topic(self, topic_ref: str) -> Optional[Dict[str, DataFrame]]:
        """
        Register Kafka topic by delegating to Kafka integration.
        
        Args:
            topic_ref: Topic reference like 'kafka.topic-name'
            
        Returns:
            Dictionary with view_name -> DataFrame or None if connection fails
        """
        try:
            parts = topic_ref.split('.')
            if len(parts) != 2:
                logger.error(f"Invalid Kafka topic reference: {topic_ref}")
                return None
                
            datasource, topic = parts
            # Remove backticks if present
            topic = topic.replace('`', '')
            
            # Find Kafka connector
            connector = self._find_connector('kafka_')
            if not connector:
                logger.error("No Kafka connector found")
                return None
            
            # Delegate to Kafka integration
            df = connector.create_spark_dataframe(self.spark, topic)
            
            # Register as temporary view
            view_name = f"{datasource}_{topic}".replace('-', '_').replace('`', '')
            df.createOrReplaceTempView(view_name)
            
            logger.info(f"Registered Kafka topic: {topic_ref} as {view_name}")
            return {view_name: df}
            
        except Exception as e:
            logger.error(f"Failed to register Kafka topic {topic_ref}: {e}")
            return None
    
    def _register_rest_endpoint(self, endpoint_ref: str) -> Optional[Dict[str, DataFrame]]:
        """
        Register REST API endpoint by delegating to REST integration.
        
        Args:
            endpoint_ref: Endpoint reference like 'rest_api.endpoint'
            
        Returns:
            Dictionary with view_name -> DataFrame or None if connection fails
        """
        try:
            parts = endpoint_ref.split('.')
            if len(parts) != 2:
                logger.error(f"Invalid REST API endpoint reference: {endpoint_ref}")
                return None
                
            datasource, endpoint = parts
            # Remove backticks if present
            endpoint = endpoint.replace('`', '')
            
            # Find REST API connector
            connector = self._find_connector('rest_')
            if not connector:
                logger.error("No REST API connector found")
                return None
            
            # Delegate to REST integration (use sync wrapper)
            df = connector.create_spark_dataframe_sync(self.spark, endpoint)
            
            # Register as temporary view
            view_name = f"{datasource}_{endpoint}".replace('-', '_').replace('`', '')
            df.createOrReplaceTempView(view_name)
            
            logger.info(f"Registered REST endpoint: {endpoint_ref} as {view_name}")
            return {view_name: df}
            
        except Exception as e:
            logger.error(f"Failed to register REST endpoint {endpoint_ref}: {e}")
            return None
    
    def _find_connector(self, prefix: str):
        """
        Find a connector by prefix.
        
        Args:
            prefix: Connector ID prefix (e.g., 'pgsql_', 'kafka_', 'rest_')
            
        Returns:
            Connector instance or None
        """
        for conn_id in self.connection_manager.list_connectors():
            if conn_id.startswith(prefix):
                connector = self.connection_manager.get_connector(conn_id)
                if connector and connector.is_connected:
                    logger.info(f"Using connector: {conn_id}")
                    return connector
        return None